use super::*;
use loom::sync::Arc;
use loom::thread;

#[cfg(feature = "local-branches-spike")]
mod local_branches;

use crate::loom_support::{exhaustive, model};

mod generations {
    use super::*;

    impl State {
        fn try_acquire_next(&self) -> bool {
            self.new.reserve(4, 4).unwrap();
            match self.class.take() {
                Ok(1) => {
                    self.refs.begin();
                    true
                }
                Err(Error::ClassDepleted) => {
                    self.new.refund(4);
                    false
                }
                result => panic!("unexpected acquire {result:?}"),
            }
        }
        fn release_next(&self) {
            if self.refs.release() {
                Return::new(&self.class, &self.new, 1, 4).publish().finish();
            }
        }
        fn assert_drained(&self) {
            assert!(self.old.drained());
            assert!(self.new.drained());
            assert_eq!(self.class.load(), 1);
            assert_eq!(self.refs.load(), 0);
        }
    }

    include!("../../test_support/issuer_generations.rs");
}

#[test]
fn masked_acquire_preserves_other_domains_under_remote_return() {
    model(|| {
        let class = Arc::new(Bitmap::new(2));
        let a = class.take_masked(1, Error::DomainClassDepleted).unwrap();
        let remote = class.clone();
        let worker = thread::spawn(move || remote.publish(a));
        let b = class.take_masked(2, Error::DomainClassDepleted).unwrap();
        assert_eq!(b, 2);
        assert_eq!(
            class.take_masked(2, Error::DomainClassDepleted),
            Err(Error::DomainClassDepleted)
        );
        class.publish(b);
        worker.join().unwrap();
        assert_eq!(class.load(), 3);
    });
}

struct State {
    class: Bitmap,
    old: DomainLedger,
    new: DomainLedger,
    refs: References,
    finished: [AtomicUsize; 2],
}
impl State {
    fn prepared() -> Arc<Self> {
        Arc::new(Self {
            class: Bitmap::new(1),
            old: DomainLedger::new(),
            new: DomainLedger::new(),
            refs: References::new(),
            finished: [AtomicUsize::new(0), AtomicUsize::new(0)],
        })
    }
    fn acquire_old(&self) {
        self.old.reserve(4, 4).unwrap();
        assert_eq!(self.class.take().unwrap(), 1);
        self.refs.begin();
    }
    fn release_old(&self) {
        if self.refs.release() {
            Return::new(&self.class, &self.old, 1, 4).publish().finish();
        }
    }
}

#[test]
fn final_reader_happens_before_overwrite_without_join_synchronization() {
    exhaustive(|| {
        let state = State::prepared();
        state.acquire_old();
        state.refs.try_clone(3).unwrap();
        let workers: Vec<_> = (0..2)
            .map(|i| {
                let state = state.clone();
                thread::spawn(move || {
                    state.finished[i].store(1, Ordering::Relaxed);
                    state.release_old();
                })
            })
            .collect();
        // Successful take must synchronize all former readers, even before join.
        if let Ok(bit) = state.class.take() {
            assert_eq!(state.finished[0].load(Ordering::Relaxed), 1);
            assert_eq!(state.finished[1].load(Ordering::Relaxed), 1);
            state.class.publish(bit);
        }
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(state.class.load(), 1);
        assert!(state.old.drained());
    });
}

#[test]
fn si11_reuse_before_old_refund_keeps_old_charge_pinned() {
    exhaustive(|| {
        let state = State::prepared();
        state.acquire_old();
        let (published_tx, published_rx) = loom::sync::mpsc::channel();
        let (resume_tx, resume_rx) = loom::sync::mpsc::channel();
        let (finished_tx, finished_rx) = loom::sync::mpsc::channel();
        let remote = state.clone();
        let worker = thread::spawn(move || {
            assert!(remote.refs.release());
            let pending = Return::new(&remote.class, &remote.old, 1, 4).publish();
            published_tx.send(()).unwrap();
            resume_rx.recv().unwrap();
            pending.finish();
            finished_tx.send(()).unwrap();
        });
        published_rx.recv().unwrap();
        assert!(!state.old.drained());
        assert_eq!(state.old.reserve(4, 4), Err(Error::BudgetExceeded));
        state.new.reserve(4, 4).unwrap();
        assert_eq!(state.class.take().unwrap(), 1);
        state.refs.begin();
        state.refs.try_clone(3).unwrap();
        state.refs.try_clone(3).unwrap();
        resume_tx.send(()).unwrap();
        finished_rx.recv().unwrap();
        assert!(state.old.drained());
        assert_eq!(state.new.admitted.load(Ordering::Acquire), 4);
        assert_eq!(state.refs.load(), 3);
        assert!(!state.refs.release());
        assert!(!state.refs.release());
        assert!(state.refs.release());
        Return::new(&state.class, &state.new, 1, 4)
            .publish()
            .finish();
        assert!(state.new.drained());
        worker.join().unwrap();
    });
}

#[test]
fn checked_clone_races_with_nonfinal_release() {
    exhaustive(|| {
        let state = State::prepared();
        state.acquire_old();
        state.refs.try_clone(2).unwrap();
        let remote = state.clone();
        let worker = thread::spawn(move || remote.release_old());
        match state.refs.try_clone(2) {
            Ok(()) => {
                assert!(state.refs.load() <= 2);
                state.release_old();
            }
            Err(Error::Contended | Error::ReferenceLimit) => {}
            result => panic!("unexpected clone {result:?}"),
        }
        state.release_old();
        worker.join().unwrap();
        assert_eq!(state.class.load(), 1);
        assert!(state.old.drained());
    });
}

#[test]
fn si10_batch_rollback_races_with_release_without_returning_original() {
    // ABPS-008: every partial-construction prefix, using the actual reservation
    // guard while another valid handle disappears. No clone from a dead source.
    for count in 1..=3 {
        for built in 0..=count {
            exhaustive(move || {
                let state = State::prepared();
                state.acquire_old();
                state.refs.try_clone(count + 2).unwrap();
                let other = state.clone();
                let worker = thread::spawn(move || other.release_old());
                match state.refs.try_reserve(count, count + 2) {
                    Ok(mut reserved) => {
                        for _ in 0..built {
                            reserved.materialize();
                        }
                        // Actual = source + reserved/materialized + optional peer.
                        let observed = state.refs.load();
                        assert!((count + 1..=count + 2).contains(&observed));
                        for _ in 0..built {
                            state.release_old();
                        }
                        drop(reserved);
                    }
                    Err(Error::Contended) => {}
                    Err(e) => panic!("unexpected batch error {e:?}"),
                }
                assert_eq!(state.class.load(), 0, "batch cleanup reclaimed original");
                worker.join().unwrap();
                assert_eq!(state.refs.load(), 1, "SI-10 pending rights leaked");
                state.release_old();
                assert_eq!(state.class.load(), 1);
                assert!(state.old.drained());
            });
        }
    }
}

#[test]
fn si10_two_batch_attempts_conserve_rights_and_final_return() {
    exhaustive(|| {
        let state = State::prepared();
        state.acquire_old();
        state.refs.try_clone(4).unwrap();
        fn batch(state: &State) {
            match state.refs.try_reserve(2, 4) {
                Ok(mut guard) => {
                    assert!(state.refs.load() <= 4);
                    guard.materialize();
                    guard.materialize();
                    drop(guard);
                    state.release_old();
                    state.release_old();
                }
                Err(Error::Contended | Error::ReferenceLimit) => {}
                Err(e) => panic!("unexpected batch error {e:?}"),
            }
            state.release_old(); // caller's original
        }
        let other = state.clone();
        let worker = thread::spawn(move || batch(&other));
        batch(&state);
        worker.join().unwrap();
        assert_eq!(state.class.load(), 1);
        assert!(state.old.drained());
    });
}

#[test]
fn si10_zero_batch_and_overflow_do_not_change_rights() {
    exhaustive(|| {
        let refs = References::new();
        refs.begin();
        drop(refs.try_reserve(0, 0).unwrap());
        assert!(matches!(
            refs.try_reserve(usize::MAX, usize::MAX),
            Err(Error::ReferenceLimit)
        ));
        assert_eq!(refs.load(), 1);
        assert!(refs.release());
    });
}

#[test]
fn si11_admission_races_with_refunds() {
    model(|| {
        let state = State::prepared();
        state.acquire_old();
        let remote = state.clone();
        let worker = thread::spawn(move || remote.release_old());
        // One issuer, no CAS retry. Either observation is a valid failed attempt.
        match state.old.reserve(4, 4) {
            Ok(()) => state.old.refund(4),
            Err(Error::BudgetExceeded | Error::Contended) => {}
            result => panic!("unexpected admission {result:?}"),
        }
        worker.join().unwrap();
        assert!(state.old.drained());
    });
}

#[test]
fn two_returns_and_one_issuer_conserve_distinct_bits() {
    model(|| {
        let ledger = Arc::new(DomainLedger::new());
        let class = Arc::new(Bitmap::new(2));
        let a = class.take().unwrap();
        let b = class.take().unwrap();
        assert_ne!(a, b);
        ledger.reserve(8, 8).unwrap();
        let workers: Vec<_> = [a, b]
            .into_iter()
            .map(|bit| {
                let (ledger, class) = (ledger.clone(), class.clone());
                thread::spawn(move || Return::new(&class, &ledger, bit, 4).publish().finish())
            })
            .collect();
        if let Ok(bit) = class.take() {
            assert!(bit == a || bit == b);
            class.publish(bit);
        }
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(class.load(), a | b);
        assert!(ledger.drained());
    });
}

#[test]
fn domain_completion_gates_new_identity_without_join_synchronization() {
    exhaustive(|| {
        let state = State::prepared();
        state.acquire_old();
        let remote = state.clone();
        let worker = thread::spawn(move || {
            remote.finished[0].store(1, Ordering::Relaxed);
            remote.release_old();
        });
        // This is the production predicate used by issuer-side maintenance.
        // A true result authorizes a new generation in this same ledger; the old
        // refund is the last storage access; nothing remains to decrement.
        if state.old.drained() {
            assert_eq!(state.finished[0].load(Ordering::Relaxed), 1);
            state.old.reserve(4, 4).unwrap();
            assert_eq!(state.class.take(), Ok(1));
            state.refs.begin();
            worker.join().unwrap();
            assert_eq!(state.old.admitted.load(Ordering::Acquire), 4);
            state.release_old();
        } else {
            worker.join().unwrap();
        }
        assert!(state.old.drained());
    });
}

impl Bitmap {
    fn take(&self) -> Result<usize, Error> {
        self.take_masked(usize::MAX, Error::ClassDepleted)
    }
    fn take_masked(&self, mask: usize, error: Error) -> Result<usize, Error> {
        take_words(1, 0, |_| (self, mask), error).map(|(_, bit)| bit)
    }
}
#[test]
fn si12_two_words_two_returns_and_one_issuer() {
    model(|| {
        let words = Arc::new([Bitmap::new(1), Bitmap::new(1)]);
        for word in words.iter() {
            word.claim(1);
        }
        let ledger = Arc::new(DomainLedger::new());
        ledger.reserve(8, 8).unwrap();
        let workers: Vec<_> = (0..2)
            .map(|i| {
                let words = words.clone();
                let ledger = ledger.clone();
                thread::spawn(move || Return::new(&words[i], &ledger, 1, 4).publish().finish())
            })
            .collect();
        let result = take_words(2, 1, |i| (&words[i], 1), Error::ClassDepleted);
        if let Ok((i, bit)) = result {
            assert_eq!(bit, 1);
            words[i].publish(bit);
        }
        for worker in workers {
            worker.join().unwrap();
        }
        assert!(ledger.drained());
        assert_eq!(words[0].load(), 1);
        assert_eq!(words[1].load(), 1);
    });
}
#[test]
fn si11_zero_observation_acquires_every_prior_return_before_destroy() {
    exhaustive(|| {
        let ledgers = Arc::new([DomainLedger::new(), DomainLedger::new()]);
        let writes = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
        for d in ledgers.iter() {
            d.reserve(1, 1).unwrap();
        }
        let workers: Vec<_> = (0..2)
            .map(|i| {
                let ds = ledgers.clone();
                let writes = writes.clone();
                thread::spawn(move || {
                    writes[i].store(1, Ordering::Relaxed);
                    ds[i].refund(1);
                })
            })
            .collect();
        if root_drained(1, ledgers.iter()) {
            for write in writes.iter() {
                assert_eq!(write.load(Ordering::Relaxed), 1);
            }
        }
        for worker in workers {
            worker.join().unwrap();
        }
    });
}

#[test]
fn si11_root_drain_covers_last_domain_and_all_refunds_without_join() {
    use std::sync::atomic::{AtomicBool as Coverage, Ordering as Host};
    for same_domain in [false, true] {
        let reached = std::sync::Arc::new(Coverage::new(false));
        let coverage = reached.clone();
        exhaustive(move || {
            let ledgers = Arc::new(std::array::from_fn::<_, 3, _>(|_| DomainLedger::new()));
            let writes = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
            let domains = if same_domain { [2, 2] } else { [0, 2] };
            for &d in &domains {
                ledgers[d].reserve(4, 8).unwrap();
            }
            assert!(!root_drained(
                2,
                std::iter::from_fn(|| {
                    panic!("SI-11 controls must short circuit ledger access")
                })
            ));
            assert!(
                !root_drained(1, ledgers.iter()),
                "SI-11 last record pins root"
            );
            let workers: Vec<_> = domains
                .into_iter()
                .enumerate()
                .map(|(i, d)| {
                    let ledgers = ledgers.clone();
                    let writes = writes.clone();
                    thread::spawn(move || {
                        writes[i].store(1, Ordering::Relaxed);
                        ledgers[d].refund(4); // final storage access for this right
                    })
                })
                .collect();
            if root_drained(1, ledgers.iter()) {
                // No observer synchronization or join may supply this edge.
                assert_eq!(writes[0].load(Ordering::Relaxed), 1);
                assert_eq!(writes[1].load(Ordering::Relaxed), 1);
                coverage.store(true, Host::Relaxed);
            }
            for worker in workers {
                worker.join().unwrap();
            }
            assert!(root_drained(1, ledgers.iter()));
        });
        assert!(reached.load(Host::Relaxed), "no pre-join drain explored");
    }
}
#[test]
fn si12_confirmation_finds_a_publication_hidden_by_an_earlier_load() {
    model(|| {
        let words = Arc::new([Bitmap::new(1), Bitmap::new(1), Bitmap::new(1)]);
        for w in words.iter() {
            w.claim(1);
        }
        let remote = words.clone();
        let (go, wait) = loom::sync::mpsc::channel();
        let (done, ack) = loom::sync::mpsc::channel();
        let t = thread::spawn(move || {
            wait.recv().unwrap();
            remote[0].publish(1);
            done.send(()).unwrap();
        });
        let mut probes = 0;
        let result = take_words(
            3,
            0,
            |i| {
                probes += 1;
                if probes == 3 {
                    go.send(()).unwrap();
                    ack.recv().unwrap();
                }
                (&words[i], 1)
            },
            Error::ClassDepleted,
        );
        assert_eq!(result, Ok((0, 1)));
        assert_eq!(probes, 4);
        words[0].publish(1);
        t.join().unwrap();
    });
}

#[test]
fn abps_depleted_witness_before_or_after_first_confirmation() {
    // ABPS-025: a publication into an already-confirmed word may happen after
    // the empty witness. Rejecting that legal history would invent a stronger
    // snapshot contract. These channels select semantic windows; no-join models
    // separately test memory visibility without channel synchronization.
    for start in 0..2 {
        for published_word in 0..2 {
            for at_probe in 1..=4 {
                exhaustive(move || {
                    let words = Arc::new([Bitmap::new(1), Bitmap::new(1)]);
                    for w in words.iter() {
                        w.claim(1);
                    }
                    let remote = words.clone();
                    let (go, wait) = loom::sync::mpsc::channel();
                    let (done, ack) = loom::sync::mpsc::channel();
                    let worker = thread::spawn(move || {
                        wait.recv().unwrap();
                        remote[published_word].publish(1);
                        done.send(()).unwrap();
                    });
                    let mut probes = 0;
                    let result = take_words(
                        2,
                        start,
                        |i| {
                            probes += 1;
                            if probes == at_probe {
                                go.send(()).unwrap();
                                ack.recv().unwrap();
                            }
                            (&words[i], 1)
                        },
                        Error::ClassDepleted,
                    );
                    if at_probe == 4 && published_word == start {
                        assert_eq!(result, Err(Error::ClassDepleted));
                    } else {
                        assert_eq!(result, Ok((published_word, 1)));
                        words[published_word].publish(1);
                    }
                    assert!(probes <= 4);
                    worker.join().unwrap();
                    assert_eq!(words[published_word].load(), 1);
                });
            }
        }
    }
}

#[test]
fn si12_masked_observations_preserve_neighbors_for_every_cursor() {
    for start in 0..2 {
        for published in 0..2 {
            exhaustive(move || {
                // Bitmap::new accepts a slot count, not an initial mask.
                let words = Arc::new([Bitmap::new(3), Bitmap::new(4)]);
                words[0].claim(0b011);
                words[1].claim(0b0111);
                let masks = [1, 2];
                let remote = words.clone();
                let t = thread::spawn(move || remote[published].publish(masks[published]));
                let mut probes = 0;
                let result = take_words(
                    2,
                    start,
                    |i| {
                        probes += 1;
                        (&words[i], masks[i])
                    },
                    Error::DomainClassDepleted,
                );
                assert!(probes <= 4, "SI-12 two bounded passes");
                if let Ok((word, bit)) = result {
                    assert_eq!(word, published);
                    assert_eq!(bit, masks[word]);
                    words[word].publish(bit);
                } else {
                    assert_eq!(result, Err(Error::DomainClassDepleted));
                }
                t.join().unwrap();
                for i in 0..2 {
                    assert_eq!(
                        words[i].load(),
                        [4, 8][i] | if i == published { masks[i] } else { 0 }
                    );
                }
            });
        }
    }
}
