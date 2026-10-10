use super::*;
use loom::sync::Arc;
use loom::thread;

use crate::loom_support::{exhaustive, model};

mod generations {
    use super::*;

    impl State {
        fn try_acquire_next(&self) -> bool {
            self.root.pin(1, 16).unwrap();
            self.new.reserve(4, 4).unwrap();
            match self.class.take() {
                Ok(1) => {
                    self.refs.begin();
                    self.new.commit();
                    true
                }
                Err(Error::ClassDepleted) => {
                    self.new.refund(4);
                    self.root.release();
                    false
                }
                result => panic!("unexpected acquire {result:?}"),
            }
        }
        fn release_next(&self) {
            if self.refs.release() {
                Return::new(&self.root, &self.class, &self.new, 1, 4)
                    .publish()
                    .finish();
            }
        }
        fn assert_drained(&self) {
            assert!(self.old.drained());
            assert!(self.new.drained());
            assert_eq!(self.class.load(), 1);
            assert_eq!(self.refs.load(), 0);
            assert_eq!(self.root.load(), 1);
        }
    }

    include!("../../test_support/issuer_generations.rs");
}

#[test]
fn si01_confirmation_rechecks_a_stale_empty_hint() {
    for (mask, expected, depleted) in [
        (usize::MAX, 1, Error::ClassDepleted),
        (2, 2, Error::DomainClassDepleted),
    ] {
        exhaustive(move || {
            let class = Arc::new(Bitmap::new(2));
            assert_eq!(class.take(), Ok(1));
            assert_eq!(class.take(), Ok(2));
            let hint = class.0.load(Ordering::Relaxed);
            assert_eq!(hint, 0);
            let remote = class.clone();
            let worker = thread::spawn(move || {
                remote.publish(1);
                remote.publish(2);
            });
            worker.join().unwrap();
            // Deliberately retain the earlier hint. The production continuation
            // must confirm it, not turn that old observation into a fresh error.
            // Join controls this regression; other models test reuse without it.
            assert_eq!(class.take_observed(mask, hint, depleted), Ok(expected));
            assert_eq!(class.load(), 3 & !expected);
            class.publish(expected);
            assert_eq!(class.load(), 3);
        });
    }
}

#[test]
fn si01_confirmed_depletion_and_empty_grant_preserve_other_bits() {
    exhaustive(|| {
        let class = Bitmap::new(2);
        assert_eq!(
            class.take_masked(0, Error::DomainClassDepleted),
            Err(Error::DomainClassDepleted)
        );
        assert_eq!(class.load(), 3);
        assert_eq!(class.take_masked(2, Error::DomainClassDepleted), Ok(2));
        assert_eq!(
            class.take_masked(2, Error::DomainClassDepleted),
            Err(Error::DomainClassDepleted)
        );
        assert_eq!(class.load(), 1);
        assert_eq!(class.take(), Ok(1));
        assert_eq!(class.take(), Err(Error::ClassDepleted));
        class.publish(2);
        class.publish(1);
        assert_eq!(class.load(), 3);
    });
}

#[test]
fn si01_positive_hint_survives_only_remote_publications() {
    for mask in [1, 3] {
        exhaustive(move || {
            let class = Arc::new(Bitmap::new(2));
            assert_eq!(class.take_masked(2, Error::DomainClassDepleted), Ok(2));
            let hint = class.0.load(Ordering::Relaxed);
            assert_eq!(hint, 1);
            let remote = class.clone();
            let worker = thread::spawn(move || remote.publish(2));
            // The hint belongs to THIS issuer operation: no second clear or
            // reentrant issuer is allowed between the sample and the claim.
            assert_eq!(class.take_observed(mask, hint, Error::ClassDepleted), Ok(1));
            assert_eq!(
                class.take_masked(1, Error::DomainClassDepleted),
                Err(Error::DomainClassDepleted)
            );
            worker.join().unwrap();
            assert_eq!(class.load(), 2);
            class.publish(1);
            assert_eq!(class.load(), 3);
        });
    }
}

#[test]
fn si01_depleted_witness_allows_a_later_publication() {
    use std::sync::atomic::{AtomicBool, Ordering as HostOrdering};
    let saw_late = std::sync::Arc::new(AtomicBool::new(false));
    let coverage = saw_late.clone();
    exhaustive(move || {
        let class = Arc::new(Bitmap::new(1));
        assert_eq!(class.take(), Ok(1));
        let remote = class.clone();
        let worker = thread::spawn(move || remote.publish(1));
        let attempt = class.take_observed(1, 0, Error::ClassDepleted);
        worker.join().unwrap(); // schedule/outcome check, not a visibility proof
        match attempt {
            Err(Error::ClassDepleted) => {
                assert_eq!(class.load(), 1, "depletion is not a persistent state");
                assert_eq!(class.take(), Ok(1));
                coverage.store(true, HostOrdering::Relaxed);
            }
            Ok(1) => assert_eq!(class.load(), 0),
            result => panic!("unexpected attempt {result:?}"),
        }
        class.publish(1);
        assert_eq!(class.load(), 1);
    });
    assert!(saw_late.load(HostOrdering::Relaxed));
}

#[test]
fn hard_grant_cannot_move_between_publication_and_refund() {
    model(|| {
        use std::cell::Cell;
        let state = State::prepared();
        let unassigned = Cell::new(1);
        let old_grant = Cell::new(0);
        let new_grant = Cell::new(0);
        assign_mask(&unassigned, &old_grant, 1);
        state.root.pin(1, 16).unwrap();
        state.old.reserve(4, 4).unwrap();
        assert_eq!(
            state
                .class
                .take_masked(old_grant.get(), Error::DomainClassDepleted),
            Ok(1)
        );
        state.refs.begin();
        state.old.commit();
        let (tx, rx) = loom::sync::mpsc::channel();
        let (resume, wait) = loom::sync::mpsc::channel();
        let remote = state.clone();
        let worker = thread::spawn(move || {
            assert!(remote.refs.release());
            let pending = Return::new(&remote.root, &remote.class, &remote.old, 1, 4).publish();
            tx.send(()).unwrap();
            wait.recv().unwrap();
            pending.finish();
        });
        rx.recv().unwrap();
        assert_eq!(state.class.load(), 1);
        assert!(!state.old.drained());
        assert_eq!(unassigned.get(), 0);
        assert_eq!(
            state
                .class
                .take_masked(new_grant.get(), Error::DomainClassDepleted),
            Err(Error::DomainClassDepleted)
        );
        resume.send(()).unwrap();
        // Same production drain predicate used by issuer-side record recycling.
        if state.old.drained() {
            release_mask(&unassigned, &old_grant);
            assign_mask(&unassigned, &new_grant, 1);
            assert_eq!(
                state
                    .class
                    .take_masked(new_grant.get(), Error::DomainClassDepleted),
                Ok(1)
            );
            state.class.publish(1);
        }
        worker.join().unwrap();
        if old_grant.get() != 0 {
            release_mask(&unassigned, &old_grant);
        }
        if new_grant.get() != 0 {
            release_mask(&unassigned, &new_grant);
        }
        assert_eq!(unassigned.get(), state.class.load());
        assert_eq!(state.root.load(), 1);
    });
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
    root: Claims,
    class: Bitmap,
    old: DomainLedger,
    new: DomainLedger,
    refs: References,
    finished: [AtomicUsize; 2],
}
impl State {
    fn prepared() -> Arc<Self> {
        Arc::new(Self {
            root: Claims::new(),
            class: Bitmap::new(1),
            old: DomainLedger::new(),
            new: DomainLedger::new(),
            refs: References::new(),
            finished: [AtomicUsize::new(0), AtomicUsize::new(0)],
        })
    }
    fn acquire_old(&self) {
        self.root.pin(1, 16).unwrap();
        self.old.reserve(4, 4).unwrap();
        assert_eq!(self.class.take().unwrap(), 1);
        self.refs.begin();
        self.old.commit();
    }
    fn release_old(&self) {
        if self.refs.release() {
            Return::new(&self.root, &self.class, &self.old, 1, 4)
                .publish()
                .finish();
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
        assert_eq!(state.root.load(), 1);
    });
}

#[test]
fn reuse_before_old_refund_and_close_keeps_claims_pinned() {
    exhaustive(|| {
        let state = State::prepared();
        state.acquire_old();
        let (published_tx, published_rx) = loom::sync::mpsc::channel();
        let (resume_tx, resume_rx) = loom::sync::mpsc::channel();
        let remote = state.clone();
        let worker = thread::spawn(move || {
            assert!(remote.refs.release());
            let pending = Return::new(&remote.root, &remote.class, &remote.old, 1, 4).publish();
            published_tx.send(()).unwrap();
            resume_rx.recv().unwrap();
            pending.finish();
        });
        published_rx.recv().unwrap();
        assert!(!state.old.drained());
        assert_eq!(state.old.reserve(4, 4), Err(Error::BudgetExceeded));
        state.root.pin(1, 16).unwrap();
        state.new.reserve(4, 4).unwrap();
        assert_eq!(state.class.take().unwrap(), 1);
        state.refs.begin();
        state.new.commit();
        assert_eq!(state.root.load(), 3); // root cannot be destroyed while Closing
        resume_tx.send(()).unwrap();
        worker.join().unwrap();
        assert!(state.old.drained());
        assert_eq!(state.new.admitted.load(Ordering::Acquire), 4);
        assert_eq!(state.refs.load(), 1);
        assert!(state.refs.release());
        Return::new(&state.root, &state.class, &state.new, 1, 4)
            .publish()
            .finish();
        assert_eq!(state.root.load(), 1);
        assert!(state.new.drained());
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
        assert_eq!(state.root.load(), 1);
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
                assert_eq!(state.root.load(), 1);
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
        assert_eq!(state.root.load(), 1);
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
fn admission_and_root_pin_race_only_with_refunds_and_decrements() {
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
        if state.root.pin(1, 2).is_ok() {
            assert!(state.root.load() <= 2);
            state.root.release();
        }
        worker.join().unwrap();
        assert!(state.old.drained());
        assert_eq!(state.root.load(), 1);
    });
}

#[test]
fn two_returns_and_one_issuer_conserve_distinct_bits() {
    model(|| {
        let root = Arc::new(Claims::new());
        let ledger = Arc::new(DomainLedger::new());
        let class = Arc::new(Bitmap::new(2));
        let a = class.take().unwrap();
        let b = class.take().unwrap();
        assert_ne!(a, b);
        root.pin(2, 3).unwrap();
        ledger.reserve(8, 8).unwrap();
        ledger.commit();
        ledger.commit();
        let workers: Vec<_> = [a, b]
            .into_iter()
            .map(|bit| {
                let (root, ledger, class) = (root.clone(), ledger.clone(), class.clone());
                thread::spawn(move || {
                    Return::new(&root, &class, &ledger, bit, 4)
                        .publish()
                        .finish()
                })
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
        assert_eq!(root.load(), 1);
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
        // return may still owe a root decrement, but may never touch this ledger.
        if state.old.drained() {
            assert_eq!(state.finished[0].load(Ordering::Relaxed), 1);
            state.root.pin(1, 16).unwrap();
            state.old.reserve(4, 4).unwrap();
            state.old.commit();
            assert_eq!(state.class.take(), Ok(1));
            state.refs.begin();
            worker.join().unwrap();
            assert_eq!(state.old.admitted.load(Ordering::Acquire), 4);
            assert_eq!(state.old.allocations.load(Ordering::Acquire), 1);
            state.release_old();
        } else {
            worker.join().unwrap();
        }
        assert!(state.old.drained());
        assert_eq!(state.root.load(), 1);
    });
}

#[test]
fn closed_two_allocation_drain_gates_record_reuse_before_join() {
    use std::sync::atomic::{AtomicBool, Ordering as HostOrdering};
    struct Closed {
        root: Claims,
        domain: DomainLedger,
        class: Bitmap,
        refs: [References; 2],
        finished: [AtomicUsize; 2],
    }
    let reused = std::sync::Arc::new(AtomicBool::new(false));
    let coverage = reused.clone();
    exhaustive(move || {
        let state = Arc::new(Closed {
            root: Claims::new(),
            domain: DomainLedger::new(),
            class: Bitmap::new(2),
            refs: std::array::from_fn(|_| References::new()),
            finished: std::array::from_fn(|_| AtomicUsize::new(0)),
        });
        for i in 0..2 {
            state.root.pin(1, 8).unwrap();
            state.domain.reserve(4, 8).unwrap();
            assert_eq!(state.class.take(), Ok(1 << i));
            state.refs[i].begin();
            state.domain.commit();
        }
        // LIFE-01/AP-01: close occurs after all issuer admissions/commits. Until
        // drain succeeds, only the remote returns below can change this ledger.
        let workers: Vec<_> = (0..2)
            .map(|i| {
                let state = state.clone();
                thread::spawn(move || {
                    state.finished[i].store(1, Ordering::Relaxed);
                    assert!(state.refs[i].release());
                    Return::new(&state.root, &state.class, &state.domain, 1 << i, 4)
                        .publish()
                        .finish();
                })
            })
            .collect();
        let bit = if state.domain.drained() {
            // No new admission yet: these monotone observations must remain
            // zero. Removing the allocation gate is a named negative control.
            assert_eq!(state.domain.allocations.load(Ordering::Relaxed), 0);
            assert_eq!(state.domain.admitted.load(Ordering::Relaxed), 0);
            for flag in &state.finished {
                assert_eq!(flag.load(Ordering::Relaxed), 1);
            }
            state.root.pin(1, 8).unwrap();
            state.domain.reserve(4, 8).unwrap();
            let bit = state.class.take().unwrap();
            state.refs[bit.trailing_zeros() as usize].begin();
            state.domain.commit();
            coverage.store(true, HostOrdering::Relaxed);
            Some(bit)
        } else {
            None
        };
        for worker in workers {
            worker.join().unwrap();
        }
        if let Some(bit) = bit {
            assert_eq!(state.domain.admitted.load(Ordering::Relaxed), 4);
            assert_eq!(state.domain.allocations.load(Ordering::Relaxed), 1);
            assert!(state.refs[bit.trailing_zeros() as usize].release());
            Return::new(&state.root, &state.class, &state.domain, bit, 4)
                .publish()
                .finish();
        }
        assert!(state.domain.drained());
        assert_eq!(state.class.load(), 3);
        assert_eq!(state.root.load(), 1);
    });
    assert!(
        reused.load(HostOrdering::Relaxed),
        "record reuse was not explored"
    );
}
