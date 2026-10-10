use super::*;

struct Families {
    shared: State,
    local: [LocalReferences; 2],
}
impl Families {
    fn new() -> Arc<Self> {
        let f = Arc::new(Self {
            shared: State {
                class: Bitmap::new(1),
                old: DomainLedger::new(),
                new: DomainLedger::new(),
                refs: References::new(),
                finished: [AtomicUsize::new(0), AtomicUsize::new(0)],
            },
            local: [LocalReferences::new(), LocalReferences::new()],
        });
        f.shared.acquire_old();
        f.shared.refs.begin_branches(2);
        for local in &f.local {
            local.begin();
        }
        f
    }
    fn release(&self, i: usize) {
        if self.local[i].release() {
            self.shared.release_old();
        }
    }
}

#[test]
fn br03_local_readers_happen_before_slot_and_sidecar_reuse_without_join() {
    use std::sync::atomic::{AtomicBool as Coverage, Ordering as Host};
    let reached = std::sync::Arc::new(Coverage::new(false));
    let coverage = reached.clone();
    exhaustive(move || {
        let f = Families::new();
        let threads: Vec<_> = (0..2)
            .map(|i| {
                let f = f.clone();
                thread::spawn(move || {
                    f.local[i].try_clone(2).unwrap();
                    assert!(!f.local[i].unique());
                    f.release(i);
                    f.shared.finished[i].store(1, Ordering::Relaxed);
                    f.release(i);
                })
            })
            .collect();
        if let Ok(bit) = f.shared.class.take() {
            for i in 0..2 {
                assert_eq!(f.shared.finished[i].load(Ordering::Relaxed), 1);
                // Loom Cell catches missing synchronization with BOTH former
                // readers when the new issuer reinitializes these same records.
            }
            assert_eq!(bit, 1);
            for families in [1, 2, 1] {
                f.shared.new.reserve(4, 4).unwrap();
                f.shared.refs.begin();
                f.shared.refs.begin_branches(families);
                for i in 0..families {
                    f.local[i].begin();
                    f.local[i].try_clone(2).unwrap();
                    assert!(!f.local[i].release());
                }
                for i in (0..families).rev() {
                    assert!(f.local[i].release());
                    if f.shared.refs.release() {
                        Return::new(&f.shared.class, &f.shared.new, bit, 4)
                            .publish()
                            .finish();
                    } else {
                        assert_eq!(f.shared.class.take(), Err(Error::ClassDepleted));
                    }
                }
                assert!(f.shared.new.drained());
                assert_eq!(f.shared.class.take(), Ok(bit));
            }
            f.shared.class.publish(bit); // release final probe's capacity only
            coverage.store(true, Host::Relaxed);
        }
        for t in threads {
            t.join().unwrap();
        }
        assert!(f.shared.old.drained());
        assert_eq!(f.shared.class.load(), 1);
    });
    assert!(reached.load(Host::Relaxed), "no sidecar reuse explored");
}

#[test]
fn br02_unique_family_can_migrate_then_clone_without_global_increment() {
    model(|| {
        let f = Families::new();
        let (tx, rx) = loom::sync::mpsc::channel();
        let first = f.clone();
        let a = thread::spawn(move || {
            first.local[0].try_clone(2).unwrap();
            assert_eq!(first.local[0].try_clone(2), Err(Error::ReferenceLimit));
            assert!(!first.local[0].release());
            assert!(first.local[0].unique());
            tx.send(first).unwrap(); // no Cell access after unique transfer
        });
        let b = thread::spawn(move || {
            let received = rx.recv().unwrap();
            received.local[0].try_clone(2).unwrap();
            received.release(0);
            received.release(0);
        });
        f.release(1);
        a.join().unwrap();
        b.join().unwrap();
        assert!(f.shared.old.drained());
        assert_eq!(f.shared.class.load(), 1);
    });
}

#[test]
fn br01_reserved_construction_right_survives_materialized_branch_return() {
    exhaustive(|| {
        let state = State::prepared();
        state.acquire_old();
        state.refs.begin_branches(2);
        let local = LocalReferences::new();
        local.begin();
        let remote = state.clone();
        let worker = thread::spawn(move || {
            assert!(local.release());
            remote.release_old();
        });
        // Second right is still owned by the construction guard: it has no
        // local handle/record and is refunded directly by the existing protocol.
        assert!(!state.old.drained());
        assert_eq!(state.class.take(), Err(Error::ClassDepleted));
        state.release_old();
        worker.join().unwrap();
        assert!(state.old.drained());
        assert_eq!(state.class.load(), 1);
    });
}

#[test]
fn br03_paused_family_release_pins_slot_then_old_refund_cannot_touch_new_family() {
    model(|| {
        let f = Families::new();
        f.release(1);
        let (phase_tx, phase_rx) = loom::sync::mpsc::channel();
        let (resume_tx, resume_rx) = loom::sync::mpsc::channel();
        let remote = f.clone();
        let worker = thread::spawn(move || {
            assert!(remote.local[0].release());
            phase_tx.send(()).unwrap();
            resume_rx.recv().unwrap();
            assert!(remote.shared.refs.release());
            let pending = Return::new(&remote.shared.class, &remote.shared.old, 1, 4).publish();
            phase_tx.send(()).unwrap();
            resume_rx.recv().unwrap();
            pending.finish();
        });
        phase_rx.recv().unwrap();
        assert_eq!(f.shared.refs.load(), 1);
        assert!(!f.shared.old.drained());
        assert_eq!(f.shared.class.take(), Err(Error::ClassDepleted));
        resume_tx.send(()).unwrap();
        phase_rx.recv().unwrap();
        assert!(!f.shared.old.drained());
        f.shared.new.reserve(4, 4).unwrap();
        f.shared.class.take().unwrap();
        f.shared.refs.begin();
        f.shared.refs.begin_branches(1);
        f.local[0].begin();
        f.local[0].try_clone(2).unwrap();
        resume_tx.send(()).unwrap();
        worker.join().unwrap();
        assert!(f.shared.old.drained());
        assert!(!f.shared.new.drained());
        assert!(!f.local[0].release());
        assert!(f.local[0].release());
        assert!(f.shared.refs.release());
        Return::new(&f.shared.class, &f.shared.new, 1, 4)
            .publish()
            .finish();
        assert!(f.shared.new.drained());
    });
}
