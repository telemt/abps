// OWN-03/SI-10: production transitions from each backend, shared scenarios.
// These host atomics only record explored coverage AFTER the assertions. Nothing
// inside a model reads them or uses them to coordinate/synchronize participants.
use std::sync::atomic::{AtomicBool as Coverage, Ordering as HostOrdering};

fn reuse_before_join(state: &State, reached: &Coverage) {
    if state.try_acquire_next() {
        // No join, channel, diagnostic Acquire, or extra release/acquire flag
        // may supply the old-reader -> new-issuer visibility edge.
        assert_eq!(state.finished[0].load(Ordering::Relaxed), 1);
        assert_eq!(state.finished[1].load(Ordering::Relaxed), 1);
        assert_eq!(state.refs.load(), 1, "OWN-03 next generation begins at one");
        assert_eq!(state.new.admitted.load(Ordering::Relaxed), 4);
        let mut next = state.refs.try_reserve(1, 2).unwrap();
        assert_eq!(next.pending(), 1);
        next.materialize();
        assert_eq!(next.pending(), 0);
        drop(next);
        state.release_next();
        assert_eq!(state.refs.load(), 1);
        state.release_next();
        reached.store(true, HostOrdering::Relaxed);
    }
}

#[test]
fn batch_release_chain_reinitializes_one_slot_without_join() {
    for batch in [false, true] {
        let reached = std::sync::Arc::new(Coverage::new(false));
        let coverage = reached.clone();
        exhaustive(move || {
            let state = State::prepared();
            state.acquire_old();
            if batch {
                let mut rights = state.refs.try_reserve(2, 3).unwrap();
                rights.materialize();
                assert_eq!(rights.pending(), 1);
                drop(rights); // partial construction returns its unused right
            } else {
                state.refs.try_clone(2).unwrap();
            }
            assert_eq!(state.refs.load(), 2);
            let workers: Vec<_> = (0..2)
                .map(|i| {
                    let state = state.clone();
                    thread::spawn(move || {
                        state.finished[i].store(1, Ordering::Relaxed);
                        state.release_old();
                    })
                })
                .collect();
            reuse_before_join(&state, &coverage);
            for worker in workers {
                worker.join().unwrap();
            }
            // Old return completion must leave the second generation's final
            // zero intact; no old ticket may access reinitialized slot metadata.
            state.assert_drained();
        });
        assert!(
            reached.load(HostOrdering::Relaxed),
            "no next generation explored"
        );
    }
}

#[test]
fn live_clone_source_survives_peer_release_and_next_generation() {
    let reached = std::sync::Arc::new(Coverage::new(false));
    let coverage = reached.clone();
    exhaustive(move || {
        let state = State::prepared();
        state.acquire_old();
        state.refs.try_clone(2).unwrap();
        let remote = state.clone();
        let worker = thread::spawn(move || {
            remote.finished[0].store(1, Ordering::Relaxed);
            remote.release_old();
        });
        // The issuer's original is live throughout the attempt; a test must not
        // fabricate a clone of a dead source to manufacture resurrection.
        match state.refs.try_clone(3) {
            Ok(()) => assert!(!state.refs.release()),
            Err(Error::Contended) => (),
            result => panic!("unexpected clone {result:?}"),
        }
        assert_eq!(state.class.load(), 0, "source still owns the slot");
        state.finished[1].store(1, Ordering::Relaxed);
        state.release_old();
        reuse_before_join(&state, &coverage);
        worker.join().unwrap();
        state.assert_drained();
    });
    assert!(
        reached.load(HostOrdering::Relaxed),
        "no next generation explored"
    );
}
