use super::super::controller::{Mode, Priority};
use super::*;
use proptest::prelude::*;

fn policy(hard: &[usize], groups: &[usize], total: usize) -> Controller {
    Controller::new(
        hard,
        groups,
        [0; GROUPS],
        total,
        Mode::Shared,
        Priority::Weighted,
        false,
        20,
    )
}

#[test]
fn ap14_whole_quantum_crosses_fractional_targets_without_creating_budget() {
    let p = policy(&[512; 2], &[0, 0], 512);
    assert_eq!(&p.soft[..2], &[256, 256]);
    let mut s = Shared::new(10);
    s.class_failure(0);
    let d = s.check(&p, 0, 0, 512).unwrap();
    assert!(d.quantum_crossing);
    s.cache.reserve(0, 0, 512).commit();
    assert_eq!(s.check(&p, 1, 0, 512), Err("SoftTotalBudget"));
    assert_eq!(s.cache.total, 512);
    s.refresh([0; STREAMS], &p); // Actual full refund, no target increase.
    assert!(s.check(&p, 1, 0, 512).is_ok());
    assert_eq!(&p.soft[..2], &[256, 256]);
}

#[test]
fn ap14_borrowing_priority_contraction_and_class_pressure_are_separate() {
    let mut p = policy(&[1024; 2], &[0, 1], 1024);
    let mut s = Shared::new(10);
    let original = p.soft;
    let d = s.check(&p, 1, 0, 768).unwrap();
    assert_eq!(d.observed_loan_bytes, 256);
    s.cache.reserve(1, 1, 768).commit();
    assert_eq!(p.soft, original);
    s.class_failure(0);
    assert_eq!(s.check(&p, 1, 0, 1), Err("SoftDebtPressure"));
    assert!(s.check(&p, 1, 1, 1).is_ok());
    assert!(s.check(&p, 0, 0, 256).is_ok()); // High/other group still below target.
    p.total = 512; // Existing debt survives contraction.
    assert_eq!(s.check(&p, 0, 1, 1), Err("SoftTotalBudget"));
    assert_eq!(s.cache.total, 768);
    p.total = 1024;
    s.now_ms = 10;
    assert!(s.check(&p, 1, 0, 1).is_ok());
}

#[test]
fn ap14_total_pressure_expiry_and_zero_interval_are_explicit() {
    for ms in [0, 10, 50, 100] {
        let p = policy(&[1024; 2], &[0, 1], 1024);
        let mut s = Shared::new(ms);
        s.cache.reserve(1, 1, 768).commit();
        s.refused("SoftTotalBudget");
        assert_eq!(s.global_pressure(), ms > 0);
        assert_eq!(s.check(&p, 1, 9, 1).is_err(), ms > 0);
        s.now_ms = ms;
        assert!(s.check(&p, 1, 9, 1).is_ok());
    }
}

#[test]
fn ap14_cache_guard_restores_error_and_unwind_without_refunding_ownership() {
    let p = policy(&[1024], &[0], 1024);
    let mut s = Shared::new(10);
    s.cache.reserve(0, 0, 64).commit();
    for panic in [false, true] {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _pending = s.cache.reserve(0, 0, 128);
            if panic {
                panic!("interrupted provisional cache charge");
            }
        }));
        assert_eq!(
            (s.cache.streams[0], s.cache.groups[0], s.cache.total),
            (64, 64, 64)
        );
    }
    assert!(s.check(&p, 0, 0, 960).is_ok());
}

#[test]
fn ap14_publish_without_refund_keeps_budget_unavailable() {
    // Controlled ledger observation, not a model of atomic publication ordering.
    let p = policy(&[64; 2], &[0, 1], 64);
    let mut s = Shared::new(10);
    let mut ledger = [0; STREAMS];
    ledger[0] = 64; // Slot published, old return not yet refunded.
    s.refresh(ledger, &p);
    assert_eq!(s.check(&p, 1, 0, 64), Err("SoftTotalBudget"));
    ledger[0] = 0; // Only the real final refund permits lowering this observation.
    s.refresh(ledger, &p);
    assert!(s.check(&p, 1, 0, 64).is_ok());
}

#[test]
fn ap14_closed_overflow_and_zero_total_do_not_mutate_observations() {
    let mut p = policy(&[usize::MAX; 2], &[0, 0], usize::MAX);
    let mut s = Shared::new(0);
    s.cache.reserve(0, 0, usize::MAX).commit();
    assert_eq!(s.check(&p, 0, 0, 1), Err("LogicalHardLimit"));
    assert_eq!(s.check(&p, 1, 0, 1), Err("SoftTotalBudget"));
    p.active[0] = false;
    assert_eq!(s.check(&p, 0, 0, 1), Err("ClosedStream"));
    assert_eq!(s.cache.total, usize::MAX);
    s.refresh([0; STREAMS], &p);
    p.total = 0;
    assert_eq!(s.check(&p, 1, 0, 1), Err("SoftTotalBudget"));
}

#[test]
fn ap14_retention_is_debt_not_new_demand() {
    let mut p = policy(&[512; 2], &[0, 1], 512);
    let mut held = [0; STREAMS];
    held[0] = 512;
    p.tick(&held, 100);
    assert_eq!(&p.soft[..2], &[0, 0]);
    assert_eq!(held[0], 512);
    p.offer(1, 512);
    p.tick(&held, 100);
    assert!(p.soft[1] > 0);
    assert_eq!(p.soft[0], 0);
}

#[test]
fn ap14_global_cap_rejects_when_each_hard_limit_still_has_space() {
    let p = policy(&[512; 2], &[0, 1], 512);
    let mut s = Shared::new(0);
    s.cache.reserve(0, 0, 256).commit();
    s.cache.reserve(1, 1, 256).commit();
    for stream in 0..2 {
        assert_eq!(s.check(&p, stream, 0, 64), Err("SoftTotalBudget"));
    }
    assert_eq!(s.cache.total, 512);
}

proptest! {
    #![proptest_config(proptest::test_runner::Config {
        cases: if cfg!(miri) { 4 } else { 256 },
        failure_persistence: if cfg!(miri) { None } else {
            Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource("proptest-regressions")))
        },
        ..Default::default()
    })]
    #[test]
    fn ap14_independent_rights_model_bounds_cached_admission(actions in prop::collection::vec(any::<u8>(), 1..200)) {
        let p = policy(&[128; 3], &[0, 1, 2], 192);
        let mut s = Shared::new(10);
        // Four physical allocations; each can have multiple owners. No model
        // soft allocation or bitmap traversal is copied from the implementation.
        let mut rights: [Option<(usize,usize,usize)>; 4] = [None;4];
        for (step, action) in actions.into_iter().enumerate() {
            let slot = action as usize % 4;
            let actual = |rights: &[Option<(usize,usize,usize)>;4]| {
                let mut out=[0;STREAMS];
                for &(stream, charge, _) in rights.iter().flatten() { out[stream]+=charge; }
                out
            };
            match action % 5 {
                0 => {
                    if let Some((stream,charge,owners))=rights[slot] {
                        rights[slot]=if owners==1 { None } else { Some((stream,charge,owners-1)) };
                    }
                },
                1 => { if let Some((_,_,owners))=&mut rights[slot] { *owners+=1; } },
                2 => { s.refresh(actual(&rights), &p); s.now_ms=step as u64*10; },
                _ => {
                    let stream=action as usize%3;
                    let charge=16*(1+action as usize%4);
                    let mut decision=s.check(&p,stream,0,charge);
                    if decision.is_err() {
                        s.refresh(actual(&rights),&p);
                        decision=s.check(&p,stream,0,charge);
                    }
                    if decision.is_ok() {
                        let before=actual(&rights);
                        prop_assert!(before[stream]+charge<=128);
                        prop_assert!(before.iter().sum::<usize>()+charge<=192);
                        let pending=s.cache.reserve(stream,p.groups[stream],charge);
                        if rights[slot].is_none() {
                            rights[slot]=Some((stream,charge,1));
                            pending.commit();
                        } else { drop(pending); s.class_failure(0); }
                    }
                }
            }
            let live=actual(&rights);
            for (i,&value) in live.iter().enumerate().take(p.n) {
                prop_assert!(s.cache.streams[i]>=value);
            }
            prop_assert_eq!(s.cache.streams.iter().sum::<usize>(),s.cache.total);
            prop_assert_eq!(s.cache.groups.iter().sum::<usize>(),s.cache.total);
            prop_assert!(live.iter().sum::<usize>()<=192);
        }
        rights.fill(None);
        s.refresh([0;STREAMS],&p);
        prop_assert_eq!(s.cache.total,0);
    }
}
