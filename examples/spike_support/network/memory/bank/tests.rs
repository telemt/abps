use super::*;
mod shared_tests;
fn plan(hard: usize) -> Plan {
    let mut config = mosaic::Config::new(
        vec![ClassConfig {
            capacity: 64,
            slots: 2,
        }],
        1,
        usize::MAX,
    );
    let bytes = mosaic::Reservation::for_config(&config)
        .unwrap()
        .total_bytes;
    config.root_limit = bytes;
    Plan {
        options: Options {
            mode: Mode::Fixed,
            priority: Priority::Weighted,
            groups: vec![0],
            ranks: [0; GROUPS],
            correlated: false,
            soft: Some(hard),
            elasticity: "cold".into(),
            chunk: 10,
            headroom: 25,
            trace: 2,
            horizon_ms: 20,
            pressure_ms: 0,
            eligible_demand: false,
            cached_accounting: false,
        },
        config,
        bytes,
        original_bytes: 4 * bytes,
        ceiling: 5 * bytes,
        initial: 4,
        standby: 0,
        hard: vec![hard],
        extra_external: 0,
    }
}
#[test]
fn ap13_hard_limit_spans_generations_and_shared_views() {
    let bank = Bank::prepare(plan(128)).unwrap();
    let h = Bank::open(&bank).unwrap();
    let mut a = h.acquire(1).unwrap();
    a.put_slice(&[0xA5]).unwrap();
    let a = a.freeze();
    let view = a.try_slice(0..1).unwrap();
    let b = h.acquire(64).unwrap();
    assert!(matches!(
        h.acquire(1),
        Err(Failure::Policy("LogicalHardLimit"))
    ));
    drop(a);
    assert_eq!(bank.borrow().admitted()[0], 128);
    drop(view);
    assert_eq!(bank.borrow().admitted()[0], 64);
    let c = h.acquire(64).unwrap();
    assert_eq!(c.filled_len(), 0);
    drop((b, c, h));
    assert!(bank.borrow_mut().shutdown());
    assert_eq!(bank.borrow().charged, 0);
}
#[test]
fn ap13_old_generation_pins_teardown_and_does_not_change_new_payload() {
    let bank = Bank::prepare(plan(1024)).unwrap();
    let h = Bank::open(&bank).unwrap();
    let old = h.acquire(64).unwrap().freeze();
    {
        let mut b = bank.borrow_mut();
        b.roots[0].phase = Phase::Draining;
        b.roots[0].owner.as_ref().unwrap().close().unwrap();
    }
    let mut new = h.acquire(64).unwrap();
    new.put_slice(&[0xBC]).unwrap();
    let start = bank.borrow().start;
    bank.borrow_mut()
        .maintenance_at(start + Duration::from_millis(100), 1);
    assert_eq!(bank.borrow().roots[0].phase, Phase::Draining);
    drop(old);
    bank.borrow_mut()
        .maintenance_at(start + Duration::from_millis(200), 2);
    assert_eq!(bank.borrow().roots[0].phase, Phase::Free);
    assert_eq!(new.as_ref(), &[0xBC]);
    drop((new, h));
    assert!(bank.borrow_mut().shutdown());
}
#[test]
fn ap13_prepare_errors_restore_only_confirmed_reservations() {
    for stage in 0..4 {
        let bank = Bank::prepare(plan(1024)).unwrap();
        let mut b = bank.borrow_mut();
        let before = b.charged;
        b.fault = Some((stage, false));
        assert!(b.create(Phase::Active).is_err());
        assert_eq!(b.charged, before);
        assert!(b.create(Phase::Active).is_ok());
        assert_eq!(b.charged, b.plan.ceiling);
        assert!(b.create(Phase::Active).is_err());
        assert_eq!(b.charged, b.plan.ceiling);
        assert!(b.shutdown());
    }
}
#[test]
fn ap13_prepare_unwind_retains_charge_until_explicit_cleanup() {
    for stage in 1..4 {
        let bank = Bank::prepare(plan(1024)).unwrap();
        let before = bank.borrow().charged;
        bank.borrow_mut().fault = Some((stage, true));
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| bank
                .borrow_mut()
                .create(Phase::Active)))
            .is_err()
        );
        assert!(bank.borrow().charged > before);
        assert!(bank.borrow_mut().shutdown());
    }
}
#[test]
fn ap13_trace_overflow_is_reported_and_budget_debt_is_preserved() {
    let bank = Bank::prepare(plan(1024)).unwrap();
    let h = Bank::open(&bank).unwrap();
    let held = h.acquire(64).unwrap();
    bank.borrow_mut().policy.total = 1;
    let start = bank.borrow().start;
    for i in 1..=4 {
        bank.borrow_mut()
            .maintenance_at(start + Duration::from_millis(i * 100), i);
    }
    assert_eq!(bank.borrow().admitted()[0], 64);
    assert!(matches!(
        h.acquire(1),
        Err(Failure::Policy("SoftStreamBudget"))
    ));
    assert_eq!(bank.borrow().trace_overflow, 2);
    drop((held, h));
    assert!(bank.borrow_mut().shutdown());
}
#[test]
fn ap13_growth_needs_physical_pressure_and_completion_progress() {
    let bank = Bank::prepare(plan(4096)).unwrap();
    let h = Bank::open(&bank).unwrap();
    let start = bank.borrow().start;
    for i in 1..=20 {
        let mut b = bank.borrow_mut();
        b.physical = 1;
        b.maintenance_at(start + Duration::from_millis(i * 100), i);
    }
    assert_eq!(bank.borrow().grown, 0); // completions alone aren't refunds
    for i in 21..=40 {
        let buffer = h.acquire(64).unwrap();
        drop(buffer);
        let mut b = bank.borrow_mut();
        b.physical = 1;
        b.maintenance_at(start + Duration::from_millis(i * 100), i);
    }
    assert_eq!(bank.borrow().grown, 1);
    assert_eq!(bank.borrow().peak, bank.borrow().plan.ceiling);
    drop(h);
    assert!(bank.borrow_mut().shutdown());
}
#[test]
fn ap13_trace_and_reservation_integral_use_transition_time() {
    let bank = Bank::prepare(plan(4096)).unwrap();
    let h = Bank::open(&bank).unwrap();
    drop(h.acquire(64).unwrap());
    let mut b = bank.borrow_mut();
    let base = b.start + Duration::from_secs(1);
    b.integrate(base);
    b.tick_at = base;
    b.deficit = Some(b.start);
    b.physical = 1;
    let before = b.area;
    let old = b.charged;
    b.maintenance_at(base + Duration::from_millis(100), 1);
    assert_eq!(b.grown, 1);
    assert_eq!(b.area, before + old as u128 * 100_000);
    assert_eq!(b.trace.last().unwrap().roots, 5);
    assert_eq!(b.trace.last().unwrap().reserved, b.plan.ceiling);
    let after = b.area;
    let charged = b.charged;
    b.maintenance_at(base + Duration::from_millis(200), 1);
    assert_eq!(b.area, after + charged as u128 * 100_000);
    drop(b);
    drop(h);
    assert!(bank.borrow_mut().shutdown());
}

#[test]
fn ap13_independent_rights_oracle_matches_long_reuse_trace() {
    let bank = Bank::prepare(plan(256)).unwrap();
    let h = Bank::open(&bank).unwrap();
    // Oracle records allocations and owners, not roots, atomics or bitmap words.
    let mut owners: Vec<(u64, mosaic::Buf)> = Vec::new();
    let mut next = 0u64;
    let rounds = if cfg!(miri) { 32 } else { 2000 };
    let mut seed = 0xABF50021u64;
    for _ in 0..rounds {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        match seed % 4 {
            0 | 1 => {
                if let Ok(mut b) = h.acquire(8) {
                    b.put_slice(&next.to_le_bytes()).unwrap();
                    owners.push((next, b.freeze()));
                    next += 1;
                }
            }
            2 if !owners.is_empty() => {
                let i = seed as usize % owners.len();
                if owners.len() < 32 {
                    owners.push((owners[i].0, owners[i].1.try_clone().unwrap()));
                }
            }
            3 if !owners.is_empty() => {
                drop(owners.swap_remove(seed as usize % owners.len()));
            }
            _ => {}
        }
        let unique: std::collections::BTreeSet<_> = owners.iter().map(|(id, _)| *id).collect();
        assert_eq!(bank.borrow().admitted()[0], unique.len() * 64);
        assert!(unique.len() <= 4);
        for (id, b) in &owners {
            assert_eq!(b.as_ref(), id.to_le_bytes());
        }
    }
    drop((owners, h));
    assert!(bank.borrow_mut().shutdown());
}

#[test]
fn ap13_abandoned_root_retains_reservation_after_late_return() {
    let bank = Bank::prepare(plan(1024)).unwrap();
    let h = Bank::open(&bank).unwrap();
    let old = h.acquire(64).unwrap();
    drop(h);
    assert!(!bank.borrow_mut().shutdown());
    let charged = bank.borrow().charged;
    assert_eq!(charged, bank.borrow().plan.bytes);
    drop(old);
    assert_eq!(bank.borrow().charged, charged);
    assert!(
        bank.borrow()
            .roots
            .iter()
            .any(|r| r.phase == Phase::Abandoned)
    );
}
#[test]
fn ap13_generation_exhaustion_and_attempt_limit_do_not_refund_or_wrap() {
    let bank = Bank::prepare(plan(1024)).unwrap();
    let h = Bank::open(&bank).unwrap();
    {
        let mut b = bank.borrow_mut();
        for r in b.roots.iter_mut() {
            if r.phase == Phase::Free {
                r.generation = u64::MAX;
            }
        }
        let before = b.charged;
        assert!(b.create(Phase::Active).is_err());
        assert_eq!(b.charged, before);
        assert_eq!(b.refused_records, 1);
        b.attempts = 64;
    }
    let start = bank.borrow().start;
    for i in 1..=20 {
        drop(h.acquire(64).unwrap());
        let mut b = bank.borrow_mut();
        b.physical = 1;
        b.maintenance_at(start + Duration::from_millis(i * 100), i);
    }
    assert_eq!(bank.borrow().attempts, 64);
    assert!(bank.borrow().refused_attempts > 0);
    drop(h);
    assert!(bank.borrow_mut().shutdown());
}

#[test]
fn ap13_remote_return_and_next_root_preserve_logical_hard_limit() {
    let bank = Bank::prepare(plan(128)).unwrap();
    let h = Bank::open(&bank).unwrap();
    let old = h.acquire(64).unwrap().freeze();
    let (release, wait) = std::sync::mpsc::sync_channel(0);
    let worker = std::thread::spawn(move || {
        wait.recv().unwrap();
        drop(old);
    });
    let new = h.acquire(64).unwrap();
    assert!(matches!(
        h.acquire(1),
        Err(Failure::Policy("LogicalHardLimit"))
    ));
    release.send(()).unwrap();
    worker.join().unwrap();
    let last = h.acquire(64).unwrap();
    assert_eq!(bank.borrow().admitted()[0], 128);
    drop((new, last, h));
    assert!(bank.borrow_mut().shutdown());
}

#[test]
fn ap13_single_root_catalog_wraps_without_unused_records() {
    let mut p = plan(1024);
    p.options.elasticity = "off".into();
    p.initial = 1;
    let bank = Bank::prepare(p).unwrap();
    assert_eq!(bank.borrow().roots.len(), 1);
    let h = Bank::open(&bank).unwrap();
    let start = bank.borrow().start;
    for i in 1..=20 {
        drop(h.acquire(64).unwrap());
        bank.borrow_mut()
            .maintenance_at(start + Duration::from_millis(i * 100), i);
    }
    assert_eq!(bank.borrow().admitted()[0], 0);
    drop(h);
    assert!(bank.borrow_mut().shutdown());
}
