use super::*;

#[test]
fn ap14_decision_reuse_checks_once_unless_refresh_invalidates_it() {
    for cached in [false, true] {
        let mut plan = shared_plan(1, 1, 64, 64);
        plan.options.cached_accounting = cached;
        let bank = Bank::prepare(plan).unwrap();
        let h = Bank::open(&bank).unwrap();
        let observations = || {
            let b = bank.borrow();
            let s = b.shared.as_ref().unwrap();
            (s.checks.get(), s.ledger_refreshes, s.cache_passes)
        };
        let before = observations();
        let held = h.acquire(64).unwrap();
        let after = observations();
        assert_eq!(after.0 - before.0, 1);
        assert_eq!(after.1 - before.1, u64::from(!cached));
        assert_eq!(after.2 - before.2, u64::from(cached));
        assert!(matches!(
            h.acquire(64),
            Err(Failure::Policy("LogicalHardLimit"))
        ));
        let rejected = observations();
        assert_eq!(rejected.0 - after.0, if cached { 2 } else { 1 });
        assert_eq!(rejected.1 - after.1, 1);
        assert_eq!(bank.borrow().admitted()[0], 64);
        drop(held);
        // The cached rejection is stale: its fresh decision must permit reuse.
        drop(h.acquire(64).unwrap());
        let refreshed = observations();
        assert_eq!(refreshed.0 - rejected.0, if cached { 2 } else { 1 });
        assert_eq!(refreshed.1 - rejected.1, 1);
        drop(h);
        assert!(matches!(
            bank.borrow_mut().acquire(0, 64),
            Err(Failure::Policy("ClosedStream"))
        ));
        let closed = observations();
        assert_eq!(closed.0 - refreshed.0, 1);
        assert_eq!(closed.1 - refreshed.1, u64::from(!cached));
        assert!(bank.borrow_mut().shutdown());
    }
}

#[test]
fn ap14_class_charge_matches_independent_first_fit_at_every_boundary() {
    for count in [1, 2, 8, 32, 63, 64] {
        let classes: Vec<_> = (1..=count)
            .map(|i| ClassConfig {
                capacity: i * i + 17,
                slots: 1,
            })
            .collect();
        let mut sizes = vec![0, 1, usize::MAX];
        for c in &classes {
            sizes.extend([c.capacity - 1, c.capacity, c.capacity + 1]);
        }
        for size in sizes {
            let oracle = classes
                .iter()
                .map(|c| c.capacity)
                .filter(|&c| c >= size)
                .min();
            assert_eq!(
                class_charge(&classes, size),
                oracle,
                "classes={count} size={size}"
            );
        }
    }
}
fn shared_plan(streams: usize, slots: usize, hard: usize, total: usize) -> Plan {
    let mut p = plan(hard);
    p.config = mosaic::Config::new(
        vec![ClassConfig {
            capacity: 64,
            slots,
        }],
        streams,
        usize::MAX,
    );
    p.bytes = mosaic::Reservation::for_config(&p.config)
        .unwrap()
        .total_bytes;
    p.config.root_limit = p.bytes;
    p.original_bytes = p.bytes;
    p.ceiling = p.bytes;
    p.initial = 1;
    p.hard = vec![hard; streams];
    p.options.mode = Mode::Shared;
    p.options.groups = (0..streams).map(|i| i % GROUPS).collect();
    p.options.soft = Some(total);
    p.options.pressure_ms = 10;
    p.options.eligible_demand = true;
    p.options.cached_accounting = true;
    p.options.elasticity = "off".into();
    p
}
#[test]
fn ap14_borrowing_uses_idle_opportunity_and_rejects_no_more_than_one_core_attempt() {
    let bank = Bank::prepare(shared_plan(2, 4, 256, 256)).unwrap();
    let h = Bank::open(&bank).unwrap();
    let other = Bank::open(&bank).unwrap();
    let targets = bank.borrow().policy.soft;
    let mut held = Vec::new();
    for _ in 0..4 {
        h.offer(64);
        held.push(h.acquire(64).unwrap());
    }
    assert!(matches!(
        other.acquire(64),
        Err(Failure::Policy("SoftTotalBudget"))
    ));
    assert_eq!(bank.borrow().accepted[0], 4);
    assert_eq!(bank.borrow().core_rejected[1], 0);
    assert_eq!(bank.borrow().policy.soft, targets);
    assert_eq!(bank.borrow().shared.as_ref().unwrap().loan_admissions, 2);
    drop(held);
    // Refresh sees the actual refunds; another group can use a whole slot.
    drop(other.acquire(64).unwrap());
    drop((h, other));
    assert!(bank.borrow_mut().shutdown());
}
#[test]
fn ap14_one_physical_slot_remains_usable_with_two_fractional_targets() {
    for cached in [false, true] {
        let mut p = shared_plan(2, 1, 64, 64);
        p.options.cached_accounting = cached;
        let bank = Bank::prepare(p).unwrap();
        let a = Bank::open(&bank).unwrap();
        let b = Bank::open(&bank).unwrap();
        for _ in 0..16 {
            let x = a.acquire(64).unwrap();
            assert!(matches!(
                b.acquire(64),
                Err(Failure::Policy("SoftTotalBudget"))
            ));
            drop(x);
            drop(b.acquire(64).unwrap());
        }
        drop((a, b));
        assert!(bank.borrow_mut().shutdown());
    }
}
#[test]
fn ap14_cache_rollback_before_and_after_core_grant_handles_error_and_unwind() {
    for stage in [0, 1] {
        for panic in [false, true] {
            let bank = Bank::prepare(shared_plan(1, 1, 64, 64)).unwrap();
            let h = Bank::open(&bank).unwrap();
            bank.borrow_mut().admission_fault = Some((stage, panic));
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| h.acquire(64)));
            assert!(if panic {
                result.is_err()
            } else {
                result.unwrap().is_err()
            });
            {
                let b = bank.borrow();
                let s = b.shared.as_ref().unwrap();
                assert_eq!(s.cache.total, 0);
                assert_eq!(s.loan_admissions, 0);
                assert_eq!(b.admitted()[0], 0);
                assert_eq!(
                    b.roots[0].pool.as_ref().unwrap().snapshot().classes[0]
                        .observed_available_slots,
                    1
                );
            }
            bank.borrow_mut().admission_fault = None;
            let mut next = h.acquire(64).unwrap();
            assert_eq!(next.filled_len(), 0);
            next.put_slice(&[0xA5; 64]).unwrap();
            assert_eq!(next.as_ref(), [0xA5; 64]);
            drop((next, h));
            assert!(bank.borrow_mut().shutdown());
        }
    }
}
#[test]
fn ap14_remote_last_owner_refunds_before_stale_cache_can_admit_again() {
    let bank = Bank::prepare(shared_plan(1, 1, 64, 64)).unwrap();
    let h = Bank::open(&bank).unwrap();
    let mut b = h.acquire(64).unwrap();
    b.put_slice(&[7; 64]).unwrap();
    let b = b.freeze();
    let view = b.try_slice(0..1).unwrap();
    drop(b);
    assert!(matches!(
        h.acquire(64),
        Err(Failure::Policy("LogicalHardLimit"))
    ));
    std::thread::spawn(move || {
        assert_eq!(view.as_ref(), [7]);
        drop(view);
    })
    .join()
    .unwrap();
    assert_eq!(bank.borrow().shared.as_ref().unwrap().cache.total, 64);
    let next = h.acquire(64).unwrap();
    assert!(next.as_ref().is_empty());
    drop((next, h));
    assert!(bank.borrow_mut().shutdown());
}
#[test]
fn ap14_no_receivers_do_not_create_eligible_demand_or_erase_offered_counts() {
    let bank = Bank::prepare(shared_plan(1, 2, 128, 128)).unwrap();
    let h = Bank::open(&bank).unwrap();
    for _ in 0..100 {
        h.offer(64);
    }
    let now = bank.borrow().start + Duration::from_millis(100);
    bank.borrow_mut().maintenance_at(now, 0);
    {
        let b = bank.borrow();
        let s = b.shared.as_ref().unwrap();
        assert_eq!(s.offered[0], 6400);
        assert_eq!(s.eligible[0], 0);
        assert_eq!(b.policy.soft[0], 0);
    }
    drop(h.acquire(64).unwrap()); // Activation does not wait for the next tick.
    drop(h);
    assert!(bank.borrow_mut().shutdown());
}
#[test]
fn ap14_class_failure_does_not_block_unrelated_class_or_consume_cache_charge() {
    let mut p = shared_plan(1, 1, 1024, 1024);
    p.config.classes.push(ClassConfig {
        capacity: 512,
        slots: 1,
    });
    p.config.root_limit = usize::MAX;
    p.bytes = mosaic::Reservation::for_config(&p.config)
        .unwrap()
        .total_bytes;
    p.config.root_limit = p.bytes;
    p.ceiling = p.bytes;
    p.original_bytes = p.bytes;
    let bank = Bank::prepare(p).unwrap();
    let h = Bank::open(&bank).unwrap();
    let a = h.acquire(64).unwrap();
    assert!(matches!(
        h.acquire(64),
        Err(Failure::Core(Error::ClassDepleted))
    ));
    {
        let mut b = bank.borrow_mut();
        assert_eq!(b.shared.as_ref().unwrap().cache.total, 64);
        b.policy.soft[0] = 64;
        b.policy.group_soft[0] = 64;
    }
    assert!(matches!(
        h.acquire(64),
        Err(Failure::Policy("SoftDebtPressure"))
    ));
    let b = h.acquire(512).unwrap();
    drop((a, b, h));
    assert!(bank.borrow_mut().shutdown());
}

#[test]
fn ap14_full_and_cached_traces_preserve_outcomes_and_accounting() {
    let mut full = shared_plan(3, 4, 192, 256);
    full.options.cached_accounting = false;
    let cached = shared_plan(3, 4, 192, 256);
    let banks = [Bank::prepare(full).unwrap(), Bank::prepare(cached).unwrap()];
    let handles: Vec<_> = banks
        .iter()
        .map(|b| (0..3).map(|_| Bank::open(b).unwrap()).collect::<Vec<_>>())
        .collect();
    let mut owned: [Vec<Option<mosaic::BufMut>>; 2] =
        std::array::from_fn(|_| (0..8).map(|_| None).collect());
    let mut rights = [None; 8]; // Independent owner -> stream ledger.
    let mut seed = 0xAB140021u32;
    for step in 0..if cfg!(miri) { 64 } else { 2048 } {
        seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
        let index = (seed >> 16) as usize % 8;
        if seed & 3 == 0 {
            for owned in &mut owned {
                drop(owned[index].take());
            }
            rights[index] = None;
        } else if rights[index].is_none() {
            let stream = (seed >> 8) as usize % 3;
            let mut outcomes = Vec::new();
            for (i, h) in handles.iter().enumerate() {
                h[stream].offer(64);
                match h[stream].acquire(64) {
                    Ok(mut b) => {
                        b.put_slice(&seed.to_le_bytes()).unwrap();
                        outcomes.push("ok".to_string());
                        owned[i][index] = Some(b);
                    }
                    Err(e) => outcomes.push(format!("{e:?}")),
                }
            }
            assert_eq!(outcomes[0], outcomes[1], "step {step}");
            if outcomes[0] == "ok" {
                rights[index] = Some(stream);
            }
        }
        let mut expected = [0; STREAMS];
        for &stream in rights.iter().flatten() {
            expected[stream] += 64;
        }
        assert!(expected.iter().sum::<usize>() <= 256);
        assert!(expected[..3].iter().all(|&n| n <= 192));
        for b in &banks {
            assert_eq!(b.borrow().admitted(), expected);
            let now = b.borrow().start + Duration::from_millis(step * 10);
            b.borrow_mut().maintenance_at(now, 0);
        }
    }
    drop(owned);
    drop(handles);
    for b in banks {
        assert!(b.borrow_mut().shutdown());
    }
}
