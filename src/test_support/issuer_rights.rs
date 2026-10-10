// Included only in raw unit tests. All transitions below execute real storage.

#[test]
fn abps_batch_boundaries_conserve_handles_pending_and_source() {
    let mut owner = StorageOwner::prepare(&config(1, 1)).unwrap();
    let pool = owner.activate().unwrap();
    let (authority, domain) = pool.try_open_domain(16).unwrap();
    let mut source = domain.try_acquire(8).unwrap();
    source.put_slice(b"01234567").unwrap();
    let source = source.freeze().try_slice(1..5).unwrap();
    for count in 0..=3 {
        for prefix in 0..=count {
            for unwind in [false, true] {
                let mut output: Vec<Option<Buf>> = (0..count).map(|_| None).collect();
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let mut build = BatchBuild::new(&source, &mut output).unwrap();
                    for built in 0..=prefix {
                        // SI-10: observe only complete transitions, with no
                        // concurrent releases that could make this a torn sample.
                        assert_eq!(build.built, built);
                        assert_eq!(build.references.pending(), count - built);
                        let live = 1 + build.dst.iter().filter(|view| view.is_some()).count();
                        assert_eq!(
                            source.lease.slot().references.load(),
                            live + build.references.pending()
                        );
                        assert!(build.dst[built..].iter().all(Option::is_none));
                        for view in build.dst[..built].iter().flatten() {
                            assert_eq!(view.as_ref(), b"1234");
                            assert_eq!(view.origin(), source.origin());
                        }
                        assert_eq!(pool.snapshot().classes[0].observed_available_slots, 0);
                        if built < prefix {
                            build.materialize();
                        }
                    }
                    if unwind {
                        panic!("SI-10 unwind at complete boundary {prefix}/{count}");
                    }
                    // Partial normal destruction is rollback too. Only the
                    // complete prefix can transfer ownership to the caller.
                    build.complete = prefix == count;
                }));
                assert_eq!(result.is_err(), unwind);
                let kept = if !unwind && prefix == count { count } else { 0 };
                assert_eq!(output.iter().filter(|view| view.is_some()).count(), kept);
                assert_eq!(source.lease.slot().references.load(), 1 + kept);
                assert_eq!(domain.metrics().admitted_bytes, 16);
                assert_eq!(source.as_ref(), b"1234");
                drop(output);
                assert_eq!(source.lease.slot().references.load(), 1);
            }
        }
    }
    drop(source);
    assert_eq!(domain.metrics().admitted_bytes, 0);
    assert_eq!(pool.snapshot().classes[0].observed_available_slots, 1);
    drop((authority, domain, pool));
    owner.try_destroy().unwrap();
}

#[test]
fn abps_generation_retirement_waits_for_closed_domain_returns() {
    for reserved in [false, true] {
        let mut c = config(2, 1);
        if reserved {
            c.capacity_policy = CapacityPolicy::Reserved;
        }
        let mut owner = StorageOwner::prepare(&c).unwrap();
        let pool = owner.activate().unwrap();
        let record = pool.root.domain(0);
        let mut state = record.state.get();
        state.generation = u64::MAX - 1;
        record.state.set(state);
        let open = || {
            if reserved {
                pool.try_open_reserved_domain(
                    32,
                    &[ClassReservation {
                        capacity: 16,
                        slots: 2,
                    }],
                )
            } else {
                pool.try_open_domain(32)
            }
        };
        let (authority, domain) = open().unwrap();
        assert_eq!(domain.identity().generation, u64::MAX);
        let first = domain.try_acquire(1).unwrap().freeze();
        let last = domain.try_acquire(1).unwrap().freeze();
        authority.close();
        let lifetime = control_lifetime(&pool);
        for _ in 0..3 {
            assert_eq!(domain.try_acquire(1).err(), Some(Error::Closed));
            assert_eq!(domain.metrics().admitted_bytes, 32);
            assert_eq!(control_lifetime(&pool), lifetime);
        }
        drop((authority, domain));
        for (frame, remaining) in [(first, 16), (last, 0)] {
            assert_eq!(record.state.get().phase, DomainPhase::Closing);
            assert_eq!(pool.poll_domains(1).reclaimed, 0);
            assert_eq!(pool.snapshot().retired_domain_records, 0);
            std::thread::spawn(move || drop(frame)).join().unwrap();
            assert_eq!(record.ledger.admitted.load(Ordering::Acquire), remaining);
        }
        assert_eq!(pool.poll_domains(0).reclaimed, 0);
        assert_eq!(record.state.get().phase, DomainPhase::Closing);
        assert_eq!(pool.poll_domains(1).reclaimed, 1);
        assert_eq!(record.state.get().phase, DomainPhase::Retired);
        for _ in 0..3 {
            assert_eq!(open().err(), Some(Error::DomainCapacityExhausted));
            assert_eq!(pool.poll_domains(1).reclaimed, 0);
            assert_eq!(pool.snapshot().retired_domain_records, 1);
        }
        drop(pool);
        owner.try_destroy().unwrap();
    }
}

#[test]
fn abps_fault_close_remote_return_reopen_preserves_neighbor() {
    // MEM-02, OWN-01/03, LIFE-02, SI-05: rollback and shutdown compose; neither
    // transfers the still-live allocation's charge/grant to the next generation.
    for reserved in [false, true] {
        for &stage in stages() {
            for unwind in [false, true] {
                let mut c = Config::new(
                    vec![crate::ClassConfig {
                        capacity: 16,
                        slots: 3,
                    }],
                    2,
                    1 << 20,
                );
                if reserved {
                    c.capacity_policy = CapacityPolicy::Reserved;
                }
                let mut storage = StorageOwner::prepare(&c).unwrap();
                let pool = storage.activate().unwrap();
                let open = |slots| {
                    if reserved {
                        pool.try_open_reserved_domain(
                            16 * slots,
                            &[ClassReservation {
                                capacity: 16,
                                slots,
                            }],
                        )
                    } else {
                        pool.try_open_domain(16 * slots)
                    }
                };
                let (a, d) = open(2).unwrap();
                let (b, e) = open(1).unwrap();
                let old_identity = d.identity();
                let mut old = d.try_acquire(1).unwrap();
                old.put_slice(&[0xa5]).unwrap();
                let old = old.freeze();
                let mut neighbor = e.try_acquire(1).unwrap();
                neighbor.put_slice(&[0xa7]).unwrap();
                let neighbor = neighbor.freeze();
                let before = control_lifetime(&pool);
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    d.acquire(
                        1,
                        Some(if unwind {
                            Fault::Panic(stage)
                        } else {
                            Fault::Error(stage)
                        }),
                    )
                }));
                if unwind {
                    assert!(outcome.is_err());
                } else {
                    assert_eq!(outcome.unwrap().err(), Some(Error::Contended));
                }
                assert_eq!(control_lifetime(&pool), before);
                assert_eq!(d.metrics().admitted_bytes, 16);
                a.close();
                assert_eq!(d.try_acquire(1).err(), Some(Error::Closed));
                drop((a, d));
                assert_eq!(pool.poll_domains(1).reclaimed, 0);
                // SI-09 checks pinned grant availability before leasing a record.
                assert_eq!(
                    open(2).err(),
                    Some(if reserved {
                        Error::ReservationUnavailable
                    } else {
                        Error::DomainMaintenanceRequired
                    })
                );
                std::thread::spawn(move || {
                    assert_eq!(old.as_ref(), &[0xa5]);
                    drop(old);
                })
                .join()
                .unwrap();
                assert_eq!(pool.poll_domains(1).reclaimed, 1);
                let (a, d) = open(2).unwrap();
                assert_ne!(d.identity(), old_identity);
                let mut fresh = d.try_acquire(1).unwrap();
                assert!(fresh.as_ref().is_empty());
                assert_eq!((fresh.filled_len(), fresh.initialized_len()), (0, 0));
                assert_eq!(fresh.initialize_to(1).unwrap(), &[0]);
                fresh.set_filled_len(1).unwrap();
                assert_eq!(fresh.origin(), d.identity());
                assert_eq!(neighbor.as_ref(), &[0xa7]);
                assert_eq!(e.metrics().admitted_bytes, 16);
                drop((fresh, neighbor, a, d, b, e, pool));
                storage.try_destroy().unwrap();
            }
        }
    }
}

#[test]
fn abps_all_class_faults_preserve_neighbors_and_rights() {
    for reserved in [false, true] {
        let capacities = [8, 32];
        let mut c = Config::new(
            capacities
                .map(|capacity| crate::ClassConfig { capacity, slots: 2 })
                .to_vec(),
            2,
            1 << 20,
        );
        if reserved {
            c.capacity_policy = CapacityPolicy::Reserved;
        }
        let mut owner = StorageOwner::prepare(&c).unwrap();
        let pool = owner.activate().unwrap();
        let open = || {
            if reserved {
                pool.try_open_reserved_domain(
                    80,
                    &capacities.map(|capacity| ClassReservation { capacity, slots: 1 }),
                )
            } else {
                pool.try_open_domain(80)
            }
        };
        let (a, d) = open().unwrap();
        let (b, e) = open().unwrap();
        let neighbors: Vec<_> = capacities
            .iter()
            .map(|&n| {
                let mut b = e.try_acquire(n).unwrap();
                b.put_slice(&vec![0xa7; n]).unwrap();
                b.freeze()
            })
            .collect();
        let reservation = owner.reservation().unwrap();
        let lifetime = control_lifetime(&pool);
        for capacity in capacities {
            for &stage in stages() {
                for unwind in [false, true] {
                    let mut stale = d.try_acquire(capacity).unwrap();
                    stale.put_slice(&vec![0xa5; capacity]).unwrap();
                    drop(stale.freeze());
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        d.acquire(
                            capacity,
                            Some(if unwind {
                                Fault::Panic(stage)
                            } else {
                                Fault::Error(stage)
                            }),
                        )
                    }));
                    if unwind {
                        assert!(result.is_err());
                    } else {
                        assert_eq!(result.unwrap().err(), Some(Error::Contended));
                    }
                    assert_eq!(d.metrics().admitted_bytes, 0, "MEM-02 rollback charge");
                    assert_eq!(e.metrics().admitted_bytes, 40, "MEM-02 neighbor charge");
                    assert_eq!(control_lifetime(&pool), lifetime, "LIFE-02 rollback claims");
                    assert_eq!(owner.reservation().unwrap(), reservation);
                    for (b, n) in neighbors.iter().zip(capacities) {
                        assert_eq!(b.as_ref(), vec![0xa7; n]);
                    }
                    for class in pool.snapshot().classes {
                        assert_eq!(class.observed_available_slots, 1);
                    }
                    let mut next = d.try_acquire(capacity).unwrap();
                    assert_eq!(next.origin(), d.identity());
                    assert_eq!((next.filled_len(), next.initialized_len()), (0, 0));
                    assert!(next.as_ref().is_empty());
                    assert_eq!(next.set_filled_len(1), Err(Error::NotInitialized));
                    assert_eq!(next.initialize_to(1).unwrap(), &[0]);
                    next.set_filled_len(1).unwrap();
                    let frozen = next.freeze();
                    assert_eq!(frozen.try_slice(1..2).err(), Some(Error::InvalidRange));
                    assert_eq!(frozen.as_ref(), &[0]);
                    drop(frozen);
                }
            }
        }
        drop((neighbors, a, d, b, e, pool));
        owner.try_destroy().unwrap();
    }
}

#[test]
fn abps_failure_idempotence_and_exact_budget_boundary() {
    let mut owner = StorageOwner::prepare(&config(2, 1)).unwrap();
    let pool = owner.activate().unwrap();
    let (a, d) = pool.try_open_domain(16).unwrap();
    for _ in 0..if cfg!(miri) { 8 } else { 1000 } {
        let b = d.try_acquire(1).unwrap();
        let lifetime = control_lifetime(&pool);
        assert_eq!(d.try_acquire(1).err(), Some(Error::BudgetExceeded));
        assert_eq!(d.metrics().admitted_bytes, 16);
        assert_eq!(control_lifetime(&pool), lifetime);
        drop(b);
        assert_eq!(d.metrics().admitted_bytes, 0);
        drop(d.try_acquire(1).unwrap());
    }
    drop((a, d, pool));
    owner.try_destroy().unwrap();
}

#[test]
fn abps_publish_refund_budget_and_single_record_reincarnation() {
    for limit in [16, 32] {
        let mut owner = StorageOwner::prepare(&config(1, 1)).unwrap();
        let pool = owner.activate().unwrap();
        let mut previous = None;
        for _ in 0..if cfg!(miri) { 2 } else { 8 } {
            let (a, d) = pool.try_open_domain(limit).unwrap();
            let identity = d.identity();
            if let Some(old) = previous {
                assert_ne!(identity, old, "LIFE-02 generation increment");
            }
            previous = Some(identity);
            let old = std::mem::ManuallyDrop::new(d.try_acquire(1).unwrap().freeze());
            let pending = old.lease.begin_return().unwrap().publish();
            assert_eq!(pool.snapshot().classes[0].observed_available_slots, 1);
            let next = if limit == 16 {
                assert_eq!(d.try_acquire(1).err(), Some(Error::BudgetExceeded));
                None
            } else {
                Some(d.try_acquire(1).unwrap())
            };
            assert_eq!(d.metrics().admitted_bytes, limit);
            a.close();
            assert_eq!(d.try_acquire(1).err(), Some(Error::Closed));
            drop((a, d));
            assert_eq!(pool.poll_domains(0).examined, 0);
            assert_eq!(
                pool.poll_domains(1).reclaimed,
                0,
                "LIFE-02 published is not drained"
            );
            assert_eq!(
                pool.try_open_domain(16).err(),
                Some(Error::DomainMaintenanceRequired)
            );
            pending.finish();
            if next.is_some() {
                assert_eq!(
                    pool.poll_domains(1).reclaimed,
                    0,
                    "new allocation pins the same origin"
                );
            }
            drop(next);
            assert_eq!(pool.poll_domains(1).reclaimed, 1);
            assert_eq!(pool.poll_domains(1).pending, 0);
        }
        drop(pool);
        owner.try_destroy().unwrap();
    }
}

#[test]
fn abps_reference_limit_scalar_batch_and_zero_length_views() {
    for limit in [1, 2, 3] {
        let mut c = config(1, 1);
        c.reference_limit = limit;
        let mut owner = StorageOwner::prepare(&c).unwrap();
        let pool = owner.activate().unwrap();
        let (a, d) = pool.try_open_domain(32).unwrap();
        let mut b = d.try_acquire(4).unwrap();
        b.put_slice(b"abcd").unwrap();
        let b = b.freeze();
        let mut batch: Vec<_> = (0..limit - 1).map(|_| None).collect();
        b.try_clone_into(&mut batch).unwrap();
        assert_eq!(b.try_clone().err(), Some(Error::ReferenceLimit));
        assert_eq!(b.try_slice(0..0).err(), Some(Error::ReferenceLimit));
        let mut extra = [None];
        assert_eq!(b.try_clone_into(&mut extra), Err(Error::ReferenceLimit));
        assert!(extra[0].is_none());
        assert_eq!(pool.snapshot().slots[0].references, limit);
        drop(batch);
        assert_eq!(pool.snapshot().slots[0].references, 1);
        assert_eq!(b.as_ref(), b"abcd");
        drop(b);
        assert_eq!(d.metrics().admitted_bytes, 0);
        drop((a, d, pool));
        owner.try_destroy().unwrap();
    }
}


#[test]
fn abps_all_records_close_remote_permutations_and_one_record_poll() {
    // LIFE-01/02, SI-05/13: configured domain/slot capacity, one FIFO examination
    // per call, and every order of complete remote returns in this finite fixture.
    fn orders(prefix: Vec<usize>, rest: Vec<usize>, out: &mut Vec<Vec<usize>>) {
        if rest.is_empty() {
            out.push(prefix);
            return;
        }
        for i in 0..rest.len() {
            let mut next = prefix.clone();
            next.push(rest[i]);
            let mut remaining = rest.clone();
            remaining.remove(i);
            orders(next, remaining, out);
        }
    }
    let count = if cfg!(miri) { 3 } else { 4 };
    let mut permutations = Vec::new();
    orders(Vec::new(), (0..count).collect(), &mut permutations);
    for reserved in [false, true] {
        for order in &permutations {
            let mut c = config(count, count);
            if reserved {
                c.capacity_policy = CapacityPolicy::Reserved;
            }
            let mut owner = StorageOwner::prepare(&c).unwrap();
            let pool = owner.activate().unwrap();
            let open = || {
                if reserved {
                    pool.try_open_reserved_domain(
                        16,
                        &[ClassReservation {
                            capacity: 16,
                            slots: 1,
                        }],
                    )
                } else {
                    pool.try_open_domain(16)
                }
            };
            let mut keys = Vec::new();
            let mut buffers = Vec::new();
            for i in 0..count {
                let (a, d) = open().unwrap();
                keys.push(d.identity());
                let mut b = d.try_acquire(1).unwrap();
                b.put_slice(&[i as u8]).unwrap();
                buffers.push(Some(b.freeze()));
                a.close();
                drop((a, d));
            }
            assert_eq!(pool.snapshot().pending_domain_records, count);
            assert_eq!(pool.snapshot().classes[0].observed_available_slots, 0);
            assert_eq!(
                open().err(),
                Some(if reserved {
                    Error::ReservationUnavailable
                } else {
                    Error::DomainMaintenanceRequired
                })
            );
            let mut pending = count;
            for &i in order {
                std::thread::spawn({
                    let b = buffers[i].take().unwrap();
                    move || {
                        assert_eq!(b.as_ref(), &[i as u8]);
                        drop(b);
                    }
                })
                .join()
                .unwrap();
                let mut reclaimed = 0;
                for _ in 0..pending {
                    let sample = pool.poll_domains(1);
                    assert_eq!(sample.examined, 1);
                    assert!(sample.reclaimed <= 1);
                    reclaimed += sample.reclaimed;
                    if reclaimed != 0 {
                        break;
                    }
                }
                assert_eq!(reclaimed, 1, "LIFE-02 a ready FIFO record must be reached");
                pending -= 1;
                assert_eq!(pool.snapshot().pending_domain_records, pending);
                for (i, retained) in buffers.iter().enumerate() {
                    if let Some(retained) = retained {
                        assert_eq!(retained.as_ref(), &[i as u8]);
                    }
                }
            }
            assert_eq!(pool.poll_domains(1).examined, 0);
            let mut reopened = Vec::new();
            for _ in 0..count {
                let pair = open().unwrap();
                let key = pair.1.identity();
                let previous = keys.iter().find(|old| old.index == key.index).unwrap();
                assert!(key.generation > previous.generation);
                let b = pair.1.try_acquire(1).unwrap();
                assert!(b.as_ref().is_empty());
                reopened.push((pair, b));
            }
            drop((reopened, pool));
            owner.try_destroy().unwrap();
        }
    }
}
