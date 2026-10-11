use super::*;
use std::sync::Barrier;

fn configured(slots: usize, domains: usize) -> (StorageOwner, Pool) {
    let owner = StorageOwner::prepare(Config::new(
        vec![ClassConfig {
            capacity: 64,
            slots,
        }],
        domains,
        1 << 20,
    ))
    .unwrap();
    let pool = owner.activate().unwrap();
    (owner, pool)
}

fn error<T>(result: Result<T, Error>) -> Error {
    result.err().expect("expected failure")
}

#[test]
fn life02_root_identity_is_assigned_once_not_an_address() {
    let (mut first, _) = configured(1, 1);
    let second = StorageOwner::prepare(Config::new(
        vec![ClassConfig {
            capacity: 64,
            slots: 1,
        }],
        1,
        1 << 20,
    ))
    .unwrap();
    let a = first.root().unwrap().identity;
    let b = second.root().unwrap().identity;
    assert!(a != 0 && b > a);
    assert_ne!(a, Arc::as_ptr(first.root().unwrap()) as usize);
    first.try_destroy().unwrap();
}

#[test]
fn io01_platform_descriptor_limit_is_an_explicit_error() {
    // Windows IoSlice lengths are 32-bit and std panics above them; the limit
    // is checked on lengths alone, without a 4 GiB allocation.
    assert!(io_slice_fits(0) && io_slice_fits(u32::MAX as usize));
    #[cfg(target_pointer_width = "64")]
    assert_eq!(io_slice_fits(u32::MAX as usize + 1), !cfg!(windows));
    let (mut owner, pool) = configured(1, 1);
    let (authority, domain) = pool.try_open_domain(64).unwrap();
    let mut buffer = domain.try_acquire(5).unwrap();
    buffer.put_slice(b"frame").unwrap();
    let frame = buffer.freeze();
    let view = frame.try_slice(1..4).unwrap();
    assert_eq!(frame.try_as_io_slice().unwrap().as_ref(), b"frame");
    assert_eq!(
        view.try_as_io_slice().unwrap().as_ref(),
        view.as_io_slice().as_ref()
    );
    drop((view, frame, domain, authority, pool));
    owner.try_destroy().unwrap();
}

#[test]
fn mem02_prog01_failure_at_each_admission_stage_restores_all_ledgers() {
    let (mut owner, pool) = configured(2, 1);
    let (authority, domain) = pool.try_open_domain(128).unwrap();
    for stage in [Stage::Charged, Stage::Credit, Stage::Slot] {
        let before = pool.snapshot();
        assert_eq!(
            error(domain.acquire_inner(1, Some(stage))),
            Error::Contended
        );
        assert_eq!(pool.snapshot(), before);
    }
    // Inspect the actual RAII transaction's pending states, then unwind it.
    {
        let mut state = lock(&pool.root.state);
        let mut attempt = Admission::new(&mut state, domain.key, 0, 64).unwrap();
        assert_eq!(attempt.state.domains[0].pending_acquire, 64);
        assert_eq!(attempt.state.domains[0].admitted(), 64);
        attempt.reserve_credit().unwrap();
        assert_eq!(attempt.state.classes[0].acquiring, 1);
        assert_eq!(attempt.state.classes[0].free.len(), 1);
        attempt.claim_slot();
        assert_eq!(
            attempt
                .state
                .slots
                .iter()
                .filter(|s| s.payload.is_none())
                .count(),
            1
        );
    }
    assert_eq!(pool.snapshot().classes[0].available, 2);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut state = lock(&pool.root.state);
        let mut attempt = Admission::new(&mut state, domain.key, 0, 64).unwrap();
        attempt.reserve_credit().unwrap();
        attempt.claim_slot();
        panic!("test rollback during unwind");
    }));
    assert!(result.is_err());
    let buffer = domain.try_acquire(64).unwrap();
    drop((buffer, domain, authority, pool));
    owner.try_destroy().unwrap();
}

#[test]
fn mem03_own02_views_retain_full_origin_charge_until_actual_last_arc() {
    let (mut owner, pool) = configured(1, 1);
    let (authority, domain) = pool.try_open_domain(64).unwrap();
    let mut mutable = domain.try_acquire(1).unwrap();
    mutable.put_slice(b"abcdef").unwrap();
    let original_address = mutable.as_ref().as_ptr();
    let buffer = mutable.freeze();
    assert_eq!(
        buffer.as_ref().as_ptr(),
        original_address,
        "freeze must move the same Box"
    );
    let nested = buffer.try_slice(1..5).unwrap().try_slice(1..2).unwrap();
    let empty = buffer.try_slice(0..0).unwrap();
    assert_eq!(nested.as_ref(), b"c");
    drop((buffer, nested));
    let snapshot = pool.snapshot();
    assert_eq!(snapshot.domains[0].live_backing_bytes, 64);
    assert_eq!(snapshot.slots[0].references, 1);
    assert_eq!(error(domain.try_acquire(1)), Error::BudgetExceeded);
    drop(empty);
    assert_eq!(pool.snapshot().classes[0].available, 1);
    let next = domain.try_acquire(64).unwrap();
    assert_eq!(
        next.as_ref().as_ptr(),
        original_address,
        "reuse the prepared payload"
    );
    assert!(next.as_ref().is_empty());
    drop((next, domain, authority, pool));
    owner.try_destroy().unwrap();
}

#[test]
fn own01_safe_initialization_and_checked_ranges() {
    let (mut owner, pool) = configured(1, 1);
    let (authority, domain) = pool.try_open_domain(64).unwrap();
    let mut buffer = domain.try_acquire(1).unwrap();
    buffer.put_slice(&[0xff; 64]).unwrap();
    drop(buffer);
    let mut buffer = domain.try_acquire(1).unwrap();
    assert_eq!(buffer.initialized_len(), 0);
    assert_eq!(buffer.set_filled_len(1), Err(Error::NotInitialized));
    assert_eq!(buffer.initialize_to(4).unwrap(), &[0; 4]);
    buffer.initialize_to(4).unwrap().copy_from_slice(b"data");
    buffer.set_filled_len(4).unwrap();
    assert_eq!(buffer.as_ref(), b"data");
    assert_eq!(error(buffer.initialize_to(65)), Error::InvalidRange);
    let buffer = buffer.freeze();
    for range in [5..5, 0..usize::MAX, Range { start: 3, end: 2 }] {
        assert_eq!(error(buffer.try_slice(range)), Error::InvalidRange);
    }
    drop(buffer);
    let empty = domain.try_acquire(1).unwrap().freeze();
    assert!(empty.is_empty());
    assert_eq!(pool.snapshot().domains[0].admitted_bytes, 64);
    drop((empty, domain, authority, pool));
    owner.try_destroy().unwrap();
}

#[test]
fn life01_domain_owner_closes_and_all_old_rights_fence_record_reuse() {
    let (mut owner, pool) = configured(2, 1);
    let (authority, domain) = pool.try_open_domain(128).unwrap();
    let old_identity = domain.identity();
    let buffer = domain.try_acquire(1).unwrap().freeze();
    let handle = domain.try_clone().unwrap();
    drop(authority);
    assert_eq!(error(domain.try_acquire(1)), Error::Closed);
    assert_eq!(
        error(pool.try_open_domain(64)),
        Error::DomainCapacityExhausted
    );
    let view_after_close = buffer.try_clone().unwrap();
    drop((buffer, domain, handle));
    assert_eq!(
        error(pool.try_open_domain(64)),
        Error::DomainCapacityExhausted
    );
    drop(view_after_close);
    let (authority, domain) = pool.try_open_domain(64).unwrap();
    assert_ne!(domain.identity(), old_identity);
    let live = domain.try_acquire(1).unwrap();
    assert_eq!(pool.snapshot().domains[0].admitted_bytes, 64);
    drop(live);
    // admitted==0 is insufficient: even a closed ordinary handle pins identity.
    drop(authority);
    assert_eq!(
        error(pool.try_open_domain(0)),
        Error::DomainCapacityExhausted
    );
    drop(domain);
    let (authority, domain) = pool.try_open_domain(0).unwrap();
    assert_eq!(error(domain.try_acquire(1)), Error::BudgetExceeded);
    drop((authority, domain, pool));
    owner.try_destroy().unwrap();
}

#[test]
fn life01_generation_exhaustion_retires_instead_of_wrapping() {
    let (mut owner, pool) = configured(1, 1);
    lock(&pool.root.state).domains[0].generation = u64::MAX - 1;
    let (authority, domain) = pool.try_open_domain(64).unwrap();
    assert_eq!(domain.key.generation, u64::MAX);
    drop((authority, domain));
    let snapshot = pool.snapshot();
    assert_eq!(snapshot.free_domain_records, 0);
    assert_eq!(snapshot.retired_domain_records, 1);
    assert_eq!(
        error(pool.try_open_domain(64)),
        Error::DomainCapacityExhausted
    );
    drop(pool);
    owner.try_destroy().unwrap();
}

#[test]
fn prog02_size_classes_budget_and_contention_are_distinct() {
    let mut owner = StorageOwner::prepare(Config::new(
        vec![
            ClassConfig {
                capacity: 8,
                slots: 1,
            },
            ClassConfig {
                capacity: 64,
                slots: 1,
            },
        ],
        2,
        1 << 20,
    ))
    .unwrap();
    let pool = owner.activate().unwrap();
    let (a_owner, a) = pool.try_open_domain(8).unwrap();
    let (b_owner, b) = pool.try_open_domain(128).unwrap();
    assert_eq!(error(a.try_acquire(0)), Error::InvalidSize);
    for size in [65, usize::MAX] {
        assert_eq!(error(a.try_acquire(size)), Error::TooLarge);
    }
    let first = a.try_acquire(8).unwrap();
    assert_eq!(error(a.try_acquire(1)), Error::BudgetExceeded);
    assert_eq!(error(b.try_acquire(1)), Error::ClassDepleted);
    assert_eq!(
        pool.snapshot()
            .domains
            .iter()
            .find(|d| d.identity == b.key)
            .unwrap()
            .admitted_bytes,
        0
    );
    let large = b.try_acquire(9).unwrap();
    assert_eq!(large.capacity(), 64);
    let lock_guard = lock(&pool.root.state);
    assert_eq!(error(b.try_acquire(1)), Error::Contended);
    assert_eq!(error(b.try_clone()), Error::Contended);
    drop(lock_guard);
    drop((first, large, a, a_owner, b, b_owner, pool));
    owner.try_destroy().unwrap();
}

#[test]
fn own02_checked_reference_and_root_claim_limits_leave_state_unchanged() {
    let mut config = Config::new(
        vec![ClassConfig {
            capacity: 64,
            slots: 2,
        }],
        1,
        1 << 20,
    );
    config.reference_limit = 2;
    config.handle_limit = 5; // owner, pool, authority, domain, one allocation
    let mut owner = StorageOwner::prepare(config).unwrap();
    let pool = owner.activate().unwrap();
    let (authority, domain) = pool.try_open_domain(128).unwrap();
    let buffer = domain.try_acquire(1).unwrap().freeze();
    assert_eq!(error(domain.try_acquire(1)), Error::ReferenceLimit);
    assert_eq!(error(domain.try_clone()), Error::ReferenceLimit);
    let view = buffer.try_clone().unwrap();
    let before = pool.snapshot();
    assert_eq!(error(buffer.try_clone()), Error::ReferenceLimit);
    assert_eq!(pool.snapshot(), before);
    drop(view);
    let view = buffer.try_clone().unwrap();
    let guard = lock(&pool.root.state);
    assert_eq!(error(buffer.try_clone()), Error::Contended);
    drop(guard);
    drop((view, buffer, domain, authority, pool));
    owner.try_destroy().unwrap();
}

#[test]
fn mem01_preparation_validates_layout_and_exact_modeled_reservation() {
    let mut config = Config::new(
        vec![ClassConfig {
            capacity: 64,
            slots: 2,
        }],
        1,
        usize::MAX,
    );
    let expected = Reservation::for_config(&config).unwrap();
    config.root_limit = expected.total_bytes - 1;
    assert_eq!(
        error(StorageOwner::prepare(config)),
        Error::RootBudgetExceeded
    );
    let config = Config::new(
        vec![ClassConfig {
            capacity: 64,
            slots: 2,
        }],
        1,
        expected.total_bytes,
    );
    let mut owner = StorageOwner::prepare(config).unwrap();
    assert_eq!(owner.snapshot().unwrap().reservation, expected);
    owner.try_destroy().unwrap();
    for classes in [
        vec![],
        vec![ClassConfig {
            capacity: 0,
            slots: 1,
        }],
        vec![ClassConfig {
            capacity: 1,
            slots: 0,
        }],
        vec![
            ClassConfig {
                capacity: 8,
                slots: 1,
            },
            ClassConfig {
                capacity: 8,
                slots: 1,
            },
        ],
    ] {
        assert_eq!(
            error(StorageOwner::prepare(Config::new(classes, 1, usize::MAX))),
            Error::InvalidConfiguration
        );
    }
    assert_eq!(
        error(StorageOwner::prepare(Config::new(
            vec![ClassConfig {
                capacity: usize::MAX,
                slots: 2
            }],
            1,
            usize::MAX,
        ))),
        Error::LayoutOverflow
    );
}

#[test]
fn life02_busy_retains_owner_and_only_cold_owner_destroys_storage() {
    let (mut owner, pool) = configured(1, 1);
    let diagnostics = owner.diagnostics().unwrap();
    let (authority, domain) = pool.try_open_domain(64).unwrap();
    let buffer = domain.try_acquire(1).unwrap().freeze();
    drop((pool, domain, authority));
    assert_eq!(owner.try_destroy(), Err(Error::Busy));
    assert!(!diagnostics.snapshot().destroyed);
    assert!(
        buffer.try_clone().is_ok(),
        "existing views remain valid after root close"
    );
    drop(buffer);
    assert!(
        !diagnostics.snapshot().destroyed,
        "buffer cannot inherit arena teardown"
    );
    owner.try_destroy().unwrap();
    assert!(diagnostics.snapshot().destroyed);
    assert_eq!(owner.try_destroy(), Err(Error::Destroyed));
}

#[test]
fn life02_abandonment_is_bounded_observable_and_terminal() {
    let (owner, pool) = configured(1, 1);
    let diagnostics = owner.diagnostics().unwrap();
    let (authority, domain) = pool.try_open_domain(64).unwrap();
    let buffer = domain.try_acquire(1).unwrap().freeze();
    let reserved = owner.snapshot().unwrap().reservation.total_bytes;
    drop(owner);
    let event = diagnostics.snapshot();
    assert_eq!(event.abps_abandoned_roots, 1);
    assert_eq!(event.abps_abandoned_bytes, reserved);
    assert_eq!(event.abandonment.unwrap().admitted_bytes, 64);
    assert_eq!(error(domain.try_acquire(1)), Error::Closed);
    drop(buffer);
    assert_eq!(pool.snapshot().domains[0].admitted_bytes, 0);
    assert_eq!(pool.snapshot().phase, RootPhase::Abandoned);
    drop((authority, domain, pool));
    assert_eq!(
        diagnostics.snapshot(),
        event,
        "later Drop must neither tear down nor duplicate event"
    );
}

#[test]
fn life02_forgotten_rights_never_refund_reuse_or_invent_events() {
    let (mut owner, pool) = configured(1, 1);
    let (authority, domain) = pool.try_open_domain(64).unwrap();
    std::mem::forget(domain.try_acquire(1).unwrap().freeze());
    drop((authority, domain));
    assert_eq!(pool.snapshot().domains[0].admitted_bytes, 64);
    assert_eq!(pool.snapshot().classes[0].available, 0);
    assert_eq!(
        error(pool.try_open_domain(64)),
        Error::DomainCapacityExhausted
    );
    drop(pool);
    assert_eq!(owner.try_destroy(), Err(Error::Busy));
    drop(owner);

    let (owner, pool) = configured(1, 1);
    let diagnostics = owner.diagnostics().unwrap();
    std::mem::forget(owner);
    assert_eq!(pool.snapshot().phase, RootPhase::Active);
    drop(pool);
    assert_eq!(diagnostics.snapshot(), DiagnosticsSnapshot::default());

    let (mut owner, pool) = configured(1, 1);
    let (authority, domain) = pool.try_open_domain(64).unwrap();
    drop(authority);
    std::mem::forget(domain);
    assert_eq!(pool.snapshot().domains[0].admitted_bytes, 0);
    assert_eq!(pool.snapshot().free_domain_records, 0);
    drop(pool);
    assert_eq!(owner.try_destroy(), Err(Error::Busy));
}

#[test]
fn shared_auto_traits() {
    fn send_sync<T: Send + Sync>() {}
    fn send<T: Send>() {}
    send_sync::<Buf>();
    send_sync::<Pool>();
    send_sync::<Domain>();
    send_sync::<DomainOwner>();
    send_sync::<StorageOwner>();
    send::<BufMut>();
}

#[test]
fn prog01_life01_final_return_acquire_and_close_race_on_new_threads() {
    // Schedule sampling of the real mutex reference, not a lock-free proof.
    for _ in 0..64 {
        let (mut owner, pool) = configured(1, 1);
        let (authority, domain) = pool.try_open_domain(128).unwrap();
        let buffer = domain.try_acquire(1).unwrap().freeze();
        let barrier = Arc::new(Barrier::new(3));
        std::thread::scope(|scope| {
            let start = Arc::clone(&barrier);
            scope.spawn(move || {
                start.wait();
                drop(buffer);
            });
            let start = Arc::clone(&barrier);
            let domain = &domain;
            scope.spawn(move || {
                start.wait();
                match domain.try_acquire(1) {
                    Ok(buffer) => drop(buffer),
                    Err(e) => assert!(matches!(
                        e,
                        Error::Closed | Error::ClassDepleted | Error::Contended
                    )),
                }
            });
            barrier.wait();
            authority.close();
        });
        let snapshot = pool.snapshot();
        assert_eq!(snapshot.classes[0].available, 1);
        assert_eq!(snapshot.domains[0].admitted_bytes, 0);
        drop((domain, authority, pool));
        owner.try_destroy().unwrap();
    }
}

#[test]
fn io03_full_input_budget_does_not_wait_for_impossible_output() {
    let (mut owner, pool) = configured(2, 1);
    let (authority, domain) = pool.try_open_domain(64).unwrap();
    let input = domain.try_acquire(64).unwrap();
    assert_eq!(error(domain.try_acquire(64)), Error::BudgetExceeded);
    drop((input, domain, authority));
    let (authority, domain) = pool.try_open_domain(128).unwrap();
    let input = domain.try_acquire(64).unwrap();
    let output = domain.try_acquire(64).unwrap();
    drop((input, output, domain, authority, pool));
    owner.try_destroy().unwrap();
}

#[test]
fn life01_root_close_overlaps_registration_return_and_drain() {
    for _ in 0..32 {
        let (mut owner, pool) = configured(1, 1);
        let diagnostics = owner.diagnostics().unwrap();
        let (authority, domain) = pool.try_open_domain(128).unwrap();
        let buffer = domain.try_acquire(1).unwrap().freeze();
        let barrier = Arc::new(Barrier::new(3));
        std::thread::scope(|scope| {
            let start = Arc::clone(&barrier);
            scope.spawn(move || {
                start.wait();
                drop(buffer);
            });
            let start = Arc::clone(&barrier);
            let domain = &domain;
            scope.spawn(move || {
                start.wait();
                match domain.try_acquire(1) {
                    Ok(buffer) => drop(buffer),
                    Err(error) => assert!(matches!(
                        error,
                        Error::Closed | Error::Contended | Error::ClassDepleted
                    )),
                }
            });
            barrier.wait();
            assert_eq!(owner.try_destroy(), Err(Error::Busy));
        });
        assert!(!diagnostics.snapshot().destroyed);
        assert_eq!(error(domain.try_acquire(1)), Error::Closed);
        assert_eq!(owner.snapshot().unwrap().classes[0].available, 1);
        drop((authority, domain, pool));
        owner.try_destroy().unwrap();
        assert!(diagnostics.snapshot().destroyed);
    }
}
