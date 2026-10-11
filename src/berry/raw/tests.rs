use super::*;
use crate::ClassConfig;
mod revisions;
mod conservation {
    use super::*;
    fn stages() -> &'static [Stage] {
        &[
            Stage::Pinned,
            Stage::Charged,
            Stage::Bit,
            Stage::Origin,
            Stage::Ready,
        ]
    }
    fn control_lifetime(p: &Pool) -> usize {
        p.root.root().claims.load()
    }
    include!("../../test_support/issuer_rights.rs");

    #[test]
    fn abps_grant_assignment_detector_has_no_destructor_cleanup() {
        // A corrupted assignment must fail its invariant, not panic again in
        // Domain::drop while unwinding. Execute the real mask protocol on Cells;
        // the separate arena regression still covers lifecycle integration.
        let free = Cell::new(0b111);
        let first = Cell::new(0);
        let second = Cell::new(0);
        protocol::assign_mask(&free, &first, 1);
        assert_eq!(free.get() & first.get(), 0, "SI-05 duplicated assignment");
        assert_eq!(free.get() | first.get(), 0b111);
        protocol::assign_mask(&free, &second, 2);
        assert_eq!(first.get() & second.get(), 0);
        assert_eq!(free.get(), 0);
        protocol::release_mask(&free, &first);
        assert_eq!(free.get() & second.get(), 0);
        protocol::release_mask(&free, &second);
        assert_eq!(free.get(), 0b111);
    }

    pub(super) fn partition(p: &Pool) {
        for c in 0..p.root.root().class_count {
            let class = p.root.class(c);
            let valid = usize::MAX >> (usize::BITS as usize - class.slots);
            let mut covered = class.unassigned.get();
            assert_eq!(covered & !valid, 0);
            for d in 0..p.root.root().domain_count {
                let grant = p.root.grant(d, c).get();
                assert_eq!(covered & grant, 0, "SI-05 overlapping grants");
                covered |= grant;
            }
            assert_eq!(covered, valid, "SI-05 lost or invalid grant bit");
        }
    }
    #[test]
    fn abps_grant_partition_survives_partial_open_and_pending_recycle() {
        let mut c = config(3, 3);
        c.capacity_policy = CapacityPolicy::Reserved;
        let mut owner = StorageOwner::prepare(&c).unwrap();
        let pool = owner.activate().unwrap();
        let grant = [ClassReservation {
            capacity: 16,
            slots: 1,
        }];
        for _ in 0..if cfg!(miri) { 2 } else { 32 } {
            let first = pool.try_open_reserved_domain(32, &grant).unwrap();
            partition(&pool);
            let second = pool.try_open_reserved_domain(32, &grant).unwrap();
            partition(&pool);
            assert_eq!(
                pool.try_open_reserved_domain(
                    32,
                    &[ClassReservation {
                        capacity: 16,
                        slots: 2
                    }]
                )
                .err(),
                Some(Error::ReservationUnavailable)
            );
            partition(&pool);
            let old = std::mem::ManuallyDrop::new(first.1.try_acquire(1).unwrap().freeze());
            let pending = old.lease.begin_return().unwrap().publish();
            drop(first);
            partition(&pool);
            assert_eq!(pool.poll_domains(1).reclaimed, 0);
            pending.finish();
            partition(&pool);
            assert_eq!(pool.poll_domains(1).reclaimed, 1);
            partition(&pool);
            let third = pool.try_open_reserved_domain(32, &grant).unwrap();
            partition(&pool);
            drop((second, third));
            partition(&pool);
        }
        drop(pool);
        owner.try_destroy().unwrap();
    }
}

mod grant_histories {
    use super::*;
    fn boundary_counts() -> Vec<usize> {
        vec![2, usize::BITS as usize - 1, usize::BITS as usize]
    }
    fn read_rights(p: &Pool, d: usize) -> Vec<std::collections::BTreeSet<usize>> {
        (0..p.root.root().class_count)
            .map(|c| {
                (0..p.root.class(c).slots)
                    .filter(|&s| p.root.grant(d, c).get() & (1 << s) != 0)
                    .collect()
            })
            .collect()
    }
    fn read_unassigned(p: &Pool) -> Vec<std::collections::BTreeSet<usize>> {
        (0..p.root.root().class_count)
            .map(|c| {
                (0..p.root.class(c).slots)
                    .filter(|&s| p.root.class(c).unassigned.get() & (1 << s) != 0)
                    .collect()
            })
            .collect()
    }
    fn audit_structure(p: &Pool) {
        super::conservation::partition(p);
    }
    include!("../../test_support/grant_histories.rs");
}

fn config(slots: usize, records: usize) -> Config {
    Config::new(
        vec![ClassConfig {
            capacity: 16,
            slots,
        }],
        records,
        1 << 20,
    )
}

#[test]
fn reserved_layout_counts_matrix_padding_and_checks_overflow_before_allocation() {
    let mut c = config(2, 3);
    let plain = Reservation::for_config(&c).unwrap();
    c.capacity_policy = CapacityPolicy::Reserved;
    let reserved = Reservation::for_config(&c).unwrap();
    assert_eq!(align_of::<Slot>(), 64);
    assert_eq!(size_of::<Slot>() % 64, 0);
    assert!(reserved.total_bytes >= plain.total_bytes);
    let plan = Plan::new(&c).unwrap();
    assert!(plan.payload[0] >= plan.grants + 3 * size_of::<Cell<usize>>());
    c.root_limit = reserved.total_bytes - 1;
    assert_eq!(Reservation::for_config(&c), Err(Error::RootBudgetExceeded));
    c.root_limit += 1;
    let mut owner = StorageOwner::prepare(&c).unwrap();
    owner.try_destroy().unwrap();
    c.domain_slots = usize::MAX;
    assert_eq!(Reservation::for_config(&c), Err(Error::LayoutOverflow));
}

#[test]
fn reserved_rollback_restores_slot_charge_and_preserves_partition() {
    let mut c = config(2, 2);
    c.capacity_policy = CapacityPolicy::Reserved;
    let owner = StorageOwner::prepare(&c).unwrap();
    let pool = owner.activate().unwrap();
    let grant = [ClassReservation {
        capacity: 16,
        slots: 1,
    }];
    let (a, d) = pool.try_open_reserved_domain(32, &grant).unwrap();
    let (b, other) = pool.try_open_reserved_domain(32, &grant).unwrap();
    for stage in [Stage::Charged, Stage::Bit, Stage::Ready] {
        assert_eq!(
            d.acquire(1, Some(Fault::Error(stage))).err(),
            Some(Error::Contended)
        );
        assert_eq!(d.metrics().admitted_bytes, 0);
        assert_eq!(pool.metrics().available_slots, 2);
        let held = d.try_acquire(1).unwrap();
        assert_eq!(d.try_acquire(1).err(), Some(Error::DomainClassDepleted));
        drop(other.try_acquire(1).unwrap());
        drop(held);
    }
    drop((b, other));
    finish(owner, pool, a, d);
}
fn fixture(slots: usize, records: usize) -> (StorageOwner, Pool, DomainOwner, Domain) {
    let owner = StorageOwner::prepare(&config(slots, records)).unwrap();
    let pool = owner.activate().unwrap();
    let (authority, domain) = pool.try_open_domain(16 * slots).unwrap();
    (owner, pool, authority, domain)
}
fn finish(mut owner: StorageOwner, pool: Pool, authority: DomainOwner, domain: Domain) {
    assert_eq!(domain.metrics().admitted_bytes, 0);
    assert_eq!(domain.metrics().allocations, 0);
    assert_eq!(pool.metrics().available_slots, pool.metrics().total_slots);
    drop((domain, authority, pool));
    owner.try_destroy().unwrap();
}

#[test]
fn full_word_capacity_errors_and_reuse() {
    let (owner, pool, authority, domain) = fixture(MAX_SLOTS_PER_CLASS, 1);
    let mut held: Vec<_> = (0..MAX_SLOTS_PER_CLASS)
        .map(|_| domain.try_acquire(16).unwrap())
        .collect();
    assert!(matches!(domain.try_acquire(1), Err(Error::BudgetExceeded)));
    drop(held.pop());
    assert_eq!(pool.metrics().available_slots, 1);
    held.push(domain.try_acquire(1).unwrap());
    assert!(matches!(domain.try_acquire(0), Err(Error::InvalidSize)));
    assert!(matches!(
        domain.try_acquire(usize::MAX),
        Err(Error::TooLarge)
    ));
    drop(held);
    finish(owner, pool, authority, domain);
}

#[test]
fn initialization_and_neighboring_live_references() {
    let (owner, pool, authority, domain) = fixture(3, 1);
    let mut a = domain.try_acquire(16).unwrap();
    let mut b = domain.try_acquire(16).unwrap();
    a.put_slice(&[0x5a; 16]).unwrap();
    let a = a.freeze();
    let reference = a.as_ref();
    b.initialize_to(16).unwrap().fill(0xa5);
    b.set_filled_len(16).unwrap();
    assert_eq!(reference, &[0x5a; 16]);
    assert_eq!(b.as_ref(), &[0xa5; 16]);
    drop(b);
    let mut b = domain.try_acquire(16).unwrap();
    assert!(b.as_ref().is_empty());
    assert_eq!(b.set_filled_len(1), Err(Error::NotInitialized));
    assert_eq!(b.initialize_to(16).unwrap(), &[0; 16]);
    b.put_slice(&[1; 16]).unwrap();
    assert_eq!(b.put_slice(&[2]), Err(Error::InvalidRange));
    assert_eq!(reference, &[0x5a; 16]);
    drop((a, b));
    finish(owner, pool, authority, domain);
}

#[test]
fn empty_views_retain_full_charge_and_limits_do_not_touch_root_claims() {
    let mut cfg = config(1, 1);
    cfg.reference_limit = 3;
    cfg.handle_limit = 5;
    let owner = StorageOwner::prepare(&cfg).unwrap();
    let pool = owner.activate().unwrap();
    let (authority, domain) = pool.try_open_domain(16).unwrap();
    let frame = domain.try_acquire(16).unwrap().freeze();
    let tiny = frame.try_slice(0..0).unwrap();
    let extra = tiny.try_clone().unwrap();
    assert!(matches!(extra.try_clone(), Err(Error::ReferenceLimit)));
    assert!(matches!(domain.try_clone(), Err(Error::ReferenceLimit)));
    assert_eq!(pool.metrics().root_claims, 5);
    drop((frame, tiny));
    assert_eq!(domain.metrics().admitted_bytes, 16);
    assert!(matches!(
        extra.try_slice(0..usize::MAX),
        Err(Error::InvalidRange)
    ));
    drop(extra);
    finish(owner, pool, authority, domain);
}

#[test]
fn mutable_handoff_freeze_and_first_remote_final_return() {
    fn send<T: Send>() {}
    fn sync<T: Sync>() {}
    send::<BufMut>();
    send::<Buf>();
    sync::<Buf>();
    let (owner, pool, authority, domain) = fixture(1, 1);
    let mut buffer = domain.try_acquire(8).unwrap();
    buffer.put_slice(b"mutable").unwrap();
    let frame = std::thread::spawn(move || {
        buffer.put_slice(b" remote").unwrap();
        buffer.freeze()
    })
    .join()
    .unwrap();
    assert_eq!(frame.as_ref(), b"mutable remote");
    assert_eq!(frame.as_io_slice().as_ref(), b"mutable remote");
    assert_eq!(frame.try_as_io_slice().unwrap().as_ref(), b"mutable remote");
    std::thread::spawn(move || drop(frame)).join().unwrap();
    finish(owner, pool, authority, domain);
}

#[test]
fn cold_teardown_observes_final_return_without_thread_join() {
    let (mut owner, pool, authority, domain) = fixture(1, 1);
    let diagnostics = owner.diagnostics().unwrap();
    let frame = domain.try_acquire(8).unwrap().freeze();
    drop((domain, authority, pool));
    let worker = std::thread::spawn(move || drop(frame));
    // Only the acquire observation of root claims may authorize deallocation.
    // The later join must not provide its synchronization edge.
    loop {
        match owner.try_destroy() {
            Ok(()) => break,
            Err(Error::Busy) => std::thread::yield_now(),
            result => panic!("unexpected destroy: {result:?}"),
        }
    }
    assert!(diagnostics.snapshot().destroyed);
    worker.join().unwrap();
}

#[test]
fn life02_final_claim_release_races_teardown_across_many_schedules() {
    // LIFE-02/AP-03: each return releases its root claim last; the owner may
    // free the arena right after acquiring claims == 1. Miri sees a protected
    // borrow across that release only when a schedule preempts the returner
    // there, so one seed runs many races (see the Mosaic final-refund test).
    // The window after the claim release spans only a few basic blocks: with
    // the padded-ledger control, 128 races exposed it on 7 of 16 seeds, 512 on 15.
    let rounds = 512;
    let (tx, rx) = std::sync::mpsc::channel::<Buf>();
    let returner = std::thread::spawn(move || rx.into_iter().for_each(drop));
    for _ in 0..rounds {
        let (mut owner, pool, authority, domain) = fixture(1, 1);
        let frame = domain.try_acquire(1).unwrap().freeze();
        drop((domain, authority, pool));
        tx.send(frame).unwrap();
        loop {
            match owner.try_destroy() {
                Ok(()) => break,
                Err(Error::Busy) if cfg!(miri) => {}
                Err(Error::Busy) => std::thread::yield_now(),
                result => panic!("unexpected destroy: {result:?}"),
            }
        }
    }
    drop(tx);
    returner.join().unwrap();
}

#[test]
fn transform_requires_explicit_input_output_headroom() {
    let (owner, pool, authority, domain) = fixture(2, 2);
    let (limited_owner, limited) = pool.try_open_domain(16).unwrap();
    let input = limited.try_acquire(16).unwrap().freeze();
    assert!(matches!(
        limited.try_acquire(16),
        Err(Error::BudgetExceeded)
    ));
    // Integration must reject this step or use an independently budgeted output.
    let output = domain.try_acquire(16).unwrap().freeze();
    assert_eq!(
        limited.metrics().admitted_bytes + domain.metrics().admitted_bytes,
        32
    );
    drop((input, output, limited, limited_owner));
    finish(owner, pool, authority, domain);
}

#[test]
fn rollback_at_every_admission_stage() {
    let (owner, pool, authority, domain) = fixture(1, 1);
    for stage in [Stage::Charged, Stage::Bit, Stage::Ready] {
        assert!(matches!(
            domain.acquire(8, Some(Fault::Error(stage))),
            Err(Error::Contended)
        ));
        assert_eq!(domain.metrics().admitted_bytes, 0);
        assert_eq!(domain.metrics().allocations, 0);
        assert_eq!(pool.metrics().available_slots, 1);
        assert_eq!(pool.metrics().root_claims, 4);
        drop(domain.try_acquire(8).unwrap().freeze());
    }
    finish(owner, pool, authority, domain);
}

#[test]
fn si01_depleted_attempts_restore_claims_and_preserve_budget_precedence() {
    for policy in [CapacityPolicy::Unreserved, CapacityPolicy::Reserved] {
        let mut c = config(2, 3);
        c.capacity_policy = policy;
        let owner = StorageOwner::prepare(&c).unwrap();
        let pool = owner.activate().unwrap();
        let (a, d) = if policy == CapacityPolicy::Reserved {
            pool.try_open_reserved_domain(
                64,
                &[ClassReservation {
                    capacity: 16,
                    slots: 1,
                }],
            )
        } else {
            pool.try_open_domain(64)
        }
        .unwrap();
        let count = if policy == CapacityPolicy::Reserved {
            1
        } else {
            2
        };
        let held: Vec<_> = (0..count).map(|_| d.try_acquire(1).unwrap()).collect();
        let expected = if policy == CapacityPolicy::Reserved {
            Error::DomainClassDepleted
        } else {
            Error::ClassDepleted
        };
        let claims = pool.metrics().root_claims;
        for _ in 0..8 {
            assert_eq!(d.try_acquire(1).err(), Some(expected));
            assert_eq!(d.metrics().admitted_bytes, count * 16);
            assert_eq!(d.metrics().allocations, count);
            assert_eq!(pool.metrics().root_claims, claims);
            // Reserved refusal must leave the unassigned, physically free bit alone.
            assert_eq!(pool.metrics().available_slots, 2 - count);
        }
        if policy == CapacityPolicy::Reserved {
            let (empty_owner, empty) = pool.try_open_reserved_domain(16, &[]).unwrap();
            let (zero_owner, zero) = pool.try_open_reserved_domain(0, &[]).unwrap();
            let claims = pool.metrics().root_claims;
            assert_eq!(empty.try_acquire(1).err(), Some(Error::DomainClassDepleted));
            assert_eq!(zero.try_acquire(1).err(), Some(Error::BudgetExceeded));
            assert_eq!(empty.metrics().admitted_bytes, 0);
            assert_eq!(empty.metrics().allocations, 0);
            assert_eq!(zero.metrics().admitted_bytes, 0);
            assert_eq!(pool.metrics().root_claims, claims);
            assert_eq!(pool.metrics().available_slots, 1);
            drop((empty, empty_owner, zero, zero_owner));
        }
        drop(held);
        finish(owner, pool, a, d);
    }
}

#[test]
fn rollback_survives_unwind_after_slot_claim() {
    let (owner, pool, authority, domain) = fixture(1, 1);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        domain.root.pin(1).unwrap();
        let mut guard = Admission {
            root: domain.root,
            key: domain.key,
            class: 0,
            charge: 0,
            bit: None,
            committed: false,
        };
        domain
            .root
            .domain(domain.key.index)
            .ledger
            .reserve(16, 16)
            .unwrap();
        guard.charge = 16;
        guard.bit = Some(domain.root.class(0).bitmap.take().unwrap());
        panic!("injected test-only unwind");
    }));
    assert!(outcome.is_err());
    finish(owner, pool, authority, domain);
}

#[test]
fn old_return_after_reuse_refunds_only_its_origin_and_pins_shutdown() {
    let (mut owner, pool, old_owner, old) = fixture(1, 2);
    let (new_owner, new) = pool.try_open_domain(16).unwrap();
    let frame = old.try_acquire(16).unwrap().freeze();
    let old_key = old.identity();
    let (published_tx, published_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        // Test driver splits the very same Return transition used by Lease::drop.
        let lease = std::mem::ManuallyDrop::new(frame.lease);
        let pending = lease.begin_return().unwrap().publish();
        published_tx.send(()).unwrap();
        resume_rx.recv().unwrap();
        pending.finish();
    });
    published_rx.recv().unwrap();
    assert_eq!(old.metrics().admitted_bytes, 16);
    assert_eq!(old.metrics().allocations, 1);
    assert!(matches!(old.try_acquire(1), Err(Error::BudgetExceeded)));
    let mut reused = new.try_acquire(16).unwrap();
    reused.put_slice(b"new incarnation").unwrap();
    assert_ne!(reused.origin(), old_key);
    drop((old_owner, old));
    assert!(matches!(
        pool.try_open_domain(16),
        Err(Error::DomainMaintenanceRequired)
    ));
    assert_eq!(owner.try_destroy(), Err(Error::Busy));
    resume_tx.send(()).unwrap();
    worker.join().unwrap();
    assert_eq!(new.metrics().admitted_bytes, 16);
    assert_eq!(reused.as_ref(), b"new incarnation");
    let snapshot = pool.snapshot();
    assert_eq!(
        snapshot
            .domains
            .iter()
            .find(|d| d.identity == old_key)
            .unwrap()
            .observed_admitted_bytes,
        0
    );
    drop((reused, new, new_owner, pool));
    owner.try_destroy().unwrap();
}

#[test]
fn close_preserves_existing_views_and_domain_generation_changes_after_drain() {
    let (mut owner, pool, authority, domain) = fixture(1, 1);
    let frame = domain.try_acquire(16).unwrap().freeze();
    let key = domain.identity();
    authority.close();
    assert!(matches!(domain.try_acquire(1), Err(Error::Closed)));
    drop((authority, domain));
    assert!(matches!(
        pool.try_open_domain(16),
        Err(Error::DomainMaintenanceRequired)
    ));
    let view = frame.try_clone().unwrap();
    drop((frame, view));
    assert_eq!(pool.snapshot().pending_domain_records, 1);
    assert_eq!(pool.poll_domains(1).reclaimed, 1);
    let (next_owner, next) = pool.try_open_domain(16).unwrap();
    assert_ne!(next.identity(), key);
    drop((next, next_owner, pool));
    owner.try_destroy().unwrap();
}

#[test]
fn generation_exhaustion_retires_record() {
    let mut owner = StorageOwner::prepare(&config(1, 1)).unwrap();
    let pool = owner.activate().unwrap();
    let record = pool.root.domain(0);
    let mut state = record.state.get();
    state.generation = u64::MAX - 1;
    record.state.set(state);
    let (authority, domain) = pool.try_open_domain(16).unwrap();
    assert_eq!(domain.key.generation, u64::MAX);
    drop((authority, domain));
    assert!(matches!(
        pool.try_open_domain(16),
        Err(Error::DomainCapacityExhausted)
    ));
    assert_eq!(pool.snapshot().retired_domain_records, 1);
    drop(pool);
    owner.try_destroy().unwrap();
}

#[test]
fn life02_root_identity_is_opaque_and_survives_storage_reuse() {
    // LIFE-02: diagnostics outlive their owner, so a destroyed root's identity
    // must not reappear when the allocator reuses its arena for a new root.
    let first = StorageOwner::prepare(&config(1, 1)).unwrap();
    let observer = first.diagnostics().unwrap();
    let address = first.root.unwrap().0.as_ptr().addr();
    drop(first);
    let second = StorageOwner::prepare(&config(1, 1)).unwrap();
    let (old, new) = (
        observer.snapshot(),
        second.diagnostics().unwrap().snapshot(),
    );
    assert!(old.destroyed);
    assert!(old.root_identity != 0 && new.root_identity > old.root_identity);
    assert_ne!(old.root_identity, address, "no heap address is exported");
}

#[test]
fn abandonment_is_observable_and_never_tears_down_in_buffer_drop() {
    let (owner, pool, authority, domain) = fixture(1, 1);
    let diagnostics = owner.diagnostics().unwrap();
    let frame = domain.try_acquire(16).unwrap().freeze();
    drop((domain, authority, pool, owner));
    assert_eq!(diagnostics.snapshot().abps_abandoned_roots, 1);
    std::thread::spawn(move || drop(frame)).join().unwrap();
    assert!(!diagnostics.snapshot().destroyed);
}

#[test]
fn forgotten_buffer_keeps_charge_and_owner_forget_runs_no_diagnostic() {
    let (mut owner, pool, authority, domain) = fixture(1, 1);
    let diagnostics = owner.diagnostics().unwrap();
    std::mem::forget(domain.try_acquire(16).unwrap());
    assert_eq!(domain.metrics().admitted_bytes, 16);
    drop((domain, authority, pool));
    assert_eq!(owner.try_destroy(), Err(Error::Busy));
    std::mem::forget(owner);
    assert_eq!(diagnostics.snapshot().abps_abandoned_roots, 0);
}

#[test]
fn checked_layout_exact_budget_and_class_boundaries() {
    let mut cfg = config(1, 1);
    let reservation = Reservation::for_config(&cfg).unwrap();
    assert_eq!(
        reservation.total_bytes,
        reservation.backing_bytes + reservation.metadata_bytes
    );
    cfg.root_limit = reservation.total_bytes - 1;
    assert!(matches!(
        StorageOwner::prepare(&cfg),
        Err(Error::RootBudgetExceeded)
    ));
    cfg.root_limit += 1;
    StorageOwner::prepare(&cfg).unwrap().try_destroy().unwrap();
    cfg = config(MAX_SLOTS_PER_CLASS + 1, 1);
    assert!(matches!(
        StorageOwner::prepare(&cfg),
        Err(Error::InvalidConfiguration)
    ));
    cfg = config(1, 1);
    cfg.classes[0].capacity = usize::MAX;
    assert!(matches!(
        StorageOwner::prepare(&cfg),
        Err(Error::LayoutOverflow)
    ));
    cfg = config(1, 1);
    cfg.classes = (1..=MAX_CLASSES)
        .map(|i| ClassConfig {
            capacity: i * 16,
            slots: 1,
        })
        .collect();
    let mut owner = StorageOwner::prepare(&cfg).unwrap();
    let pool = owner.activate().unwrap();
    let (authority, domain) = pool.try_open_domain(1 << 20).unwrap();
    for i in 1..=MAX_CLASSES {
        assert_eq!(domain.try_acquire(i * 16).unwrap().capacity(), i * 16);
        assert_eq!(
            domain.try_acquire((i - 1) * 16 + 1).unwrap().capacity(),
            i * 16
        );
    }
    assert!(matches!(
        domain.try_acquire(MAX_CLASSES * 16 + 1),
        Err(Error::TooLarge)
    ));
    drop((domain, authority, pool));
    owner.try_destroy().unwrap();
}

#[test]
fn stranded_capacity_is_not_borrowed_from_another_root() {
    let (owner, pool, authority, domain) = fixture(1, 2);
    let (other_owner, other_pool, other_authority, other_domain) = fixture(1, 1);
    let (observer_owner, observer) = pool.try_open_domain(16).unwrap();
    let retained = domain.try_acquire(16).unwrap().freeze();
    assert!(matches!(observer.try_acquire(1), Err(Error::ClassDepleted)));
    assert_eq!(other_pool.metrics().available_slots, 1);
    drop((retained, observer_owner, observer));
    finish(owner, pool, authority, domain);
    finish(other_owner, other_pool, other_authority, other_domain);
}

#[test]
fn maintenance_is_bounded_fair_and_snapshots_do_not_reap() {
    let (mut owner, pool, a_owner, a) = fixture(2, 3);
    let (b_owner, b) = pool.try_open_domain(16).unwrap();
    let (c_owner, c) = pool.try_open_domain(16).unwrap();
    let stuck = a.try_acquire(16).unwrap();
    let remote = b.try_acquire(16).unwrap();
    let old = b.identity();
    drop((a_owner, a, b, b_owner));
    assert_eq!(
        pool.poll_domains(0),
        DomainMaintenance {
            examined: 0,
            reclaimed: 0,
            pending: 2
        }
    );
    std::thread::spawn(move || drop(remote)).join().unwrap();
    for _ in 0..2 {
        assert_eq!(pool.snapshot().pending_domain_records, 2);
        assert!(matches!(
            pool.try_open_domain(16),
            Err(Error::DomainMaintenanceRequired)
        ));
    }
    assert_eq!(
        pool.poll_domains(1),
        DomainMaintenance {
            examined: 1,
            reclaimed: 0,
            pending: 2
        }
    );
    assert_eq!(
        pool.poll_domains(1),
        DomainMaintenance {
            examined: 1,
            reclaimed: 1,
            pending: 1
        }
    );
    let (new_owner, new) = pool.try_open_domain(16).unwrap();
    assert_eq!(new.identity().index, old.index);
    assert_ne!(new.identity(), old);
    assert_eq!(pool.poll_domains(usize::MAX).examined, 1);
    drop((new, new_owner, c_owner, c, stuck));
    // Pending records don't pin the root or require a final sweep to destroy it.
    assert_eq!(pool.snapshot().pending_domain_records, 1);
    drop(pool);
    owner.try_destroy().unwrap();
}

#[test]
fn immediate_domain_reuse_does_not_need_maintenance() {
    let (mut owner, pool, authority, domain) = fixture(1, 1);
    let key = domain.identity();
    drop((domain, authority));
    assert_eq!(pool.metrics().pending_domain_records, 0);
    let (authority, domain) = pool.try_open_domain(16).unwrap();
    assert_ne!(domain.identity(), key);
    drop((authority, domain, pool));
    owner.try_destroy().unwrap();
}

#[cfg(feature = "bytes-interop")]
#[test]
fn bytes_cursor_commits_only_written_prefix_and_cannot_cross_neighbor() {
    use bytes::BufMut as _;
    let (mut owner, pool, authority, domain) = fixture(2, 1);
    // fixture has limit 16; enlarge it before activation would change fixture API.
    drop((authority, domain));
    let (authority, domain) = pool.try_open_domain(32).unwrap();
    let mut a = domain.try_acquire(16).unwrap();
    let mut b = domain.try_acquire(16).unwrap();
    b.put_slice(b"neighbor").unwrap();
    assert_eq!(a.remaining_mut(), 16);
    let mut limited = (&mut a).limit(3);
    limited.put_slice(b"abc");
    assert_eq!(limited.remaining_mut(), 0);
    assert_eq!(a.as_ref(), b"abc");
    assert_eq!(a.initialized_len(), 3);
    assert_eq!(a.chunk_mut().len(), 13);
    a.put_bytes(0x55, 13);
    assert_eq!(a.chunk_mut().len(), 0);
    assert_eq!(b.as_ref(), b"neighbor");
    drop(a.freeze());
    let a = domain.try_acquire(16).unwrap();
    assert_eq!((a.filled_len(), a.initialized_len()), (0, 0));
    assert!(a.as_ref().is_empty());
    drop((a, b, domain, authority, pool));
    owner.try_destroy().unwrap();
}

// AP-02/03/05: execute the production transition one phase at a time. These
// samples describe publication, not an invented partition of pending phases.
#[test]
fn ap05_zero_references_and_published_pending_refund_are_honest_samples() {
    let (owner, pool, a, d) = fixture(1, 1);
    let frame = std::mem::ManuallyDrop::new(d.try_acquire(1).unwrap().freeze());
    let ticket = frame.lease.begin_return().unwrap();
    let sample = pool.snapshot();
    assert_eq!(sample.slots[0].references, 0);
    assert_eq!(sample.classes[0].observed_available_slots, 0);
    assert_eq!(sample.classes[0].observed_unpublished_slots, 1);
    assert_eq!(
        sample.domains[d.identity().index].observed_unpublished_backing_bytes,
        16
    );
    assert_eq!(
        sample.domains[d.identity().index].observed_admitted_bytes,
        16
    );
    let pending = ticket.publish();
    let sample = pool.snapshot();
    assert_eq!(sample.classes[0].observed_available_slots, 1);
    assert_eq!(sample.classes[0].observed_unpublished_slots, 0);
    assert_eq!(
        sample.domains[d.identity().index].observed_unpublished_backing_bytes,
        0
    );
    assert_eq!(
        sample.domains[d.identity().index].observed_admitted_bytes,
        16
    );
    pending.finish();
    finish(owner, pool, a, d);
}

#[test]
fn ap04_owner_drop_races_final_return_without_delegating_teardown() {
    for _ in 0..if cfg!(miri) { 2 } else { 16 } {
        let (owner, pool, a, d) = fixture(1, 1);
        let diagnostics = owner.diagnostics().unwrap();
        let frame = d.try_acquire(1).unwrap().freeze();
        drop((a, d, pool));
        let worker = std::thread::spawn(move || drop(frame));
        drop(owner);
        worker.join().unwrap();
        let sample = diagnostics.snapshot();
        // Either owner observed complete drain, or abandonment is permanent.
        assert_eq!(
            usize::from(sample.destroyed) + sample.abps_abandoned_roots,
            1
        );
        assert_eq!(sample.abps_abandoned_bytes > 0, !sample.destroyed);
    }
}

#[test]
fn mem02_own03_every_guarded_admission_fault_restores_resources() {
    // The actual acquisition runs through every guarded phase, including origin
    // overwrite on the same slot after earlier successful incarnations.
    for reserved in [false, true] {
        let mut c = config(1, 1);
        if reserved {
            c.capacity_policy = CapacityPolicy::Reserved;
        }
        let mut owner = StorageOwner::prepare(&c).unwrap();
        let pool = owner.activate().unwrap();
        let (a, d) = if reserved {
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
        .unwrap();
        let reservation = owner.reservation().unwrap();
        for stage in [
            Stage::Pinned,
            Stage::Charged,
            Stage::Bit,
            Stage::Origin,
            Stage::Ready,
        ] {
            for unwind in [false, true] {
                let mut old = d.try_acquire(16).unwrap();
                old.put_slice(&[0x5a; 16]).unwrap();
                drop(old.freeze());
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    d.acquire(
                        16,
                        Some(if unwind {
                            Fault::Panic(stage)
                        } else {
                            Fault::Error(stage)
                        }),
                    )
                }));
                if unwind {
                    assert!(outcome.is_err(), "{stage:?}");
                } else {
                    assert_eq!(outcome.unwrap().err(), Some(Error::Contended), "{stage:?}");
                }
                assert_eq!(d.metrics().admitted_bytes, 0, "MEM-02 {stage:?}");
                assert_eq!(owner.reservation().unwrap(), reservation, "MEM-01");
                let mut next = d.try_acquire(16).unwrap();
                assert_eq!(
                    (next.filled_len(), next.initialized_len()),
                    (0, 0),
                    "OWN-01"
                );
                assert_eq!(next.initialize_to(16).unwrap(), &[0; 16]);
                next.set_filled_len(16).unwrap();
                drop(next.freeze());
            }
        }
        drop((a, d, pool));
        owner.try_destroy().unwrap(); // LIFE-02: no leaked claim/charge at any phase.
    }
}

mod layouts {
    use super::*;
    fn configure_layout(_: &mut Config) {}
    fn additional_ranges(p: &Plan, c: &Config, ranges: &mut Vec<(u128, u128)>) {
        let n = if c.capacity_policy == CapacityPolicy::Reserved {
            c.classes.len() * c.domain_slots
        } else {
            0
        };
        ranges.push(range::<Cell<usize>>(p.grants, n));
    }
    include!("../../test_support/layout_ranges.rs");
}
