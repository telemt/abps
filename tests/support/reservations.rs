use super::backend::*;

fn config() -> Config {
    let mut c = Config::new(vec![ClassConfig { capacity: 8, slots: 2 }, ClassConfig { capacity: 32, slots: 1 }], 3, 1 << 20);
    c.capacity_policy = CapacityPolicy::Reserved;
    c
}
fn grant(capacity: usize, slots: usize) -> ClassReservation { ClassReservation { capacity, slots } }

#[test]
fn isolated_grants_do_not_borrow_idle_slots_or_spill_classes() {
    let mut owner = prepare(config()).unwrap();
    let pool = owner.activate().unwrap();
    assert_eq!(pool.try_open_domain(128).err(), Some(Error::InvalidConfiguration));
    let (a, da) = pool.try_open_reserved_domain(128, &[grant(8, 1)]).unwrap();
    let (b, db) = pool.try_open_reserved_domain(128, &[grant(8, 1), grant(32, 1)]).unwrap();
    let one = da.try_acquire(1).unwrap().freeze();
    let view = one.try_slice(0..0).unwrap();
    drop(one);
    assert_eq!(da.try_acquire(1).err(), Some(Error::DomainClassDepleted));
    assert_eq!(da.try_acquire(9).err(), Some(Error::DomainClassDepleted));
    let other = db.try_acquire(1).unwrap();
    drop(view);
    let reused = da.try_acquire(1).unwrap();
    assert_eq!(pool.snapshot().classes[0].reserved, 2);
    drop((other, reused, da, a, db, b, pool));
    owner.try_destroy().unwrap();
}

#[test]
fn all_grants_and_record_commit_together_and_closed_handles_pin_rights() {
    let mut owner = prepare(config()).unwrap();
    let pool = owner.activate().unwrap();
    let (a, da) = pool.try_open_reserved_domain(32, &[grant(32, 1)]).unwrap();
    let before = pool.snapshot();
    assert_eq!(pool.try_open_reserved_domain(128, &[grant(8, 2), grant(32, 1)]).err(), Some(Error::ReservationUnavailable));
    let after = pool.snapshot();
    assert_eq!(after.free_domain_records, before.free_domain_records);
    assert_eq!(after.classes[0].unassigned, 2);
    for grants in [&[grant(8, 0)][..], &[grant(7, 1)], &[grant(32, 1), grant(8, 1)], &[grant(8, 1), grant(8, 1)]] {
        assert_eq!(pool.try_open_reserved_domain(128, grants).err(), Some(Error::InvalidConfiguration));
    }
    a.close();
    assert_eq!(pool.try_open_reserved_domain(32, &[grant(32, 1)]).err(), Some(Error::ReservationUnavailable));
    drop(a);
    assert_eq!(pool.try_open_reserved_domain(32, &[grant(32, 1)]).err(), Some(Error::ReservationUnavailable));
    drop(da);
    let (b, db) = pool.try_open_reserved_domain(0, &[grant(32, 1)]).unwrap();
    assert_eq!(db.try_acquire(9).err(), Some(Error::BudgetExceeded));
    drop((b, db, pool));
    owner.try_destroy().unwrap();
}

#[test]
fn remote_return_releases_capacity_but_grants_wait_for_domain_recycle() {
    let mut owner = prepare(config()).unwrap();
    let pool = owner.activate().unwrap();
    let (a, da) = pool.try_open_reserved_domain(32, &[grant(32, 1)]).unwrap();
    let old = da.identity();
    let buffer = da.try_acquire(32).unwrap();
    drop((a, da));
    assert_eq!(pool.try_open_reserved_domain(32, &[grant(32, 1)]).err(), Some(Error::ReservationUnavailable));
    assert_eq!(poll(&pool, 1).1, 0);
    std::thread::spawn(move || drop(buffer)).join().unwrap();
    if EXPLICIT_MAINTENANCE {
        assert_eq!(class_counts(&pool.snapshot().classes[1]).0, 1);
        assert_eq!(pool.try_open_reserved_domain(32, &[grant(32, 1)]).err(), Some(Error::ReservationUnavailable));
        assert_eq!(poll(&pool, 1), (1, 1, 0));
    }
    let (b, db) = pool.try_open_reserved_domain(32, &[grant(32, 1)]).unwrap();
    assert_ne!(old, db.identity());
    drop((b, db, pool));
    owner.try_destroy().unwrap();
}

#[test]
fn forgotten_buffer_pins_grant_and_abandonment_remains_safe() {
    let owner = prepare(config()).unwrap();
    let diagnostics = owner.diagnostics().unwrap();
    let pool = owner.activate().unwrap();
    let (a, da) = pool.try_open_reserved_domain(32, &[grant(32, 1)]).unwrap();
    std::mem::forget(da.try_acquire(32).unwrap());
    drop((a, da));
    poll(&pool, 3);
    assert_eq!(pool.try_open_reserved_domain(32, &[grant(32, 1)]).err(), Some(Error::ReservationUnavailable));
    assert_eq!(pool.snapshot().classes[1].reserved, 1);
    drop((pool, owner));
    assert_eq!(diagnostics.snapshot().abps_abandoned_roots, 1);
}

#[test]
fn si10_batch_is_atomic_at_limits_and_preserves_slice_bytes_through_reuse() {
    let mut cfg = config();
    cfg.reference_limit = 3;
    let mut owner = prepare(cfg).unwrap();
    let pool = owner.activate().unwrap();
    let (a,d) = pool.try_open_reserved_domain(32, &[grant(32,1)]).unwrap();
    for _ in 0..8 {
        let mut b = d.try_acquire(32).unwrap();
        b.put_slice(b"0123456789").unwrap();
        let b = b.freeze().try_slice(2..7).unwrap();
        let mut views = std::array::from_fn::<_,2,_>(|_| None);
        b.try_clone_into(&mut views).unwrap();
        b.try_clone_into(&mut []).unwrap();
        assert_eq!(b.try_clone_into(&mut [None]), Err(Error::ReferenceLimit));
        assert_eq!(b.try_clone_into(&mut views), Err(Error::OutputNotEmpty));
        assert!(views.iter().all(|b| b.as_ref().unwrap().as_ref()==b"23456"));
        assert_eq!(domain_bytes(pool.snapshot().domains.iter().find(|s| s.identity == d.identity()).unwrap()).0,32);
        drop(b);
        drop(views[0].take());
        assert_eq!(d.try_acquire(1).err(), Some(Error::BudgetExceeded));
        drop(views);
        assert_eq!(domain_bytes(pool.snapshot().domains.iter().find(|s| s.identity == d.identity()).unwrap()).0,0);
    }
    drop((a,d,pool));
    owner.try_destroy().unwrap();
}
