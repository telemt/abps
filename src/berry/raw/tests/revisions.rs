use super::*;

#[test]
fn si07_pages_bound_record_visits_and_preserve_unwritten_output() {
    let (owner, pool, authority, domain) = fixture(1, 8);
    let sample = domain.metrics();
    let mut dst = [sample; 3];
    let mut visited = 0;
    let page = domain_metrics_page(5, &mut dst, 7, |i| {
        assert_eq!(i, 5 + visited);
        visited += 1;
        domain_metrics(pool.root, i)
    })
    .unwrap();
    assert_eq!(visited, 2);
    assert_eq!(
        page,
        MetricsPage {
            written: 2,
            next_index: 7,
            done: true
        }
    );
    assert_eq!(dst[2], sample);
    assert_eq!(
        pool.domain_metrics_page(9, &mut dst),
        Err(Error::InvalidRange)
    );
    assert_eq!(pool.domain_metrics_page(8, &mut dst).unwrap().written, 0);
    assert_eq!(
        pool.domain_metrics_page(0, &mut []).unwrap(),
        MetricsPage {
            written: 0,
            next_index: 0,
            done: false
        }
    );
    let page = pool.domain_metrics_page(0, &mut dst).unwrap();
    assert_eq!(page.next_index, 3);
    assert_eq!(
        dst.iter().map(|d| d.identity.index).collect::<Vec<_>>(),
        [0, 1, 2]
    );
    assert!(dst.iter().all(|d| d.phase == DomainPhase::Free));
    finish(owner, pool, authority, domain);
}

#[test]
fn si07_sampling_is_observational_across_pending_return_and_incarnation_reuse() {
    let mut cfg = config(2, 2);
    cfg.capacity_policy = CapacityPolicy::Reserved;
    let mut owner = StorageOwner::prepare(&cfg).unwrap();
    let pool = owner.activate().unwrap();
    let (a, d) = pool
        .try_open_reserved_domain(
            32,
            &[ClassReservation {
                capacity: 16,
                slots: 1,
            }],
        )
        .unwrap();
    let old = d.identity();
    let extra = d.try_clone().unwrap();
    assert_eq!((d.metrics().owner, d.metrics().control_handles), (true, 2));
    drop(extra);
    let mut dst = [d.metrics(); 2];
    let mut grants = [
        ClassGrantSnapshot {
            capacity: 0,
            reserved: 0,
            available: 0,
        },
        ClassGrantSnapshot {
            capacity: 99,
            reserved: 99,
            available: 99,
        },
    ];
    let original = grants.clone();
    assert_eq!(d.class_grants_into(&mut []), Err(Error::InvalidSize));
    assert_eq!(grants, original);
    assert_eq!(d.class_grants_into(&mut grants), Ok(1));
    assert_eq!(grants[1], original[1]);
    assert_eq!(grants[0], pool.snapshot().domains[old.index].grants[0]);
    let b = d.try_acquire(1).unwrap();
    drop((a, d));
    drop(b);
    assert_eq!(pool.metrics().pending_domain_records, 1);
    pool.domain_metrics_page(0, &mut dst).unwrap();
    let s = pool.snapshot();
    for d in &dst {
        let expected = &s.domains[d.identity.index];
        assert_eq!(
            (
                d.identity,
                d.phase,
                d.admitted_bytes,
                d.allocations,
                d.owner,
                d.control_handles
            ),
            (
                expected.identity,
                expected.phase,
                expected.observed_admitted_bytes,
                expected.allocations,
                expected.owner,
                expected.handles
            )
        );
    }
    assert_eq!(
        pool.metrics().pending_domain_records,
        1,
        "sampling ran maintenance"
    );
    assert_eq!(pool.poll_domains(1).reclaimed, 1);
    let (a, d) = pool.try_open_reserved_domain(32, &[]).unwrap();
    assert_eq!(d.identity().index, old.index);
    assert_ne!(d.identity().generation, old.generation);
    pool.domain_metrics_page(old.index, &mut dst[..1]).unwrap();
    assert_eq!(dst[0].identity, d.identity());
    drop((a, d, pool));
    owner.try_destroy().unwrap();
}

#[test]
fn si09_preflight_preserves_records_claims_grants_and_error_order() {
    let mut cfg = config(2, 1);
    cfg.classes.push(ClassConfig {
        capacity: 32,
        slots: 1,
    });
    cfg.capacity_policy = CapacityPolicy::Reserved;
    cfg.handle_limit = 3; // owner + pool cannot pin two domain controls
    let mut owner = StorageOwner::prepare(&cfg).unwrap();
    let pool = owner.activate().unwrap();
    let grants = [
        ClassReservation {
            capacity: 16,
            slots: 1,
        },
        ClassReservation {
            capacity: 32,
            slots: 1,
        },
    ];
    assert_eq!(
        pool.try_open_reserved_domain(64, &grants).err(),
        Some(Error::ReferenceLimit)
    );
    assert_eq!(pool.metrics().root_claims, 2);
    assert_eq!(pool.snapshot().free_domain_records, 1);
    assert!(pool.snapshot().classes.iter().all(|c| c.reserved == 0));
    let mut invalid = grants;
    invalid[1].capacity = 33;
    assert_eq!(
        pool.try_open_reserved_domain(64, &invalid).err(),
        Some(Error::InvalidConfiguration)
    );
    invalid[1] = ClassReservation {
        capacity: 32,
        slots: 2,
    };
    assert_eq!(
        pool.try_open_reserved_domain(64, &invalid).err(),
        Some(Error::ReservationUnavailable)
    );
    owner.close().unwrap();
    invalid[1].capacity = 33;
    assert_eq!(
        pool.try_open_reserved_domain(64, &invalid).err(),
        Some(Error::InvalidConfiguration)
    );
    assert_eq!(
        pool.try_open_reserved_domain(64, &grants).err(),
        Some(Error::Closed)
    );
    drop(pool);
    owner.try_destroy().unwrap();
}

#[test]
fn si10_batch_unwind_restores_every_prefix_and_its_reserved_rights() {
    let (owner, pool, a, d) = fixture(1, 1);
    let b = d.try_acquire(16).unwrap().freeze();
    let mut dst = std::array::from_fn::<_, 4, _>(|_| None);
    for prefix in 0..=dst.len() {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut build = BatchBuild::new(&b, &mut dst).unwrap();
            for _ in 0..prefix {
                build.materialize();
            }
            panic!("SI-10 interrupt after {prefix} outputs");
        }));
        assert!(result.is_err());
        assert!(dst.iter().all(Option::is_none));
        assert_eq!(b.lease.slot().references.load(), 1);
        assert_eq!(d.metrics().admitted_bytes, 16);
        assert_eq!(pool.metrics().available_slots, 0);
    }
    drop(b);
    finish(owner, pool, a, d);
}

#[test]
fn si10_batch_survives_owner_close_remote_return_and_slot_reuse() {
    let (mut owner, pool, a, d) = fixture(1, 1);
    let mut b = d.try_acquire(16).unwrap();
    b.put_slice(b"0123456789").unwrap();
    let b = b.freeze().try_slice(2..7).unwrap();
    let mut views = std::array::from_fn::<_, 3, _>(|_| None);
    b.try_clone_into(&mut views).unwrap();
    drop(b);
    a.close();
    assert_eq!(owner.try_destroy(), Err(Error::Busy));
    for view in views.into_iter().flatten() {
        std::thread::spawn(move || {
            assert_eq!(view.as_ref(), b"23456");
            drop(view);
        })
        .join()
        .unwrap();
    }
    assert_eq!(d.metrics().admitted_bytes, 0);
    drop((a, d));
    // try_destroy closes the root; a new root exercises repeated slot reuse in
    // the shared reference/model tests rather than reopening a closed root.
    drop(pool);
    owner.try_destroy().unwrap();
}
