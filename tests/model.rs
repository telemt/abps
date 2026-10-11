#![forbid(unsafe_code)]
#[path = "support/backends.rs"]
mod backends;
#[path = "support/trace.rs"]
mod trace;
use backends::*;

#[cfg(feature = "mosaic-spike")]
#[test]
fn mosaic_large_grants_match_reference_across_reuse_and_fragmentation() {
    use abps_reference::{
        self as reference, CapacityPolicy, ClassConfig, ClassReservation, Error, mosaic,
    };
    let classes = vec![
        ClassConfig {
            capacity: 8,
            slots: 137,
        },
        ClassConfig {
            capacity: 32,
            slots: 131,
        },
    ];
    let mut rc = reference::Config::new(classes.clone(), 3, 1 << 20);
    rc.capacity_policy = CapacityPolicy::Reserved;
    let mut sc = mosaic::Config::new(classes, 3, 1 << 20);
    sc.capacity_policy = CapacityPolicy::Reserved;
    let mut r = reference::StorageOwner::prepare(rc).unwrap();
    let rp = r.activate().unwrap();
    let mut s = mosaic::StorageOwner::prepare(&sc).unwrap();
    let sp = s.activate().unwrap();
    for generation in 0..20 {
        let g = [
            ClassReservation {
                capacity: 8,
                slots: 70,
            },
            ClassReservation {
                capacity: 32,
                slots: 67,
            },
        ];
        let (ra, rd) = rp.try_open_reserved_domain(10000, &g).unwrap();
        let (sa, sd) = sp.try_open_reserved_domain(10000, &g).unwrap();
        let mut held = Vec::new();
        for (n, size) in [(70, 7), (67, 31)] {
            for _ in 0..n {
                let mut rb = rd.try_acquire(size).unwrap();
                let mut sb = sd.try_acquire(size).unwrap();
                let bytes = vec![generation as u8; size];
                rb.put_slice(&bytes).unwrap();
                sb.put_slice(&bytes).unwrap();
                held.push((rb.freeze(), sb.freeze()));
            }
            assert_eq!(rd.try_acquire(size).err(), Some(Error::DomainClassDepleted));
            assert_eq!(sd.try_acquire(size).err(), Some(Error::DomainClassDepleted));
        }
        for (rb, sb) in &held {
            assert_eq!(rb.as_ref(), sb.as_ref());
            assert_eq!(rb.backing_capacity(), sb.backing_capacity());
        }
        drop((ra, rd, sa, sd));
        drop(held);
        sp.poll_domains(3);
        let rs = rp.snapshot();
        let ss = sp.snapshot();
        assert_eq!(rs.free_domain_records, ss.free_domain_records);
        for (r, s) in rs.classes.iter().zip(ss.classes.iter()) {
            assert_eq!(
                (r.available, r.unassigned),
                (s.observed_available_slots, s.unassigned)
            );
        }
    }
    drop((rp, sp));
    r.try_destroy().unwrap();
    s.try_destroy().unwrap();
}

/// SI-09/SI-13: one sequential history with competing open failures yields one
/// answer: validation, Active gate, grant availability, record, root claims.
/// Pool clones exhaust root claims, so each backend's claim accounting
/// (allocations or controls only) cannot select a different outcome.
macro_rules! open_error_order {
    ($($api:ident)::+, $limit:ident, $prepare:expr) => {{
        use $($api)::+::{CapacityPolicy, ClassConfig, ClassReservation, Config, Error, Pool};
        fn fill(pool: &Pool) -> Vec<Pool> {
            let mut clones = Vec::new();
            loop {
                match pool.try_clone() {
                    Ok(clone) => clones.push(clone),
                    Err(e) => {
                        assert_eq!(e, Error::ReferenceLimit);
                        return clones;
                    }
                }
            }
        }
        let config = |reserved: bool, records: usize| {
            let classes = vec![ClassConfig { capacity: 16, slots: 1 }];
            let mut c = Config::new(classes, records, 1 << 20);
            c.$limit = 8;
            if reserved {
                c.capacity_policy = CapacityPolicy::Reserved;
            }
            c
        };
        let prepare = $prepare;
        let grant = |slots| [ClassReservation { capacity: 16, slots }];
        let mut errors = Vec::new();
        // Each case also exhausts root claims: 0 grant, 1 record, 2 record for
        // an empty grant, 3 claims only, 4 Closed, 5 unknown class, 6 policy
        // mismatch, 7 record pinned by a closed domain's buffer.
        for case in 0..8 {
            let reserved = !matches!(case, 1 | 6 | 7);
            let records = if matches!(case, 1 | 2 | 7) { 1 } else { 2 };
            let mut owner = prepare(config(reserved, records)).unwrap();
            let pool = owner.activate().unwrap();
            let mut held = Vec::new();
            let mut retained = None;
            match case {
                1 => held.push(pool.try_open_domain(16).unwrap()),
                2 => held.push(pool.try_open_reserved_domain(16, &[]).unwrap()),
                7 => {
                    // A closed record pinned by its buffer: pending maintenance
                    // in issuer backends, an unavailable record in the reference.
                    let (_authority, domain) = pool.try_open_domain(16).unwrap();
                    retained = Some(domain.try_acquire(1).unwrap());
                }
                _ => {}
            }
            if matches!(case, 4..=6) {
                owner.close().unwrap();
            }
            let clones = fill(&pool);
            let unknown = [ClassReservation { capacity: 17, slots: 1 }];
            errors.push(match case {
                0 | 4 => pool.try_open_reserved_domain(16, &grant(2)).err(),
                1 | 7 => pool.try_open_domain(16).err(),
                2 => pool.try_open_reserved_domain(16, &[]).err(),
                5 => pool.try_open_reserved_domain(16, &unknown).err(),
                _ => pool.try_open_reserved_domain(16, &grant(1)).err(),
            });
            drop((clones, held, retained, pool));
            owner.try_destroy().unwrap();
        }
        errors
    }};
}
#[test]
fn si09_open_error_order_matches_reference_in_issuer_backends() {
    use abps_reference::Error::*;
    let expected = [
        ReservationUnavailable,
        DomainCapacityExhausted,
        DomainCapacityExhausted,
        ReferenceLimit,
        Closed,
        InvalidConfiguration,
        InvalidConfiguration,
    ]
    .map(Some);
    let reference = open_error_order!(abps_reference, handle_limit, |c| {
        abps_reference::StorageOwner::prepare(c)
    });
    assert_eq!(reference[..7], expected);
    // Normalized maintenance difference: the reference has no pending FIFO.
    assert_eq!(reference[7], Some(DomainCapacityExhausted));
    #[cfg(feature = "berry-spike")]
    {
        let berry = open_error_order!(abps_reference::berry, handle_limit, |c| {
            abps_reference::berry::StorageOwner::prepare(&c)
        });
        assert_eq!(berry[..7], expected, "Berry open error order");
        assert_eq!(berry[7], Some(DomainMaintenanceRequired));
    }
    #[cfg(feature = "mosaic-spike")]
    {
        let mosaic = open_error_order!(abps_reference::mosaic, control_handle_limit, |c| {
            abps_reference::mosaic::StorageOwner::prepare(&c)
        });
        assert_eq!(mosaic[..7], expected, "Mosaic open error order");
        assert_eq!(mosaic[7], Some(DomainMaintenanceRequired));
    }
}

fn property_config() -> proptest::test_runner::Config {
    proptest::test_runner::Config {
        cases: if cfg!(miri) {
            3
        } else {
            std::env::var("ABPS_PROPTEST_CASES")
                .map(|v| v.parse().unwrap())
                .unwrap_or(128)
        },
        failure_persistence: if cfg!(miri) {
            None
        } else {
            Some(Box::new(
                proptest::test_runner::FileFailurePersistence::WithSource("proptest-regressions"),
            ))
        },
        ..Default::default()
    }
}
fn max_trace_bytes() -> usize {
    3 + 4 * if cfg!(miri) {
        20
    } else {
        std::env::var("ABPS_TRACE_OPERATIONS")
            .map(|v| v.parse::<usize>().unwrap().clamp(1, trace::MAX_OPERATIONS))
            .unwrap_or(250)
    }
}
fn replay_all(input: &[u8], wide: bool) {
    let trace = trace::Trace::decode(input, wide);
    let expected = reference::oracle::replay(&trace);
    #[cfg(feature = "berry-spike")]
    if trace.counts.iter().all(|&n| n <= usize::BITS as usize) {
        assert_eq!(
            berry::oracle::replay(&trace),
            expected,
            "Berry common trace"
        );
    }
    #[cfg(feature = "mosaic-spike")]
    assert_eq!(
        mosaic::oracle::replay(&trace),
        expected,
        "Mosaic common trace"
    );
    let _ = expected;
}
proptest::proptest! {
    #![proptest_config(property_config())]
    #[test]
    fn mem_own_life_same_trace_matches_all_backends(input in proptest::collection::vec(proptest::prelude::any::<u8>(), 3..max_trace_bytes())) {
        replay_all(&input, true);
    }
}
#[test]
fn verification_corpus_replays_and_can_be_selected_by_path() {
    if let Ok(path) = std::env::var("ABPS_TRACE_FILE") {
        replay_all(&std::fs::read(path).unwrap(), true);
        return;
    }
    for entry in std::fs::read_dir(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/verification/corpus/ownership"
    ))
    .unwrap()
    {
        replay_all(&std::fs::read(entry.unwrap().path()).unwrap(), true);
    }
}
