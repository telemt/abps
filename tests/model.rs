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
