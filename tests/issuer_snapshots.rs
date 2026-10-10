//! AP-01/05, OWN-03: observations may overlap remote atomic transitions, but
//! never another issuer's origin write. No equality across concurrent samples.
#![forbid(unsafe_code)]
#![cfg(any(feature = "berry-spike", feature = "mosaic-spike"))]

macro_rules! snapshot_test {
    ($name:ident, $backend:path) => {
        mod $name {
            use std::sync::Barrier;
            use $backend as b;

            #[test]
            fn remote_freeze_clone_drop_then_issuer_reuse() {
                let config = b::Config::new(
                    vec![b::ClassConfig {
                        capacity: 16,
                        slots: 1,
                    }],
                    1,
                    1 << 20,
                );
                let mut owner = b::StorageOwner::prepare(&config).unwrap();
                let pool = owner.activate().unwrap();
                let mut previous = None;
                for incarnation in 0..if cfg!(miri) { 3 } else { 16 } {
                    let (authority, domain) = pool.try_open_domain(16).unwrap();
                    let identity = domain.identity();
                    if let Some(old) = previous {
                        assert_ne!(identity, old);
                    }
                    previous = Some(identity);
                    let mut lease = domain.try_acquire(1).unwrap();
                    lease.put_slice(&[incarnation; 16]).unwrap();
                    let initial = pool.snapshot();
                    assert_eq!(initial.slots[0].origin, Some(identity));
                    assert_eq!(initial.slots[0].ownership, b::OwnershipKind::Mutable);
                    let gate = Barrier::new(2);
                    std::thread::scope(|scope| {
                        let gate_ref = &gate;
                        scope.spawn(move || {
                            gate_ref.wait();
                            let frame = lease.freeze();
                            for _ in 0..8 {
                                let other = frame.try_clone().unwrap();
                                let view = other.try_slice(2..7).unwrap();
                                std::thread::yield_now();
                                assert_eq!(view.as_ref(), &[incarnation; 5]);
                                drop((other, view));
                            }
                            drop(frame);
                        });
                        gate.wait();
                        for _ in 0..8 {
                            let sample = pool.snapshot();
                            assert!(sample.slots[0].origin.is_none_or(|key| key == identity));
                            assert!(sample.slots[0].references <= 3);
                            assert!(sample.classes[0].observed_available_slots <= 1);
                            assert!(sample.classes[0].observed_unpublished_slots <= 1);
                            let domain = sample
                                .domains
                                .iter()
                                .find(|d| d.identity == identity)
                                .unwrap();
                            assert!(domain.observed_admitted_bytes <= 16);
                            assert!(domain.observed_unpublished_backing_bytes <= 16);
                            std::thread::yield_now();
                        }
                    });
                    // Join gives quiescence only here. Exact comparisons and the
                    // next issuer-side incarnation must not precede this boundary.
                    let drained = pool.snapshot();
                    assert_eq!(drained.classes[0].observed_available_slots, 1);
                    assert_eq!(drained.slots[0].origin, None);
                    assert_eq!(drained.slots[0].references, 0);
                    let domain_sample = drained
                        .domains
                        .iter()
                        .find(|d| d.identity == identity)
                        .unwrap();
                    assert_eq!(domain_sample.observed_admitted_bytes, 0);
                    assert_eq!(domain_sample.observed_unpublished_backing_bytes, 0);
                    drop((domain, authority));
                    assert_eq!(pool.snapshot().free_domain_records, 1);
                }
                drop(pool);
                owner.try_destroy().unwrap();
            }
        }
    };
}

#[cfg(feature = "berry-spike")]
snapshot_test!(berry, abps_reference::berry);
#[cfg(feature = "mosaic-spike")]
snapshot_test!(mosaic, abps_reference::mosaic);
