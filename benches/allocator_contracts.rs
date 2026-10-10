//! Primitive distributions only; TCP and tail latency stay in existing probes.
use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
macro_rules! bench_backend {
    ($name:ident,$path:path) => {
        fn $name(c: &mut Criterion) {
            use $path as b;
            let cfg = b::Config::new(
                vec![b::ClassConfig {
                    capacity: 2048,
                    slots: 8,
                }],
                1,
                1 << 20,
            );
            let mut owner = b::StorageOwner::prepare(&cfg).unwrap();
            let pool = owner.activate().unwrap();
            let (authority, domain) = pool.try_open_domain(16 * 2048).unwrap();
            let mut group = c.benchmark_group(stringify!($name));
            group.bench_function("acquire_drop", |b| {
                b.iter(|| drop(black_box(domain.try_acquire(1500).unwrap())))
            });
            group.bench_function("copy_freeze_drop", |b| {
                b.iter(|| {
                    let mut lease = domain.try_acquire(1500).unwrap();
                    lease.put_slice(black_box(&[7; 1500])).unwrap();
                    drop(black_box(lease.freeze()));
                })
            });
            let mut lease = domain.try_acquire(1500).unwrap();
            lease.put_slice(&[7; 1500]).unwrap();
            let frame = lease.freeze();
            group.bench_function("clone_drop", |b| {
                b.iter(|| drop(black_box(frame.try_clone().unwrap())))
            });
            group.bench_function("slice_drop", |b| {
                b.iter(|| drop(black_box(frame.try_slice(1..128).unwrap())))
            });
            let mut dst = [None, None, None, None];
            group.bench_function("batch_four_drop", |b| {
                b.iter(|| {
                    frame.try_clone_into(&mut dst).unwrap();
                    for x in &mut dst {
                        drop(black_box(x.take()));
                    }
                })
            });
            drop(frame);
            let held: Vec<_> = (0..8).map(|_| domain.try_acquire(1).unwrap()).collect();
            group.bench_function("class_depleted", |b| {
                b.iter(|| {
                    assert_eq!(
                        black_box(domain.try_acquire(1).err()),
                        Some(b::Error::ClassDepleted)
                    )
                })
            });
            group.finish();
            drop((held, domain, authority, pool));
            owner.try_destroy().unwrap();
        }
    };
}
bench_backend!(berry, abps_reference::berry);
bench_backend!(mosaic, abps_reference::mosaic);
criterion_group!(allocator_contracts, berry, mosaic);
criterion_main!(allocator_contracts);
