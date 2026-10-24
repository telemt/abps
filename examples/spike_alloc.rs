//! No timing claims from this process: stats_alloc instruments allocator calls.
#![forbid(unsafe_code)]
#[allow(dead_code)]
mod perf_support;
#[allow(dead_code)]
mod spike_support;
use abps_reference::{ClassConfig, Error, berry};
use perf_support::{Args, Result};
use serde_json::{Value, json};
use spike_support::{backend::Fixture, exercise::Exercise};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, Stats, StatsAlloc};
use std::alloc::System;
use std::hint::black_box;
#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;
fn value(s: Stats) -> Value {
    json!({"allocations":s.allocations,"deallocations":s.deallocations,
    "reallocations":s.reallocations,"bytes_allocated":s.bytes_allocated,"bytes_deallocated":s.bytes_deallocated})
}
fn zero(s: Stats) {
    assert_eq!((s.allocations, s.deallocations, s.reallocations), (0, 0, 0));
}
fn revisions(args: &Args) -> Result<Value> {
    let classes = args.n("classes", 64);
    let records = args.n("records", 65536);
    let page_size = args.n("page-size", 32);
    if !(1..=64).contains(&classes)
        || !(1..=65536).contains(&records)
        || !(1..=4096).contains(&page_size)
    {
        return Err("invalid revision probe dimensions".into());
    }
    let mut c = abps_reference::Config::new(
        (0..classes)
            .map(|i| ClassConfig {
                capacity: 64 + i,
                slots: 64,
            })
            .collect(),
        records,
        usize::MAX,
    );
    c.capacity_policy = abps_reference::CapacityPolicy::Reserved;
    c.reference_limit = 4;
    let grants: Vec<_> = c
        .classes
        .iter()
        .map(|c| abps_reference::ClassReservation {
            capacity: c.capacity,
            slots: 1,
        })
        .collect();
    let mut owner = berry::StorageOwner::prepare(&c)?;
    let pool = owner.activate()?;
    let (a, d) = pool.try_open_reserved_domain(1 << 20, &grants)?;
    let b = d.try_acquire(1)?.freeze();
    let mut page = vec![berry::DomainMetrics::default(); page_size];
    let mut grant_samples = vec![
        abps_reference::ClassGrantSnapshot {
            capacity: 0,
            reserved: 0,
            available: 0
        };
        classes
    ];
    let mut views: [Option<berry::Buf>; 3] = std::array::from_fn(|_| None);
    let mut cursor = 0;
    let region = Region::new(GLOBAL);
    for _ in 0..1000 {
        let progress = pool.domain_metrics_page(cursor, &mut page)?;
        black_box(&page[..progress.written]);
        cursor = if progress.done {
            0
        } else {
            progress.next_index
        };
        d.class_grants_into(&mut grant_samples)?;
        b.try_clone_into(&mut views)?;
        assert_eq!(b.try_clone_into(&mut views), Err(Error::OutputNotEmpty));
        assert_eq!(b.try_clone_into(&mut [None]), Err(Error::ReferenceLimit));
        b.try_clone_into(&mut [])?;
        for v in &mut views {
            drop(black_box(v.take()));
        }
    }
    let measured = region.change();
    zero(measured);
    drop((b, a, d, pool));
    owner.try_destroy()?;
    Ok(
        json!({"classes":classes,"records":records,"page_size":page_size,"calls":1000,"allocations":value(measured)}),
    )
}
fn main() -> Result<()> {
    if std::env::args().any(|a| a == "--case=admission-replay") {
        let args = Args::parse()?;
        spike_support::validate(&args)?;
        println!(
            "{}",
            json!({"args":args.0,"result":spike_support::network::admission::allocation_probe(GLOBAL)})
        );
        return Ok(());
    }

    let args = Args::parse()?;
    spike_support::validate(&args)?;
    let control = Region::new(GLOBAL);
    drop(black_box(vec![7u8; 1234]));
    let control = control.change();
    assert!(
        control.allocations > 0 && control.deallocations > 0 && control.bytes_allocated >= 1234
    );
    #[cfg(feature = "mosaic-spike")]
    if args.get("case", "core") == "memory-policy" {
        let probe = spike_support::network::memory_allocation_probe(GLOBAL, &args)?;
        println!(
            "{}",
            json!({"positive_control":value(control),"memory_policy":probe})
        );
        return Ok(());
    }
    #[cfg(feature = "mosaic-spike")]
    if args.get("case", "core") == "staged-ingress" {
        let probe =
            spike_support::network::staged_allocation_probe(GLOBAL, args.n("iterations", 10000))?;
        println!(
            "{}",
            json!({"positive_control":value(control),"staged_ingress":probe})
        );
        return Ok(());
    }
    #[cfg(feature = "local-branches-spike")]
    if args.get("case", "core") == "worker-fanout" {
        let probe = spike_support::network::worker_allocation_probe(GLOBAL, &args)?;
        println!(
            "{}",
            json!({"positive_control":value(control),"worker_fanout":probe})
        );
        return Ok(());
    }
    #[cfg(feature = "mosaic-spike")]
    if matches!(args.get("case", "core"), "local-queue" | "adaptive-queue") {
        let probe = spike_support::network::local_queue_allocation_probe(
            GLOBAL,
            args.n("iterations", 10000),
            args.get("case", "core") == "adaptive-queue",
        )?;
        println!(
            "{}",
            json!({"positive_control":value(control),"local_queue":probe})
        );
        return Ok(());
    }
    if args.get("case", "core") != "core" {
        return Err("unknown or disabled allocation probe case".into());
    }
    let size = args.n("size", 1500);
    let backend = args.get("backend", "berry");
    let setup = Region::new(GLOBAL);
    let fixture = Fixture::prepared(
        backend,
        vec![ClassConfig {
            capacity: size,
            slots: 64,
        }],
        64,
        None,
        abps_reference::CapacityPolicy::Unreserved,
        args.get("preparation", "lazy") == "zero-payload",
    )?;
    let origin = fixture.open(size * 64)?;
    let exercise = Exercise::new(origin, size, args.get("op", "cycle"), args.n("fanout", 20));
    let preparation = setup.change();
    for _ in 0..100 {
        exercise.step(false).0?;
    }
    let region = Region::new(GLOBAL);
    for _ in 0..args.n("iterations", 10000) {
        exercise.step(false).0?;
    }
    let measured = region.change();
    drop(exercise);
    let origin = fixture.open(size * 64)?;
    let frame = origin.acquire(size)?.freeze();
    let remote = std::thread::spawn(move || {
        let region = Region::new(GLOBAL);
        drop(frame);
        region.change()
    })
    .join()
    .unwrap();
    drop(origin);
    if matches!(backend, "berry" | "mosaic") {
        zero(measured);
        zero(remote);
    }
    let drained = fixture.finish()?;
    // Distinct failure/control paths, after preparing the limited fixture.
    let mut config = abps_reference::Config::new(
        vec![ClassConfig {
            capacity: 16,
            slots: 1,
        }],
        2,
        1 << 20,
    );
    config.reference_limit = 1;
    let mut owner = berry::StorageOwner::prepare(&config)?;
    let pool = owner.activate()?;
    let (authority, domain) = pool.try_open_domain(16)?;
    let (other_owner, other) = pool.try_open_domain(32)?;
    let retained = domain.try_acquire(16)?.freeze();
    let failures = Region::new(GLOBAL);
    assert!(matches!(domain.try_acquire(1), Err(Error::BudgetExceeded)));
    assert!(matches!(other.try_acquire(1), Err(Error::ClassDepleted)));
    assert!(matches!(
        domain.try_acquire(usize::MAX),
        Err(Error::TooLarge)
    ));
    assert!(matches!(domain.try_acquire(0), Err(Error::InvalidSize)));
    assert!(matches!(retained.try_clone(), Err(Error::ReferenceLimit)));
    assert!(matches!(retained.try_slice(1..2), Err(Error::InvalidRange)));
    authority.close();
    assert!(matches!(domain.try_acquire(1), Err(Error::Closed)));
    drop(retained);
    let failures = failures.change();
    zero(failures);
    drop((domain, authority, other, other_owner));
    let maintenance = Region::new(GLOBAL);
    let (authority, domain) = pool.try_open_domain(16)?;
    let buffer = domain.try_acquire(1)?;
    drop((authority, domain));
    assert_eq!(pool.poll_domains(1).reclaimed, 0);
    drop(buffer);
    assert_eq!(pool.poll_domains(1).reclaimed, 1);
    let (authority, domain) = pool.try_open_domain(16)?;
    #[cfg(feature = "bytes-interop")]
    {
        use bytes::BufMut as _;
        let mut b = domain.try_acquire(16)?;
        (&mut b).limit(5).put_slice(b"hello");
        assert_eq!(b.as_ref(), b"hello");
    }
    drop((domain, authority));
    let maintenance = maintenance.change();
    zero(maintenance);
    drop(pool);
    owner.try_destroy()?;
    let mut reserved_config = abps_reference::Config::new(
        vec![ClassConfig {
            capacity: 16,
            slots: 2,
        }],
        2,
        1 << 20,
    );
    reserved_config.capacity_policy = abps_reference::CapacityPolicy::Reserved;
    let mut owner = berry::StorageOwner::prepare(&reserved_config)?;
    let pool = owner.activate()?;
    let grants = [abps_reference::ClassReservation {
        capacity: 16,
        slots: 1,
    }];
    let region = Region::new(GLOBAL);
    for _ in 0..1000 {
        let (a, da) = pool.try_open_reserved_domain(32, &grants)?;
        let (b, db) = pool.try_open_reserved_domain(32, &grants)?;
        assert!(matches!(
            pool.try_open_reserved_domain(32, &grants),
            Err(Error::ReservationUnavailable)
        ));
        let buffer = da.try_acquire(1)?.freeze();
        assert!(matches!(da.try_acquire(1), Err(Error::DomainClassDepleted)));
        let view = buffer.try_slice(0..0)?;
        drop((buffer, a, da));
        assert_eq!(pool.poll_domains(32).reclaimed, 0);
        drop(view);
        assert_eq!(pool.poll_domains(32).reclaimed, 1);
        drop((b, db));
    }
    let reserved_paths = region.change();
    zero(reserved_paths);
    let (a, da) = pool.try_open_reserved_domain(32, &grants)?;
    let buffer = da.try_acquire(1)?.freeze();
    drop((a, da));
    let reserved_remote = std::thread::spawn(move || {
        let region = Region::new(GLOBAL);
        drop(buffer);
        region.change()
    })
    .join()
    .unwrap();
    zero(reserved_remote);
    assert_eq!(pool.poll_domains(32).reclaimed, 1);
    drop(pool);
    owner.try_destroy()?;
    println!(
        "{}",
        json!({"args":args.0,"positive_control":value(control),"result":{
        "preparation":value(preparation),"steady_core":value(measured),"first_thread_return":value(remote),
        "berry_failure_paths":value(failures),"maintenance_and_interop":value(maintenance),
        "reserved_paths":value(reserved_paths),"reserved_first_thread_return":value(reserved_remote),
        "si07_si10":revisions(&args)?,"mosaic":mosaic_paths()?,"drained":drained}})
    );
    Ok(())
}

#[cfg(not(feature = "mosaic-spike"))]
fn mosaic_paths() -> Result<Value> {
    Ok(Value::Null)
}
#[cfg(feature = "mosaic-spike")]
fn mosaic_paths() -> Result<Value> {
    use abps_reference::{CapacityPolicy, ClassReservation, mosaic};
    let mut config = mosaic::Config::new(
        vec![ClassConfig {
            capacity: 16,
            slots: 65,
        }],
        4,
        usize::MAX,
    );
    config.capacity_policy = CapacityPolicy::Reserved;
    config.reference_limit = 4;
    let mut owner = mosaic::StorageOwner::prepare(&config)?;
    let pool = owner.activate()?;
    let grant = [ClassReservation {
        capacity: 16,
        slots: 65,
    }];
    let mut slots: Vec<Option<mosaic::BufMut>> = (0..65).map(|_| None).collect();
    let mut page = [mosaic::DomainMetrics::default(); 2];
    let mut words = [mosaic::BitmapMetrics::default(); 1];
    let region = Region::new(GLOBAL);
    for _ in 0..100 {
        let (a, d) = pool.try_open_reserved_domain(16 * 66, &grant)?;
        for slot in &mut slots {
            *slot = Some(d.try_acquire(1)?);
        }
        assert_eq!(d.try_acquire(1).err(), Some(Error::DomainClassDepleted));
        assert_eq!(d.try_acquire(0).err(), Some(Error::InvalidSize));
        assert_eq!(d.try_acquire(usize::MAX).err(), Some(Error::TooLarge));
        for slot in &mut slots {
            drop(slot.take());
        }
        let b = d.try_acquire(1)?.freeze();
        black_box(b.try_view(0..0)?);
        let mut views = [None, None, None];
        b.try_clone_into(&mut views)?;
        assert_eq!(b.try_clone_into(&mut [None]), Err(Error::ReferenceLimit));
        assert_eq!(b.try_clone_into(&mut views), Err(Error::OutputNotEmpty));
        drop(views);
        b.try_clone_into(&mut [])?;
        let mut one = [None];
        b.try_clone_into(&mut one)?;
        drop(one);
        pool.domain_metrics_page(0, &mut page)?;
        pool.bitmap_metrics_page(0, &mut words)?;
        a.close();
        assert_eq!(d.try_acquire(1).err(), Some(Error::Closed));
        drop((a, d));
        assert_eq!(pool.poll_domains(32).reclaimed, 0);
        drop(b);
        assert_eq!(pool.poll_domains(32).reclaimed, 1);
    }
    let measured = region.change();
    zero(measured);
    let (a, d) = pool.try_open_reserved_domain(16, &grant)?;
    let b = d.try_acquire(1)?.freeze();
    let errors = Region::new(GLOBAL);
    assert_eq!(d.try_acquire(1).err(), Some(Error::BudgetExceeded));
    let errors = errors.change();
    zero(errors);
    drop((a, d));
    let remote = std::thread::spawn(move || {
        let r = Region::new(GLOBAL);
        drop(b);
        r.change()
    })
    .join()
    .unwrap();
    zero(remote);
    assert_eq!(pool.poll_domains(32).reclaimed, 1);
    drop(pool);
    owner.try_destroy()?;
    Ok(
        json!({"prepared_paths":value(measured),"budget_error":value(errors),"first_remote_return":value(remote)}),
    )
}
