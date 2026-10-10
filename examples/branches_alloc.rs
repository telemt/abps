//! BR-04 allocation assertions, separate from timing. GlobalAlloc hooks never panic.
#![forbid(unsafe_code)]
#[allow(dead_code)]
mod perf_support;
use abps_reference::mosaic::*;
use perf_support::{Args, Result};
use serde_json::json;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, Stats, StatsAlloc};
use std::{alloc::System, hint::black_box};
#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;
fn zero(s: Stats) {
    assert_eq!((s.allocations, s.deallocations, s.reallocations), (0, 0, 0));
}
fn main() -> Result<()> {
    let args = Args::parse()?;
    let control = Region::new(GLOBAL);
    drop(black_box(vec![1u8; 4096]));
    let control = control.change();
    assert!(control.allocations > 0 && control.deallocations > 0);
    let mut c = Config::new(
        vec![ClassConfig {
            capacity: 64,
            slots: 2,
        }],
        1,
        1 << 20,
    );
    c.reference_limit = 2;
    c.local_branches = Some(LocalBranchConfig {
        max_branches_per_slot: 2,
        local_reference_limit: 3,
    });
    let mut owner = StorageOwner::prepare(&c)?;
    let p = owner.activate()?;
    let (authority, d) = p.try_open_domain(128)?;
    let region = Region::new(GLOBAL);
    for _ in 0..args.n("iterations", 1000) {
        let mut b = d.try_acquire(4)?;
        b.put_slice(b"ABPS")?;
        let e = b.freeze_branches_into(&mut [None, None, None]).unwrap_err();
        assert_eq!(e.reason(), Error::BranchCapacityExceeded);
        let mut out = [None, None];
        e.into_parts()
            .1
            .freeze_branches_into(&mut out)
            .map_err(|e| e.reason())?;
        let local = out[0].take().unwrap().into_local();
        let a = local.try_slice(1..3)?;
        let b = a.try_clone()?;
        assert_eq!(local.try_clone().err(), Some(Error::ReferenceLimit));
        assert_eq!(
            local.try_slice(0..usize::MAX).err(),
            Some(Error::InvalidRange)
        );
        let local = local.try_into_branch().unwrap_err();
        black_box(local.try_view(0..4)?);
        drop((a, b));
        drop(black_box(local.try_into_branch().unwrap()));
        drop(out);
    }
    let hot = region.change();
    zero(hot);
    let mut out = [None];
    d.try_acquire(1)?
        .freeze_branches_into(&mut out)
        .map_err(|e| e.reason())?;
    let branch = out[0].take().unwrap();
    let remote = std::thread::spawn(move || {
        // Thread/runtime startup is outside the measured first return.
        let region = Region::new(GLOBAL);
        let local = branch.into_local();
        let clone = local.try_clone().unwrap();
        drop((local, clone));
        region.change()
    })
    .join()
    .unwrap();
    zero(remote);
    assert_eq!(d.metrics().admitted_bytes, 0);
    let reservation = owner.reservation()?;
    drop((d, authority, p));
    owner.try_destroy()?;
    println!(
        "{}",
        json!({"control_allocations":control.allocations,"control_frees":control.deallocations,
        "hot_allocations":hot.allocations,"hot_frees":hot.deallocations,"hot_reallocations":hot.reallocations,
        "first_remote_allocations":remote.allocations,"first_remote_frees":remote.deallocations,
        "first_remote_reallocations":remote.reallocations,"metadata_bytes":reservation.metadata_bytes,"drained":true})
    );
    Ok(())
}
