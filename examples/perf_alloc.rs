//! Separate instrumented process: never use its elapsed times for timing claims.
#![forbid(unsafe_code)]
#[allow(dead_code)]
mod perf_support;
use abps_reference::ClassConfig;
use perf_support::backend::Fixture;
use perf_support::{Args, Result, core::Exercise, tcp};
use serde_json::{Value, json};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, Stats, StatsAlloc};
use std::alloc::System;
use std::hint::black_box;

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

fn value(stats: Stats) -> Value {
    json!({"allocations": stats.allocations, "deallocations": stats.deallocations,
        "reallocations": stats.reallocations, "bytes_allocated": stats.bytes_allocated,
        "bytes_deallocated": stats.bytes_deallocated, "bytes_reallocated": stats.bytes_reallocated})
}
fn main() -> Result<()> {
    let args = Args::parse()?;
    args.validate()?;
    let control = Region::new(GLOBAL);
    drop(black_box(vec![7u8; 1234]));
    let control = control.change();
    assert!(
        control.allocations > 0 && control.deallocations > 0 && control.bytes_allocated >= 1234,
        "allocator positive control did not observe intentional allocation"
    );
    let result = if args.get("case", "core") == "tcp" {
        let region = Region::new(GLOBAL);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()?;
        let runtime_setup = region.change();
        // Fixed array: collecting phase boundaries cannot itself grow a Vec.
        let mut phases = [Stats::default(); 4];
        let mut last = GLOBAL.stats();
        let mut index = 0;
        let result = runtime.block_on(tcp::run(&args, |_| {
            let now = GLOBAL.stats();
            phases[index] = now - last;
            last = now;
            index += 1;
        }))?;
        assert_eq!(index, 4);
        drop(runtime);
        json!({"runtime_setup": value(runtime_setup),
            "adapter_preparation": value(phases[0]), "runtime_and_queues_and_core": value(phases[1]),
            "drain": value(phases[2]), "teardown_and_snapshot": value(phases[3]), "pipeline": result})
    } else {
        let size = args.n("size", 1500);
        let records = args.n("records", 64);
        let preparation = Region::new(GLOBAL);
        let fixture = Fixture::new(
            args.get("backend", "abps"),
            vec![ClassConfig {
                capacity: size,
                slots: 64,
            }],
            records,
        )?;
        let origin = fixture.open(size * 64)?;
        let exercise = Exercise::new(origin, size, args.get("op", "cycle"), args.n("fanout", 20));
        let prepared = preparation.change();
        // Warm malloc bins and library internals outside the measured region.
        for _ in 0..100 {
            exercise.step(false).0?;
        }
        let region = Region::new(GLOBAL);
        let iterations = args.n("iterations", 10_000);
        for _ in 0..iterations {
            exercise.step(false).0?;
        }
        let measured = region.change();
        drop(exercise);
        let origin = fixture.open(size * 64)?;
        let frame = origin.acquire(size)?.freeze();
        let first_return = std::thread::spawn(move || {
            // Thread creation/runtime setup precedes this region. Only the
            // return on a previously uninvolved thread is attributed to core.
            let region = Region::new(GLOBAL);
            drop(frame);
            region.change()
        })
        .join()
        .unwrap();
        drop(origin);
        let snapshot = if args.get("backend", "abps") == "abps" {
            let observer = fixture.observer();
            let region = Region::new(GLOBAL);
            drop(black_box(observer.snapshot()));
            let stats = region.change();
            drop(observer);
            Some(value(stats))
        } else {
            None
        };
        json!({"preparation_including_sources": value(prepared), "iterations": iterations,
            "steady_core": value(measured), "first_thread_return": value(first_return),
            "one_snapshot": snapshot, "drained": fixture.finish()?})
    };
    println!(
        "{}",
        json!({"args": args.0, "positive_control": value(control), "result": result})
    );
    Ok(())
}
