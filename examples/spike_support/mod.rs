pub mod backend;
pub mod diagnostics;
pub mod exercise;
pub mod network;
pub mod observability;
pub mod recovery;
pub mod soak;
pub mod supervisor;

use crate::perf_support::{Args, Result, metrics::Counts};
use abps_reference::{self as reference, CapacityPolicy, ClassConfig, ClassReservation, Config};
use serde_json::{Value, json};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Instant;

pub fn validate(args: &Args) -> Result<()> {
    const KEYS: &[&str] = &[
        "admission-pacing",
        "admission-rate-bytes",
        "admission-burst-bytes",
        "return-workers",
        "slow-bytes-per-second",
        "release-pattern",
        "release-period-us",
        "case",
        "active-domains",
        "fragmentation",
        "diagnostic-interval",
        "diagnostic-kind",
        "role",
        "op",
        "backend",
        "size",
        "seconds",
        "warmup",
        "threads",
        "roots",
        "classes",
        "records",
        "observe-hz",
        "latency",
        "fanout",
        "rate",
        "distribution",
        "slow",
        "overflow",
        "iterations",
        "cpus",
        "streams",
        "queue",
        "queue-backend",
        "freshness-priority",
        "freshness-min-age-ms",
        "freshness-target-ms",
        "slow-grace-ms",
        "write-stall-ms",
        "frame-dependencies",
        "keyframe-wait-ms",
        "keyframe-interval",
        "slow-profile",
        "return",
        "ports",
        "load",
        "full",
        "seed",
        "keyframe-burst",
        "source-queue",
        "generator-cpus",
        "reader-cpus",
        "write-batch",
        "ingress",
        "ingress-buffer-bytes",
        "external-memory-limit-bytes",
        "working-set",
        "backing",
        "topology",
        "capacity-policy",
        "max-frame-age-ms",
        "send-buffer-bytes",
        "slot-offset",
        "slot-stride",
        "pending-records",
        "maintenance-batch",
        "class-slots",
        "class-capacities",
        "class-profile",
        "domain-limit-bytes",
        "capacity-check",
        "fanout-api",
        "page-size",
        "setup-timeout-ms",
        "drain-timeout-ms",
        "supervisor-roots",
        "supervisor-bytes",
        "replacement-attempts",
        "preparation",
        "return-layout",
        "arrival-pattern",
        "memory-policy",
        "memory-priority",
        "memory-groups",
        "memory-correlation",
        "memory-soft-bytes",
        "elasticity",
        "root-chunk-percent",
        "root-headroom-percent",
        "memory-trace-capacity",
        "memory-pressure-ms",
        "memory-demand",
        "memory-accounting",
    ];
    for key in args.0.keys() {
        if !KEYS.contains(&key.as_str()) {
            return Err(format!("unknown argument: {key}").into());
        }
    }
    for (key, allowed, default) in [
        (
            "backend",
            &["berry", "mosaic", "abps", "bytes"][..],
            "berry",
        ),
        ("roots", &["separate", "shared"][..], "separate"),
        ("latency", &["no", "yes"][..], "no"),
        ("slow", &["no", "yes"][..], "no"),
        ("full", &["no", "yes"][..], "no"),
        (
            "capacity-policy",
            &["unreserved", "reserved"][..],
            "unreserved",
        ),
    ] {
        if !allowed.contains(&args.get(key, default)) {
            return Err(format!("invalid --{key}").into());
        }
    }
    for key in ["seconds", "warmup"] {
        if let Some(value) = args.0.get(key) {
            let n: f64 = value.parse()?;
            if !n.is_finite() || !(0.0..=3600.0).contains(&n) || (key == "seconds" && n == 0.0) {
                return Err(format!("invalid --{key}").into());
            }
        }
    }
    let max_fanout = if args.get("case", "micro").starts_with("admission-") {
        100
    } else {
        20
    };
    if !(0..=max_fanout).contains(&args.n("fanout", 20)) {
        return Err("micro fanout must be 0..20; TCP requires 1..20".into());
    }
    if ![
        "control",
        "acquire_drop",
        "cycle",
        "copy_cycle",
        "freeze",
        "final_drop",
        "fill",
        "init_fill",
        "clone",
        "slice",
        "borrowed",
        "fanout",
        "budget",
        "fanout_batch",
        "churn",
        "snapshot",
        "metrics",
        "pages",
        "too_large",
        "grants",
        "busy-destroy",
        "mixed",
    ]
    .contains(&args.get("op", "cycle"))
    {
        return Err("unsupported operation".into());
    }
    Ok(())
}

/// Linux measurement helper only, before pool preparation. No TLS/affinity API
/// is part of the allocator. Without --cpus the inherited affinity is recorded.
pub fn pin_worker(args: &Args, index: usize) -> Result<()> {
    if let Some(cpus) = args.0.get("cpus") {
        let cpus: Vec<_> = cpus.split(',').collect();
        if cpus.is_empty() {
            return Err("empty --cpus".into());
        }
        let tid = std::fs::read_link("/proc/thread-self")?;
        let output = std::process::Command::new("taskset")
            .args(["-pc", cpus[index % cpus.len()]])
            .arg(tid.file_name().ok_or("missing task id")?)
            .output()?;
        if !output.status.success() {
            return Err(String::from_utf8_lossy(&output.stderr).into_owned().into());
        }
    }
    Ok(())
}

pub fn micro(args: &Args) -> Result<Value> {
    let workers = args.n("threads", 1);
    let backend = args.get("backend", "berry");
    let shared = args.get("roots", "separate") == "shared";
    let policy = if args.get("capacity-policy", "unreserved") == "reserved" {
        CapacityPolicy::Reserved
    } else {
        CapacityPolicy::Unreserved
    };
    if !(1..=8).contains(&workers) || (shared && backend != "abps") {
        return Err("1..8 workers; shared root is reference-only".into());
    }
    let size = args.n("size", 1500);
    let count = args.n("classes", 1);
    let records = args.n("records", 64);
    if count == 0 || count > 64 || size == 0 || records < workers {
        return Err("invalid dimensions".into());
    }
    let class_size = if count == 1 { size } else { 512 };
    let classes: Vec<_> = (1..=count)
        .map(|i| ClassConfig {
            capacity: i * class_size,
            slots: 64,
        })
        .collect();
    if size > classes.last().unwrap().capacity && args.get("op", "cycle") != "oversized" {
        return Err("requested size exceeds configured classes".into());
    }
    let mut owner = None;
    let mut pool = None;
    if shared {
        let mut config = Config::new(classes.clone(), records, isize::MAX as usize);
        config.capacity_policy = policy;
        config.root_limit = reference::Reservation::for_config(&config)?.total_bytes;
        let root = reference::StorageOwner::prepare(config)?;
        pool = Some(root.activate()?);
        owner = Some(root);
    }
    let ready = Arc::new(Barrier::new(workers));
    let done = Arc::new(Barrier::new(workers));
    // Cold setup is serialized so reference try_lock failures are measured only
    // in the offered workload, not while creating the worker fixtures.
    let setup = Arc::new(Mutex::new(()));
    let shared_pools = (0..workers)
        .map(|_| pool.as_ref().map(|p| p.try_clone()).transpose())
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut tasks = Vec::new();
    for (index, shared_pool) in shared_pools.into_iter().enumerate() {
        let classes = classes
            .iter()
            .map(|c| ClassConfig {
                capacity: c.capacity,
                slots: if shared {
                    c.slots
                } else {
                    c.slots / workers + usize::from(index < c.slots % workers)
                },
            })
            .collect::<Vec<_>>();
        let ready = ready.clone();
        let done = done.clone();
        let setup = setup.clone();
        let args = args.clone();
        tasks.push(std::thread::spawn(move || -> Result<_> {
            pin_worker(&args, index)?;
            let setup_guard = setup.lock().unwrap();
            let fixture = backend::Fixture::with_policy(
                args.get("backend", "berry"),
                classes.clone(),
                if shared {
                    records
                } else {
                    records / workers + usize::from(index < records % workers)
                },
                shared_pool,
                policy,
            )?;
            let op = args.get("op", "cycle");
            let limit = if op == "budget" {
                classes
                    .iter()
                    .find(|c| c.capacity >= size)
                    .unwrap()
                    .capacity
            } else {
                classes
                    .iter()
                    .map(|c| {
                        let slots = if shared {
                            c.slots / workers + usize::from(index < c.slots % workers)
                        } else {
                            c.slots
                        };
                        c.capacity * slots
                    })
                    .sum()
            };
            let grants: Vec<_> = classes
                .iter()
                .map(|c| ClassReservation {
                    capacity: c.capacity,
                    slots: if shared {
                        c.slots / workers + usize::from(index < c.slots % workers)
                    } else {
                        c.slots
                    },
                })
                .collect();
            let origin = if policy == CapacityPolicy::Reserved {
                fixture.open_reserved(limit, &grants)?
            } else {
                fixture.open(limit)?
            };
            let exercise = exercise::Exercise::new(origin, size, op, args.n("fanout", 20));
            drop(setup_guard);
            ready.wait();
            exercise.run(args.seconds("warmup", 0.5), false);
            ready.wait();
            let started = Instant::now();
            let (counts, elapsed) = exercise.run(
                args.seconds("seconds", 2.0),
                args.get("latency", "no") == "yes",
            );
            let wall = started.elapsed();
            drop(exercise);
            done.wait();
            Ok((counts, elapsed, wall, fixture.finish()?))
        }));
    }
    let mut counts = Counts::default();
    let mut elapsed = std::time::Duration::ZERO;
    let mut drained = Vec::new();
    for task in tasks {
        let (c, d, _, s) = task.join().map_err(|_| "worker panic")??;
        counts.merge(c);
        elapsed = elapsed.max(d);
        drained.push(s);
    }
    drop(pool);
    if let Some(mut owner) = owner {
        owner.try_destroy()?;
    }
    Ok(
        json!({"metrics":counts.json(elapsed),"workers":workers,"roots":if shared {1}else{workers},
        "drained":drained,"scope":"complete cycles; root prepared within issuer worker"}),
    )
}

pub fn observe(args: &Args) -> Result<Value> {
    let fixture = backend::Fixture::new(
        args.get("backend", "berry"),
        vec![ClassConfig {
            capacity: 512,
            slots: 64,
        }],
        args.n("records", 65536),
        None,
    )?;
    let full = args.get("full", "no") == "yes";
    let page_size = args
        .0
        .get("page-size")
        .map(|n| n.parse::<usize>())
        .transpose()?;
    if page_size.is_some_and(|n| !(1..=4096).contains(&n))
        || (page_size.is_some() && (full || args.get("backend", "berry") != "berry"))
    {
        return Err("page-size requires berry backend, full=no, and 1..4096 records".into());
    }
    let mut page = vec![abps_reference::berry::DomainMetrics::default(); page_size.unwrap_or(0)];
    let mut cursor = 0;
    let origin = fixture.open(32768)?;
    let exercise = exercise::Exercise::new(origin, 512, "cycle", 1);
    exercise.run(args.seconds("warmup", 0.2), false);
    let start = Instant::now();
    let duration = args.seconds("seconds", 2.0);
    let hz = args.n("observe-hz", 100);
    let mut next = 0.;
    let mut samples = 0;
    let mut observer_ns = 0u128;
    let mut counts = Counts::default();
    while start.elapsed() < duration {
        if hz != 0 && start.elapsed().as_secs_f64() >= next {
            let at = Instant::now();
            if page_size.is_some() {
                let backend::Fixture::Berry { pool, .. } = &fixture else {
                    unreachable!()
                };
                let progress = pool.domain_metrics_page(cursor, &mut page)?;
                std::hint::black_box(&page[..progress.written]);
                cursor = if progress.done {
                    0
                } else {
                    progress.next_index
                };
            } else {
                fixture.sample(full);
            }
            observer_ns += at.elapsed().as_nanos();
            samples += 1;
            next = start.elapsed().as_secs_f64() + 1. / hz as f64;
        }
        for _ in 0..64 {
            let (r, d) = exercise.step(false);
            counts.result(r, 0, d);
        }
    }
    let elapsed = start.elapsed();
    drop(exercise);
    Ok(
        json!({"metrics":counts.json(elapsed),"observations":samples,"observer_ns":observer_ns,
        "observation":if page_size.is_some() {"bounded_domain_page"}else if args.get("backend","berry")=="berry"&&!full {"bounded_capacity_metrics"}else if args.get("backend","berry")=="mosaic"&&!full {"bounded_control_metrics"}else{"full_snapshot"},
        "page_size":page_size,
        "drained":fixture.finish()?}),
    )
}
