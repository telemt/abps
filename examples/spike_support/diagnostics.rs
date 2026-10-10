//! Permanent probes for the dimensions hidden by immediate one-slot reuse.
use super::{
    backend::{Fixture, Frame},
    pin_worker,
};
use crate::perf_support::{Args, Result, metrics::Latency};
use abps_reference::ClassConfig;
use serde_json::{Value, json};
use std::{
    hint::black_box,
    sync::{Arc, Barrier},
    time::Instant,
};

pub fn working_set(args: &Args) -> Result<Value> {
    pin_worker(args, 0)?;
    let size = args.n("size", 2 * 1024 * 1024);
    let working = args.n("working-set", 64);
    if size == 0 || size > 16 * 1024 * 1024 || !(1..=64).contains(&working) {
        return Err("invalid working set".into());
    }
    let fixture = Fixture::new(
        args.get("backend", "berry"),
        vec![ClassConfig {
            capacity: size,
            slots: 64,
        }],
        1,
        None,
    )?;
    let origin = fixture.open(size * 64)?;
    let payload = vec![0x5a; size];
    let mut frames = Vec::with_capacity(working);
    for _ in 0..working {
        let mut b = origin.acquire(size)?;
        b.put(&payload);
        frames.push(Some(b.freeze()));
    }
    let initialize = args.get("op", "fill") == "init_fill";
    let mut iterations = 0u64;
    let mut elapsed = std::time::Duration::ZERO;
    let mut latency = Latency::default();
    for measured in [false, true] {
        let duration = args.seconds(
            if measured { "seconds" } else { "warmup" },
            if measured { 1.0 } else { 0.15 },
        );
        let start = Instant::now();
        let mut count = 0;
        while start.elapsed() < duration {
            let then = Instant::now();
            let i = count % working;
            drop(frames[i].take());
            let mut buffer = origin.acquire(size)?;
            if initialize {
                buffer.initialize(size).copy_from_slice(black_box(&payload));
                buffer.set_filled(size);
            } else {
                buffer.put(black_box(&payload));
            }
            frames[i] = Some(black_box(buffer.freeze()));
            count += 1;
            if measured && args.get("latency", "no") == "yes" {
                latency.record(then.elapsed());
            }
        }
        if measured {
            elapsed = start.elapsed();
            iterations = count as u64;
        }
    }
    for frame in frames.into_iter().flatten() {
        assert!(frame.as_ref().iter().all(|b| *b == 0x5a));
    }
    drop(origin);
    Ok(
        json!({"working_set":working,"bytes_in_rotation":working*size,"operations":iterations,
        "measurement_seconds":elapsed.as_secs_f64(),"accepted_per_s":iterations as f64/elapsed.as_secs_f64(),
        "payload_GB_s":iterations as f64*size as f64/elapsed.as_secs_f64()/1e9,"latency":latency.json(),"drained":fixture.finish()?}),
    )
}
pub fn churn(args: &Args) -> Result<Value> {
    pin_worker(args, 0)?;
    let records = args.n("records", 4096);
    if !(1..=65536).contains(&records) {
        return Err("records must be 1..65536".into());
    }
    let fixture = Fixture::new(
        args.get("backend", "berry"),
        vec![ClassConfig {
            capacity: 64,
            slots: 64,
        }],
        records,
        None,
    )?;
    let mut domains = Vec::with_capacity(records);
    for _ in 0..records {
        domains.push(fixture.open(64)?);
    }
    let mut pair = domains.pop().unwrap();
    for _ in 0..100 {
        drop(pair);
        pair = fixture.open(64)?;
    }
    let mut latency = Latency::default();
    let count = args.n("iterations", 10000);
    let start = Instant::now();
    for _ in 0..count {
        let then = Instant::now();
        drop(pair);
        pair = black_box(fixture.open(64)?);
        latency.record(then.elapsed());
    }
    let elapsed = start.elapsed();
    drop((pair, domains));
    Ok(
        json!({"records":records,"operations":count,"measurement_seconds":elapsed.as_secs_f64(),
        "accepted_per_s":count as f64/elapsed.as_secs_f64(),"latency":latency.json(),"drained":fixture.finish()?}),
    )
}
pub fn concurrent_views(args: &Args) -> Result<Value> {
    let n = args.n("threads", 4);
    if !(1..=8).contains(&n) {
        return Err("threads must be 1..8".into());
    }
    let same = match args.get("backing", "same") {
        "same" => true,
        "distinct" => false,
        _ => return Err("backing must be same or distinct".into()),
    };
    let offset = args.n("slot-offset", 0);
    let stride = args.n("slot-stride", 1);
    if stride == 0
        || offset
            .checked_add((n - 1).checked_mul(stride).ok_or("slot stride overflow")?)
            .is_none_or(|i| i >= 64)
    {
        return Err("selected slots must fit the prepared 64-slot class".into());
    }
    let fixture = Fixture::new(
        args.get("backend", "berry"),
        vec![ClassConfig {
            capacity: 1500,
            slots: 64,
        }],
        1,
        None,
    )?;
    let origin = fixture.open(1500 * 64)?;
    let mut all: Vec<_> = (0..64)
        .map(|_| origin.acquire(1500).map(|b| Some(b.freeze())))
        .collect::<std::result::Result<_, _>>()?;
    let mut frames: Vec<Frame> = Vec::with_capacity(n);
    for i in 0..n {
        if i > 0 && same {
            frames.push(frames[0].view(0..0)?);
        } else {
            frames.push(all[offset + i * stride].take().unwrap());
        }
    }
    drop(all);
    let barrier = Arc::new(Barrier::new(n));
    let mut threads = Vec::new();
    for (i, frame) in frames.into_iter().enumerate() {
        let barrier = barrier.clone();
        let args = args.clone();
        threads.push(std::thread::spawn(move || -> Result<_> {
            pin_worker(&args, i)?;
            let mut good = 0u64;
            let mut contended = 0u64;
            let mut elapsed = std::time::Duration::ZERO;
            for measured in [false, true] {
                barrier.wait();
                let start = Instant::now();
                let duration = args.seconds(
                    if measured { "seconds" } else { "warmup" },
                    if measured { 1.0 } else { 0.15 },
                );
                while start.elapsed() < duration {
                    for _ in 0..256 {
                        match frame.view(0..0) {
                            Ok(view) => {
                                drop(black_box(view));
                                if measured {
                                    good += 1;
                                }
                            }
                            Err(abps_reference::Error::Contended) => {
                                if measured {
                                    contended += 1;
                                }
                            }
                            Err(error) => return Err(error.into()),
                        }
                    }
                }
                if measured {
                    elapsed = start.elapsed();
                }
            }
            barrier.wait(); // final-drop traffic stays outside every timed window
            Ok((good, contended, elapsed))
        }));
    }
    let mut accepted = 0u64;
    let mut rejected = 0u64;
    let mut elapsed = std::time::Duration::ZERO;
    for task in threads {
        let (a, r, t) = task.join().map_err(|_| "clone worker panicked")??;
        accepted += a;
        rejected += r;
        elapsed = elapsed.max(t);
    }
    drop(origin);
    Ok(
        json!({"slot_offset":offset,"slot_stride":stride,"accepted":accepted,"contended":rejected,"measurement_seconds":elapsed.as_secs_f64(),
        "accepted_per_s":accepted as f64/elapsed.as_secs_f64(),"failure_fraction":rejected as f64/(accepted+rejected).max(1) as f64,"drained":fixture.finish()?}),
    )
}

/// Caller-driven maintenance latency with busy records ahead of a ready tail.
/// Each poll remains bounded; the probe reports the total work across retries.
pub fn pending_domains(args: &Args) -> Result<Value> {
    if args.get("backend", "berry") != "berry" {
        return Err("pending-domains is a Berry-only probe".into());
    }
    use abps_reference::{Config, Error, berry};
    pin_worker(args, 0)?;
    let busy = args.n("pending-records", 4095);
    let batch = args.n("maintenance-batch", 32);
    let episodes = args.n("iterations", 25);
    if busy > 4095 || batch == 0 || episodes == 0 {
        return Err("invalid pending maintenance dimensions".into());
    }
    let mut samples = Latency::default();
    let mut poll_samples = Latency::default();
    let mut total_polls = 0;
    let mut total_examined = 0;
    for _ in 0..episodes {
        let classes = (0..=busy / 64)
            .map(|i| ClassConfig {
                capacity: i + 1,
                slots: 64,
            })
            .collect();
        let mut owner = berry::StorageOwner::prepare(&Config::new(classes, busy + 1, 1 << 24))?;
        let pool = owner.activate()?;
        let mut held = Vec::with_capacity(busy);
        for i in 0..busy {
            let capacity = i / 64 + 1;
            let (a, d) = pool.try_open_domain(capacity)?;
            held.push(d.try_acquire(capacity)?);
            drop((a, d));
        }
        let capacity = busy / 64 + 1;
        let (a, d) = pool.try_open_domain(capacity)?;
        let b = d.try_acquire(capacity)?;
        drop((a, d));
        drop(b);
        assert!(matches!(
            pool.try_open_domain(1),
            Err(Error::DomainMaintenanceRequired)
        ));
        let start = Instant::now();
        loop {
            let at = Instant::now();
            let m = pool.poll_domains(batch);
            poll_samples.record(at.elapsed());
            assert!(m.examined <= batch);
            total_polls += 1;
            total_examined += m.examined;
            if m.reclaimed != 0 {
                break;
            }
        }
        let reopened = pool.try_open_domain(1)?;
        samples.record(start.elapsed());
        drop((reopened, held));
        pool.poll_domains(busy + 1);
        assert_eq!(pool.metrics().pending_domain_records, 0);
        assert_eq!(pool.metrics().available_slots, pool.metrics().total_slots);
        drop(pool);
        owner.try_destroy()?;
    }
    Ok(
        json!({"busy_records":busy,"ready_tail":1,"batch":batch,"episodes":episodes,
        "total_polls":total_polls,"total_examined":total_examined,"complete_retry_latency":samples.json(),"poll_latency":poll_samples.json()}),
    )
}

/// SI-07/09 permanent dimensions for preparation-independent API comparisons.
pub fn public_api(args: &Args) -> Result<Value> {
    #[cfg(feature = "mosaic-spike")]
    if args.get("backend", "berry") == "mosaic" {
        return public_api_mosaic(args);
    }
    use abps_reference::{CapacityPolicy, ClassReservation, Config, berry};
    pin_worker(args, 0)?;
    let classes = args.n("classes", 64);
    let records = args.n("records", 65536);
    let count = args.n("iterations", 1000);
    let page_size = args.n("page-size", 32);
    if args.get("backend", "berry") != "berry"
        || !(1..=64).contains(&classes)
        || !(1..=65536).contains(&records)
        || count == 0
        || !(1..=4096).contains(&page_size)
    {
        return Err("invalid public-api dimensions".into());
    }
    let mut config = Config::new(
        (0..classes)
            .map(|i| ClassConfig {
                capacity: 64 + i,
                slots: 64,
            })
            .collect(),
        records,
        usize::MAX,
    );
    let reserved = args.get("capacity-policy", "unreserved") == "reserved";
    if reserved {
        config.capacity_policy = CapacityPolicy::Reserved;
    }
    let grants: Vec<_> = config
        .classes
        .iter()
        .map(|c| ClassReservation {
            capacity: c.capacity,
            slots: 1,
        })
        .collect();
    let mut owner = berry::StorageOwner::prepare(&config)?;
    let pool = owner.activate()?;
    let mut page = vec![berry::DomainMetrics::default(); page_size];
    let mut cursor = 0;
    let mut latency = Latency::default();
    let op = args.get("op", "churn");
    let mut one = || -> Result<()> {
        match op {
            "churn" => {
                let d = if reserved {
                    pool.try_open_reserved_domain(1 << 20, &grants)
                } else {
                    pool.try_open_domain(1 << 20)
                }?;
                drop(black_box(d));
            }
            "metrics" => {
                black_box(pool.metrics());
            }
            "snapshot" => drop(black_box(pool.snapshot())),
            "pages" => {
                let p = pool.domain_metrics_page(cursor, &mut page)?;
                black_box(&page[..p.written]);
                cursor = if p.done { 0 } else { p.next_index };
            }
            _ => return Err("public-api op must be churn, metrics, snapshot, or pages".into()),
        }
        Ok(())
    };
    for _ in 0..count.min(20) {
        one()?;
    }
    let start = Instant::now();
    for _ in 0..count {
        let at = Instant::now();
        one()?;
        latency.record(at.elapsed());
    }
    let elapsed = start.elapsed();
    assert_eq!(pool.metrics().available_slots, classes * 64);
    drop(pool);
    owner.try_destroy()?;
    Ok(
        json!({"classes":classes,"records":records,"page_size":page_size,"op":op,"operations":count,
        "latency":latency.json(),"measurement_seconds":elapsed.as_secs_f64(),"drained":true}),
    )
}

#[cfg(feature = "mosaic-spike")]
fn public_api_mosaic(args: &Args) -> Result<Value> {
    use abps_reference::mosaic as berry;
    use abps_reference::mosaic::{CapacityPolicy, ClassReservation, Config};
    pin_worker(args, 0)?;
    let classes = args.n("classes", 64);
    let records = args.n("records", 65536);
    let count = args.n("iterations", 1000);
    let page_size = args.n("page-size", 32);
    if args.get("backend", "berry") != "mosaic"
        || !(1..=64).contains(&classes)
        || !(1..=65536).contains(&records)
        || count == 0
        || !(1..=4096).contains(&page_size)
    {
        return Err("invalid public-api dimensions".into());
    }
    let mut config = Config::new(
        (0..classes)
            .map(|i| ClassConfig {
                capacity: 64 + i,
                slots: 64,
            })
            .collect(),
        records,
        usize::MAX,
    );
    let reserved = args.get("capacity-policy", "unreserved") == "reserved";
    if reserved {
        config.capacity_policy = CapacityPolicy::Reserved;
    }
    let grants: Vec<_> = config
        .classes
        .iter()
        .map(|c| ClassReservation {
            capacity: c.capacity,
            slots: 1,
        })
        .collect();
    let mut owner = berry::StorageOwner::prepare(&config)?;
    let pool = owner.activate()?;
    let mut page = vec![berry::DomainMetrics::default(); page_size];
    let mut cursor = 0;
    let mut latency = Latency::default();
    let op = args.get("op", "churn");
    let mut one = || -> Result<()> {
        match op {
            "churn" => {
                let d = if reserved {
                    pool.try_open_reserved_domain(1 << 20, &grants)
                } else {
                    pool.try_open_domain(1 << 20)
                }?;
                drop(black_box(d));
            }
            "metrics" => {
                black_box(pool.metrics());
            }
            "snapshot" => drop(black_box(pool.snapshot())),
            "pages" => {
                let p = pool.domain_metrics_page(cursor, &mut page)?;
                black_box(&page[..p.written]);
                cursor = if p.done { 0 } else { p.next_index };
            }
            _ => return Err("public-api op must be churn, metrics, snapshot, or pages".into()),
        }
        Ok(())
    };
    for _ in 0..count.min(20) {
        one()?;
    }
    let start = Instant::now();
    for _ in 0..count {
        let at = Instant::now();
        one()?;
        latency.record(at.elapsed());
    }
    let elapsed = start.elapsed();
    assert_eq!(
        pool.snapshot()
            .classes
            .iter()
            .map(|c| c.observed_available_slots)
            .sum::<usize>(),
        classes * 64
    );
    drop(pool);
    owner.try_destroy()?;
    Ok(
        json!({"classes":classes,"records":records,"page_size":page_size,"op":op,"operations":count,
        "latency":latency.json(),"measurement_seconds":elapsed.as_secs_f64(),"drained":true}),
    )
}

/// Final returns only. Admission, channel transfer and barriers are outside the
/// clock; these burst results must not be reported as full pipeline throughput.
pub fn returns(args: &Args) -> Result<Value> {
    use std::sync::mpsc;
    let workers = args.n("threads", 4);
    let rounds = args.n("iterations", 300);
    let topology = args.get("topology", "domains");
    let layout = args.get("return-layout", "historical");
    let per_class = match layout {
        "historical" => 8,
        "scaling" => 1,
        "shared-word" => 1,
        _ => return Err("invalid return-layout".into()),
    };
    let shared_word = layout == "shared-word";
    let per_word = if shared_word { 64 / workers.max(1) } else { 64 };
    if let Some(cpus) = args.0.get("cpus")
        && cpus
            .split(',')
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            < workers
    {
        return Err("return scaling requires one distinct configured CPU per returner".into());
    }
    if !(1..=if layout == "historical" { 8 } else { 32 }).contains(&workers)
        || !(1..=10000).contains(&rounds)
        || !["roots", "domains", "domain"].contains(&topology)
    {
        return Err("invalid return dimensions".into());
    }
    let isolated = topology == "roots";
    let same = topology == "domain";
    let classes = if isolated || shared_word {
        per_class
    } else {
        per_class * workers
    };
    let batch = per_class * per_word;
    let sampled = args.get("latency", "no") == "yes";
    let fixtures: Vec<_> = (0..if isolated { workers } else { 1 })
        .map(|_| {
            Fixture::new(
                args.get("backend", "berry"),
                (0..classes)
                    .map(|i| ClassConfig {
                        capacity: 64 + i,
                        slots: 64,
                    })
                    .collect(),
                workers,
                None,
            )
        })
        .collect::<Result<_>>()?;
    let domains: Vec<_> = (0..if same { 1 } else { workers })
        .map(|i| fixtures[if isolated { i } else { 0 }].open(1 << 24))
        .collect::<Result<_>>()?;
    let barrier = Arc::new(Barrier::new(workers));
    let (mut senders, mut receivers, mut tasks) = (Vec::new(), Vec::new(), Vec::new());
    for i in 0..workers {
        let (tx, rx) = mpsc::sync_channel::<Vec<Frame>>(1);
        let (back, done) = mpsc::sync_channel(1);
        senders.push(tx);
        receivers.push(done);
        let barrier = barrier.clone();
        let args = args.clone();
        tasks.push(std::thread::spawn(move || -> Result<Latency> {
            pin_worker(&args, i)?;
            let mut per_drop = Latency::default();
            let mut round = 0;
            while let Ok(mut frames) = rx.recv() {
                barrier.wait();
                let start = Instant::now();
                while let Some(frame) = frames.pop() {
                    if sampled && round >= 20 {
                        let at = Instant::now();
                        drop(std::hint::black_box(frame));
                        per_drop.record(at.elapsed());
                    } else {
                        drop(std::hint::black_box(frame));
                    }
                }
                let elapsed = start.elapsed();
                back.send((frames, elapsed))?;
                round += 1;
            }
            Ok(per_drop)
        }));
    }
    let mut scratch: Vec<_> = (0..workers).map(|_| Vec::with_capacity(batch)).collect();
    let mut latency = Latency::default();
    let mut sum = std::time::Duration::ZERO;
    let mut raw = Vec::with_capacity(rounds);
    for round in 0..rounds + 20 {
        for (i, frames) in scratch.iter_mut().enumerate() {
            let domain = &domains[if same { 0 } else { i }];
            let first = if isolated || shared_word {
                0
            } else {
                i * per_class
            };
            for class in first..first + per_class {
                for _ in 0..per_word {
                    frames.push(domain.acquire(64 + class)?.freeze());
                }
            }
        }
        for (i, frames) in scratch.iter_mut().enumerate() {
            senders[i].send(std::mem::take(frames))?;
        }
        let mut times = Vec::with_capacity(workers);
        for (i, rx) in receivers.iter().enumerate() {
            let (frames, elapsed) = rx.recv()?;
            scratch[i] = frames;
            times.push(elapsed);
        }
        if round >= 20 {
            let max = *times.iter().max().unwrap();
            sum += max;
            latency.record(max);
            raw.push(times.iter().map(|t| t.as_nanos()).collect::<Vec<_>>());
        }
    }
    drop(senders);
    let mut per_drop = Latency::default();
    for t in tasks {
        per_drop.merge(&t.join().map_err(|_| "return worker panic")??);
    }
    drop(domains);
    for fixture in fixtures {
        fixture.finish()?;
    }
    Ok(
        json!({"topology":topology,"rounds":rounds,"layout":layout,"drops_per_worker_per_round":batch,"per_drop":per_drop.json(),"per_drop_instrumented":sampled,"failures":0,
        "burst_drops_per_s":if sampled {None}else{Some(rounds as f64*workers as f64*batch as f64/sum.as_secs_f64())},
        "slowest_worker_batch":latency.json(),"round_times_ns":raw,"drained":true,
        "scope":"synchronized final Drop bursts; excludes acquisition, channel, barrier; not pipeline throughput"}),
    )
}
