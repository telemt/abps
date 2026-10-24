use crate::perf_support::{Args, Result};
use abps_reference::{mosaic as m, serial::SerialBuffer};
use serde_json::{Value, json};
use std::{hint::black_box, time::Instant};

#[inline(never)]
fn observe(bytes: &[u8]) -> usize {
    let b = black_box(bytes);
    usize::from(b[0]) + usize::from(b[b.len() - 1])
}

pub fn run(
    args: &Args,
    allocator: Option<&stats_alloc::StatsAlloc<std::alloc::System>>,
) -> Result<Value> {
    for key in args.0.keys() {
        if !["mode", "size", "working-set", "iterations", "sample-stride"].contains(&key.as_str()) {
            return Err(format!("unknown serial option: {key}").into());
        }
    }
    let mode = args.get("mode", "serial-copy");
    if ![
        "fresh-copy",
        "fresh-init",
        "park-copy",
        "park-init",
        "serial-copy",
        "serial-init",
    ]
    .contains(&mode)
    {
        return Err("invalid serial mode".into());
    }
    let number = |name: &str, default: usize| -> Result<usize> {
        Ok(args
            .0
            .get(name)
            .map(|n| n.parse())
            .transpose()?
            .unwrap_or(default))
    };
    let size = number("size", 1500)?;
    let working = number("working-set", 1)?;
    let iterations = number("iterations", 100000)?;
    let stride = number("sample-stride", 0)?;
    if !(1..=1048576).contains(&size)
        || !(1..=64).contains(&working)
        || !(1..=200000000).contains(&iterations)
        || ![0, 64].contains(&stride)
    {
        return Err(
            "size 1..1MiB, working-set 1..64, iterations 1..200M, sample-stride 0|64 required"
                .into(),
        );
    }
    let config = m::Config::new(
        vec![m::ClassConfig {
            capacity: size,
            slots: working,
        }],
        1,
        usize::MAX,
    );
    let layout = m::Reservation::for_config(&config)?;
    let mut storage = m::StorageOwner::prepare(&config)?;
    let pool = storage.activate()?;
    let (owner, domain) = pool.try_open_domain(size * working)?;
    let source = vec![0xa5; size];
    let serial = mode.starts_with("serial");
    let fresh = mode.starts_with("fresh");
    let init = mode.ends_with("init");
    let mut direct = Vec::with_capacity(if serial { 0 } else { working });
    let mut held = Vec::with_capacity(if serial { working } else { 0 });
    for _ in 0..working {
        let mut b = domain.try_acquire(size)?;
        b.put_slice(&source)?;
        if serial {
            held.push(SerialBuffer::from_buffer(b));
        } else {
            direct.push(Some(b));
        }
    }
    // Prepared payload, handles and histogram storage. All W slots are retained
    // in both comparisons; fresh releases/reacquires only the current position.
    let mut samples = Vec::with_capacity(iterations.checked_div(stride).map_or(0, |n| n + 1));
    let positive = allocator.map(|a| {
        let region = stats_alloc::Region::new(a);
        drop(black_box(vec![0u8; 4096]));
        let c = region.change();
        assert!(c.allocations > 0 && c.deallocations > 0);
        true
    });
    let region = allocator.map(stats_alloc::Region::new);
    let begin = Instant::now();
    let mut checksum = 0usize;
    for i in 0..iterations {
        let at = (stride != 0 && i % stride == 0).then(Instant::now);
        let index = i % working;
        let bytes = if serial {
            let mut write = held[index].begin();
            if init {
                black_box(write.initialize_to(size)?).copy_from_slice(black_box(&source));
                write.set_filled_len(size)?;
            } else {
                write.put_slice(black_box(&source))?;
            }
            observe(write.finish().as_ref())
        } else {
            let entry = &mut direct[index];
            if fresh {
                drop(entry.take());
                *entry = Some(domain.try_acquire(size)?);
            }
            let b = entry.as_mut().unwrap();
            b.set_filled_len(0)?;
            if init {
                black_box(b.initialize_to(size)?).copy_from_slice(black_box(&source));
                b.set_filled_len(size)?;
            } else {
                b.put_slice(black_box(&source))?;
            }
            observe(b.as_ref())
        };
        checksum = checksum.wrapping_add(bytes);
        if let Some(at) = at {
            samples.push(at.elapsed().as_nanos() as u64);
        }
    }
    let elapsed = begin.elapsed();
    let costs = region.map(|r| r.change());
    if let Some(c) = costs {
        assert_eq!((c.allocations, c.deallocations, c.reallocations), (0, 0, 0));
    }
    assert_eq!(checksum, iterations * 330);
    for buffer in held {
        let mut b = buffer.into_buffer();
        assert_eq!(b.filled_len(), 0);
        b.set_filled_len(size)?;
        assert_eq!(b.as_ref(), source);
        direct.push(Some(b));
    }
    for b in direct.iter_mut().flatten() {
        assert_eq!(b.as_ref(), source);
        b.set_filled_len(0)?;
    }
    let idle = domain.metrics().admitted_bytes;
    assert_eq!(idle, size * working);
    drop(direct);
    assert_eq!(domain.metrics().admitted_bytes, 0);
    drop((domain, owner, pool));
    storage.try_destroy()?;
    samples.sort_unstable();
    let q = |p| {
        samples
            .get(samples.len().saturating_sub(1) * p / 1000)
            .copied()
    };
    Ok(
        json!({"kind":"serial cycles","mode":mode,"size":size,"working_set":working,
        "iterations":iterations,"checksum":checksum,"sample_stride":stride,"samples":samples.len(),
        "ns_per_frame":if allocator.is_none(){Some(elapsed.as_nanos() as f64/iterations as f64)}else{None},
        "p50_ns":q(500),"p99_ns":q(990),"p999_ns":if samples.len()>=1000 {q(999)}else{None},
        "p999_status":if samples.len()>=1000 {"sampled"}else{"insufficient_samples"},
        "positive_control":positive,"allocations":costs.map(|c|c.allocations),
        "deallocations":costs.map(|c|c.deallocations),"reallocations":costs.map(|c|c.reallocations),
        "retained_idle_bytes":idle,"held_slots":working,"arena_bytes":layout.total_bytes,
        "prepared_source_bytes":size,"successful_acquire_calls":working+if fresh {iterations}else{0},
        "drained":true,"scope":"prepared serial ring; fresh and retained hold the same other W-1 slots; no independent fan-out"}),
    )
}
