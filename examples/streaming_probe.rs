//! AP-18: prepared retention/occupancy surface. Channel/clock cost is included.
#![forbid(unsafe_code)]
#[allow(dead_code)]
mod perf_support;
#[allow(dead_code)]
mod spike_support;
use abps_reference::{CapacityPolicy, ClassConfig, ClassReservation};
use perf_support::{Args, Result, metrics::Counts};
use serde_json::{Value, json};
use spike_support::backend::{Fixture, Frame};
use std::{
    collections::VecDeque,
    sync::mpsc,
    time::{Duration, Instant},
};

fn run(args: &Args) -> Result<Value> {
    for k in args.0.keys() {
        if ![
            "backend",
            "size",
            "slots",
            "occupancy",
            "remote-percent",
            "hold-us",
            "iterations",
            "capacity-policy",
            "cpus",
        ]
        .contains(&k.as_str())
        {
            return Err(format!("unknown surface option {k}").into());
        }
    }
    let backend = args.get("backend", "mosaic");
    let size = args.n("size", 1500);
    let slots = args.n("slots", 64);
    let occupancy = args.n("occupancy", 75);
    let remote = args.n("remote-percent", 50);
    let n = args.n("iterations", 100000);
    let hold_us = args.n("hold-us", 0);
    if !(1..=1048576).contains(&size)
        || !(1..=16384).contains(&slots)
        || occupancy > 100
        || remote > 100
        || n == 0
        || n > 20000000
        || hold_us > 1000000
    {
        return Err("invalid surface bound".into());
    }
    let limit = size.checked_mul(slots).ok_or("surface overflow")?;
    let policy = match args.get("capacity-policy", "unreserved") {
        "reserved" => CapacityPolicy::Reserved,
        "unreserved" => CapacityPolicy::Unreserved,
        _ => return Err("invalid capacity policy".into()),
    };
    spike_support::pin_worker(args, 0)?;
    let fixture = Fixture::with_policy(
        backend,
        vec![ClassConfig {
            capacity: size,
            slots,
        }],
        1,
        None,
        policy,
    )?;
    let origin = if policy == CapacityPolicy::Reserved {
        fixture.open_reserved(
            limit,
            &[ClassReservation {
                capacity: size,
                slots,
            }],
        )?
    } else {
        fixture.open(limit)?
    };
    let held_slots = slots * occupancy / 100;
    let mut held = Vec::with_capacity(held_slots);
    for _ in 0..held_slots {
        held.push(origin.acquire(size)?.freeze());
    }
    let mut local = VecDeque::<(Instant, Frame)>::with_capacity(slots);
    let (tx, rx) = mpsc::sync_channel::<Frame>(slots);
    let a = args.clone();
    let hold = Duration::from_micros(hold_us as u64);
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || -> Result<usize> {
        spike_support::pin_worker(&a, 1)?;
        let mut pending = VecDeque::<(Instant, Frame)>::with_capacity(slots);
        ready_tx.send(())?;
        let mut returned = 0;
        loop {
            while pending
                .front()
                .is_some_and(|(due, _)| *due <= Instant::now())
            {
                pending.pop_front();
                returned += 1;
            }
            // Total backing rights, including pending, cannot exceed slots.
            match rx.recv_timeout(Duration::from_micros(50)) {
                Ok(f) => {
                    assert!(pending.len() < slots);
                    pending.push_back((Instant::now() + hold, f));
                }
                Err(mpsc::RecvTimeoutError::Timeout) => (),
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    returned += pending.len();
                    drop(pending);
                    return Ok(returned);
                }
            }
        }
    });
    ready_rx.recv()?;
    let mut counts = Counts::default();
    let mut dispatched = 0;
    let mut queue_full = 0;
    let start = Instant::now();
    for i in 0..n {
        while local.front().is_some_and(|(due, _)| *due <= Instant::now()) {
            local.pop_front();
        }
        let at = Instant::now();
        match origin.acquire(size) {
            Ok(mut b) => {
                b.put(&[0xA5]);
                let f = b.freeze();
                counts.success.record(at.elapsed());
                counts.accepted += 1;
                counts.bytes += size as u64;
                if i % 100 < remote {
                    match tx.try_send(f) {
                        Ok(()) => dispatched += 1,
                        Err(mpsc::TrySendError::Full(f)) => {
                            queue_full += 1;
                            drop(f);
                        }
                        Err(e) => return Err(e.to_string().into()),
                    }
                } else {
                    assert!(local.len() < slots);
                    local.push_back((Instant::now() + hold, f));
                }
            }
            Err(e) => {
                counts.failure.record(at.elapsed());
                let index = perf_support::metrics::ERRORS
                    .iter()
                    .position(|v| *v == e)
                    .ok_or("unclassified core error")?;
                counts.errors[index] += 1;
            }
        }
        counts.attempted += 1;
    }
    let elapsed = start.elapsed();
    drop((tx, local));
    let returned = worker.join().map_err(|_| "return worker panicked")??;
    assert_eq!(returned, dispatched);
    drop((held, origin));
    let drained = fixture.finish()?;
    Ok(
        json!({"backend":backend,"size":size,"slots":slots,"held_slots":held_slots,
        "remote_percent_requested":remote,"remote_dispatched":dispatched,"remote_returned":returned,
        "queue_full_local_returns":queue_full,"hold_us":hold_us,"iterations":n,"elapsed_ns":elapsed.as_nanos(),
        "accepted":counts.accepted,"errors":perf_support::metrics::ERRORS.iter().zip(counts.errors).map(|(k,v)|(format!("{k:?}"),v)).collect::<std::collections::BTreeMap<_,_>>(),
        "success":counts.success.json(),"failure":counts.failure.json(),"drained":drained,
        "scope":"fixed attempt count; preheld occupancy floor; clock, channel and return scheduling included; shutdown shortens remaining holds"}),
    )
}
fn main() -> Result<()> {
    println!("{}", run(&Args::parse()?)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ap18_preheld_full_capacity_rejects_every_attempt_and_drains() {
        for backend in ["bytes", "abps", "berry"] {
            let a = Args(
                [
                    ("backend", backend),
                    ("occupancy", "100"),
                    ("slots", "2"),
                    ("iterations", "100"),
                ]
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            );
            let r = run(&a).unwrap();
            assert_eq!(r["accepted"], 0);
            assert_eq!(r["errors"]["BudgetExceeded"], 100);
            assert_eq!(r["drained"]["admitted"], 0);
            assert_eq!(r["drained"]["available"], 2);
        }
    }
}
