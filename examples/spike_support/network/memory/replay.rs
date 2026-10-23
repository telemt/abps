//! AP-13 deterministic retention experiment. This driver deliberately uses plain
//! collections as an independent rights oracle, not as adapter hot-path storage.
#![forbid(unsafe_code)]
use super::*;
use std::collections::BTreeMap;
struct View {
    id: u64,
    stream: usize,
    recipient: usize,
    due: u64,
    offered: u64,
    frame: abps_reference::mosaic::Buf,
}
struct Right {
    stream: usize,
    charge: usize,
    owners: usize,
}
pub fn run(args: &Args) -> Result<Value> {
    let s = Settings::new(args)?;
    let plan = plan::PipelinePlan::new(&s, false, "mosaic")?;
    let p = plan
        .memory
        .as_ref()
        .ok_or("memory replay requires memory-policy")?
        .clone();
    let bank = Bank::prepare(p)?;
    let start = bank.borrow().clock_origin();
    let handles = (0..s.streams)
        .map(|_| Bank::open(&bank))
        .collect::<Result<Vec<_>>>()?;
    let rate = args.n("rate", 1000) as u64;
    if rate == 0 || rate > 1_000_000 {
        return Err("replay rate must be 1..1000000".into());
    }
    let count = (s.seconds.as_secs_f64() * rate as f64) as u64;
    if count > 2_000_000 {
        return Err("replay count exceeds 2000000".into());
    }
    let mut owners: Vec<View> = Vec::new();
    let mut rights = BTreeMap::<u64, Right>::new();
    let mut offered = vec![0u64; s.streams];
    let mut delivered = offered.clone();
    let mut timely = offered.clone();
    let mut rejected = offered.clone();
    let mut queue_losses = offered.clone();
    let mut occupied = vec![0usize; s.streams * s.fanout];
    let mut peak_backing = 0usize;
    let mut peak_views = 0usize;
    let mut last_tick = 0u64;
    let mut completed = 0;
    let mut evicted_views_without_refund = 0u64;
    let mut last_view_refunds = 0u64;
    let max_age = s
        .max_frame_age
        .unwrap_or(Duration::from_millis(100))
        .as_micros() as u64;
    // Timing is a logical schedule, never throughput/latency evidence for a CPU.
    for id in 0..=count {
        let (stream, seq, due, _) =
            super::super::loadgen::scheduled(id, s.streams, false, &s.arrival_pattern, rate);
        let now = if id == count {
            due.as_micros() as u64 + 2_000_000
        } else {
            due.as_micros() as u64
        };
        let mut i = 0;
        while i < owners.len() {
            if owners[i].due > now {
                i += 1;
                continue;
            }
            let v = owners.swap_remove(i);
            assert_eq!(v.frame.as_ref(), v.id.to_le_bytes());
            occupied[v.stream * s.fanout + v.recipient] -= 1;
            delivered[v.stream] += 1;
            completed += 1;
            if v.due - v.offered <= max_age {
                timely[v.stream] += 1;
            }
            let r = rights.get_mut(&v.id).unwrap();
            r.owners -= 1;
            if r.owners == 0 {
                rights.remove(&v.id);
                last_view_refunds += 1;
            } else {
                evicted_views_without_refund += 1;
            }
            drop(v);
        }
        let mut expected = [0usize; STREAMS];
        for r in rights.values() {
            expected[r.stream] += r.charge;
        }
        let observed = bank.borrow().observed_admitted();
        if expected != observed {
            return Err(format!(
                "rights oracle mismatch at event {id}: {expected:?} vs {observed:?}"
            )
            .into());
        }
        peak_backing = peak_backing.max(expected.iter().sum());
        peak_views = peak_views.max(owners.len());
        // Match native issuer maintenance, including AP-14 pressure expiry.
        // Budget recomputation remains 100 ms inside Bank.
        if now / 10_000 > last_tick {
            bank.borrow_mut()
                .maintenance_at(start + Duration::from_micros(now), completed);
            last_tick = now / 10_000;
        }
        if id == count {
            break;
        }
        offered[stream] += s.fanout as u64;
        let size = s.payload_size(stream, seq);
        handles[stream].offer(size);
        let recipients: Vec<_> = (0..s.fanout)
            .filter(|&r| occupied[stream * s.fanout + r] < s.queue)
            .collect();
        queue_losses[stream] += (s.fanout - recipients.len()) as u64;
        if recipients.is_empty() {
            continue;
        }
        let mut b = match handles[stream].acquire(size) {
            Ok(b) => b,
            Err(_) => {
                rejected[stream] += recipients.len() as u64;
                continue;
            }
        };
        let charge = b.capacity();
        b.put_slice(&id.to_le_bytes())?;
        let frame = b.freeze();
        rights.insert(
            id,
            Right {
                stream,
                charge,
                owners: recipients.len(),
            },
        );
        for recipient in recipients {
            let delay = if s.slow && recipient == 0 {
                match s.slow_pattern {
                    SlowPattern::Constant => 200_000,
                    SlowPattern::Pause if (now / 1_000_000) % 4 == 1 => 400_000,
                    SlowPattern::Ramp => 5_000 + (now / 1000 % 1000) * 200,
                    SlowPattern::StopResume if (now / 1_000_000) % 4 < 2 => 800_000,
                    _ => 2_000,
                }
            } else {
                2_000
            };
            occupied[stream * s.fanout + recipient] += 1;
            owners.push(View {
                id,
                stream,
                recipient,
                due: now + delay,
                offered: now,
                frame: frame.try_clone()?,
            });
        }
        drop(frame);
    }
    if !owners.is_empty() || !rights.is_empty() {
        return Err("replay failed to drain".into());
    }
    for i in 0..s.streams {
        if offered[i] != delivered[i] + rejected[i] + queue_losses[i] {
            return Err("replay flow mismatch".into());
        }
    }
    drop(handles);
    let destroyed = bank.borrow_mut().shutdown();
    let result = bank.borrow_mut().json();
    Ok(
        json!({"kind":"deterministic retention replay; not TCP or native latency evidence","failed":!destroyed,"events":count,"bank":result,"peak_unique_backing_bytes":peak_backing,"peak_owner_views":peak_views,
        "view_releases_without_backing_refund":evicted_views_without_refund,"last_view_refunds":last_view_refunds,
        "streams":(0..s.streams).map(|i|json!({"stream":i,"offered":offered[i],"delivered":delivered[i],"timely":timely[i],"admission_rejected":rejected[i],"queue_losses":queue_losses[i]})).collect::<Vec<_>>()}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ap13_replay_uses_controller_epoch_not_preparation_wall_time() {
        let args = Args(
            [
                ("backend", "mosaic"),
                ("streams", "8"),
                ("fanout", "8"),
                ("queue", "64"),
                ("queue-backend", "local"),
                ("return", "local"),
                ("overflow", "skip"),
                ("memory-policy", "adaptive"),
                ("memory-trace-capacity", "4"),
                ("class-slots", "128,128"),
                ("seconds", "0.3"),
                ("rate", "100"),
            ]
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect(),
        );
        let r = run(&args).unwrap();
        assert_eq!(r["bank"]["trace"][0]["ms"], 100);
        assert_eq!(r["bank"]["trace"][1]["ms"], 200);
        assert_eq!(r["bank"]["trace_complete"], true);
    }
}
