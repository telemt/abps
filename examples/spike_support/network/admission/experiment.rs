//! Same immutable offered/service trace drives logical replay and native returns.
//! Replay timings are logical, never CPU performance evidence.
use super::*;
use std::{cmp::Ordering, collections::BinaryHeap, sync::mpsc as channel};
#[derive(Clone, Copy)]
struct Offered {
    id: usize,
    stream: usize,
    due: u64,
    hold: u64,
    size: usize,
    charge: usize,
}
struct Held {
    due: u64,
    id: usize,
    stream: usize,
    charge: usize,
    frame: Frame,
}
impl PartialEq for Held {
    fn eq(&self, o: &Self) -> bool {
        self.id == o.id
    }
}
impl Eq for Held {}
impl PartialOrd for Held {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Held {
    fn cmp(&self, o: &Self) -> Ordering {
        o.due.cmp(&self.due).then_with(|| o.id.cmp(&self.id))
    }
}
fn trace(s: &Settings, args: &Args) -> Result<Vec<Offered>> {
    let count = args.get("iterations", "10000").parse::<usize>()?;
    let rate = args.get("rate", "3100").parse::<u64>()?;
    let period = args.get("release-period-us", "10000").parse::<u64>()?;
    if count == 0
        || count > 2_000_000
        || rate == 0
        || rate > 1_000_000
        || period == 0
        || period > 1_000_000
    {
        return Err("invalid bounded trace dimensions".into());
    }
    let period = period * 1000;
    let pattern = args.get("release-pattern", "synchronized");
    if !matches!(pattern, "synchronized" | "spread" | "impulse") {
        return Err("invalid release-pattern".into());
    }
    let mut out = Vec::with_capacity(count);
    for id in 0..count {
        let due = id as u64 * 1_000_000_000 / rate;
        let size = s.payload_size(id % s.streams, (id / s.streams) as u64);
        let charge = *s
            .capacities()
            .iter()
            .find(|&&c| c >= size)
            .ok_or("unserviceable trace size")?;
        out.push(Offered {
            id,
            stream: id % s.streams,
            due,
            hold: period + period - due % period,
            size,
            charge,
        });
    }
    // Permute complete service requirements within each 31-event cohort. This
    // changes phase correlation, not the service-time multiset or offered bytes.
    if pattern == "spread" {
        for chunk in out.chunks_mut(31) {
            for i in 0..chunk.len() / 2 {
                let j = chunk.len() - 1 - i;
                let h = chunk[i].hold;
                chunk[i].hold = chunk[j].hold;
                chunk[j].hold = h;
            }
        }
    } else if pattern == "impulse" {
        for e in &mut out[31.min(count)..] {
            e.hold = period;
        }
    }
    out.iter().try_fold(0u64, |sum, e| {
        sum.checked_add(e.charge as u64)
            .ok_or("offered charge counter overflow")
    })?;
    Ok(out)
}
fn hash(values: impl Iterator<Item = u64>) -> String {
    let h = values.fold(0xcbf29ce484222325u64, |mut h, v| {
        for b in v.to_le_bytes() {
            h = (h ^ b as u64).wrapping_mul(0x100000001b3);
        }
        h
    });
    format!("{h:016x}")
}
fn release(h: Held) -> usize {
    assert_eq!(h.frame.as_ref(), (h.id as u64).to_le_bytes());
    let charge = h.charge;
    drop(h);
    charge
}
fn worker(
    rx: channel::Receiver<Held>,
    start: Instant,
    capacity: usize,
    records: usize,
) -> Vec<(usize, u64, u64)> {
    let mut heap = BinaryHeap::<Held>::with_capacity(capacity);
    let mut events = Vec::with_capacity(records);
    let mut closed = false;
    loop {
        let now = start.elapsed().as_nanos() as u64;
        while heap.peek().is_some_and(|h| h.due <= now) {
            let h = heap.pop().unwrap();
            let id = h.id;
            let before = start.elapsed().as_nanos() as u64;
            release(h);
            let after = start.elapsed().as_nanos() as u64;
            assert!(events.len() < events.capacity());
            events.push((id, before, after));
        }
        if closed && heap.is_empty() {
            return events;
        }
        let wait = heap.peek().map_or(Duration::from_millis(1), |h| {
            Duration::from_nanos(h.due.saturating_sub(now)).min(Duration::from_millis(1))
        });
        if closed {
            std::thread::sleep(wait);
            continue;
        }
        match rx.recv_timeout(wait) {
            Ok(h) => {
                assert!(heap.len() < heap.capacity());
                heap.push(h);
            }
            Err(channel::RecvTimeoutError::Timeout) => {}
            Err(channel::RecvTimeoutError::Disconnected) => closed = true,
        }
    }
}
pub fn run(args: &Args) -> Result<Value> {
    let s = Settings::new(args)?;
    let o = s.admission.clone().ok_or("not an admission case")?;
    if s.fanout != 1 || s.memory.is_some() || s.arrival_pattern != "steady" {
        return Err(
            "replay/returns requires fanout=1, memory-policy=off and steady offered arrivals"
                .into(),
        );
    }
    if s.capacity_policy != CapacityPolicy::Unreserved {
        return Err("admission replay/returns requires unreserved".into());
    }
    let offered = trace(&s, args)?;
    let native = args.get("case", "") == "admission-returns";
    let fixture = Fixture::new(
        args.get("backend", "mosaic"),
        s.capacities()
            .iter()
            .copied()
            .zip(&s.class_slots)
            .map(|(capacity, &slots)| ClassConfig { capacity, slots })
            .collect(),
        s.streams,
        None,
    )?;
    let origins = (0..s.streams)
        .map(|_| {
            fixture.open(
                s.domain_limit.unwrap_or(
                    s.capacities()
                        .iter()
                        .zip(&s.class_slots)
                        .map(|(c, n)| c * n)
                        .sum(),
                ),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let initial = fixture.snapshot();
    let mut engine = Engine::new(o.clone(), s.streams);
    let mut pending = vec![None::<(Ticket, Offered)>; s.streams];
    let mut held = BinaryHeap::<Held>::with_capacity(s.class_slots.iter().sum());
    let mut live = vec![0usize; s.streams];
    let mut peak = 0;
    let mut samples = Vec::new();
    // Bounded instrumentation, separate from the TCP timing campaign.
    samples.reserve_exact(offered.len() * 2 + 1);
    let mut events = Vec::with_capacity(offered.len());
    let mut grants = Latency::default();
    let mut attempts = Latency::default();
    let mut errors = [0u64; 4];
    let mut rejected = 0;
    let mut expired = 0;
    let mut source_full = 0;
    let mut streak = vec![0usize; s.streams];
    let mut maximum_streak = 0;
    let mut start_streak = vec![None::<u64>; s.streams];
    let mut longest_streak = 0;
    let mut senders = Vec::new();
    let mut workers = Vec::new();
    let start = Instant::now()
        + if native {
            Duration::from_millis(100)
        } else {
            Duration::ZERO
        };
    if native {
        for n in 0..o.return_workers {
            let (tx, rx) = channel::sync_channel(held.capacity());
            senders.push(tx);
            let cap = held.capacity();
            let records = offered.len().div_ceil(o.return_workers) + 1;
            let a = args.clone();
            workers.push(std::thread::spawn(move || -> Result<_> {
                super::super::super::pin_worker(&a, n + 1)?;
                Ok(worker(rx, start, cap, records))
            }));
        }
    }
    super::super::super::pin_worker(args, 0)?;
    if native {
        if Instant::now() >= start {
            return Err("native setup missed start; no measurement".into());
        }
        std::thread::sleep(start.saturating_duration_since(Instant::now()));
    }
    let mut pos = 0;
    let mut now = 0u64;
    while pos < offered.len() || pending.iter().any(Option::is_some) {
        if native {
            now = start.elapsed().as_nanos() as u64;
        }
        if !native {
            while held.peek().is_some_and(|h| h.due <= now) {
                let h = held.pop().unwrap();
                live[h.stream] -= h.charge;
                release(h);
            }
            assert_eq!(
                fixture.snapshot()["admitted"].as_u64().unwrap() as usize,
                live.iter().sum::<usize>()
            );
        }
        while pos < offered.len() && offered[pos].due <= now {
            let e = offered[pos];
            pos += 1;
            if pending[e.stream].is_some() {
                source_full += 1;
                continue;
            }
            let deadline = e.due + s.max_frame_age.unwrap().as_nanos() as u64;
            let t = engine.register(e.stream, e.charge, deadline)?;
            pending[e.stream] = Some((t, e));
        }
        engine.service(now);
        let mut progressed = false;
        for _ in 0..PASS {
            for p in &mut pending {
                if let Some((t, _)) = *p
                    && engine.expired(t, now)
                {
                    engine.cancel(t);
                    *p = None;
                    expired += 1;
                }
            }
            let Some(&stream) = engine.queue.front() else {
                break;
            };
            let (t, e) = pending[stream].unwrap();
            if !engine.begin(t, now) {
                break;
            }
            pending[stream] = None;
            progressed = true;
            let before = Instant::now();
            let r = origins[stream].acquire(e.size);
            if native {
                attempts.record(before.elapsed());
                now = start.elapsed().as_nanos() as u64;
            }
            match r {
                Ok(mut b) => {
                    engine.commit_at(t, now);
                    grants.record(Duration::from_nanos(now - e.due));
                    if let Some(at) = start_streak[stream].take() {
                        longest_streak = longest_streak.max(now - at);
                    }
                    streak[stream] = 0;
                    b.put(&(e.id as u64).to_le_bytes());
                    let h = Held {
                        due: now + e.hold,
                        id: e.id,
                        stream,
                        charge: e.charge,
                        frame: b.freeze(),
                    };
                    events.push((e.id, now, e.charge));
                    if native {
                        senders[e.id % o.return_workers]
                            .try_send(h)
                            .map_err(|_| "bounded return mailbox failed")?;
                    } else {
                        live[stream] += e.charge;
                        held.push(h);
                    }
                }
                Err(e) => {
                    engine.cancel(t);
                    rejected += 1;
                    let i = match e {
                        Error::Contended => 0,
                        Error::BudgetExceeded => 1,
                        Error::ClassDepleted => 2,
                        Error::DomainClassDepleted => 3,
                        _ => return Err(e.into()),
                    };
                    errors[i] += 1;
                    if !native && e == Error::Contended {
                        return Err("spurious sequential contention".into());
                    }
                    streak[stream] += 1;
                    maximum_streak = maximum_streak.max(streak[stream]);
                    start_streak[stream].get_or_insert(now);
                }
            }
            let observed = fixture.snapshot()["admitted"].as_u64().unwrap() as usize;
            peak = peak.max(observed);
            assert!(samples.len() < samples.capacity());
            samples.push((now, observed));
        }
        let next = [
            offered.get(pos).map(|e| e.due),
            engine.next(now),
            if native {
                None
            } else {
                held.peek().map(|h| h.due)
            },
        ]
        .into_iter()
        .flatten()
        .min();
        if let Some(next) = next {
            if native {
                if !progressed {
                    let wait = next.saturating_sub(now);
                    if wait > 100_000 {
                        std::thread::sleep(Duration::from_nanos(wait - 50_000));
                    } else {
                        std::thread::yield_now();
                    }
                }
            } else {
                now = next.max(now);
            }
        } else {
            break;
        }
    }
    drop(senders);
    let mut returns = Vec::new();
    for worker in workers {
        returns.extend(worker.join().map_err(|_| "returner panic")??);
    }
    while let Some(h) = held.pop() {
        live[h.stream] -= h.charge;
        release(h);
    }
    for at in start_streak.into_iter().flatten() {
        longest_streak = longest_streak.max(now - at);
    }
    assert!(pending.iter().all(Option::is_none));
    assert_eq!(
        offered.len() as u64,
        engine.counts.committed + rejected + expired + source_full
    );
    drop(origins);
    let final_state = fixture.finish()?;
    let mut hold_multiset = offered.iter().map(|e| e.hold).collect::<Vec<_>>();
    hold_multiset.sort_unstable();
    Ok(
        json!({"kind":if native{"instrumented_native"}else{"logical_replay"},"timing_evidence":native,"drained":true,
        "offered":offered.len(),"accepted":engine.counts.committed,"core_rejected":rejected,"expired":expired,"source_pending_full":source_full,
        "errors":{"Contended":errors[0],"BudgetExceeded":errors[1],"ClassDepleted":errors[2],"DomainClassDepleted":errors[3]},
        "offered_hash":hash(offered.iter().flat_map(|e|[e.id as u64,e.due,e.size as u64,e.charge as u64])),"service_multiset_hash":hash(hold_multiset.into_iter()),
        "pacing":engine.json(),"options":o.json(),"grant_latency":grants.json(),"core_attempt":attempts.json(),"peak_observed_admitted":peak,
        "max_failure_streak":maximum_streak,"max_failure_streak_ns":longest_streak,"initial":initial,"final":final_state,
        "admissions":events,"observed_occupancy":samples,"final_drop_intervals":returns,
        "phase_scope":"immutable phase-correlated service requirements; actual releases follow successful admission, not a forced periodic barrier"}),
    )
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ap16_service_permutation_preserves_work() {
        let mut a = Args(Default::default());
        a.0.extend([
            ("case".into(), "admission-replay".into()),
            ("iterations".into(), "125".into()),
        ]);
        let s = Settings::new(&a).unwrap();
        let one = trace(&s, &a).unwrap();
        a.0.insert("release-pattern".into(), "spread".into());
        let two = trace(&s, &a).unwrap();
        assert!(
            one.iter()
                .zip(&two)
                .all(|(a, b)| a.due == b.due && a.size == b.size && a.charge == b.charge)
        );
        let mut a = one.iter().map(|e| e.hold).collect::<Vec<_>>();
        let mut b = two.iter().map(|e| e.hold).collect::<Vec<_>>();
        a.sort();
        b.sort();
        assert_eq!(a, b);
    }
}
