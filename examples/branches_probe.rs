//! BR-06: compare equivalent offered views, retaining every rejected attempt.
//! Harness channels/timers are external overhead, not part of the core contract.
#![forbid(unsafe_code)]
#[allow(dead_code)]
mod perf_support;
use abps_reference::mosaic::*;
use perf_support::{Args, Result, metrics::Latency};
use serde_json::{Value, json};
use std::{
    hint::black_box,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Scalar,
    Batch,
    Borrowed,
    Local,
    Control,
}
enum Packet {
    Shared(Buf),
    Branch(Branch),
}
enum Held {
    Shared(Buf),
    Local(LocalBuf),
}
impl Packet {
    fn bind(self) -> Held {
        match self {
            Self::Shared(b) => Held::Shared(b),
            Self::Branch(b) => Held::Local(b.into_local()),
        }
    }
}
struct Scratch {
    local: [Option<LocalBuf>; 64],
    shared: [Option<Buf>; 64],
}
impl Scratch {
    fn consume(&mut self, mode: Mode, views: usize) {
        if mode == Mode::Local {
            for b in self.local[..views].iter_mut().filter_map(Option::take) {
                black_box(b.as_ref());
                drop(black_box(b));
            }
        } else {
            for b in self.shared[..views].iter_mut().filter_map(Option::take) {
                black_box(b.as_ref());
                drop(black_box(b));
            }
        }
    }
}
impl Held {
    fn materialize<'a>(
        &'a self,
        mode: Mode,
        views: usize,
        scratch: &mut Scratch,
        borrowed: &mut [std::io::IoSlice<'a>; 64],
    ) -> (u64, u64) {
        if mode == Mode::Control {
            black_box(views);
            return (0, 0);
        }
        let mut good = 0;
        let mut failed = 0;
        match self {
            Self::Local(b) => {
                let scratch = &mut scratch.local;
                for out in &mut scratch[..views] {
                    *out = Some(b.try_slice(0..b.len()).unwrap());
                }
                good = views as u64;
            }
            Self::Shared(b) if mode == Mode::Borrowed => {
                // Lexically borrowed views intentionally have a narrower lifetime.
                let scratch = borrowed;
                for out in &mut scratch[..views] {
                    *out = b.as_io_slice();
                }
                black_box(&scratch[..views]);
                good = views as u64;
            }
            Self::Shared(b) => {
                let scratch = &mut scratch.shared;
                if mode == Mode::Batch {
                    match b.try_clone_into(&mut scratch[..views]) {
                        Ok(()) => good = views as u64,
                        Err(Error::Contended) => failed = views as u64,
                        Err(e) => panic!("unexpected batch failure: {e}"),
                    }
                } else {
                    for out in &mut scratch[..views] {
                        match b.try_slice(0..b.len()) {
                            Ok(v) => {
                                *out = Some(v);
                                good += 1;
                            }
                            Err(Error::Contended) => failed += 1,
                            Err(e) => panic!("unexpected scalar failure: {e}"),
                        }
                    }
                }
            }
        }
        (good, failed)
    }
}
struct Work {
    packets: Vec<Packet>,
    repeats: usize,
    measured: bool,
    epoch: usize,
}
struct Done {
    packets: Vec<Packet>,
    elapsed: Duration,
    accepted: u64,
    rejected: u64,
    incomplete: u64,
}
struct Worker {
    work: mpsc::SyncSender<Option<Work>>,
    done: mpsc::Receiver<Done>,
    armed: mpsc::Receiver<()>,
    thread: std::thread::JoinHandle<Result<(Value, Value, Value, Value)>>,
}
fn pin(args: &Args, index: usize) -> Result<()> {
    if let Some(cpus) = args.0.get("cpus") {
        let cpus: Vec<_> = cpus.split(',').collect();
        let tid = std::fs::read_link("/proc/thread-self")?;
        let status = std::process::Command::new("taskset")
            .args(["-pc", cpus[index]])
            .arg(tid.file_name().ok_or("missing thread id")?)
            .output()?;
        if !status.status.success() {
            return Err(String::from_utf8_lossy(&status.stderr).into_owned().into());
        }
    }
    Ok(())
}
struct Start {
    epoch: AtomicUsize,
    cancelled: AtomicBool,
}
struct Cancel(Arc<Start>);
impl Drop for Cancel {
    fn drop(&mut self) {
        self.0.cancelled.store(true, Ordering::Release);
    }
}
fn start_worker(
    args: Args,
    i: usize,
    mode: Mode,
    views: usize,
    sample: bool,
    gate: Arc<Start>,
) -> Result<Worker> {
    let (work, input) = mpsc::sync_channel::<Option<Work>>(1);
    let (arm, armed) = mpsc::sync_channel(1);
    let (completed, done) = mpsc::channel();
    let (ready, readiness) = mpsc::channel();
    let thread = std::thread::spawn(move || -> Result<_> {
        if let Err(e) = pin(&args, i) {
            let _ = ready.send(Err(e.to_string()));
            return Err(e);
        }
        let mut operations = Latency::default();
        let mut drops = Latency::default();
        let mut materialization = Latency::default();
        let mut view_drops = Latency::default();
        let mut scratch = Scratch {
            local: std::array::from_fn(|_| None),
            shared: std::array::from_fn(|_| None),
        };
        ready.send(Ok(()))?;
        while let Some(mut job) = input.recv_timeout(Duration::from_secs(30))? {
            let mut accepted = 0;
            let mut rejected = 0;
            let mut incomplete = 0;
            arm.send(())?;
            let deadline = Instant::now() + Duration::from_secs(10);
            while gate.epoch.load(Ordering::Acquire) < job.epoch {
                if gate.cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
                    return Err("cohort start cancelled or timed out".into());
                }
                std::thread::yield_now();
            }
            let start = Instant::now();
            for (index, packet) in job.packets.drain(..).enumerate() {
                let held = packet.bind();
                let mut borrowed = [std::io::IoSlice::new(&[]); 64];
                for n in 0..job.repeats {
                    let at = (sample && job.measured && n % 256 == 0).then(Instant::now);
                    let (a, r) = held.materialize(mode, views, &mut scratch, &mut borrowed);
                    let materialized = at.map(|at| {
                        let now = Instant::now();
                        materialization.record(now - at);
                        now
                    });
                    scratch.consume(mode, views);
                    if let Some(at) = materialized {
                        view_drops.record(at.elapsed());
                    }
                    accepted += a;
                    rejected += r;
                    if r > 0 {
                        incomplete |= 1u64 << index;
                    }
                    if let Some(at) = at {
                        operations.record(at.elapsed());
                    }
                }
                let at = (sample && job.measured).then(Instant::now);
                drop(held);
                if let Some(at) = at {
                    drops.record(at.elapsed());
                }
            }
            completed
                .send(Done {
                    packets: job.packets,
                    elapsed: start.elapsed(),
                    accepted,
                    rejected,
                    incomplete,
                })
                .map_err(|_| "completion receiver closed")?;
        }
        Ok((
            operations.json(),
            drops.json(),
            materialization.json(),
            view_drops.json(),
        ))
    });
    match readiness.recv_timeout(Duration::from_secs(10))? {
        Ok(()) => Ok(Worker {
            work,
            done,
            armed,
            thread,
        }),
        Err(e) => {
            let _ = thread.join();
            Err(e.into())
        }
    }
}
fn prepare(
    d: &Domain,
    mode: Mode,
    outgoing: &mut [Vec<Packet>],
    frames: usize,
    size: usize,
    measure_materialize: bool,
) -> Result<Duration> {
    let mut elapsed = Duration::ZERO;
    for _ in 0..frames {
        let mut b = d.try_acquire(size)?;
        b.initialize_to(size)?.fill(7);
        b.set_filled_len(size)?;
        let at = measure_materialize.then(Instant::now);
        let count = outgoing.len();
        if mode == Mode::Local {
            let mut out: [Option<Branch>; 32] = std::array::from_fn(|_| None);
            b.freeze_branches_into(&mut out[..count])
                .map_err(|e| e.reason())?;
            for (dst, b) in outgoing.iter_mut().zip(&mut out) {
                dst.push(Packet::Branch(b.take().unwrap()));
            }
        } else {
            let b = b.freeze();
            let mut out: [Option<Buf>; 32] = std::array::from_fn(|_| None);
            if mode == Mode::Scalar {
                for dst in &mut out[..count - 1] {
                    *dst = Some(b.try_clone()?);
                }
            } else {
                b.try_clone_into(&mut out[..count - 1])?;
            }
            out[count - 1] = Some(b);
            for (dst, b) in outgoing.iter_mut().zip(&mut out) {
                dst.push(Packet::Shared(b.take().unwrap()));
            }
        }
        if let Some(at) = at {
            elapsed += at.elapsed();
        }
    }
    Ok(elapsed)
}
fn main() -> Result<()> {
    let args = Args::parse()?;
    let mode_name = args.get("mode", "local");
    let mode = match mode_name {
        "scalar" => Mode::Scalar,
        "batch" => Mode::Batch,
        "borrowed" => Mode::Borrowed,
        "local" => Mode::Local,
        "control" => Mode::Control,
        _ => return Err("invalid mode".into()),
    };
    let case = args.get("case", "churn");
    if !matches!(
        case,
        "churn" | "cycle" | "drops" | "prepare" | "materialize" | "final-drop"
    ) {
        return Err("invalid case".into());
    }
    let n = args.n("threads", 4);
    if case == "final-drop" && n != 1 {
        return Err("final-drop requires exactly one owner/worker".into());
    }
    let views = args.n("views", 8);
    let iterations = args.n("iterations", if case == "churn" { 50_000 } else { 100 });
    let size = args.n("size", 64);
    let sample = args.get("latency", "no") == "yes";
    if !(1..=32).contains(&n)
        || !(1..=64).contains(&views)
        || !(1..=100_000_000).contains(&iterations)
        || !(1..=65536).contains(&size)
    {
        return Err("invalid dimensions".into());
    }
    if let Some(cpus) = args.0.get("cpus") {
        let distinct = cpus.split(',').collect::<std::collections::BTreeSet<_>>();
        if distinct.len() < n {
            return Err("distinct CPU per worker required".into());
        }
    }
    let frames = if case == "churn" { 1 } else { 64 };
    let mut cfg = Config::new(
        vec![ClassConfig {
            capacity: size,
            slots: frames,
        }],
        1,
        usize::MAX,
    );
    let base = Reservation::for_config(&cfg)?;
    // Equal layout across modes, with explicit incremental memory reporting.
    cfg.local_branches = Some(LocalBranchConfig {
        max_branches_per_slot: n,
        local_reference_limit: 65,
    });
    let reservation = Reservation::for_config(&cfg)?;
    let mut owner = StorageOwner::prepare(&cfg)?;
    let p = owner.activate()?;
    let (a, d) = p.try_open_domain(size * frames)?;
    let gate = Arc::new(Start {
        epoch: AtomicUsize::new(0),
        cancelled: AtomicBool::new(false),
    });
    let _cancel = Cancel(gate.clone());
    let mut workers = (0..n)
        .map(|i| start_worker(args.clone(), i, mode, views, sample, gate.clone()))
        .collect::<Result<Vec<_>>>()?;
    let mut outgoing: Vec<_> = (0..n).map(|_| Vec::with_capacity(frames)).collect();
    let rounds = if case == "churn" { 1 } else { iterations };
    let mut elapsed = Duration::ZERO;
    let mut worker_elapsed = Duration::ZERO;
    let mut preparation_elapsed = Duration::ZERO;
    let mut dispatch_elapsed = Duration::ZERO;
    let mut fully_served_frames = 0u64;
    let mut accepted = 0;
    let mut rejected = 0;
    let mut offered_frames = 0u64;
    for round in 0..rounds + 2 {
        let measured = round >= 2;
        let start = Instant::now();
        let materialized = prepare(&d, mode, &mut outgoing, frames, size, case == "materialize")?;
        let prepared = Instant::now();
        for (worker, packets) in workers.iter_mut().zip(&mut outgoing) {
            worker
                .work
                .send(Some(Work {
                    packets: std::mem::take(packets),
                    measured,
                    epoch: round + 1,
                    repeats: if matches!(case, "drops" | "final-drop" | "prepare" | "materialize") {
                        0
                    } else if case == "churn" {
                        if measured { iterations } else { 1000 }
                    } else {
                        1
                    },
                }))
                .map_err(|_| "worker stopped before dispatch")?;
        }
        for worker in &workers {
            worker.armed.recv_timeout(Duration::from_secs(10))?;
        }
        let dispatched = Instant::now();
        gate.epoch.store(round + 1, Ordering::Release);
        let mut incomplete = 0u64;
        let mut longest = Duration::ZERO;
        for (worker, packets) in workers.iter().zip(&mut outgoing) {
            let done = worker.done.recv_timeout(Duration::from_secs(30))?;
            *packets = done.packets;
            longest = longest.max(done.elapsed);
            incomplete |= done.incomplete;
            if measured {
                accepted += done.accepted;
                rejected += done.rejected;
            }
        }
        if measured {
            elapsed += match case {
                "cycle" => start.elapsed(),
                "prepare" => prepared - start,
                "materialize" => materialized,
                _ => dispatched.elapsed(),
            };
            preparation_elapsed += prepared - start;
            dispatch_elapsed += dispatched - prepared;
            fully_served_frames += frames as u64 - incomplete.count_ones() as u64;
            worker_elapsed += longest;
            offered_frames += frames as u64;
        }
        assert_eq!(d.metrics().admitted_bytes, 0);
    }
    for worker in &workers {
        worker
            .work
            .send(None)
            .map_err(|_| "worker stopped before shutdown")?;
    }
    let mut latency = Vec::new();
    for worker in workers {
        let (ops, drops, materialization, view_drops) =
            worker.thread.join().map_err(|_| "worker panicked")??;
        latency.push(json!({"view_group":ops,"owner_drop":drops,"view_materialization":materialization,"nonfinal_view_drops":view_drops}));
    }
    drop((outgoing, a, d, p));
    owner.try_destroy()?;
    println!(
        "{}",
        json!({"case":case,"mode":mode_name,"threads":n,"views":views,"iterations":iterations,
        "offered_frames":offered_frames,"fully_served_frames":fully_served_frames,"completed_owner_drops":offered_frames*n as u64,"offered_views":accepted+rejected,
        "accepted_views":accepted,"contended_views":rejected,"wall_seconds":elapsed.as_secs_f64(),
        "worker_seconds":worker_elapsed.as_secs_f64(),
        "preparation_seconds":preparation_elapsed.as_secs_f64(),"dispatch_seconds":dispatch_elapsed.as_secs_f64(),"accepted_per_s":accepted as f64/elapsed.as_secs_f64(),
        "frames_per_s":offered_frames as f64/elapsed.as_secs_f64(),"latency_instrumented":sample,"latency":latency,
        "backing_bytes":reservation.backing_bytes,"metadata_bytes":reservation.metadata_bytes,
        "extra_branch_metadata_bytes":reservation.total_bytes-base.total_bytes,"drained":true,
        "timing_scope":match case { "cycle" => "prepare/acquire/fill/fanout, dispatch, worker views and complete drops",
            "prepare" => "prepare/acquire/fill/fanout only; worker drops excluded",
            "materialize" => "freeze/fanout/materialize only; one timer pair per frame",
            _ => "common start to completion; preparation and dispatch excluded" },
        "drop_scope":if n==1 {"last backing owner (family completion for local)"} else {"mixed nonfinal and final owners; not isolated final Drop"},
        "coordination":"common epoch publication after all workers arm; 10s cancellation deadline",
        "scratch":"owning scratch once per worker; borrowed descriptors once per packet; reused across repeats",
        "limits":"fixed-work saturation probe, no independent arrival schedule; owner drops include nonfinal drops; borrowed is a lifetime-restricted baseline"})
    );
    Ok(())
}
