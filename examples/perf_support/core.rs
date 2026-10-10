use super::backend::{Fixture, Frame, Origin};
use super::metrics::{Counts, Latency};
use super::{Args, Result};
use abps_reference::{ClassConfig, Error};
use serde_json::{Value, json};
use std::hint::black_box;
use std::sync::{
    Barrier,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::time::{Duration, Instant};

pub struct Exercise {
    origin: Origin,
    seed: Option<Frame>,
    payload: Vec<u8>,
    op: String,
    fanout: usize,
}
impl Exercise {
    pub fn new(origin: Origin, size: usize, op: &str, fanout: usize) -> Self {
        let payload = vec![0x5a; size];
        let seed = if matches!(op, "clone" | "slice" | "fanout" | "budget" | "depleted") {
            let mut b = origin.acquire(size).unwrap();
            b.put(&payload);
            Some(b.freeze())
        } else {
            None
        };
        Self {
            origin,
            seed,
            payload,
            op: op.to_string(),
            fanout,
        }
    }
    // Rate always includes the complete iteration. For freeze/final_drop/fill,
    // the latency span excludes lease preparation; names make this explicit.
    pub fn step(&self, timed: bool) -> (std::result::Result<(), Error>, Option<Duration>) {
        let mut setup = if matches!(
            self.op.as_str(),
            "freeze" | "final_drop" | "fill" | "init_fill"
        ) {
            match self.origin.acquire(self.payload.len()) {
                Ok(b) => Some(b),
                Err(e) => return (Err(e), None),
            }
        } else {
            None
        };
        let ready = if self.op == "final_drop" {
            Some(setup.take().unwrap().freeze())
        } else {
            None
        };
        let start = timed.then(Instant::now);
        let result = match self.op.as_str() {
            "control" => {
                black_box(&self.payload);
                Ok(())
            }
            "acquire_drop" | "budget" | "depleted" | "too_large" => self
                .origin
                .acquire(if self.op == "too_large" {
                    usize::MAX
                } else {
                    self.payload.len()
                })
                .map(|b| drop(black_box(b))),
            "cycle" => self
                .origin
                .acquire(self.payload.len())
                .map(|b| drop(black_box(b.freeze()))),
            "copy_cycle" => self.origin.acquire(self.payload.len()).map(|mut b| {
                b.put(black_box(&self.payload));
                drop(black_box(b.freeze()));
            }),
            "freeze" => {
                let frame = setup.take().unwrap().freeze();
                let elapsed = start.map(|s| s.elapsed());
                drop(black_box(frame));
                return (Ok(()), elapsed);
            }
            "final_drop" => {
                drop(black_box(ready));
                Ok(())
            }
            "fill" | "init_fill" => {
                let mut b = setup.take().unwrap();
                if self.op == "fill" {
                    b.put(black_box(&self.payload));
                } else {
                    // The initialization call is observable; do not let LTO
                    // silently fuse the two explicitly requested writes.
                    black_box(b.initialize(self.payload.len()))
                        .copy_from_slice(black_box(&self.payload));
                    b.set_filled(self.payload.len());
                }
                let elapsed = start.map(|s| s.elapsed());
                drop(black_box(b));
                return (Ok(()), elapsed);
            }
            "clone" | "slice" => {
                let frame = self.seed.as_ref().unwrap();
                let n = if self.op == "slice" {
                    frame.as_ref().len().min(128)
                } else {
                    frame.as_ref().len()
                };
                frame.view(0..n).map(|view| drop(black_box(view)))
            }
            "fanout" => {
                let frame = self.seed.as_ref().unwrap();
                let mut views: [Option<Frame>; 20] = std::array::from_fn(|_| None);
                let mut result = Ok(());
                for view in &mut views[..self.fanout] {
                    match frame.view(0..frame.as_ref().len()) {
                        Ok(v) => *view = Some(v),
                        Err(e) => {
                            result = Err(e);
                            break;
                        }
                    }
                }
                drop(black_box(views));
                result
            }
            op => panic!("unknown operation: {op}"),
        };
        (result, start.map(|s| s.elapsed()))
    }
    fn run(&self, duration: Duration, timed: bool) -> (Counts, Duration) {
        let mut counts = Counts::default();
        let start = Instant::now();
        while start.elapsed() < duration {
            for _ in 0..64 {
                let (result, elapsed) = self.step(timed);
                let bytes = if matches!(self.op.as_str(), "fill" | "init_fill" | "copy_cycle") {
                    self.payload.len()
                } else {
                    0
                };
                counts.result(result, bytes, elapsed);
            }
        }
        (counts, start.elapsed())
    }
}
pub fn micro(args: &Args) -> Result<Value> {
    let threads = args.n("threads", 1);
    assert!((1..=8).contains(&threads));
    let roots = args.get("roots", "shared");
    assert!(matches!(roots, "shared" | "separate"));
    let records = args.n("records", 64).max(threads);
    let class_count = args.n("classes", 1);
    let size = args.n("size", 512);
    let op = args.get("op", "cycle");
    let fanout = args.n("fanout", 20);
    assert!((1..=20).contains(&fanout));
    let total_slots = if op == "depleted" { 1 } else { records.max(64) };
    let per_root = if roots == "separate" { threads } else { 1 };
    let classes: Vec<_> = (0..class_count)
        .map(|i| ClassConfig {
            capacity: if class_count == 1 {
                size
            } else {
                (i + 1) * 512
            },
            slots: (total_slots / per_root).max(1),
        })
        .collect();
    assert!(size > 0 && classes.last().unwrap().capacity >= size);
    let fixtures: Vec<_> = (0..per_root)
        .map(|_| {
            Fixture::new(
                args.get("backend", "abps"),
                classes.clone(),
                records / per_root,
            )
        })
        .collect::<Result<_>>()?;
    let exercises: Vec<_> = (0..threads)
        .map(|i| {
            let limit = if op == "budget" {
                size
            } else {
                classes.last().unwrap().capacity * total_slots * 2
            };
            Ok(Exercise::new(
                fixtures[i % per_root].open(limit)?,
                size,
                op,
                fanout,
            ))
        })
        .collect::<Result<_>>()?;
    let duration = args.seconds("seconds", 2.0);
    let warmup = args.seconds("warmup", 0.5);
    let timed = args.get("latency", "no") == "yes";
    let barrier = Barrier::new(threads);
    let observing = AtomicBool::new(false);
    let stop = AtomicBool::new(false);
    let hz = args.n("observe-hz", 0);
    let observer = (hz > 0).then(|| fixtures[0].observer());
    let (mut counts, mut elapsed) = (Counts::default(), Duration::ZERO);
    let mut snapshots = Latency::default();
    std::thread::scope(|scope| {
        let observer_task = observer.map(|pool| {
            let observing = &observing;
            let stop = &stop;
            scope.spawn(move || {
                let mut latency = Latency::default();
                let period = Duration::from_secs_f64(1.0 / hz as f64);
                while !observing.load(Ordering::Acquire) {
                    std::thread::yield_now();
                }
                let mut next = Instant::now();
                while !stop.load(Ordering::Acquire) {
                    let now = Instant::now();
                    if now < next {
                        std::thread::park_timeout((next - now).min(Duration::from_millis(10)));
                        continue;
                    }
                    let started = Instant::now();
                    drop(black_box(pool.snapshot()));
                    latency.record(started.elapsed());
                    // No catch-up storm when one snapshot exceeds its period.
                    next = Instant::now() + period;
                }
                latency
            })
        });
        let tasks: Vec<_> = exercises
            .into_iter()
            .map(|exercise| {
                let barrier = &barrier;
                let observing = &observing;
                scope.spawn(move || {
                    barrier.wait();
                    exercise.run(warmup, false);
                    barrier.wait();
                    observing.store(true, Ordering::Release);
                    exercise.run(duration, timed)
                })
            })
            .collect();
        for task in tasks {
            let (stats, time) = task.join().unwrap();
            counts.merge(stats);
            elapsed = elapsed.max(time);
        }
        stop.store(true, Ordering::Release);
        if let Some(task) = observer_task {
            task.thread().unpark();
            snapshots = task.join().unwrap();
        }
    });
    let drains: Vec<_> = fixtures
        .into_iter()
        .map(Fixture::finish)
        .collect::<Result<_>>()?;
    Ok(
        json!({"metrics": counts.json(elapsed), "snapshot_latency": snapshots.json(),
        "drained": drains, "timed_spans": timed,
        "rate_scope": "complete iterations including setup and release"}),
    )
}

pub fn snapshot(args: &Args) -> Result<Value> {
    let n = args.n("records", 4096);
    let fixture = Fixture::new(
        "abps",
        vec![ClassConfig {
            capacity: 8,
            slots: n,
        }],
        n,
    )?;
    let observer = fixture.observer();
    let warmup = Instant::now();
    while warmup.elapsed() < args.seconds("warmup", 0.5) {
        drop(black_box(observer.snapshot()));
    }
    let mut latency = Latency::default();
    let start = Instant::now();
    let mut count = 0;
    while start.elapsed() < args.seconds("seconds", 2.0) {
        let t = Instant::now();
        drop(black_box(observer.snapshot()));
        latency.record(t.elapsed());
        count += 1;
    }
    let elapsed = start.elapsed();
    drop(observer);
    Ok(
        json!({"snapshots": count, "per_s": count as f64 / elapsed.as_secs_f64(),
        "latency": latency.json(), "drained": fixture.finish()?}),
    )
}

pub fn retention(args: &Args) -> Result<Value> {
    let fixture = Fixture::new(
        args.get("backend", "abps"),
        vec![
            ClassConfig {
                capacity: 512,
                slots: 1,
            },
            ClassConfig {
                capacity: 2 * 1024 * 1024,
                slots: 1,
            },
        ],
        3,
    )?;
    let origin = fixture.open(2 * 1024 * 1024)?;
    let other = fixture.open(4 * 1024 * 1024)?;
    let mut b = origin.acquire(2 * 1024 * 1024)?;
    b.put(&[42; 128]);
    let b = b.freeze();
    let small = b.view(0..128)?;
    let empty = b.view(0..0)?;
    drop(b);
    let tiny_held = fixture.snapshot();
    assert_eq!(tiny_held["admitted"], 2 * 1024 * 1024);
    assert!(
        matches!(origin.acquire(512), Err(Error::BudgetExceeded)),
        "no output headroom"
    );
    assert!(
        matches!(other.acquire(2 * 1024 * 1024), Err(Error::ClassDepleted)),
        "limit is not reservation"
    );
    drop(small);
    let empty_held = fixture.snapshot();
    assert_eq!(
        empty_held["admitted"],
        2 * 1024 * 1024,
        "empty view retains origin charge"
    );
    drop(empty);
    let output = origin.acquire(512)?;
    drop(output);
    drop(origin);
    drop(other);
    Ok(json!({"logical_tiny": 128, "logical_empty": 0,
        "tiny_held": tiny_held, "empty_held": empty_held,
        "headroom_error": "BudgetExceeded", "other_domain_error": "ClassDepleted",
        "drained": fixture.finish()?}))
}

pub fn handoff(args: &Args) -> Result<Value> {
    let size = args.n("size", 512);
    let fixture = Fixture::new(
        args.get("backend", "abps"),
        vec![ClassConfig {
            capacity: size,
            slots: 64,
        }],
        1,
    )?;
    let origin = fixture.open(size * 64)?;
    let (tx, rx) = mpsc::sync_channel::<Frame>(8);
    let (ack_tx, ack_rx) = mpsc::sync_channel::<()>(8);
    let consumer = std::thread::spawn(move || {
        for frame in rx {
            drop(frame);
            ack_tx.send(()).unwrap();
        }
    });
    let mut stats = Counts::default();
    let start = Instant::now();
    while start.elapsed() < args.seconds("seconds", 2.0) {
        let t = Instant::now();
        let result = origin.acquire(size).map(|b| {
            tx.send(b.freeze()).ok().unwrap();
            ack_rx.recv().unwrap();
        });
        stats.result(result, 0, Some(t.elapsed()));
    }
    let elapsed = start.elapsed();
    drop(tx);
    consumer.join().unwrap();
    drop(origin);
    Ok(
        json!({"metrics": stats.json(elapsed), "scope": "cross-thread send, final return, acknowledgement",
        "drained": fixture.finish()?}),
    )
}
