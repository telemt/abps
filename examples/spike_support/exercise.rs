use super::backend::{Frame, Origin};
use crate::perf_support::metrics::Counts;
use abps_reference::Error;
use std::hint::black_box;
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
        let seed = if matches!(
            op,
            "borrowed" | "clone" | "slice" | "fanout" | "fanout_batch" | "budget" | "depleted"
        ) {
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
            "borrowed" => {
                let frame = self.seed.as_ref().unwrap();
                frame.borrowed(0..frame.as_ref().len().min(128)).map(|v| {
                    black_box(v);
                })
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
            "fanout_batch" => {
                let frame = self.seed.as_ref().unwrap();
                let mut views: [Option<Frame>; 20] = std::array::from_fn(|_| None);
                let result = frame.try_clone_into(&mut views[..self.fanout]);
                drop(black_box(views));
                result
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
    pub fn run(&self, duration: Duration, timed: bool) -> (Counts, Duration) {
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
