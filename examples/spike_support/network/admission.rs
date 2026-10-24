//! AP-16: issuer-local experimental pacing. No allocator authority lives here.
#![forbid(unsafe_code)]
use super::*;
use std::{cell::RefCell, collections::VecDeque, rc::Rc, task::Waker};

mod experiment;
pub(super) mod tcp;
pub fn run(args: &Args) -> Result<Value> {
    experiment::run(args)
}
#[cfg(test)]
mod tests;
const SCALE: u128 = 1_000_000_000;
const PASS: usize = 8;
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Mode {
    Off,
    Bucket,
    Tick,
}
#[derive(Clone, Debug)]
pub struct Options {
    pub mode: Mode,
    pub rate: u64,
    pub burst: usize,
    pub return_workers: usize,
    pub slow_bytes: u64,
}
impl Options {
    pub fn parse(args: &Args, classes: &[usize]) -> Result<Option<Self>> {
        let enabled = matches!(
            args.get("case", "micro"),
            "admission-replay" | "admission-returns" | "admission-tcp"
        );
        let keys = [
            "admission-pacing",
            "admission-rate-bytes",
            "admission-burst-bytes",
            "return-workers",
            "slow-bytes-per-second",
            "release-pattern",
            "release-period-us",
        ];
        if !enabled {
            if keys.iter().any(|k| args.0.contains_key(*k)) {
                return Err("admission options require an admission-* case".into());
            }
            return Ok(None);
        }
        let mode = match args.get("admission-pacing", "off") {
            "off" => Mode::Off,
            "bucket" => Mode::Bucket,
            "tick-cap" => Mode::Tick,
            _ => return Err("invalid admission-pacing".into()),
        };
        let rate = args.get("admission-rate-bytes", "0").parse::<u64>()?;
        let burst = args.get("admission-burst-bytes", "0").parse::<usize>()?;
        if mode != Mode::Off && (rate == 0 || burst < *classes.last().ok_or("missing classes")?) {
            return Err("pacing needs positive rate and burst >= largest class".into());
        }
        if mode == Mode::Off && (rate != 0 || burst != 0) {
            return Err("off does not accept pacing rate/burst".into());
        }
        if mode == Mode::Tick && (classes.len() != 1 || !burst.is_multiple_of(classes[0])) {
            return Err("tick-cap requires one class and a whole-slot burst".into());
        }
        if mode == Mode::Tick && (burst as u128 * SCALE).div_ceil(rate as u128) > u64::MAX as u128 {
            return Err("tick duration overflow".into());
        }
        let return_workers = args.get("return-workers", "1").parse()?;
        if !(1..=32).contains(&return_workers) {
            return Err("return-workers must be 1..32".into());
        }
        let slow_bytes = args.get("slow-bytes-per-second", "1024").parse()?;
        if slow_bytes == 0 || slow_bytes > 1_000_000_000 {
            return Err("invalid slow byte rate".into());
        }
        Ok(Some(Self {
            mode,
            rate,
            burst,
            return_workers,
            slow_bytes,
        }))
    }
    pub fn json(&self) -> Value {
        let tick = (self.mode == Mode::Tick)
            .then(|| (self.burst as u128 * SCALE).div_ceil(self.rate as u128) as u64);
        json!({"mode":format!("{:?}",self.mode),"rate_bytes_per_second":self.rate,"burst_bytes":self.burst,
            "tick_ns":tick,"effective_rate_bytes_per_second":tick.map(|t|self.burst as f64*1e9/t as f64),
            "return_workers":self.return_workers,"slow_wire_bytes_per_second":self.slow_bytes,"attempts":1,"deadline_origin":"validated_header"})
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ticket {
    stream: usize,
    generation: u64,
}
#[derive(Clone, Copy, Debug, PartialEq)]
enum State {
    Idle,
    Waiting,
    Reserved,
    Expired,
}
struct Entry {
    generation: u64,
    state: State,
    charge: usize,
    deadline: u64,
    signalled: bool,
}
#[derive(Default)]
pub struct Counters {
    pub committed: u64,
    pub bytes: u64,
    pub rollback: u64,
    pub expired: u64,
    pub cancelled: u64,
    pub peak: usize,
    pub waited: u64,
}
pub struct Engine {
    options: Options,
    credit: u128,
    last: u64,
    epoch: u64,
    tick: u64,
    entries: Vec<Entry>,
    queue: VecDeque<usize>,
    pub counts: Counters,
}
impl Engine {
    pub fn new(options: Options, streams: usize) -> Self {
        assert!(streams <= 64 && (streams > 0 || options.mode == Mode::Off));
        let tick = if options.mode == Mode::Tick {
            (options.burst as u128 * SCALE).div_ceil(options.rate as u128) as u64
        } else {
            1
        };
        Self {
            credit: options.burst as u128 * SCALE,
            options,
            last: 0,
            epoch: 0,
            tick,
            entries: (0..streams)
                .map(|_| Entry {
                    generation: 0,
                    state: State::Idle,
                    charge: 0,
                    deadline: 0,
                    signalled: false,
                })
                .collect(),
            queue: VecDeque::with_capacity(streams),
            counts: Counters::default(),
        }
    }
    fn refill(&mut self, now: u64) {
        assert!(now >= self.last, "monotone clock required");
        let cap = self.options.burst as u128 * SCALE;
        match self.options.mode {
            Mode::Bucket => {
                self.credit = self
                    .credit
                    .saturating_add((now - self.last) as u128 * self.options.rate as u128)
                    .min(cap)
            }
            Mode::Tick if now / self.tick != self.epoch => {
                self.credit = cap;
                self.epoch = now / self.tick;
            }
            _ => {}
        }
        self.last = now;
    }
    pub fn register(
        &mut self,
        stream: usize,
        charge: usize,
        deadline: u64,
    ) -> std::result::Result<Ticket, &'static str> {
        if charge == 0 || (self.options.mode != Mode::Off && charge > self.options.burst) {
            return Err("invalid pacing charge");
        }
        let e = self.entries.get_mut(stream).ok_or("invalid stream")?;
        if e.state != State::Idle {
            return Err("duplicate pending stream");
        }
        e.generation = e
            .generation
            .checked_add(1)
            .ok_or("registration generation exhausted")?;
        e.state = State::Waiting;
        e.charge = charge;
        e.deadline = deadline;
        e.signalled = false;
        self.queue.push_back(stream);
        self.counts.peak = self.counts.peak.max(self.queue.len());
        Ok(Ticket {
            stream,
            generation: e.generation,
        })
    }
    fn state(&self, t: Ticket) -> Option<State> {
        self.entries
            .get(t.stream)
            .filter(|e| e.generation == t.generation)
            .map(|e| e.state)
    }
    fn enough(&self, charge: usize) -> bool {
        self.options.mode == Mode::Off || self.credit >= charge as u128 * SCALE
    }
    pub fn ready(&mut self, t: Ticket, now: u64) -> bool {
        self.refill(now);
        self.state(t) == Some(State::Waiting)
            && self.entries[t.stream].deadline > now
            && self.queue.front() == Some(&t.stream)
            && self.enough(self.entries[t.stream].charge)
    }
    pub fn expired(&self, t: Ticket, now: u64) -> bool {
        self.state(t) == Some(State::Expired) || self.entries[t.stream].deadline <= now
    }
    pub fn begin(&mut self, t: Ticket, now: u64) -> bool {
        if !self.ready(t, now) {
            return false;
        }
        self.queue.pop_front();
        let e = &mut self.entries[t.stream];
        e.state = State::Reserved;
        if self.options.mode != Mode::Off {
            self.credit -= e.charge as u128 * SCALE;
        }
        true
    }
    pub fn commit(&mut self, t: Ticket) {
        self.commit_at(t, self.last);
    }
    pub fn commit_at(&mut self, t: Ticket, now: u64) {
        assert!(now >= self.last);
        assert_eq!(self.state(t), Some(State::Reserved));
        // A delayed attempt cannot bank refill before its successful admission.
        // For tick control a cross-epoch commit belongs to the current window.
        if self.options.mode == Mode::Tick && now / self.tick != self.epoch {
            self.epoch = now / self.tick;
            self.credit = (self.options.burst - self.entries[t.stream].charge) as u128 * SCALE;
        }
        self.last = now;
        self.counts.committed += 1;
        self.counts.bytes += self.entries[t.stream].charge as u64;
        self.entries[t.stream].state = State::Idle;
    }
    pub fn cancel(&mut self, t: Ticket) {
        match self.state(t) {
            Some(State::Reserved) => {
                self.credit = (self.credit + self.entries[t.stream].charge as u128 * SCALE)
                    .min(self.options.burst as u128 * SCALE);
                self.counts.rollback += 1;
            }
            Some(State::Waiting) => {
                self.queue.retain(|&s| s != t.stream);
                self.counts.cancelled += 1;
            }
            Some(State::Expired) => {}
            _ => return,
        }
        self.entries[t.stream].state = State::Idle;
    }
    // At most PASS expirations/signals. The bounded deadline scan below is cold
    // scheduling work over the configured (<=64) stream table, never core work.
    fn service(&mut self, now: u64) -> [Option<Ticket>; PASS] {
        self.refill(now);
        let mut out = [None; PASS];
        let mut n = 0;
        for &s in &self.queue {
            let e = &mut self.entries[s];
            if e.deadline <= now && n < PASS {
                e.state = State::Expired;
                self.counts.expired += 1;
                out[n] = Some(Ticket {
                    stream: s,
                    generation: e.generation,
                });
                n += 1;
            }
        }
        self.queue
            .retain(|&s| self.entries[s].state == State::Waiting);
        if n < PASS
            && let Some(&s) = self.queue.front()
            && self.enough(self.entries[s].charge)
            && !self.entries[s].signalled
        {
            self.entries[s].signalled = true;
            out[n] = Some(Ticket {
                stream: s,
                generation: self.entries[s].generation,
            });
        }
        out
    }
    pub fn next(&mut self, now: u64) -> Option<u64> {
        self.refill(now);
        let mut next = self.queue.iter().map(|&s| self.entries[s].deadline).min()?;
        let e = &self.entries[*self.queue.front().unwrap()];
        if !e.signalled {
            let eligible = if self.enough(e.charge) {
                now
            } else if self.options.mode == Mode::Tick {
                (now / self.tick)
                    .saturating_add(1)
                    .saturating_mul(self.tick)
            } else {
                now.saturating_add(
                    (e.charge as u128 * SCALE - self.credit)
                        .div_ceil(self.options.rate as u128)
                        .min(u64::MAX as u128) as u64,
                )
            };
            next = next.min(eligible);
        }
        Some(next.max(now))
    }
    pub fn json(&self) -> Value {
        json!({"committed":self.counts.committed,"committed_bytes":self.counts.bytes,"rolled_back":self.counts.rollback,"expired":self.counts.expired,"cancelled":self.counts.cancelled,"waited":self.counts.waited,"peak_pending":self.counts.peak,"pending":self.queue.len()})
    }
}
struct Shared {
    engine: Engine,
    wakers: Vec<Option<Waker>>,
}
pub struct Gate {
    state: RefCell<Shared>,
    changed: tokio::sync::Notify,
    start: tokio::time::Instant,
}
impl Gate {
    pub fn new(o: Options, streams: usize) -> Rc<Self> {
        let streams = if o.mode == Mode::Off { 0 } else { streams };
        Rc::new(Self {
            state: RefCell::new(Shared {
                engine: Engine::new(o, streams),
                wakers: vec![None; streams],
            }),
            changed: tokio::sync::Notify::new(),
            start: tokio::time::Instant::now(),
        })
    }
    pub fn paced(&self) -> bool {
        self.state.borrow().engine.options.mode != Mode::Off
    }
    fn now(&self) -> u64 {
        self.start.elapsed().as_nanos().min(u64::MAX as u128) as u64
    }
    pub fn json(&self) -> Value {
        self.state.borrow().engine.json()
    }
    pub async fn attempt<T, E>(
        &self,
        stream: usize,
        charge: usize,
        deadline: tokio::time::Instant,
        f: impl FnOnce() -> std::result::Result<T, E>,
    ) -> std::result::Result<std::result::Result<T, E>, &'static str> {
        if !self.paced() {
            if tokio::time::Instant::now() >= deadline {
                self.state.borrow_mut().engine.counts.expired += 1;
                return Err("pacing_deadline");
            }
            let result = f();
            let mut state = self.state.borrow_mut();
            if result.is_ok() {
                state.engine.counts.committed += 1;
                state.engine.counts.bytes += charge as u64;
            } else {
                state.engine.counts.rollback += 1;
            }
            return Ok(result);
        }
        let end = deadline
            .saturating_duration_since(self.start)
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        let ticket = self
            .state
            .borrow_mut()
            .engine
            .register(stream, charge, end)?;
        let mut guard = Registration {
            gate: self,
            ticket,
            done: false,
        };
        let mut waited = false;
        std::future::poll_fn(|cx| {
            let now = self.now();
            let mut state = self.state.borrow_mut();
            if state.engine.expired(ticket, now) {
                if state.engine.state(ticket) == Some(State::Waiting) {
                    state.engine.counts.expired += 1;
                    state.engine.entries[stream].state = State::Expired;
                    state.engine.queue.retain(|&s| s != stream);
                }
                return std::task::Poll::Ready(Err("pacing_deadline"));
            }
            if state.engine.begin(ticket, now) {
                state.wakers[stream] = None;
                return std::task::Poll::Ready(Ok(()));
            }
            if !waited {
                waited = true;
                state.engine.counts.waited += 1;
            }
            state.wakers[stream] = Some(cx.waker().clone());
            self.changed.notify_one();
            std::task::Poll::Pending
        })
        .await?;
        // AP-16: no await/callback into application code between debit and commit.
        // f is a private adapter operation: queue preflight + one actual acquire.
        let result = f();
        if result.is_ok() {
            self.state.borrow_mut().engine.commit_at(ticket, self.now());
            guard.done = true;
        }
        Ok(result)
    }
    pub async fn drive(self: Rc<Self>) {
        let sleep = tokio::time::sleep(Duration::from_secs(86400));
        tokio::pin!(sleep);
        loop {
            let now = self.now();
            let (signals, next) = {
                let mut s = self.state.borrow_mut();
                let signals = s.engine.service(now);
                (signals, s.engine.next(now))
            };
            for t in signals.into_iter().flatten() {
                let w = {
                    let mut state = self.state.borrow_mut();
                    if state.engine.state(t).is_some() {
                        state.wakers[t.stream].take()
                    } else {
                        None
                    }
                };
                if let Some(w) = w {
                    w.wake();
                }
            }
            // Runtime timer granularity can round an elapsed deadline. Yield
            // between bounded passes instead of looping inside one poll.
            if next.is_some_and(|t| t <= now) {
                tokio::task::yield_now().await;
                continue;
            }
            sleep.as_mut().reset(
                self.start
                    + Duration::from_nanos(next.unwrap_or(now.saturating_add(86_400_000_000_000))),
            );
            tokio::select! {_=self.changed.notified()=>{},_=sleep.as_mut()=>{}}
        }
    }
}
struct Registration<'a> {
    gate: &'a Gate,
    ticket: Ticket,
    done: bool,
}
impl Drop for Registration<'_> {
    fn drop(&mut self) {
        let mut s = self.gate.state.borrow_mut();
        if s.engine.state(self.ticket).is_some() {
            if !self.done {
                s.engine.cancel(self.ticket);
            }
            s.wakers[self.ticket.stream] = None;
        }
        if !s.engine.queue.is_empty() {
            self.gate.changed.notify_one();
        }
    }
}
pub struct Driver(pub tokio::task::JoinHandle<()>);
impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub async fn before_deadline<T>(
    deadline: Option<tokio::time::Instant>,
    future: impl std::future::Future<Output = std::io::Result<T>>,
) -> std::io::Result<T> {
    if let Some(d) = deadline {
        tokio::pin!(future);
        let checked = std::future::poll_fn(|cx| {
            if tokio::time::Instant::now() >= d {
                return std::task::Poll::Ready(Err(std::io::ErrorKind::TimedOut.into()));
            }
            future.as_mut().poll(cx)
        });
        tokio::time::timeout_at(d, checked)
            .await
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::TimedOut))?
    } else {
        future.await
    }
}

// Explicit modeled storage; allocator/task/socket overhead is reported separately.
pub fn storage_bytes(streams: usize, fanout: usize, o: &Options) -> usize {
    let controller = if o.mode == Mode::Off {
        0
    } else {
        std::mem::size_of::<tokio::time::Sleep>()
            + streams
                * (std::mem::size_of::<Entry>()
                    + std::mem::size_of::<usize>()
                    + std::mem::size_of::<Option<Waker>>())
    };
    std::mem::size_of::<Gate>()
        + 2 * std::mem::size_of::<usize>()
        + controller
        + streams * tcp::storage_bytes(fanout)
}

pub fn allocation_probe(allocator: &stats_alloc::StatsAlloc<std::alloc::System>) -> Value {
    let mut e = Engine::new(
        Options {
            mode: Mode::Bucket,
            rate: 1_000_000,
            burst: 1500,
            return_workers: 1,
            slow_bytes: 1024,
        },
        64,
    );
    let region = stats_alloc::Region::new(allocator);
    for i in 0..100_000u64 {
        let now = i * 2_000_000;
        let t = e.register(i as usize % 64, 1500, now + 1_000_000).unwrap();
        assert!(e.begin(t, now));
        if i % 2 == 0 {
            e.commit(t);
        } else {
            e.cancel(t);
        }
        std::hint::black_box(e.service(now));
    }
    let measured = region.change();
    assert_eq!(
        (
            measured.allocations,
            measured.deallocations,
            measured.reallocations
        ),
        (0, 0, 0)
    );
    let control = stats_alloc::Region::new(allocator);
    drop(std::hint::black_box(vec![1u8; 4096]));
    let control = control.change();
    assert!(control.allocations > 0 && control.deallocations > 0);
    json!({"iterations":100000,"allocations":measured.allocations,"deallocations":measured.deallocations,"reallocations":measured.reallocations,"positive_control":true,"timing_evidence":false,"scope":"prepared Engine registration, debit, rollback, commit and service; excludes runtime I/O/timers", "pacing":e.json()})
}
