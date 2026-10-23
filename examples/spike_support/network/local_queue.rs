//! AP-10: prepared worker-local ownership, no cross-thread queue protocol.
#![forbid(unsafe_code)]

use super::*;
use std::{
    cell::RefCell,
    collections::VecDeque,
    rc::Rc,
    task::{Context, Poll, Waker},
};
use tokio::time::Instant;

struct Entry<T> {
    value: T,
    ready: Instant,
    deadline: Option<Instant>,
}
#[derive(Clone, Copy)]
enum Stage {
    Reserved,
    Transfer,
    Releasing,
}
struct State<T> {
    entries: VecDeque<Entry<T>>,
    capacity: usize,
    reserved: usize,
    transferring: usize,
    releasing: usize,
    sender_open: bool,
    receiver_open: bool,
    waker: Option<Waker>,
    min_age: Option<Duration>,
    adaptive: Option<adaptive::Controller>,
    enqueued: u64,
    transferred: u64,
    evicted: u64,
    failed_replacements: u64,
    cancelled: u64,
    rejected_full: u64,
    high_water: usize,
    age_samples: u64,
    max_age_ns: u64,
}
impl<T> State<T> {
    fn used(&self) -> usize {
        self.entries.len() + self.reserved + self.transferring + self.releasing
    }
    fn counter(&mut self, stage: Stage) -> &mut usize {
        match stage {
            Stage::Reserved => &mut self.reserved,
            Stage::Transfer => &mut self.transferring,
            Stage::Releasing => &mut self.releasing,
        }
    }
    fn age(&mut self, ready: Instant, now: Instant) {
        self.age_samples += 1;
        self.max_age_ns = self.max_age_ns.max(
            now.saturating_duration_since(ready)
                .as_nanos()
                .min(u64::MAX as u128) as u64,
        );
    }
}
type Shared<T> = Rc<RefCell<State<T>>>;
pub(super) struct Sender<T>(Shared<T>);
pub(super) struct Receiver<T>(Shared<T>);
pub(super) struct Observer<T>(Shared<T>);
struct Right<T> {
    state: Shared<T>,
    stage: Stage,
    replacement: bool,
    active: bool,
    epoch: u64,
    keyframe: bool,
}
impl<T> Right<T> {
    fn new(state: &Shared<T>, stage: Stage, replacement: bool) -> Self {
        Self {
            state: state.clone(),
            stage,
            replacement,
            active: true,
            epoch: state.borrow().adaptive.as_ref().map_or(0, |a| a.epoch),
            keyframe: false,
        }
    }
    fn transition(&mut self, stage: Stage) {
        let mut s = self.state.borrow_mut();
        *s.counter(self.stage) -= 1;
        *s.counter(stage) += 1;
        self.stage = stage;
    }
}
impl<T> Drop for Right<T> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let waker = {
            let mut s = self.state.borrow_mut();
            if matches!(self.stage, Stage::Reserved)
                && let Some(a) = &mut s.adaptive
                && self.epoch == a.epoch
            {
                a.gap(Instant::now());
            }
            *s.counter(self.stage) -= 1;
            if self.replacement {
                s.failed_replacements += 1;
            }
            s.waker.take()
        };
        if let Some(w) = waker {
            w.wake();
        }
    }
}
pub(super) struct Permit<T>(Right<T>);
pub(super) struct Delivery<T> {
    value: Option<T>,
    right: Right<T>,
    ready: Instant,
}
impl<T> Drop for Delivery<T> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            self.right.state.borrow_mut().cancelled += 1;
            // The right's field destructor follows the payload, even on unwind.
            drop(value);
        }
    }
}
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Rejected {
    Full,
    Closed,
    Policy,
}

pub(super) fn channel<T>(
    capacity: usize,
    min_age: Option<Duration>,
) -> (Sender<T>, Receiver<T>, Observer<T>) {
    assert!(capacity > 0);
    let s = Rc::new(RefCell::new(State {
        entries: VecDeque::with_capacity(capacity),
        capacity,
        reserved: 0,
        transferring: 0,
        releasing: 0,
        sender_open: true,
        receiver_open: true,
        waker: None,
        min_age,
        adaptive: None,
        enqueued: 0,
        transferred: 0,
        evicted: 0,
        failed_replacements: 0,
        cancelled: 0,
        rejected_full: 0,
        high_water: 0,
        age_samples: 0,
        max_age_ns: 0,
    }));
    (Sender(s.clone()), Receiver(s.clone()), Observer(s))
}
pub(super) fn storage_model_bytes<T>(capacity: usize) -> Option<usize> {
    capacity
        .checked_mul(std::mem::size_of::<Entry<T>>())?
        .checked_add(std::mem::size_of::<State<T>>())?
        .checked_add(2 * std::mem::size_of::<usize>()) // Rc counters, excluding allocator rounding
}
// AP-11: an eviction remains a failed replacement on error or unwind before
// a reservation guard takes responsibility; one attempt may remove several frames.
struct ReplacementAttempt<T> {
    state: Shared<T>,
    before: u64,
    handed_off: bool,
}
impl<T> Drop for ReplacementAttempt<T> {
    fn drop(&mut self) {
        if !self.handed_off {
            let mut s = self.state.borrow_mut();
            if s.evicted > self.before {
                s.failed_replacements += 1;
            }
        }
    }
}
impl<T> Sender<T> {
    pub fn configure(&self, policy: adaptive::AdaptivePolicy) {
        let mut s = self.0.borrow_mut();
        s.adaptive = Some(adaptive::Controller::new(
            policy,
            s.capacity,
            Instant::now(),
        ));
    }
    pub fn gap(&self) {
        if let Some(a) = &mut self.0.borrow_mut().adaptive {
            a.gap(Instant::now());
        }
    }
    pub fn reserve_frame(&self, keyframe: bool) -> std::result::Result<Permit<T>, Rejected> {
        if self.0.borrow().adaptive.is_none() {
            return self.try_reserve();
        }
        {
            let mut s = self.0.borrow_mut();
            s.adaptive.as_mut().unwrap().offer(Instant::now());
        }
        let before = self.0.borrow().evicted;
        let mut attempt = ReplacementAttempt {
            state: self.0.clone(),
            before,
            handed_off: false,
        };
        maintain(&self.0, true);
        let replaced = self.0.borrow().evicted > before;
        let mut s = self.0.borrow_mut();
        let used = s.used();
        let open = s.receiver_open;
        let a = s.adaptive.as_mut().unwrap();
        if !open || a.closed.is_some() {
            return Err(Rejected::Closed);
        }
        if a.cleaning || (a.waiting && !keyframe) {
            a.dependency_skips += 1;
            return Err(Rejected::Policy);
        }
        if used >= a.window {
            a.policy_rejections += 1;
            a.pressure_loss();
            a.gap(Instant::now());
            return Err(Rejected::Policy);
        }
        s.reserved += 1;
        s.high_water = s.high_water.max(s.used());
        drop(s);
        let mut right = Right::new(&self.0, Stage::Reserved, false);
        right.keyframe = keyframe;
        right.replacement = replaced;
        attempt.handed_off = true;
        Ok(Permit(right))
    }
    pub fn try_reserve(&self) -> std::result::Result<Permit<T>, Rejected> {
        let old = {
            let mut s = self.0.borrow_mut();
            if !s.receiver_open {
                return Err(Rejected::Closed);
            }
            if s.used() < s.capacity {
                s.reserved += 1;
                s.high_water = s.high_water.max(s.used());
                drop(s);
                return Ok(Permit(Right::new(&self.0, Stage::Reserved, false)));
            }
            let now = Instant::now();
            if !s.min_age.is_some_and(|age| {
                s.entries
                    .front()
                    .is_some_and(|e| now.saturating_duration_since(e.ready) >= age)
            }) {
                s.rejected_full += 1;
                return Err(Rejected::Full);
            }
            let old = s.entries.pop_front().unwrap();
            s.age(old.ready, now);
            s.evicted += 1;
            s.releasing += 1;
            old.value
        };
        let mut right = Right::new(&self.0, Stage::Releasing, true);
        // AP-10: replacement cannot acquire before the old Drop/refund completes.
        drop(old);
        right.transition(Stage::Reserved);
        Ok(Permit(right))
    }
}
impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let wake = {
            let mut s = self.0.borrow_mut();
            s.sender_open = false;
            s.waker.take()
        };
        if let Some(w) = wake {
            w.wake();
        }
    }
}
impl<T> Permit<T> {
    pub fn send(self, value: T) {
        self.send_at(value, Instant::now(), None);
    }
    fn send_at(mut self, value: T, ready: Instant, deadline: Option<Instant>) {
        let valid = {
            let s = self.0.state.borrow();
            s.receiver_open
                && s.adaptive.as_ref().is_none_or(|a| {
                    a.closed.is_none()
                        && a.epoch == self.0.epoch
                        && !a.cleaning
                        && (!a.waiting || self.0.keyframe)
                })
        };
        if !valid {
            {
                let mut s = self.0.state.borrow_mut();
                s.enqueued += 1;
                s.cancelled += 1;
                if let Some(a) = &mut s.adaptive {
                    a.stale_commits += 1;
                }
            }
            // Never return a credit before dropping rejected ownership.
            drop(value);
            return;
        }
        let wake = {
            let mut s = self.0.state.borrow_mut();
            assert!(s.entries.len() < s.capacity && s.reserved > 0);
            s.entries.push_back(Entry {
                value,
                ready,
                deadline,
            });
            if self.0.keyframe
                && let Some(a) = &mut s.adaptive
            {
                a.committed_key();
            }
            s.reserved -= 1;
            s.enqueued += 1;
            self.0.active = false;
            s.waker.take()
        };
        if let Some(w) = wake {
            w.wake();
        }
    }
}
impl<T> Receiver<T> {
    fn pop(&mut self) -> Option<Delivery<T>> {
        let mut s = self.0.borrow_mut();
        let entry = s.entries.pop_front()?;
        s.age(entry.ready, Instant::now());
        s.transferring += 1;
        drop(s);
        Some(Delivery {
            ready: entry.ready,
            value: Some(entry.value),
            right: Right::new(&self.0, Stage::Transfer, false),
        })
    }
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Option<Delivery<T>>> {
        if let Some(value) = self.pop() {
            return Poll::Ready(Some(value));
        }
        let waker = cx.waker().clone();
        let old = {
            let mut s = self.0.borrow_mut();
            if !s.sender_open && s.reserved == 0 {
                return Poll::Ready(None);
            }
            s.waker.replace(waker)
        };
        drop(old);
        Poll::Pending
    }
}
impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let waker = {
            let mut s = self.0.borrow_mut();
            s.receiver_open = false;
            s.waker.take()
        };
        drop(waker);
        // Cold cancellation, bounded by Q. Release each payload outside the borrow.
        while let Some(value) = self.pop() {
            drop(value);
        }
    }
}
impl<T> Observer<T> {
    pub fn json(&self) -> Value {
        let s = self.0.borrow();
        json!({"capacity":s.capacity,"prepared_capacity":s.entries.capacity(),
            "storage_model_bytes":storage_model_bytes::<T>(s.entries.capacity()),
            "free":s.capacity-s.used(),"reserved":s.reserved,"queued":s.entries.len(),
            "transferring":s.transferring,"releasing":s.releasing,
            "adaptive":s.adaptive.as_ref().map(adaptive::Controller::json),
            "enqueued":s.enqueued,"transferred":s.transferred,"evicted":s.evicted,
            "failed_replacements":s.failed_replacements,"cancelled":s.cancelled,
            "rejected_full":s.rejected_full,"occupancy_high_water":s.high_water,
            "queue_age_samples":s.age_samples,"queue_age_max_ns":s.max_age_ns,
            "oldest_queued_age_ns":s.entries.front().map(|e| Instant::now().saturating_duration_since(e.ready).as_nanos().min(u64::MAX as u128) as u64)})
    }
}
impl writer::Delivery for Delivery<Message> {
    type Frame = WriterFrame;
    fn ready(&self) -> Option<Instant> {
        Some(self.ready)
    }
    fn into_batch(mut self, dst: &mut Option<Message<WriterFrame>>) {
        assert!(dst.is_none());
        *dst = Some(self.value.take().unwrap().into_writer());
        self.right.state.borrow_mut().transferred += 1;
        // self's right is refunded only after the bounded batch owns the frame.
    }
}
impl<T: writer::Delivery> writer::Queue for Receiver<T>
where
    Delivery<T>: writer::Delivery,
{
    type Item = Delivery<T>;
    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        maintain(&self.0, false);
        if blocked(&self.0) {
            if self
                .0
                .borrow()
                .adaptive
                .as_ref()
                .is_some_and(|a| a.closed.is_some())
            {
                return Poll::Ready(None);
            }
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.poll(cx)
    }
    fn close_reason(&self) -> Option<adaptive::CloseReason> {
        self.0.borrow().adaptive.as_ref().and_then(|a| a.closed)
    }
    fn adaptive(&self) -> bool {
        self.0.borrow().adaptive.is_some()
    }
    fn maintain(&mut self) -> Option<adaptive::CloseReason> {
        maintain(&self.0, false);
        self.0.borrow().adaptive.as_ref().and_then(|a| a.closed)
    }
    fn batch_limit(&self, max: usize) -> usize {
        let mut s = self.0.borrow_mut();
        s.adaptive.as_mut().map_or(max, |a| {
            let n = a.batch_limit(max);
            a.last_batch_limit = Some(n);
            n
        })
    }
    fn batch_state(&mut self, ready: Option<Instant>, deadline: Option<Instant>) {
        if let Some(a) = &mut self.0.borrow_mut().adaptive {
            a.batch(ready, deadline, Instant::now());
        }
    }
    fn written(&mut self, bytes: usize) {
        if let Some(a) = &mut self.0.borrow_mut().adaptive {
            a.progress(bytes, Instant::now());
        }
    }
    fn try_recv(&mut self) -> Option<Self::Item> {
        maintain(&self.0, false);
        if blocked(&self.0) {
            return None;
        }
        self.pop()
    }
}

// AP-11: every removed payload is guarded before Drop, outside the queue borrow.
fn maintain<T>(state: &Shared<T>, reserving: bool) {
    if state.borrow().adaptive.is_none() {
        return;
    }
    let now = Instant::now();
    {
        let mut s = state.borrow_mut();
        let head = s.entries.front().map(|e| (e.ready, e.deadline));
        let used = s.used();
        let a = s.adaptive.as_mut().unwrap();
        if let Some((ready, deadline)) = head {
            a.age(ready, now);
            if deadline.is_some_and(|d| now >= d) {
                a.close(adaptive::CloseReason::FrameDeadline);
            }
        }
        a.sample(now, used);
    }
    for _ in 0..8 {
        let old = {
            let mut s = state.borrow_mut();
            let used = s.used();
            let ready = s.entries.front().map(|e| e.ready);
            let a = s.adaptive.as_mut().unwrap();
            if a.closed.is_some() {
                break;
            }
            let Some(ready) = ready else {
                a.cleaning = false;
                break;
            };
            if a.cleaning {
                a.dependency_evictions += 1;
            } else if now.saturating_duration_since(ready) >= a.policy.target {
                a.age_evictions += 1;
                a.pressure_loss();
                a.gap(now);
            } else if used > a.window || (reserving && used == a.window) {
                a.window_evictions += 1;
                a.pressure_loss();
                a.gap(now);
            } else {
                break;
            }
            s.evicted += 1;
            s.releasing += 1;
            s.entries.pop_front().unwrap().value
        };
        let right = Right::new(state, Stage::Releasing, false);
        drop(old);
        drop(right);
    }
    let mut s = state.borrow_mut();
    if s.entries.is_empty() {
        s.adaptive.as_mut().unwrap().cleaning = false;
    }
}
fn blocked<T>(state: &Shared<T>) -> bool {
    let s = state.borrow();
    s.adaptive.as_ref().is_some_and(|a| {
        a.closed.is_some()
            || a.cleaning
            || s.used() > a.window
            || s.entries.front().is_some_and(|e| {
                Instant::now().saturating_duration_since(e.ready) >= a.policy.target
            })
    })
}

// The common ingress dispatch keeps the ordinary Tokio queue unchanged.
pub(super) enum IngressSender {
    Tokio(mpsc::Sender<Message>),
    Local(Sender<Message>),
}
impl From<mpsc::Sender<Message>> for IngressSender {
    fn from(s: mpsc::Sender<Message>) -> Self {
        Self::Tokio(s)
    }
}
pub(super) enum IngressPermit<'a> {
    Tokio(mpsc::Permit<'a, Message>),
    Local(Permit<Message>),
}
impl IngressSender {
    pub fn gap(&self) {
        if let Self::Local(s) = self {
            s.gap();
        }
    }
    pub fn reserve_frame(
        &self,
        keyframe: bool,
    ) -> std::result::Result<IngressPermit<'_>, Rejected> {
        match self {
            Self::Local(s) => s.reserve_frame(keyframe).map(IngressPermit::Local),
            _ => self.try_reserve(),
        }
    }
    pub fn try_reserve(&self) -> std::result::Result<IngressPermit<'_>, Rejected> {
        match self {
            Self::Tokio(s) => s
                .try_reserve()
                .map(IngressPermit::Tokio)
                .map_err(|e| match e {
                    mpsc::error::TrySendError::Full(_) => Rejected::Full,
                    mpsc::error::TrySendError::Closed(_) => Rejected::Closed,
                }),
            Self::Local(s) => s.try_reserve().map(IngressPermit::Local),
        }
    }
}
impl IngressPermit<'_> {
    pub fn send(self, value: Message) {
        self.send_ready(value, Instant::now());
    }
    pub fn send_ready(self, value: Message, ready: Instant) {
        match self {
            Self::Tokio(p) => p.send(value),
            Self::Local(p) => {
                let deadline = value.deadline;
                p.send_at(value, ready, deadline);
            }
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(feature = "mosaic-spike")]
pub(super) fn allocation_probe(
    allocator: &stats_alloc::StatsAlloc<std::alloc::System>,
    iterations: usize,
    adaptive: bool,
) -> Result<Value> {
    let fixture = Fixture::new(
        "mosaic",
        vec![ClassConfig {
            capacity: 64,
            slots: 4,
        }],
        1,
        None,
    )?;
    let origin = fixture.open(256)?;
    let (tx, mut rx, observer) = channel(2, Some(Duration::ZERO));
    if adaptive {
        tx.configure(adaptive::AdaptivePolicy {
            target: Duration::from_secs(3600),
            grace: Duration::from_secs(3600),
            stall: Duration::from_secs(3600),
            key_wait: Duration::from_secs(3600),
            keyed: false,
        });
    }
    let region = stats_alloc::Region::new(allocator);
    for _ in 0..iterations {
        let mut cx = Context::from_waker(Waker::noop());
        assert!(rx.poll(&mut cx).is_pending());
        for id in 0..3 {
            let permit = tx.reserve_frame(false).unwrap();
            let mut lease = origin.acquire(4)?;
            lease.put(&[id as u8; 4]);
            permit.send(Message {
                header: Header {
                    id,
                    len: 4,
                    due: 0,
                    ..Header::default()
                },
                frame: lease.freeze().into(),
                deadline: None,
            });
        }
        for _ in 0..2 {
            let mut batch = None;
            writer::Delivery::into_batch(rx.pop().unwrap(), &mut batch);
            drop(batch);
        }
    }
    let costs = region.change();
    assert_eq!(
        (costs.allocations, costs.deallocations, costs.reallocations),
        (0, 0, 0)
    );
    drop((tx, rx, origin));
    fixture.finish()?;
    Ok(
        json!({"iterations":iterations,"drained":true,"queue":observer.json(),
        "allocations":costs.allocations,"deallocations":costs.deallocations,"reallocations":costs.reallocations}),
    )
}

#[cfg(test)]
mod adaptive_tests;
