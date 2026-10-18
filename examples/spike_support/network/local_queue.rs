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
}
impl<T> Right<T> {
    fn new(state: &Shared<T>, stage: Stage, replacement: bool) -> Self {
        Self {
            state: state.clone(),
            stage,
            replacement,
            active: true,
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
impl<T> Sender<T> {
    pub fn try_reserve(&self) -> std::result::Result<Permit<T>, Rejected> {
        let old = {
            let mut s = self.0.borrow_mut();
            if !s.receiver_open {
                return Err(Rejected::Closed);
            }
            if s.used() < s.capacity {
                s.reserved += 1;
                s.high_water = s.high_water.max(s.used());
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
    pub fn send(mut self, value: T) {
        if !self.0.state.borrow().receiver_open {
            {
                let mut s = self.0.state.borrow_mut();
                s.enqueued += 1;
                s.cancelled += 1;
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
                ready: Instant::now(),
            });
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
        Some(Delivery {
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
            "enqueued":s.enqueued,"transferred":s.transferred,"evicted":s.evicted,
            "failed_replacements":s.failed_replacements,"cancelled":s.cancelled,
            "rejected_full":s.rejected_full,"occupancy_high_water":s.high_water,
            "queue_age_samples":s.age_samples,"queue_age_max_ns":s.max_age_ns,
            "oldest_queued_age_ns":s.entries.front().map(|e| Instant::now().saturating_duration_since(e.ready).as_nanos().min(u64::MAX as u128) as u64)})
    }
}
impl writer::Delivery for Delivery<Message> {
    type Frame = WriterFrame;
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
        self.poll(cx)
    }
    fn try_recv(&mut self) -> Option<Self::Item> {
        self.pop()
    }
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
        match self {
            Self::Tokio(p) => p.send(value),
            Self::Local(p) => p.send(value),
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(feature = "mosaic-spike")]
pub(super) fn allocation_probe(
    allocator: &stats_alloc::StatsAlloc<std::alloc::System>,
    iterations: usize,
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
    let region = stats_alloc::Region::new(allocator);
    for _ in 0..iterations {
        let mut cx = Context::from_waker(Waker::noop());
        assert!(rx.poll(&mut cx).is_pending());
        for id in 0..3 {
            let permit = tx.try_reserve().unwrap();
            let mut lease = origin.acquire(4)?;
            lease.put(&[id as u8; 4]);
            permit.send(Message {
                header: Header { id, len: 4, due: 0 },
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
