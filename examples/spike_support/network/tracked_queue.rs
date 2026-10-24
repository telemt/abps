//! AP-19: queue-stage receipts; never an authority for buffer reclamation.
#![forbid(unsafe_code)]
use super::*;

#[derive(Default)]
pub(super) struct Counts {
    enqueued: AtomicU64,
    transferred: AtomicU64,
    cancelled: AtomicU64,
}
impl Counts {
    pub fn json(&self) -> Value {
        json!({"enqueued":self.enqueued.load(Ordering::Relaxed),
            "transferred":self.transferred.load(Ordering::Relaxed),
            "cancelled":self.cancelled.load(Ordering::Relaxed)})
    }
}
pub(super) struct Entry<T> {
    value: Option<T>,
    counts: Arc<Counts>,
}
impl<T> Entry<T> {
    fn new(value: T, counts: Arc<Counts>) -> Self {
        counts.enqueued.fetch_add(1, Ordering::Relaxed);
        Self {
            value: Some(value),
            counts,
        }
    }
}
impl<T> Drop for Entry<T> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            // This scope guard records completion even if a test payload unwinds.
            struct Cancel<'a>(&'a AtomicU64);
            impl Drop for Cancel<'_> {
                fn drop(&mut self) {
                    self.0.fetch_add(1, Ordering::Relaxed);
                }
            }
            let _finished = Cancel(&self.counts.cancelled);
            drop(value);
        }
    }
}
impl writer::Delivery for Entry<Message> {
    type Frame = WriterFrame;
    fn into_batch(mut self, dst: &mut Option<Message<WriterFrame>>) {
        // All validation precedes the non-unwinding ownership move. No callbacks.
        assert!(dst.is_none());
        let message = self.value.take().expect("queue receipt owns its message");
        *dst = Some(message.into_writer());
        self.counts.transferred.fetch_add(1, Ordering::Relaxed);
    }
}
pub(super) struct Sender<T> {
    inner: mpsc::Sender<Entry<T>>,
    counts: Arc<Counts>,
}
pub(super) struct Permit<'a, T> {
    inner: mpsc::Permit<'a, Entry<T>>,
    counts: Arc<Counts>,
}
pub(super) struct OwnedPermit<T> {
    inner: mpsc::OwnedPermit<Entry<T>>,
    counts: Arc<Counts>,
}
impl<T> Permit<'_, T> {
    pub fn send(self, value: T) {
        self.inner.send(Entry::new(value, self.counts));
    }
}
impl<T> OwnedPermit<T> {
    pub fn send(self, value: T) {
        self.inner.send(Entry::new(value, self.counts));
    }
}
fn rejected<T>(e: mpsc::error::TrySendError<T>) -> local_queue::Rejected {
    match e {
        mpsc::error::TrySendError::Full(_) => local_queue::Rejected::Full,
        mpsc::error::TrySendError::Closed(_) => local_queue::Rejected::Closed,
    }
}
impl<T> Sender<T> {
    pub fn try_reserve(&self) -> std::result::Result<Permit<'_, T>, local_queue::Rejected> {
        Ok(Permit {
            inner: self.inner.try_reserve().map_err(rejected)?,
            counts: self.counts.clone(),
        })
    }
    pub fn try_reserve_owned(&self) -> std::result::Result<OwnedPermit<T>, local_queue::Rejected> {
        Ok(OwnedPermit {
            inner: self.inner.clone().try_reserve_owned().map_err(rejected)?,
            counts: self.counts.clone(),
        })
    }
}
pub(super) type Receiver<T> = mpsc::Receiver<Entry<T>>;
pub(super) fn channel<T>(n: usize) -> (Sender<T>, Receiver<T>, Arc<Counts>) {
    let (inner, receiver) = mpsc::channel(n);
    let counts = Arc::new(Counts::default());
    (
        Sender {
            inner,
            counts: counts.clone(),
        },
        receiver,
        counts,
    )
}
pub(super) fn storage_model_bytes<T>(n: usize) -> Option<usize> {
    n.checked_mul(std::mem::size_of::<Entry<T>>())?.checked_add(
        std::mem::size_of::<Counts>()
            + 2 * std::mem::size_of::<usize>()
            + std::mem::size_of::<(usize, usize, Arc<Counts>)>(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn ap19_partial_write_cancellation_separates_queue_and_batch() {
        use super::super::scripted_io::{ScriptedIo, Step};
        use std::{future::Future, task::Poll};
        for backend in ["berry", "bytes"] {
            let fixture = Fixture::new(
                backend,
                vec![ClassConfig {
                    capacity: 16,
                    slots: 2,
                }],
                1,
                None,
            )
            .unwrap();
            let origin = fixture.open(32).unwrap();
            let (tx, rx, c) = channel(2);
            let (cancel, stop) = oneshot::channel();
            for id in 0..2 {
                let mut b = origin.acquire(4).unwrap();
                b.put(b"abcd");
                tx.try_reserve().unwrap().send(Message {
                    header: Header {
                        id,
                        len: 4,
                        ..Header::default()
                    },
                    frame: b.freeze().into(),
                    deadline: None,
                });
            }
            let counts = Arc::new(writer::WriterCounts::default());
            let socket = ScriptedIo::new(b"", [Step::Data(3), Step::Stall]);
            let future = writer::run_io(socket, rx, stop, counts.clone(), 1);
            tokio::pin!(future);
            assert!(
                std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            assert_eq!(c.transferred.load(Ordering::Relaxed), 1);
            cancel.send(()).unwrap();
            future.await.unwrap();
            drop(tx);
            assert_eq!(
                c.json(),
                json!({"enqueued":2,"transferred":1,"cancelled":1})
            );
            assert_eq!(counts.aborted_cancel.load(Ordering::Relaxed), 1);
            assert_eq!(counts.completed.load(Ordering::Relaxed), 0);
            assert_eq!(fixture.snapshot()["admitted"], 0);
            drop(origin);
            fixture.finish().unwrap();
        }
    }
    #[test]
    fn ap19_invalid_batch_destination_preserves_queue_ownership() {
        use writer::Delivery;
        let fixture = Fixture::new(
            "berry",
            vec![ClassConfig {
                capacity: 16,
                slots: 2,
            }],
            1,
            None,
        )
        .unwrap();
        let origin = fixture.open(32).unwrap();
        let make = || {
            let mut b = origin.acquire(1).unwrap();
            b.put(b"x");
            Message {
                header: Header {
                    len: 1,
                    ..Header::default()
                },
                frame: b.freeze().into(),
                deadline: None,
            }
        };
        let mut dst = Some(make().into_writer());
        let (tx, mut rx, c) = channel(1);
        tx.try_reserve().unwrap().send(make());
        let item = rx.try_recv().unwrap();
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| item.into_batch(&mut dst)))
                .is_err()
        );
        assert_eq!(dst.as_ref().unwrap().frame.as_ref(), b"x");
        assert_eq!(
            c.json(),
            json!({"enqueued":1,"transferred":0,"cancelled":1})
        );
        drop((dst, tx, rx, origin));
        fixture.finish().unwrap();
    }
    #[test]
    fn ap19_permits_are_not_frames_and_closed_receiver_cancels_late_send() {
        let (tx, rx, c) = channel::<usize>(2);
        drop(tx.try_reserve().unwrap());
        assert_eq!(
            c.json(),
            json!({"enqueued":0,"transferred":0,"cancelled":0})
        );
        let late = tx.try_reserve_owned().unwrap();
        tx.try_reserve().unwrap().send(1);
        drop(rx);
        late.send(2);
        // Tokio may retain a late send until the remaining senders disappear.
        drop(tx);
        assert_eq!(
            c.json(),
            json!({"enqueued":2,"transferred":0,"cancelled":2})
        );
    }
    #[test]
    fn ap19_dequeued_receipt_cancels_until_installed_in_batch() {
        let (tx, mut rx, c) = channel::<usize>(1);
        tx.try_reserve().unwrap().send(7);
        let item = rx.try_recv().unwrap();
        assert_eq!(c.cancelled.load(Ordering::Relaxed), 0);
        drop(item);
        assert_eq!(c.cancelled.load(Ordering::Relaxed), 1);
    }
    #[test]
    fn ap19_payload_drop_precedes_cancel_observation_even_on_unwind() {
        struct Payload(Arc<Counts>, bool);
        impl Drop for Payload {
            fn drop(&mut self) {
                assert_eq!(self.0.cancelled.load(Ordering::Relaxed), 0);
                assert!(!self.1, "payload unwind");
            }
        }
        for panic in [false, true] {
            let (tx, rx, c) = channel(1);
            tx.try_reserve().unwrap().send(Payload(c.clone(), panic));
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(rx)));
            assert_eq!(result.is_err(), panic);
            assert_eq!(c.cancelled.load(Ordering::Relaxed), 1);
        }
    }
    #[test]
    fn ap19_remote_receiver_drop_conserves_outstanding_permit() {
        let (tx, rx, c) = channel::<usize>(2);
        let p = tx.try_reserve_owned().unwrap();
        tx.try_reserve().unwrap().send(1);
        std::thread::scope(|s| {
            s.spawn(|| drop(rx));
            p.send(2);
        });
        drop(tx);
        assert_eq!(
            c.json(),
            json!({"enqueued":2,"transferred":0,"cancelled":2})
        );
    }
}
