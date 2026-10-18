use super::*;
use std::{future::Future, pin::Pin, task::Poll};
use tokio::io::AsyncWrite;

#[derive(Default)]
pub(super) struct WriterCounts {
    pub completed: AtomicU64,
    pub write_calls: AtomicU64,
    pub expired: AtomicU64,
}
async fn write_before_deadline<W: AsyncWrite + Unpin>(
    socket: &mut W,
    slices: &[IoSlice<'_>],
    deadline: Option<tokio::time::Instant>,
) -> std::io::Result<usize> {
    let timer = async {
        if let Some(at) = deadline {
            tokio::time::sleep_until(at).await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    tokio::pin!(timer);
    // SI-06: timeout_at polls its inner future first. Check before *each* socket
    // poll, including wakes after Pending and immediately ready writes.
    std::future::poll_fn(|cx| {
        if deadline.is_some_and(|d| tokio::time::Instant::now() >= d)
            || timer.as_mut().poll(cx).is_ready()
        {
            return Poll::Ready(Err(std::io::ErrorKind::TimedOut.into()));
        }
        Pin::new(&mut *socket).poll_write_vectored(cx, slices)
    })
    .await
}
async fn batch<W: AsyncWrite + Unpin, F: AsRef<[u8]>>(
    socket: &mut W,
    messages: &mut [Option<Message<F>>; 8],
    count: usize,
    counters: &WriterCounts,
) -> std::io::Result<()> {
    let headers: [[u8; HEADER]; 8] = std::array::from_fn(|i| {
        messages[i]
            .as_ref()
            .map_or([0; HEADER], |m| m.header.encode())
    });
    let mut current = 0;
    let mut offset = 0;
    while current < count {
        let mut slices = [IoSlice::new(&[]); 16];
        let mut n = 0;
        let mut pending = 0;
        for i in current..count {
            let payload = messages[i].as_ref().unwrap().frame.as_ref();
            let offset = if i == current { offset } else { 0 };
            if offset < HEADER {
                slices[n] = IoSlice::new(&headers[i][offset..]);
                n += 1;
                pending += HEADER - offset;
            }
            let payload_offset = offset.saturating_sub(HEADER);
            if payload_offset < payload.len() {
                slices[n] = IoSlice::new(&payload[payload_offset..]);
                n += 1;
                pending += payload.len() - payload_offset;
            }
        }
        counters.write_calls.fetch_add(1, Ordering::Relaxed);
        let deadline = messages[current..count]
            .iter()
            .flatten()
            .filter_map(|m| m.deadline)
            .min();
        let written = write_before_deadline(socket, &slices[..n], deadline).await?;
        if written == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        if written > pending {
            return Err(std::io::ErrorKind::InvalidData.into());
        }
        // End all borrowed descriptors before releasing any frame.
        let mut remaining = written;
        while current < count {
            let size = HEADER + messages[current].as_ref().unwrap().frame.as_ref().len();
            if remaining < size - offset {
                offset += remaining;
                break;
            }
            remaining -= size - offset;
            messages[current].take(); // release now, even if a later write stalls
            counters.completed.fetch_add(1, Ordering::Relaxed);
            current += 1;
            offset = 0;
        }
    }
    Ok(())
}
pub(super) async fn run(
    job: WriterJob,
    counters: Arc<WriterCounts>,
    max_batch: usize,
) -> std::io::Result<()> {
    let WriterJob {
        socket,
        queue,
        cancel,
    } = job;
    run_io(into_async(socket)?, queue, cancel, counters, max_batch).await
}
// AP-07: a delivery chooses its local frame type and transfers into an already
// bounded batch slot before releasing any queue credit.
pub(super) trait Delivery {
    type Frame: AsRef<[u8]>;
    fn into_batch(self, dst: &mut Option<Message<Self::Frame>>);
}
impl Delivery for Message {
    type Frame = WriterFrame;
    fn into_batch(self, dst: &mut Option<Message<WriterFrame>>) {
        *dst = Some(self.into_writer());
    }
}
pub(super) trait Queue {
    type Item: Delivery;
    fn poll_recv(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>>;
    fn try_recv(&mut self) -> Option<Self::Item>;
}
impl<M: Delivery> Queue for mpsc::Receiver<M> {
    type Item = M;
    fn poll_recv(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Option<M>> {
        mpsc::Receiver::poll_recv(self, cx)
    }
    fn try_recv(&mut self) -> Option<M> {
        mpsc::Receiver::try_recv(self).ok()
    }
}
pub(super) async fn run_io<W: AsyncWrite + Unpin, Q: Queue>(
    mut socket: W,
    mut queue: Q,
    cancel: oneshot::Receiver<()>,
    counters: Arc<WriterCounts>,
    max_batch: usize,
) -> std::io::Result<()> {
    let cancellation = async {
        if cancel.await.is_err() {
            std::future::pending::<()>().await;
        }
    };
    tokio::pin!(cancellation);
    tokio::select! {
        _=&mut cancellation=>Ok(()),
        result=async {
            while let Some(message)=std::future::poll_fn(|cx| queue.poll_recv(cx)).await {
                let mut messages = std::array::from_fn(|_| None);
                message.into_batch(&mut messages[0]);
                let mut count = 1;
                while count < max_batch {
                    match queue.try_recv() {
                        Some(message) => { message.into_batch(&mut messages[count]); count += 1; }
                        None => break,
                    }
                }
                if let Err(error) = batch(&mut socket, &mut messages, count, &counters).await {
                    if error.kind() == std::io::ErrorKind::TimedOut { counters.expired.fetch_add(1, Ordering::Relaxed); }
                    return Err(error); // closes socket and drops batch + queued ownership
                }
            }
            socket.shutdown().await
        }=>result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn abps_scripted_writer_error_cleanup_preserves_exact_wire_prefix() {
        use super::super::scripted_io::{ScriptedIo, Step};
        use std::io::ErrorKind;
        let backends = if cfg!(feature = "mosaic-spike") {
            vec!["berry", "mosaic"]
        } else {
            vec!["berry"]
        };
        for backend in backends {
            for branches in [false, true] {
                if branches && !(backend == "mosaic" && cfg!(feature = "local-branches-spike")) {
                    continue;
                }
                for batch_size in [1, 2, 8] {
                    for cut in [0, 1, HEADER - 1, HEADER, HEADER + 1, HEADER + 4, HEADER + 5] {
                        for terminal in [
                            Step::Error(ErrorKind::BrokenPipe),
                            Step::Error(ErrorKind::Interrupted),
                            Step::Zero,
                            Step::OverReport,
                        ] {
                            let fixture = Fixture::prepared_branches(
                                backend,
                                vec![ClassConfig {
                                    capacity: 16,
                                    slots: 4,
                                }],
                                2,
                                None,
                                CapacityPolicy::Unreserved,
                                false,
                                usize::from(branches),
                            )
                            .unwrap();
                            let origin = fixture.open(48).unwrap();
                            let other = fixture.open(16).unwrap();
                            let mut neighbor = other.acquire(1).unwrap();
                            neighbor.put(&[0xa7]);
                            let neighbor = neighbor.freeze();
                            let (sender, receiver) = mpsc::channel(3);
                            let (_cancel, stop) = oneshot::channel();
                            let mut expected = Vec::new();
                            for id in 0..3 {
                                let mut lease = origin.acquire(4).unwrap();
                                lease.put(&[id as u8; 4]);
                                let header = Header { id, len: 4, due: 0 };
                                expected.extend_from_slice(&header.encode());
                                expected.extend_from_slice(&[id as u8; 4]);
                                let frame = {
                                    #[cfg(feature = "local-branches-spike")]
                                    if branches {
                                        ingress::branch_views(lease, 1).unwrap()[0].take().unwrap()
                                    } else {
                                        lease.freeze().into()
                                    }
                                    #[cfg(not(feature = "local-branches-spike"))]
                                    {
                                        lease.freeze().into()
                                    }
                                };
                                assert!(
                                    sender
                                        .try_send(Message {
                                            header,
                                            frame,
                                            deadline: None
                                        })
                                        .is_ok()
                                );
                            }
                            let mut steps = Vec::new();
                            if cut > 0 {
                                steps.push(Step::Data(cut));
                            }
                            steps.extend([Step::Pending, terminal]);
                            let socket = ScriptedIo::new(b"", steps);
                            let output = socket.output.clone();
                            let counts = Arc::new(WriterCounts::default());
                            let error = run_io(socket, receiver, stop, counts.clone(), batch_size)
                                .await
                                .unwrap_err()
                                .kind();
                            assert_eq!(
                                error,
                                match terminal {
                                    Step::Error(e) => e,
                                    Step::Zero => ErrorKind::WriteZero,
                                    _ => ErrorKind::InvalidData,
                                }
                            );
                            let accepted = cut.min(batch_size * (HEADER + 4));
                            assert_eq!(
                                &*output.borrow(),
                                &expected[..accepted],
                                "IO-02 no replay, omission or boundary corruption"
                            );
                            assert_eq!(
                                counts.completed.load(Ordering::Relaxed),
                                (accepted / (HEADER + 4)) as u64
                            );
                            assert!(sender.is_closed());
                            assert_eq!(
                                fixture.snapshot()["admitted"],
                                16,
                                "IO-02 all failed-writer ownership returned"
                            );
                            assert_eq!(neighbor.as_ref(), &[0xa7]);
                            drop((neighbor, origin, other));
                            fixture.finish().unwrap();
                        }
                    }
                }
            }
        }
    }
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };
    struct ShortWriter {
        bytes: Vec<u8>,
        max: usize,
        stop: usize,
    }
    #[cfg(feature = "local-branches-spike")]
    #[tokio::test(start_paused = true)]
    async fn br06_cancel_and_expire_release_bound_and_unbound_branches() {
        for expire in [false, true] {
            let fixture = Fixture::prepared_branches(
                "mosaic",
                vec![ClassConfig {
                    capacity: 16,
                    slots: 3,
                }],
                1,
                None,
                CapacityPolicy::Unreserved,
                false,
                1,
            )
            .unwrap();
            let origin = fixture.open(48).unwrap();
            let (sender, receiver) = mpsc::channel(3);
            let (cancel, stop) = oneshot::channel();
            for id in 0..3 {
                let mut lease = origin.acquire(4).unwrap();
                lease.put(b"abcd");
                let mut ready = ingress::branch_views(lease, 1).unwrap();
                assert!(
                    sender
                        .try_send(Message {
                            header: Header { id, len: 4, due: 0 },
                            frame: ready[0].take().unwrap(),
                            deadline: Some(
                                tokio::time::Instant::now() + Duration::from_millis(100)
                            ),
                        })
                        .is_ok()
                );
            }
            let counts = Arc::new(WriterCounts::default());
            let future = run_io(
                ShortWriter {
                    bytes: Vec::new(),
                    max: 2,
                    stop: 3,
                },
                receiver,
                stop,
                counts.clone(),
                2,
            );
            tokio::pin!(future);
            assert!(
                std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            assert_eq!(fixture.snapshot()["admitted"], 48);
            if expire {
                tokio::time::advance(Duration::from_millis(101)).await;
                assert_eq!(
                    future.await.unwrap_err().kind(),
                    std::io::ErrorKind::TimedOut
                );
                assert_eq!(counts.expired.load(Ordering::Relaxed), 1);
            } else {
                cancel.send(()).unwrap();
                future.await.unwrap();
            }
            assert_eq!(fixture.snapshot()["admitted"], 0);
            drop((sender, origin));
            fixture.finish().unwrap();
        }
    }
    impl AsyncWrite for ShortWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            if self.bytes.len() >= self.stop {
                return Poll::Pending;
            }
            let n = buf.len().min(self.max).min(self.stop - self.bytes.len());
            self.bytes.extend_from_slice(&buf[..n]);
            Poll::Ready(Ok(n))
        }
        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<std::io::Result<usize>> {
            if self.bytes.len() >= self.stop {
                return Poll::Pending;
            }
            let mut remaining = self.max.min(self.stop - self.bytes.len());
            let mut written = 0;
            for buf in bufs {
                let n = remaining.min(buf.len());
                self.bytes.extend_from_slice(&buf[..n]);
                remaining -= n;
                written += n;
                if remaining == 0 {
                    break;
                }
            }
            Poll::Ready(Ok(written))
        }
        fn is_write_vectored(&self) -> bool {
            true
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
    #[tokio::test(start_paused = true)]
    async fn expired_ready_socket_is_never_polled_and_pending_poll_rechecks_deadline() {
        let now = tokio::time::Instant::now();
        let mut socket = ShortWriter {
            bytes: Vec::new(),
            max: 32,
            stop: usize::MAX,
        };
        assert_eq!(
            write_before_deadline(&mut socket, &[IoSlice::new(b"expired")], Some(now))
                .await
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::TimedOut
        );
        assert!(socket.bytes.is_empty());
        socket.stop = 0;
        let at = now + Duration::from_millis(100);
        {
            let slices = [IoSlice::new(b"pending")];
            let write = write_before_deadline(&mut socket, &slices, Some(at));
            tokio::pin!(write);
            assert!(
                std::future::poll_fn(|cx| Poll::Ready(write.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            tokio::time::advance(Duration::from_millis(100)).await;
            assert_eq!(
                write.await.unwrap_err().kind(),
                std::io::ErrorKind::TimedOut
            );
        }
        assert!(socket.bytes.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn partial_frame_expiry_closes_writer_and_releases_batch_and_queue() {
        let fixture = Fixture::new(
            "berry",
            vec![ClassConfig {
                capacity: 16,
                slots: 3,
            }],
            1,
            None,
        )
        .unwrap();
        let origin = fixture.open(48).unwrap();
        let (sender, receiver) = mpsc::channel(3);
        let (_cancel, stop) = oneshot::channel();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(100);
        for id in 0..3 {
            let mut b = origin.acquire(4).unwrap();
            b.put(b"abcd");
            assert!(
                sender
                    .try_send(Message {
                        header: Header { len: 4, id, due: 0 },
                        frame: b.freeze().into(),
                        deadline: Some(deadline)
                    })
                    .is_ok()
            );
        }
        let counts = Arc::new(WriterCounts::default());
        let socket = ShortWriter {
            bytes: Vec::new(),
            max: 2,
            stop: 3,
        }; // incomplete first header
        let future = run_io(socket, receiver, stop, counts.clone(), 2);
        tokio::pin!(future);
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        assert_eq!(fixture.snapshot()["admitted"], 48);
        tokio::time::advance(Duration::from_millis(100)).await;
        assert_eq!(
            future.await.unwrap_err().kind(),
            std::io::ErrorKind::TimedOut
        );
        assert!(sender.is_closed());
        assert_eq!(counts.expired.load(Ordering::Relaxed), 1);
        assert_eq!(counts.completed.load(Ordering::Relaxed), 0);
        assert_eq!(fixture.snapshot()["admitted"], 0);
        drop(origin);
        fixture.finish().unwrap();
    }

    #[tokio::test]
    async fn cancellation_drops_writer_batch_and_queued_ownership() {
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
        let (sender, receiver) = mpsc::channel(2);
        let (cancel, stop) = oneshot::channel();
        for id in 0..2 {
            let mut b = origin.acquire(4).unwrap();
            b.put(b"abcd");
            assert!(
                sender
                    .try_send(Message {
                        header: Header { len: 4, id, due: 0 },
                        frame: b.freeze().into(),
                        deadline: None
                    })
                    .is_ok()
            );
        }
        let socket = ShortWriter {
            bytes: Vec::new(),
            max: 2,
            stop: 3,
        };
        let future = run_io(socket, receiver, stop, Arc::new(WriterCounts::default()), 1);
        tokio::pin!(future);
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        cancel.send(()).unwrap();
        future.await.unwrap();
        assert!(sender.is_closed());
        assert_eq!(fixture.snapshot()["admitted"], 0);
        drop(origin);
        fixture.finish().unwrap();
    }
    #[tokio::test]
    async fn partial_writes_release_completed_frames_before_stalled_tail() {
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
        let mut messages: [Option<Message>; 8] = std::array::from_fn(|_| None);
        for (i, slot) in messages[..2].iter_mut().enumerate() {
            let mut b = origin.acquire(4).unwrap();
            b.put(&[i as u8; 4]);
            *slot = Some(Message {
                header: Header {
                    len: 4,
                    id: i as u64,
                    due: 0,
                },
                frame: b.freeze().into(),
                deadline: None,
            });
        }
        let counts = WriterCounts::default();
        let mut socket = ShortWriter {
            bytes: Vec::new(),
            max: 3,
            stop: HEADER + 4,
        };
        {
            let future = batch(&mut socket, &mut messages, 2, &counts);
            tokio::pin!(future);
            assert!(
                std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
        }
        assert!(messages[0].is_none());
        assert!(messages[1].is_some());
        assert_eq!(fixture.snapshot()["admitted"], 16);
        assert_eq!(counts.completed.load(Ordering::Relaxed), 1);
        drop((messages, origin));
        fixture.finish().unwrap();
    }
    #[tokio::test]
    async fn vectored_batches_preserve_headers_and_payload_on_short_writes() {
        let fixture = Fixture::new(
            "berry",
            vec![ClassConfig {
                capacity: 16,
                slots: 8,
            }],
            1,
            None,
        )
        .unwrap();
        let origin = fixture.open(128).unwrap();
        let mut messages: [Option<Message>; 8] = std::array::from_fn(|_| None);
        let mut expected = Vec::new();
        for (i, slot) in messages.iter_mut().enumerate() {
            let mut b = origin.acquire(4).unwrap();
            b.put(&[i as u8; 4]);
            let header = Header {
                len: 4,
                id: i as u64,
                due: 0,
            };
            expected.extend_from_slice(&header.encode());
            expected.extend_from_slice(&[i as u8; 4]);
            *slot = Some(Message {
                header,
                frame: b.freeze().into(),
                deadline: None,
            });
        }
        let mut socket = ShortWriter {
            bytes: Vec::new(),
            max: 29,
            stop: usize::MAX,
        };
        let counters = WriterCounts::default();
        batch(&mut socket, &mut messages, 8, &counters)
            .await
            .unwrap();
        assert_eq!(socket.bytes, expected);
        assert_eq!(counters.completed.load(Ordering::Relaxed), 8);
        assert!(messages.iter().all(Option::is_none));
        drop(origin);
        fixture.finish().unwrap();
    }
    #[tokio::test]
    async fn zero_write_keeps_ownership_until_error_cleanup() {
        let fixture = Fixture::new(
            "berry",
            vec![ClassConfig {
                capacity: 16,
                slots: 1,
            }],
            1,
            None,
        )
        .unwrap();
        let origin = fixture.open(16).unwrap();
        let mut messages: [Option<Message>; 8] = std::array::from_fn(|_| None);
        let mut b = origin.acquire(4).unwrap();
        b.put(b"abcd");
        messages[0] = Some(Message {
            header: Header {
                len: 4,
                id: 1,
                due: 0,
            },
            frame: b.freeze().into(),
            deadline: None,
        });
        let mut socket = ShortWriter {
            bytes: Vec::new(),
            max: 0,
            stop: usize::MAX,
        };
        let counters = WriterCounts::default();
        assert_eq!(
            batch(&mut socket, &mut messages, 1, &counters)
                .await
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::WriteZero
        );
        assert_eq!(counters.completed.load(Ordering::Relaxed), 0);
        assert_eq!(fixture.snapshot()["admitted"], 16);
        drop((messages, origin));
        fixture.finish().unwrap();
    }
}
