use super::super::backend::Lease;
use super::*;
use tokio::io::AsyncRead;

#[derive(Default)]
struct Decisions {
    skipped: u64,
    rejected: u64,
    absent: u64,
    disconnected: u64,
}
impl Decisions {
    fn commit(self, stats: &mut IngressStats) {
        stats.skipped += self.skipped;
        stats.rejected += self.rejected;
        stats.no_subscriber += self.absent;
    }
}
fn preflight(
    subscribers: &mut [Subscriber],
    skip: bool,
) -> ([Option<local_queue::IngressPermit<'_>>; 20], Decisions) {
    let mut permits = std::array::from_fn(|_| None);
    let mut decisions = Decisions::default();
    for (subscriber, slot) in subscribers.iter_mut().zip(&mut permits) {
        if !subscriber.active {
            decisions.absent += 1;
            continue;
        }
        match subscriber.queue.try_reserve() {
            Ok(permit) => *slot = Some(permit),
            Err(local_queue::Rejected::Full) if skip => decisions.skipped += 1,
            Err(_) => {
                decisions.rejected += 1;
                decisions.disconnected += 1;
                subscriber.active = false;
                if let Some(cancel) = subscriber.cancel.take() {
                    let _ = cancel.send(());
                }
            }
        }
    }
    (permits, decisions)
}
pub(super) fn overload(error: Error) -> bool {
    matches!(
        error,
        Error::BudgetExceeded
            | Error::ClassDepleted
            | Error::DomainClassDepleted
            | Error::Contended
    )
}
pub(super) async fn discard<R: AsyncRead + Unpin>(
    reader: &mut R,
    mut len: usize,
    scratch: &mut [u8],
) -> std::io::Result<()> {
    assert!(!scratch.is_empty());
    while len != 0 {
        let n = len.min(scratch.len());
        reader.read_exact(&mut scratch[..n]).await?;
        len -= n;
    }
    Ok(())
}
pub(super) async fn fill<R: AsyncRead + Unpin>(
    reader: &mut R,
    lease: &mut Lease,
    len: usize,
    read_buf: bool,
) -> std::io::Result<()> {
    if read_buf {
        #[cfg(feature = "bytes-interop")]
        if let Lease::Berry(buffer) = lease {
            use bytes::BufMut as _;
            while buffer.filled_len() < len {
                let remaining = len - buffer.filled_len();
                // The cursor cannot consume bytes from the next frame. Tokio
                // commits only a successfully read, initialized prefix.
                let n = reader
                    .read_buf(&mut (&mut *buffer).limit(remaining))
                    .await?;
                if n == 0 {
                    return Err(std::io::ErrorKind::UnexpectedEof.into());
                }
            }
            return Ok(());
        }
        #[cfg(all(feature = "bytes-interop", feature = "mosaic-spike"))]
        if let Lease::Mosaic(buffer) = lease {
            use bytes::BufMut as _;
            while buffer.filled_len() < len {
                let remaining = len - buffer.filled_len();
                // The cursor cannot consume bytes from the next frame. Tokio
                // commits only a successfully read, initialized prefix.
                let n = reader
                    .read_buf(&mut (&mut *buffer).limit(remaining))
                    .await?;
                if n == 0 {
                    return Err(std::io::ErrorKind::UnexpectedEof.into());
                }
            }
            return Ok(());
        }
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "read-buf requires an enabled berry or mosaic backend and bytes-interop",
        ));
    }
    reader.read_exact(lease.initialize(len)).await?;
    lease.set_filled(len);
    Ok(())
}
impl IngressStats {
    pub(super) fn admission_discard(&mut self, error: Error, recipients: u64) {
        self.discarded += 1;
        self.ingress_rejected += recipients;
        match error {
            Error::BudgetExceeded => self.budget_discards += 1,
            Error::ClassDepleted => self.class_discards += 1,
            Error::DomainClassDepleted => self.domain_class_discards += 1,
            Error::Contended => self.contention_discards += 1,
            _ => self.fatal_discards += 1,
        }
    }
}
pub(super) async fn run(session: Session, settings: Settings) -> Result<IngressStats> {
    let Session {
        stream,
        socket,
        origin,
        subscribers,
        scratch,
        #[cfg(feature = "local-branches-spike")]
        worker_fanout,
    } = session;
    #[cfg(feature = "local-branches-spike")]
    if let Some(group) = worker_fanout {
        return super::worker_fanout::ingress(
            into_async(socket)?,
            origin,
            group,
            scratch,
            settings,
            stream,
        )
        .await;
    }
    run_io(
        into_async(socket)?,
        origin,
        subscribers,
        scratch,
        settings,
        stream,
    )
    .await
}
pub(super) async fn run_io<S: AsyncRead + tokio::io::AsyncWrite + Unpin>(
    mut socket: S,
    origin: Origin,
    mut subscribers: Vec<Subscriber>,
    mut scratch: Box<[u8]>,
    settings: Settings,
    stream: usize,
) -> Result<IngressStats> {
    let mut stats = IngressStats {
        stream,
        ..IngressStats::default()
    };
    socket.write_all(&[1]).await?;
    loop {
        let header = match Header::read(&mut socket).await {
            Ok(header) => header,
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(error) => {
                stats.stop = Some(format!("read_header:{error}"));
                break;
            }
        };
        if header.len == 0 || header.len > settings.max_payload() {
            stats.stop = Some("invalid_length".into());
            break;
        }
        let (mut permits, decisions) = preflight(&mut subscribers, settings.skip);
        stats.disconnected += decisions.disconnected;
        let recipients = permits.iter().flatten().count() as u64;
        if recipients == 0 {
            if let Err(error) = discard(&mut socket, header.len, &mut scratch).await {
                stats.stop = Some(format!("discard_payload:{error}"));
                break;
            }
            stats.received += 1;
            stats.discarded += 1;
            stats.no_receiver_discards += 1;
            decisions.commit(&mut stats);
            continue;
        }
        let mut lease = match acquire(&origin, header.len, &mut stats.admission_contended).await {
            Ok(lease) => lease,
            Err(error) if settings.skip && overload(error) => {
                if let Err(error) = discard(&mut socket, header.len, &mut scratch).await {
                    stats.stop = Some(format!("discard_payload:{error}"));
                    break;
                }
                stats.received += 1;
                stats.admission_discard(error, recipients);
                decisions.commit(&mut stats);
                continue;
            }
            Err(error) => {
                stats.stop = Some(format!("acquire:{error}"));
                break;
            }
        };
        if let Err(error) = fill(&mut socket, &mut lease, header.len, settings.read_buf).await {
            stats.stop = Some(format!("read_payload:{error}"));
            break;
        }
        stats.received += 1;
        decisions.commit(&mut stats);
        let mut views: [Option<QueuedFrame>; 20] = std::array::from_fn(|_| None);
        let mut failed = None;
        if settings.local_branches {
            match branch_views(lease, recipients as usize) {
                Ok(mut ready) => {
                    let mut ready = ready.iter_mut();
                    for (slot, permit) in views.iter_mut().zip(&permits) {
                        if permit.is_some() {
                            *slot = ready.next().unwrap().take();
                        }
                    }
                }
                Err(error) => failed = Some(error),
            }
        } else {
            let frame = lease.freeze();
            if settings.batch_fanout {
                match batch_views(frame, recipients as usize, &mut stats.batch_clone_contended)
                    .await
                {
                    Ok(mut ready) => {
                        let mut ready = ready.iter_mut();
                        for (slot, permit) in views.iter_mut().zip(&permits) {
                            if permit.is_some() {
                                *slot = ready.next().unwrap().take().map(Into::into);
                            }
                        }
                    }
                    Err(error) => failed = Some(error),
                }
            } else {
                for (slot, permit) in views.iter_mut().zip(&permits) {
                    if permit.is_none() {
                        continue;
                    }
                    match view(&frame, &mut stats.scalar_clone_contended).await {
                        Ok(value) => *slot = Some(value.into()),
                        Err(error) => {
                            failed = Some(error);
                            break;
                        }
                    }
                }
            }
        }
        if let Some(error) = failed {
            stats.admission_discard(error, recipients);
            if settings.skip && overload(error) {
                continue;
            }
            stats.stop = Some(format!("view:{error}"));
            break;
        }
        stats.accepted += 1;
        let deadline = settings
            .max_frame_age
            .map(|age| tokio::time::Instant::now() + age);
        for (permit, frame) in permits.iter_mut().zip(&mut views) {
            if let Some(permit) = permit.take() {
                permit.send(Message {
                    header,
                    frame: frame.take().unwrap(),
                    deadline,
                });
                stats.enqueued += 1;
            }
        }
    }
    Ok(stats)
}

pub(super) fn branch_views(
    lease: super::super::backend::Lease,
    recipients: usize,
) -> std::result::Result<[Option<QueuedFrame>; 20], Error> {
    if !(1..=20).contains(&recipients) {
        return Err(Error::InvalidSize);
    }
    #[cfg(feature = "local-branches-spike")]
    if let super::super::backend::Lease::Mosaic(lease) = lease {
        let mut branches = std::array::from_fn::<_, 20, _>(|_| None);
        // The adapter discards a rejected frame; the core error still returns
        // the untouched lease for callers with a different explicit policy.
        lease
            .freeze_branches_into(&mut branches[..recipients])
            .map_err(|e| e.reason())?;
        return Ok(branches.map(|b| b.map(QueuedFrame::Branch)));
    }
    #[cfg(not(feature = "local-branches-spike"))]
    drop(lease);
    Err(Error::InvalidConfiguration)
}

async fn batch_views(
    frame: Frame,
    recipients: usize,
    contended: &mut u64,
) -> std::result::Result<[Option<Frame>; 20], Error> {
    if !(1..=20).contains(&recipients) {
        return Err(Error::InvalidSize);
    }
    let mut ready = std::array::from_fn(|_| None);
    for attempt in 0..3 {
        match frame.try_clone_into(&mut ready[..recipients - 1]) {
            Ok(()) => {
                ready[recipients - 1] = Some(frame);
                return Ok(ready);
            }
            Err(Error::Contended) => {
                *contended += 1;
                if attempt < 2 {
                    tokio::task::yield_now().await;
                }
            }
            Err(error) => return Err(error),
        }
    }
    Err(Error::Contended)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn abps_scripted_ingress_errors_wakeups_and_cancellation_preserve_prefix() {
        use super::super::scripted_io::{ScriptedIo, Step};
        use std::{future::Future, io::ErrorKind, task::Poll};
        let backends = if cfg!(feature = "mosaic-spike") {
            vec!["berry", "mosaic"]
        } else {
            vec!["berry"]
        };
        for backend in backends {
            for read_buf in [false, true] {
                if read_buf && !cfg!(feature = "bytes-interop") {
                    continue;
                }
                let fixture = Fixture::new(
                    backend,
                    vec![ClassConfig {
                        capacity: 16,
                        slots: 2,
                    }],
                    2,
                    None,
                )
                .unwrap();
                let origin = fixture.open(16).unwrap();
                let other = fixture.open(16).unwrap();
                let mut neighbor = other.acquire(1).unwrap();
                neighbor.put(&[0xa7]);
                let neighbor = neighbor.freeze();
                for terminal in [
                    Step::Error(ErrorKind::ConnectionReset),
                    Step::Error(ErrorKind::Interrupted),
                    Step::Zero,
                    Step::Stall,
                ] {
                    let mut lease = origin.acquire(8).unwrap();
                    let mut reader =
                        ScriptedIo::new(b"abcDEFGHNEXT", [Step::Data(3), Step::Pending, terminal]);
                    if matches!(terminal, Step::Stall) {
                        let mut future = Box::pin(fill(&mut reader, &mut lease, 8, read_buf));
                        assert!(
                            std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx)))
                                .await
                                .is_pending()
                        );
                        assert!(
                            std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx)))
                                .await
                                .is_pending()
                        );
                        drop(future);
                    } else {
                        let error = fill(&mut reader, &mut lease, 8, read_buf)
                            .await
                            .unwrap_err()
                            .kind();
                        assert_eq!(
                            error,
                            match terminal {
                                Step::Error(e) => e,
                                _ => ErrorKind::UnexpectedEof,
                            }
                        );
                    }
                    assert_eq!(reader.consumed, 3, "IO-01 no read beyond completed prefix");
                    assert_eq!(reader.calls, 3, "IO-01 no implicit retry after an error");
                    let expected = if read_buf { &b"abc"[..] } else { &b""[..] };
                    assert_eq!(lease.freeze().as_ref(), expected);
                    assert_eq!(neighbor.as_ref(), &[0xa7]);
                    assert_eq!(fixture.snapshot()["admitted"], 16);
                }
                let mut lease = origin.acquire(8).unwrap();
                let mut reader = ScriptedIo::new(
                    b"abcDEFGHNEXT",
                    [Step::Data(3), Step::Pending, Step::Data(usize::MAX)],
                );
                fill(&mut reader, &mut lease, 8, read_buf).await.unwrap();
                assert_eq!(reader.consumed, 8);
                assert_eq!(&reader.input[reader.consumed..], b"NEXT");
                assert_eq!(lease.freeze().as_ref(), b"abcDEFGH");
                drop((neighbor, origin, other));
                fixture.finish().unwrap();
            }
        }
    }
    fn subscriber(size: usize) -> (Subscriber, mpsc::Receiver<Message>) {
        let (queue, rx) = mpsc::channel(size);
        (
            Subscriber {
                queue: queue.into(),
                active: true,
                cancel: None,
            },
            rx,
        )
    }
    fn config(skip: bool) -> Settings {
        Settings::new(&Args(
            [
                ("fanout", "1"),
                ("overflow", if skip { "skip" } else { "disconnect" }),
            ]
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect(),
        ))
        .unwrap()
    }
    #[tokio::test]
    async fn skip_exhausted_frame_and_resume_at_next_header() {
        for budget in [512, 1024] {
            let fixture = Fixture::new(
                "berry",
                vec![ClassConfig {
                    capacity: 512,
                    slots: 1,
                }],
                1,
                None,
            )
            .unwrap();
            let origin = fixture.open(budget).unwrap();
            let held = origin.acquire(1).unwrap();
            let (sub, mut receiver) = subscriber(2);
            let (socket, mut source) = tokio::io::duplex(8);
            let ingress = run_io(
                socket,
                origin,
                vec![sub],
                vec![0; 17].into_boxed_slice(),
                config(true),
                0,
            );
            let drive = async {
                source.read_u8().await.unwrap();
                let first = Header {
                    len: 32,
                    id: 1,
                    due: 0,
                };
                source.write_all(&first.encode()).await.unwrap();
                source.write_all(&[7; 32]).await.unwrap();
                // Put the next header in the same stream; it cannot be swallowed
                // by scratch draining. Release capacity only after discard runs.
                drop(held);
                let second = Header {
                    len: 32,
                    id: 2,
                    due: 0,
                };
                source.write_all(&second.encode()).await.unwrap();
                source.write_all(&[9; 32]).await.unwrap();
                source.shutdown().await.unwrap();
            };
            let (stats, ()) = tokio::join!(ingress, drive);
            let stats = stats.unwrap();
            assert_eq!((stats.received, stats.accepted, stats.discarded), (2, 1, 1));
            assert_eq!(stats.budget_discards, u64::from(budget == 512));
            assert_eq!(stats.class_discards, u64::from(budget == 1024));
            assert!(stats.stop.is_none());
            let message = receiver.recv().await.unwrap();
            assert_eq!(message.header.id, 2);
            assert_eq!(message.frame.as_ref(), &[9; 32]);
            drop((message, receiver));
            fixture.finish().unwrap();
        }
    }
    #[test]
    fn closed_and_reference_limit_are_not_skip_overload() {
        assert!(!overload(Error::Closed));
        assert!(!overload(Error::ReferenceLimit));
        assert!(overload(Error::Contended));
    }
}

#[cfg(test)]
mod regression {
    use super::*;
    fn settings(skip: bool) -> Settings {
        Settings::new(&Args(
            [
                ("fanout", "1"),
                ("overflow", if skip { "skip" } else { "disconnect" }),
            ]
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect(),
        ))
        .unwrap()
    }
    #[tokio::test]
    async fn queue_full_is_decided_before_payload_admission_and_default_disconnects() {
        for skip in [false, true] {
            let fixture = Fixture::new(
                "berry",
                vec![ClassConfig {
                    capacity: 512,
                    slots: 1,
                }],
                1,
                None,
            )
            .unwrap();
            let origin = fixture.open(512).unwrap();
            let (queue, receiver) = mpsc::channel(1);
            let (cancel, stop) = oneshot::channel();
            queue
                .try_send(Message {
                    header: Header {
                        len: 0,
                        id: 0,
                        due: 0,
                    },
                    frame: origin.acquire(1).unwrap().freeze().into(),
                    deadline: None,
                })
                .ok()
                .unwrap();
            let subscriber = Subscriber {
                queue: queue.into(),
                active: true,
                cancel: Some(cancel),
            };
            let (server, mut source) = tokio::io::duplex(1024);
            let drive = async {
                source.read_u8().await.unwrap();
                source
                    .write_all(
                        &Header {
                            len: 32,
                            id: 1,
                            due: 0,
                        }
                        .encode(),
                    )
                    .await
                    .unwrap();
                source.write_all(&[1; 32]).await.unwrap();
                source.shutdown().await.unwrap();
            };
            let (result, ()) = tokio::join!(
                run_io(
                    server,
                    origin,
                    vec![subscriber],
                    vec![0; 16].into_boxed_slice(),
                    settings(skip),
                    0
                ),
                drive
            );
            let stats = result.unwrap();
            assert!(stats.stop.is_none());
            assert_eq!(
                (stats.received, stats.discarded, stats.no_receiver_discards),
                (1, 1, 1)
            );
            assert_eq!((stats.budget_discards, stats.class_discards), (0, 0));
            assert_eq!(stats.disconnected, u64::from(!skip));
            assert_eq!(stats.skipped, u64::from(skip));
            assert_eq!(stop.await.is_ok(), !skip);
            drop(receiver);
            fixture.finish().unwrap();
        }
    }
    #[tokio::test]
    async fn default_admission_failure_and_partial_payload_end_the_stream() {
        for partial in [false, true] {
            let fixture = Fixture::new(
                "berry",
                vec![ClassConfig {
                    capacity: 512,
                    slots: 1,
                }],
                1,
                None,
            )
            .unwrap();
            let origin = fixture.open(if partial { 512 } else { 0 }).unwrap();
            let (queue, mut receiver) = mpsc::channel(1);
            let sub = Subscriber {
                queue: queue.into(),
                active: true,
                cancel: None,
            };
            let (server, mut source) = tokio::io::duplex(1024);
            source
                .write_all(
                    &Header {
                        len: 32,
                        id: 1,
                        due: 0,
                    }
                    .encode(),
                )
                .await
                .unwrap();
            if partial {
                source.write_all(&[1; 4]).await.unwrap();
            }
            source.shutdown().await.unwrap();
            let stats = run_io(
                server,
                origin,
                vec![sub],
                vec![0; 16].into_boxed_slice(),
                settings(false),
                0,
            )
            .await
            .unwrap();
            assert_eq!(stats.received, 0);
            assert!(stats.stop.unwrap().starts_with(if partial {
                "read_payload:"
            } else {
                "acquire:BudgetExceeded"
            }));
            assert!(receiver.recv().await.is_none());
            fixture.finish().unwrap();
        }
    }
    #[cfg(feature = "bytes-interop")]
    #[tokio::test]
    async fn limited_read_buf_preserves_next_frame_and_cancelled_partial_prefix() {
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
        let mut lease = origin.acquire(3).unwrap();
        let mut input: &[u8] = b"abcNEXT";
        fill(&mut input, &mut lease, 3, true).await.unwrap();
        assert_eq!(input, b"NEXT");
        let Lease::Berry(ref b) = lease else { panic!() };
        assert_eq!((b.filled_len(), b.initialized_len()), (3, 3));
        drop(lease);
        let mut lease = origin.acquire(8).unwrap();
        let (mut writer, mut reader) = tokio::io::duplex(16);
        writer.write_all(b"xyz").await.unwrap();
        {
            let future = fill(&mut reader, &mut lease, 8, true);
            tokio::pin!(future);
            assert!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(future.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
        }
        let Lease::Berry(ref b) = lease else { panic!() };
        assert_eq!((b.filled_len(), b.initialized_len()), (3, 3));
        assert_eq!(b.as_ref(), b"xyz");
        drop((lease, origin));
        fixture.finish().unwrap();
    }
    #[cfg(all(feature = "bytes-interop", feature = "mosaic-spike"))]
    #[tokio::test]
    async fn mosaic_read_buf_preserves_next_frame_and_cancelled_partial_prefix() {
        let fixture = Fixture::new(
            "mosaic",
            vec![ClassConfig {
                capacity: 16,
                slots: 1,
            }],
            1,
            None,
        )
        .unwrap();
        let origin = fixture.open(16).unwrap();
        let mut lease = origin.acquire(3).unwrap();
        let mut input: &[u8] = b"abcNEXT";
        fill(&mut input, &mut lease, 3, true).await.unwrap();
        assert_eq!(input, b"NEXT");
        let Lease::Mosaic(ref b) = lease else {
            panic!()
        };
        assert_eq!((b.filled_len(), b.initialized_len()), (3, 3));
        drop(lease);
        let mut lease = origin.acquire(8).unwrap();
        let (mut writer, mut reader) = tokio::io::duplex(16);
        writer.write_all(b"xyz").await.unwrap();
        {
            let future = fill(&mut reader, &mut lease, 8, true);
            tokio::pin!(future);
            assert!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(future.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
        }
        let Lease::Mosaic(ref b) = lease else {
            panic!()
        };
        assert_eq!((b.filled_len(), b.initialized_len()), (3, 3));
        assert_eq!(b.as_ref(), b"xyz");
        drop((lease, origin));
        fixture.finish().unwrap();
    }
    #[tokio::test]
    async fn failed_view_preparation_publishes_no_partial_fanout() {
        for batch in [false, true] {
            let mut cfg = Config::new(
                vec![ClassConfig {
                    capacity: 512,
                    slots: 1,
                }],
                1,
                1 << 20,
            );
            cfg.reference_limit = 2; // original + first view; second must fail
            let owner = abps_reference::berry::StorageOwner::prepare(&cfg).unwrap();
            let pool = owner.activate().unwrap();
            let fixture = Fixture::Berry { owner, pool };
            let origin = fixture.open(512).unwrap();
            let mut subscribers = Vec::new();
            let mut readers = Vec::new();
            let recipients = if batch { 3 } else { 2 };
            for _ in 0..recipients {
                let (queue, receiver) = mpsc::channel(1);
                subscribers.push(Subscriber {
                    queue: queue.into(),
                    active: true,
                    cancel: None,
                });
                readers.push(receiver);
            }
            let (server, mut source) = tokio::io::duplex(1024);
            source
                .write_all(
                    &Header {
                        len: 4,
                        id: 1,
                        due: 0,
                    }
                    .encode(),
                )
                .await
                .unwrap();
            source.write_all(b"abcd").await.unwrap();
            source.shutdown().await.unwrap();
            let mut settings = settings(true);
            settings.fanout = recipients;
            settings.batch_fanout = batch;
            let stats = run_io(
                server,
                origin,
                subscribers,
                vec![0; 16].into_boxed_slice(),
                settings,
                0,
            )
            .await
            .unwrap();
            assert_eq!(stats.stop.as_deref(), Some("view:ReferenceLimit"));
            assert_eq!(
                (
                    stats.received,
                    stats.accepted,
                    stats.discarded,
                    stats.enqueued
                ),
                (1, 0, 1, 0)
            );
            for reader in &mut readers {
                assert!(reader.recv().await.is_none());
            }
            fixture.finish().unwrap();
        }
    }
    #[cfg(feature = "local-branches-spike")]
    #[tokio::test]
    async fn br06_branch_fanout_preserves_recipient_holes_and_has_no_partial_errors() {
        for capacity in [1, 2] {
            let fixture = Fixture::prepared_branches(
                "mosaic",
                vec![ClassConfig {
                    capacity: 512,
                    slots: 1,
                }],
                1,
                None,
                CapacityPolicy::Unreserved,
                false,
                capacity,
            )
            .unwrap();
            let origin = fixture.open(512).unwrap();
            let mut subscribers = Vec::new();
            let mut readers = Vec::new();
            for i in 0..3 {
                let (queue, reader) = mpsc::channel(1);
                subscribers.push(Subscriber {
                    queue: queue.into(),
                    active: i != 1,
                    cancel: None,
                });
                readers.push(reader);
            }
            let (server, mut source) = tokio::io::duplex(1024);
            source
                .write_all(
                    &Header {
                        len: 4,
                        id: 123,
                        due: 0,
                    }
                    .encode(),
                )
                .await
                .unwrap();
            source.write_all(b"ABPS").await.unwrap();
            source.shutdown().await.unwrap();
            let mut s = settings(true);
            s.fanout = 3;
            s.local_branches = true;
            let result = run_io(
                server,
                origin,
                subscribers,
                vec![0; 16].into_boxed_slice(),
                s,
                0,
            )
            .await
            .unwrap();
            assert_eq!(result.accepted, u64::from(capacity == 2));
            assert_eq!(result.enqueued, if capacity == 2 { 2 } else { 0 });
            for (i, reader) in readers.iter_mut().enumerate() {
                let msg = reader.try_recv();
                if capacity == 2 && i != 1 {
                    let msg = msg.unwrap().into_writer();
                    assert_eq!(msg.frame.as_ref(), b"ABPS");
                    assert_eq!(msg.header.id, 123);
                } else {
                    assert!(msg.is_err());
                }
            }
            drop(readers);
            assert_eq!(fixture.snapshot()["admitted"], 0);
            fixture.finish().unwrap();
        }
    }

    #[tokio::test]
    async fn si10_batch_transfers_original_and_maps_sparse_queue_permits() {
        let mut cfg = Config::new(
            vec![ClassConfig {
                capacity: 512,
                slots: 1,
            }],
            1,
            1 << 20,
        );
        cfg.reference_limit = 2; // two recipients fit only by moving the original
        let owner = abps_reference::berry::StorageOwner::prepare(&cfg).unwrap();
        let pool = owner.activate().unwrap();
        let fixture = Fixture::Berry { owner, pool };
        let origin = fixture.open(512).unwrap();
        let mut subscribers = Vec::new();
        let mut readers = Vec::new();
        for i in 0..3 {
            let (queue, reader) = mpsc::channel(1);
            subscribers.push(Subscriber {
                queue: queue.into(),
                active: i != 1,
                cancel: None,
            });
            readers.push(reader);
        }
        let (server, mut source) = tokio::io::duplex(1024);
        source
            .write_all(
                &Header {
                    len: 4,
                    id: 123,
                    due: 0,
                }
                .encode(),
            )
            .await
            .unwrap();
        source.write_all(b"ABPS").await.unwrap();
        source.shutdown().await.unwrap();
        let mut s = settings(true);
        s.fanout = 3;
        s.batch_fanout = true;
        let result = run_io(
            server,
            origin,
            subscribers,
            vec![0; 16].into_boxed_slice(),
            s,
            0,
        )
        .await
        .unwrap();
        assert_eq!(
            (result.accepted, result.enqueued, result.no_subscriber),
            (1, 2, 1)
        );
        for (i, reader) in readers.iter_mut().enumerate() {
            let msg = reader.recv().await;
            if i == 1 {
                assert!(msg.is_none());
            } else {
                let msg = msg.unwrap();
                assert_eq!(msg.frame.as_ref(), b"ABPS");
                assert_eq!(msg.header.id, 123);
            }
        }
        drop(readers);
        fixture.finish().unwrap();
    }
}
