//! Runtime adapter for the reference experiment only. No networking in the core.

use std::io::{self, IoSlice};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use abps_reference::{Buf, ClassConfig, Config, Domain, Error, Snapshot, StorageOwner};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::timeout;

pub const SUBSCRIBERS: usize = 20;
pub const PAYLOAD: usize = 512;
const WAIT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Overflow {
    #[default]
    Disconnect,
    Skip,
}

#[derive(Debug, Clone, Copy)]
pub struct Experiment {
    pub frames: usize,
    pub overflow: Overflow,
    pub reference_limit: usize,
    pub slow_subscriber: bool,
}

impl Default for Experiment {
    fn default() -> Self {
        Self {
            frames: 24,
            overflow: Overflow::Disconnect,
            reference_limit: 64,
            slow_subscriber: true,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Failure {
    Acquire(Error),
    Views(Error),
    Network(String),
}

#[derive(Debug)]
pub struct Report {
    pub planned_frames: usize,
    pub offered_frames: usize,
    pub read_frames: usize,
    pub enqueued: usize,
    pub delivered: usize,
    pub undelivered_enqueued: usize,
    pub skipped: usize,
    pub rejected_full: usize,
    pub disconnected: usize,
    pub ingress_failure: Option<Failure>,
    pub delivered_ids: Vec<Vec<u64>>,
    pub drained: Snapshot,
    pub destroyed: bool,
    pub domain_reused: bool,
    pub headroom_error: Error,
}

struct Message {
    id: u64,
    buffer: Buf,
}

struct Subscriber {
    sender: Option<mpsc::Sender<Message>>,
    writer: Option<JoinHandle<io::Result<()>>>,
    consumer: Option<JoinHandle<io::Result<Vec<u64>>>>,
    release: Option<oneshot::Sender<()>>,
    disconnected: bool,
}

// Fail closed even if the experiment itself is cancelled. Normal run() joins all
// tasks explicitly before destroying storage. Cancellation without that drain may
// abandon the owner, as specified, but never detaches an unbounded live writer.
impl Drop for Subscriber {
    fn drop(&mut self) {
        if let Some(task) = &self.writer {
            task.abort();
        }
        if let Some(task) = &self.consumer {
            task.abort();
        }
    }
}

struct Producer(Option<JoinHandle<io::Result<()>>>);
impl Drop for Producer {
    fn drop(&mut self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn network(error: impl std::fmt::Display) -> Failure {
    Failure::Network(error.to_string())
}

async fn pair() -> io::Result<(TcpStream, TcpStream)> {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let client = TcpStream::connect(listener.local_addr()?).await?;
    let (server, _) = listener.accept().await?;
    client.set_nodelay(true)?;
    server.set_nodelay(true)?;
    Ok((client, server))
}

/// Bounded, borrowed descriptors; correctly handles short writes in either slice.
/// Cancellation is connection termination, not permission to resume this frame.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    id: u64,
    payload: &[u8],
) -> io::Result<()> {
    let len = u32::try_from(payload.len()).map_err(|_| invalid("oversized frame"))?;
    let mut header = [0u8; 12];
    header[..4].copy_from_slice(&len.to_be_bytes());
    header[4..].copy_from_slice(&id.to_be_bytes());
    let mut header_offset = 0;
    let mut payload_offset = 0;
    while header_offset < header.len() || payload_offset < payload.len() {
        let descriptors = [
            IoSlice::new(&header[header_offset..]),
            IoSlice::new(&payload[payload_offset..]),
        ];
        let pending = &descriptors[usize::from(header_offset == header.len())..];
        let written = writer.write_vectored(pending).await?;
        if written == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        let header_written = written.min(header.len() - header_offset);
        header_offset += header_written;
        let payload_written = written - header_written;
        if payload_written > payload.len() - payload_offset {
            return Err(invalid("writer reported an invalid length"));
        }
        payload_offset += payload_written;
    }
    Ok(())
}

pub async fn read_header<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Option<(u64, usize)>> {
    let mut header = [0u8; 12];
    if reader.read(&mut header[..1]).await? == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut header[1..]).await?;
    let len = u32::from_be_bytes(header[..4].try_into().expect("four bytes")) as usize;
    let id = u64::from_be_bytes(header[4..].try_into().expect("eight bytes"));
    if !(1..=PAYLOAD).contains(&len) {
        return Err(invalid("unsupported frame size"));
    }
    Ok(Some((id, len)))
}

async fn acquire(domain: &Domain, len: usize) -> Result<abps_reference::BufMut, Error> {
    for attempt in 0..3 {
        match domain.try_acquire(len) {
            Err(Error::Contended) if attempt < 2 => tokio::task::yield_now().await,
            result => return result,
        }
    }
    unreachable!("three attempts return")
}

async fn views(buffer: &Buf, active: &[Subscriber]) -> Result<[Option<Buf>; SUBSCRIBERS], Error> {
    let mut views = std::array::from_fn(|_| None);
    for (index, subscriber) in active.iter().enumerate() {
        if subscriber.sender.is_none() {
            continue;
        }
        for attempt in 0..3 {
            match buffer.try_clone() {
                Err(Error::Contended) if attempt < 2 => tokio::task::yield_now().await,
                Ok(view) => {
                    views[index] = Some(view);
                    break;
                }
                Err(error) => return Err(error), // already prepared views are dropped
            }
        }
    }
    Ok(views)
}

async fn join<T>(mut task: JoinHandle<io::Result<T>>) -> io::Result<T> {
    match timeout(WAIT, &mut task).await {
        Ok(result) => result.map_err(io::Error::other)?,
        Err(_) => {
            task.abort();
            let _ = task.await;
            Err(io::ErrorKind::TimedOut.into())
        }
    }
}

/// Real loopback TCP -> ABPS -> twenty bounded queues -> TCP consumers -> reuse.
/// ACK-paced ingress intentionally makes overload deterministic; not a benchmark.
pub async fn run(
    experiment: Experiment,
) -> Result<Report, Box<dyn std::error::Error + Send + Sync>> {
    let mut config = Config::new(
        vec![ClassConfig {
            capacity: PAYLOAD,
            slots: 32,
        }],
        2,
        1 << 20,
    );
    config.reference_limit = experiment.reference_limit;
    let mut owner = StorageOwner::prepare(config)?;
    let diagnostics = owner.diagnostics()?;
    let pool = owner.activate()?;
    let (domain_owner, domain) = pool.try_open_domain(32 * PAYLOAD)?;
    let original_identity = domain.identity();
    let mut subscribers = Vec::with_capacity(SUBSCRIBERS);
    let mut producer = Producer(None);
    let (ack_sender, mut acks) = mpsc::channel(SUBSCRIBERS);
    let (returned_sender, mut returned) = mpsc::channel(SUBSCRIBERS);
    let offered_frames = Arc::new(AtomicUsize::new(0));
    let mut read_frames = 0;
    let mut enqueued = 0;
    let mut skipped = 0;
    let mut rejected_full = 0;
    let mut disconnected = 0;

    // All exits from network setup/processing flow into the same join-and-drain.
    let processing: Result<(), Failure> = async {
        for index in 0..SUBSCRIBERS {
            let (mut writer, mut reader) = pair().await.map_err(network)?;
            let (sender, mut receiver) = mpsc::channel::<Message>(2);
            let (release, start) = oneshot::channel();
            let slow = experiment.slow_subscriber && index == 0;
            let returned_sender = returned_sender.clone();
            let writer_task = tokio::spawn(async move {
                if slow && start.await.is_err() {
                    return Ok(());
                }
                while let Some(message) = receiver.recv().await {
                    // message owns backing through the entire regular TCP write.
                    write_frame(&mut writer, message.id, message.buffer.as_ref()).await?;
                    let id = message.id;
                    drop(message);
                    returned_sender
                        .send((index, id))
                        .await
                        .map_err(io::Error::other)?;
                }
                writer.shutdown().await
            });
            let ack_sender = ack_sender.clone();
            let consumer = tokio::spawn(async move {
                let mut ids = Vec::with_capacity(experiment.frames);
                let mut payload = [0u8; PAYLOAD];
                while let Some((id, len)) = read_header(&mut reader).await? {
                    reader.read_exact(&mut payload[..len]).await?;
                    if payload[..len].iter().any(|b| *b != id as u8)
                        || ids.last().is_some_and(|last| *last >= id)
                    {
                        return Err(invalid("payload or frame ordering mismatch"));
                    }
                    ids.push(id);
                    if !slow {
                        ack_sender
                            .send((index, id))
                            .await
                            .map_err(io::Error::other)?;
                    }
                }
                Ok(ids)
            });
            subscribers.push(Subscriber {
                sender: Some(sender),
                writer: Some(writer_task),
                consumer: Some(consumer),
                release: Some(release),
                disconnected: false,
            });
        }
        let (mut source, mut ingress) = pair().await.map_err(network)?;
        let offered_frames = Arc::clone(&offered_frames);
        producer.0 = Some(tokio::spawn(async move {
            for id in 0..experiment.frames as u64 {
                offered_frames.fetch_add(1, Ordering::Relaxed);
                write_frame(&mut source, id, &[id as u8; PAYLOAD]).await?;
                source.read_u8().await?;
            }
            source.shutdown().await
        }));
        while let Some((id, len)) = read_header(&mut ingress).await.map_err(network)? {
            let mut lease = acquire(&domain, len).await.map_err(Failure::Acquire)?;
            ingress
                .read_exact(lease.initialize_to(len).map_err(Failure::Acquire)?)
                .await
                .map_err(network)?;
            lease.set_filled_len(len).map_err(Failure::Acquire)?;
            let frame = lease.freeze();
            read_frames += 1;
            let mut ready = views(&frame, &subscribers).await.map_err(Failure::Views)?;
            drop(frame);
            let mut expect_ack = [false; SUBSCRIBERS];
            let mut expect_return = [false; SUBSCRIBERS];
            for (index, subscriber) in subscribers.iter_mut().enumerate() {
                let Some(buffer) = ready[index].take() else {
                    continue;
                };
                match subscriber
                    .sender
                    .as_ref()
                    .expect("prepared active view")
                    .try_send(Message { id, buffer })
                {
                    Ok(()) => {
                        enqueued += 1;
                        expect_ack[index] = !(experiment.slow_subscriber && index == 0);
                        expect_return[index] = expect_ack[index] || id > 5;
                    }
                    Err(mpsc::error::TrySendError::Full(message)) => {
                        drop(message); // a whole new frame, never accepted by this queue
                        match experiment.overflow {
                            Overflow::Skip => skipped += 1,
                            Overflow::Disconnect => {
                                rejected_full += 1;
                                disconnected += 1;
                                subscriber.disconnected = true;
                                subscriber.sender.take();
                                if let Some(writer) = subscriber.writer.take() {
                                    writer.abort();
                                    let _ = writer.await; // destruction completes before moving on
                                }
                            }
                        }
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        return Err(network("subscriber closed unexpectedly"));
                    }
                }
            }
            // Application workload control, outside the allocator. Without pacing,
            // a shared CI scheduler could overload every supposedly fast subscriber.
            while expect_ack.iter().any(|expected| *expected) {
                let (index, delivered_id) = timeout(WAIT, acks.recv())
                    .await
                    .map_err(network)?
                    .ok_or_else(|| network("consumer acknowledgements closed"))?;
                if delivered_id != id || !expect_ack[index] {
                    return Err(network("unexpected acknowledgement"));
                }
                expect_ack[index] = false;
            }
            while expect_return.iter().any(|expected| *expected) {
                let (index, returned_id) =
                    timeout(WAIT, returned.recv())
                        .await
                        .map_err(network)?
                        .ok_or_else(|| network("writer return acknowledgements closed"))?;
                if returned_id != id || !expect_return[index] {
                    return Err(network("unexpected writer return"));
                }
                expect_return[index] = false;
            }
            if id == 5
                && let Some(release) = subscribers[0].release.take()
            {
                let _ = release.send(());
                if experiment.slow_subscriber && !subscribers[0].disconnected {
                    // Settle the stalled writer's accepted frames before resuming
                    // ingress; this is reproducible workload control, not core retry.
                    for buffered_id in 0..2 {
                        let event = timeout(WAIT, returned.recv()).await.map_err(network)?;
                        if event != Some((0, buffered_id)) {
                            return Err(network("unexpected resumed return"));
                        }
                    }
                }
            }
            ingress.write_u8(1).await.map_err(network)?;
        }
        Ok(())
    }
    .await;

    let mut failure = processing.err();
    // Producers/writers must finish or be cancelled and joined before root drain.
    if let Some(task) = producer.0.take() {
        if failure.is_some() {
            task.abort();
            let _ = task.await;
        } else if let Err(error) = join(task).await {
            failure = Some(network(error));
        }
    }
    for subscriber in &mut subscribers {
        subscriber.sender.take();
        if let Some(release) = subscriber.release.take() {
            let _ = release.send(());
        }
    }
    let mut delivered_ids = Vec::with_capacity(subscribers.len());
    for subscriber in &mut subscribers {
        if let Some(task) = subscriber.writer.take() {
            if failure.is_some() {
                task.abort();
                let _ = task.await;
            } else if let Err(error) = join(task).await {
                failure = Some(network(error));
            }
        }
        if let Some(task) = subscriber.consumer.take() {
            if failure.is_some() {
                task.abort();
                let _ = task.await;
                delivered_ids.push(Vec::new());
            } else {
                match join(task).await {
                    Ok(ids) => delivered_ids.push(ids),
                    Err(_) if subscriber.disconnected => delivered_ids.push(Vec::new()),
                    Err(error) => {
                        failure = Some(network(error));
                        delivered_ids.push(Vec::new());
                    }
                }
            }
        }
    }
    drop(subscribers);

    // Same-slot domain reuse and explicit transform headroom failure.
    drop((domain, domain_owner));
    let (headroom_owner, headroom) = pool.try_open_domain(PAYLOAD)?;
    let domain_reused = headroom.identity() != original_identity;
    let input = headroom.try_acquire(PAYLOAD)?;
    let headroom_error = headroom
        .try_acquire(PAYLOAD)
        .err()
        .ok_or("expected headroom failure")?;
    drop((input, headroom, headroom_owner));
    let drained = pool.snapshot();
    drop(pool);
    let destruction = owner.try_destroy();
    // If an unexpected outstanding claim prevents drain, publish abandonment
    // before inspecting diagnostics, rather than losing the event on `?`.
    drop(owner);
    let diagnostic = diagnostics.snapshot();
    if let Some(event) = diagnostic.abandonment {
        eprintln!("ABPS abandoned root: {event:?}");
    }
    destruction?;
    let delivered = delivered_ids.iter().map(Vec::len).sum();
    Ok(Report {
        planned_frames: experiment.frames,
        offered_frames: offered_frames.load(Ordering::Relaxed),
        read_frames,
        enqueued,
        delivered,
        undelivered_enqueued: enqueued - delivered,
        skipped,
        rejected_full,
        disconnected,
        ingress_failure: failure,
        delivered_ids,
        drained,
        destroyed: diagnostic.destroyed,
        domain_reused,
        headroom_error,
    })
}
