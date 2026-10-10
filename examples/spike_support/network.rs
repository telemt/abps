//! Real TCP integration. The server never generates arrivals or measures client
//! delivery latency. A separate process owns source pacing and validating readers.
use super::{
    backend::{Fixture, Frame, Origin},
    pin_worker,
};
use crate::perf_support::{Args, Result, metrics::Latency};
use abps_reference::{
    self as reference, CapacityPolicy, ClassConfig, ClassReservation, Config, Error,
};
use serde_json::{Value, json};
use std::io::{IoSlice, Write};
use std::net::{TcpListener as Listener, TcpStream as Socket};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

mod ingress;
mod plan;
#[cfg(test)]
mod scripted_io;
mod server;
#[cfg(feature = "local-branches-spike")]
mod worker_fanout;
mod writer;

const HEADER: usize = 20;
#[derive(Clone)]
struct Settings {
    workers: usize,
    streams: usize,
    fanout: usize,
    queue: usize,
    write_batch: usize,
    read_buf: bool,
    capacity_policy: CapacityPolicy,
    class_slots: Vec<usize>,
    class_capacities: Vec<usize>,
    distribution: String,
    domain_limit: Option<usize>,
    require_envelope: bool,
    batch_fanout: bool,
    local_branches: bool,
    worker_branches: bool,
    max_frame_age: Option<Duration>,
    send_buffer_bytes: Option<usize>,
    seed: u64,
    keyframe_burst: usize,
    seconds: Duration,
    setup_timeout: Duration,
    drain_timeout: Duration,
    zero_payload: bool,
    frames: bool,
    remote: bool,
    skip: bool,
    slow: bool,
}
impl Settings {
    fn new(args: &Args) -> Result<Self> {
        let workers = args.n("threads", 1);
        let streams = args.n("streams", workers);
        let fanout = args.n("fanout", 20);
        let queue = args.n("queue", 8);
        let write_batch = args.n("write-batch", 1);
        let read_buf = match args.get("ingress", "initialized") {
            "initialized" => false,
            "read-buf" => true,
            _ => return Err("invalid ingress mode".into()),
        };
        if read_buf
            && (!cfg!(feature = "bytes-interop")
                || !matches!(args.get("backend", "berry"), "berry" | "mosaic"))
        {
            return Err("read-buf requires bytes-interop and a berry or mosaic backend".into());
        }
        let keyframe_burst = args.n("keyframe-burst", 1);
        let capacity_policy = match args.get("capacity-policy", "unreserved") {
            "unreserved" => CapacityPolicy::Unreserved,
            "reserved" => CapacityPolicy::Reserved,
            _ => return Err("invalid capacity policy".into()),
        };
        let class_slots = args
            .get("class-slots", "64,64")
            .split(',')
            .map(str::parse::<usize>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let distribution = args.get("distribution", "packets");
        if !matches!(
            distribution,
            "packets" | "frames" | "mixed-frames" | "boundaries"
        ) {
            return Err("invalid distribution".into());
        }
        let frames = distribution != "packets";
        let class_capacities = if let Some(c) = args.0.get("class-capacities") {
            if args.0.contains_key("class-profile") {
                return Err("choose class-capacities or class-profile".into());
            }
            c.split(',')
                .map(str::parse::<usize>)
                .collect::<std::result::Result<Vec<_>, _>>()?
        } else {
            match args.get("class-profile", "legacy") {
                "legacy" => {
                    if frames {
                        vec![65536, 2097152]
                    } else {
                        vec![512, 1500]
                    }
                }
                "geometric" => {
                    if frames {
                        (12..=21).map(|p| 1usize << p).collect()
                    } else {
                        (8..=11).map(|p| 1usize << p).collect()
                    }
                }
                _ => return Err("invalid class-profile".into()),
            }
        };
        if class_capacities.is_empty()
            || class_capacities.len() > 64
            || class_capacities[0] == 0
            || class_capacities.windows(2).any(|w| w[0] >= w[1])
            || class_slots.contains(&0)
            || class_slots.len() != class_capacities.len()
        {
            return Err(
                "sorted positive class-capacities and matching positive class-slots required"
                    .into(),
            );
        }
        let domain_limit = args
            .0
            .get("domain-limit-bytes")
            .map(|n| n.parse::<usize>())
            .transpose()?;
        let require_envelope = match args.get("capacity-check", "report") {
            "report" => false,
            "require-envelope" => true,
            _ => return Err("invalid capacity-check".into()),
        };
        let (batch_fanout, local_branches, worker_branches) = match args.get("fanout-api", "scalar")
        {
            "scalar" => (false, false, false),
            "batch" => (true, false, false),
            "local-branches"
                if cfg!(feature = "local-branches-spike")
                    && args.get("backend", "berry") == "mosaic" =>
            {
                (false, true, false)
            }
            "worker-branches"
                if cfg!(feature = "local-branches-spike")
                    && args.get("backend", "berry") == "mosaic" =>
            {
                (false, false, true)
            }
            _ => return Err("invalid fanout-api".into()),
        };
        let max_frame_age = args
            .0
            .get("max-frame-age-ms")
            .map(|s| s.parse::<u64>())
            .transpose()?;
        if max_frame_age.is_some_and(|ms| ms == 0 || ms > 3_600_000) {
            return Err("max-frame-age-ms must be 1..3600000".into());
        }
        let send_buffer_bytes = args
            .0
            .get("send-buffer-bytes")
            .map(|s| s.parse::<usize>())
            .transpose()?;
        if send_buffer_bytes.is_some_and(|n| n == 0 || n > i32::MAX as usize) {
            return Err("invalid send-buffer-bytes".into());
        }
        if !(1..=8).contains(&workers)
            || !(workers..=64).contains(&streams)
            || !(1..=20).contains(&fanout)
            || !(1..=1024).contains(&queue)
            || !(1..=8).contains(&write_batch)
            || !(1..=20).contains(&keyframe_burst)
        {
            return Err("invalid worker/stream/fanout/queue dimensions".into());
        }
        let remote = match args.get("return", "local") {
            "local" => false,
            "remote" => true,
            _ => return Err("invalid return mode".into()),
        };
        let skip = match args.get("overflow", "disconnect") {
            "disconnect" => false,
            "skip" => true,
            _ => return Err("invalid overflow policy".into()),
        };
        let setup_ms = args.n("setup-timeout-ms", 30_000);
        let drain_ms = args.n("drain-timeout-ms", 2_000);
        if !(1..=300_000).contains(&setup_ms) || !(1..=300_000).contains(&drain_ms) {
            return Err("setup/drain timeouts must be 1..300000 ms".into());
        }
        let zero_payload = match args.get("preparation", "lazy") {
            "lazy" => false,
            "zero-payload" if args.get("backend", "berry") == "mosaic" => true,
            _ => return Err("preparation must be lazy or mosaic zero-payload".into()),
        };
        Ok(Self {
            setup_timeout: Duration::from_millis(setup_ms as u64),
            drain_timeout: Duration::from_millis(drain_ms as u64),
            zero_payload,
            workers,
            streams,
            fanout,
            queue,
            write_batch,
            read_buf,
            capacity_policy,
            class_slots,
            class_capacities,
            distribution: distribution.to_owned(),
            domain_limit,
            require_envelope,
            batch_fanout,
            local_branches,
            worker_branches,
            max_frame_age: max_frame_age.map(Duration::from_millis),
            send_buffer_bytes,
            seed: args.n(
                "seed",
                if matches!(distribution, "mixed-frames" | "boundaries") {
                    1
                } else {
                    42
                },
            ) as u64,
            keyframe_burst,
            frames,
            remote,
            skip,
            slow: args.get("slow", "no") == "yes",
            seconds: args.seconds("seconds", 10.0),
        })
    }
    fn runtime_workers(&self) -> usize {
        if self.remote {
            self.workers.max(2)
        } else {
            self.workers
        }
    }
    fn capacities(&self) -> &[usize] {
        &self.class_capacities
    }
    fn max_class(&self) -> usize {
        *self.class_capacities.last().unwrap()
    }
    fn max_payload(&self) -> usize {
        if self.frames { 2097152 } else { 1500 }
    }
    fn payload_size(&self, stream: usize, sequence: u64) -> usize {
        // Independent stream phase: global routing IDs cannot concentrate all
        // keyframes in one shard. Bursts preserve one large frame in twenty.
        let phase = self
            .seed
            .wrapping_add((stream as u64).wrapping_mul(0x9e3779b97f4a7c15));
        let large = if self.frames {
            sequence.wrapping_add(phase) % (20 * self.keyframe_burst as u64)
                < self.keyframe_burst as u64
        } else {
            sequence.wrapping_mul(17).wrapping_add(phase) % 20 >= 10
        };
        match self.distribution.as_str() {
            "mixed-frames" => {
                // SplitMix64 finalizer, deterministic per stream sequence.
                let mut x = phase.wrapping_add(sequence.wrapping_mul(0x9e3779b97f4a7c15));
                x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
                x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
                x ^= x >> 31;
                let (min, max) = if large {
                    (512 * 1024, 2 * 1024 * 1024)
                } else {
                    (32 * 1024, 96 * 1024)
                };
                min + (x % ((max - min + 1) as u64)) as usize
            }
            "boundaries" => [
                65535, 65536, 65537, 131071, 131072, 131073, 2097151, 2097152,
            ][(sequence % 8) as usize],
            _ => {
                if self.frames {
                    [65536, 2097152][usize::from(large)]
                } else {
                    [512, 1500][usize::from(large)]
                }
            }
        }
    }
}
#[derive(Clone, Copy, Debug)]
struct Header {
    len: usize,
    id: u64,
    due: u64,
}
impl Header {
    fn encode(self) -> [u8; HEADER] {
        let mut bytes = [0u8; HEADER];
        bytes[..4].copy_from_slice(&(self.len as u32).to_be_bytes());
        bytes[4..12].copy_from_slice(&self.id.to_be_bytes());
        bytes[12..].copy_from_slice(&self.due.to_be_bytes());
        bytes
    }
    async fn read<R: tokio::io::AsyncRead + Unpin>(stream: &mut R) -> std::io::Result<Self> {
        let mut bytes = [0; HEADER];
        stream.read_exact(&mut bytes).await?;
        Ok(Self {
            len: u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize,
            id: u64::from_be_bytes(bytes[4..12].try_into().unwrap()),
            due: u64::from_be_bytes(bytes[12..].try_into().unwrap()),
        })
    }
}
async fn write_frame(
    socket: &mut TcpStream,
    header: Header,
    payload: &[u8],
) -> std::io::Result<()> {
    let bytes = header.encode();
    let mut offset = 0;
    while offset < HEADER + payload.len() {
        let written = if offset < HEADER {
            socket
                .write_vectored(&[IoSlice::new(&bytes[offset..]), IoSlice::new(payload)])
                .await?
        } else {
            socket.write(&payload[offset - HEADER..]).await?
        };
        if written == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        offset += written;
    }
    Ok(())
}
fn into_async(socket: Socket) -> std::io::Result<TcpStream> {
    socket.set_nonblocking(true)?;
    socket.set_nodelay(true)?;
    TcpStream::from_std(socket)
}
enum QueuedFrame {
    Shared(Frame),
    #[cfg(feature = "local-branches-spike")]
    Branch(abps_reference::mosaic::Branch),
}
impl From<Frame> for QueuedFrame {
    fn from(frame: Frame) -> Self {
        Self::Shared(frame)
    }
}
impl AsRef<[u8]> for QueuedFrame {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Shared(f) => f.as_ref(),
            #[cfg(feature = "local-branches-spike")]
            Self::Branch(f) => f.as_ref(),
        }
    }
}
enum WriterFrame {
    Shared(Frame),
    #[cfg(feature = "local-branches-spike")]
    Local(abps_reference::mosaic::LocalBuf),
}
impl AsRef<[u8]> for WriterFrame {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Shared(f) => f.as_ref(),
            #[cfg(feature = "local-branches-spike")]
            Self::Local(f) => f.as_ref(),
        }
    }
}
struct Message<F = QueuedFrame> {
    header: Header,
    frame: F,
    deadline: Option<tokio::time::Instant>,
}
impl Message {
    fn into_writer(self) -> Message<WriterFrame> {
        let frame = match self.frame {
            QueuedFrame::Shared(f) => WriterFrame::Shared(f),
            #[cfg(feature = "local-branches-spike")]
            QueuedFrame::Branch(f) => WriterFrame::Local(f.into_local()),
        };
        Message {
            header: self.header,
            deadline: self.deadline,
            frame,
        }
    }
}
struct Subscriber {
    queue: mpsc::Sender<Message>,
    active: bool,
    cancel: Option<oneshot::Sender<()>>,
}
struct WriterJob {
    socket: Socket,
    queue: mpsc::Receiver<Message>,
    cancel: oneshot::Receiver<()>,
}
enum SetupJob {
    Writer(WriterJob),
    #[cfg(feature = "local-branches-spike")]
    Group(worker_fanout::Job),
}
struct Session {
    stream: usize,
    socket: Socket,
    origin: Origin,
    subscribers: Vec<Subscriber>,
    scratch: Box<[u8]>,
    #[cfg(feature = "local-branches-spike")]
    worker_fanout: Option<worker_fanout::Ingress>,
}
#[derive(Default)]
struct IngressStats {
    stream: usize,
    #[cfg(feature = "local-branches-spike")]
    worker_flow: Option<Arc<worker_fanout::Flow>>,
    dispatched: u64,
    queue_credit_attempts: u64,
    queue_credit_acquired: u64,
    received: u64,
    accepted: u64,
    discarded: u64,
    ingress_rejected: u64,
    budget_discards: u64,
    class_discards: u64,
    domain_class_discards: u64,
    contention_discards: u64,
    no_receiver_discards: u64,
    fatal_discards: u64,
    enqueued: u64,
    skipped: u64,
    rejected: u64,
    no_subscriber: u64,
    disconnected: u64,
    admission_contended: u64,
    scalar_clone_contended: u64,
    batch_clone_contended: u64,
    stop: Option<String>,
}
impl IngressStats {
    fn json(&self) -> Value {
        #[cfg(feature = "local-branches-spike")]
        let (forwarded, cancelled) = self.worker_flow.as_ref().map_or((0, 0), |f| f.sample());
        #[cfg(not(feature = "local-branches-spike"))]
        let (forwarded, cancelled) = (0u64, 0u64);
        json!({"stream":self.stream,"received":self.received,"accepted":self.accepted,
        "enqueued":self.enqueued+forwarded,
        "dispatched_recipient_rights":self.dispatched,"locally_enqueued":forwarded,
        "cancelled_before_forward":cancelled,
        "queue_credit_attempts":self.queue_credit_attempts,"queue_credit_acquired":self.queue_credit_acquired,"skipped":self.skipped,"queue_rejected":self.rejected,
        "no_subscriber_deliveries":self.no_subscriber,"disconnected":self.disconnected,
        "contended":self.admission_contended+self.scalar_clone_contended+self.batch_clone_contended,
        "contention_by_operation":{"admission":self.admission_contended,
            "scalar_clone":self.scalar_clone_contended,"batch_clone":self.batch_clone_contended},"stop":self.stop,"discarded":self.discarded,
        "ingress_rejected_deliveries":self.ingress_rejected,
        "discard_reasons":{"BudgetExceeded":self.budget_discards,"ClassDepleted":self.class_discards,
            "DomainClassDepleted":self.domain_class_discards,
            "Contended":self.contention_discards,"NoReceivers":self.no_receiver_discards,"Fatal":self.fatal_discards}})
    }
}
async fn acquire(
    origin: &Origin,
    size: usize,
    contended: &mut u64,
) -> std::result::Result<super::backend::Lease, Error> {
    for attempt in 0..3 {
        match origin.acquire(size) {
            Err(Error::Contended) => {
                *contended += 1;
                if attempt < 2 {
                    tokio::task::yield_now().await;
                }
            }
            result => return result,
        }
    }
    Err(Error::Contended)
}
async fn view(frame: &Frame, contended: &mut u64) -> std::result::Result<Frame, Error> {
    for attempt in 0..3 {
        match frame.view(0..frame.as_ref().len()) {
            Err(Error::Contended) => {
                *contended += 1;
                if attempt < 2 {
                    tokio::task::yield_now().await;
                }
            }
            result => return result,
        }
    }
    Err(Error::Contended)
}

pub fn server(args: &Args) -> Result<Value> {
    server::run(args)
}

async fn connect(port: u16, stream: usize, subscriber: usize) -> Result<TcpStream> {
    let mut socket = TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port)).await?;
    socket.set_nodelay(true)?;
    socket.write_all(&(stream as u32).to_be_bytes()).await?;
    socket.write_all(&(subscriber as u32).to_be_bytes()).await?;
    Ok(socket)
}
struct Consumer {
    stream: usize,
    subscriber: usize,
    delivered: u64,
    bytes: u64,
    overdue: u64,
    timely_bytes: u64,
    invalid: u64,
    cancelled: bool,
    latency: Latency,
}
impl Consumer {
    fn new(stream: usize, subscriber: usize) -> Self {
        Self {
            stream,
            subscriber,
            delivered: 0,
            bytes: 0,
            overdue: 0,
            timely_bytes: 0,
            invalid: 0,
            cancelled: false,
            latency: Latency::default(),
        }
    }
    fn json(&self) -> Value {
        json!({"stream":self.stream,"subscriber":self.subscriber,"delivered":self.delivered,
        "bytes":self.bytes,"overdue":self.overdue,"invalid":self.invalid,"cancelled":self.cancelled,
        "timely_delivered":self.delivered-self.overdue,"expired_delivered":self.overdue,"timely_bytes":self.timely_bytes,
        "scheduled_arrival_latency":self.latency.json()})
    }
}
async fn consume(
    mut socket: TcpStream,
    mut stats: Consumer,
    start: Instant,
    deadline: tokio::time::Instant,
    max: usize,
    slow: Duration,
    max_age: Option<Duration>,
) -> Consumer {
    let mut payload = vec![0; max];
    let mut previous = None;
    loop {
        let frame = async {
            let header = Header::read(&mut socket).await?;
            if header.len == 0 || header.len > max {
                return Err(std::io::ErrorKind::InvalidData.into());
            }
            socket.read_exact(&mut payload[..header.len]).await?;
            Ok::<_, std::io::Error>(header)
        };
        let header = match tokio::time::timeout_at(deadline, frame).await {
            Ok(Ok(header)) => header,
            Ok(Err(error)) => {
                if error.kind() == std::io::ErrorKind::InvalidData {
                    stats.invalid += 1;
                }
                break;
            }
            Err(_) => {
                stats.cancelled = true;
                break;
            }
        };
        if previous.is_some_and(|id| header.id <= id)
            || payload[0] != 0x5a
            || payload[header.len - 1] != 0x5a
        {
            stats.invalid += 1;
            break;
        }
        previous = Some(header.id);
        let latency = start
            .elapsed()
            .saturating_sub(Duration::from_nanos(header.due));
        stats.delivered += 1;
        stats.bytes += header.len as u64;
        let expired = max_age.is_some_and(|age| latency >= age);
        stats.overdue += u64::from(expired);
        if !expired {
            stats.timely_bytes += header.len as u64;
        }
        stats.latency.record(latency);
        if !slow.is_zero()
            && tokio::time::timeout_at(deadline, tokio::time::sleep(slow))
                .await
                .is_err()
        {
            stats.cancelled = true;
            break;
        }
    }
    stats // includes completed slow samples when read/sleep is cancelled
}

mod loadgen;
pub fn loadgen(args: &Args) -> Result<Value> {
    loadgen::run(args)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_stream_grants_partition_every_root_with_stable_remainders() {
        let s = Settings::new(&Args(std::collections::BTreeMap::from([
            ("threads".into(), "3".into()),
            ("streams".into(), "8".into()),
            ("capacity-policy".into(), "reserved".into()),
        ])))
        .unwrap();
        for shared in [false, true] {
            let plan = plan::PipelinePlan::new(&s, shared, "abps").unwrap();
            for root in 0..if shared { 1 } else { s.workers } {
                let streams: Vec<_> = (0..s.streams)
                    .filter(|id| shared || id % s.workers == root)
                    .collect();
                for class in &plan.roots[root].classes {
                    let counts: Vec<_> = streams
                        .iter()
                        .map(|&id| {
                            plan.domain(id)
                                .grants
                                .iter()
                                .find(|g| g.capacity == class.capacity)
                                .map_or(0, |g| g.slots)
                        })
                        .collect();
                    assert_eq!(counts.iter().sum::<usize>(), class.slots);
                    assert!(counts.windows(2).all(|p| p[0] >= p[1] && p[0] - p[1] <= 1));
                }
            }
        }
    }

    #[tokio::test]
    async fn expired_network_delivery_stays_in_counts_and_latency_but_is_not_useful() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let socket = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (mut sender, _) = listener.accept().await.unwrap();
        write_frame(
            &mut sender,
            Header {
                len: 4,
                id: 1,
                due: 0,
            },
            &[0x5a; 4],
        )
        .await
        .unwrap();
        sender.shutdown().await.unwrap();
        let result = consume(
            socket,
            Consumer::new(0, 0),
            Instant::now() - Duration::from_millis(200),
            tokio::time::Instant::now() + Duration::from_secs(1),
            4,
            Duration::ZERO,
            Some(Duration::from_millis(100)),
        )
        .await;
        assert_eq!(
            (
                result.delivered,
                result.overdue,
                result.timely_bytes,
                result.bytes
            ),
            (1, 1, 0, 4)
        );
        assert_eq!(result.latency.json()["samples"], 1);
        assert_eq!(result.invalid, 0);
    }

    #[tokio::test]
    async fn cancellation_preserves_completed_samples_before_partial_frame() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let socket = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (mut sender, _) = listener.accept().await.unwrap();
        let writer = tokio::spawn(async move {
            write_frame(
                &mut sender,
                Header {
                    len: 16,
                    id: 1,
                    due: 0,
                },
                &[0x5a; 16],
            )
            .await
            .unwrap();
            sender
                .write_all(
                    &Header {
                        len: 16,
                        id: 2,
                        due: 0,
                    }
                    .encode(),
                )
                .await
                .unwrap();
            sender.write_all(&[0x5a; 4]).await.unwrap();
            std::future::pending::<()>().await;
        });
        let result = consume(
            socket,
            Consumer::new(0, 0),
            Instant::now(),
            tokio::time::Instant::now() + Duration::from_millis(100),
            16,
            Duration::ZERO,
            Some(Duration::from_millis(100)),
        )
        .await;
        writer.abort();
        let _ = writer.await;
        assert!(result.cancelled);
        assert_eq!(result.delivered, 1);
        assert_eq!(result.latency.json()["samples"], 1);
        assert_eq!(result.invalid, 0);
    }
}

#[cfg(feature = "local-branches-spike")]
pub fn worker_allocation_probe(
    global: &stats_alloc::StatsAlloc<std::alloc::System>,
    args: &Args,
) -> Result<Value> {
    worker_fanout::allocation_probe(global, args)
}
