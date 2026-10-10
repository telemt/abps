//! Independently scheduled loopback load. No per-frame acknowledgement or
//! implicit application retry after the three explicit Contended attempts.
use super::backend::{Fixture, Frame, Lease, Origin};
use super::metrics::{Counts, Latency};
use super::{AnyError, Args, Result};
use abps_reference::{ClassConfig, Error};
use serde_json::{Value, json};
use std::io::IoSlice;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

const SUBSCRIBERS: usize = 20;
const QUEUE: usize = 8;
const SLOTS: usize = 64;
const HEADER: usize = 20;
struct Message {
    frame: Frame,
    id: u64,
    due_ns: u64,
}
struct Subscriber {
    queue: Option<mpsc::Sender<Message>>,
    writer: Option<JoinHandle<std::io::Result<u64>>>,
    reader: JoinHandle<Result<ReaderStats>>,
    stop_reader: oneshot::Sender<()>,
}
struct ReaderStats {
    delivered: u64,
    bytes: u64,
    overdue: u64,
    latency: Latency,
    cancelled: bool,
}
impl ReaderStats {
    fn json(&self) -> Value {
        json!({"delivered": self.delivered, "bytes": self.bytes,
            "overdue_100ms": self.overdue, "scheduled_arrival_latency": self.latency.json(),
            "cancelled_after_drain_deadline": self.cancelled})
    }
}
async fn pair() -> std::io::Result<(TcpStream, TcpStream)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (a, b) = tokio::try_join!(TcpStream::connect(address), listener.accept())?;
    a.set_nodelay(true)?;
    b.0.set_nodelay(true)?;
    Ok((a, b.0))
}
async fn write_frame(
    socket: &mut TcpStream,
    id: u64,
    due_ns: u64,
    payload: &[u8],
) -> std::io::Result<()> {
    let mut header = [0; HEADER];
    header[..4].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    header[4..12].copy_from_slice(&id.to_be_bytes());
    header[12..].copy_from_slice(&due_ns.to_be_bytes());
    let mut descriptors = [IoSlice::new(&header), IoSlice::new(payload)];
    let mut remaining = &mut descriptors[..];
    while !remaining.is_empty() {
        let n = socket.write_vectored(remaining).await?;
        if n == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        IoSlice::advance_slices(&mut remaining, n);
    }
    Ok(())
}
// EOF at frame boundary or cancellation partway through a frame ends that
// connection. Only complete, validated frames become delivered samples.
async fn header(
    socket: &mut (impl AsyncRead + Unpin),
) -> std::io::Result<Option<(usize, u64, u64)>> {
    let mut bytes = [0; HEADER];
    match socket.read_exact(&mut bytes).await {
        Ok(_) => Ok(Some((
            u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize,
            u64::from_be_bytes(bytes[4..12].try_into().unwrap()),
            u64::from_be_bytes(bytes[12..].try_into().unwrap()),
        ))),
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
            ) =>
        {
            Ok(None)
        }
        Err(e) => Err(e),
    }
}
async fn consume(
    mut socket: impl AsyncRead + Unpin,
    max_size: usize,
    start: Instant,
    slow_delay: Duration,
    ready: oneshot::Sender<()>,
    stop: oneshot::Receiver<()>,
) -> Result<ReaderStats> {
    let mut payload = vec![0; max_size];
    let mut stats = ReaderStats {
        delivered: 0,
        bytes: 0,
        overdue: 0,
        latency: Latency::default(),
        cancelled: false,
    };
    let mut previous = None;
    let _ = ready.send(());
    let read_loop = async {
        while let Some((len, id, due_ns)) = header(&mut socket).await? {
            if len == 0 || len > max_size {
                return Err("invalid frame length".into());
            }
            if previous.is_some_and(|prev| id <= prev) {
                return Err("reordered frame".into());
            }
            if let Err(e) = socket.read_exact(&mut payload[..len]).await {
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
                ) {
                    break;
                }
                return Err(e.into());
            }
            if payload[0] != 0x5a || payload[len - 1] != 0x5a {
                return Err("payload corrupted".into());
            }
            previous = Some(id);
            let latency =
                Instant::now().saturating_duration_since(start + Duration::from_nanos(due_ns));
            stats.latency.record(latency);
            stats.overdue += u64::from(latency > Duration::from_millis(100));
            stats.delivered += 1;
            stats.bytes += len as u64;
            // An explicitly slowed real reader (not an ingress ACK or producer gate).
            if !slow_delay.is_zero() {
                tokio::time::sleep(slow_delay).await;
            }
        }
        Ok::<(), AnyError>(())
    };
    let cancelled = tokio::select! {
        result = read_loop => { result?; false },
        _ = stop => true,
    };
    // Cancelling a slow reader must preserve its completed samples, including
    // the worst queueing delays. Partial frames are never counted as delivered.
    stats.cancelled = cancelled;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn cancellation_keeps_completed_slow_consumer_samples() -> Result<()> {
        let mut wire = vec![0; HEADER + 16];
        wire[..4].copy_from_slice(&16u32.to_be_bytes());
        wire[HEADER..].fill(0x5a);
        let (ready_tx, _ready_rx) = oneshot::channel();
        let (stop_tx, stop_rx) = oneshot::channel();
        let mut reader = Box::pin(consume(
            &wire[..],
            16,
            Instant::now(),
            Duration::from_secs(3600),
            ready_tx,
            stop_rx,
        ));
        // Buffered input completes synchronously in this poll. The long
        // processing delay then suspends after one completed delivery.
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(reader.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        stop_tx.send(()).unwrap();
        let stats = reader.await?;
        assert!(stats.cancelled);
        assert_eq!(stats.delivered, 1);
        assert_eq!(stats.bytes, 16);
        assert_eq!(stats.latency.json()["samples"], 1);
        Ok(())
    }
}
async fn acquire(
    origin: &Origin,
    size: usize,
    contended: &mut u64,
) -> std::result::Result<Lease, Error> {
    for attempt in 0..3 {
        match origin.acquire(size) {
            Err(Error::Contended) => {
                *contended += 1;
                if attempt == 2 {
                    return Err(Error::Contended);
                }
                tokio::task::yield_now().await;
            }
            result => return result,
        }
    }
    unreachable!()
}
async fn view(frame: &Frame, contended: &mut u64) -> std::result::Result<Frame, Error> {
    for attempt in 0..3 {
        match frame.view(0..frame.as_ref().len()) {
            Err(Error::Contended) => {
                *contended += 1;
                if attempt == 2 {
                    return Err(Error::Contended);
                }
                tokio::task::yield_now().await;
            }
            result => return result,
        }
    }
    unreachable!()
}
fn size_for(id: u64, frames: bool) -> usize {
    // A stable bijective scramble gives deterministic 50/50 or 95/5 classes.
    // Exact block ratios, no random-number overhead or producer feedback.
    let bucket = id.wrapping_mul(17).wrapping_add(7) % 20;
    if frames {
        if bucket == 0 {
            2 * 1024 * 1024
        } else {
            64 * 1024
        }
    } else if bucket < 10 {
        512
    } else {
        1500
    }
}

// Hooks are called outside measured adapter operations to separate allocation
// phases. The normal timing binary passes a no-op closure.
pub async fn run(args: &Args, mut phase: impl FnMut(&str)) -> Result<Value> {
    let frames = match args.get("distribution", "packets") {
        "frames" => true,
        "packets" => false,
        _ => return Err("unknown distribution".into()),
    };
    let skip = match args.get("overflow", "disconnect") {
        "skip" => true,
        "disconnect" => false,
        _ => return Err("unknown overflow policy".into()),
    };
    let slow = args.get("slow", "no") == "yes";
    let rate = args.n("rate", if frames { 100 } else { 1000 });
    assert!(rate > 0 && rate <= 10_000_000);
    let slow_delay = Duration::from_secs_f64(4.0 / rate as f64).max(Duration::from_millis(5));
    let max = if frames { 2 * 1024 * 1024 } else { 1500 };
    let capacities = if frames {
        [64 * 1024, 2 * 1024 * 1024]
    } else {
        [512, 1500]
    };
    let fixture = Fixture::new(
        args.get("backend", "abps"),
        capacities
            .map(|capacity| ClassConfig {
                capacity,
                slots: SLOTS,
            })
            .to_vec(),
        1,
    )?;
    let origin = fixture.open(capacities.iter().sum::<usize>() * SLOTS)?;
    let (mut source_socket, mut ingress) = pair().await?;
    let mut subscribers = Vec::with_capacity(SUBSCRIBERS);
    let mut ready = Vec::with_capacity(SUBSCRIBERS);
    // All sockets/tasks/reader storage are prepared before scheduled ingress.
    let start = Instant::now() + Duration::from_millis(200);
    for index in 0..SUBSCRIBERS {
        let (mut writer_socket, reader_socket) = pair().await?;
        let (sender, mut receiver) = mpsc::channel::<Message>(QUEUE);
        let writer = tokio::spawn(async move {
            let mut sent = 0;
            while let Some(message) = receiver.recv().await {
                write_frame(
                    &mut writer_socket,
                    message.id,
                    message.due_ns,
                    message.frame.as_ref(),
                )
                .await?;
                sent += 1;
            }
            Ok(sent)
        });
        let (ready_tx, ready_rx) = oneshot::channel();
        let (stop_reader, stop_rx) = oneshot::channel();
        ready.push(ready_rx);
        let reader = tokio::spawn(consume(
            reader_socket,
            max,
            start,
            if slow && index == 0 {
                slow_delay
            } else {
                Duration::ZERO
            },
            ready_tx,
            stop_rx,
        ));
        subscribers.push(Subscriber {
            queue: Some(sender),
            writer: Some(writer),
            reader,
            stop_reader,
        });
    }
    let payload = vec![0x5a; max];
    let duration = args.seconds("seconds", 10.0);
    let planned = (duration.as_secs_f64() * rate as f64).floor() as u64;
    let end = start + duration;
    // Exclude scheduling/setup lag from neither offered count nor latency.
    // If setup is late, the independent generator counts missed slots.
    for ready in ready {
        ready.await?;
    }
    let mut acquisition = Counts::default();
    let mut views_latency = Latency::default();
    phase("prepared");
    let source = tokio::spawn(async move {
        let mut attempted = 0;
        let mut sent = 0;
        let mut missed = 0;
        let mut id = 0;
        let mut failure = None;
        while id < planned {
            let due_ns = ((id as u128 * 1_000_000_000) / rate as u128) as u64;
            let due = start + Duration::from_nanos(due_ns);
            while Instant::now() < due {
                let remaining = due.saturating_duration_since(Instant::now());
                if remaining > Duration::from_millis(2) {
                    // Tokio timers alone have too coarse a resolution for
                    // submillisecond offered intervals. Yield for the last
                    // millisecond without blocking a runtime worker.
                    tokio::time::sleep_until(tokio::time::Instant::from_std(
                        due - Duration::from_millis(1),
                    ))
                    .await;
                } else {
                    tokio::task::yield_now().await;
                }
            }
            let now = Instant::now();
            if now >= end {
                missed += planned - id;
                break;
            }
            let current = (now.saturating_duration_since(start).as_nanos() * rate as u128
                / 1_000_000_000) as u64;
            if current > id {
                // Expired arrival slots are recorded, never a catch-up burst.
                let skipped = (current - id).min(planned - id);
                missed += skipped;
                id += skipped;
                continue;
            }
            attempted += 1;
            let size = size_for(id, frames);
            let send = write_frame(&mut source_socket, id, due_ns, &payload[..size]);
            match tokio::time::timeout_at(tokio::time::Instant::from_std(end), send).await {
                Ok(Ok(())) => sent += 1,
                Ok(Err(e)) => {
                    failure = Some(e.to_string());
                    break;
                }
                Err(_) => {
                    failure = Some("source_deadline".to_string());
                    break;
                }
            }
            id += 1;
        }
        (
            planned,
            attempted,
            sent,
            missed,
            planned - missed - attempted,
            failure,
        )
    });
    let mut accepted = 0u64;
    let mut ingress_bytes = 0u64;
    let mut enqueued = 0u64;
    let mut skipped = 0u64;
    let mut disconnected = 0u64;
    let mut queue_rejected = 0u64;
    let mut no_subscriber = 0u64;
    let mut contended_attempts = 0u64;
    let mut stop_reason = None;
    // Deadline applies to ingress; bounded drain follows. No infinite wait for
    // output that cannot be admitted, and no catch-up producer burst.
    loop {
        let received =
            tokio::time::timeout_at(tokio::time::Instant::from_std(end), header(&mut ingress))
                .await;
        let (len, id, due_ns) = match received {
            Ok(Ok(Some(h))) => h,
            Ok(Ok(None)) => break,
            Ok(Err(e)) => {
                stop_reason = Some(format!("ingress_io:{e}"));
                break;
            }
            Err(_) => {
                stop_reason = Some("measurement_deadline".to_string());
                break;
            }
        };
        if len != size_for(id, frames) || len > max {
            return Err("invalid source frame".into());
        }
        let t = Instant::now();
        let acquired = acquire(&origin, len, &mut contended_attempts).await;
        acquisition.result(
            acquired.as_ref().map(|_| ()).map_err(|e| *e),
            0,
            Some(t.elapsed()),
        );
        let mut lease = match acquired {
            Ok(b) => b,
            Err(e) => {
                stop_reason = Some(format!("acquire:{e}"));
                break;
            }
        };
        let read = ingress.read_exact(lease.initialize(len));
        match tokio::time::timeout_at(tokio::time::Instant::from_std(end), read).await {
            Ok(Ok(_)) => (),
            Ok(Err(e)) => {
                stop_reason = Some(format!("ingress_payload:{e}"));
                break;
            }
            Err(_) => {
                stop_reason = Some("payload_deadline".to_string());
                break;
            }
        }
        lease.set_filled(len);
        let frame = lease.freeze();
        let t = Instant::now();
        let mut views: [Option<Frame>; SUBSCRIBERS] = std::array::from_fn(|_| None);
        let mut view_error = None;
        for (subscriber, slot) in subscribers.iter().zip(&mut views) {
            if subscriber.queue.is_some() {
                match view(&frame, &mut contended_attempts).await {
                    Ok(v) => *slot = Some(v),
                    Err(e) => {
                        view_error = Some(e);
                        break;
                    }
                }
            }
        }
        views_latency.record(t.elapsed());
        if let Some(e) = view_error {
            stop_reason = Some(format!("view:{e}"));
            break;
        }
        accepted += 1;
        ingress_bytes += len as u64;
        for (subscriber, view) in subscribers.iter_mut().zip(views) {
            let Some(view) = view else {
                no_subscriber += 1;
                continue;
            };
            let message = Message {
                frame: view,
                id,
                due_ns,
            };
            match subscriber.queue.as_ref().unwrap().try_send(message) {
                Ok(()) => enqueued += 1,
                Err(mpsc::error::TrySendError::Full(message)) if skip => {
                    skipped += 1;
                    drop(message);
                }
                Err(error) => {
                    drop(error);
                    queue_rejected += 1;
                    disconnected += 1;
                    subscriber.queue.take();
                    if let Some(writer) = subscriber.writer.take() {
                        writer.abort();
                        let _ = writer.await;
                    }
                }
            }
        }
    }
    let ingress_elapsed = Instant::now().saturating_duration_since(start);
    drop(ingress);
    let source = source.await?;
    phase("ingress_finished");
    let at_ingress_stop = fixture.snapshot();
    for subscriber in &mut subscribers {
        subscriber.queue.take();
    }
    let mut consumers = Vec::with_capacity(SUBSCRIBERS);
    let mut total = ReaderStats {
        delivered: 0,
        bytes: 0,
        overdue: 0,
        latency: Latency::default(),
        cancelled: false,
    };
    let mut drain_cancelled = 0;
    let mut writer_errors = Vec::new();
    // All peers drain concurrently; a single common deadline bounds teardown.
    let drain_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    for mut subscriber in subscribers {
        if let Some(mut writer) = subscriber.writer.take() {
            match tokio::time::timeout_at(drain_deadline, &mut writer).await {
                Ok(Ok(Ok(_))) => (),
                Ok(Ok(Err(e))) => writer_errors.push(e.to_string()),
                Ok(Err(e)) => writer_errors.push(e.to_string()),
                Err(_) => {
                    writer.abort();
                    let _ = writer.await;
                    drain_cancelled += 1;
                }
            }
        }
        let reader = match tokio::time::timeout_at(drain_deadline, &mut subscriber.reader).await {
            Ok(stats) => stats??,
            Err(_) => {
                let _ = subscriber.stop_reader.send(());
                drain_cancelled += 1;
                // The task's own cancellation branch returns accumulated stats.
                subscriber.reader.await??
            }
        };
        total.delivered += reader.delivered;
        total.bytes += reader.bytes;
        total.overdue += reader.overdue;
        total.cancelled |= reader.cancelled;
        total.latency.merge(&reader.latency);
        consumers.push(reader.json());
    }
    phase("drained");
    drop(origin);
    let drained = fixture.finish()?;
    phase("destroyed");
    let source = json!({"planned": source.0, "attempted": source.1, "sent": source.2,
        "missed_arrivals": source.3, "not_attempted_after_stop": source.4, "stop": source.5});
    Ok(json!({
        "source": source, "accepted": accepted, "ingress_bytes": ingress_bytes,
        "accepted_per_planned_s": accepted as f64 / duration.as_secs_f64(),
        "ingress_elapsed_s": ingress_elapsed.as_secs_f64(), "planned_seconds": duration.as_secs_f64(),
        "ingress_stop": stop_reason, "enqueued": enqueued, "skipped": skipped,
        "disconnected": disconnected, "queue_rejected": queue_rejected,
        "no_subscriber_deliveries": no_subscriber, "contended_attempts": contended_attempts,
        "acquisition": acquisition.json(duration), "all_views_latency": views_latency.json(),
        "consumers": consumers, "total": total.json(),
        "delivered_bytes_per_planned_s": total.bytes as f64 / duration.as_secs_f64(),
        "undelivered_enqueued": enqueued - total.delivered,
        "drain_cancelled_tasks": drain_cancelled, "drained": drained,
        "at_ingress_stop": at_ingress_stop, "writer_errors": writer_errors,
        "queue_capacity": QUEUE, "subscribers": SUBSCRIBERS,
        "slow_delay_ms": if slow { slow_delay.as_secs_f64() * 1000.0 } else { 0.0 },
    }))
}
