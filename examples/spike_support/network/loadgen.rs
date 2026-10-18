//! The pacing thread never waits for a socket. Bounded per-stream source queues
//! feed independent I/O tasks; readers and source writers run on the I/O runtime.
use super::*;
use std::sync::atomic::AtomicUsize;

#[derive(Default)]
struct Schedule {
    planned: u64,
    attempted: u64,
    missed: u64,
    enqueued: u64,
    full: u64,
    closed: u64,
    classes: Vec<Vec<u64>>,
    sequence_hash: u64,
    requested_bytes: u64,
    rounded_bytes: u64,
    unserviceable: u64,
}

pub(super) fn route(id: u64, streams: usize, skew: bool) -> usize {
    if skew && id % 10 < 9 {
        0
    } else {
        (id as usize / if skew { 10 } else { 1 }) % streams
    }
}

// Number of scheduled arrivals for this stream before id, including arrivals
// later missed by the pacer or skipped by the server. Received counts are not IDs.
pub(super) fn sequence_before(id: u64, stream: usize, streams: usize, skew: bool) -> u64 {
    if !skew {
        return id / streams as u64;
    }
    let blocks = id / 10;
    let routed = if blocks > stream as u64 {
        1 + (blocks - 1 - stream as u64) / streams as u64
    } else {
        0
    };
    routed
        + if stream == 0 {
            blocks * 9 + (id % 10).min(9)
        } else {
            0
        }
}

fn pace(
    args: Args,
    settings: Settings,
    start: Instant,
    rate: u64,
    queues: Vec<mpsc::Sender<Header>>,
    skew: bool,
) -> Result<Schedule> {
    let mut pin = args;
    pin.0.remove("cpus");
    if let Some(cpus) = pin.0.get("generator-cpus").cloned() {
        pin.0.insert("cpus".into(), cpus);
    }
    pin_worker(&pin, 0)?;
    let planned = (settings.seconds.as_secs_f64() * rate as f64).floor() as u64;
    let mut stats = Schedule {
        planned,
        classes: vec![vec![0; settings.capacities().len()]; settings.streams],
        ..Schedule::default()
    };
    let mut sequence = vec![0u64; settings.streams];
    for id in 0..planned {
        let stream = route(id, settings.streams, skew);
        let len = settings.payload_size(stream, sequence[stream]);
        sequence[stream] += 1;
        let class = settings.capacities().partition_point(|&n| n < len);
        if class < settings.capacities().len() {
            stats.classes[stream][class] += 1;
            stats.rounded_bytes += settings.capacities()[class] as u64;
        } else {
            stats.unserviceable += 1;
        }
        stats.requested_bytes += len as u64;
        for value in [stream as u64, sequence[stream] - 1, len as u64] {
            stats.sequence_hash = (stats.sequence_hash ^ value).wrapping_mul(0x100000001b3);
        }
        let due = Duration::from_secs_f64(id as f64 / rate as f64);
        let at = start + due;
        if let Some(wait) = at.checked_duration_since(Instant::now())
            && wait > Duration::from_millis(1)
        {
            std::thread::sleep(wait - Duration::from_millis(1));
        }
        while Instant::now() < at {
            std::hint::spin_loop();
        }
        if Instant::now()
            .saturating_duration_since(start)
            .as_secs_f64()
            * rate as f64
            >= (id + 1) as f64
        {
            stats.missed += 1;
            continue;
        }
        stats.attempted += 1;
        match queues[stream].try_send(Header {
            len,
            id,
            due: due.as_nanos() as u64,
        }) {
            Ok(()) => stats.enqueued += 1,
            Err(mpsc::error::TrySendError::Full(_)) => stats.full += 1,
            Err(mpsc::error::TrySendError::Closed(_)) => stats.closed += 1,
        }
    }
    Ok(stats) // drops every sender, including on failure or unwind
}

async fn source(
    mut socket: TcpStream,
    mut queue: mpsc::Receiver<Header>,
    mut payload: Vec<u8>,
    seed: u64,
    stream: usize,
    end: tokio::time::Instant,
) -> (u64, u64) {
    let (mut sent, mut failed) = (0, 0);
    let mut broken = false;
    while let Some(header) = queue.recv().await {
        if broken {
            failed += 1;
            continue;
        }
        payload::fill_payload(&mut payload[..header.len], seed, stream, header.id);
        match tokio::time::timeout_at(
            end,
            write_frame(&mut socket, header, &payload[..header.len]),
        )
        .await
        {
            Ok(Ok(())) => sent += 1,
            _ => {
                failed += 1;
                broken = true;
            }
        }
    }
    let _ = tokio::time::timeout(Duration::from_secs(1), socket.shutdown()).await;
    (sent, failed)
}

pub(super) fn run(args: &Args) -> Result<Value> {
    let settings = Settings::new(args)?;
    let ports: Vec<u16> = args
        .get("ports", "")
        .split(',')
        .map(str::parse)
        .collect::<std::result::Result<_, _>>()?;
    if ports.len() != settings.workers {
        return Err("one port per issuer worker is required".into());
    }
    let rate = args.n("rate", if settings.frames { 200 } else { 1000 });
    let queue_capacity = args.n("source-queue", 64);
    if rate == 0 || rate > 10_000_000 || !(1..=1024).contains(&queue_capacity) {
        return Err("invalid source rate/queue".into());
    }
    let skew = match args.get("load", "uniform") {
        "uniform" => false,
        "skew" => true,
        _ => return Err("invalid load distribution".into()),
    };
    let mut io_pin = args.clone();
    io_pin.0.remove("cpus");
    if let Some(cpus) = args.0.get("reader-cpus") {
        io_pin.0.insert("cpus".into(), cpus.clone());
    }
    pin_worker(&io_pin, 0)?;
    let io_threads = args
        .0
        .get("reader-cpus")
        .map_or(2, |s| s.split(',').count());
    let next = AtomicUsize::new(0);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(io_threads)
        .on_thread_start(move || {
            pin_worker(&io_pin, next.fetch_add(1, Ordering::Relaxed)).expect("I/O worker affinity")
        })
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let mut sockets = Vec::new();
        let mut sources = Vec::new();
        for stream in 0..settings.streams {
            sources.push(connect(ports[stream % settings.workers], stream, settings.fanout).await?);
            for subscriber in 0..settings.fanout {
                sockets.push((
                    connect(ports[stream % settings.workers], stream, subscriber).await?,
                    stream,
                    subscriber,
                ));
            }
        }
        for socket in &mut sources {
            let mut ready = [0];
            socket.read_exact(&mut ready).await?;
            if ready != [1] {
                return Err("server readiness handshake failed".into());
            }
        }
        // Scratch and histograms are prepared before the scheduled window.
        let consumer_storage: Vec<_> = sockets.iter().map(|(_, stream, subscriber)|
            (Consumer::new(*stream, *subscriber), vec![0; settings.max_payload()])).collect();
        let source_storage: Vec<_> = sources.iter().map(|_|vec![0; settings.max_payload()]).collect();
        let validation = Validation { settings: settings.clone(), rate: rate as u64,
            planned: (settings.seconds.as_secs_f64() * rate as f64).floor() as u64, skew };
        let start = Instant::now() + Duration::from_millis(100);
        let end = start + settings.seconds;
        let reader_deadline = tokio::time::Instant::from_std(end + Duration::from_secs(2));
        let mut consumers = Vec::new();
        for ((socket, stream, subscriber), (stats, scratch)) in sockets.into_iter().zip(consumer_storage) {
            let delay = if settings.slow && stream == 0 && subscriber == 0 {
                Duration::from_secs_f64(4.0 * settings.streams as f64 / rate as f64)
                    .max(Duration::from_millis(5))
            } else {
                Duration::ZERO
            };
            consumers.push(tokio::spawn(consume(
                socket,
                stats,
                start,
                reader_deadline,
                scratch,
                delay,
                validation.clone(),
            )));
        }
        let mut writers = Vec::new();
        let mut queues = Vec::new();
        for (stream, (socket, scratch)) in sources.into_iter().zip(source_storage).enumerate() {
            let (tx, rx) = mpsc::channel(queue_capacity);
            queues.push(tx);
            writers.push(tokio::spawn(source(
                socket,
                rx,
                scratch,
                settings.seed,
                stream,
                tokio::time::Instant::from_std(end),
            )));
        }
        let (completion, completed) = oneshot::channel();
        let pace_args = args.clone();
        let pace_settings = settings.clone();
        let thread = std::thread::spawn(move || {
            let _ = completion.send(pace(
                pace_args,
                pace_settings,
                start,
                rate as u64,
                queues,
                skew,
            ));
        });
        let schedule = completed.await.map_err(|_| "pacer panicked")??;
        thread.join().map_err(|_| "pacer panicked")?;
        let (mut sent, mut failed) = (0, 0);
        for writer in writers {
            let (s, f) = writer.await.map_err(|_| "source writer panicked")?;
            sent += s;
            failed += f;
        }
        let source_completed_seconds = start.elapsed().as_secs_f64();
        let mut total = Latency::default();
        let (mut delivered, mut bytes, mut overdue, mut invalid, mut timely_bytes) = (0, 0, 0, 0, 0);
        let mut results = Vec::new();
        for task in consumers {
            let c = task.await.map_err(|_| "consumer panicked")?;
            delivered += c.delivered;
            bytes += c.bytes;
            overdue += c.overdue;
            timely_bytes += c.timely_bytes;
            invalid += c.invalid;
            total.merge(&c.latency);
            results.push(c.json());
        }
        assert_eq!(schedule.planned, schedule.attempted + schedule.missed);
        assert_eq!(
            schedule.attempted,
            schedule.enqueued + schedule.full + schedule.closed
        );
        assert_eq!(schedule.enqueued, sent + failed);
        Ok(json!({"role":"loadgen", "failed":invalid!=0, "payload_pattern":payload::VERSION,
            "measurement_seconds":settings.seconds.as_secs_f64(),
            "source_completed_seconds":source_completed_seconds,
            "load_and_drain_seconds":start.elapsed().as_secs_f64(), "source":{
            "planned":schedule.planned,"attempted":schedule.attempted,"sent":sent,
            "missed_arrivals":schedule.missed,"failed_writes":failed,"enqueued":schedule.enqueued,
            "queue_full":schedule.full,"queue_closed":schedule.closed,
            "planned_classes_by_stream":schedule.classes,"sequence_hash":format!("{:016x}",schedule.sequence_hash),
            "planned_requested_bytes":schedule.requested_bytes,"planned_rounded_bytes":schedule.rounded_bytes,"unserviceable_frames":schedule.unserviceable,"queue_capacity":queue_capacity,
            "seed":settings.seed,"keyframe_burst":settings.keyframe_burst},
            "consumers":results,"total":{"invalid":invalid,"delivered":delivered,"bytes":bytes,"overdue":overdue,
            "timely_delivered":delivered-overdue,"expired_delivered":overdue,"timely_bytes":timely_bytes,
            "max_frame_age_ms":settings.max_frame_age.map(|a|a.as_millis()),
            "timely_delivered_per_planned_s":(delivered-overdue) as f64/settings.seconds.as_secs_f64(),
            "timely_bytes_per_planned_s":timely_bytes as f64/settings.seconds.as_secs_f64(),
            "delivered_per_planned_s":delivered as f64/settings.seconds.as_secs_f64(),
            "scheduled_arrival_latency":total.json()},"pid":std::process::id()}))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sizes_follow_each_stream_independently_of_routing_and_bursts() {
        let args = Args(std::collections::BTreeMap::from([
            ("threads".into(), "4".into()),
            ("streams".into(), "4".into()),
            ("distribution".into(), "frames".into()),
            ("keyframe-burst".into(), "4".into()),
        ]));
        let settings = Settings::new(&args).unwrap();
        for skew in [false, true] {
            let mut sequence = [0; 4];
            let mut large = [0; 4];
            for id in 0..32000 {
                let stream = route(id, 4, skew);
                assert_eq!(sequence_before(id, stream, 4, skew), sequence[stream]);
                large[stream] +=
                    usize::from(settings.payload_size(stream, sequence[stream]) == 2097152);
                sequence[stream] += 1;
            }
            for i in 0..4 {
                assert_eq!(large[i] as u64 * 20, sequence[i]);
            }
        }
    }
    #[test]
    fn full_source_queue_cannot_block_another_stream() {
        let (a, _a) = mpsc::channel(1);
        let (b, mut b_rx) = mpsc::channel(1);
        let frame = Header {
            len: 1,
            id: 0,
            due: 0,
        };
        a.try_send(frame).unwrap();
        assert!(matches!(
            a.try_send(frame),
            Err(mpsc::error::TrySendError::Full(_))
        ));
        b.try_send(frame).unwrap();
        assert_eq!(b_rx.try_recv().unwrap().len, 1);
    }
}
