//! AP-16 research ingress; ordinary ingress and its retry/default policy stay intact.
use super::*;
use tokio::io::{AsyncRead, AsyncWrite};

enum Permit {
    Tokio(mpsc::OwnedPermit<Message>),
    Tracked(tracked_queue::OwnedPermit<Message>),
    Local(local_queue::Permit<Message>),
}
impl Permit {
    fn send(self, m: Message, ready: tokio::time::Instant) {
        match self {
            Self::Tokio(p) => {
                p.send(m);
            }
            Self::Tracked(p) => p.send(m),
            Self::Local(p) => {
                local_queue::IngressPermit::Local(p).send_ready(m, ready);
            }
        }
    }
}
enum CloneScratch {
    Berry(Box<[Option<abps_reference::berry::Buf>]>),
    #[cfg(feature = "mosaic-spike")]
    Mosaic(Box<[Option<abps_reference::mosaic::Buf>]>),
    Bytes,
}
impl CloneScratch {
    fn new(backend: &str, n: usize) -> Self {
        match backend {
            "berry" => Self::Berry(std::iter::repeat_with(|| None).take(n).collect()),
            #[cfg(feature = "mosaic-spike")]
            "mosaic" => Self::Mosaic(std::iter::repeat_with(|| None).take(n).collect()),
            "bytes" => Self::Bytes,
            _ => unreachable!("validated research backend"),
        }
    }
    fn clone_into(
        &mut self,
        frame: &Frame,
        dst: &mut [Option<Frame>],
    ) -> std::result::Result<(), Error> {
        if dst.iter().any(Option::is_some) {
            return Err(Error::OutputNotEmpty);
        }
        match (self, frame) {
            (Self::Berry(s), Frame::Berry(b)) => {
                b.try_clone_into(&mut s[..dst.len()])?;
                for (d, b) in dst.iter_mut().zip(s.iter_mut()) {
                    *d = b.take().map(Frame::Berry);
                }
            }
            #[cfg(feature = "mosaic-spike")]
            (Self::Mosaic(s), Frame::Mosaic(b)) => {
                b.try_clone_into(&mut s[..dst.len()])?;
                for (d, b) in dst.iter_mut().zip(s.iter_mut()) {
                    *d = b.take().map(Frame::Mosaic);
                }
            }
            (Self::Bytes, Frame::Bytes(b)) => {
                for d in dst.iter_mut() {
                    *d = Some(Frame::Bytes(b.view(0..b.as_ref().len())?));
                }
            }
            _ => return Err(Error::InvalidConfiguration),
        }
        Ok(())
    }
}
pub fn storage_bytes(fanout: usize) -> usize {
    fanout * (std::mem::size_of::<Option<Permit>>() + 2 * std::mem::size_of::<Option<Frame>>())
}
#[derive(Default)]
struct FailureRun {
    count: u64,
    start: Option<Instant>,
    runs: u64,
    max_count: u64,
    max_ns: u128,
}
impl FailureRun {
    fn hit(&mut self) {
        if self.start.is_none() {
            self.start = Some(Instant::now());
            self.runs += 1;
        }
        self.count += 1;
    }
    fn finish(&mut self) {
        if let Some(t) = self.start.take() {
            self.max_ns = self.max_ns.max(t.elapsed().as_nanos());
        }
        self.max_count = self.max_count.max(self.count);
        self.count = 0;
    }
    fn json(&self) -> Value {
        json!({"runs":self.runs,"max_attempts":self.max_count,"max_ns":self.max_ns})
    }
}
#[derive(Default)]
struct Study {
    grant: Latency,
    attempt: Latency,
    wait: Latency,
    expired: u64,
    fill_failed: u64,
    errors: [u64; 4],
    runs: [FailureRun; 4],
    streak: usize,
    streak_start: Option<Instant>,
    max_streak: usize,
    max_streak_ns: u128,
}
impl Study {
    fn end_streak(&mut self) {
        for r in &mut self.runs {
            r.finish();
        }
        if let Some(at) = self.streak_start.take() {
            self.max_streak_ns = self.max_streak_ns.max(at.elapsed().as_nanos());
        }
        self.max_streak = self.max_streak.max(self.streak);
        self.streak = 0;
    }
    fn error(&mut self, e: Error) {
        let i = match e {
            Error::Contended => 0,
            Error::BudgetExceeded => 1,
            Error::ClassDepleted => 2,
            Error::DomainClassDepleted => 3,
            _ => return,
        };
        self.errors[i] += 1;
        for (j, r) in self.runs.iter_mut().enumerate() {
            if i == j {
                r.hit();
            } else {
                r.finish();
            }
        }
        self.streak += 1;
        self.streak_start.get_or_insert_with(Instant::now);
    }
    fn json(mut self) -> Value {
        self.end_streak();
        json!({"header_to_grant":self.grant.json(),"core_attempt":self.attempt.json(),"pacing_wait":self.wait.json(),"expired_headers":self.expired,"payload_io_failures":self.fill_failed,
        "failure_streaks":{"Contended":self.runs[0].json(),"BudgetExceeded":self.runs[1].json(),"ClassDepleted":self.runs[2].json(),"DomainClassDepleted":self.runs[3].json()},
        "core_errors":{"Contended":self.errors[0],"BudgetExceeded":self.errors[1],"ClassDepleted":self.errors[2],"DomainClassDepleted":self.errors[3]},"max_failure_streak":self.max_streak,"max_failure_streak_ns":self.max_streak_ns})
    }
}
enum Rejected {
    Deadline,
    NoReceivers,
    Memory(memory::Failure),
}
#[derive(Default)]
struct Preflight {
    skipped: u64,
    rejected: u64,
    absent: u64,
}
impl Preflight {
    fn commit(self, stats: &mut IngressStats) {
        stats.skipped += self.skipped;
        stats.rejected += self.rejected;
        stats.no_subscriber += self.absent;
    }
}
pub(in super::super) async fn run_io<S: AsyncRead + AsyncWrite + Unpin>(
    mut socket: S,
    origin: memory::StreamOrigin,
    mut subscribers: Vec<Subscriber>,
    scratch: Box<[u8]>,
    settings: Settings,
    stream: usize,
    gate: Rc<Gate>,
) -> Result<IngressStats> {
    let mut permits: Box<[Option<Permit>]> = std::iter::repeat_with(|| None)
        .take(settings.fanout)
        .collect();
    let mut views: Box<[Option<Frame>]> = std::iter::repeat_with(|| None)
        .take(settings.fanout)
        .collect();
    let backend = match &origin {
        memory::StreamOrigin::Ordinary(Origin::Berry { .. }) => "berry",
        #[cfg(feature = "mosaic-spike")]
        memory::StreamOrigin::Ordinary(Origin::Mosaic { .. })
        | memory::StreamOrigin::Managed(_) => "mosaic",
        memory::StreamOrigin::Ordinary(Origin::Bytes(_)) => "bytes",
        _ => return Err("unsupported research backend".into()),
    };
    let mut clones = settings
        .batch_fanout
        .then(|| CloneScratch::new(backend, settings.fanout));
    let mut study = Study::default();
    let mut stats = IngressStats {
        stream,
        ..IngressStats::default()
    };
    // All scratch/histograms exist before the producer readiness handshake.
    socket.write_all(&[1]).await?;
    let mut input = input::Input::new(socket, scratch, false);
    let mut previous = None;
    loop {
        let header = match Header::read_mode(&mut input, settings.keyed()).await {
            Ok(h) => h,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => {
                stats.stop = Some(format!("read_header:{e}"));
                break;
            }
        };
        if header.len == 0 || header.len > settings.max_payload() {
            stats.stop = Some("invalid_length".into());
            break;
        }
        if let Some(seq) = header.sequence {
            if previous.is_some_and(|old| seq <= old) {
                stats.stop = Some("invalid_sequence".into());
                break;
            }
            if previous.and_then(|old: u64| old.checked_add(1)) != Some(seq) {
                for s in &subscribers {
                    s.queue.gap();
                }
            }
            previous = Some(seq);
        }
        let received = tokio::time::Instant::now();
        let deadline = received + settings.max_frame_age.expect("research deadline");
        let charge = *settings
            .capacities()
            .iter()
            .find(|&&c| c >= header.len)
            .ok_or("no class")?;
        origin.offer(header.len);
        // AP-09/AP-16: received counts complete payloads (including drained
        // rejects). Header attempts and incomplete I/O have separate observations.
        // Queue outcomes become delivery losses only after that frame boundary.
        let mut decisions = Preflight::default();
        let start = Instant::now();
        let result = gate
            .attempt(stream, charge, deadline, || {
                study.wait.record(start.elapsed());
                // AP-16: permits precede the single core attempt, never pacing wait.
                for (subscriber, p) in subscribers.iter_mut().zip(permits.iter_mut()) {
                    if !subscriber.active {
                        decisions.absent += 1;
                        continue;
                    }
                    stats.queue_credit_attempts += 1;
                    let r = match &subscriber.queue {
                        local_queue::IngressSender::Tokio(s) => s
                            .clone()
                            .try_reserve_owned()
                            .map(Permit::Tokio)
                            .map_err(|e| match e {
                                mpsc::error::TrySendError::Full(_) => local_queue::Rejected::Full,
                                mpsc::error::TrySendError::Closed(_) => {
                                    local_queue::Rejected::Closed
                                }
                            }),
                        local_queue::IngressSender::Tracked(s) => {
                            s.try_reserve_owned().map(Permit::Tracked)
                        }
                        local_queue::IngressSender::Local(s) => {
                            s.reserve_frame(header.keyframe).map(Permit::Local)
                        }
                    };
                    match r {
                        Ok(right) => {
                            *p = Some(right);
                            stats.queue_credit_acquired += 1;
                        }
                        Err(local_queue::Rejected::Full | local_queue::Rejected::Policy)
                            if settings.skip =>
                        {
                            decisions.skipped += 1
                        }
                        Err(_) => {
                            decisions.rejected += 1;
                            stats.disconnected += 1;
                            subscriber.active = false;
                            if let Some(c) = subscriber.cancel.take() {
                                let _ = c.send(());
                            }
                        }
                    }
                }
                let recipients = permits.iter().flatten().count() as u64;
                if recipients == 0 {
                    return Err(Rejected::NoReceivers);
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(Rejected::Deadline);
                }
                let core = Instant::now();
                let r = origin.acquire_once(header.len);
                study.attempt.record(core.elapsed());
                r.map(|lease| (lease, recipients)).map_err(Rejected::Memory)
            })
            .await;
        let (mut lease, recipients) = match result {
            Ok(Ok(value)) => {
                study.grant.record(start.elapsed());
                study.end_streak();
                value
            }
            Err(reason) => {
                study.expired += 1;
                stats.stop = Some(reason.into());
                break;
            }
            Ok(Err(reason)) => {
                let recipients = permits.iter().flatten().count() as u64;
                for p in &mut permits {
                    drop(p.take());
                }
                let terminal = match &reason {
                    Rejected::Deadline => {
                        study.expired += 1;
                        true
                    }
                    Rejected::NoReceivers | Rejected::Memory(memory::Failure::Policy(_)) => false,
                    Rejected::Memory(memory::Failure::Core(e)) => {
                        study.error(*e);
                        if *e == Error::Contended {
                            stats.admission_contended += 1;
                        }
                        !settings.skip || !ingress::overload(*e)
                    }
                };
                if terminal {
                    stats.stop = Some("admission_terminal".into());
                    break;
                }
                if let Err(e) = before_deadline(Some(deadline), input.discard(header.len)).await {
                    stats.stop = Some(format!("discard_payload:{e}"));
                    break;
                }
                stats.received += 1;
                decisions.commit(&mut stats);
                match reason {
                    Rejected::NoReceivers => {
                        stats.discarded += 1;
                        stats.no_receiver_discards += 1;
                    }
                    Rejected::Memory(memory::Failure::Policy(_)) => {
                        stats.discarded += 1;
                        stats.memory_policy_discards += 1;
                        stats.ingress_rejected += recipients;
                    }
                    Rejected::Memory(memory::Failure::Core(e)) => {
                        stats.admission_discard(e, recipients);
                    }
                    Rejected::Deadline => unreachable!("terminal above"),
                }
                continue;
            }
        };
        if let Err(e) =
            before_deadline(Some(deadline), input.fill(&mut lease, header.len, false)).await
        {
            study.fill_failed += 1;
            stats.stop = Some(format!("read_payload:{e}"));
            break;
        }
        stats.received += 1;
        decisions.commit(&mut stats);
        let frame = lease.freeze();
        let built = if settings.batch_fanout {
            clones
                .as_mut()
                .unwrap()
                .clone_into(&frame, &mut views[..recipients as usize - 1])
        } else {
            let mut r = Ok(());
            for v in &mut views[..recipients as usize - 1] {
                match frame.view(0..frame.as_ref().len()) {
                    Ok(f) => *v = Some(f),
                    Err(e) => {
                        r = Err(e);
                        break;
                    }
                }
            }
            r
        };
        if let Err(e) = built {
            for v in &mut views {
                drop(v.take());
            }
            for p in &mut permits {
                drop(p.take());
            }
            if e == Error::Contended {
                if settings.batch_fanout {
                    stats.batch_clone_contended += 1
                } else {
                    stats.scalar_clone_contended += 1
                }
            }
            stats.admission_discard(e, recipients);
            if settings.skip && ingress::overload(e) {
                continue;
            }
            stats.stop = Some(format!("view:{e}"));
            break;
        }
        views[recipients as usize - 1] = Some(frame);
        let ready = tokio::time::Instant::now();
        if ready >= deadline {
            for v in &mut views {
                drop(v.take());
            }
            for p in &mut permits {
                drop(p.take());
            }
            study.expired += 1;
            stats.ingress_rejected += recipients;
            stats.discarded += 1;
            stats.fatal_discards += 1;
            stats.stop = Some("frame_deadline".into());
            break;
        }
        let mut index = 0;
        for p in &mut permits {
            if let Some(p) = p.take() {
                p.send(
                    Message {
                        header,
                        frame: views[index].take().unwrap().into(),
                        deadline: Some(deadline),
                    },
                    ready,
                );
                index += 1;
                stats.enqueued += 1;
            }
        }
        stats.accepted += 1;
    }
    stats.admission_study = Some(study.json());
    Ok(stats)
}

// A byte-rate limiter on both headers and payload; no read-ahead beyond credit.
// Burst is 64 bytes. Refill retains fractions and never catches up after idle.
pub async fn paced_prefix<R: AsyncRead + Unpin>(
    socket: &mut R,
    dst: &mut [u8],
    prefix: &mut usize,
    pace: &mut Option<WirePacer>,
) -> std::io::Result<bool> {
    let Some(p) = pace else {
        return payload::read_prefix(socket, dst, prefix).await;
    };
    while *prefix < dst.len() {
        let now = tokio::time::Instant::now();
        let credit = (now.saturating_duration_since(p.last).as_nanos() * p.rate as u128
            + p.fraction)
            .min(64 * SCALE);
        p.last = now;
        p.fraction = credit;
        if credit < SCALE {
            let ns = (SCALE - credit).div_ceil(p.rate as u128) as u64;
            tokio::time::sleep(Duration::from_nanos(ns)).await;
            continue;
        }
        let count = ((credit / SCALE) as usize).min(dst.len() - *prefix);
        let n = socket.read(&mut dst[*prefix..*prefix + count]).await?;
        if n == 0 {
            return Ok(false);
        }
        p.fraction -= n as u128 * SCALE;
        *prefix += n;
    }
    Ok(true)
}
pub struct WirePacer {
    rate: u64,
    last: tokio::time::Instant,
    fraction: u128,
}
impl WirePacer {
    pub fn new(rate: u64) -> Self {
        Self {
            rate,
            last: tokio::time::Instant::now(),
            fraction: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::scripted_io::{ScriptedIo, Step};
    use super::*;
    use std::future::Future;
    fn conserved(r: &IngressStats, fanout: u64) {
        assert_eq!(r.received, r.accepted + r.discarded);
        assert_eq!(
            r.received * fanout,
            r.enqueued + r.skipped + r.rejected + r.no_subscriber + r.ingress_rejected
        );
        let reasons = r.json()["discard_reasons"]
            .as_object()
            .unwrap()
            .values()
            .map(|v| v.as_u64().unwrap())
            .sum::<u64>();
        assert_eq!(r.discarded, reasons);
    }
    fn settings(fanout: usize) -> Settings {
        Settings::new(&Args(
            [
                ("case".into(), "admission-tcp".into()),
                ("backend".into(), "berry".into()),
                ("fanout".into(), fanout.to_string()),
                ("overflow".into(), "skip".into()),
                ("class-capacities".into(), "1500".into()),
                ("class-slots".into(), "1".into()),
                ("fanout-api".into(), "batch".into()),
            ]
            .into(),
        ))
        .unwrap()
    }
    fn fixture() -> Fixture {
        Fixture::new(
            "berry",
            vec![ClassConfig {
                capacity: 1500,
                slots: 1,
            }],
            1,
            None,
        )
        .unwrap()
    }
    fn subscribers(n: usize) -> (Vec<Subscriber>, Vec<mpsc::Receiver<Message>>) {
        let mut txs = Vec::new();
        let mut rxs = Vec::new();
        for _ in 0..n {
            let (tx, rx) = mpsc::channel(2);
            txs.push(Subscriber {
                queue: tx.into(),
                cancel: None,
                active: true,
            });
            rxs.push(rx);
        }
        (txs, rxs)
    }
    #[tokio::test(start_paused = true)]
    async fn ap16_hundred_recipient_fanout_and_skip_preserve_framing() {
        let f = fixture();
        let o = f.open(1500).unwrap();
        let s = settings(100);
        let g = Gate::new(s.admission.clone().unwrap(), 1);
        let (txs, mut rxs) = subscribers(100);
        let mut wire = Vec::new();
        for id in 0..3 {
            wire.extend(
                Header {
                    len: 8,
                    id,
                    ..Header::default()
                }
                .encode(),
            );
            wire.extend((id as u64).to_le_bytes());
        }
        let mut io = ScriptedIo::new(
            &wire,
            [
                Step::Data(1),
                Step::Data(20),
                Step::Data(8),
                Step::Data(20),
                Step::Data(8),
                Step::Data(20),
                Step::Data(8),
                Step::Zero,
            ],
        );
        let r = run_io(
            &mut io,
            o.into(),
            txs,
            vec![0; 64].into_boxed_slice(),
            s,
            0,
            g.clone(),
        )
        .await
        .unwrap();
        conserved(&r, 100);
        assert_eq!((r.accepted, r.enqueued, r.budget_discards), (1, 100, 2));
        assert_eq!(io.consumed, wire.len());
        assert_eq!(g.json()["committed"], 1);
        assert_eq!(g.json()["rolled_back"], 2);
        for rx in &mut rxs {
            let m = rx.try_recv().unwrap();
            assert_eq!(m.frame.as_ref(), 0u64.to_le_bytes());
            assert!(m.deadline.is_some());
            drop(m);
        }
        drop(rxs);
        f.finish().unwrap();
    }
    #[tokio::test(start_paused = true)]
    async fn ap16_pending_header_owns_no_payload_or_queue_credit() {
        let f = fixture();
        let o = f.open(1500).unwrap();
        let s = settings(1);
        let g = Gate::new(
            Options {
                mode: Mode::Bucket,
                rate: 1,
                burst: 1500,
                return_workers: 1,
                slow_bytes: 1024,
            },
            1,
        );
        let d = tokio::time::Instant::now() + Duration::from_secs(10);
        g.attempt(0, 1500, d, || Ok::<_, ()>(()))
            .await
            .unwrap()
            .unwrap();
        let (txs, mut rxs) = subscribers(1);
        let wire = Header {
            len: 8,
            ..Header::default()
        }
        .encode();
        let mut io = ScriptedIo::new(&wire, [Step::Data(1), Step::Data(20), Step::Stall]);
        let mut future = Box::pin(run_io(
            &mut io,
            o.into(),
            txs,
            vec![0; 64].into_boxed_slice(),
            s,
            0,
            g.clone(),
        ));
        let mut cx = std::task::Context::from_waker(Waker::noop());
        assert!(future.as_mut().poll(&mut cx).is_pending());
        assert_eq!(f.snapshot()["admitted"], 0);
        assert_eq!(rxs[0].capacity(), 2);
        assert_eq!(g.json()["pending"], 1);
        drop(future);
        assert_eq!(io.consumed, 20);
        assert_eq!(g.json()["pending"], 0);
        assert!(rxs[0].try_recv().is_err());
        drop(rxs);
        f.finish().unwrap();
    }
    #[tokio::test(start_paused = true)]
    async fn ap16_partial_payload_refunds_memory_but_not_committed_rate() {
        let f = fixture();
        let o = f.open(1500).unwrap();
        let s = settings(1);
        let g = Gate::new(s.admission.clone().unwrap(), 1);
        let (txs, rxs) = subscribers(1);
        let mut wire = Header {
            len: 8,
            ..Header::default()
        }
        .encode()
        .to_vec();
        wire.extend([7; 3]);
        let io = ScriptedIo::new(
            &wire,
            [Step::Data(1), Step::Data(20), Step::Data(3), Step::Zero],
        );
        let r = run_io(
            io,
            o.into(),
            txs,
            vec![0; 64].into_boxed_slice(),
            s,
            0,
            g.clone(),
        )
        .await
        .unwrap();
        conserved(&r, 1);
        assert_eq!(r.admission_study.unwrap()["payload_io_failures"], 1);
        assert_eq!(g.json()["committed_bytes"], 1500);
        assert_eq!(g.json()["rolled_back"], 0);
        drop(rxs);
        f.finish().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn ap16_unread_expiry_is_not_a_received_frame() {
        let f = fixture();
        let o = f.open(1500).unwrap();
        let s = settings(1);
        let g = Gate::new(
            Options {
                mode: Mode::Bucket,
                rate: 1,
                burst: 1500,
                return_workers: 1,
                slow_bytes: 1024,
            },
            1,
        );
        g.attempt(
            0,
            1500,
            tokio::time::Instant::now() + Duration::from_secs(1),
            || Ok::<_, ()>(()),
        )
        .await
        .unwrap()
        .unwrap();
        let (txs, rxs) = subscribers(1);
        let wire = Header {
            len: 8,
            ..Header::default()
        }
        .encode();
        let mut io = ScriptedIo::new(&wire, [Step::Data(1), Step::Data(20), Step::Stall]);
        let mut future = Box::pin(run_io(
            &mut io,
            o.into(),
            txs,
            vec![0; 64].into_boxed_slice(),
            s,
            0,
            g.clone(),
        ));
        let mut cx = std::task::Context::from_waker(Waker::noop());
        assert!(future.as_mut().poll(&mut cx).is_pending());
        tokio::time::advance(Duration::from_secs(1)).await;
        let r = future.await.unwrap();
        conserved(&r, 1);
        assert_eq!(r.received, 0);
        assert_eq!(r.admission_study.unwrap()["expired_headers"], 1);
        assert_eq!(io.consumed, 20);
        assert_eq!(g.json()["pending"], 0);
        drop(rxs);
        f.finish().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn ap16_partial_discard_does_not_commit_preflight_losses() {
        for full in [false, true] {
            let f = fixture();
            let o = f.open(0).unwrap();
            let s = settings(2);
            let g = Gate::new(s.admission.clone().unwrap(), 1);
            let (txs, mut rxs) = subscribers(2);
            drop(rxs.pop()); // Actual disconnect, even if the payload is incomplete.
            let mut wire = Header {
                len: 8,
                ..Header::default()
            }
            .encode()
            .to_vec();
            let n = if full { 8 } else { 3 };
            wire.extend(vec![7; n]);
            let io = ScriptedIo::new(
                &wire,
                [Step::Data(1), Step::Data(20), Step::Data(n), Step::Zero],
            );
            let r = run_io(
                io,
                o.into(),
                txs,
                vec![0; 64].into_boxed_slice(),
                s,
                0,
                g.clone(),
            )
            .await
            .unwrap();
            conserved(&r, 2);
            assert_eq!(r.received, u64::from(full));
            assert_eq!(r.discarded, u64::from(full));
            assert_eq!(r.disconnected, 1);
            assert_eq!(
                r.admission_study.unwrap()["core_errors"]["BudgetExceeded"],
                1
            );
            assert_eq!(g.json()["committed_bytes"], 0);
            drop(rxs);
            f.finish().unwrap();
        }
    }
}
