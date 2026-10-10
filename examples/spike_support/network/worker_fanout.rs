//! AP-07: one transferable family per stream/worker, explicit queue credits.
//! Safe adapter code; channel/semaphore costs are outside core allocation claims.
use super::super::supervisor::Identity;
use super::*;
use abps_reference::mosaic::{Branch, LocalBuf};
use tokio::io::AsyncRead;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio::task::JoinSet;

#[derive(Default)]
pub(super) struct Flow {
    forwarded: AtomicU64,
    cancelled: AtomicU64,
}
impl Flow {
    pub fn sample(&self) -> (u64, u64) {
        (
            self.forwarded.load(Ordering::Relaxed),
            self.cancelled.load(Ordering::Relaxed),
        )
    }
}
pub(super) struct CreditSubscriber {
    credits: Arc<Semaphore>,
    active: bool,
    pub cancel: Option<oneshot::Sender<()>>,
}
pub(super) struct Ingress {
    pub subscribers: Vec<CreditSubscriber>,
    sender: mpsc::Sender<Bundle>,
    generation: Identity,
    flow: Arc<Flow>,
}
pub(super) struct Job {
    pub outputs: Vec<(Socket, oneshot::Receiver<()>)>,
    credits: Vec<Arc<Semaphore>>,
    receiver: mpsc::Receiver<Bundle>,
    generation: Identity,
    queue: usize,
}
// Frame precedes credits on every failure path, including unopened mailboxes.
pub(super) struct Bundle {
    frame: Option<Branch>,
    credits: [Option<OwnedSemaphorePermit>; 20],
    header: Header,
    deadline: Option<tokio::time::Instant>,
    generation: Identity,
    flow: Arc<Flow>,
}
impl Drop for Bundle {
    fn drop(&mut self) {
        drop(self.frame.take());
        let mut cancelled = 0;
        for credit in &mut self.credits {
            if let Some(credit) = credit.take() {
                cancelled += 1;
                drop(credit);
            }
        }
        // Removed credits were forwarded or explicitly cancelled by forward().
        self.flow.cancelled.fetch_add(cancelled, Ordering::Relaxed);
    }
}
pub(super) struct LocalDelivery {
    // Rust drops fields in declaration order: payload first, credit second.
    message: Message<LocalBuf>,
    credit: OwnedSemaphorePermit,
}
impl writer::Delivery for LocalDelivery {
    type Frame = LocalBuf;
    fn into_batch(self, dst: &mut Option<Message<LocalBuf>>) {
        assert!(dst.is_none(), "writer batch slot already occupied");
        let Self { message, credit } = self;
        *dst = Some(message);
        drop(credit); // AP-07: bounded writer position now owns the frame
    }
}
struct CloseCredits(Arc<Semaphore>);
impl Drop for CloseCredits {
    fn drop(&mut self) {
        self.0.close();
    }
}

pub(super) fn prepare(fanout: usize, queue: usize, generation: Identity) -> (Ingress, Job) {
    assert!((1..=20).contains(&fanout) && (1..=1024).contains(&queue));
    let credits: Vec<_> = (0..fanout)
        .map(|_| Arc::new(Semaphore::new(queue)))
        .collect();
    let subscribers = credits
        .iter()
        .map(|c| CreditSubscriber {
            credits: c.clone(),
            active: true,
            cancel: None,
        })
        .collect();
    let (sender, receiver) = mpsc::channel(fanout * queue);
    let flow = Arc::new(Flow::default());
    (
        Ingress {
            subscribers,
            sender,
            generation,
            flow,
        },
        Job {
            outputs: Vec::with_capacity(fanout),
            credits,
            receiver,
            generation,
            queue,
        },
    )
}
fn preflight(
    group: &mut Ingress,
    skip: bool,
    stats: &mut IngressStats,
) -> ([Option<OwnedSemaphorePermit>; 20], u64, u64, u64) {
    let mut credits = std::array::from_fn(|_| None);
    let (mut skipped, mut rejected, mut absent) = (0, 0, 0);
    for (sub, out) in group.subscribers.iter_mut().zip(&mut credits) {
        if !sub.active {
            absent += 1;
            continue;
        }
        stats.queue_credit_attempts += 1;
        match sub.credits.clone().try_acquire_owned() {
            Ok(c) => {
                *out = Some(c);
                stats.queue_credit_acquired += 1;
            }
            Err(TryAcquireError::NoPermits) if skip => skipped += 1,
            Err(_) => {
                rejected += 1;
                stats.disconnected += 1;
                sub.active = false;
                if let Some(cancel) = sub.cancel.take() {
                    let _ = cancel.send(());
                }
            }
        }
    }
    (credits, skipped, rejected, absent)
}

pub(super) async fn ingress<S: AsyncRead + tokio::io::AsyncWrite + Unpin>(
    mut socket: S,
    origin: Origin,
    mut group: Ingress,
    mut scratch: Box<[u8]>,
    settings: Settings,
    stream: usize,
) -> Result<IngressStats> {
    let mut stats = IngressStats {
        stream,
        worker_flow: Some(group.flow.clone()),
        ..Default::default()
    };
    socket.write_all(&[1]).await?;
    loop {
        let header = match Header::read(&mut socket).await {
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
        // Declared before payload ownership, so cancellation refunds credits last.
        let (credits, skipped, rejected, absent) = preflight(&mut group, settings.skip, &mut stats);
        let recipients = credits.iter().flatten().count() as u64;
        if recipients == 0 {
            if let Err(e) = super::ingress::discard(&mut socket, header.len, &mut scratch).await {
                stats.stop = Some(format!("discard_payload:{e}"));
                break;
            }
            stats.received += 1;
            stats.discarded += 1;
            stats.no_receiver_discards += 1;
            stats.skipped += skipped;
            stats.rejected += rejected;
            stats.no_subscriber += absent;
            continue;
        }
        let mut lease = match acquire(&origin, header.len, &mut stats.admission_contended).await {
            Ok(b) => b,
            Err(e) if settings.skip && super::ingress::overload(e) => {
                if let Err(e) = super::ingress::discard(&mut socket, header.len, &mut scratch).await
                {
                    stats.stop = Some(format!("discard_payload:{e}"));
                    break;
                }
                stats.received += 1;
                stats.admission_discard(e, recipients);
                stats.skipped += skipped;
                stats.rejected += rejected;
                stats.no_subscriber += absent;
                continue;
            }
            Err(e) => {
                stats.stop = Some(format!("acquire:{e}"));
                break;
            }
        };
        if let Err(e) =
            super::ingress::fill(&mut socket, &mut lease, header.len, settings.read_buf).await
        {
            stats.stop = Some(format!("read_payload:{e}"));
            break;
        }
        stats.received += 1;
        stats.skipped += skipped;
        stats.rejected += rejected;
        stats.no_subscriber += absent;
        let super::super::backend::Lease::Mosaic(lease) = lease else {
            return Err("worker fanout requires mosaic lease".into());
        };
        let mut ready = [None];
        if let Err(e) = lease.freeze_branches_into(&mut ready) {
            stats.admission_discard(e.reason(), recipients);
            stats.stop = Some(format!("branch:{}", e.reason()));
            break;
        }
        let bundle = Bundle {
            frame: ready[0].take(),
            credits,
            header,
            deadline: settings
                .max_frame_age
                .map(|age| tokio::time::Instant::now() + age),
            generation: group.generation,
            flow: group.flow.clone(),
        };
        stats.accepted += 1;
        stats.dispatched += recipients;
        match group.sender.try_send(bundle) {
            Ok(()) => (),
            Err(mpsc::error::TrySendError::Closed(bundle)) => {
                drop(bundle);
                stats.stop = Some("mailbox_closed".into());
                break;
            }
            Err(mpsc::error::TrySendError::Full(bundle)) => {
                drop(bundle);
                return Err("AP-07: mailbox full despite reserved recipient credits".into());
            }
        }
    }
    Ok(stats)
}

fn forward(
    mut bundle: Bundle,
    generation: Identity,
    queues: &[mpsc::Sender<LocalDelivery>],
) -> std::io::Result<()> {
    if bundle.generation != generation {
        return Ok(());
    } // drops only old rights
    let last = bundle
        .credits
        .iter()
        .rposition(Option::is_some)
        .ok_or_else(|| std::io::Error::other("AP-07: empty recipient bundle"))?;
    let mut original = Some(
        bundle
            .frame
            .take()
            .expect("bundle owns one branch")
            .into_local(),
    );
    for (index, queue) in queues.iter().enumerate() {
        if bundle.credits[index].is_none() {
            continue;
        }
        let frame = if index == last {
            original.take().unwrap()
        } else {
            original
                .as_ref()
                .unwrap()
                .try_clone()
                .map_err(std::io::Error::other)?
        };
        let delivery = LocalDelivery {
            message: Message {
                header: bundle.header,
                frame,
                deadline: bundle.deadline,
            },
            credit: bundle.credits[index].take().unwrap(),
        };
        match queue.try_send(delivery) {
            Ok(()) => {
                bundle.flow.forwarded.fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Closed(delivery)) => {
                drop(delivery);
                bundle.flow.cancelled.fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Full(delivery)) => {
                drop(delivery);
                bundle.flow.cancelled.fetch_add(1, Ordering::Relaxed);
                return Err(std::io::Error::other(
                    "AP-07: local queue full despite reserved credit",
                ));
            }
        }
    }
    Ok(())
}
async fn dispatch(
    mut receiver: mpsc::Receiver<Bundle>,
    generation: Identity,
    queues: Vec<mpsc::Sender<LocalDelivery>>,
) -> std::io::Result<()> {
    let mut work = 0;
    while let Some(bundle) = receiver.recv().await {
        forward(bundle, generation, &queues)?;
        work += 1;
        if work == 8 {
            tokio::task::yield_now().await;
            work = 0;
        }
    }
    Ok(())
}
pub(super) async fn run(
    job: Job,
    counts: Arc<writer::WriterCounts>,
    batch: usize,
) -> std::io::Result<()> {
    let Job {
        outputs,
        credits,
        receiver,
        generation,
        queue,
    } = job;
    if outputs.len() != credits.len() {
        return Err(std::io::Error::other("incomplete worker group"));
    }
    let mut writers = JoinSet::new();
    let mut queues = Vec::with_capacity(outputs.len());
    for ((socket, cancel), credit) in outputs.into_iter().zip(credits) {
        let (sender, receiver) = mpsc::channel(queue);
        queues.push(sender);
        let counts = counts.clone();
        let guard = CloseCredits(credit);
        writers.spawn_local(async move {
            let _guard = guard;
            writer::run_io(into_async(socket)?, receiver, cancel, counts, batch).await
        });
    }
    let forwarding = dispatch(receiver, generation, queues);
    tokio::pin!(forwarding);
    let mut finished = false;
    while !finished || !writers.is_empty() {
        tokio::select! {
            result = &mut forwarding, if !finished => { result?; finished = true; },
            result = writers.join_next(), if !writers.is_empty() => {
                // Normal socket/deadline failures close only this subscriber.
                result.unwrap().map_err(std::io::Error::other)?.ok();
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::super::supervisor::Registry;
    use super::super::writer::Delivery;
    use super::*;

    fn identity() -> Identity {
        let registry = Registry::new(1, 1, 1, 0).unwrap();
        let permit = registry.reserve(&[1], false).unwrap().pop().unwrap();
        let identity = permit.identity();
        permit.prepare_failed();
        identity
    }
    fn fixture(fanout: usize) -> (Fixture, Origin) {
        let f = Fixture::prepared_branch_layout(
            "mosaic",
            vec![ClassConfig {
                capacity: 16,
                slots: 4,
            }],
            1,
            None,
            CapacityPolicy::Unreserved,
            false,
            (1, fanout),
        )
        .unwrap();
        let d = f.open(64).unwrap();
        (f, d)
    }
    fn bundle(group: &mut Ingress, d: &Origin, id: u64) -> Bundle {
        let (credits, skipped, rejected, absent) =
            preflight(group, true, &mut IngressStats::default());
        assert_eq!((skipped, rejected, absent), (0, 0, 0));
        let mut b = d.acquire(4).unwrap();
        b.put(b"abcd");
        let super::super::super::backend::Lease::Mosaic(b) = b else {
            unreachable!()
        };
        let mut out = [None];
        b.freeze_branches_into(&mut out).unwrap();
        Bundle {
            frame: out[0].take(),
            credits,
            header: Header { len: 4, id, due: 0 },
            deadline: None,
            generation: group.generation,
            flow: group.flow.clone(),
        }
    }
    fn done(f: Fixture, d: Origin) {
        assert_eq!(f.snapshot()["admitted"], 0);
        drop(d);
        assert!(f.shutdown(Duration::from_secs(1)).0);
    }

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum CreditStage {
        Ingress,
        Mailbox,
        Local,
        Batch,
    }
    // AP-07: token identities survive movement between queues. Batch ownership
    // is tracked but no longer spends a queue credit. No observed semaphore
    // value is used to predict the permitted transitions.
    struct CreditOracle {
        q: usize,
        fanout: usize,
        tokens: std::collections::BTreeMap<(usize, usize), CreditStage>,
    }
    impl CreditOracle {
        fn move_token(&mut self, key: (usize, usize), from: CreditStage, to: CreditStage) {
            assert_eq!(self.tokens.insert(key, to), Some(from));
        }
        fn remove(&mut self, key: (usize, usize), from: CreditStage) {
            assert_eq!(self.tokens.remove(&key), Some(from));
        }
        fn check(&self, credits: &[Arc<Semaphore>], f: &Fixture) {
            for (sub, credit) in credits.iter().enumerate() {
                let charged = self
                    .tokens
                    .iter()
                    .filter(|((s, _), stage)| *s == sub && **stage != CreditStage::Batch)
                    .count();
                let batch = self
                    .tokens
                    .iter()
                    .filter(|((s, _), stage)| *s == sub && **stage == CreditStage::Batch)
                    .count();
                assert!(charged <= self.q && batch <= 8, "AP-07 bounded resources");
                assert_eq!(
                    credit.available_permits(),
                    self.q - charged,
                    "AP-07 credit conservation"
                );
            }
            assert_eq!(credits.len(), self.fanout);
            let allocations: std::collections::BTreeSet<_> =
                self.tokens.keys().map(|(_, id)| id).collect();
            assert_eq!(
                f.snapshot()["admitted"],
                16 * allocations.len(),
                "BR/AP payload rights outlive batch transfer"
            );
        }
    }

    #[test]
    fn abps_queue_credit_conservation_at_every_cancellation_stage() {
        for q in [1, 2, 8] {
            for fanout in [1, 2, 20] {
                for cancel_at in 0..5 {
                    let f = Fixture::prepared_branch_layout(
                        "mosaic",
                        vec![ClassConfig {
                            capacity: 16,
                            slots: q + 1,
                        }],
                        1,
                        None,
                        CapacityPolicy::Unreserved,
                        false,
                        (1, fanout),
                    )
                    .unwrap();
                    let d = f.open(16 * (q + 1)).unwrap();
                    let (mut group, mut job) = prepare(fanout, q, identity());
                    let credits = job.credits.clone();
                    let mut model = CreditOracle {
                        q,
                        fanout,
                        tokens: std::collections::BTreeMap::new(),
                    };
                    let mut ingress = Vec::new();
                    for id in 0..q {
                        ingress.push(bundle(&mut group, &d, id as u64));
                        for sub in 0..fanout {
                            assert!(
                                model
                                    .tokens
                                    .insert((sub, id), CreditStage::Ingress)
                                    .is_none()
                            );
                        }
                        model.check(&credits, &f);
                    }
                    let (rejected, skipped, disconnected, absent) =
                        preflight(&mut group, true, &mut IngressStats::default());
                    assert!(rejected.iter().all(Option::is_none));
                    assert_eq!((skipped, disconnected, absent), (fanout as u64, 0, 0));
                    model.check(&credits, &f);
                    if cancel_at == 0 {
                        drop(ingress);
                        model.tokens.clear();
                        model.check(&credits, &f);
                        drop((group, job));
                        done(f, d);
                        continue;
                    }
                    for (id, b) in ingress.into_iter().enumerate() {
                        assert!(group.sender.try_send(b).is_ok());
                        for sub in 0..fanout {
                            model.move_token((sub, id), CreditStage::Ingress, CreditStage::Mailbox);
                        }
                        model.check(&credits, &f);
                    }
                    if cancel_at == 1 {
                        drop(job);
                        model.tokens.clear();
                        model.check(&credits, &f);
                        drop(group);
                        done(f, d);
                        continue;
                    }
                    let (senders, mut receivers): (Vec<_>, Vec<_>) =
                        (0..fanout).map(|_| mpsc::channel(q)).unzip();
                    for id in 0..q {
                        forward(job.receiver.try_recv().unwrap(), job.generation, &senders)
                            .unwrap();
                        for sub in 0..fanout {
                            model.move_token((sub, id), CreditStage::Mailbox, CreditStage::Local);
                        }
                        model.check(&credits, &f);
                    }
                    if cancel_at == 2 {
                        drop(receivers);
                        model.tokens.clear();
                        model.check(&credits, &f);
                        drop((senders, group, job));
                        done(f, d);
                        continue;
                    }
                    let mut deliveries = Vec::new();
                    for (sub, receiver) in receivers.iter_mut().enumerate() {
                        for id in 0..q {
                            let delivery = receiver.try_recv().unwrap();
                            assert_eq!(delivery.message.header.id, id as u64, "AP-07 FIFO");
                            deliveries.push((sub, id, delivery));
                            model.check(&credits, &f); // dequeue is not refund
                        }
                    }
                    if cancel_at == 3 {
                        for (sub, id, delivery) in deliveries {
                            drop(delivery);
                            model.remove((sub, id), CreditStage::Local);
                            model.check(&credits, &f);
                        }
                    } else {
                        let mut batches = Vec::new();
                        for (sub, id, delivery) in deliveries {
                            let mut batch = None;
                            delivery.into_batch(&mut batch);
                            model.move_token((sub, id), CreditStage::Local, CreditStage::Batch);
                            model.check(&credits, &f);
                            batches.push((sub, id, batch));
                        }
                        for (sub, id, batch) in batches {
                            drop(batch);
                            model.remove((sub, id), CreditStage::Batch);
                            model.check(&credits, &f);
                        }
                    }
                    drop((receivers, senders, group, job));
                    done(f, d);
                }
            }
        }
    }

    #[test]
    fn ap07_credit_covers_mailbox_local_queue_and_transfer_to_batch() {
        let (f, d) = fixture(2);
        let (mut group, mut job) = prepare(2, 1, identity());
        let credits = job.credits.clone();
        let b = bundle(&mut group, &d, 1);
        assert_eq!(
            credits.iter().map(|s| s.available_permits()).sum::<usize>(),
            0
        );
        assert!(group.sender.try_send(b).is_ok());
        let (held, skipped, rejected, absent) =
            preflight(&mut group, true, &mut IngressStats::default());
        assert!(held.iter().all(Option::is_none));
        assert_eq!((skipped, rejected, absent), (2, 0, 0));
        let (a, mut ar) = mpsc::channel(1);
        let (b, br) = mpsc::channel(1);
        forward(job.receiver.try_recv().unwrap(), job.generation, &[a, b]).unwrap();
        assert_eq!(group.flow.sample(), (2, 0));
        let message = ar.try_recv().unwrap();
        assert_eq!(credits[0].available_permits(), 0); // pop is not credit refund
        let mut batch = None;
        message.into_batch(&mut batch);
        assert_eq!(credits[0].available_permits(), 1);
        drop(br); // payload before queue credit; the other batch still pins backing
        assert_eq!(credits[1].available_permits(), 1);
        assert_eq!(f.snapshot()["admitted"], 16);
        drop(batch);
        done(f, d);
    }

    #[test]
    fn ap07_mailbox_cancel_and_closed_subscriber_release_exactly_their_rights() {
        let (f, d) = fixture(2);
        let (mut group, job) = prepare(2, 1, identity());
        let credits = job.credits.clone();
        let b = bundle(&mut group, &d, 0);
        assert!(group.sender.try_send(b).is_ok());
        drop(job); // unopened mailbox
        assert_eq!(group.flow.sample(), (0, 2));
        assert!(credits.iter().all(|s| s.available_permits() == 1));
        let b = bundle(&mut group, &d, 1);
        let (a, ar) = mpsc::channel(1);
        let (bq, br) = mpsc::channel(1);
        drop(ar); // closes between ingress preflight and worker forwarding
        forward(b, group.generation, &[a, bq]).unwrap();
        assert_eq!(group.flow.sample(), (1, 3));
        assert_eq!(credits[0].available_permits(), 1);
        drop(br);
        done(f, d);
    }

    #[test]
    fn ap07_stale_generation_drops_old_ownership_without_refunding_new_credits() {
        let registry = Registry::new(1, 1, 1, 0).unwrap();
        let old = registry.reserve(&[1], false).unwrap().pop().unwrap();
        let old_id = old.identity();
        old.prepare_failed();
        let new = registry.reserve(&[1], false).unwrap().pop().unwrap();
        assert_ne!(old_id, new.identity());
        let (mut group, job) = prepare(1, 1, old_id);
        let (mut next, next_job) = prepare(1, 1, new.identity());
        let (f, d) = fixture(1);
        let b = bundle(&mut group, &d, 0);
        let (hold, _, _, _) = preflight(&mut next, false, &mut IngressStats::default());
        let (queue, receiver) = mpsc::channel(1);
        forward(b, new.identity(), &[queue]).unwrap();
        assert_eq!(group.flow.sample(), (0, 1));
        assert_eq!(job.credits[0].available_permits(), 1);
        assert_eq!(next_job.credits[0].available_permits(), 0);
        drop((hold, receiver));
        new.prepare_failed();
        done(f, d);
    }

    #[test]
    fn ap07_skip_disconnect_and_impossible_full_have_distinct_outcomes() {
        let (f, d) = fixture(1);
        let (mut group, job) = prepare(1, 2, identity());
        let first = bundle(&mut group, &d, 0);
        let second = bundle(&mut group, &d, 1);
        let (_, skipped, rejected, _) = preflight(&mut group, true, &mut IngressStats::default());
        assert_eq!((skipped, rejected), (1, 0));
        let (cancel, stop) = oneshot::channel();
        group.subscribers[0].cancel = Some(cancel);
        let (_, skipped, rejected, _) = preflight(&mut group, false, &mut IngressStats::default());
        assert_eq!((skipped, rejected), (0, 1));
        assert_eq!(stop.blocking_recv(), Ok(()));
        let (_, _, _, absent) = preflight(&mut group, false, &mut IngressStats::default());
        assert_eq!(absent, 1);
        let (queue, receiver) = mpsc::channel(1); // deliberately violate configured Q=2
        forward(first, group.generation, std::slice::from_ref(&queue)).unwrap();
        assert!(forward(second, group.generation, &[queue]).is_err());
        assert_eq!(group.flow.sample(), (1, 1));
        drop(receiver);
        assert_eq!(job.credits[0].available_permits(), 2);
        done(f, d);
    }

    #[tokio::test(start_paused = true)]
    async fn ap07_fifo_partial_writes_and_original_deadline_survive_forwarding() {
        for expired in [false, true] {
            let (f, d) = fixture(1);
            let (mut group, job) = prepare(1, 3, identity());
            let (queue, receiver) = mpsc::channel(3);
            let deadline = tokio::time::Instant::now() + Duration::from_millis(100);
            let mut expected = Vec::new();
            for id in 0..3 {
                let mut b = bundle(&mut group, &d, id);
                b.deadline = Some(deadline);
                expected.extend_from_slice(&b.header.encode());
                expected.extend_from_slice(b"abcd");
                forward(b, group.generation, std::slice::from_ref(&queue)).unwrap();
            }
            drop(queue);
            if expired {
                tokio::time::advance(Duration::from_millis(101)).await;
            }
            let (output, mut input) = tokio::io::duplex(7); // partial header/payload writes
            let (_cancel, stop) = oneshot::channel();
            let counts = Arc::new(writer::WriterCounts::default());
            let (result, received) = tokio::join!(
                writer::run_io(output, receiver, stop, counts.clone(), 2),
                async {
                    let mut bytes = Vec::new();
                    input.read_to_end(&mut bytes).await.unwrap();
                    bytes
                }
            );
            if expired {
                assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
                assert!(received.is_empty());
                assert_eq!(counts.expired.load(Ordering::Relaxed), 1);
            } else {
                result.unwrap();
                assert_eq!(received, expected);
            }
            assert_eq!(job.credits[0].available_permits(), 3);
            done(f, d);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn ap07_cancel_after_admission_and_cancel_writer_release_ownership_before_credits() {
        let (f, d) = fixture(1);
        let (group, job) = prepare(1, 1, identity());
        let (socket, mut peer) = tokio::io::duplex(128);
        let settings = Settings::new(&Args(
            [
                ("backend".into(), "mosaic".into()),
                ("fanout-api".into(), "worker-branches".into()),
                ("fanout".into(), "1".into()),
            ]
            .into(),
        ))
        .unwrap();
        {
            let future = ingress(
                socket,
                d,
                group,
                vec![0; 16].into_boxed_slice(),
                settings,
                0,
            );
            tokio::pin!(future);
            let send_header = async {
                let mut ack = [0];
                peer.read_exact(&mut ack).await.unwrap();
                peer.write_all(
                    &Header {
                        len: 4,
                        id: 0,
                        due: 0,
                    }
                    .encode(),
                )
                .await
                .unwrap();
                tokio::task::yield_now().await;
            };
            tokio::select! { _ = &mut future => panic!("ingress completed early"), _ = send_header => {} }
            // Poll until it is waiting for the missing payload.
            tokio::select! { _ = &mut future => panic!("ingress completed early"), _ = tokio::task::yield_now() => {} }
            assert_eq!(f.snapshot()["admitted"], 16);
            assert_eq!(job.credits[0].available_permits(), 0);
        } // cancel after acquire; lease must release before reserved queue credit
        assert_eq!(job.credits[0].available_permits(), 1);
        assert_eq!(f.snapshot()["admitted"], 0);
        assert!(f.shutdown(Duration::from_secs(1)).0);

        let (f, d) = fixture(1);
        let (mut group, job) = prepare(1, 1, identity());
        let b = bundle(&mut group, &d, 0);
        let (q, rx) = mpsc::channel(1);
        forward(b, group.generation, &[q]).unwrap();
        let (output, _input) = tokio::io::duplex(1);
        let (cancel, stop) = oneshot::channel();
        let writing = writer::run_io(
            output,
            rx,
            stop,
            Arc::new(writer::WriterCounts::default()),
            1,
        );
        tokio::pin!(writing);
        tokio::select! { _ = &mut writing => panic!("writer completed before cancellation"), _ = tokio::task::yield_now() => {} }
        assert_eq!(job.credits[0].available_permits(), 1); // in bounded writer batch
        assert_eq!(f.snapshot()["admitted"], 16); // partial write still owns backing
        cancel.send(()).unwrap();
        writing.await.unwrap();
        assert_eq!(job.credits[0].available_permits(), 1);
        done(f, d);
    }
}

/// AP-09: allocation instrumentation, deliberately separate from timing and I/O.
pub(super) fn allocation_probe(
    global: &stats_alloc::StatsAlloc<std::alloc::System>,
    args: &Args,
) -> Result<Value> {
    use stats_alloc::Region;
    use writer::Delivery;
    let fanout = args.n("fanout", 20);
    let queue = args.n("queue", 8);
    let iterations = args.n("iterations", 1000);
    if !(1..=20).contains(&fanout) || !(1..=1024).contains(&queue) || iterations == 0 {
        return Err("invalid allocation probe dimensions".into());
    }
    let setup = Region::new(global);
    let registry = super::super::supervisor::Registry::new(1, 1, 1, 0)?;
    let permit = registry.reserve(&[1], false)?.pop().unwrap();
    let (mut group, mut job) = prepare(fanout, queue, permit.identity());
    let fixture = Fixture::prepared_branch_layout(
        "mosaic",
        vec![ClassConfig {
            capacity: 16,
            slots: 1,
        }],
        1,
        None,
        CapacityPolicy::Unreserved,
        false,
        (1, fanout),
    )?;
    let origin = fixture.open(16)?;
    let mut senders = Vec::with_capacity(fanout);
    let mut receivers = Vec::with_capacity(fanout);
    let mut controls = Vec::with_capacity(fanout + 1);
    let mut batches = Vec::with_capacity(fanout);
    for _ in 0..fanout {
        let (tx, rx) = mpsc::channel(queue);
        senders.push(tx);
        receivers.push(rx);
        batches.push(None);
    }
    for _ in 0..fanout + 1 {
        controls.push(mpsc::channel::<usize>(queue));
    }
    let setup = setup.change();
    let mut statistics = IngressStats::default();
    let mut cycle = || -> Result<()> {
        let (credits, skipped, rejected, _) = preflight(&mut group, false, &mut statistics);
        assert_eq!((skipped, rejected), (0, 0));
        let mut b = origin.acquire(4)?;
        b.put(b"ABPS");
        let super::super::backend::Lease::Mosaic(b) = b else {
            unreachable!()
        };
        let mut branch = [None];
        b.freeze_branches_into(&mut branch)
            .map_err(|e| e.reason())?;
        group
            .sender
            .try_send(Bundle {
                frame: branch[0].take(),
                credits,
                header: Header {
                    id: 0,
                    len: 4,
                    due: 0,
                },
                deadline: None,
                generation: group.generation,
                flow: group.flow.clone(),
            })
            .map_err(|_| "mailbox capacity")?;
        forward(job.receiver.try_recv()?, job.generation, &senders)?;
        for (receiver, batch) in receivers.iter_mut().zip(&mut batches) {
            receiver.try_recv()?.into_batch(batch);
            drop(std::hint::black_box(batch.take()));
        }
        Ok(())
    };
    for _ in 0..100 {
        cycle()?;
    }
    let region = Region::new(global);
    for _ in 0..iterations {
        cycle()?;
    }
    let dispatch = region.change();
    let region = Region::new(global);
    for _ in 0..iterations {
        let (credits, _, _, _) = preflight(&mut group, false, &mut statistics);
        drop(std::hint::black_box(credits));
    }
    let credits = region.change();
    for _ in 0..100 {
        for (tx, rx) in &mut controls {
            tx.try_send(1)?;
            std::hint::black_box(rx.try_recv()?);
        }
    }
    let region = Region::new(global);
    for _ in 0..iterations {
        for (tx, rx) in &mut controls {
            tx.try_send(1)?;
            std::hint::black_box(rx.try_recv()?);
        }
    }
    let channels = region.change();
    assert_eq!(fixture.snapshot()["admitted"], 0);
    drop((origin, group, job, senders, receivers, batches, controls));
    assert!(fixture.shutdown(Duration::from_secs(1)).0);
    permit.prepare_failed(); // registry was used only for an isolated identity
    let value = |s: stats_alloc::Stats| {
        json!({"allocations":s.allocations,"frees":s.deallocations,
        "reallocations":s.reallocations,"bytes_allocated":s.bytes_allocated})
    };
    Ok(
        json!({"fanout":fanout,"queue":queue,"iterations":iterations,"cold_setup":value(setup),
        "credit_only":value(credits),"fanout_dispatch":value(dispatch),"tokio_queue_control":value(channels),
        "drained":true,"scope":"synchronous prepared adapter dispatch; includes Tokio channels and semaphore credits; excludes tasks, sockets and runtime scheduling; core measured independently; no subtraction"}),
    )
}
