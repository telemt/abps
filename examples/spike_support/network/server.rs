//! SI-16: cancellable coordination; ownership is cleaned on the issuing thread.
use super::super::supervisor::{Permit, Registry};
use super::*;
use std::panic::{AssertUnwindSafe, catch_unwind};
use tokio::sync::watch;
use tokio::task::JoinSet;

#[derive(Clone)]
struct Progress {
    ready: Vec<bool>,
    quiet: Vec<bool>,
    ingress_done: usize,
    cancelled: bool,
}
#[derive(Clone)]
struct Cohort(watch::Sender<Progress>);
impl Cohort {
    fn new(n: usize) -> Self {
        Self(
            watch::channel(Progress {
                ready: vec![false; n],
                quiet: vec![false; n],
                ingress_done: 0,
                cancelled: false,
            })
            .0,
        )
    }
    fn cancel(&self) {
        self.0.send_modify(|p| p.cancelled = true);
    }
    async fn wait(
        &self,
        deadline: tokio::time::Instant,
        allow_cancelled: bool,
        done: impl Fn(&Progress) -> bool,
    ) -> Result<()> {
        let mut rx = self.0.subscribe();
        loop {
            {
                let p = rx.borrow_and_update();
                if !allow_cancelled && p.cancelled {
                    return Err("worker cohort cancelled".into());
                }
                if done(&p) {
                    return Ok(());
                }
            }
            tokio::time::timeout_at(deadline, rx.changed())
                .await
                .map_err(|_| "worker coordination timed out")??;
        }
    }
    async fn cancelled(&self) {
        let _ = self
            .wait(
                tokio::time::Instant::now() + Duration::from_secs(86400),
                true,
                |p| p.cancelled,
            )
            .await;
    }
}

struct Managed {
    preparing: bool,
    fixture: Option<Fixture>,
    permit: Option<Permit>,
}
impl Managed {
    fn finish(&mut self, timeout: Duration) -> (bool, Value) {
        if let Some(p) = &self.permit {
            p.draining();
        }
        if self.preparing && self.fixture.is_none() {
            drop(self.permit.take()); // interrupted prepare: no destruction proof
            return (false, Value::Null);
        }
        let result = self
            .fixture
            .take()
            .map(|f| f.shutdown(timeout))
            .unwrap_or((true, Value::Null));
        if let Some(p) = self.permit.take() {
            if result.0 {
                p.destroyed();
            } else {
                p.abandoned();
            }
        }
        result
    }
}
impl Drop for Managed {
    fn drop(&mut self) {
        self.finish(Duration::ZERO);
    }
}
async fn accept(
    listener: &tokio::net::TcpListener,
    stream: usize,
    subscriber: usize,
) -> Result<Socket> {
    let (mut socket, _) = listener.accept().await?;
    let mut handshake = [0; 8];
    socket.read_exact(&mut handshake).await?;
    if u32::from_be_bytes(handshake[..4].try_into().unwrap()) as usize != stream
        || u32::from_be_bytes(handshake[4..].try_into().unwrap()) as usize != subscriber
    {
        return Err("unexpected connection order".into());
    }
    Ok(socket.into_std()?)
}
#[cfg(test)]
#[derive(Clone, Copy, Default, PartialEq)]
enum Fault {
    #[default]
    None,
    Prepare,
    Ingress,
    Writer,
}
struct CloseOnExit<'a>(Option<&'a Fixture>);
impl Drop for CloseOnExit<'_> {
    fn drop(&mut self) {
        if let Some(f) = self.0 {
            f.close();
        }
    }
}
struct Worker {
    deferred_stats: Vec<IngressStats>,
    #[cfg(test)]
    fault: Fault,
    id: usize,
    listener: Option<Listener>,
    pool: Option<reference::Pool>,
    permit: Option<Permit>,
    args: Args,
    settings: Settings,
    plan: Arc<plan::PipelinePlan>,
    cohort: Cohort,
    setup_deadline: Instant,
    senders: Vec<std::sync::mpsc::SyncSender<SetupJob>>,
    receiver: std::sync::mpsc::Receiver<SetupJob>,
    domain_setup: Arc<std::sync::Mutex<()>>,
}
impl Worker {
    fn execute(&mut self, managed: &mut Managed) -> Result<Value> {
        let worker = self.id;
        pin_worker(&self.args, worker)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let local = tokio::task::LocalSet::new();
        if worker < self.settings.workers {
            let shared = self.args.get("roots", "separate") == "shared";
            let r = &self.plan.roots[if shared { 0 } else { worker }];
            #[cfg(test)]
            if self.fault == Fault::Prepare {
                if let Some(p) = managed.permit.take() {
                    p.prepare_failed();
                }
                return Err("injected prepare failure".into());
            }
            managed.preparing = true;
            let f = Fixture::prepared_branch_layout(
                self.args.get("backend", "berry"),
                r.classes.clone(),
                (0..self.settings.streams)
                    .filter(|s| s % self.settings.workers == worker)
                    .count(),
                self.pool.take(),
                self.settings.capacity_policy,
                self.settings.zero_payload,
                if self.settings.worker_branches {
                    (1, self.settings.fanout)
                } else if self.settings.local_branches {
                    (self.settings.fanout, isize::MAX as usize)
                } else {
                    (0, 0)
                },
            );
            match f {
                Ok(f) => {
                    managed.preparing = false;
                    managed.fixture = Some(f);
                    if let Some(p) = &managed.permit {
                        p.active();
                    }
                }
                Err(e) => {
                    managed.preparing = false;
                    if let Some(p) = managed.permit.take() {
                        p.prepare_failed();
                    }
                    return Err(e);
                }
            }
        }
        runtime.block_on(local.run_until(self.serve(
            managed.fixture.as_ref(),
            managed.permit.as_ref().map(Permit::identity),
        )))
    }
    async fn serve(
        &mut self,
        f: Option<&Fixture>,
        _generation: Option<super::super::supervisor::Identity>,
    ) -> Result<Value> {
        let worker = self.id;

        let setup = async {
            let mut sessions = Vec::new();
            let mut buffers = Vec::new();
            let mut local_jobs = Vec::new();
            let mut local_observers = Vec::new();
            if let (Some(listener), Some(fixture)) = (self.listener.take(), f) {
                listener.set_nonblocking(true)?;
                let listener = tokio::net::TcpListener::from_std(listener)?;
                for stream in
                    (0..self.settings.streams).filter(|s| s % self.settings.workers == worker)
                {
                    let socket = accept(&listener, stream, self.settings.fanout).await?;
                    let origin = {
                        let _guard = self
                            .domain_setup
                            .lock()
                            .map_err(|_| "domain setup panicked")?;
                        let d = self.plan.domain(stream);
                        if self.settings.capacity_policy == CapacityPolicy::Reserved {
                            fixture.open_reserved(d.limit, &d.grants)?
                        } else {
                            fixture.open(d.limit)?
                        }
                    };
                    let mut subscribers = Vec::with_capacity(self.settings.fanout);
                    #[cfg(feature = "local-branches-spike")]
                    let (mut worker_ingress, mut group) = if self.settings.worker_branches {
                        let (ingress, group) = worker_fanout::prepare(
                            self.settings.fanout,
                            self.settings.queue,
                            _generation.ok_or("worker fanout requires root identity")?,
                        );
                        (Some(ingress), Some(group))
                    } else {
                        (None, None)
                    };
                    let target = if self.settings.remote {
                        (worker + 1) % self.settings.runtime_workers()
                    } else {
                        worker
                    };
                    for subscriber in 0..self.settings.fanout {
                        let output = accept(&listener, stream, subscriber).await?;
                        let sr = socket2::SockRef::from(&output);
                        if let Some(n) = self.settings.send_buffer_bytes {
                            sr.set_send_buffer_size(n)?;
                        }
                        buffers.push(json!({"stream":stream,"subscriber":subscriber,"requested":self.settings.send_buffer_bytes,"actual":sr.send_buffer_size()?}));
                        let (cancel, stop) = oneshot::channel();
                        #[cfg(feature = "local-branches-spike")]
                        if let Some(group) = &mut group {
                            group.outputs.push((output, stop));
                            worker_ingress.as_mut().unwrap().subscribers[subscriber].cancel =
                                Some(cancel);
                            continue;
                        }
                        if self.settings.local_queue {
                            let (queue, receiver, observer) = local_queue::channel(
                                self.settings.queue,
                                self.settings.freshness_min_age,
                            );
                            local_jobs.push((output, receiver, stop));
                            local_observers.push((stream, subscriber, observer));
                            subscribers.push(Subscriber {
                                queue: local_queue::IngressSender::Local(queue),
                                active: true,
                                cancel: Some(cancel),
                            });
                            continue;
                        }
                        let (queue, receiver) = mpsc::channel(self.settings.queue);
                        self.senders[target]
                            .try_send(SetupJob::Writer(WriterJob {
                                socket: output,
                                queue: receiver,
                                cancel: stop,
                            }))
                            .map_err(|_| "writer setup capacity/receiver unavailable")?;
                        subscribers.push(Subscriber {
                            queue: queue.into(),
                            active: true,
                            cancel: Some(cancel),
                        });
                    }
                    #[cfg(feature = "local-branches-spike")]
                    if let Some(group) = group {
                        self.senders[target]
                            .try_send(SetupJob::Group(group))
                            .map_err(|_| "group setup capacity/receiver unavailable")?;
                    }
                    sessions.push(Session {
                        stream,
                        socket,
                        origin,
                        subscribers,
                        #[cfg(feature = "local-branches-spike")]
                        worker_fanout: worker_ingress,
                        scratch: vec![0; self.settings.max_payload().min(16384)].into_boxed_slice(),
                    });
                }
            }
            self.senders.clear();
            self.cohort.0.send_modify(|p| p.ready[worker] = true);
            self.cohort
                .wait(self.setup_deadline.into(), false, |p| {
                    p.ready.iter().all(|x| *x)
                })
                .await?;
            Ok::<_, crate::perf_support::AnyError>((sessions, buffers, local_jobs, local_observers))
        };
        let (sessions, buffers, local_jobs, local_observers) = tokio::select! {
            _ = self.cohort.cancelled() => return Err("setup cancelled".into()),
            r = tokio::time::timeout_at(self.setup_deadline.into(), setup) => r.map_err(|_|"accept/handshake setup timed out")??,
        };
        let jobs: Vec<_> = self.receiver.try_iter().collect();
        let written = Arc::new(writer::WriterCounts::default());
        let mut writers = JoinSet::new();
        let mut writer_errors = std::collections::BTreeMap::<String, usize>::new();
        let mut sources = JoinSet::new();
        for job in jobs {
            #[cfg(test)]
            if self.fault == Fault::Writer {
                writers.spawn_local(async move {
                    let _held = match job {
                        SetupJob::Writer(mut job) => job.queue.recv().await,
                        #[cfg(feature = "local-branches-spike")]
                        SetupJob::Group(_) => None,
                    };
                    panic!("injected writer panic");
                    #[allow(unreachable_code)]
                    Ok::<_, std::io::Error>(())
                });
                continue;
            }
            match job {
                SetupJob::Writer(job) => {
                    writers.spawn_local(writer::run(
                        job,
                        written.clone(),
                        self.settings.write_batch,
                    ));
                }
                #[cfg(feature = "local-branches-spike")]
                SetupJob::Group(job) => {
                    let cohort = self.cohort.clone();
                    let written = written.clone();
                    let batch = self.settings.write_batch;
                    writers.spawn_local(async move {
                        let result = worker_fanout::run(job, written, batch).await;
                        if result.is_err() {
                            cohort.cancel();
                        }
                        result
                    });
                }
            }
        }
        for (socket, queue, cancel) in local_jobs {
            writers.spawn_local(writer::run_io(
                into_async(socket)?,
                queue,
                cancel,
                written.clone(),
                self.settings.write_batch,
            ));
        }
        for session in sessions {
            #[cfg(test)]
            if self.fault == Fault::Ingress {
                sources.spawn_local(async move {
                    let _session = session;
                    panic!("injected ingress panic");
                    #[allow(unreachable_code)]
                    Ok::<_, crate::perf_support::AnyError>(IngressStats::default())
                });
                continue;
            }
            sources.spawn_local(ingress::run(session, self.settings.clone()));
        }
        let _close_before_tasks = CloseOnExit(f);
        let mut stats = Vec::new();
        let mut cancelled = 0;
        let load_deadline =
            tokio::time::Instant::now() + self.settings.seconds + self.settings.setup_timeout;
        let mut maintenance = tokio::time::interval(Duration::from_millis(10));
        maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        maintenance.tick().await;
        let mut drain_deadline = None;
        let mut progress = self.cohort.0.subscribe();
        loop {
            if self.cohort.0.borrow().cancelled {
                if let Some(f) = f {
                    f.close();
                }
                return Err("running cohort cancelled".into());
            }
            if self.cohort.0.borrow().ingress_done == self.settings.streams
                && drain_deadline.is_none()
            {
                if let Some(f) = f {
                    f.close();
                }
                drain_deadline = Some(tokio::time::Instant::now() + self.settings.drain_timeout);
            }
            if sources.is_empty() && writers.is_empty() && drain_deadline.is_some() {
                break;
            }
            let deadline = drain_deadline.unwrap_or(load_deadline);
            tokio::select! {
                biased;
                r = sources.join_next(), if !sources.is_empty() => {
                    let result = r.unwrap().map_err(|_|"ingress task panicked")??;
                    stats.push(result);
                    self.cohort.0.send_modify(|p|p.ingress_done+=1);
                },
                r = writers.join_next(), if !writers.is_empty() => {
                    // A residual delivery count cannot identify an I/O failure.
                    if let Err(error) = r.unwrap().map_err(|_|"writer task panicked")? {
                        *writer_errors.entry(format!("{:?}", error.kind())).or_default() += 1;
                    }
                },
                _ = maintenance.tick() => { if let Some(f) = f { f.maintenance(); } },
                r = progress.changed() => { r?; },
                _ = tokio::time::sleep_until(deadline) => {
                    if drain_deadline.is_none() { return Err("ingress/loadgen timed out".into()); }
                    if let Some(f) = f { f.close(); }
                    cancelled = writers.len(); writers.abort_all();
                    while writers.join_next().await.is_some() {}
                    break;
                }
            }
        }
        // AP-07: remote forwarding may still be running on a different worker.
        // Serialize these counters only after cohort quiescence in run().
        self.deferred_stats = stats;
        let local_queues: Vec<_> = local_observers.iter().map(|(stream, subscriber, observer)|
            json!({"stream":stream,"subscriber":subscriber,"metrics":observer.json()})).collect();
        Ok(
            json!({"worker":worker,"streams":[],"writer_completed":written.completed.load(Ordering::Relaxed),
                    "write_api_calls":written.write_calls.load(Ordering::Relaxed),"expired_writers":written.expired.load(Ordering::Relaxed),
                    "cancelled_writers":cancelled,"writer_io_errors":writer_errors,"send_buffers":buffers,"local_queues":local_queues}),
        )
    }

    fn run(mut self) -> Value {
        let mut managed = Managed {
            preparing: false,
            fixture: None,
            permit: self.permit.take(),
        };
        let worker = self.id;
        let cohort = self.cohort.clone();
        // Managed survives unwind. The runtime/LocalSet and every owned job are
        // dropped before quiescence is announced or an arena is destroyed.
        let result = catch_unwind(AssertUnwindSafe(|| self.execute(&mut managed)))
            .unwrap_or_else(|_| Err("server worker panicked".into()));
        if result.is_err() {
            cohort.cancel();
        }
        if let Some(f) = &managed.fixture {
            f.close();
        }
        // Drop setup queues even on a partial handshake, before quiet publication.
        self.senders.clear();
        drop(self.receiver);
        drop(self.pool);
        drop(self.listener);
        cohort.0.send_modify(|p| p.quiet[worker] = true);
        let deadline = Instant::now() + self.settings.drain_timeout;
        // Cold bounded coordination. This is not a participant barrier: errors
        // still publish quiet, and an absent/stopped peer expires the deadline.
        while !cohort.0.borrow().quiet.iter().all(|x| *x) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        let (destroyed, snapshot) =
            managed.finish(deadline.saturating_duration_since(Instant::now()));
        let mut value = result.unwrap_or_else(|e|json!({"worker":worker,"error":e.to_string(),"streams":[],"loss_accounting":"client undelivered; partial ingress statistics unavailable"}));
        value["streams"] = json!(
            self.deferred_stats
                .iter()
                .map(IngressStats::json)
                .collect::<Vec<_>>()
        );
        value["flow_complete"] = json!(cohort.0.borrow().quiet.iter().all(|x| *x));
        value["destroyed"] = json!(destroyed);
        value["drained"] = snapshot;
        value
    }
}

pub fn run(args: &Args) -> Result<Value> {
    let settings = Settings::new(args)?;
    let shared = args.get("roots", "separate") == "shared";
    let plan = Arc::new(plan::PipelinePlan::new(
        &settings,
        shared,
        args.get("backend", "berry"),
    )?);
    let reservations: Vec<_> = plan.roots.iter().map(|r| r.reservation_bytes).collect();
    let initial = reservations
        .iter()
        .try_fold(0usize, |n, &r| n.checked_add(r))
        .ok_or("root total overflow")?;
    let registry = Registry::new(
        args.n("supervisor-roots", reservations.len()),
        args.n("supervisor-bytes", initial),
        reservations.len(),
        args.n("replacement-attempts", 1),
    )?;
    // Keep the ledger for process lifetime, including any detached worker. Only
    // copied snapshots escape; a new generation always uses this same registry.
    static PROCESS: std::sync::OnceLock<Registry> = std::sync::OnceLock::new();
    if PROCESS.set(registry.clone()).is_err() {
        return Err("one TCP supervisor per process".into());
    }
    let mut generations = Vec::new();
    loop {
        let permits = match registry.reserve(&reservations, !generations.is_empty()) {
            Ok(p) => p,
            Err(e) if !generations.is_empty() => {
                return Ok(
                    json!({"role":"server","failed":true,"replacement_rejected":e.to_string(),"generations":generations,"supervisor":registry.snapshot()}),
                );
            }
            Err(e) => return Err(e),
        };
        let ids: Vec<_> = permits.iter().map(Permit::identity).collect();
        let listeners = match (0..settings.workers)
            .map(|_| Listener::bind("127.0.0.1:0"))
            .collect::<std::io::Result<Vec<_>>>()
        {
            Ok(listeners) => listeners,
            Err(e) => {
                for p in permits {
                    p.prepare_failed();
                }
                return Err(e.into());
            }
        };
        let ports: Vec<_> = listeners
            .iter()
            .map(|l| l.local_addr().map(|a| a.port()))
            .collect::<std::io::Result<_>>()?;
        let mut permits = permits.into_iter();
        let mut shared_managed = Managed {
            preparing: false,
            fixture: None,
            permit: if shared { permits.next() } else { None },
        };
        if shared {
            shared_managed.preparing = true;
            let f = Fixture::with_policy(
                "abps",
                plan.roots[0].classes.clone(),
                settings.streams,
                None,
                settings.capacity_policy,
            );
            match f {
                Ok(f) => {
                    shared_managed.preparing = false;
                    shared_managed.fixture = Some(f);
                    shared_managed.permit.as_ref().unwrap().active();
                }
                Err(e) => {
                    shared_managed.preparing = false;
                    shared_managed.permit.take().unwrap().prepare_failed();
                    return Err(e);
                }
            }
        }
        let workers = settings.runtime_workers();
        let cohort = Cohort::new(workers);
        let mut senders = Vec::new();
        let mut receivers = Vec::new();
        for _ in 0..workers {
            let (tx, rx) = std::sync::mpsc::sync_channel(settings.streams * settings.fanout);
            senders.push(tx);
            receivers.push(rx);
        }
        let mut listeners = listeners.into_iter();
        let mut tasks = Vec::new();
        let (done, results) = std::sync::mpsc::channel();
        let deadline = Instant::now() + settings.setup_timeout;
        let domain_setup = Arc::new(std::sync::Mutex::new(()));
        // Resolve reference controls before workers can mutate its mutex model.
        // This setup must not race pool cloning against domain opening.
        let pools: Vec<_> = (0..workers)
            .map(|id| match &shared_managed.fixture {
                Some(Fixture::Reference { pool, .. }) if id < settings.workers => {
                    pool.try_clone().map(Some)
                }
                _ => Ok(None),
            })
            .collect::<std::result::Result<_, Error>>()?;
        for ((id, receiver), pool) in receivers.into_iter().enumerate().zip(pools) {
            let w = Worker {
                deferred_stats: Vec::new(),
                #[cfg(test)]
                fault: Fault::None,
                id,
                listener: listeners.next(),
                pool,
                permit: if shared { None } else { permits.next() },
                args: args.clone(),
                settings: settings.clone(),
                plan: plan.clone(),
                cohort: cohort.clone(),
                setup_deadline: deadline,
                senders: senders.clone(),
                receiver,
                domain_setup: domain_setup.clone(),
            };
            let tx = done.clone();
            tasks.push(Some(std::thread::spawn(move || {
                let v = w.run();
                let _ = tx.send((id, v));
            })));
        }
        drop(done);
        drop(senders);
        println!(
            "{}",
            json!({"ready":true,"ports":ports,"pid":std::process::id(),"generation":generations.len(),"new_work_only":true})
        );
        std::io::stdout().flush()?;
        let mut values = Vec::new();
        let mut end =
            deadline + settings.seconds + settings.setup_timeout + settings.drain_timeout * 2;
        let mut cancelled = false;
        while values.len() < workers && Instant::now() < end {
            if cohort.0.borrow().cancelled && !cancelled {
                cancelled = true;
                end = Instant::now() + settings.drain_timeout * 2;
            }
            match results.recv_timeout(
                Duration::from_millis(10).min(end.saturating_duration_since(Instant::now())),
            ) {
                Ok((id, value)) => {
                    values.push(value);
                    if let Some(t) = tasks[id].take()
                        && t.is_finished()
                    {
                        t.join().map_err(|_| "completion sender panicked")?;
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                Err(_) => break,
            }
        }
        cohort.cancel();
        let mut unconfirmed = 0;
        for (id, t) in tasks.into_iter().enumerate() {
            if t.is_some() {
                unconfirmed += 1;
                if let Some(&identity) = ids.get(if shared { 0 } else { id }) {
                    registry.unconfirmed(identity);
                }
                drop(t);
            }
        }
        let (shared_destroyed, shared_drained) = shared_managed.finish(Duration::ZERO);
        let undrained =
            unconfirmed != 0 || !shared_destroyed || values.iter().any(|v| v["destroyed"] != true);
        let failed = undrained || values.iter().any(|v| v.get("error").is_some());
        let value = json!({"role":"server","failed":failed,"pipeline_plan":plan.json(),"workers":values,"shared_drained":shared_drained,
            "unconfirmed_workers":unconfirmed,"runtime_workers":workers,"roots":reservations.len(),"class_slots":settings.class_slots,"fanout":settings.fanout,"streams":settings.streams,
            "supervisor":registry.snapshot()});
        if !undrained {
            let mut final_value = value;
            final_value["prior_generations"] = json!(generations);
            return Ok(final_value);
        }
        generations.push(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn interrupted_prepare_is_unconfirmed_not_destroyed() {
        let registry = Registry::new(1, 100, 1, 1).unwrap();
        let mut managed = Managed {
            preparing: true,
            fixture: None,
            permit: registry.reserve(&[100], false).unwrap().pop(),
        };
        assert!(!managed.finish(Duration::ZERO).0);
        assert_eq!(registry.snapshot()["charged_bytes"], 100);
        assert_eq!(registry.snapshot()["records"][0]["phase"], "Unconfirmed");
    }
    #[tokio::test]
    async fn cancelled_setup_and_missing_quiet_participants_never_barrier() {
        let c = Cohort::new(2);
        c.0.send_modify(|p| p.ready[0] = true);
        c.cancel();
        assert!(
            c.wait(
                tokio::time::Instant::now() + Duration::from_secs(1),
                false,
                |p| p.ready.iter().all(|x| *x)
            )
            .await
            .is_err()
        );
        assert!(
            c.wait(
                tokio::time::Instant::now() + Duration::from_millis(1),
                true,
                |p| p.quiet.iter().all(|x| *x)
            )
            .await
            .is_err()
        );
    }
}

#[cfg(test)]
mod worker_regressions {
    use super::*;
    fn exercise(fault: Fault, bad_second: bool, silent: bool) {
        exercise_mode(fault, bad_second, silent, false);
    }
    fn exercise_mode(fault: Fault, bad_second: bool, silent: bool, worker_branches: bool) {
        let backend = if cfg!(feature = "mosaic-spike") {
            "mosaic"
        } else {
            "berry"
        };
        let args = Args(
            [
                ("backend", backend),
                ("threads", "2"),
                ("streams", "2"),
                ("fanout", "1"),
                ("return", "remote"),
                (
                    "fanout-api",
                    if worker_branches {
                        "worker-branches"
                    } else {
                        "scalar"
                    },
                ),
                ("setup-timeout-ms", "300"),
                ("drain-timeout-ms", "100"),
                ("seconds", "0.01"),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        );
        let settings = Settings::new(&args).unwrap();
        let plan = Arc::new(plan::PipelinePlan::new(&settings, false, backend).unwrap());
        let sizes: Vec<_> = plan.roots.iter().map(|r| r.reservation_bytes).collect();
        let registry = Registry::new(2, sizes.iter().sum(), 2, 1).unwrap();
        let mut permits = registry.reserve(&sizes, false).unwrap().into_iter();
        let cohort = Cohort::new(2);
        let mut txs = Vec::new();
        let mut rxs = Vec::new();
        for _ in 0..2 {
            let (tx, rx) = std::sync::mpsc::sync_channel(2);
            txs.push(tx);
            rxs.push(rx);
        }
        let setup = Arc::new(std::sync::Mutex::new(()));
        let (tx, rx) = std::sync::mpsc::channel();
        let mut ports = Vec::new();
        let mut tasks = Vec::new();
        for (id, receiver) in rxs.into_iter().enumerate() {
            let listener = Listener::bind("127.0.0.1:0").unwrap();
            ports.push(listener.local_addr().unwrap().port());
            let worker = Worker {
                deferred_stats: Vec::new(),
                fault: if id == 1 { fault } else { Fault::None },
                id,
                listener: Some(listener),
                pool: None,
                permit: permits.next(),
                args: args.clone(),
                settings: settings.clone(),
                plan: plan.clone(),
                cohort: cohort.clone(),
                setup_deadline: Instant::now() + settings.setup_timeout,
                senders: txs.clone(),
                receiver,
                domain_setup: setup.clone(),
            };
            let tx = tx.clone();
            tasks.push(std::thread::spawn(move || tx.send(worker.run()).unwrap()));
        }
        drop(txs);
        drop(tx);
        let mut sockets = Vec::new();
        if fault != Fault::Prepare {
            for (stream, &port) in ports.iter().enumerate() {
                for subscriber in [1u32, 0] {
                    let Ok(mut socket) = Socket::connect((std::net::Ipv4Addr::LOCALHOST, port))
                    else {
                        continue;
                    };
                    socket
                        .set_write_timeout(Some(Duration::from_millis(100)))
                        .unwrap();
                    if !(silent && stream == 1) {
                        let id = if bad_second && stream == 1 {
                            99
                        } else {
                            stream as u32
                        };
                        let _ = socket.write_all(&id.to_be_bytes());
                        let _ = socket.write_all(&subscriber.to_be_bytes());
                        if subscriber == 1 && !bad_second {
                            let _ = socket.write_all(
                                &Header {
                                    len: 1,
                                    id: stream as u64,
                                    due: 0,
                                }
                                .encode(),
                            );
                            let _ = socket.write_all(&[42]);
                            let _ = socket.shutdown(std::net::Shutdown::Write);
                        }
                    }
                    sockets.push(socket);
                    if stream == 1 && (bad_second || silent) {
                        break;
                    }
                }
            }
        }
        let mut errors = 0;
        for _ in 0..2 {
            let value = rx
                .recv_timeout(Duration::from_secs(3))
                .expect("worker stuck after peer failure");
            errors += usize::from(value.get("error").is_some());
            if worker_branches && fault == Fault::None && value.get("error").is_none() {
                assert_eq!(value["flow_complete"], true);
                for stream in value["streams"].as_array().unwrap() {
                    assert_eq!(stream["dispatched_recipient_rights"], 1);
                    assert_eq!(stream["locally_enqueued"], 1);
                    assert_eq!(stream["cancelled_before_forward"], 0);
                }
            }
            assert_eq!(value["destroyed"], true, "{value}");
            if !value["drained"].is_null() {
                assert_eq!(value["drained"]["admitted"], 0);
            }
        }
        assert_eq!(registry.snapshot()["charged_bytes"], 0);
        assert_eq!(registry.snapshot()["abandoned_bytes"], 0);
        if fault != Fault::None || bad_second || silent {
            assert!(errors > 0);
        } else {
            assert_eq!(errors, 0);
        }
        drop(sockets);
        for t in tasks {
            if t.is_finished() {
                t.join().unwrap();
            }
        }
    }
    #[test]
    fn invalid_second_worker_handshake_cancels_ready_peer() {
        exercise(Fault::None, true, false);
    }
    #[test]
    fn prepare_failure_cancels_peers_and_refunds_all_roots() {
        exercise(Fault::Prepare, false, false);
    }
    #[test]
    fn ingress_panic_cancels_peers_and_refunds_all_roots() {
        exercise(Fault::Ingress, false, false);
    }
    #[test]
    fn writer_panic_cancels_peers_and_refunds_all_roots() {
        exercise(Fault::Writer, false, false);
    }
    #[test]
    fn silent_handshake_has_an_overall_deadline() {
        exercise(Fault::None, false, true);
    }
    #[cfg(feature = "local-branches-spike")]
    #[test]
    fn ap07_remote_flow_is_sampled_after_forwarding_and_shutdown() {
        exercise_mode(Fault::None, false, false, true);
        exercise_mode(Fault::Ingress, false, false, true);
    }
    #[test]
    fn normal_remote_return_shutdown_destroys_all_roots() {
        exercise(Fault::None, false, false);
    }
}
