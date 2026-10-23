//! AP-13: all allocation/teardown authority stays on the issuer. Buffers remain
//! ordinary Mosaic handles; their final return has no adapter work.
#![forbid(unsafe_code)]
use super::{controller::*, *};
use abps_reference::mosaic;
use std::{cell::RefCell, rc::Rc};
const ROOTS: usize = 32;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Free,
    Preparing,
    Active,
    Standby,
    Draining,
    Abandoned,
    Unconfirmed,
}
struct Root {
    generation: u64,
    phase: Phase,
    owner: Option<mosaic::StorageOwner>,
    pool: Option<mosaic::Pool>,
    domains: Box<[Option<(mosaic::DomainOwner, mosaic::Domain)>]>,
}
#[derive(Clone, Copy)]
struct Sample {
    ms: u64,
    reserved: usize,
    admitted: usize,
    soft: usize,
    roots: usize,
    rejected: u64,
    debt: usize,
    class_pressure: u64,
    total_pressure: bool,
}
pub fn trace_bytes(n: usize) -> usize {
    n * std::mem::size_of::<Sample>()
}
pub fn external_bytes(
    streams: usize,
    classes: usize,
    root_records: usize,
    shared: bool,
) -> Result<usize> {
    // Includes prepared domain-control slots, retained config vectors, controller,
    // Rc header and the fixed trace descriptor (trace payload has separate budget).
    let records = root_records
        .checked_mul(
            std::mem::size_of::<Root>()
                + streams * std::mem::size_of::<Option<(mosaic::DomainOwner, mosaic::Domain)>>(),
        )
        .ok_or("external overflow")?;
    Ok(std::mem::size_of::<Bank>()
        + if shared {
            std::mem::size_of::<super::shared::Shared>()
        } else {
            0
        }
        + records
        + 2 * std::mem::size_of::<Plan>()
        + (streams + 16) * (std::mem::size_of::<Options>() + 8)
        + (streams + 16) * streams * std::mem::size_of::<usize>()
        + classes
            * (3 * std::mem::size_of::<ClassConfig>() + std::mem::size_of::<ClassReservation>())
        + 4 * std::mem::size_of::<usize>())
}
pub struct Bank {
    plan: Plan,
    roots: Box<[Root]>,
    policy: Controller,
    shared: Option<Box<super::shared::Shared>>,
    next_stream: usize,
    cursor: usize,
    start: Instant,
    tick_at: Instant,
    changed: Instant,
    deficit: Option<Instant>,
    calm: Option<Instant>,
    charged: usize,
    peak: usize,
    area: u128,
    area_at: Instant,
    added: usize,
    prior_admitted: usize,
    pub attempts: u64,
    grown: u64,
    shrunk: u64,
    prepare_ns: u128,
    prepare_max_ns: u128,
    destroy_ns: u128,
    physical: u64,
    progress: u64,
    refused_headroom: u64,
    refused_records: u64,
    refused_attempts: u64,
    accepted: [u64; STREAMS],
    policy_rejected: [u64; STREAMS],
    core_rejected: [u64; STREAMS],
    logical_rejected: [u64; STREAMS],
    group_rejected: [u64; STREAMS],
    trace: Vec<Sample>,
    trace_overflow: u64,
    maintenance_cursor: usize,
    observed_admitted_at_abandon: usize,
    finished: bool,
    #[cfg(test)]
    fault: Option<(u8, bool)>,
    #[cfg(test)]
    admission_fault: Option<(u8, bool)>,
}
pub struct Handle {
    bank: Rc<RefCell<Bank>>,
    stream: usize,
}
impl Drop for Handle {
    fn drop(&mut self) {
        // Only stops policy admission. Old core controls stay until their root
        // drains; they are not lifetime claims inferred from queue observation.
        self.bank.borrow_mut().policy.active[self.stream] = false;
    }
}
impl Handle {
    pub fn offer(&self, size: usize) {
        let mut b = self.bank.borrow_mut();
        if let Some(charge) = b.charge(size) {
            if let Some(shared) = &mut b.shared {
                shared.offered[self.stream] =
                    shared.offered[self.stream].saturating_add(charge as u64);
            }
            if !b.plan.options.eligible_demand {
                b.policy.offer(self.stream, charge);
            }
        }
    }
    pub fn acquire(&self, size: usize) -> std::result::Result<mosaic::BufMut, Failure> {
        self.bank.borrow_mut().acquire(self.stream, size)
    }
}
impl Bank {
    pub fn prepare(plan: Plan) -> Result<Rc<RefCell<Self>>> {
        let now = Instant::now();
        let soft = plan.options.soft.unwrap_or_else(|| {
            plan.config
                .classes
                .iter()
                .map(|c| c.capacity * c.slots)
                .sum::<usize>()
                * plan.initial
        });
        let policy = Controller::new(
            &plan.hard,
            &plan.options.groups,
            plan.options.ranks,
            soft,
            plan.options.mode,
            plan.options.priority,
            plan.options.correlated,
            plan.options.horizon_ms,
        );
        let records = if plan.options.elasticity == "off" {
            1
        } else {
            ROOTS
        };
        let roots = (0..records)
            .map(|_| Root {
                generation: 0,
                phase: Phase::Free,
                owner: None,
                pool: None,
                domains: (0..plan.hard.len()).map(|_| None).collect(),
            })
            .collect();
        let trace = Vec::with_capacity(plan.options.trace);
        let shared = (plan.options.mode == Mode::Shared)
            .then(|| Box::new(super::shared::Shared::new(plan.options.pressure_ms)));
        let mut bank = Self {
            plan,
            roots,
            policy,
            shared,
            next_stream: 0,
            cursor: 0,
            start: now,
            tick_at: now,
            changed: now,
            deficit: None,
            calm: None,
            charged: 0,
            peak: 0,
            area: 0,
            area_at: now,
            added: 0,
            prior_admitted: 0,
            attempts: 0,
            grown: 0,
            shrunk: 0,
            prepare_ns: 0,
            prepare_max_ns: 0,
            destroy_ns: 0,
            physical: 0,
            progress: 0,
            refused_headroom: 0,
            refused_records: 0,
            refused_attempts: 0,
            accepted: [0; STREAMS],
            policy_rejected: [0; STREAMS],
            core_rejected: [0; STREAMS],
            logical_rejected: [0; STREAMS],
            group_rejected: [0; STREAMS],
            trace,
            trace_overflow: 0,
            maintenance_cursor: 0,
            observed_admitted_at_abandon: 0,
            finished: false,
            #[cfg(test)]
            fault: None,
            #[cfg(test)]
            admission_fault: None,
        };
        for _ in 0..bank.plan.initial {
            bank.create(Phase::Active)?;
        }
        for _ in 0..bank.plan.standby {
            bank.create(Phase::Standby)?;
        }
        Ok(Rc::new(RefCell::new(bank)))
    }
    pub fn open(bank: &Rc<RefCell<Self>>) -> Result<Handle> {
        let mut b = bank.borrow_mut();
        if b.finished {
            return Err("memory bank closed".into());
        }
        let stream = b.next_stream;
        if stream >= b.policy.n {
            return Err("too many memory streams".into());
        }
        b.next_stream += 1;
        Ok(Handle {
            bank: bank.clone(),
            stream,
        })
    }
    fn integrate(&mut self, now: Instant) {
        let now = now.max(self.area_at);
        self.area = self.area.saturating_add(
            self.charged as u128 * now.saturating_duration_since(self.area_at).as_micros(),
        );
        self.area_at = now;
    }
    fn create(&mut self, phase: Phase) -> Result<()> {
        self.create_at(phase, Instant::now())
    }
    fn create_at(&mut self, phase: Phase, now: Instant) -> Result<()> {
        if self
            .charged
            .checked_add(self.plan.bytes)
            .is_none_or(|v| v > self.plan.ceiling)
        {
            self.refused_headroom += 1;
            return Err("elastic headroom exhausted".into());
        }
        let index = self
            .roots
            .iter()
            .position(|r| r.phase == Phase::Free && r.generation < u64::MAX)
            .ok_or_else(|| {
                self.refused_records += 1;
                "elastic root records exhausted"
            })?;
        self.integrate(now);
        self.charged += self.plan.bytes;
        self.peak = self.peak.max(self.charged);
        let r = &mut self.roots[index];
        r.generation += 1;
        r.phase = Phase::Preparing;
        let begin = Instant::now();
        #[cfg(test)]
        let fault = self.fault.take();
        let result = (|| -> Result<()> {
            #[cfg(test)]
            inject(fault, 0)?;
            r.owner = Some(mosaic::StorageOwner::prepare(&self.plan.config)?);
            #[cfg(test)]
            inject(fault, 1)?;
            r.pool = Some(r.owner.as_ref().unwrap().activate()?);
            // Cold-only grants scratch. Physical grants never cross roots.
            let grants: Vec<_> = self
                .plan
                .config
                .classes
                .iter()
                .filter(|c| c.slots >= self.policy.n)
                .map(|c| ClassReservation {
                    capacity: c.capacity,
                    slots: c.slots / self.policy.n,
                })
                .collect();
            for i in 0..self.policy.n {
                let pool = r.pool.as_ref().unwrap();
                let pair = if self.plan.config.capacity_policy == CapacityPolicy::Reserved {
                    pool.try_open_reserved_domain(self.policy.hard[i], &grants)?
                } else {
                    pool.try_open_domain(self.policy.hard[i])?
                };
                r.domains[i] = Some(pair);
                #[cfg(test)]
                inject(fault, 2)?;
            }
            #[cfg(test)]
            inject(fault, 3)?;
            r.phase = phase;
            Ok(())
        })();
        let elapsed = begin.elapsed().as_nanos();
        self.prepare_ns += elapsed;
        self.prepare_max_ns = self.prepare_max_ns.max(elapsed);
        if result.is_err() {
            self.retire_at(index, now);
        }
        result
    }
    fn charge(&self, size: usize) -> Option<usize> {
        self.plan
            .config
            .classes
            .iter()
            .find(|c| c.capacity >= size)
            .map(|c| c.capacity)
    }
    pub(super) fn clock_origin(&self) -> Instant {
        self.start
    }
    pub(super) fn observed_admitted(&self) -> [usize; STREAMS] {
        self.admitted()
    }
    fn admitted(&self) -> [usize; STREAMS] {
        let mut out = [0usize; STREAMS];
        for r in self.roots.iter() {
            for (i, pair) in r.domains.iter().enumerate() {
                if let Some((_, d)) = pair {
                    out[i] = out[i].saturating_add(d.metrics().admitted_bytes);
                }
            }
        }
        out
    }
    fn acquire(
        &mut self,
        stream: usize,
        size: usize,
    ) -> std::result::Result<mosaic::BufMut, Failure> {
        let charge = self.charge(size).ok_or(Failure::Core(Error::TooLarge))?;
        if self.shared.is_some() {
            return self.acquire_shared(stream, size, charge);
        }
        let admitted = self.admitted(); // No await/callback through acquire below.
        if let Err(reason) = self.policy.admission(stream, charge, &admitted) {
            self.policy_rejected[stream] += 1;
            if reason == "LogicalHardLimit" {
                self.logical_rejected[stream] += 1;
            }
            if reason == "SoftGroupBudget" {
                self.group_rejected[stream] += 1;
            }
            return Err(Failure::Policy(reason));
        }
        // One bounded routing search, then exactly one core admission attempt.
        // A root failure is not a claim about all roots' simultaneous capacity.
        let index = (0..self.roots.len())
            .map(|off| (self.cursor + off) % self.roots.len())
            .find(|&i| self.roots[i].phase == Phase::Active)
            .ok_or(Failure::Policy("NoActiveRoot"))?;
        self.cursor = (index + 1) % self.roots.len();
        let domain = &self.roots[index].domains[stream].as_ref().unwrap().1;
        match domain.try_acquire(size) {
            Ok(buffer) => {
                self.added = self.added.saturating_add(charge);
                self.accepted[stream] += 1;
                Ok(buffer)
            }
            Err(e) => {
                self.core_rejected[stream] += 1;
                if matches!(e, Error::ClassDepleted | Error::DomainClassDepleted) {
                    self.physical += 1;
                }
                Err(Failure::Core(e))
            }
        }
    }
    fn acquire_shared(
        &mut self,
        stream: usize,
        size: usize,
        charge: usize,
    ) -> std::result::Result<mosaic::BufMut, Failure> {
        // AP-14 is deliberately one unreserved root. No routing scan or retry.
        let class = self
            .plan
            .config
            .classes
            .partition_point(|c| c.capacity < charge);
        let shared = self.shared.as_mut().unwrap();
        shared.eligible[stream] = shared.eligible[stream].saturating_add(charge as u64);
        if self.plan.options.eligible_demand {
            self.policy.offer(stream, charge);
        }
        let cached = self.plan.options.cached_accounting;
        let first = shared.check(&self.policy, stream, class, charge);
        // Closed is independent of observations. Every other count-based
        // rejection gets one real refresh, not another core admission attempt.
        let refresh = !cached || (first.is_err() && first != Err("ClosedStream"));
        if refresh {
            let values = self.admitted();
            self.shared.as_mut().unwrap().refresh(values, &self.policy);
        } else if first.is_ok() {
            self.shared.as_mut().unwrap().cache_passes += 1;
        }
        let shared = self.shared.as_mut().unwrap();
        let decision = match shared.check(&self.policy, stream, class, charge) {
            Ok(d) => d,
            Err(reason) => {
                shared.refused(reason);
                self.policy_rejected[stream] += 1;
                self.logical_rejected[stream] += u64::from(reason == "LogicalHardLimit");
                return Err(Failure::Policy(reason));
            }
        };
        if self.roots[0].phase != Phase::Active {
            return Err(Failure::Policy("NoActiveRoot"));
        }
        let domain = &self.roots[0].domains[stream].as_ref().unwrap().1;
        let pending = shared
            .cache
            .reserve(stream, self.policy.groups[stream], charge);
        #[cfg(test)]
        if inject(self.admission_fault, 0).is_err() {
            return Err(Failure::Policy("Injected"));
        }
        let buffer = match domain.try_acquire(size) {
            Ok(b) => b,
            Err(error) => {
                drop(pending);
                self.core_rejected[stream] += 1;
                if error == Error::ClassDepleted {
                    shared.class_failure(class);
                }
                return Err(Failure::Core(error));
            }
        };
        #[cfg(test)]
        if inject(self.admission_fault, 1).is_err() {
            return Err(Failure::Policy("Injected"));
        }
        // No callbacks/await between the core grant and cache commit. On unwind
        // above, buffer is dropped before pending rolls its observation back.
        pending.commit();
        shared.success(stream, charge, decision);
        self.added = self.added.saturating_add(charge);
        self.accepted[stream] += 1;
        Ok(buffer)
    }
    fn retire(&mut self, index: usize) {
        self.retire_at(index, Instant::now());
    }
    fn retire_at(&mut self, index: usize, now: Instant) {
        let begin = Instant::now();
        let r = &mut self.roots[index];
        let observed = r.domains.iter().flatten().fold(0usize, |sum, (_, d)| {
            sum.saturating_add(d.metrics().admitted_bytes)
        });
        if let Some(o) = &r.owner {
            let _ = o.close();
        }
        for d in r.domains.iter_mut() {
            *d = None;
        }
        r.pool = None;
        let destroyed = match r.owner.as_mut() {
            Some(o) => o.try_destroy().is_ok(),
            None => r.phase == Phase::Preparing,
        };
        r.owner = None; // Diagnostic ownership ends before reservation refund.
        r.phase = if destroyed {
            Phase::Free
        } else {
            Phase::Abandoned
        };
        if !destroyed {
            self.observed_admitted_at_abandon =
                self.observed_admitted_at_abandon.saturating_add(observed);
        }
        self.destroy_ns += begin.elapsed().as_nanos();
        if destroyed {
            self.integrate(now);
            self.charged -= self.plan.bytes;
        }
    }
    pub fn maintenance(&mut self, completed: u64) {
        self.maintenance_at(Instant::now(), completed);
    }
    pub(in super::super) fn maintenance_at(&mut self, now: Instant, completed: u64) {
        if self.finished {
            return;
        }
        if let Some(shared) = &mut self.shared {
            shared.now_ms = shared.now_ms.max(
                now.saturating_duration_since(self.start)
                    .as_millis()
                    .min(u64::MAX as u128) as u64,
            );
        }
        // One root per 10ms tick, <=32 pending domain examinations total.
        let i = self.maintenance_cursor;
        self.maintenance_cursor = (i + 1) % self.roots.len();
        if let Some(pool) = &self.roots[i].pool {
            pool.poll_domains(32);
        }
        if now.duration_since(self.tick_at) < Duration::from_millis(100) {
            return;
        }
        let admitted = self.admitted();
        if let Some(shared) = &mut self.shared {
            shared.refresh(admitted, &self.policy);
            shared.sample(&admitted, self.policy.n);
        }
        let total = admitted[..self.policy.n]
            .iter()
            .copied()
            .fold(0usize, usize::saturating_add);
        let returned = self
            .prior_admitted
            .saturating_add(self.added)
            .saturating_sub(total);
        self.prior_admitted = total;
        self.added = 0;
        let elapsed = now.duration_since(self.tick_at).as_millis() as u64;
        self.tick_at = now;
        self.policy.tick(&admitted, elapsed);
        if self.physical > 0 {
            self.deficit.get_or_insert(now);
        } else {
            self.deficit = None;
        }
        let progress = returned > 0 && completed > self.progress;
        self.progress = completed;
        self.physical = 0;
        for i in 0..self.roots.len() {
            if self.roots[i].phase == Phase::Draining
                && self.roots[i]
                    .domains
                    .iter()
                    .flatten()
                    .all(|(_, d)| d.metrics().admitted_bytes == 0)
            {
                self.retire_at(i, now);
            }
        }
        let active = self
            .roots
            .iter()
            .filter(|r| r.phase == Phase::Active)
            .count();
        if total < self.plan.initial * self.plan.bytes / 4 {
            self.calm.get_or_insert(now);
        } else {
            self.calm = None;
        }
        if self.plan.options.elasticity != "off"
            && now.duration_since(self.changed) >= Duration::from_secs(1)
        {
            let grow = progress
                && self
                    .deficit
                    .is_some_and(|t| now.duration_since(t) >= Duration::from_secs(1));
            let shrink = active > self.plan.initial
                && self
                    .calm
                    .is_some_and(|t| now.duration_since(t) >= Duration::from_secs(5));
            if grow || shrink {
                self.changed = now;
                if self.attempts >= 64 {
                    self.refused_attempts += 1;
                } else {
                    self.attempts += 1;
                    if grow {
                        if let Some(r) = self.roots.iter_mut().find(|r| r.phase == Phase::Standby) {
                            r.phase = Phase::Active;
                            self.grown += 1;
                        } else if self.create_at(Phase::Active, now).is_ok() {
                            self.grown += 1;
                        }
                    } else if let Some(r) = self
                        .roots
                        .iter_mut()
                        .rev()
                        .find(|r| r.phase == Phase::Active)
                    {
                        r.phase = Phase::Draining;
                        if let Some(o) = &r.owner {
                            let _ = o.close();
                        }
                        self.shrunk += 1;
                    }
                }
            }
        }
        self.integrate(now.max(self.area_at));
        if self.plan.options.trace > 0 {
            if self.trace.len() < self.plan.options.trace {
                self.trace.push(Sample {
                    ms: now.duration_since(self.start).as_millis() as u64,
                    reserved: self.charged,
                    admitted: total,
                    soft: self.policy.soft.iter().sum(),
                    roots: self
                        .roots
                        .iter()
                        .filter(|r| r.phase == Phase::Active)
                        .count(),
                    rejected: self.policy_rejected.iter().sum(),
                    debt: admitted
                        .iter()
                        .zip(&self.policy.soft)
                        .map(|(&a, &s)| a.saturating_sub(s))
                        .sum(),
                    class_pressure: self.shared.as_ref().map_or(0, |s| s.pressure_mask()),
                    total_pressure: self.shared.as_ref().is_some_and(|s| s.global_pressure()),
                });
            } else {
                self.trace_overflow += 1;
            }
        }
    }
    pub fn close(&mut self) {
        self.policy.active.fill(false);
        for r in self.roots.iter_mut() {
            if r.phase == Phase::Active || r.phase == Phase::Standby {
                r.phase = Phase::Draining;
                if let Some(o) = &r.owner {
                    let _ = o.close();
                }
            }
        }
    }
    pub fn shutdown(&mut self) -> bool {
        self.close();
        for i in 0..self.roots.len() {
            if self.roots[i].owner.is_some() {
                self.retire(i);
            } else if self.roots[i].phase == Phase::Preparing {
                self.roots[i].phase = Phase::Unconfirmed;
            }
        }
        self.finished = true;
        self.charged == 0
    }
    pub fn json(&mut self) -> Value {
        self.integrate(Instant::now());
        let admitted = self.admitted();
        if (!self.finished || self.charged == 0)
            && let Some(shared) = &mut self.shared
        {
            shared.sample(&admitted, self.policy.n);
        }
        let shared = self.shared.as_ref().map(|s| json!({
            "ledger_refreshes":s.ledger_refreshes,"ledger_reads":s.ledger_reads,"cache_passes":s.cache_passes,
            "loan_admissions":s.loan_admissions,"observed_loan_bytes":s.observed_loan_bytes,"quantum_crossings":s.quantum_crossings,
            "total_refusals":s.total_refusals,"pressure_refusals":s.pressure_refusals,"pressure_events":s.pressure_events,
            "class_pressure_mask":s.pressure_mask(),"total_pressure":s.global_pressure(),
            "observation_scope":"conservative admission/maintenance observations; peaks and loan decisions may overestimate; debt projections overlap",
            "streams":(0..self.policy.n).map(|i|json!({
                "offered_bytes":s.offered[i],"eligible_bytes":s.eligible[i],"accepted_bytes":s.accepted[i],
                "observed_peak_admitted":s.observed_peak[i],"observed_refunded_bytes":s.observed_refunded[i],
                "observed_debt":admitted[i].saturating_sub(self.policy.soft[i])
            })).collect::<Vec<_>>()
        }));
        json!({"plan":self.plan.json(),"observed_admitted_at_abandon":self.observed_admitted_at_abandon,"reserved_bytes":self.charged,"peak_reserved_bytes":self.peak,"reservation_byte_us":self.area.to_string(),"attempts":self.attempts,"grown":self.grown,"shrunk":self.shrunk,
            "shared":shared,
            "refused_headroom":self.refused_headroom,"refused_records":self.refused_records,"refused_attempts":self.refused_attempts,"prepare_ns":self.prepare_ns.to_string(),"prepare_max_ns":self.prepare_max_ns.to_string(),"destroy_ns":self.destroy_ns.to_string(),
            "streams":(0..self.policy.n).map(|i|json!({"stream":i,"group":self.policy.groups[i],"rank":self.policy.ranks[self.policy.groups[i]],"admitted":admitted[i],"soft":self.policy.soft[i],"hard":self.policy.hard[i],"accepted":self.accepted[i],"policy_rejected":self.policy_rejected[i],"logical_hard_rejected":self.logical_rejected[i],"group_rejected":self.group_rejected[i],"core_rejected":self.core_rejected[i]})).collect::<Vec<_>>(),
            "records":self.roots.iter().enumerate().filter(|(_,r)|r.generation>0).map(|(i,r)|json!({"record":i,"generation":r.generation,"phase":format!("{:?}",r.phase)})).collect::<Vec<_>>(),
            "trace_overflow":self.trace_overflow,"trace_complete":self.trace_overflow==0,"trace":self.trace.iter().map(|s|json!({"ms":s.ms,"reserved":s.reserved,"admitted":s.admitted,"soft":s.soft,"active_roots":s.roots,"rejected":s.rejected,"observed_debt":s.debt,"class_pressure_mask":s.class_pressure,"total_pressure":s.total_pressure})).collect::<Vec<_>>()})
    }
}
impl Drop for Bank {
    fn drop(&mut self) {
        if !self.finished {
            self.shutdown();
        }
    }
}

#[cfg(test)]
fn inject(fault: Option<(u8, bool)>, stage: u8) -> Result<()> {
    if let Some((at, panic)) = fault
        && at == stage
    {
        if panic {
            panic!("injected preparation interruption");
        }
        return Err("injected prepare error".into());
    }
    Ok(())
}
#[cfg(test)]
mod tests;
