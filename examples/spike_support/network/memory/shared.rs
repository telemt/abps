//! AP-14: scheduling observations, never ownership or refund authority.
#![forbid(unsafe_code)]
use super::controller::{Controller, GROUPS, STREAMS};

pub const CLASSES: usize = 64;

pub struct Accounting {
    pub streams: [usize; STREAMS],
    pub groups: [usize; GROUPS],
    pub total: usize,
}
impl Default for Accounting {
    fn default() -> Self {
        Self {
            streams: [0; STREAMS],
            groups: [0; GROUPS],
            total: 0,
        }
    }
}
impl Accounting {
    pub fn replace(&mut self, values: [usize; STREAMS], policy: &Controller) {
        let mut next = Self::default();
        for (i, &value) in values.iter().enumerate().take(policy.n) {
            next.streams[i] = value;
            next.total = next
                .total
                .checked_add(value)
                .expect("prepared layout bound");
            let group = &mut next.groups[policy.groups[i]];
            *group = group.checked_add(value).expect("prepared layout bound");
        }
        *self = next;
    }
    pub fn reserve(&mut self, stream: usize, group: usize, charge: usize) -> Pending<'_> {
        // AP-14: all checks precede mutation. The exclusive guard prevents a
        // refresh or another admission while this charge is provisional.
        let s = self.streams[stream]
            .checked_add(charge)
            .expect("checked hard limit");
        let g = self.groups[group]
            .checked_add(charge)
            .expect("checked total");
        let t = self.total.checked_add(charge).expect("checked total");
        self.streams[stream] = s;
        self.groups[group] = g;
        self.total = t;
        Pending {
            cache: self,
            stream,
            group,
            charge,
            committed: false,
        }
    }
}
pub struct Pending<'a> {
    cache: &'a mut Accounting,
    stream: usize,
    group: usize,
    charge: usize,
    committed: bool,
}
impl Pending<'_> {
    pub fn commit(mut self) {
        self.committed = true;
    }
}
impl Drop for Pending<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.cache.streams[self.stream] -= self.charge;
            self.cache.groups[self.group] -= self.charge;
            self.cache.total -= self.charge;
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Decision {
    pub observed_loan_bytes: usize,
    pub quantum_crossing: bool,
}

pub struct Shared {
    #[cfg(test)]
    pub checks: std::cell::Cell<usize>,
    pub cache: Accounting,
    pub now_ms: u64,
    pressure_ms: u64,
    class_until: [u64; CLASSES],
    total_until: u64,
    pub ledger_refreshes: u64,
    pub ledger_reads: u64,
    pub cache_passes: u64,
    pub offered: [u64; STREAMS],
    pub eligible: [u64; STREAMS],
    pub accepted: [u64; STREAMS],
    pub observed_peak: [usize; STREAMS],
    pub observed_refunded: [u64; STREAMS],
    previous: [usize; STREAMS],
    added: [usize; STREAMS],
    pub loan_admissions: u64,
    pub observed_loan_bytes: u64,
    pub quantum_crossings: u64,
    pub total_refusals: u64,
    pub pressure_refusals: u64,
    pub pressure_events: u64,
}
impl Shared {
    pub fn new(pressure_ms: u64) -> Self {
        Self {
            #[cfg(test)]
            checks: std::cell::Cell::new(0),
            cache: Accounting::default(),
            now_ms: 0,
            pressure_ms,
            class_until: [0; CLASSES],
            total_until: 0,
            ledger_refreshes: 0,
            ledger_reads: 0,
            cache_passes: 0,
            offered: [0; STREAMS],
            eligible: [0; STREAMS],
            accepted: [0; STREAMS],
            observed_peak: [0; STREAMS],
            observed_refunded: [0; STREAMS],
            previous: [0; STREAMS],
            added: [0; STREAMS],
            loan_admissions: 0,
            observed_loan_bytes: 0,
            quantum_crossings: 0,
            total_refusals: 0,
            pressure_refusals: 0,
            pressure_events: 0,
        }
    }
    pub fn pressure_mask(&self) -> u64 {
        self.class_until
            .iter()
            .enumerate()
            .fold(0, |mask, (i, &until)| {
                mask | (u64::from(self.now_ms < until) << i)
            })
    }
    pub fn global_pressure(&self) -> bool {
        self.now_ms < self.total_until
    }
    pub fn class_failure(&mut self, class: usize) {
        self.class_until[class] = self.now_ms.saturating_add(self.pressure_ms);
        self.pressure_events = self.pressure_events.saturating_add(1);
    }
    pub fn refused(&mut self, reason: &str) {
        if reason == "SoftTotalBudget" {
            self.total_until = self.now_ms.saturating_add(self.pressure_ms);
            self.total_refusals = self.total_refusals.saturating_add(1);
            self.pressure_events = self.pressure_events.saturating_add(1);
        } else if reason == "SoftDebtPressure" {
            self.pressure_refusals = self.pressure_refusals.saturating_add(1);
        }
    }
    pub fn check(
        &self,
        p: &Controller,
        stream: usize,
        class: usize,
        charge: usize,
    ) -> Result<Decision, &'static str> {
        #[cfg(test)]
        self.checks.set(self.checks.get() + 1);
        if !p.active[stream] {
            return Err("ClosedStream");
        }
        let held = self.cache.streams[stream];
        let next = held.checked_add(charge).ok_or("LogicalHardLimit")?;
        if next > p.hard[stream] {
            return Err("LogicalHardLimit");
        }
        if self
            .cache
            .total
            .checked_add(charge)
            .is_none_or(|v| v > p.total)
        {
            return Err("SoftTotalBudget");
        }
        let g = p.groups[stream];
        let group = self.cache.groups[g];
        let pressured = self.global_pressure() || self.now_ms < self.class_until[class];
        if pressured && (held >= p.soft[stream] || group >= p.group_soft[g]) {
            return Err("SoftDebtPressure");
        }
        // Stream and group debt describe overlapping bytes. Never add them.
        let loan = (next.saturating_sub(p.soft[stream]) - held.saturating_sub(p.soft[stream])).max(
            (group + charge).saturating_sub(p.group_soft[g])
                - group.saturating_sub(p.group_soft[g]),
        );
        Ok(Decision {
            observed_loan_bytes: loan,
            quantum_crossing: pressured && loan > 0,
        })
    }
    pub fn refresh(&mut self, values: [usize; STREAMS], p: &Controller) {
        self.cache.replace(values, p);
        self.ledger_refreshes = self.ledger_refreshes.saturating_add(1);
        self.ledger_reads = self.ledger_reads.saturating_add(p.n as u64);
    }
    pub fn success(&mut self, stream: usize, charge: usize, decision: Decision) {
        self.accepted[stream] = self.accepted[stream].saturating_add(charge as u64);
        self.added[stream] = self.added[stream].saturating_add(charge);
        self.observed_peak[stream] = self.observed_peak[stream].max(self.cache.streams[stream]);
        self.loan_admissions = self
            .loan_admissions
            .saturating_add(u64::from(decision.observed_loan_bytes > 0));
        self.observed_loan_bytes = self
            .observed_loan_bytes
            .saturating_add(decision.observed_loan_bytes as u64);
        self.quantum_crossings = self
            .quantum_crossings
            .saturating_add(u64::from(decision.quantum_crossing));
    }
    pub fn sample(&mut self, values: &[usize; STREAMS], n: usize) {
        for (i, &value) in values.iter().enumerate().take(n) {
            let returned = self.previous[i]
                .saturating_add(self.added[i])
                .saturating_sub(value);
            self.observed_refunded[i] = self.observed_refunded[i].saturating_add(returned as u64);
            self.previous[i] = value;
            self.added[i] = 0;
            self.observed_peak[i] = self.observed_peak[i].max(value);
        }
    }
}

#[cfg(test)]
mod tests;
