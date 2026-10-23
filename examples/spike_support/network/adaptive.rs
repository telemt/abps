//! AP-11: worker-local policy; no ownership or atomic protocol lives here.
#![forbid(unsafe_code)]
use super::*;
use tokio::time::Instant;

pub(super) const TICK: Duration = Duration::from_millis(10);
const SCALE: u64 = 65536;
#[derive(Clone, Copy, Debug)]
pub(super) struct AdaptivePolicy {
    pub target: Duration,
    pub grace: Duration,
    pub stall: Duration,
    pub key_wait: Duration,
    pub keyed: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CloseReason {
    Pressure,
    WriteStall,
    KeyframeTimeout,
    FrameDeadline,
    GenerationExhausted,
}
impl CloseReason {
    pub fn name(self) -> &'static str {
        match self {
            Self::Pressure => "sustained_pressure",
            Self::WriteStall => "write_stall",
            Self::KeyframeTimeout => "keyframe_timeout",
            Self::FrameDeadline => "frame_deadline",
            Self::GenerationExhausted => "recovery_generation_exhausted",
        }
    }
}
pub(super) struct Controller {
    pub policy: AdaptivePolicy,
    pub window: usize,
    pub epoch: u64,
    pub waiting: bool,
    pub cleaning: bool,
    pub closed: Option<CloseReason>,
    pub age_evictions: u64,
    pub window_evictions: u64,
    pub dependency_evictions: u64,
    pub dependency_skips: u64,
    pub policy_rejections: u64,
    pub stale_commits: u64,
    pub last_batch_limit: Option<usize>,
    pub fast: u64,
    pub debt_ms: u64,
    capacity: usize,
    updated: Instant,
    resized: Instant,
    calm: Option<Instant>,
    recovery_since: Option<Instant>,
    last_progress: Option<Instant>,
    pub batch_ready: Option<Instant>,
    pub batch_deadline: Option<Instant>,
    offers: u64,
    losses: u64,
    initial_rights: usize,
    peak_age: Duration,
}
impl Controller {
    pub fn new(policy: AdaptivePolicy, capacity: usize, now: Instant) -> Self {
        Self {
            policy,
            window: capacity,
            capacity,
            epoch: 0,
            waiting: policy.keyed,
            cleaning: false,
            closed: None,
            age_evictions: 0,
            window_evictions: 0,
            dependency_evictions: 0,
            dependency_skips: 0,
            policy_rejections: 0,
            stale_commits: 0,
            last_batch_limit: None,
            fast: 0,
            debt_ms: 0,
            updated: now,
            resized: now,
            calm: None,
            recovery_since: None,
            last_progress: None,
            batch_ready: None,
            batch_deadline: None,
            offers: 0,
            losses: 0,
            initial_rights: 0,
            peak_age: Duration::ZERO,
        }
    }
    pub fn close(&mut self, reason: CloseReason) {
        self.closed.get_or_insert(reason);
    }
    pub fn gap(&mut self, now: Instant) {
        if !self.policy.keyed || self.closed.is_some() {
            return;
        }
        self.waiting = true;
        self.cleaning = true;
        self.recovery_since.get_or_insert(now);
        match self.epoch.checked_add(1) {
            Some(n) => self.epoch = n,
            None => self.close(CloseReason::GenerationExhausted),
        }
    }
    pub fn offer(&mut self, now: Instant) {
        self.offers = self.offers.saturating_add(1);
        if self.waiting {
            self.recovery_since.get_or_insert(now);
        }
    }
    pub fn pressure_loss(&mut self) {
        self.losses = self.losses.saturating_add(1);
    }
    pub fn age(&mut self, ready: Instant, now: Instant) {
        self.peak_age = self.peak_age.max(now.saturating_duration_since(ready));
    }
    pub fn committed_key(&mut self) {
        self.waiting = false;
        self.cleaning = false;
        self.recovery_since = None;
    }
    pub fn batch(&mut self, ready: Option<Instant>, deadline: Option<Instant>, now: Instant) {
        if ready.is_some() && self.batch_ready.is_none() {
            self.last_progress = Some(now);
        }
        if ready.is_none() {
            self.last_progress = None;
        }
        self.batch_ready = ready;
        self.batch_deadline = deadline;
    }
    pub fn progress(&mut self, bytes: usize, now: Instant) {
        if bytes != 0 {
            self.last_progress = Some(now);
        }
    }
    pub fn batch_limit(&self, configured: usize) -> usize {
        if self.window < self.capacity || self.debt_ms != 0 {
            1
        } else {
            configured
        }
    }
    pub fn sample(&mut self, now: Instant, queued: usize) {
        if let Some(ready) = self.batch_ready {
            self.age(ready, now);
        }
        if self.batch_deadline.is_some_and(|d| now >= d) {
            self.close(CloseReason::FrameDeadline);
        }
        if self
            .last_progress
            .is_some_and(|p| now.saturating_duration_since(p) >= self.policy.stall)
        {
            self.close(CloseReason::WriteStall);
        }
        if self
            .recovery_since
            .is_some_and(|t| now.saturating_duration_since(t) >= self.policy.key_wait)
        {
            self.close(CloseReason::KeyframeTimeout);
        }
        let elapsed = now.saturating_duration_since(self.updated);
        if elapsed < TICK {
            return;
        }
        let dt = elapsed.as_millis().min(u64::MAX as u128) as u64;
        let population = (self.initial_rights as u128).saturating_add(self.offers as u128);
        let loss = (self.losses as u128 * SCALE as u128)
            .checked_div(population)
            .unwrap_or(0)
            .min(SCALE as u128) as u64;
        let age = ((self.peak_age.as_nanos().saturating_mul(SCALE as u128))
            / self.policy.target.as_nanos())
        .min(SCALE as u128) as u64;
        let sample = age.max(loss);
        self.fast = ((100u128 * self.fast as u128 + dt as u128 * sample as u128)
            / (100u128 + dt as u128)) as u64;
        let idle = queued == 0
            && self.batch_ready.is_none()
            && self.peak_age.is_zero()
            && self.losses == 0;
        if !idle && self.fast >= SCALE * 3 / 5 {
            self.debt_ms = self.debt_ms.saturating_add(dt);
            self.calm = None;
            if now.saturating_duration_since(self.resized) >= Duration::from_millis(100) {
                self.window = (self.window / 2).max(1);
                self.resized = now;
            }
        } else if idle || self.fast <= SCALE / 5 {
            self.debt_ms = self.debt_ms.saturating_sub(dt.saturating_mul(2));
            if self.debt_ms == 0 {
                let calm = *self.calm.get_or_insert(now);
                if now.saturating_duration_since(calm) >= Duration::from_millis(500)
                    && now.saturating_duration_since(self.resized) >= Duration::from_millis(100)
                {
                    self.window = (self.window + 1).min(self.capacity);
                    self.resized = now;
                }
            } else {
                self.calm = None;
            }
        } else {
            self.calm = None;
        }
        if self.debt_ms as u128 >= self.policy.grace.as_millis() {
            self.close(CloseReason::Pressure);
        }
        self.updated = now;
        self.offers = 0;
        self.losses = 0;
        self.initial_rights = queued;
        self.peak_age = Duration::ZERO;
    }
    pub fn json(&self) -> Value {
        json!({"window":self.window,"last_batch_limit":self.last_batch_limit,"pressure_q16":self.fast,"debt_ms":self.debt_ms,
            "state":if self.closed.is_some(){"closing"}else if self.waiting{"await_keyframe"}else{"streaming"},
            "recovery_generation":self.epoch,"disconnect_reason":self.closed.map(CloseReason::name),
            "age_evictions":self.age_evictions,"window_evictions":self.window_evictions,
            "dependency_evictions":self.dependency_evictions,"dependency_skips":self.dependency_skips,
            "policy_rejections":self.policy_rejections,"stale_commits":self.stale_commits,
            "write_stall_age_ms":self.last_progress.map(|t|Instant::now().saturating_duration_since(t).as_millis())})
    }
}

#[cfg(test)]
mod tests;
