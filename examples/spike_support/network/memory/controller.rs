//! AP-13: issuer-local policy, no ownership/refund authority.
#![forbid(unsafe_code)]

pub const STREAMS: usize = 64;
pub const GROUPS: usize = 8;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Fixed,
    Adaptive,
    Shared,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Priority {
    Weighted,
    Strict,
}
#[derive(Clone, Copy, Debug, Default)]
struct Estimate {
    mean: u64,
    deviation: u64,
}
impl Estimate {
    fn sample(&mut self, value: u64) {
        let delta = self.mean.abs_diff(value);
        // 100 ms samples, 400 ms time constant. Overflow cannot lower a bound.
        self.mean = ((self.mean as u128 * 3 + value as u128) / 4) as u64;
        self.deviation = ((self.deviation as u128 * 3 + delta as u128) / 4) as u64;
    }
    fn need(self, horizon_ms: u64) -> usize {
        ((self.mean as u128 + 2 * self.deviation as u128) * horizon_ms as u128 / 100)
            .min(usize::MAX as u128) as usize
    }
}
#[derive(Clone)]
pub struct Controller {
    pub n: usize,
    pub groups: [usize; STREAMS],
    pub ranks: [usize; GROUPS],
    pub hard: [usize; STREAMS],
    pub soft: [usize; STREAMS],
    pub group_soft: [usize; GROUPS],
    pub offered: [u64; STREAMS],
    estimates: [Estimate; STREAMS],
    quantum: [usize; STREAMS],
    aggregate: [Estimate; GROUPS],
    pub active: [bool; STREAMS],
    pub total: usize,
    pub mode: Mode,
    pub priority: Priority,
    pub correlated: bool,
    pub horizon_ms: u64,
}
// Independent of allocator traversal. Capped weighted water filling: at most
// N+1 rounds; each nonterminal round satisfies at least one capped recipient.
fn divide<const N: usize>(budget: usize, caps: [usize; N], weights: [usize; N]) -> [usize; N] {
    let mut out = [0usize; N];
    let mut left = budget;
    for _ in 0..=N {
        let weight: usize = (0..N)
            .filter(|&i| out[i] < caps[i])
            .map(|i| weights[i])
            .sum();
        if left == 0 || weight == 0 {
            break;
        }
        let before = left;
        for i in 0..N {
            if out[i] == caps[i] || weights[i] == 0 {
                continue;
            }
            let share = ((before as u128 * weights[i] as u128) / weight as u128) as usize;
            let amount = share.min(caps[i] - out[i]).min(left);
            out[i] += amount;
            left -= amount;
        }
        // At most N bytes of deterministic rounding, not a work loop per byte.
        for i in 0..N {
            if left > 0 && weights[i] > 0 && out[i] < caps[i] {
                out[i] += 1;
                left -= 1;
            }
        }
        if left == before {
            break;
        }
    }
    out
}
impl Controller {
    #[allow(clippy::too_many_arguments)] // Explicit cold model dimensions.
    pub fn new(
        hard: &[usize],
        groups: &[usize],
        ranks: [usize; GROUPS],
        total: usize,
        mode: Mode,
        priority: Priority,
        correlated: bool,
        horizon_ms: u64,
    ) -> Self {
        assert!(!hard.is_empty() && hard.len() <= STREAMS && hard.len() == groups.len());
        assert!(groups.iter().all(|&g| g < GROUPS) && ranks.iter().all(|&r| r < 3));
        let mut c = Self {
            n: hard.len(),
            groups: [0; STREAMS],
            ranks,
            hard: [0; STREAMS],
            soft: [0; STREAMS],
            group_soft: [0; GROUPS],
            offered: [0; STREAMS],
            estimates: [Estimate::default(); STREAMS],
            quantum: [0; STREAMS],
            aggregate: [Estimate::default(); GROUPS],
            active: [false; STREAMS],
            total,
            mode,
            priority,
            correlated,
            horizon_ms,
        };
        c.hard[..hard.len()].copy_from_slice(hard);
        c.groups[..groups.len()].copy_from_slice(groups);
        c.active[..hard.len()].fill(true);
        c.allocate(c.hard, [usize::MAX; GROUPS]);
        c
    }
    pub fn offer(&mut self, stream: usize, charge: usize) {
        self.quantum[stream] = self.quantum[stream].max(charge);
        self.offered[stream] = self.offered[stream].saturating_add(charge as u64);
    }
    pub fn tick(&mut self, admitted: &[usize; STREAMS], elapsed_ms: u64) {
        let mut group = [0u64; GROUPS];
        let mut need = self.hard;
        let mut group_need = [usize::MAX; GROUPS];
        let mut retained = [0usize; GROUPS];
        let mut minimum = [0usize; GROUPS];
        for i in 0..self.n {
            let value = ((self.offered[i] as u128 * 100) / elapsed_ms.max(1) as u128)
                .min(u64::MAX as u128) as u64;
            let quantum = if value > 0 { self.quantum[i] } else { 0 };
            minimum[self.groups[i]] = minimum[self.groups[i]].saturating_add(quantum);
            self.offered[i] = 0;
            self.quantum[i] = 0;
            group[self.groups[i]] = group[self.groups[i]].saturating_add(value);
            retained[self.groups[i]] = retained[self.groups[i]].saturating_add(admitted[i]);
            self.estimates[i].sample(value);
            if self.mode != Mode::Fixed {
                need[i] = self.estimates[i]
                    .need(self.horizon_ms)
                    .max(if self.mode == Mode::Shared {
                        0
                    } else {
                        admitted[i]
                    })
                    .max(quantum)
                    .min(self.hard[i]);
            }
        }
        for g in 0..GROUPS {
            self.aggregate[g].sample(group[g]);
            if self.mode != Mode::Fixed && self.correlated {
                group_need[g] = self.aggregate[g]
                    .need(self.horizon_ms)
                    .max(if self.mode == Mode::Shared {
                        0
                    } else {
                        retained[g]
                    })
                    .max(minimum[g]);
            }
        }
        self.allocate(need, group_need);
    }
    fn allocate(&mut self, mut need: [usize; STREAMS], aggregate: [usize; GROUPS]) {
        let mut caps = [0usize; GROUPS];
        for (i, n) in need.iter_mut().enumerate().take(self.n) {
            if !self.active[i] {
                *n = 0;
            }
            caps[self.groups[i]] = caps[self.groups[i]].saturating_add(*n);
        }
        for g in 0..GROUPS {
            caps[g] = caps[g].min(aggregate[g]);
        }
        self.group_soft = match self.priority {
            Priority::Weighted => {
                let minimum = divide(self.total / 10, caps, [1; GROUPS]);
                let left = self.total - minimum.iter().sum::<usize>();
                let residual: [usize; GROUPS] = std::array::from_fn(|g| caps[g] - minimum[g]);
                let extra = divide(
                    left,
                    residual,
                    std::array::from_fn(|g| [8, 3, 1][self.ranks[g]]),
                );
                std::array::from_fn(|g| minimum[g] + extra[g])
            }
            Priority::Strict => {
                let mut out = [0; GROUPS];
                let mut left = self.total;
                for rank in 0..3 {
                    let share = divide(
                        left,
                        std::array::from_fn(|g| if self.ranks[g] == rank { caps[g] } else { 0 }),
                        [1; GROUPS],
                    );
                    for g in 0..GROUPS {
                        out[g] += share[g];
                        left -= share[g];
                    }
                }
                out
            }
        };
        self.soft.fill(0);
        for g in 0..GROUPS {
            let caps = std::array::from_fn(|i| if self.groups[i] == g { need[i] } else { 0 });
            let share: [usize; STREAMS] = divide(self.group_soft[g], caps, [1; STREAMS]);
            for (dst, src) in self.soft.iter_mut().zip(share) {
                *dst += src;
            }
        }
    }
    /// Spend only unassigned soft opportunity, never another group's allocation.
    /// Contraction debt is excluded from that slack; retained rights are untouched.
    pub fn admission(
        &mut self,
        stream: usize,
        charge: usize,
        admitted: &[usize; STREAMS],
    ) -> Result<(), &'static str> {
        let result = self.permits(stream, charge, admitted);
        if result != Err("SoftStreamBudget")
            || self.mode != Mode::Adaptive
            || admitted[stream] > self.soft[stream]
        {
            return result;
        }
        let next = admitted[stream]
            .checked_add(charge)
            .ok_or("LogicalHardLimit")?;
        let delta = next - self.soft[stream]; // Positive after SoftStreamBudget.
        let allocated = self.soft.iter().sum::<usize>();
        let debt = (0..self.n).fold(0usize, |sum, i| {
            sum.saturating_add(admitted[i].saturating_sub(self.soft[i]))
        });
        let spare = self.total.saturating_sub(allocated).saturating_sub(debt);
        let group = self.groups[stream];
        let group_charge = (0..self.n)
            .filter(|&i| self.groups[i] == group)
            .try_fold(charge, |sum, i| sum.checked_add(admitted[i]))
            .ok_or("SoftGroupBudget")?;
        if delta > spare || group_charge > self.group_soft[group].saturating_add(delta) {
            return result;
        }
        self.soft[stream] = next;
        self.group_soft[group] += delta;
        self.permits(stream, charge, admitted)
    }
    pub fn permits(
        &self,
        stream: usize,
        charge: usize,
        admitted: &[usize; STREAMS],
    ) -> Result<(), &'static str> {
        if !self.active[stream] {
            return Err("ClosedStream");
        }
        let next = admitted[stream]
            .checked_add(charge)
            .ok_or("LogicalHardLimit")?;
        if next > self.hard[stream] {
            return Err("LogicalHardLimit");
        }
        if next > self.soft[stream] {
            return Err("SoftStreamBudget");
        }
        let mut group = charge;
        for (i, &value) in admitted.iter().enumerate().take(self.n) {
            if self.groups[i] == self.groups[stream] {
                group = group.checked_add(value).ok_or("SoftGroupBudget")?;
            }
        }
        if group > self.group_soft[self.groups[stream]] {
            return Err("SoftGroupBudget");
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    #[test]
    fn ap13_soft_shrink_never_refunds_existing_rights() {
        let mut c = Controller::new(
            &[1024],
            &[0],
            [0; GROUPS],
            1024,
            Mode::Fixed,
            Priority::Weighted,
            false,
            20,
        );
        let held = [1024; STREAMS];
        c.total = 512;
        c.tick(&held, 100);
        assert_eq!(c.soft[0], 512);
        assert_eq!(held[0], 1024);
        assert_eq!(c.permits(0, 1, &held), Err("LogicalHardLimit"));
        let mut lower = held;
        lower[0] = 512;
        assert_eq!(c.permits(0, 1, &lower), Err("SoftStreamBudget"));
        lower[0] = 0;
        assert!(c.permits(0, 512, &lower).is_ok());
    }
    #[test]
    fn ap13_weighted_minimum_and_strict_starvation_are_explicit() {
        let mut ranks = [0; GROUPS];
        ranks[1] = 1;
        ranks[2] = 2;
        let weighted = Controller::new(
            &[1000; 3],
            &[0, 1, 2],
            ranks,
            1000,
            Mode::Fixed,
            Priority::Weighted,
            false,
            20,
        );
        assert!(weighted.soft[2] >= 33);
        assert!(weighted.soft[0] > weighted.soft[1]);
        let strict = Controller::new(
            &[1000; 3],
            &[0, 1, 2],
            ranks,
            1000,
            Mode::Fixed,
            Priority::Strict,
            false,
            20,
        );
        assert_eq!(&strict.soft[..3], &[1000, 0, 0]);
    }
    #[test]
    fn ap13_rejected_offers_recover_demand_from_zero() {
        let mut c = Controller::new(
            &[1000],
            &[0],
            [0; GROUPS],
            1000,
            Mode::Adaptive,
            Priority::Weighted,
            false,
            100,
        );
        c.tick(&[0; STREAMS], 100);
        assert_eq!(c.soft[0], 0);
        c.offer(0, 1000);
        c.tick(&[0; STREAMS], 100);
        assert!(c.soft[0] > 0);
    }
    #[test]
    fn ap13_idle_burst_uses_only_unassigned_soft_opportunity() {
        let mut c = Controller::new(
            &[1000; 2],
            &[0, 1],
            [0; GROUPS],
            1000,
            Mode::Adaptive,
            Priority::Weighted,
            false,
            20,
        );
        let mut admitted = [0; STREAMS];
        c.tick(&admitted, 100);
        assert_eq!(c.soft[0], 0);
        for _ in 0..4 {
            assert!(c.admission(0, 100, &admitted).is_ok());
            admitted[0] += 100;
        }
        assert!(c.admission(1, 600, &admitted).is_ok());
        admitted[1] = 600;
        assert_eq!(c.soft.iter().sum::<usize>(), 1000);
        assert!(c.admission(0, 1, &admitted).is_err());
        c.soft[0] = 100;
        c.group_soft[0] = 100; // contraction creates 300 debt
        assert!(c.admission(1, 1, &admitted).is_err());
        assert_eq!(admitted[0], 400);
    }
    proptest! {
        #![proptest_config(proptest::test_runner::Config {
            cases: if cfg!(miri) {4} else {proptest::test_runner::Config::default().cases},
            failure_persistence: if cfg!(miri) {None} else {Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource("proptest-regressions")))},
            ..Default::default()
        })]
        #[test]
        fn ap13_policy_conserves_total_and_hard_caps(hard in prop::collection::vec(0usize..100000,1..65), total in 0usize..1000000) {
            let groups:Vec<_>=(0..hard.len()).map(|i|i%GROUPS).collect();
            for priority in [Priority::Weighted,Priority::Strict] {
                let mut c=Controller::new(&hard,&groups,std::array::from_fn(|g|g%3),total,Mode::Adaptive,priority,true,20);
                for k in 0..6 {
                    for (i,&h) in hard.iter().enumerate(){ c.offer(i,h.saturating_mul(k)); }
                    c.tick(&[0;STREAMS],100);
                    prop_assert!(c.soft.iter().sum::<usize>()<=total);
                    prop_assert!(c.group_soft.iter().sum::<usize>()<=total);
                    for (i,&h) in hard.iter().enumerate(){prop_assert!(c.soft[i]<=h);}
                }
            }
        }
    }
}
