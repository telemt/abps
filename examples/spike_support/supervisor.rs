//! SI-16 cold adapter bookkeeping. Never reached from a buffer destructor.
//! The process supervisor retains this registry across every worker generation.
use crate::perf_support::Result;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Free,
    Preparing,
    Active,
    Draining,
    Abandoned,
    Unconfirmed,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Identity {
    index: usize,
    generation: u64,
}
struct Record {
    generation: u64,
    worker: usize,
    bytes: usize,
    phase: Phase,
}
struct State {
    records: Box<[Record]>,
    attempts: Box<[usize]>,
    charged: usize,
    peak: usize,
}
#[derive(Clone)]
pub struct Registry {
    state: Arc<Mutex<State>>,
    byte_limit: usize,
    attempts: usize,
}
impl Registry {
    pub fn new(roots: usize, bytes: usize, workers: usize, attempts: usize) -> Result<Self> {
        if roots == 0 || bytes == 0 || workers == 0 || attempts > 1 {
            return Err("invalid supervisor bounds (replacement attempts: 0 or 1)".into());
        }
        Ok(Self {
            state: Arc::new(Mutex::new(State {
                records: (0..roots)
                    .map(|_| Record {
                        generation: 0,
                        worker: 0,
                        bytes: 0,
                        phase: Phase::Free,
                    })
                    .collect(),
                attempts: vec![0; workers].into_boxed_slice(),
                charged: 0,
                peak: 0,
            })),
            byte_limit: bytes,
            attempts,
        })
    }
    /// Entire cohort is preflighted before any root is prepared or listener bound.
    /// A failed attempt consumes the lifetime attempt allowance as well.
    pub fn reserve(&self, bytes: &[usize], replacement: bool) -> Result<Vec<Permit>> {
        let mut s = self.state.lock().unwrap();
        if bytes.len() != s.attempts.len() || bytes.contains(&0) {
            return Err("invalid root plan".into());
        }
        if replacement {
            if s.attempts.iter().any(|&n| n >= self.attempts) {
                return Err("replacement attempt limit reached".into());
            }
            for n in &mut s.attempts {
                *n += 1;
            }
        }
        let total = bytes
            .iter()
            .try_fold(0usize, |a, &b| a.checked_add(b))
            .ok_or("root plan overflow")?;
        if total > self.byte_limit - s.charged {
            return Err("supervisor byte limit exceeded".into());
        }
        let free: Vec<_> = s
            .records
            .iter()
            .enumerate()
            .filter(|(_, r)| r.phase == Phase::Free && r.generation != u64::MAX)
            .take(bytes.len())
            .map(|(i, _)| i)
            .collect();
        if free.len() != bytes.len() {
            return Err("supervisor root record limit exceeded".into());
        }
        let mut permits = Vec::with_capacity(bytes.len());
        for (worker, (&index, &n)) in free.iter().zip(bytes).enumerate() {
            let r = &mut s.records[index];
            r.generation += 1;
            r.worker = worker;
            r.bytes = n;
            r.phase = Phase::Preparing;
            permits.push(Permit {
                registry: self.clone(),
                id: Identity {
                    index,
                    generation: r.generation,
                },
                complete: false,
            });
        }
        s.charged += total;
        s.peak = s.peak.max(s.charged);
        Ok(permits)
    }
    fn update(&self, id: Identity, phase: Phase) {
        let mut s = self.state.lock().unwrap();
        let r = &mut s.records[id.index];
        if r.generation != id.generation || r.phase == Phase::Free || r.phase == Phase::Abandoned {
            return;
        }
        r.phase = phase;
    }
    fn refund(&self, id: Identity) {
        let mut s = self.state.lock().unwrap();
        let r = &mut s.records[id.index];
        if r.generation != id.generation || r.phase == Phase::Free || r.phase == Phase::Abandoned {
            return;
        }
        let n = r.bytes;
        r.bytes = 0;
        r.phase = Phase::Free;
        s.charged -= n;
    }
    pub fn unconfirmed(&self, id: Identity) {
        self.update(id, Phase::Unconfirmed);
    }
    pub fn snapshot(&self) -> Value {
        let s = self.state.lock().unwrap();
        json!({"byte_limit":self.byte_limit,"root_limit":s.records.len(),"charged_bytes":s.charged,"peak_bytes":s.peak,
            "replacement_attempts":s.attempts,"abandoned_roots":s.records.iter().filter(|r|r.phase==Phase::Abandoned).count(),"unconfirmed_bytes":s.records.iter().filter(|r|r.phase==Phase::Unconfirmed).map(|r|r.bytes).sum::<usize>(),"abandoned_bytes":s.records.iter().filter(|r|r.phase==Phase::Abandoned).map(|r|r.bytes).sum::<usize>(),
            "records":s.records.iter().enumerate().filter(|(_,r)|r.phase!=Phase::Free).map(|(i,r)|json!({"index":i,"generation":r.generation,"worker":r.worker,"bytes":r.bytes,"phase":format!("{:?}",r.phase)})).collect::<Vec<_>>()})
    }
}
/// One non-cloneable completion authority. Dropping it cannot invent teardown.
/// Cold adapter code calls destroyed only after dropping its storage owner and
/// every diagnostic observer; prepare_failed only when no arena was returned.
pub struct Permit {
    registry: Registry,
    id: Identity,
    complete: bool,
}
impl Permit {
    pub fn identity(&self) -> Identity {
        self.id
    }
    pub fn active(&self) {
        self.registry.update(self.id, Phase::Active);
    }
    pub fn draining(&self) {
        self.registry.update(self.id, Phase::Draining);
    }
    pub fn prepare_failed(mut self) {
        self.registry.refund(self.id);
        self.complete = true;
    }
    pub fn destroyed(mut self) {
        self.registry.refund(self.id);
        self.complete = true;
    }
    pub fn abandoned(mut self) {
        self.registry.update(self.id, Phase::Abandoned);
        self.complete = true;
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        if !self.complete {
            self.registry.unconfirmed(self.id);
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retained_generations_and_stale_completions_cannot_refund_replacements() {
        let r = Registry::new(2, 200, 1, 1).unwrap();
        let old = r.reserve(&[100], false).unwrap().pop().unwrap();
        let stale = old.identity();
        old.abandoned();
        let new = r.reserve(&[100], true).unwrap().pop().unwrap();
        r.refund(stale);
        assert_eq!(r.snapshot()["charged_bytes"], 200);
        assert!(r.reserve(&[100], true).is_err());
        new.destroyed();
        assert_eq!(r.snapshot()["charged_bytes"], 100);
        assert!(r.reserve(&[100], true).is_err());
    }
    #[test]
    fn unconfirmed_charged_until_proof_and_reused_record_rejects_old_identity() {
        let r = Registry::new(1, 100, 1, 1).unwrap();
        let p = r.reserve(&[100], false).unwrap().pop().unwrap();
        let old = p.identity();
        r.unconfirmed(old);
        assert!(r.reserve(&[100], false).is_err());
        p.destroyed();
        let p = r.reserve(&[100], false).unwrap().pop().unwrap();
        r.refund(old);
        assert_eq!(r.snapshot()["charged_bytes"], 100);
        p.prepare_failed();
        assert_eq!(r.snapshot()["charged_bytes"], 0);
    }
    #[test]
    fn bounds_are_independent_transactional_and_failed_attempt_is_not_retried() {
        for (roots, bytes) in [(1, 200), (2, 100)] {
            let r = Registry::new(roots, bytes, 1, 1).unwrap();
            let p = r.reserve(&[100], false).unwrap();
            assert!(r.reserve(&[100], true).is_err());
            assert_eq!(r.snapshot()["charged_bytes"], 100);
            drop(p);
            assert_eq!(r.snapshot()["records"][0]["phase"], "Unconfirmed");
            assert!(r.reserve(&[100], true).is_err());
        }
        let r = Registry::new(1, 100, 1, 1).unwrap();
        r.state.lock().unwrap().records[0].generation = u64::MAX;
        assert!(r.reserve(&[1], false).is_err());
    }
}
