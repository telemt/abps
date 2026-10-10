//! MEM/OWN/LIFE executable specification. No production transition helpers.
#![forbid(unsafe_code)]
#![allow(dead_code)]

use abps_reference::Error;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

pub const CAPACITIES: [usize; 2] = [8, 32];
pub type Id = usize;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Configuration {
    pub counts: [usize; 2],
    pub records: usize,
    pub reserved: bool,
    pub reference_limit: usize,
}
impl Default for Configuration {
    fn default() -> Self {
        Self {
            counts: [2, 2],
            records: 2,
            reserved: false,
            reference_limit: 4,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    Open {
        id: Id,
        limit: usize,
        grants: [usize; 2],
    },
    Acquire {
        id: Id,
        domain: Id,
        size: usize,
    },
    Write {
        id: Id,
        bytes: Vec<u8>,
    },
    Initialize {
        id: Id,
        len: usize,
    },
    SetFilled {
        id: Id,
        len: usize,
    },
    Freeze {
        id: Id,
    },
    Clone {
        source: Id,
        id: Id,
    },
    Slice {
        source: Id,
        id: Id,
        start: usize,
        end: usize,
    },
    Batch {
        source: Id,
        ids: Vec<Id>,
    },
    Drop {
        id: Id,
    },
    Close {
        domain: Id,
    },
    DropControls {
        domain: Id,
    },
    Poll {
        budget: usize,
    },
    Snapshot,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Domain {
    pub limit: usize,
    pub grants: [usize; 2],
    pub controls: bool,
    pub closed: bool,
    pub recycled: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Allocation {
    pub domain: Id,
    pub class: usize,
    pub initialized: Vec<u8>,
    pub filled: usize,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Handle {
    pub allocation: Id,
    pub mutable: bool,
    pub start: usize,
    pub len: usize,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvariantOracle {
    pub config: Configuration,
    pub domains: BTreeMap<Id, Domain>,
    pub allocations: BTreeMap<Id, Allocation>,
    pub handles: BTreeMap<Id, Handle>,
    pub pending: VecDeque<Id>,
    pub explicit_maintenance: bool,
    // An action's identity never changes after shrinking. Previously used IDs
    // cannot be rebound to an unrelated domain or ownership right.
    used_domains: BTreeSet<Id>,
    used_handles: BTreeSet<Id>,
}

impl InvariantOracle {
    pub fn new(config: Configuration, explicit_maintenance: bool) -> Self {
        Self {
            config,
            domains: BTreeMap::new(),
            allocations: BTreeMap::new(),
            handles: BTreeMap::new(),
            pending: VecDeque::new(),
            explicit_maintenance,
            used_domains: BTreeSet::new(),
            used_handles: BTreeSet::new(),
        }
    }
    pub fn admitted(&self, domain: Id) -> usize {
        self.allocations
            .values()
            .filter(|a| a.domain == domain)
            .map(|a| CAPACITIES[a.class])
            .sum()
    }
    pub fn references(&self, allocation: Id) -> usize {
        self.handles
            .values()
            .filter(|h| h.allocation == allocation)
            .count()
    }
    pub fn available(&self, class: usize) -> usize {
        self.config.counts[class]
            - self
                .allocations
                .values()
                .filter(|a| a.class == class)
                .count()
    }
    pub fn free_records(&self) -> usize {
        self.config.records - self.domains.values().filter(|d| !d.recycled).count()
    }
    pub fn assigned(&self, class: usize) -> usize {
        if !self.config.reserved {
            return 0;
        }
        self.domains
            .values()
            .filter(|d| !d.recycled)
            .map(|d| d.grants[class])
            .sum()
    }
    pub fn payload(&self, id: Id) -> &[u8] {
        let h = &self.handles[&id];
        &self.allocations[&h.allocation].initialized[h.start..h.start + h.len]
    }
    fn mutable(&self, id: Id) -> Result<Id, String> {
        self.handles
            .get(&id)
            .filter(|h| h.mutable)
            .map(|h| h.allocation)
            .ok_or_else(|| format!("missing mutable handle {id}"))
    }
    fn shared(&self, id: Id) -> Result<Handle, String> {
        self.handles
            .get(&id)
            .filter(|h| !h.mutable)
            .cloned()
            .ok_or_else(|| format!("missing frozen handle {id}"))
    }
    fn fresh_handle(&self, id: Id) -> Result<(), String> {
        if self.used_handles.contains(&id) {
            Err(format!("rebound handle {id}"))
        } else {
            Ok(())
        }
    }
    fn domain(&self, id: Id) -> Result<&Domain, String> {
        self.domains
            .get(&id)
            .filter(|d| d.controls && !d.recycled)
            .ok_or_else(|| format!("missing domain controls {id}"))
    }
    // A trace dependency failure is distinct from an API error. A shrinker must
    // reject it, rather than skip or retarget the action to another live handle.
    pub fn apply(&mut self, action: &Action) -> Result<Result<(), Error>, String> {
        use Action::*;
        match action {
            Open { id, limit, grants } => {
                if self.used_domains.contains(id) {
                    return Err(format!("rebound domain {id}"));
                }
                if self.config.reserved
                    && (0..2).any(|c| {
                        self.assigned(c)
                            .checked_add(grants[c])
                            .is_none_or(|n| n > self.config.counts[c])
                    })
                {
                    return Ok(Err(Error::ReservationUnavailable));
                }
                if self.free_records() == 0 {
                    return Ok(Err(if self.pending.is_empty() {
                        Error::DomainCapacityExhausted
                    } else {
                        Error::DomainMaintenanceRequired
                    }));
                }
                self.domains.insert(
                    *id,
                    Domain {
                        limit: *limit,
                        grants: *grants,
                        controls: true,
                        closed: false,
                        recycled: false,
                    },
                );
                self.used_domains.insert(*id);
            }
            Acquire { id, domain, size } => {
                self.fresh_handle(*id)?;
                let d = self.domain(*domain)?;
                if *size == 0 {
                    return Ok(Err(Error::InvalidSize));
                }
                let Some(class) = CAPACITIES.iter().position(|c| size <= c) else {
                    return Ok(Err(Error::TooLarge));
                };
                if d.closed {
                    return Ok(Err(Error::Closed));
                }
                if self
                    .admitted(*domain)
                    .checked_add(CAPACITIES[class])
                    .is_none_or(|n| n > d.limit)
                {
                    return Ok(Err(Error::BudgetExceeded));
                }
                if self.config.reserved {
                    let occupied = self
                        .allocations
                        .values()
                        .filter(|a| a.class == class && a.domain == *domain)
                        .count();
                    if occupied == d.grants[class] {
                        return Ok(Err(Error::DomainClassDepleted));
                    }
                } else if self.available(class) == 0 {
                    return Ok(Err(Error::ClassDepleted));
                }
                self.allocations.insert(
                    *id,
                    Allocation {
                        domain: *domain,
                        class,
                        initialized: Vec::new(),
                        filled: 0,
                    },
                );
                self.handles.insert(
                    *id,
                    Handle {
                        allocation: *id,
                        mutable: true,
                        start: 0,
                        len: 0,
                    },
                );
                self.used_handles.insert(*id);
            }
            Write { id, bytes } => {
                let key = self.mutable(*id)?;
                let a = &mut self.allocations.get_mut(&key).unwrap();
                let end = a.filled.checked_add(bytes.len());
                if end.is_none_or(|end| end > CAPACITIES[a.class]) {
                    return Ok(Err(Error::InvalidRange));
                }
                let end = end.unwrap();
                a.initialized.resize(a.initialized.len().max(end), 0);
                a.initialized[a.filled..end].copy_from_slice(bytes);
                a.filled = end;
                self.handles.get_mut(id).unwrap().len = end;
            }
            Initialize { id, len } => {
                let key = self.mutable(*id)?;
                let a = self.allocations.get_mut(&key).unwrap();
                if *len > CAPACITIES[a.class] {
                    return Ok(Err(Error::InvalidRange));
                }
                a.initialized.resize(a.initialized.len().max(*len), 0);
            }
            SetFilled { id, len } => {
                let key = self.mutable(*id)?;
                let a = self.allocations.get_mut(&key).unwrap();
                if *len > a.initialized.len() {
                    return Ok(Err(Error::NotInitialized));
                }
                a.filled = *len;
                self.handles.get_mut(id).unwrap().len = *len;
            }
            Freeze { id } => {
                self.mutable(*id)?;
                self.handles.get_mut(id).unwrap().mutable = false;
            }
            Clone { source, id } | Slice { source, id, .. } => {
                self.fresh_handle(*id)?;
                let mut h = self.shared(*source)?;
                if let Slice { start, end, .. } = action {
                    if start > end || *end > h.len {
                        return Ok(Err(Error::InvalidRange));
                    }
                    h.start += start;
                    h.len = end - start;
                }
                if self.references(h.allocation) == self.config.reference_limit {
                    return Ok(Err(Error::ReferenceLimit));
                }
                self.handles.insert(*id, h);
                self.used_handles.insert(*id);
            }
            Batch { source, ids } => {
                let h = self.shared(*source)?;
                if ids.iter().collect::<BTreeSet<_>>().len() != ids.len() {
                    return Err("duplicate batch identity".into());
                }
                for id in ids {
                    self.fresh_handle(*id)?;
                }
                if self
                    .references(h.allocation)
                    .checked_add(ids.len())
                    .is_none_or(|n| n > self.config.reference_limit)
                {
                    return Ok(Err(Error::ReferenceLimit));
                }
                for id in ids {
                    self.handles.insert(*id, h.clone());
                    self.used_handles.insert(*id);
                }
            }
            Drop { id } => {
                let h = self
                    .handles
                    .remove(id)
                    .ok_or_else(|| format!("missing handle {id}"))?;
                if self.references(h.allocation) == 0 {
                    self.allocations.remove(&h.allocation);
                }
            }
            Close { domain } => {
                self.domain(*domain)?;
                self.domains.get_mut(domain).unwrap().closed = true;
            }
            DropControls { domain } => {
                self.domain(*domain)?;
                let empty = self.admitted(*domain) == 0;
                let d = self.domains.get_mut(domain).unwrap();
                d.controls = false;
                d.closed = true;
                d.recycled = empty;
                if !empty && self.explicit_maintenance {
                    self.pending.push_back(*domain);
                }
            }
            Poll { budget } => {
                for _ in 0..(*budget).min(self.pending.len()) {
                    let id = self.pending.pop_front().unwrap();
                    if self.admitted(id) == 0 {
                        self.domains.get_mut(&id).unwrap().recycled = true;
                    } else {
                        self.pending.push_back(id);
                    }
                }
            }
            Snapshot => {}
        }
        if !self.explicit_maintenance {
            let ready: Vec<_> = self
                .domains
                .iter()
                .filter(|(id, d)| !d.controls && self.admitted(**id) == 0)
                .map(|(id, _)| *id)
                .collect();
            for id in ready {
                self.domains.get_mut(&id).unwrap().recycled = true;
            }
        }
        self.validate()?;
        Ok(Ok(()))
    }
    pub fn validate(&self) -> Result<(), String> {
        let require = |b, message: &str| if b { Ok(()) } else { Err(message.to_owned()) };
        require(
            self.domains.values().filter(|d| !d.recycled).count() <= self.config.records,
            "LIFE-02 record capacity",
        )?;
        require(
            self.pending.iter().collect::<BTreeSet<_>>().len() == self.pending.len(),
            "LIFE-02 duplicate FIFO membership",
        )?;
        for (id, d) in &self.domains {
            require(self.admitted(*id) <= d.limit, "MEM-02 budget")?;
            require(
                !d.recycled || (!d.controls && self.admitted(*id) == 0),
                "LIFE-02 early recycle",
            )?;
            if self.explicit_maintenance {
                require(
                    self.pending.contains(id) == (!d.controls && !d.recycled),
                    "LIFE-02 missing FIFO membership",
                )?;
            }
        }
        for class in 0..2 {
            require(
                self.allocations
                    .values()
                    .filter(|a| a.class == class)
                    .count()
                    <= self.config.counts[class],
                "PROG-01 class capacity",
            )?;
            require(
                self.assigned(class) <= self.config.counts[class],
                "SI-05 grant capacity",
            )?;
        }
        for (id, a) in &self.allocations {
            require(
                self.references(*id) > 0 && self.references(*id) <= self.config.reference_limit,
                "OWN-02 orphan or excess references",
            )?;
            require(
                a.filled <= a.initialized.len() && a.initialized.len() <= CAPACITIES[a.class],
                "OWN-01 initialized boundary",
            )?;
            require(
                self.domains.contains_key(&a.domain) && !self.domains[&a.domain].recycled,
                "LIFE-02 allocation origin",
            )?;
        }
        for h in self.handles.values() {
            require(
                self.allocations.contains_key(&h.allocation),
                "OWN-02 missing allocation",
            )?;
            let a = &self.allocations[&h.allocation];
            require(
                h.start
                    .checked_add(h.len)
                    .is_some_and(|end| end <= a.filled),
                "OWN-01 view boundary",
            )?;
            require(
                !h.mutable || self.references(h.allocation) == 1,
                "OWN-01 mutable aliases",
            )?;
        }
        Ok(())
    }
}

/// A separate staged-return oracle. Slot identity and allocation identity are
/// deliberately different: an old charge can outlive publication of its slot.
#[derive(Default)]
pub struct ReturnLedger {
    pub slots: BTreeMap<usize, Id>,
    pub charges: BTreeMap<Id, (Id, usize)>,
    pub published: BTreeSet<Id>,
}
impl ReturnLedger {
    pub fn issue(&mut self, slot: usize, allocation: Id, domain: Id, charge: usize) {
        assert!(!self.slots.contains_key(&slot));
        assert!(!self.charges.contains_key(&allocation));
        self.slots.insert(slot, allocation);
        self.charges.insert(allocation, (domain, charge));
    }
    pub fn publish(&mut self, slot: usize, allocation: Id) {
        assert_eq!(self.slots.remove(&slot), Some(allocation));
        assert!(self.published.insert(allocation));
    }
    pub fn finish(&mut self, allocation: Id) {
        assert!(
            self.published.remove(&allocation),
            "refund before publication or double refund"
        );
        assert!(self.charges.remove(&allocation).is_some());
    }
    pub fn admitted(&self, domain: Id) -> usize {
        self.charges
            .values()
            .filter(|(d, _)| *d == domain)
            .map(|(_, n)| *n)
            .sum()
    }
}
