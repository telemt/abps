use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootPhase {
    Prepared,
    Active,
    Closing,
    Abandoned,
}

/// Drained/Reusable are instantaneous transitions under the reference mutex.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DomainPhase {
    Free,
    Active,
    Closing,
    Retired,
}

/// Opaque accounting identity. Diagnostic formatting is not a wire representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DomainIdentity {
    pub(super) index: usize,
    pub(super) generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnershipKind {
    Free,
    Mutable,
    Immutable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassSnapshot {
    pub capacity: usize,
    pub total: usize,
    pub available: usize,
    /// Hard assigned rights, including idle slots; zero in Unreserved mode.
    pub reserved: usize,
    /// Unassigned rights in Reserved mode; all slots in Unreserved mode.
    pub unassigned: usize,
    pub acquiring: usize,
    pub issued: usize,
    pub returning: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassGrantSnapshot {
    pub capacity: usize,
    pub reserved: usize,
    pub available: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainSnapshot {
    pub identity: DomainIdentity,
    pub phase: DomainPhase,
    pub limit: usize,
    pub owner: bool,
    pub handles: usize,
    pub allocations: usize,
    pub pending_acquire_bytes: usize,
    pub live_backing_bytes: usize,
    pub pending_return_bytes: usize,
    pub admitted_bytes: usize,
    pub grants: Vec<ClassGrantSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotSnapshot {
    pub class: usize,
    pub origin: Option<DomainIdentity>,
    pub ownership: OwnershipKind,
    /// Exact at quiescence. Final Arc drops can be waiting for the state mutex.
    pub references: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub phase: RootPhase,
    pub reservation: Reservation,
    pub classes: Vec<ClassSnapshot>,
    pub domains: Vec<DomainSnapshot>,
    pub slots: Vec<SlotSnapshot>,
    pub free_domain_records: usize,
    pub retired_domain_records: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Abandonment {
    pub root_identity: usize,
    pub reservation: Reservation,
    pub outstanding_root_claims: usize,
    pub admitted_bytes: usize,
    pub issued_slots: usize,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DiagnosticsSnapshot {
    /// These counters describe this root, not an implicit global registry.
    pub abps_abandoned_roots: usize,
    pub abps_abandoned_bytes: usize,
    pub abandonment: Option<Abandonment>,
    pub destroyed: bool,
}

/// Bounded external observer; contains no root claim, payload, or event history.
#[derive(Clone)]
pub struct Diagnostics {
    pub(super) inner: Arc<Mutex<DiagnosticsSnapshot>>,
}

impl Diagnostics {
    pub(super) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(DiagnosticsSnapshot::default())),
        }
    }
    pub fn snapshot(&self) -> DiagnosticsSnapshot {
        lock(&self.inner).clone()
    }
}

pub(super) struct Root {
    pub config: Config,
    pub reservation: Reservation,
    pub diagnostics: Diagnostics,
    pub state: Mutex<State>,
}

impl Root {
    pub fn check_handles(self: &Arc<Self>, adding: usize) -> Result<(), Error> {
        // All increments are gated by state; concurrent decrements can only make
        // this observation conservative. No transient Weak upgrades are used.
        if Arc::strong_count(self)
            .checked_add(adding)
            .is_none_or(|n| n > self.config.handle_limit)
        {
            Err(Error::ReferenceLimit)
        } else {
            Ok(())
        }
    }

    pub fn close(&self) {
        let mut state = lock(&self.state);
        if matches!(state.phase, RootPhase::Prepared | RootPhase::Active) {
            state.phase = RootPhase::Closing;
        }
        for domain in &mut state.domains {
            if domain.phase == DomainPhase::Active {
                domain.phase = DomainPhase::Closing;
            }
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        let state = lock(&self.state);
        Snapshot {
            phase: state.phase,
            reservation: self.reservation,
            classes: state
                .classes
                .iter()
                .zip(&self.config.classes)
                .enumerate()
                .map(|(index, (class, config))| ClassSnapshot {
                    capacity: config.capacity,
                    total: config.slots,
                    available: class.free.len(),
                    reserved: state
                        .slots
                        .iter()
                        .filter(|s| s.class == index && s.assigned.is_some())
                        .count(),
                    unassigned: state
                        .slots
                        .iter()
                        .filter(|s| s.class == index && s.assigned.is_none())
                        .count(),
                    acquiring: class.acquiring,
                    issued: class.issued,
                    returning: class.returning,
                })
                .collect(),
            domains: state
                .domains
                .iter()
                .enumerate()
                .map(|(index, d)| DomainSnapshot {
                    identity: DomainIdentity {
                        index,
                        generation: d.generation,
                    },
                    phase: d.phase,
                    limit: d.limit,
                    owner: d.owner,
                    handles: d.handles,
                    allocations: d.allocations,
                    pending_acquire_bytes: d.pending_acquire,
                    live_backing_bytes: d.live,
                    pending_return_bytes: d.pending_return,
                    admitted_bytes: d.admitted(),
                    grants: if self.config.capacity_policy == CapacityPolicy::Reserved {
                        self.config
                            .classes
                            .iter()
                            .enumerate()
                            .map(|(class, c)| ClassGrantSnapshot {
                                capacity: c.capacity,
                                reserved: state
                                    .slots
                                    .iter()
                                    .filter(|s| s.class == class && s.assigned == Some(index))
                                    .count(),
                                available: state.classes[class]
                                    .free
                                    .iter()
                                    .filter(|&&s| state.slots[s].assigned == Some(index))
                                    .count(),
                            })
                            .collect()
                    } else {
                        Vec::new()
                    },
                })
                .collect(),
            slots: state
                .slots
                .iter()
                .map(|slot| {
                    let (ownership, references) = match &slot.ownership {
                        Ownership::Free => (OwnershipKind::Free, 0),
                        Ownership::Mutable => (OwnershipKind::Mutable, 1),
                        Ownership::Frozen(weak) => (OwnershipKind::Immutable, weak.strong_count()),
                    };
                    SlotSnapshot {
                        class: slot.class,
                        origin: slot.origin,
                        ownership,
                        references,
                    }
                })
                .collect(),
            free_domain_records: state.free_domains.len(),
            retired_domain_records: state
                .domains
                .iter()
                .filter(|d| d.phase == DomainPhase::Retired)
                .count(),
        }
    }
}

pub(super) struct State {
    pub phase: RootPhase,
    pub capacity_policy: CapacityPolicy,
    pub classes: Vec<Class>,
    pub slots: Vec<Slot>,
    pub domains: Vec<DomainRecord>,
    pub free_domains: Vec<usize>,
}

impl State {
    pub fn domain_mut(&mut self, key: DomainIdentity) -> &mut DomainRecord {
        let domain = &mut self.domains[key.index];
        assert_eq!(
            domain.generation, key.generation,
            "origin identity must remain pinned"
        );
        domain
    }

    pub fn recycle_domain(&mut self, key: DomainIdentity) {
        let domain = self.domain_mut(key);
        if domain.phase == DomainPhase::Closing
            && !domain.owner
            && domain.handles == 0
            && domain.allocations == 0
            && domain.admitted() == 0
        {
            domain.limit = 0;
            if domain.generation == u64::MAX {
                domain.phase = DomainPhase::Retired;
            } else {
                domain.phase = DomainPhase::Free;
                self.free_domains.push(key.index);
            }
            if self.capacity_policy == CapacityPolicy::Reserved {
                for slot in &mut self.slots {
                    if slot.assigned == Some(key.index) {
                        debug_assert!(matches!(slot.ownership, Ownership::Free));
                        slot.assigned = None;
                    }
                }
            }
        }
    }
}

pub(super) struct DomainRecord {
    pub generation: u64,
    pub phase: DomainPhase,
    pub limit: usize,
    pub owner: bool,
    pub handles: usize,
    pub allocations: usize,
    pub pending_acquire: usize,
    pub live: usize,
    pub pending_return: usize,
}

impl DomainRecord {
    pub fn free() -> Self {
        Self {
            generation: 0,
            phase: DomainPhase::Free,
            limit: 0,
            owner: false,
            handles: 0,
            allocations: 0,
            pending_acquire: 0,
            live: 0,
            pending_return: 0,
        }
    }
    pub fn admitted(&self) -> usize {
        self.pending_acquire + self.live + self.pending_return
    }
}

pub(super) struct Class {
    pub free: Vec<usize>,
    pub acquiring: usize,
    pub issued: usize,
    pub returning: usize,
}

pub(super) enum Ownership {
    Free,
    Mutable,
    Frozen(Weak<Allocation>),
}

pub(super) struct Slot {
    pub class: usize,
    pub payload: Option<Box<[u8]>>,
    pub origin: Option<DomainIdentity>,
    pub assigned: Option<usize>,
    pub ownership: Ownership,
}

/// RAII rollback uses the same transitions as the successful acquisition path.
pub(super) struct Admission<'a> {
    pub state: &'a mut State,
    key: DomainIdentity,
    class: usize,
    charge: usize,
    credit: Option<usize>,
    payload: Option<Box<[u8]>>,
    committed: bool,
}

impl<'a> Admission<'a> {
    pub fn new(
        state: &'a mut State,
        key: DomainIdentity,
        class: usize,
        charge: usize,
    ) -> Result<Self, Error> {
        let domain = state.domain_mut(key);
        if domain
            .admitted()
            .checked_add(charge)
            .is_none_or(|n| n > domain.limit)
        {
            return Err(Error::BudgetExceeded);
        }
        domain.pending_acquire += charge;
        Ok(Self {
            state,
            key,
            class,
            charge,
            credit: None,
            payload: None,
            committed: false,
        })
    }

    pub fn reserve_credit(&mut self) -> Result<(), Error> {
        let class = &mut self.state.classes[self.class];
        let slot = if self.state.capacity_policy == CapacityPolicy::Reserved {
            let position = class
                .free
                .iter()
                .position(|&s| self.state.slots[s].assigned == Some(self.key.index))
                .ok_or(Error::DomainClassDepleted)?;
            class.free.swap_remove(position)
        } else {
            class
                .free
                .pop()
                .ok_or(if class.acquiring == 0 && class.returning == 0 {
                    Error::ClassDepleted
                } else {
                    Error::Contended
                })?
        };
        class.acquiring += 1;
        self.credit = Some(slot);
        Ok(())
    }

    pub fn claim_slot(&mut self) {
        let slot = self.credit.expect("credit before slot claim");
        self.payload = self.state.slots[slot].payload.take();
        assert!(self.payload.is_some(), "one payload per credit");
    }

    pub fn commit(mut self) -> (usize, Box<[u8]>) {
        let slot = self.credit.take().expect("claimed credit");
        let payload = self.payload.take().expect("claimed slot");
        let domain = self.state.domain_mut(self.key);
        domain.pending_acquire -= self.charge;
        domain.live += self.charge;
        domain.allocations += 1;
        self.state.classes[self.class].acquiring -= 1;
        self.state.classes[self.class].issued += 1;
        self.state.slots[slot].origin = Some(self.key);
        self.state.slots[slot].ownership = Ownership::Mutable;
        self.committed = true;
        (slot, payload)
    }
}

impl Drop for Admission<'_> {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        if let Some(slot) = self.credit {
            if let Some(payload) = self.payload.take() {
                self.state.slots[slot].payload = Some(payload);
            }
            self.state.classes[self.class].acquiring -= 1;
            self.state.classes[self.class].free.push(slot);
        }
        self.state.domain_mut(self.key).pending_acquire -= self.charge;
    }
}
