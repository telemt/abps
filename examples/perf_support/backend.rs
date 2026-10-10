use abps_reference::{
    Buf, BufMut, CapacityPolicy, ClassConfig, ClassReservation, Config, Domain, DomainOwner, Error,
    Pool, StorageOwner,
};
use bytes::Bytes;
use serde_json::{Value, json};
use std::ops::Range;
use std::sync::{Arc, Mutex};

use super::Result;

pub struct Fixture {
    root: Root,
}
enum Root {
    Abps { owner: StorageOwner, pool: Pool },
    Bytes(Arc<Budget>),
}
pub struct Budget {
    classes: Vec<ClassConfig>,
    policy: CapacityPolicy,
    state: Mutex<BudgetState>,
}
struct BudgetState {
    free: Vec<usize>,
    domains: Vec<(usize, usize)>, // immutable limit, admitted backing
    owners: Vec<bool>,
    free_domains: Vec<usize>,
    unassigned: Vec<usize>,
    grants: Vec<usize>,
    used: Vec<usize>,
}
impl Budget {
    fn recycle(&self, state: &mut BudgetState, domain: usize) {
        if state.owners[domain] || state.domains[domain].1 != 0 {
            return;
        }
        if self.policy == CapacityPolicy::Reserved {
            for c in 0..self.classes.len() {
                let i = domain * self.classes.len() + c;
                debug_assert_eq!(state.used[i], 0);
                state.unassigned[c] += std::mem::take(&mut state.grants[i]);
            }
        }
        state.free_domains.push(domain);
    }
}
pub enum Origin {
    Abps { domain: Domain, _owner: DomainOwner },
    Bytes { root: Arc<Budget>, domain: usize },
}
impl Drop for Origin {
    fn drop(&mut self) {
        if let Self::Bytes { root, domain } = self {
            let mut state = root.state.lock().unwrap();
            state.owners[*domain] = false;
            root.recycle(&mut state, *domain);
        }
    }
}
// Payload is dropped before the permit refunds backing rights. The owner, not
// a view's length, carries the charge through Bytes::clone and nonempty slice.
pub struct BytesLease {
    data: Vec<u8>,
    _permit: Permit,
    initialized: usize,
}
struct Permit {
    root: Arc<Budget>,
    domain: usize,
    class: usize,
}
impl Drop for Permit {
    fn drop(&mut self) {
        let mut state = self.root.state.lock().unwrap();
        state.free[self.class] += 1;
        state.domains[self.domain].1 -= self.root.classes[self.class].capacity;
        if self.root.policy == CapacityPolicy::Reserved {
            state.used[self.domain * self.root.classes.len() + self.class] -= 1;
        }
        self.root.recycle(&mut state, self.domain);
    }
}
impl AsRef<[u8]> for BytesLease {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}
pub enum Lease {
    Abps(BufMut),
    Bytes(BytesLease),
}
pub enum Frame {
    Abps(Buf),
    Bytes(Bytes),
}

impl Fixture {
    pub fn new(backend: &str, classes: Vec<ClassConfig>, records: usize) -> Result<Self> {
        Self::with_policy(backend, classes, records, CapacityPolicy::Unreserved)
    }
    pub fn with_policy(
        backend: &str,
        classes: Vec<ClassConfig>,
        records: usize,
        policy: CapacityPolicy,
    ) -> Result<Self> {
        let root = match backend {
            "abps" => {
                // Exact modeled reservation, not an unbounded root budget.
                let mut config = Config::new(classes, records, isize::MAX as usize);
                config.capacity_policy = policy;
                config.root_limit = abps_reference::Reservation::for_config(&config)?.total_bytes;
                let owner = StorageOwner::prepare(config)?;
                let pool = owner.activate()?;
                Root::Abps { owner, pool }
            }
            "bytes" => {
                let free = classes.iter().map(|c| c.slots).collect();
                let unassigned = classes.iter().map(|c| c.slots).collect();
                let entries = if policy == CapacityPolicy::Reserved {
                    records
                        .checked_mul(classes.len())
                        .ok_or(Error::LayoutOverflow)?
                } else {
                    0
                };
                Root::Bytes(Arc::new(Budget {
                    classes,
                    policy,
                    state: Mutex::new(BudgetState {
                        free,
                        domains: vec![(0, 0); records],
                        owners: vec![false; records],
                        free_domains: (0..records).rev().collect(),
                        unassigned,
                        grants: vec![0; entries],
                        used: vec![0; entries],
                    }),
                }))
            }
            _ => return Err("backend must be abps or bytes".into()),
        };
        Ok(Self { root })
    }
    pub fn open(&self, limit: usize) -> Result<Origin> {
        self.open_inner(limit, None)
    }
    #[allow(dead_code)] // Also compiled by the older unreserved-only perf_probe.
    pub fn open_reserved(&self, limit: usize, grants: &[ClassReservation]) -> Result<Origin> {
        self.open_inner(limit, Some(grants))
    }
    fn open_inner(&self, limit: usize, grants: Option<&[ClassReservation]>) -> Result<Origin> {
        Ok(match &self.root {
            Root::Abps { pool, .. } => {
                let (owner, domain) = match grants {
                    Some(g) => pool.try_open_reserved_domain(limit, g)?,
                    None => pool.try_open_domain(limit)?,
                };
                Origin::Abps {
                    domain,
                    _owner: owner,
                }
            }
            Root::Bytes(root) => {
                let mut state = root.state.lock().unwrap();
                if grants.is_some() != (root.policy == CapacityPolicy::Reserved) {
                    return Err(Error::InvalidConfiguration.into());
                }
                let mut previous = 0;
                for g in grants.unwrap_or(&[]) {
                    if g.capacity <= previous || g.slots == 0 {
                        return Err(Error::InvalidConfiguration.into());
                    }
                    root.classes
                        .binary_search_by_key(&g.capacity, |c| c.capacity)
                        .map_err(|_| Error::InvalidConfiguration)?;
                    previous = g.capacity;
                }
                for g in grants.unwrap_or(&[]) {
                    let c = root
                        .classes
                        .binary_search_by_key(&g.capacity, |c| c.capacity)
                        .unwrap();
                    if state.unassigned[c] < g.slots {
                        return Err(Error::ReservationUnavailable.into());
                    }
                }
                let domain = state
                    .free_domains
                    .pop()
                    .ok_or(Error::DomainCapacityExhausted)?;
                for g in grants.unwrap_or(&[]) {
                    let c = root
                        .classes
                        .binary_search_by_key(&g.capacity, |c| c.capacity)
                        .unwrap();
                    state.unassigned[c] -= g.slots;
                    state.grants[domain * root.classes.len() + c] = g.slots;
                }
                state.domains[domain] = (limit, 0);
                state.owners[domain] = true;
                Origin::Bytes {
                    root: Arc::clone(root),
                    domain,
                }
            }
        })
    }
    // Only used cold, or explicitly by the snapshot/observer benchmark.
    pub fn snapshot(&self) -> Value {
        match &self.root {
            Root::Abps { pool, .. } => snapshot_value(pool.snapshot()),
            Root::Bytes(root) => {
                let state = root.state.lock().unwrap();
                json!({
                    "reserved_backing": 0,
                    "capacity_bytes": root.classes.iter().map(|c| c.capacity * c.slots).sum::<usize>(),
                    "admitted": state.domains.iter().map(|d| d.1).sum::<usize>(),
                    "available": state.free.iter().sum::<usize>(),
                    "assigned_slots":state.grants.iter().sum::<usize>(),
                    "unassigned_slots":state.unassigned.iter().sum::<usize>(),
                    "idle_assigned_slots":state.grants.iter().zip(&state.used).map(|(g,u)|g-u).sum::<usize>(),
                    "total_slots": root.classes.iter().map(|c| c.slots).sum::<usize>(),
                })
            }
        }
    }
    pub fn observer(&self) -> Pool {
        match &self.root {
            Root::Abps { pool, .. } => pool.try_clone().unwrap(),
            Root::Bytes(_) => panic!("observer scaling measures ABPS snapshots"),
        }
    }
    pub fn finish(self) -> Result<Value> {
        let result = self.snapshot();
        assert_eq!(result["admitted"], 0, "admission did not drain");
        assert_eq!(
            result["available"], result["total_slots"],
            "capacity did not drain"
        );
        match self.root {
            Root::Abps { mut owner, pool } => {
                drop(pool);
                owner.try_destroy()?;
            }
            Root::Bytes(root) => assert_eq!(Arc::strong_count(&root), 1, "baseline owner leaked"),
        }
        Ok(result)
    }
}
fn snapshot_value(s: abps_reference::Snapshot) -> Value {
    json!({
        "reserved_backing": s.reservation.backing_bytes,
        "modeled_metadata": s.reservation.modeled_metadata_bytes,
        "admitted": s.domains.iter().map(|d| d.admitted_bytes).sum::<usize>(),
        "available": s.classes.iter().map(|c| c.available).sum::<usize>(),
        "total_slots": s.classes.iter().map(|c| c.total).sum::<usize>(),
        "free_domain_records": s.free_domain_records,
    })
}
impl Origin {
    pub fn acquire(&self, size: usize) -> std::result::Result<Lease, Error> {
        match self {
            Self::Abps { domain, .. } => domain.try_acquire(size).map(Lease::Abps),
            Self::Bytes { root, domain } => {
                if size == 0 {
                    return Err(Error::InvalidSize);
                }
                let class = root
                    .classes
                    .iter()
                    .position(|c| c.capacity >= size)
                    .ok_or(Error::TooLarge)?;
                let capacity = root.classes[class].capacity;
                let mut state = root.state.try_lock().map_err(|_| Error::Contended)?;
                let (limit, admitted) = state.domains[*domain];
                let charged = admitted
                    .checked_add(capacity)
                    .ok_or(Error::BudgetExceeded)?;
                if charged > limit {
                    return Err(Error::BudgetExceeded);
                }
                if root.policy == CapacityPolicy::Reserved {
                    let i = domain * root.classes.len() + class;
                    if state.used[i] == state.grants[i] {
                        return Err(Error::DomainClassDepleted);
                    }
                }
                if state.free[class] == 0 {
                    return Err(Error::ClassDepleted);
                }
                state.free[class] -= 1;
                if root.policy == CapacityPolicy::Reserved {
                    state.used[domain * root.classes.len() + class] += 1;
                }
                state.domains[*domain].1 = charged;
                let permit = Permit {
                    root: Arc::clone(root),
                    domain: *domain,
                    class,
                };
                drop(state);
                let mut data = Vec::new();
                // RAII also refunds on allocation failure; no unchecked lengths.
                data.try_reserve_exact(capacity)
                    .map_err(|_| Error::AllocationFailed)?;
                Ok(Lease::Bytes(BytesLease {
                    data,
                    _permit: permit,
                    initialized: 0,
                }))
            }
        }
    }
}
impl Lease {
    pub fn put(&mut self, data: &[u8]) {
        match self {
            Self::Abps(b) => b.put_slice(data).unwrap(),
            Self::Bytes(b) => {
                assert!(b.data.len() + data.len() <= b.data.capacity());
                b.data.extend_from_slice(data);
                b.initialized = b.data.len();
            }
        }
    }
    pub fn initialize(&mut self, len: usize) -> &mut [u8] {
        match self {
            Self::Abps(b) => b.initialize_to(len).unwrap(),
            Self::Bytes(b) => {
                assert!(len <= b.data.capacity());
                b.data.resize(len, 0);
                b.initialized = len;
                &mut b.data
            }
        }
    }
    pub fn set_filled(&mut self, len: usize) {
        match self {
            Self::Abps(b) => b.set_filled_len(len).unwrap(),
            Self::Bytes(b) => {
                assert!(len <= b.initialized);
                b.data.truncate(len);
            }
        }
    }
    pub fn freeze(self) -> Frame {
        match self {
            Self::Abps(b) => Frame::Abps(b.freeze()),
            Self::Bytes(b) => Frame::Bytes(Bytes::from_owner(b)),
        }
    }
}
impl Frame {
    pub fn view(&self, range: Range<usize>) -> std::result::Result<Self, Error> {
        match self {
            Self::Abps(b) => b.try_slice(range).map(Self::Abps),
            Self::Bytes(b) => {
                if range.start > range.end || range.end > b.len() {
                    return Err(Error::InvalidRange);
                }
                // Bytes::slice(0..0) releases the backing. These benchmark
                // adapters preserve ABPS empty-view retention using truncate.
                let view = if range.is_empty() {
                    let mut view = b.clone();
                    view.truncate(0);
                    view
                } else {
                    b.slice(range)
                };
                Ok(Self::Bytes(view))
            }
        }
    }
}
impl AsRef<[u8]> for Frame {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Abps(b) => b.as_ref(),
            Self::Bytes(b) => b.as_ref(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reserved_baselines_are_transactional_isolated_and_recycle_after_last_view() -> Result<()> {
        for backend in ["abps", "bytes"] {
            let fixture = Fixture::with_policy(
                backend,
                vec![
                    ClassConfig {
                        capacity: 8,
                        slots: 2,
                    },
                    ClassConfig {
                        capacity: 32,
                        slots: 1,
                    },
                ],
                2,
                CapacityPolicy::Reserved,
            )?;
            let a = fixture.open_reserved(
                128,
                &[
                    ClassReservation {
                        capacity: 8,
                        slots: 1,
                    },
                    ClassReservation {
                        capacity: 32,
                        slots: 1,
                    },
                ],
            )?;
            let e = fixture
                .open_reserved(
                    128,
                    &[
                        ClassReservation {
                            capacity: 8,
                            slots: 1,
                        },
                        ClassReservation {
                            capacity: 32,
                            slots: 1,
                        },
                    ],
                )
                .err()
                .unwrap();
            assert_eq!(
                e.downcast_ref::<Error>(),
                Some(&Error::ReservationUnavailable)
            );
            let b = fixture.open_reserved(
                128,
                &[ClassReservation {
                    capacity: 8,
                    slots: 1,
                }],
            )?;
            let x = a.acquire(1)?.freeze();
            let v = x.view(0..0)?;
            drop(x);
            assert!(matches!(a.acquire(1), Err(Error::DomainClassDepleted)));
            drop(b.acquire(1)?);
            drop(a);
            assert!(
                fixture
                    .open_reserved(
                        128,
                        &[ClassReservation {
                            capacity: 8,
                            slots: 1
                        }]
                    )
                    .is_err()
            );
            drop(v);
            let c = fixture.open_reserved(
                128,
                &[
                    ClassReservation {
                        capacity: 8,
                        slots: 1,
                    },
                    ClassReservation {
                        capacity: 32,
                        slots: 1,
                    },
                ],
            )?;
            drop((b, c));
            fixture.finish()?;
        }
        Ok(())
    }
    #[test]
    fn both_backends_preserve_charges_through_views_and_failure() -> Result<()> {
        for backend in ["abps", "bytes"] {
            let fixture = Fixture::new(
                backend,
                vec![ClassConfig {
                    capacity: 512,
                    slots: 1,
                }],
                2,
            )?;
            let origin = fixture.open(512)?;
            let other = fixture.open(1024)?;
            let mut lease = origin.acquire(10)?;
            lease.put(&[7; 10]);
            let frame = lease.freeze();
            let view = frame.view(2..3)?;
            let empty = frame.view(0..0)?;
            assert!(matches!(frame.view(9..11), Err(Error::InvalidRange)));
            drop(frame);
            assert_eq!(view.as_ref(), &[7]);
            assert!(matches!(origin.acquire(1), Err(Error::BudgetExceeded)));
            assert!(matches!(other.acquire(1), Err(Error::ClassDepleted)));
            drop(view);
            assert_eq!(fixture.snapshot()["admitted"], 512);
            drop(empty);
            assert_eq!(fixture.snapshot()["admitted"], 0);
            drop(origin);
            drop(other);
            fixture.finish()?;
        }
        Ok(())
    }
}
