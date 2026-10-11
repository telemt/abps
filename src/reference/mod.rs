//! Executable resource-accounting model, **not a production allocator**.
//!
//! Try operations use one mutex attempt. Freeze, Drop, and management operations
//! may block. Payload is prepared; ownership wrappers and snapshots allocate.
//! Reservation metadata is a model, not a physical-memory bound. See the repository's
//! CONTRIBUTING.md for the production contract and explicit reference exceptions.
//!
//! ```
//! use abps_reference::{ClassConfig, Config, StorageOwner};
//! let mut owner = StorageOwner::prepare(Config::new(
//!     vec![ClassConfig { capacity: 4096, slots: 2 }], 1, 1 << 20,
//! ))?;
//! let pool = owner.activate()?;
//! let (domain_owner, domain) = pool.try_open_domain(8192)?;
//! let mut writable = domain.try_acquire(100)?;
//! writable.put_slice(b"frame")?;
//! let frame = writable.freeze();
//! let view = frame.try_slice(1..4)?;
//! assert_eq!(view.as_ref(), b"ram");
//! drop((frame, view, domain, domain_owner, pool));
//! owner.try_destroy()?;
//! # Ok::<(), abps_reference::Error>(())
//! ```

#![forbid(unsafe_code)]

use std::cell::Cell;
use std::fmt;
use std::io::IoSlice;
use std::marker::PhantomData;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError, Weak};

mod state;
pub use state::{
    Abandonment, ClassGrantSnapshot, ClassSnapshot, Diagnostics, DiagnosticsSnapshot,
    DomainIdentity, DomainPhase, DomainSnapshot, OwnershipKind, RootPhase, SlotSnapshot, Snapshot,
};
use state::{Admission, Class, DomainRecord, Root, Slot, State};

/// Errors are control flow; none triggers fallback allocation or a core retry.
///
/// | Error | Caller action |
/// | --- | --- |
/// | `Contended` | A bounded attempt lost a race, even to a remote refund far below a limit; a bounded retry may succeed. Not proof of exhaustion. |
/// | `BudgetExceeded` | This attempt observed insufficient byte headroom; the caller owns retry policy. |
/// | `ClassDepleted`, `DomainClassDepleted` | This attempt observed no eligible slot; a concurrent return can make a later attempt succeed. |
/// | `DomainMaintenanceRequired` | Issuer calls bounded `poll_domains`; no implicit scan. |
/// | `Closed` | Stop acquisition; already issued buffers remain readable. |
/// | `Busy` | Keep the storage owner until drain; never infer that memory was freed. |
/// | `TooLarge`, invalid configuration/range | Correct the request/configuration. |
///
/// There is no universal retryable predicate: the application owns retry, wait
/// and overload policy. Exact-class selection never borrows a larger class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    InvalidConfiguration,
    LayoutOverflow,
    RootBudgetExceeded,
    AllocationFailed,
    InvalidSize,
    TooLarge,
    BudgetExceeded,
    /// No eligible slot at an observation point inside this attempt. This does
    /// not assert that the class remains empty when the call returns. Berry's
    /// witness is its confirming RMW; Mosaic's confirming pass establishes a
    /// common empty witness across eligible words (SI-01/SI-12).
    ClassDepleted,
    /// The same observation contract as ClassDepleted, restricted to the
    /// domain's hard grant in the selected class. No borrowing or hidden retry.
    DomainClassDepleted,
    /// Opening a reserved domain could not obtain every requested grant.
    ReservationUnavailable,
    Contended,
    Closed,
    ReferenceLimit,
    /// Batch clone output must contain only None before the operation.
    OutputNotEmpty,
    /// Local branch output exceeds the explicitly prepared per-slot capacity.
    BranchCapacityExceeded,
    DomainCapacityExhausted,
    /// Berry experiment: explicit record maintenance may free capacity.
    DomainMaintenanceRequired,
    InvalidRange,
    NotInitialized,
    Busy,
    AlreadyActive,
    Destroyed,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

/// LIFE-02: opaque process-local root identity for diagnostics, never an
/// address. Each value is assigned once across all backends. When the space
/// is exhausted, later roots report 0 (unassigned) instead of reusing one.
/// Cold preparation work only; only concurrent preparations contend.
pub(crate) fn next_root_identity() -> usize {
    static NEXT: AtomicUsize = AtomicUsize::new(1);
    let mut current = NEXT.load(Ordering::Relaxed);
    while let Some(next) = current.checked_add(1) {
        match NEXT.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return current,
            Err(actual) => current = actual,
        }
    }
    0
}

/// IO-01: whether one platform descriptor can cover `len` bytes. Windows
/// `IoSlice` stores a 32-bit length and std panics above it. Other targets
/// accept any slice; there a comparison against `usize::MAX` would be the
/// deny-by-default `clippy::absurd_extreme_comparisons`, so no limit is spelled.
#[cfg(windows)]
fn io_slice_fits(len: usize) -> bool {
    u32::try_from(len).is_ok()
}
#[cfg(not(windows))]
fn io_slice_fits(_len: usize) -> bool {
    true
}

/// IO-01: borrowed descriptor of `bytes`, or `TooLarge` where one platform
/// `IoSlice` cannot describe them. Callers split larger views explicitly.
pub(crate) fn try_io_slice(bytes: &[u8]) -> Result<IoSlice<'_>, Error> {
    if io_slice_fits(bytes.len()) {
        Ok(IoSlice::new(bytes))
    } else {
        Err(Error::TooLarge)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ClassConfig {
    pub capacity: usize,
    pub slots: usize,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum CapacityPolicy {
    #[default]
    Unreserved,
    Reserved,
}

/// Hard slot rights, separate from a domain's byte limit. No idle borrowing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClassReservation {
    pub capacity: usize,
    pub slots: usize,
}

fn validate_grants(classes: &[ClassConfig], grants: &[ClassReservation]) -> Result<(), Error> {
    let mut previous = 0;
    for grant in grants {
        if grant.capacity <= previous
            || grant.slots == 0
            || classes
                .binary_search_by_key(&grant.capacity, |c| c.capacity)
                .is_err()
        {
            return Err(Error::InvalidConfiguration);
        }
        previous = grant.capacity;
    }
    Ok(())
}

#[derive(Debug)]
pub struct Config {
    /// Strictly increasing positive capacities, each with at least one slot.
    pub classes: Vec<ClassConfig>,
    pub capacity_policy: CapacityPolicy,
    pub domain_slots: usize,
    pub root_limit: usize,
    /// Checked read-right limit per immutable allocation, including the original.
    pub reference_limit: usize,
    /// Checked root Arc claim limit, including StorageOwner and allocations.
    pub handle_limit: usize,
}

impl Config {
    pub fn new(classes: Vec<ClassConfig>, domain_slots: usize, root_limit: usize) -> Self {
        Self {
            classes,
            capacity_policy: CapacityPolicy::Unreserved,
            domain_slots,
            root_limit,
            reference_limit: isize::MAX as usize,
            handle_limit: isize::MAX as usize,
        }
    }
}

/// Accounted reservation. `modeled_metadata_bytes` is not a measured heap bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reservation {
    pub backing_bytes: usize,
    pub modeled_metadata_bytes: usize,
    pub total_bytes: usize,
}

impl Reservation {
    pub fn for_config(config: &Config) -> Result<Self, Error> {
        if config.classes.is_empty()
            || config.domain_slots == 0
            || !(1..=isize::MAX as usize).contains(&config.reference_limit)
            || !(1..=isize::MAX as usize).contains(&config.handle_limit)
        {
            return Err(Error::InvalidConfiguration);
        }
        let mut backing = 0usize;
        let mut slots = 0usize;
        let mut previous = 0;
        for class in &config.classes {
            if class.capacity <= previous || class.slots == 0 {
                return Err(Error::InvalidConfiguration);
            }
            previous = class.capacity;
            backing = backing
                .checked_add(
                    class
                        .capacity
                        .checked_mul(class.slots)
                        .ok_or(Error::LayoutOverflow)?,
                )
                .ok_or(Error::LayoutOverflow)?;
            slots = slots
                .checked_add(class.slots)
                .ok_or(Error::LayoutOverflow)?;
        }
        let mut metadata =
            size_of::<Root>() + size_of::<State>() + size_of::<DiagnosticsSnapshot>();
        // This is an explicit fixed-table accounting model. Allocator headers,
        // transient Arc destruction, snapshot vectors, and preparation peaks are
        // NOT measured here and cannot support a physical MEM-01 claim.
        for (n, size) in [
            (
                config.classes.len(),
                size_of::<Class>() + size_of::<ClassConfig>(),
            ),
            (
                slots,
                size_of::<Slot>()
                    + size_of::<usize>()
                    + size_of::<Allocation>()
                    + 2 * size_of::<usize>(),
            ),
            (
                config.domain_slots,
                size_of::<DomainRecord>() + size_of::<usize>(),
            ),
        ] {
            metadata = metadata
                .checked_add(n.checked_mul(size).ok_or(Error::LayoutOverflow)?)
                .ok_or(Error::LayoutOverflow)?;
        }
        let total = backing.checked_add(metadata).ok_or(Error::LayoutOverflow)?;
        if total > isize::MAX as usize {
            return Err(Error::LayoutOverflow);
        }
        if total > config.root_limit {
            return Err(Error::RootBudgetExceeded);
        }
        Ok(Self {
            backing_bytes: backing,
            modeled_metadata_bytes: metadata,
            total_bytes: total,
        })
    }
}

fn prepared_vec<T>(capacity: usize) -> Result<Vec<T>, Error> {
    let mut result = Vec::new();
    result
        .try_reserve_exact(capacity)
        .map_err(|_| Error::AllocationFailed)?;
    Ok(result)
}

// No user callbacks run under these locks. A poisoned mutex means an internal
// invariant failure; recover the guard so destructors can finish RAII rollback.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn try_lock<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>, Error> {
    match mutex.try_lock() {
        Ok(guard) => Ok(guard),
        Err(TryLockError::WouldBlock) => Err(Error::Contended),
        Err(TryLockError::Poisoned(poisoned)) => Ok(poisoned.into_inner()),
    }
}

/// Unique cold owner. Dropping before drain intentionally abandons the arena.
pub struct StorageOwner {
    root: Option<Arc<Root>>,
}

impl StorageOwner {
    pub fn prepare(config: Config) -> Result<Self, Error> {
        let reservation = Reservation::for_config(&config)?;
        let mut classes = prepared_vec(config.classes.len())?;
        let count = config.classes.iter().map(|c| c.slots).sum();
        let mut slots = prepared_vec(count)?;
        for (class_id, class) in config.classes.iter().enumerate() {
            let mut free = prepared_vec(class.slots)?;
            for _ in 0..class.slots {
                let mut payload = prepared_vec(class.capacity)?;
                payload.resize(class.capacity, 0);
                free.push(slots.len());
                slots.push(Slot {
                    class: class_id,
                    payload: Some(payload.into_boxed_slice()),
                    origin: None,
                    assigned: None,
                    ownership: state::Ownership::Free,
                });
            }
            classes.push(Class {
                free,
                acquiring: 0,
                issued: 0,
                returning: 0,
            });
        }
        let mut domains = prepared_vec(config.domain_slots)?;
        let mut free_domains = prepared_vec(config.domain_slots)?;
        for index in 0..config.domain_slots {
            domains.push(DomainRecord::free());
            free_domains.push(index);
        }
        let diagnostics = Diagnostics::new();
        let capacity_policy = config.capacity_policy;
        Ok(Self {
            root: Some(Arc::new(Root {
                config,
                reservation,
                identity: next_root_identity(),
                diagnostics,
                state: Mutex::new(State {
                    phase: RootPhase::Prepared,
                    capacity_policy,
                    classes,
                    slots,
                    domains,
                    free_domains,
                }),
            })),
        })
    }

    pub fn diagnostics(&self) -> Result<Diagnostics, Error> {
        Ok(self.root()?.diagnostics.clone())
    }

    pub fn activate(&self) -> Result<Pool, Error> {
        let root = self.root()?;
        let mut state = lock(&root.state);
        match state.phase {
            RootPhase::Prepared => {}
            RootPhase::Active => return Err(Error::AlreadyActive),
            _ => return Err(Error::Closed),
        }
        root.check_handles(1)?;
        state.phase = RootPhase::Active;
        Ok(Pool {
            root: Arc::clone(root),
        })
    }

    pub fn close(&self) -> Result<(), Error> {
        self.root()?.close();
        Ok(())
    }

    /// Cold, allocating snapshot. Ownership counts are exact only at quiescence.
    pub fn snapshot(&self) -> Result<Snapshot, Error> {
        Ok(self.root()?.snapshot())
    }

    /// Busy keeps this owner intact. The only successful explicit teardown path.
    pub fn try_destroy(&mut self) -> Result<(), Error> {
        let root = self.root.take().ok_or(Error::Destroyed)?;
        root.close();
        match Arc::try_unwrap(root) {
            Ok(root) => {
                lock(&root.diagnostics.inner).destroyed = true;
                drop(root);
                Ok(())
            }
            Err(root) => {
                self.root = Some(root);
                Err(Error::Busy)
            }
        }
    }

    fn root(&self) -> Result<&Arc<Root>, Error> {
        self.root.as_ref().ok_or(Error::Destroyed)
    }
}

impl Drop for StorageOwner {
    fn drop(&mut self) {
        let Some(root) = self.root.take() else { return };
        root.close();
        match Arc::try_unwrap(root) {
            Ok(root) => {
                lock(&root.diagnostics.inner).destroyed = true;
            }
            Err(root) => {
                {
                    let mut state = lock(&root.state);
                    state.phase = RootPhase::Abandoned;
                    let event = Abandonment {
                        root_identity: root.identity,
                        reservation: root.reservation,
                        outstanding_root_claims: Arc::strong_count(&root) - 1,
                        admitted_bytes: state.domains.iter().map(DomainRecord::admitted).sum(),
                        issued_slots: state.classes.iter().map(|c| c.issued).sum(),
                    };
                    let mut diagnostics = lock(&root.diagnostics.inner);
                    diagnostics.abps_abandoned_roots = 1;
                    diagnostics.abps_abandoned_bytes = root.reservation.total_bytes;
                    diagnostics.abandonment = Some(event);
                }
                // LIFE-02: retain one strong arena claim permanently. No buffer,
                // diagnostic observer, or later handle can inherit physical teardown.
                std::mem::forget(root);
            }
        }
    }
}

pub struct Pool {
    root: Arc<Root>,
}

impl Pool {
    pub fn try_clone(&self) -> Result<Self, Error> {
        let _state = try_lock(&self.root.state)?;
        self.root.check_handles(1)?;
        Ok(Self {
            root: Arc::clone(&self.root),
        })
    }

    /// Leases a prepared record. Limit is immutable for this incarnation.
    pub fn try_open_domain(&self, limit: usize) -> Result<(DomainOwner, Domain), Error> {
        if self.root.config.capacity_policy != CapacityPolicy::Unreserved {
            return Err(Error::InvalidConfiguration);
        }
        self.open_inner(limit, &[])
    }

    pub fn try_open_reserved_domain(
        &self,
        limit: usize,
        grants: &[ClassReservation],
    ) -> Result<(DomainOwner, Domain), Error> {
        if self.root.config.capacity_policy != CapacityPolicy::Reserved {
            return Err(Error::InvalidConfiguration);
        }
        validate_grants(&self.root.config.classes, grants)?;
        self.open_inner(limit, grants)
    }

    fn open_inner(
        &self,
        limit: usize,
        grants: &[ClassReservation],
    ) -> Result<(DomainOwner, Domain), Error> {
        let mut state = try_lock(&self.root.state)?;
        if state.phase != RootPhase::Active {
            return Err(Error::Closed);
        }
        // SI-05/SI-09: preflight grants, then the record, then root claims, in
        // the issuer backends' order, before consuming a record or any grant.
        for grant in grants {
            let class = self
                .root
                .config
                .classes
                .binary_search_by_key(&grant.capacity, |c| c.capacity)
                .expect("validated class");
            if state
                .slots
                .iter()
                .filter(|s| s.class == class && s.assigned.is_none())
                .count()
                < grant.slots
            {
                return Err(Error::ReservationUnavailable);
            }
        }
        if state.free_domains.is_empty() {
            return Err(Error::DomainCapacityExhausted);
        }
        self.root.check_handles(2)?;
        let index = state.free_domains.pop().expect("checked free record");
        let record = &mut state.domains[index];
        // Recycle retires u64::MAX, so this increment cannot wrap.
        record.generation = record
            .generation
            .checked_add(1)
            .expect("unretired generation");
        record.phase = DomainPhase::Active;
        record.limit = limit;
        record.owner = true;
        record.handles = 1;
        let key = DomainIdentity {
            index,
            generation: record.generation,
        };
        for grant in grants {
            let class = self
                .root
                .config
                .classes
                .binary_search_by_key(&grant.capacity, |c| c.capacity)
                .expect("validated class");
            for slot in state
                .slots
                .iter_mut()
                .filter(|s| s.class == class && s.assigned.is_none())
                .take(grant.slots)
            {
                slot.assigned = Some(index);
            }
        }
        Ok((
            DomainOwner {
                root: Arc::clone(&self.root),
                key,
            },
            Domain {
                root: Arc::clone(&self.root),
                key,
            },
        ))
    }

    pub fn snapshot(&self) -> Snapshot {
        self.root.snapshot()
    }
}

/// Unique close authority. Its Drop closes admission even when Domain survives.
pub struct DomainOwner {
    root: Arc<Root>,
    key: DomainIdentity,
}

impl DomainOwner {
    pub fn identity(&self) -> DomainIdentity {
        self.key
    }

    pub fn close(&self) {
        lock(&self.root.state).domain_mut(self.key).phase = DomainPhase::Closing;
    }
}

impl Drop for DomainOwner {
    fn drop(&mut self) {
        let mut state = lock(&self.root.state);
        let record = state.domain_mut(self.key);
        record.phase = DomainPhase::Closing;
        record.owner = false;
        state.recycle_domain(self.key);
    }
}

pub struct Domain {
    root: Arc<Root>,
    key: DomainIdentity,
}

impl Domain {
    pub fn identity(&self) -> DomainIdentity {
        self.key
    }

    pub fn try_clone(&self) -> Result<Self, Error> {
        let mut state = try_lock(&self.root.state)?;
        self.root.check_handles(1)?;
        state.domain_mut(self.key).handles += 1;
        Ok(Self {
            root: Arc::clone(&self.root),
            key: self.key,
        })
    }

    pub fn try_acquire(&self, size: usize) -> Result<BufMut, Error> {
        self.acquire_inner(size, None)
    }

    fn acquire_inner(&self, size: usize, fail_at: Option<Stage>) -> Result<BufMut, Error> {
        if size == 0 {
            return Err(Error::InvalidSize);
        }
        let class = self
            .root
            .config
            .classes
            .iter()
            .position(|c| c.capacity >= size)
            .ok_or(Error::TooLarge)?;
        let charge = self.root.config.classes[class].capacity;
        let mut state = try_lock(&self.root.state)?;
        if state.phase != RootPhase::Active
            || state.domain_mut(self.key).phase != DomainPhase::Active
        {
            return Err(Error::Closed);
        }
        self.root.check_handles(1)?;
        // One mutex serializes both gates, admission, and class claim in this
        // reference only. Admission's destructor rolls back every early return.
        let mut admission = Admission::new(&mut state, self.key, class, charge)?;
        if fail_at == Some(Stage::Charged) {
            return Err(Error::Contended);
        }
        admission.reserve_credit()?;
        if fail_at == Some(Stage::Credit) {
            return Err(Error::Contended);
        }
        admission.claim_slot();
        if fail_at == Some(Stage::Slot) {
            return Err(Error::Contended);
        }
        let allocation_root = Arc::clone(&self.root);
        let (slot, payload) = admission.commit();
        Ok(BufMut {
            allocation: Allocation {
                root: allocation_root,
                key: self.key,
                slot,
                payload: Some(payload),
            },
            filled: 0,
            initialized: 0,
            not_sync: PhantomData,
        })
    }
}

impl Drop for Domain {
    fn drop(&mut self) {
        let mut state = lock(&self.root.state);
        state.domain_mut(self.key).handles -= 1;
        state.recycle_domain(self.key);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Stage {
    Charged,
    Credit,
    Slot,
}

struct Allocation {
    root: Arc<Root>,
    key: DomainIdentity,
    slot: usize,
    payload: Option<Box<[u8]>>,
}

impl Allocation {
    fn bytes(&self) -> &[u8] {
        self.payload.as_ref().expect("owned payload")
    }
    fn bytes_mut(&mut self) -> &mut [u8] {
        self.payload.as_mut().expect("exclusive payload")
    }
}

impl Drop for Allocation {
    fn drop(&mut self) {
        let Some(payload) = self.payload.take() else {
            return;
        };
        let charge = payload.len();
        let mut state = lock(&self.root.state);
        let class = state.slots[self.slot].class;
        // MEM-02/PROG-01: final ownership -> published return -> capacity -> refund.
        // The mutex makes intermediate states unobservable to concurrent calls;
        // it does not implement production helping or bounded nonblocking Drop.
        let domain = state.domain_mut(self.key);
        domain.live -= charge;
        domain.pending_return += charge;
        state.classes[class].issued -= 1;
        state.classes[class].returning += 1;
        let slot = &mut state.slots[self.slot];
        slot.payload = Some(payload);
        slot.origin = None;
        slot.ownership = state::Ownership::Free;
        state.classes[class].returning -= 1;
        state.classes[class].free.push(self.slot);
        let domain = state.domain_mut(self.key);
        domain.pending_return -= charge;
        domain.allocations -= 1;
        state.recycle_domain(self.key);
    }
}

/// Exclusive, Send but not Sync/Clone. Only filled bytes are readable as payload.
///
/// ```compile_fail
/// fn assert_sync<T: Sync>() {}
/// assert_sync::<abps_reference::BufMut>();
/// ```
/// ```compile_fail
/// fn assert_clone<T: Clone>() {}
/// assert_clone::<abps_reference::BufMut>();
/// ```
pub struct BufMut {
    allocation: Allocation,
    filled: usize,
    initialized: usize,
    not_sync: PhantomData<Cell<()>>,
}

impl BufMut {
    pub fn capacity(&self) -> usize {
        self.allocation.bytes().len()
    }
    pub fn filled_len(&self) -> usize {
        self.filled
    }
    pub fn initialized_len(&self) -> usize {
        self.initialized
    }
    pub fn origin(&self) -> DomainIdentity {
        self.allocation.key
    }

    /// Appends an explicit copy, establishing both filled and initialized bytes.
    pub fn put_slice(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let end = self
            .filled
            .checked_add(bytes.len())
            .ok_or(Error::InvalidRange)?;
        if end > self.capacity() {
            return Err(Error::InvalidRange);
        }
        self.allocation.bytes_mut()[self.filled..end].copy_from_slice(bytes);
        self.filled = end;
        self.initialized = self.initialized.max(end);
        Ok(())
    }

    /// Explicit O(n) initialization for generic safe Read/ReadBuf ingress.
    /// Already initialized bytes are preserved; the new working tail is zeroed.
    pub fn initialize_to(&mut self, len: usize) -> Result<&mut [u8], Error> {
        if len > self.capacity() {
            return Err(Error::InvalidRange);
        }
        if len > self.initialized {
            self.allocation.bytes_mut()[self.initialized..len].fill(0);
            self.initialized = len;
        }
        Ok(&mut self.allocation.bytes_mut()[..len])
    }

    /// Cannot establish initialization merely from a caller-provided length.
    pub fn set_filled_len(&mut self, len: usize) -> Result<(), Error> {
        if len > self.initialized {
            return Err(Error::NotInitialized);
        }
        self.filled = len;
        Ok(())
    }

    /// Consumes mutation rights; moves the original Box into an Arc wrapper.
    /// Allocates reference metadata and may block. Does not copy payload bytes.
    pub fn freeze(self) -> Buf {
        let Self {
            allocation, filled, ..
        } = self;
        let allocation = Arc::new(allocation);
        {
            let mut state = lock(&allocation.root.state);
            state.slots[allocation.slot].ownership =
                state::Ownership::Frozen(Arc::downgrade(&allocation));
        }
        Buf {
            allocation,
            range: 0..filled,
        }
    }
}

impl AsRef<[u8]> for BufMut {
    fn as_ref(&self) -> &[u8] {
        &self.allocation.bytes()[..self.filled]
    }
}

/// Immutable ownership; views are fallible and retain the entire backing charge.
pub struct Buf {
    allocation: Arc<Allocation>,
    range: Range<usize>,
}

impl Buf {
    /// SI-10: create a complete batch or leave the destination unchanged.
    /// The reference backend retains its mutex scope exception.
    pub fn try_clone_into(&self, dst: &mut [Option<Buf>]) -> Result<(), Error> {
        if dst.iter().any(Option::is_some) {
            return Err(Error::OutputNotEmpty);
        }
        if dst.is_empty() {
            return Ok(());
        }
        let _state = try_lock(&self.allocation.root.state)?;
        if Arc::strong_count(&self.allocation)
            .checked_add(dst.len())
            .is_none_or(|n| n > self.allocation.root.config.reference_limit)
        {
            return Err(Error::ReferenceLimit);
        }
        // All increments require this mutex; concurrent drops only reduce the
        // count. These concrete Arc/Range constructions cannot call user code.
        for out in dst {
            *out = Some(Self {
                allocation: Arc::clone(&self.allocation),
                range: self.range.clone(),
            });
        }
        Ok(())
    }
    pub fn origin(&self) -> DomainIdentity {
        self.allocation.key
    }
    pub fn len(&self) -> usize {
        self.range.len()
    }
    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }
    pub fn backing_capacity(&self) -> usize {
        self.allocation.bytes().len()
    }
    /// Borrowed descriptor of this view's bytes.
    ///
    /// # Panics
    ///
    /// On Windows when the view exceeds `u32::MAX` bytes, as `IoSlice::new`
    /// does. [`Buf::try_as_io_slice`] reports `TooLarge` instead.
    pub fn as_io_slice(&self) -> IoSlice<'_> {
        IoSlice::new(self.as_ref())
    }
    /// IO-01: `TooLarge` when one platform descriptor cannot cover this view;
    /// split it with `try_slice`. No copy, allocation or ownership change.
    pub fn try_as_io_slice(&self) -> Result<IoSlice<'_>, Error> {
        try_io_slice(self.as_ref())
    }

    pub fn try_clone(&self) -> Result<Self, Error> {
        self.try_slice(0..self.len())
    }

    pub fn try_slice(&self, range: Range<usize>) -> Result<Self, Error> {
        if range.start > range.end || range.end > self.len() {
            return Err(Error::InvalidRange);
        }
        let start = self
            .range
            .start
            .checked_add(range.start)
            .ok_or(Error::InvalidRange)?;
        let end = self
            .range
            .start
            .checked_add(range.end)
            .ok_or(Error::InvalidRange)?;
        let _state = try_lock(&self.allocation.root.state)?;
        if Arc::strong_count(&self.allocation) >= self.allocation.root.config.reference_limit {
            return Err(Error::ReferenceLimit);
        }
        Ok(Self {
            allocation: Arc::clone(&self.allocation),
            range: start..end,
        })
    }
}

impl AsRef<[u8]> for Buf {
    fn as_ref(&self) -> &[u8] {
        &self.allocation.bytes()[self.range.clone()]
    }
}

impl fmt::Debug for Buf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Buf")
            .field("origin", &self.origin())
            .field("len", &self.len())
            .field("backing_capacity", &self.backing_capacity())
            .finish()
    }
}

#[cfg(test)]
mod tests;
