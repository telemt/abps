//! Trusted SI-11..15 storage boundary. The issuer alone accesses Cell fields. Remote
//! buffers access immutable layout, atomics, and their exclusively owned slot.
//! A positive charge protects each allocation through refund; teardown is cold.

use super::protocol::{self, AtomicBool, AtomicUsize, Ordering};
use super::{
    CapacityPolicy, ClassGrantSnapshot, ClassReservation, Config, DomainPhase, Error,
    OwnershipKind, RootPhase,
};
use std::alloc::{Layout, alloc, dealloc};
use std::cell::{Cell, UnsafeCell};
use std::fmt;
use std::io::IoSlice;
use std::marker::PhantomData;
use std::ops::Range;
use std::ptr::NonNull;
use std::rc::Rc;

/// Checked implementation limits, not a future multi-word bitmap promise.
pub const MAX_CLASSES: usize = 64;
pub const MAX_BITMAP_WORDS: usize = 256;
pub const MAX_SLOTS_PER_CLASS: usize = MAX_BITMAP_WORDS * usize::BITS as usize;
const NONE: usize = usize::MAX;
type Local = PhantomData<Rc<()>>;
mod remote;
use remote::RemotePtr;

#[cfg(feature = "local-branches-spike")]
mod branches;
#[cfg(feature = "local-branches-spike")]
pub use branches::{Branch, FreezeBranchesError, LocalBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reservation {
    pub backing_bytes: usize,
    pub metadata_bytes: usize,
    pub arena_bytes: usize,
    pub diagnostic_bytes: usize,
    pub total_bytes: usize,
}
impl Reservation {
    pub fn for_config(config: &Config) -> Result<Self, Error> {
        Ok(Plan::new(config)?.reservation)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DomainIdentity {
    index: usize,
    generation: u64,
}

#[derive(Debug, Clone, Copy)]
struct DomainState {
    generation: u64,
    phase: DomainPhase,
    limit: usize,
    owner: bool,
    handles: usize,
}
impl DomainState {
    fn free() -> Self {
        Self {
            generation: 0,
            phase: DomainPhase::Free,
            limit: 0,
            owner: false,
            handles: 0,
        }
    }
}
struct DomainRecord {
    state: Cell<DomainState>,
    ledger: protocol::DomainLedger,
    // Issuer-only intrusive FIFO; never accessed by remote returns.
    queued: Cell<bool>,
    next: Cell<Option<usize>>,
    grant_row: Cell<usize>,
}
struct Class {
    capacity: usize,
    slots: usize,
    first: usize,
    word_first: usize,
    words: usize,
}
// SI-13: remote bitmap writes do not share the issuer's masks/cursors.
#[repr(align(64))]
struct Word {
    bitmap: protocol::Bitmap,
}
#[derive(Clone, Copy)]
struct GrantEntry {
    head: usize,
    cursor: usize,
    count: usize,
    reserved: usize,
}
impl GrantEntry {
    const EMPTY: Self = Self {
        head: NONE,
        cursor: NONE,
        count: 0,
        reserved: 0,
    };
}
#[derive(Clone, Copy)]
struct MaskNode {
    word: usize,
    mask: usize,
    next: usize,
}
// SI-05: distinct refcounts occupy separate 64-byte metadata units. Layout::array
// and pad_to_align account the complete cost; payload alignment is independent.
#[repr(align(64))]
struct Slot {
    class: usize,
    bit: usize,
    word: usize,
    data_offset: usize,
    origin: UnsafeCell<DomainIdentity>,
    references: protocol::References,
    frozen: AtomicBool,
}
struct Root {
    plan: Plan,
    class_count: usize,
    domain_count: usize,
    capacity_policy: CapacityPolicy,
    reference_limit: usize,
    control_handle_limit: usize,
    phase: Cell<RootPhase>,
    controls: Cell<usize>,
    free_rows: Cell<usize>,
    free_node: Cell<usize>,
    free_nodes: Cell<usize>,
    free_count: Cell<usize>,
    retired: Cell<usize>,
    pending_head: Cell<Option<usize>>,
    pending_tail: Cell<Option<usize>>,
    pending_count: Cell<usize>,
}

#[derive(Clone, Copy)]
struct Plan {
    layout: Layout,
    classes: usize,
    domains: usize,
    slots: usize,
    free_ids: usize,
    grants: usize,
    words: usize,
    unassigned: usize,
    cursors: usize,
    nodes: usize,
    row_ids: usize,
    row_count: usize,
    node_count: usize,
    word_count: usize,
    slot_count: usize,
    #[cfg(feature = "local-branches-spike")]
    branch_records: usize,
    #[cfg(feature = "local-branches-spike")]
    branches_per_slot: usize,
    #[cfg(feature = "local-branches-spike")]
    local_reference_limit: usize,
    payload: [usize; MAX_CLASSES],
    reservation: Reservation,
}
fn layout_error(_: std::alloc::LayoutError) -> Error {
    Error::LayoutOverflow
}
fn extend<T>(layout: Layout, n: usize) -> Result<(Layout, usize), Error> {
    layout
        .extend(Layout::array::<T>(n).map_err(layout_error)?)
        .map_err(layout_error)
}
impl Plan {
    fn new(config: &Config) -> Result<Self, Error> {
        if config.classes.is_empty()
            || config.classes.len() > MAX_CLASSES
            || config.domain_slots == 0
            || !(1..=MAX_BITMAP_WORDS).contains(&config.max_bitmap_words_per_class)
            || !(1..=isize::MAX as usize).contains(&config.reference_limit)
            || !(1..=isize::MAX as usize).contains(&config.control_handle_limit)
        {
            return Err(Error::InvalidConfiguration);
        }
        let mut previous = 0;
        let mut slot_count = 0usize;
        let mut backing_bytes = 0usize;
        let mut word_count = 0usize;
        for class in &config.classes {
            if class.capacity <= previous
                || class.slots == 0
                || class.slots.div_ceil(usize::BITS as usize) > config.max_bitmap_words_per_class
            {
                return Err(Error::InvalidConfiguration);
            }
            previous = class.capacity;
            word_count += class.slots.div_ceil(usize::BITS as usize);
            slot_count = slot_count
                .checked_add(class.slots)
                .ok_or(Error::LayoutOverflow)?;
            backing_bytes = backing_bytes
                .checked_add(
                    class
                        .capacity
                        .checked_mul(class.slots)
                        .ok_or(Error::LayoutOverflow)?,
                )
                .ok_or(Error::LayoutOverflow)?;
        }
        let (layout, classes) = extend::<Class>(Layout::new::<Root>(), config.classes.len())?;
        let (layout, domains) = extend::<DomainRecord>(layout, config.domain_slots)?;
        let (layout, slots) = extend::<Slot>(layout, slot_count)?;
        #[cfg(feature = "local-branches-spike")]
        let (layout, branch_records, branches_per_slot, local_reference_limit) = {
            let (b, l) = config.local_branches.map_or((0, 0), |c| {
                (c.max_branches_per_slot, c.local_reference_limit)
            });
            if config.local_branches.is_some()
                && (!(1..=isize::MAX as usize).contains(&b)
                    || !(1..=isize::MAX as usize).contains(&l))
            {
                return Err(Error::InvalidConfiguration);
            }
            let count = slot_count.checked_mul(b).ok_or(Error::LayoutOverflow)?;
            let (layout, offset) = extend::<branches::BranchRecord>(layout, count)?;
            (layout, offset, b, l)
        };
        let (layout, free_ids) = extend::<usize>(layout, config.domain_slots)?;
        let reserved = config.capacity_policy == CapacityPolicy::Reserved;
        let row_count = if reserved {
            config.domain_slots.min(slot_count)
        } else {
            0
        };
        let node_count = if reserved { slot_count } else { 0 };
        let grant_count = row_count
            .checked_mul(config.classes.len())
            .ok_or(Error::LayoutOverflow)?;
        let (layout, grants) = extend::<Cell<GrantEntry>>(layout, grant_count)?;
        let (layout, row_ids) = extend::<Cell<usize>>(layout, row_count)?;
        let (layout, nodes) = extend::<Cell<MaskNode>>(layout, node_count)?;
        let (layout, words) = extend::<Word>(layout, word_count)?;
        let (layout, unassigned) =
            extend::<Cell<usize>>(layout, if reserved { word_count } else { 0 })?;
        let (mut layout, cursors) = extend::<Cell<usize>>(layout, config.classes.len())?;
        let mut payload = [0; MAX_CLASSES];
        for (index, class) in config.classes.iter().enumerate() {
            let size = class
                .capacity
                .checked_mul(class.slots)
                .ok_or(Error::LayoutOverflow)?;
            (layout, payload[index]) = extend::<u8>(layout, size)?;
        }
        let layout = layout.pad_to_align();
        let diagnostic_bytes = Layout::new::<DiagnosticRecord>().size();
        let total_bytes = layout
            .size()
            .checked_add(diagnostic_bytes)
            .ok_or(Error::LayoutOverflow)?;
        if total_bytes > config.root_limit {
            return Err(Error::RootBudgetExceeded);
        }
        let reservation = Reservation {
            backing_bytes,
            metadata_bytes: total_bytes - backing_bytes,
            arena_bytes: layout.size(),
            diagnostic_bytes,
            total_bytes,
        };
        Ok(Self {
            layout,
            classes,
            domains,
            slots,
            free_ids,
            grants,
            words,
            unassigned,
            cursors,
            nodes,
            row_ids,
            row_count,
            node_count,
            word_count,
            slot_count,
            #[cfg(feature = "local-branches-spike")]
            branch_records,
            #[cfg(feature = "local-branches-spike")]
            branches_per_slot,
            #[cfg(feature = "local-branches-spike")]
            local_reference_limit,
            payload,
            reservation,
        })
    }
}

#[derive(Clone, Copy)]
struct RootPtr(NonNull<Root>);
impl RootPtr {
    fn root(&self) -> &Root {
        // SAFETY LIFE-02: every caller holds the owner, a control claim, or a
        // positive allocation charge. Only drained owner teardown deallocates this Root.
        unsafe { self.0.as_ref() }
    }
    fn class(&self, index: usize) -> &Class {
        let root = self.root();
        assert!(index < root.class_count);
        // SAFETY MEM-01: Plan::new aligned and bounded this initialized array.
        unsafe {
            &*self
                .0
                .as_ptr()
                .cast::<u8>()
                .add(root.plan.classes)
                .cast::<Class>()
                .add(index)
        }
    }
    fn domain(&self, index: usize) -> &DomainRecord {
        let root = self.root();
        assert!(index < root.domain_count);
        // SAFETY MEM-01/LIFE-02: fixed initialized array within the pinned arena.
        unsafe {
            &*self
                .0
                .as_ptr()
                .cast::<u8>()
                .add(root.plan.domains)
                .cast::<DomainRecord>()
                .add(index)
        }
    }
    fn slot(&self, index: usize) -> &Slot {
        let root = self.root();
        assert!(index < root.plan.slot_count);
        // SAFETY MEM-01: layout range/alignment checked before allocation.
        unsafe {
            &*self
                .0
                .as_ptr()
                .cast::<u8>()
                .add(root.plan.slots)
                .cast::<Slot>()
                .add(index)
        }
    }
    fn free_id(&self, index: usize) -> *mut usize {
        assert!(index < self.root().domain_count);
        // SAFETY MEM-01: only issuer control methods dereference this prepared array.
        unsafe {
            self.0
                .as_ptr()
                .cast::<u8>()
                .add(self.root().plan.free_ids)
                .cast::<usize>()
                .add(index)
        }
    }
    /// SI-13/MEM-01: callers select a typed, initialized Plan array and check
    /// its index. Only issuer code accesses Cells; remote paths use Word only.
    unsafe fn at<T>(&self, offset: usize, index: usize) -> &T {
        // SAFETY: private callers prove layout, initialization and bounds; no
        // mutable reference to the arena or neighboring slots is ever formed.
        unsafe {
            &*self
                .0
                .as_ptr()
                .cast::<u8>()
                .add(offset)
                .cast::<T>()
                .add(index)
        }
    }
    fn word(&self, index: usize) -> &Word {
        assert!(index < self.root().plan.word_count);
        // SAFETY SI-12/OWN-03: bounded initialized Word array, atomics only.
        unsafe { self.at(self.root().plan.words, index) }
    }
    fn unassigned(&self, index: usize) -> &Cell<usize> {
        assert!(
            self.root().capacity_policy == CapacityPolicy::Reserved
                && index < self.root().plan.word_count
        );
        // SAFETY SI-13: bounded prepared rights, accessed only by issuer.
        unsafe { self.at(self.root().plan.unassigned, index) }
    }
    fn cursor(&self, index: usize) -> &Cell<usize> {
        assert!(index < self.root().class_count);
        // SAFETY SI-12: one initialized cursor per class, issuer only.
        unsafe { self.at(self.root().plan.cursors, index) }
    }
    fn entry(&self, row: usize, class: usize) -> &Cell<GrantEntry> {
        assert!(row < self.root().plan.row_count && class < self.root().class_count);
        // SAFETY SI-13: checked row/class product and prepared array.
        unsafe {
            self.at(
                self.root().plan.grants,
                row * self.root().class_count + class,
            )
        }
    }
    fn node(&self, index: usize) -> &Cell<MaskNode> {
        assert!(index < self.root().plan.node_count);
        // SAFETY SI-13: nodes are prepared Cells, issuer-only accesses.
        unsafe { self.at(self.root().plan.nodes, index) }
    }
    fn row_id(&self, index: usize) -> &Cell<usize> {
        assert!(index < self.root().plan.row_count);
        // SAFETY SI-13: bounded prepared free-row stack.
        unsafe { self.at(self.root().plan.row_ids, index) }
    }
    fn pin(&self, count: usize) -> Result<(), Error> {
        let root = self.root();
        let n = root
            .controls
            .get()
            .checked_add(count)
            .filter(|&n| n <= root.control_handle_limit)
            .ok_or(Error::ReferenceLimit)?;
        root.controls.set(n);
        Ok(())
    }
    fn unpin(&self) {
        let controls = &self.root().controls;
        let old = controls.get();
        debug_assert!(old > 0, "unpin must own one control right");
        controls.set(old - 1);
    }
    fn drained(&self) -> bool {
        protocol::root_drained(
            self.root().controls.get(),
            (0..self.root().domain_count).map(|i| &self.domain(i).ledger),
        )
    }
    fn grant_metrics(&self, domain: usize, class: usize) -> ClassGrantSnapshot {
        let row = self.domain(domain).grant_row.get();
        let mut result = ClassGrantSnapshot {
            capacity: self.class(class).capacity,
            reserved: 0,
            available: 0,
        };
        if row != NONE {
            let entry = self.entry(row, class).get();
            result.reserved = entry.reserved;
            let mut id = entry.head;
            for _ in 0..entry.count {
                let node = self.node(id).get();
                result.available +=
                    (self.word(node.word).bitmap.load() & node.mask).count_ones() as usize;
                id = node.next;
            }
        }
        result
    }
    fn unassigned_count(&self, class: usize) -> usize {
        let c = self.class(class);
        (0..c.words)
            .map(|i| self.unassigned(c.word_first + i).get().count_ones() as usize)
            .sum()
    }
    fn assign_grants(&self, domain: usize, grants: &[ClassReservation], indices: &[usize]) {
        if grants.is_empty() {
            return;
        }
        let root = self.root();
        let free = root.free_rows.get() - 1;
        let row = self.row_id(free).get();
        root.free_rows.set(free);
        self.domain(domain).grant_row.set(row);
        for (grant, &class) in grants.iter().zip(indices) {
            let c = self.class(class);
            let mut remaining = grant.slots;
            let mut entry = GrantEntry::EMPTY;
            let mut tail = NONE;
            for i in 0..c.words {
                let word = c.word_first + i;
                let free = self.unassigned(word).get();
                let take = remaining.min(free.count_ones() as usize);
                if take == 0 {
                    continue;
                }
                // Whole-word grants avoid a per-slot loop; only a partial word
                // needs <=usize::BITS low-bit selections.
                let mask = if take == free.count_ones() as usize {
                    free
                } else {
                    let mut bits = free;
                    let mut mask = 0;
                    for _ in 0..take {
                        let bit = bits & bits.wrapping_neg();
                        mask |= bit;
                        bits &= !bit;
                    }
                    mask
                };
                let id = root.free_node.get();
                root.free_node.set(self.node(id).get().next);
                root.free_nodes.set(root.free_nodes.get() - 1);
                self.node(id).set(MaskNode {
                    word,
                    mask,
                    next: NONE,
                });
                if tail == NONE {
                    entry.head = id;
                } else {
                    let mut n = self.node(tail).get();
                    n.next = id;
                    self.node(tail).set(n);
                }
                tail = id;
                entry.count += 1;
                entry.reserved += take;
                self.unassigned(word).set(free & !mask);
                remaining -= take;
                if remaining == 0 {
                    break;
                }
            }
            entry.cursor = entry.head;
            self.entry(row, class).set(entry);
        }
    }
    fn release_grants(&self, domain: usize) {
        let row = self.domain(domain).grant_row.replace(NONE);
        if row == NONE {
            return;
        }
        let root = self.root();
        for c in 0..root.class_count {
            let entry = self.entry(row, c).replace(GrantEntry::EMPTY);
            let mut id = entry.head;
            for _ in 0..entry.count {
                let node = self.node(id).get();
                self.unassigned(node.word)
                    .set(self.unassigned(node.word).get() | node.mask);
                self.node(id).set(MaskNode {
                    next: root.free_node.get(),
                    ..node
                });
                root.free_node.set(id);
                root.free_nodes.set(root.free_nodes.get() + 1);
                id = node.next;
            }
        }
        let free = root.free_rows.get();
        self.row_id(free).set(row);
        root.free_rows.set(free + 1);
    }
    fn take(&self, domain: usize, class: usize) -> Result<(usize, usize), Error> {
        let c = self.class(class);
        if self.root().capacity_policy == CapacityPolicy::Unreserved {
            let (i, bit) = protocol::take_words(
                c.words,
                self.cursor(class).get(),
                |i| (&self.word(c.word_first + i).bitmap, usize::MAX),
                Error::ClassDepleted,
            )?;
            // SI-12: keep the winning word for locality; this is not round-robin.
            self.cursor(class).set(i);
            Ok((c.word_first + i, bit))
        } else {
            let row = self.domain(domain).grant_row.get();
            if row == NONE {
                return Err(Error::DomainClassDepleted);
            }
            let mut e = self.entry(row, class).get();
            let mut current = e.cursor;
            let mut last = NONE;
            let (_, bit) = protocol::take_words(
                e.count,
                0,
                |_| {
                    last = current;
                    let node = self.node(current).get();
                    current = if node.next == NONE { e.head } else { node.next };
                    (&self.word(node.word).bitmap, node.mask)
                },
                Error::DomainClassDepleted,
            )?;
            // Same winning-word policy for sparse grant nodes (SI-12/13).
            e.cursor = last;
            self.entry(row, class).set(e);
            Ok((self.node(last).get().word, bit))
        }
    }
    fn close(&self) {
        let root = self.root();
        if matches!(root.phase.get(), RootPhase::Prepared | RootPhase::Active) {
            root.phase.set(RootPhase::Closing);
        }
        for index in 0..root.domain_count {
            let record = self.domain(index);
            let mut state = record.state.get();
            if state.phase == DomainPhase::Active {
                state.phase = DomainPhase::Closing;
                record.state.set(state);
            }
        }
    }
    fn recycle_domain(&self, index: usize) {
        let record = self.domain(index);
        let mut state = record.state.get();
        debug_assert!(!state.owner && state.handles == 0 && !record.queued.get());
        debug_assert_eq!(state.phase, DomainPhase::Closing);
        // SI-05/LIFE-01: callers acquired ledger.drained(), and no controls can
        // admit more work. Publication and refund have both completed. Releasing
        // rights now cannot reassign a slot with an outstanding old owner.
        self.release_grants(index);
        state.limit = 0;
        if state.generation == u64::MAX {
            state.phase = DomainPhase::Retired;
            self.root().retired.set(self.root().retired.get() + 1);
        } else {
            state.phase = DomainPhase::Free;
            let count = self.root().free_count.get();
            // SAFETY LIFE-01/SI-01: only the issuer edits the stack. The last
            // control claim is gone and ledger.drained() acquired completion of
            // all refunds. This unqueued Closing record is pushed exactly once;
            // the array has one entry per record, so count is strictly in bounds.
            unsafe { self.free_id(count).write(index) };
            self.root().free_count.set(count + 1);
        }
        record.state.set(state);
    }
    fn enqueue_domain(&self, index: usize) {
        let root = self.root();
        let record = self.domain(index);
        debug_assert!(!record.queued.get());
        record.queued.set(true);
        record.next.set(None);
        if let Some(tail) = root.pending_tail.get() {
            self.domain(tail).next.set(Some(index));
        } else {
            root.pending_head.set(Some(index));
        }
        root.pending_tail.set(Some(index));
        root.pending_count.set(root.pending_count.get() + 1);
    }
    fn control_dropped(&self, index: usize) {
        let record = self.domain(index);
        let state = record.state.get();
        if state.phase != DomainPhase::Closing
            || state.owner
            || state.handles != 0
            || record.queued.get()
        {
            return;
        }
        if record.ledger.drained() {
            self.recycle_domain(index);
        } else {
            self.enqueue_domain(index);
        }
    }
    fn poll_domains(&self, max_records: usize) -> DomainMaintenance {
        let root = self.root();
        let examined = max_records.min(root.pending_count.get());
        let mut reclaimed = 0;
        for _ in 0..examined {
            let index = root.pending_head.get().expect("pending FIFO is nonempty");
            let record = self.domain(index);
            let next = record.next.replace(None);
            root.pending_head.set(next);
            if next.is_none() {
                root.pending_tail.set(None);
            }
            root.pending_count.set(root.pending_count.get() - 1);
            record.queued.set(false);
            if record.ledger.drained() {
                self.recycle_domain(index);
                reclaimed += 1;
            } else {
                self.enqueue_domain(index);
            }
        }
        DomainMaintenance {
            examined,
            reclaimed,
            pending: root.pending_count.get(),
        }
    }
    fn state(&self, key: DomainIdentity) -> DomainState {
        let state = self.domain(key.index).state.get();
        assert_eq!(state.generation, key.generation, "origin remains pinned");
        state
    }
    fn destroy(self) {
        let layout = self.root().plan.layout;
        // SAFETY LIFE-02: caller is the closed sole owner, controls==1 and every domain admitted==0 observed
        // with Acquire (SI-11). No buffers/control handles can still access any record.
        // Arena fields have no heap ownership or destructors; payload may be uninit.
        unsafe {
            dealloc(self.0.as_ptr().cast(), layout);
        }
    }
}

struct DiagnosticRecord {
    references: protocol::References,
    root_identity: usize,
    reservation: Reservation,
    abandoned: AtomicBool,
    destroyed: AtomicBool,
    outstanding: AtomicUsize,
    admitted: AtomicUsize,
    admitted_saturated: AtomicBool,
}
/// Independent bounded observer. Clone/release are cold monitoring operations.
pub struct Diagnostics {
    ptr: NonNull<DiagnosticRecord>,
}
// SAFETY LIFE-02: observers share only immutable fields and atomics; checked
// references keep the independently allocated record alive through every access.
unsafe impl Send for Diagnostics {}
// SAFETY LIFE-02: no observer can mutate a non-atomic field after publication.
unsafe impl Sync for Diagnostics {}
#[derive(Debug, Clone, Copy)]
pub struct DiagnosticsSnapshot {
    pub abps_abandoned_roots: usize,
    pub abps_abandoned_bytes: usize,
    pub root_identity: usize,
    pub outstanding_controls: usize,
    pub admitted_bytes: usize,
    pub admitted_saturated: bool,
    pub destroyed: bool,
}
impl Diagnostics {
    fn record(&self) -> &DiagnosticRecord {
        // SAFETY LIFE-02: this observer owns a checked reference to the record.
        unsafe { self.ptr.as_ref() }
    }
    pub fn try_clone(&self) -> Result<Self, Error> {
        self.record().references.try_clone(isize::MAX as usize)?;
        Ok(Self { ptr: self.ptr })
    }
    pub fn snapshot(&self) -> DiagnosticsSnapshot {
        let record = self.record();
        let abandoned = record.abandoned.load(Ordering::Acquire);
        DiagnosticsSnapshot {
            abps_abandoned_roots: usize::from(abandoned),
            abps_abandoned_bytes: if abandoned {
                record.reservation.total_bytes
            } else {
                0
            },
            root_identity: record.root_identity,
            outstanding_controls: if abandoned {
                record.outstanding.load(Ordering::Relaxed)
            } else {
                0
            },
            admitted_bytes: if abandoned {
                record.admitted.load(Ordering::Relaxed)
            } else {
                0
            },
            admitted_saturated: abandoned && record.admitted_saturated.load(Ordering::Relaxed),
            destroyed: record.destroyed.load(Ordering::Acquire),
        }
    }
}
impl Drop for Diagnostics {
    fn drop(&mut self) {
        if self.record().references.release() {
            // SAFETY LIFE-02: final observer, acquire fence completed. No arena
            // points here and no field needs Drop. This is a cold observer free.
            unsafe {
                dealloc(self.ptr.as_ptr().cast(), Layout::new::<DiagnosticRecord>());
            }
        }
    }
}

/// SI-17 / OWN-03: caller owns an exclusively writable, checked payload range.
/// No handles exist; do not form a reference over neighboring slots/metadata.
unsafe fn prepare_payload(mut ptr: *mut u8, mut len: usize) {
    let word = std::mem::size_of::<usize>();
    while len != 0 && !ptr.addr().is_multiple_of(word) {
        // SAFETY SI-17: this byte is inside the remaining exclusive range.
        unsafe {
            ptr.write_volatile(0);
            ptr = ptr.add(1);
        }
        len -= 1;
    }
    while len >= word {
        // SAFETY SI-17: aligned, full word fits; no initialized value is read.
        unsafe {
            ptr.cast::<usize>().write_volatile(0);
            ptr = ptr.add(word);
        }
        len -= word;
    }
    while len != 0 {
        // SAFETY SI-17: only the checked tail remains, excluding layout padding.
        unsafe {
            ptr.write_volatile(0);
            ptr = ptr.add(1);
        }
        len -= 1;
    }
}

/// Worker-local close/teardown authority. Config is borrowed only during prepare.
///
/// ```compile_fail
/// fn send<T: Send>() {}
/// send::<abps_reference::mosaic::StorageOwner>();
/// ```
pub struct StorageOwner {
    root: Option<RootPtr>,
    diagnostics: Diagnostics,
    local: Local,
}
impl StorageOwner {
    pub fn prepare(config: &Config) -> Result<Self, Error> {
        let plan = Plan::new(config)?;
        // SAFETY MEM-01: nonzero, checked Layout; no references exist yet.
        let memory = NonNull::new(unsafe { alloc(plan.layout) }).ok_or(Error::AllocationFailed)?;
        // SAFETY MEM-01: fixed nonzero layout, included in the checked root limit.
        let diagnostic = NonNull::new(unsafe { alloc(Layout::new::<DiagnosticRecord>()) });
        let Some(diagnostic) = diagnostic else {
            // SAFETY MEM-01: still uninitialized, uniquely owned allocation.
            unsafe {
                dealloc(memory.as_ptr(), plan.layout);
            }
            return Err(Error::AllocationFailed);
        };
        let ptr = RootPtr(memory.cast());
        // SAFETY MEM-01/OWN-03: initialize only the typed metadata ranges derived
        // by Layout::extend, before any handle exists. No whole-arena &mut is
        // formed, and payload ranges stay uninitialized. Constructors below do
        // not allocate or invoke user code; all arithmetic was checked by Plan.
        unsafe {
            ptr.0.as_ptr().write(Root {
                plan,
                class_count: config.classes.len(),
                domain_count: config.domain_slots,
                capacity_policy: config.capacity_policy,
                reference_limit: config.reference_limit,
                control_handle_limit: config.control_handle_limit,
                phase: Cell::new(RootPhase::Prepared),
                controls: Cell::new(1),
                free_rows: Cell::new(plan.row_count),
                free_node: Cell::new(if plan.node_count == 0 { NONE } else { 0 }),
                free_nodes: Cell::new(plan.node_count),
                free_count: Cell::new(config.domain_slots),
                retired: Cell::new(0),
                pending_head: Cell::new(None),
                pending_tail: Cell::new(None),
                pending_count: Cell::new(0),
            });
            let mut first = 0;
            let mut word_first = 0;
            for (index, config) in config.classes.iter().enumerate() {
                memory
                    .as_ptr()
                    .add(plan.classes)
                    .cast::<Class>()
                    .add(index)
                    .write(Class {
                        capacity: config.capacity,
                        slots: config.slots,
                        first,
                        word_first,
                        words: config.slots.div_ceil(usize::BITS as usize),
                    });
                memory
                    .as_ptr()
                    .add(plan.cursors)
                    .cast::<Cell<usize>>()
                    .add(index)
                    .write(Cell::new(0));
                let words = config.slots.div_ceil(usize::BITS as usize);
                for i in 0..words {
                    let bits = (config.slots - i * usize::BITS as usize).min(usize::BITS as usize);
                    memory
                        .as_ptr()
                        .add(plan.words)
                        .cast::<Word>()
                        .add(word_first + i)
                        .write(Word {
                            bitmap: protocol::Bitmap::new(bits),
                        });
                    if plan.node_count != 0 {
                        memory
                            .as_ptr()
                            .add(plan.unassigned)
                            .cast::<Cell<usize>>()
                            .add(word_first + i)
                            .write(Cell::new(usize::MAX >> (usize::BITS as usize - bits)));
                    }
                }
                for slot in 0..config.slots {
                    memory
                        .as_ptr()
                        .add(plan.slots)
                        .cast::<Slot>()
                        .add(first + slot)
                        .write(Slot {
                            class: index,
                            bit: 1usize << (slot % usize::BITS as usize),
                            word: word_first + slot / usize::BITS as usize,
                            data_offset: plan.payload[index] + config.capacity * slot,
                            origin: UnsafeCell::new(DomainIdentity {
                                index: 0,
                                generation: 0,
                            }),
                            references: protocol::References::new(),
                            frozen: AtomicBool::new(false),
                        });
                }
                first += config.slots;
                word_first += words;
            }
            #[cfg(feature = "local-branches-spike")]
            for index in 0..plan.slot_count * plan.branches_per_slot {
                // AP-06/BR-04: slots/classes are initialized, arithmetic was
                // checked by Plan. Immutable per-family descriptors point only
                // into this allocation; no payload reference is formed.
                let slot_index = index / plan.branches_per_slot;
                let slot = ptr.slot(slot_index);
                let record = branches::BranchRecord {
                    references: protocol::LocalReferences::new(),
                    root: RemotePtr::from_issuer(ptr),
                    slot: slot_index,
                    data: NonNull::new_unchecked(memory.as_ptr().add(slot.data_offset)),
                    capacity: ptr.class(slot.class).capacity,
                    limit: plan.local_reference_limit,
                };
                memory
                    .as_ptr()
                    .add(plan.branch_records)
                    .cast::<branches::BranchRecord>()
                    .add(index)
                    .write(record);
            }
            for index in 0..config.domain_slots {
                memory
                    .as_ptr()
                    .add(plan.domains)
                    .cast::<DomainRecord>()
                    .add(index)
                    .write(DomainRecord {
                        state: Cell::new(DomainState::free()),
                        ledger: protocol::DomainLedger::new(),
                        queued: Cell::new(false),
                        next: Cell::new(None),
                        grant_row: Cell::new(NONE),
                    });
                ptr.free_id(index).write(index);
            }
            for row in 0..plan.row_count {
                memory
                    .as_ptr()
                    .add(plan.row_ids)
                    .cast::<Cell<usize>>()
                    .add(row)
                    .write(Cell::new(row));
                for class in 0..config.classes.len() {
                    memory
                        .as_ptr()
                        .add(plan.grants)
                        .cast::<Cell<GrantEntry>>()
                        .add(row * config.classes.len() + class)
                        .write(Cell::new(GrantEntry::EMPTY));
                }
            }
            for node in 0..plan.node_count {
                memory
                    .as_ptr()
                    .add(plan.nodes)
                    .cast::<Cell<MaskNode>>()
                    .add(node)
                    .write(Cell::new(MaskNode {
                        word: 0,
                        mask: 0,
                        next: if node + 1 < plan.node_count {
                            node + 1
                        } else {
                            NONE
                        },
                    }));
            }
            diagnostic
                .as_ptr()
                .cast::<DiagnosticRecord>()
                .write(DiagnosticRecord {
                    references: protocol::References::new(),
                    root_identity: memory.as_ptr().addr(),
                    reservation: plan.reservation,
                    abandoned: AtomicBool::new(false),
                    destroyed: AtomicBool::new(false),
                    outstanding: AtomicUsize::new(0),
                    admitted: AtomicUsize::new(0),
                    admitted_saturated: AtomicBool::new(false),
                });
        }
        if config.preparation == super::PreparationPolicy::ZeroPayload {
            for (index, class) in config.classes.iter().enumerate() {
                // SAFETY SI-17/MEM-01/OWN-03: Plan checked capacity*slots and
                // distinct payload offsets. Prepared -> owned: no handles exist,
                // these ranges exclude all metadata, with no whole-slab borrow.
                unsafe {
                    prepare_payload(
                        memory.as_ptr().add(plan.payload[index]),
                        class.capacity * class.slots,
                    );
                }
            }
        }
        let diagnostics = Diagnostics {
            ptr: diagnostic.cast(),
        };
        diagnostics.record().references.begin();
        Ok(Self {
            root: Some(ptr),
            diagnostics,
            local: PhantomData,
        })
    }
    pub fn reservation(&self) -> Result<Reservation, Error> {
        Ok(self.root.ok_or(Error::Destroyed)?.root().plan.reservation)
    }
    pub fn activate(&self) -> Result<Pool, Error> {
        let ptr = self.root.ok_or(Error::Destroyed)?;
        match ptr.root().phase.get() {
            RootPhase::Prepared => {}
            RootPhase::Active => return Err(Error::AlreadyActive),
            _ => return Err(Error::Closed),
        }
        ptr.pin(1)?;
        ptr.root().phase.set(RootPhase::Active);
        Ok(Pool {
            root: ptr,
            local: PhantomData,
        })
    }
    pub fn diagnostics(&self) -> Result<Diagnostics, Error> {
        self.diagnostics.try_clone()
    }
    pub fn close(&self) -> Result<(), Error> {
        self.root.ok_or(Error::Destroyed)?.close();
        Ok(())
    }
    pub fn snapshot(&self) -> Result<Snapshot, Error> {
        Ok(snapshot(self.root.ok_or(Error::Destroyed)?))
    }
    /// Cold owner-side work: closes all prepared domains before checking drain.
    /// O(domain capacity), plus allocator-dependent physical teardown on success.
    pub fn try_destroy(&mut self) -> Result<(), Error> {
        let ptr = self.root.ok_or(Error::Destroyed)?;
        ptr.close();
        if !ptr.drained() {
            return Err(Error::Busy);
        }
        self.root = None;
        ptr.destroy();
        self.diagnostics
            .record()
            .destroyed
            .store(true, Ordering::Release);
        Ok(())
    }
}
impl Drop for StorageOwner {
    fn drop(&mut self) {
        let Some(ptr) = self.root.take() else {
            return;
        };
        ptr.close();
        let claims = ptr.root().controls.get();
        if ptr.drained() {
            ptr.destroy();
            self.diagnostics
                .record()
                .destroyed
                .store(true, Ordering::Release);
        } else {
            ptr.root().phase.set(RootPhase::Abandoned);
            let mut saturated = false;
            let admitted = (0..ptr.root().domain_count).fold(0usize, |sum, i| {
                sum.checked_add(ptr.domain(i).ledger.admitted.load(Ordering::Acquire))
                    .unwrap_or_else(|| {
                        saturated = true;
                        usize::MAX
                    })
            });
            let record = self.diagnostics.record();
            record.outstanding.store(claims - 1, Ordering::Relaxed);
            record.admitted.store(admitted, Ordering::Relaxed);
            record
                .admitted_saturated
                .store(saturated, Ordering::Relaxed);
            record.abandoned.store(true, Ordering::Release);
            // Retain the owner control permanently. No buffer can inherit teardown.
        }
    }
}

/// Worker-local admission handle; shared buffer ownership is separate.
/// ```compile_fail
/// fn sync<T: Sync>() {}
/// sync::<abps_reference::mosaic::Pool>();
/// ```
pub struct Pool {
    root: RootPtr,
    local: Local,
}
impl Pool {
    pub fn try_clone(&self) -> Result<Self, Error> {
        self.root.pin(1)?;
        Ok(Self {
            root: self.root,
            local: PhantomData,
        })
    }
    pub fn try_open_domain(&self, limit: usize) -> Result<(DomainOwner, Domain), Error> {
        if self.root.root().capacity_policy != CapacityPolicy::Unreserved {
            return Err(Error::InvalidConfiguration);
        }
        self.open_inner(limit, &[], &[])
    }
    pub fn try_open_reserved_domain(
        &self,
        limit: usize,
        grants: &[ClassReservation],
    ) -> Result<(DomainOwner, Domain), Error> {
        if self.root.root().capacity_policy != CapacityPolicy::Reserved
            || grants.len() > self.root.root().class_count
        {
            return Err(Error::InvalidConfiguration);
        }
        let mut indices = [0; 64];
        super::grants::resolve(
            grants,
            self.root.root().class_count,
            |i| self.root.class(i).capacity,
            &mut indices,
        )?;
        self.open_inner(limit, grants, &indices[..grants.len()])
    }
    fn open_inner(
        &self,
        limit: usize,
        grants: &[ClassReservation],
        indices: &[usize],
    ) -> Result<(DomainOwner, Domain), Error> {
        if self.root.root().phase.get() != RootPhase::Active {
            return Err(Error::Closed);
        }
        // SI-13: validate public failures before mutation. The prepared pools
        // suffice by nodes <= assigned slots and rows <= nonempty domains;
        // this is an infallible commit from valid private state, not recovery
        // from a corrupted freelist or an injected panic inside the commit.
        for (grant, &index) in grants.iter().zip(indices) {
            if grant.slots > self.root.unassigned_count(index) {
                return Err(Error::ReservationUnavailable);
            }
        }
        let count = self.root.root().free_count.get();
        if count == 0 {
            return Err(if self.root.root().pending_count.get() != 0 {
                Error::DomainMaintenanceRequired
            } else {
                Error::DomainCapacityExhausted
            });
        }
        if !grants.is_empty() {
            // Check the pool sufficiency proof before pinning controls or
            // changing any record. No freelist scan and no release-build work.
            debug_assert!(self.root.root().free_rows.get() > 0, "SI-13 free row");
            debug_assert!(
                self.root.root().free_nodes.get() >= grants.iter().map(|g| g.slots).sum::<usize>(),
                "SI-13 free nodes cover requested rights"
            );
        }
        self.root.pin(2)?;
        // SAFETY LIFE-01: the issuer owns this prepared stack and its valid prefix.
        let index = unsafe { self.root.free_id(count - 1).read() };
        self.root.root().free_count.set(count - 1);
        let record = self.root.domain(index);
        let generation = record.state.get().generation + 1; // exhausted records retire
        record.state.set(DomainState {
            generation,
            phase: DomainPhase::Active,
            limit,
            owner: true,
            handles: 1,
        });
        self.root.assign_grants(index, grants, indices);
        let key = DomainIdentity { index, generation };
        Ok((
            DomainOwner {
                root: self.root,
                key,
                local: PhantomData,
            },
            Domain {
                root: self.root,
                key,
                local: PhantomData,
            },
        ))
    }
    pub fn metrics(&self) -> Metrics {
        let root = self.root.root();
        Metrics {
            phase: root.phase.get(),
            reservation: root.plan.reservation,
            total_slots: root.plan.slot_count,
            control_handles: root.controls.get(),
            pending_domain_records: root.pending_count.get(),
        }
    }
    pub fn snapshot(&self) -> Snapshot {
        snapshot(self.root)
    }
    /// SI-07: sample a consecutive page, including free records, without
    /// allocation, slot scans, or maintenance. Only the written prefix changes.
    /// Separate fields/pages are observations, never admission evidence.
    pub fn domain_metrics_page(
        &self,
        start: usize,
        dst: &mut [DomainMetrics],
    ) -> Result<MetricsPage, Error> {
        domain_metrics_page(start, dst, self.root.root().domain_count, |index| {
            domain_metrics(self.root, index)
        })
    }
    /// SI-14: consecutive bitmap words, not a coherent class snapshot.
    pub fn bitmap_metrics_page(
        &self,
        start: usize,
        dst: &mut [BitmapMetrics],
    ) -> Result<MetricsPage, Error> {
        let count = self.root.root().plan.word_count;
        if start > count {
            return Err(Error::InvalidRange);
        }
        let written = dst.len().min(count - start);
        for (i, out) in dst[..written].iter_mut().enumerate() {
            *out = BitmapMetrics {
                word_index: start + i,
                available: self.root.word(start + i).bitmap.load().count_ones() as usize,
            };
        }
        Ok(MetricsPage {
            written,
            next_index: start + written,
            done: start + written == count,
        })
    }
    /// Inspect at most `max_records` entry-time candidates; busy records rotate.
    /// This never returns payload or refunds admission: final buffer Drop does so.
    pub fn poll_domains(&self, max_records: usize) -> DomainMaintenance {
        self.root.poll_domains(max_records)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DomainMaintenance {
    pub examined: usize,
    /// Completed candidates, including generation-exhausted records retired.
    pub reclaimed: usize,
    pub pending: usize,
}
impl Drop for Pool {
    fn drop(&mut self) {
        self.root.unpin();
    }
}

pub struct DomainOwner {
    root: RootPtr,
    key: DomainIdentity,
    local: Local,
}
impl DomainOwner {
    pub fn identity(&self) -> DomainIdentity {
        self.key
    }
    pub fn close(&self) {
        let mut state = self.root.state(self.key);
        state.phase = DomainPhase::Closing;
        self.root.domain(self.key.index).state.set(state);
    }
}
impl Drop for DomainOwner {
    fn drop(&mut self) {
        let mut state = self.root.state(self.key);
        state.owner = false;
        state.phase = DomainPhase::Closing;
        self.root.domain(self.key.index).state.set(state);
        self.root.control_dropped(self.key.index);
        self.root.unpin();
    }
}

/// Admission cannot migrate across worker threads.
/// ```compile_fail
/// fn send<T: Send>() {}
/// send::<abps_reference::mosaic::Domain>();
/// ```
pub struct Domain {
    root: RootPtr,
    key: DomainIdentity,
    local: Local,
}
impl Domain {
    pub fn identity(&self) -> DomainIdentity {
        self.key
    }
    pub fn try_clone(&self) -> Result<Self, Error> {
        self.root.pin(1)?;
        let mut state = self.root.state(self.key);
        state.handles += 1; // bounded above by checked control count
        self.root.domain(self.key.index).state.set(state);
        Ok(Self {
            root: self.root,
            key: self.key,
            local: PhantomData,
        })
    }
    pub fn metrics(&self) -> DomainMetrics {
        domain_metrics(self.root, self.key.index)
    }
    /// SI-07: sample all class grants into prepared caller storage. Unreserved
    /// domains write nothing. A short destination fails without changing it.
    /// Work: O(classes + visited mask nodes).
    pub fn class_grants_into(&self, dst: &mut [ClassGrantSnapshot]) -> Result<usize, Error> {
        let root = self.root.root();
        if root.capacity_policy == CapacityPolicy::Unreserved {
            return Ok(0);
        }
        if dst.len() < root.class_count {
            return Err(Error::InvalidSize);
        }
        for (index, out) in dst[..root.class_count].iter_mut().enumerate() {
            *out = self.root.grant_metrics(self.key.index, index);
        }
        Ok(root.class_count)
    }
    pub fn try_acquire(&self, size: usize) -> Result<BufMut, Error> {
        self.acquire(
            size,
            #[cfg(test)]
            None,
        )
    }
    fn acquire(&self, size: usize, #[cfg(test)] fail: Option<Fault>) -> Result<BufMut, Error> {
        if size == 0 {
            return Err(Error::InvalidSize);
        }
        let mut low = 0;
        let mut high = self.root.root().class_count;
        while low < high {
            let middle = low + (high - low) / 2;
            if self.root.class(middle).capacity < size {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        if low == self.root.root().class_count {
            return Err(Error::TooLarge);
        }
        let state = self.root.state(self.key);
        if self.root.root().phase.get() != RootPhase::Active || state.phase != DomainPhase::Active {
            return Err(Error::Closed);
        }
        let class = self.root.class(low);
        let mut guard = Admission {
            root: self.root,
            key: self.key,
            class: low,
            charge: 0,
            bit: None,
            committed: false,
        };
        self.root
            .domain(self.key.index)
            .ledger
            .reserve(class.capacity, state.limit)?;
        guard.charge = class.capacity;
        #[cfg(test)]
        inject(fail, Stage::Charged)?;
        let (word, bit) = self.root.take(self.key.index, low)?;
        guard.bit = Some((word, bit));
        #[cfg(test)]
        inject(fail, Stage::Bit)?;
        let index = class.first
            + (word - class.word_first) * usize::BITS as usize
            + bit.trailing_zeros() as usize;
        let slot = self.root.slot(index);
        // SAFETY OWN-03/SI-01: acquiring this bit grants exclusive slot metadata
        // access. The prior last Drop copied origin before its release OR and
        // never accesses it afterwards; this issuer is its only writer.
        unsafe {
            slot.origin.get().write(self.key);
        }
        #[cfg(test)]
        inject(fail, Stage::Origin)?;
        slot.frozen.store(false, Ordering::Relaxed);
        slot.references.begin();
        #[cfg(test)]
        inject(fail, Stage::Ready)?;
        guard.committed = true;
        Ok(BufMut {
            lease: Lease {
                root: RemotePtr::from_issuer(self.root),
                slot: index,
            },
            filled: 0,
            initialized: 0,
            not_sync: PhantomData,
        })
    }
}
impl Drop for Domain {
    fn drop(&mut self) {
        let mut state = self.root.state(self.key);
        state.handles -= 1;
        self.root.domain(self.key.index).state.set(state);
        self.root.control_dropped(self.key.index);
        self.root.unpin();
    }
}
// MEM-02/OWN-03: injection is private, after RAII protection, and absent
// from ordinary builds. It executes the production transition, never a callback.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    Charged,
    Bit,
    Origin,
    Ready,
}
#[cfg(test)]
#[derive(Clone, Copy)]
enum Fault {
    Error(Stage),
    Panic(Stage),
}
#[cfg(test)]
fn inject(fault: Option<Fault>, stage: Stage) -> Result<(), Error> {
    match fault {
        Some(Fault::Error(at)) if at == stage => Err(Error::Contended),
        Some(Fault::Panic(at)) if at == stage => panic!("test-only admission fault at {stage:?}"),
        _ => Ok(()),
    }
}
struct Admission {
    root: RootPtr,
    key: DomainIdentity,
    class: usize,
    charge: usize,
    bit: Option<(usize, usize)>,
    committed: bool,
}
impl Drop for Admission {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        if let Some((word, bit)) = self.bit {
            let class = self.root.class(self.class);
            let index = class.first
                + (word - class.word_first) * usize::BITS as usize
                + bit.trailing_zeros() as usize;
            self.root
                .slot(index)
                .references
                .0
                .store(0, Ordering::Relaxed);
            self.root.word(word).bitmap.publish(bit);
        }
        if self.charge != 0 {
            self.root.domain(self.key.index).ledger.refund(self.charge);
        }
    }
}

struct Lease {
    root: RemotePtr,
    slot: usize,
}
// SAFETY OWN-03: a token pins one positive allocation charge. Only exclusive BufMut writes bytes;
// immutable Buf shares initialized bytes. Remote token operations touch immutable
// layout, atomics, and the origin protected by the slot's read rights. RemotePtr
// has no issuer accessors and never constructs &Root or &DomainRecord.
unsafe impl Send for Lease {}
// SAFETY OWN-03: sharing a token permits reads/refcount operations only; mutable
// buffer access needs &mut BufMut, and BufMut separately prevents Sync.
unsafe impl Sync for Lease {}
impl Lease {
    fn slot(&self) -> &Slot {
        self.root.slot(self.slot)
    }
    fn capacity(&self) -> usize {
        self.root.class(self.slot().class).capacity
    }
    fn origin(&self) -> DomainIdentity {
        // SAFETY MEM-03: a live handle or the exclusive final-return right
        // prevents bit publication/reuse during this read. Origin was installed
        // before exposure; the final return copies it before publishing the bit.
        unsafe { *self.slot().origin.get() }
    }
    fn data(&self) -> *mut u8 {
        self.root.data(self.slot)
    }
    fn try_clone(&self) -> Result<Self, Error> {
        self.slot()
            .references
            .try_clone(self.root.reference_limit())?;
        Ok(Self {
            root: self.root,
            slot: self.slot,
        })
    }
    fn begin_return(&self) -> Option<protocol::Return<'_>> {
        let slot = self.slot();
        if !slot.references.release() {
            return None;
        }
        // Capture every operand BEFORE publication. These records, unlike slot
        // metadata, remain valid until the positive admitted charge is refunded.
        let origin = self.origin();
        let class = self.root.class(slot.class);
        Some(protocol::Return::new(
            &self.root.word(slot.word).bitmap,
            self.root.ledger(origin.index),
            slot.bit,
            class.capacity,
        ))
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        if let Some(ticket) = self.begin_return() {
            ticket.publish().finish();
        }
    }
}

/// Exclusive lease; only initialized and filled bytes are observable.
/// ```compile_fail
/// fn sync<T: Sync>() {}
/// sync::<abps_reference::mosaic::BufMut>();
/// ```
/// ```compile_fail
/// fn clone<T: Clone>() {}
/// clone::<abps_reference::mosaic::BufMut>();
/// ```
pub struct BufMut {
    lease: Lease,
    filled: usize,
    initialized: usize,
    not_sync: PhantomData<Cell<()>>,
}
// SAFETY OWN-01/03, SI-02: remaining_mut is the exact in-slot writable tail.
// chunk_mut borrows that exclusive tail as UninitSlice, whose safe operations
// cannot de-initialize bytes. The unsafe caller of advance_mut must initialize
// the advanced prefix; checked advancement commits only that prefix. No method
// grows storage or exposes spare bytes through a readable slice.
#[cfg(feature = "bytes-interop")]
unsafe impl bytes::BufMut for BufMut {
    fn remaining_mut(&self) -> usize {
        self.capacity() - self.filled
    }
    fn chunk_mut(&mut self) -> &mut bytes::buf::UninitSlice {
        let remaining = self.capacity() - self.filled;
        // SAFETY OWN-03: exclusive borrow of this slot, including the valid
        // one-past-end pointer for an empty chunk. No whole-slab reference.
        unsafe {
            bytes::buf::UninitSlice::from_raw_parts_mut(
                self.lease.data().add(self.filled),
                remaining,
            )
        }
    }
    unsafe fn advance_mut(&mut self, count: usize) {
        assert!(
            count <= self.capacity() - self.filled,
            "advance beyond slot"
        );
        // OWN-01: the unsafe trait caller guarantees initialization of these
        // bytes. Bounds are checked here independently, including overflow.
        self.filled += count;
        self.initialized = self.initialized.max(self.filled);
    }
}
impl BufMut {
    pub fn capacity(&self) -> usize {
        self.lease.capacity()
    }
    pub fn filled_len(&self) -> usize {
        self.filled
    }
    pub fn initialized_len(&self) -> usize {
        self.initialized
    }
    pub fn origin(&self) -> DomainIdentity {
        self.lease.origin()
    }
    pub fn put_slice(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let end = self
            .filled
            .checked_add(bytes.len())
            .filter(|n| *n <= self.capacity())
            .ok_or(Error::InvalidRange)?;
        // SAFETY OWN-01/03: unique mutable lease, checked in-slot destination,
        // source is initialized and cannot alias this exclusive lease in safe Rust.
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.lease.data().add(self.filled),
                bytes.len(),
            );
        }
        self.filled = end;
        self.initialized = self.initialized.max(end);
        Ok(())
    }
    pub fn initialize_to(&mut self, len: usize) -> Result<&mut [u8], Error> {
        if len > self.capacity() {
            return Err(Error::InvalidRange);
        }
        if len > self.initialized {
            // SAFETY OWN-01: only the new working tail is written; range checked,
            // exclusively owned. No stale byte is exposed as initialized data.
            unsafe {
                std::ptr::write_bytes(
                    self.lease.data().add(self.initialized),
                    0,
                    len - self.initialized,
                );
            }
            self.initialized = len;
        }
        // SAFETY OWN-01: this lease established initialization for [0,len), and
        // the returned mutable borrow prevents any overlapping lease operation.
        Ok(unsafe { std::slice::from_raw_parts_mut(self.lease.data(), len) })
    }
    pub fn set_filled_len(&mut self, len: usize) -> Result<(), Error> {
        if len > self.initialized {
            return Err(Error::NotInitialized);
        }
        self.filled = len;
        Ok(())
    }
    pub fn freeze(self) -> Buf {
        let Self { lease, filled, .. } = self;
        lease.slot().frozen.store(true, Ordering::Release);
        Buf {
            lease,
            range: 0..filled,
        }
    }
}
impl AsRef<[u8]> for BufMut {
    fn as_ref(&self) -> &[u8] {
        // SAFETY OWN-01: filled<=initialized<=capacity, the immutable borrow
        // excludes mutation, and lease ownership pins this initialized prefix.
        unsafe { std::slice::from_raw_parts(self.lease.data(), self.filled) }
    }
}

/// Immutable views share only their slot's checked refcount, never a root Arc.
pub struct Buf {
    lease: Lease,
    range: Range<usize>,
}
/// Borrowed initialized bytes. Copies retain no independent ownership.
/// ```compile_fail
/// use abps_reference::mosaic::{Buf, BufView};
/// fn escape(buf: Buf) -> BufView<'static> { buf.try_view(0..0).unwrap() }
/// ```
#[derive(Clone, Copy, Debug)]
pub struct BufView<'a>(&'a [u8]);
impl AsRef<[u8]> for BufView<'_> {
    fn as_ref(&self) -> &[u8] {
        self.0
    }
}
impl BufView<'_> {
    pub fn as_io_slice(&self) -> IoSlice<'_> {
        IoSlice::new(self.0)
    }
}
impl Buf {
    /// SI-14: checked borrow, with no ownership/refcount operation.
    pub fn try_view(&self, range: Range<usize>) -> Result<BufView<'_>, Error> {
        self.as_ref()
            .get(range)
            .map(BufView)
            .ok_or(Error::InvalidRange)
    }

    /// SI-10: one CAS reserves an entire batch of identical views into prepared
    /// empty output. Errors leave the output unchanged; no implicit retries.
    pub fn try_clone_into(&self, dst: &mut [Option<Buf>]) -> Result<(), Error> {
        if dst.iter().any(Option::is_some) {
            return Err(Error::OutputNotEmpty);
        }
        if dst.is_empty() {
            return Ok(());
        }
        if dst.len() == 1 {
            dst[0] = Some(self.try_clone()?);
            return Ok(());
        }
        let mut batch = BatchBuild::new(self, dst)?;
        while batch.built < batch.dst.len() {
            batch.materialize();
        }
        batch.complete = true;
        Ok(())
    }
    pub fn len(&self) -> usize {
        self.range.len()
    }
    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }
    pub fn origin(&self) -> DomainIdentity {
        self.lease.origin()
    }
    pub fn backing_capacity(&self) -> usize {
        self.lease.capacity()
    }
    pub fn as_io_slice(&self) -> IoSlice<'_> {
        IoSlice::new(self.as_ref())
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
        let lease = self.lease.try_clone()?;
        Ok(Self {
            lease,
            range: start..end,
        })
    }
}
impl AsRef<[u8]> for Buf {
    fn as_ref(&self) -> &[u8] {
        // SAFETY OWN-01/03: freeze consumed all mutation rights; slices stay
        // within its initialized filled range, pinned through this read borrow.
        unsafe { std::slice::from_raw_parts(self.lease.data().add(self.range.start), self.len()) }
    }
}

/// SI-10/OWN-03: the source borrow pins its slot, origin, filled range and root
/// until every reserved right has become a handle or has been refunded. No
/// partially constructed output is exposed to another thread or user callback.
struct BatchBuild<'a> {
    source: &'a Buf,
    dst: &'a mut [Option<Buf>],
    references: protocol::BatchReferences<'a>,
    built: usize,
    complete: bool,
}
impl<'a> BatchBuild<'a> {
    fn new(source: &'a Buf, dst: &'a mut [Option<Buf>]) -> Result<Self, Error> {
        if dst.iter().any(Option::is_some) {
            return Err(Error::OutputNotEmpty);
        }
        let references = source
            .lease
            .slot()
            .references
            .try_reserve(dst.len(), source.lease.root.reference_limit())?;
        Ok(Self {
            source,
            dst,
            references,
            built: 0,
            complete: false,
        })
    }
    fn materialize(&mut self) {
        // Bounds are checked before rights transfer. Output is known empty;
        // transferring a right and installing these concrete fields cannot panic.
        // SI-10: Range<usize>::clone only copies integers; replacing None runs
        // no Buf destructor. The successful index check proves built + 1 <= len,
        // so prefix advancement cannot overflow. No unwind point is permitted
        // from pending decrement through prefix advancement. The execution frame
        // owns the transferring right until the installed Buf represents it.
        let out = &mut self.dst[self.built];
        self.references.materialize();
        *out = Some(Buf {
            lease: Lease {
                root: self.source.lease.root,
                slot: self.source.lease.slot,
            },
            range: self.source.range.clone(),
        });
        self.built += 1;
    }
}
impl Drop for BatchBuild<'_> {
    fn drop(&mut self) {
        if !self.complete {
            for out in &mut self.dst[..self.built] {
                drop(out.take());
            }
        }
        // BatchReferences then refunds only unmaterialized rights. The original
        // source still exists, so neither cleanup path can publish the slot.
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BitmapMetrics {
    pub word_index: usize,
    pub available: usize,
}
/// Constant work, issuer-local metrics. No hidden bitmap/domain scan.
#[derive(Debug, Clone, Copy)]
pub struct Metrics {
    pub pending_domain_records: usize,
    pub phase: RootPhase,
    pub reservation: Reservation,
    pub total_slots: usize,
    pub control_handles: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DomainMetrics {
    pub identity: DomainIdentity,
    pub phase: DomainPhase,
    pub limit: usize,
    pub admitted_bytes: usize,
    pub owner: bool,
    pub control_handles: usize,
}
impl Default for DomainMetrics {
    /// Initialized caller scratch, not an observation of any particular pool.
    fn default() -> Self {
        Self {
            identity: DomainIdentity {
                index: 0,
                generation: 0,
            },
            phase: DomainPhase::Free,
            limit: 0,
            admitted_bytes: 0,
            owner: false,
            control_handles: 0,
        }
    }
}
/// Progress through the fixed record table, not an atomic snapshot cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsPage {
    pub written: usize,
    pub next_index: usize,
    pub done: bool,
}
fn domain_metrics(ptr: RootPtr, index: usize) -> DomainMetrics {
    let record = ptr.domain(index);
    let state = record.state.get();
    DomainMetrics {
        identity: DomainIdentity {
            index,
            generation: state.generation,
        },
        phase: state.phase,
        limit: state.limit,
        admitted_bytes: record.ledger.admitted.load(Ordering::Acquire),
        owner: state.owner,
        control_handles: state.handles,
    }
}
fn domain_metrics_page(
    start: usize,
    dst: &mut [DomainMetrics],
    count: usize,
    mut sample: impl FnMut(usize) -> DomainMetrics,
) -> Result<MetricsPage, Error> {
    if start > count {
        return Err(Error::InvalidRange);
    }
    let written = dst.len().min(count - start);
    for (index, out) in dst[..written].iter_mut().enumerate() {
        *out = sample(start + index);
    }
    let next_index = start + written; // bounded by count
    Ok(MetricsPage {
        written,
        next_index,
        done: next_index == count,
    })
}
#[derive(Debug, Clone)]
pub struct ClassSnapshot {
    pub capacity: usize,
    pub total: usize,
    /// Independently observed free bits; not a consistent snapshot.
    pub observed_available_slots: usize,
    pub reserved: usize,
    pub unassigned: usize,
    /// Includes final returns before bit publication, even at zero references.
    pub observed_unpublished_slots: usize,
}
#[derive(Debug, Clone)]
pub struct DomainSnapshot {
    pub identity: DomainIdentity,
    pub phase: DomainPhase,
    pub limit: usize,
    pub owner: bool,
    pub handles: usize,
    /// Sum of independently sampled unpublished slots, including pending returns.
    pub observed_unpublished_backing_bytes: usize,
    /// An independent ledger load. Do not subtract other samples to infer phases.
    pub observed_admitted_bytes: usize,
    pub grants: Vec<ClassGrantSnapshot>,
}
#[derive(Debug, Clone)]
pub struct SlotSnapshot {
    pub class: usize,
    pub origin: Option<DomainIdentity>,
    pub ownership: OwnershipKind,
    /// Global backing rights; a local family consumes one, regardless of its
    /// handle count. Diagnostics never inspect another thread's local counter.
    pub references: usize,
}
/// Allocating cold diagnostic, exact at quiescence only. Never admission evidence.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub pending_domain_records: usize,
    pub phase: RootPhase,
    pub reservation: Reservation,
    pub classes: Vec<ClassSnapshot>,
    pub domains: Vec<DomainSnapshot>,
    pub slots: Vec<SlotSnapshot>,
    pub free_domain_records: usize,
    pub retired_domain_records: usize,
}
fn snapshot(ptr: RootPtr) -> Snapshot {
    let root = ptr.root();
    let classes = (0..root.class_count)
        .map(|i| {
            let class = ptr.class(i);
            let available = (0..class.words)
                .map(|w| ptr.word(class.word_first + w).bitmap.load().count_ones() as usize)
                .sum();
            let unassigned = if root.capacity_policy == CapacityPolicy::Reserved {
                ptr.unassigned_count(i)
            } else {
                class.slots
            };
            ClassSnapshot {
                capacity: class.capacity,
                total: class.slots,
                observed_available_slots: available,
                reserved: class.slots - unassigned,
                unassigned,
                observed_unpublished_slots: class.slots - available,
            }
        })
        .collect();
    let mut domains: Vec<_> = (0..root.domain_count)
        .map(|index| {
            let record = ptr.domain(index);
            let state = record.state.get();
            DomainSnapshot {
                identity: DomainIdentity {
                    index,
                    generation: state.generation,
                },
                phase: state.phase,
                limit: state.limit,
                owner: state.owner,
                handles: state.handles,
                observed_unpublished_backing_bytes: 0,
                observed_admitted_bytes: record.ledger.admitted.load(Ordering::Acquire),
                grants: if root.capacity_policy == CapacityPolicy::Reserved {
                    (0..root.class_count)
                        .map(|class| ptr.grant_metrics(index, class))
                        .collect()
                } else {
                    Vec::new()
                },
            }
        })
        .collect();
    let slots = (0..root.plan.slot_count)
        .map(|index| {
            let slot = ptr.slot(index);
            let class = ptr.class(slot.class);
            if ptr.word(slot.word).bitmap.load() & slot.bit != 0 {
                SlotSnapshot {
                    class: slot.class,
                    origin: None,
                    ownership: OwnershipKind::Free,
                    references: 0,
                }
            } else {
                // SAFETY OWN-03: only the issuer writes origin; this cold issuer
                // observation cannot overlap another acquire. Remote returns only
                // read origin and do not mutate it, even after publishing the bit.
                let origin = unsafe { *slot.origin.get() };
                domains[origin.index].observed_unpublished_backing_bytes += class.capacity;
                SlotSnapshot {
                    class: slot.class,
                    origin: Some(origin),
                    ownership: if slot.frozen.load(Ordering::Acquire) {
                        OwnershipKind::Immutable
                    } else {
                        OwnershipKind::Mutable
                    },
                    references: slot.references.load(),
                }
            }
        })
        .collect();
    Snapshot {
        pending_domain_records: root.pending_count.get(),
        phase: root.phase.get(),
        reservation: root.plan.reservation,
        classes,
        domains,
        slots,
        free_domain_records: root.free_count.get(),
        retired_domain_records: root.retired.get(),
    }
}

#[cfg(test)]
mod tests;
