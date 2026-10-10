//! OWN-03/AP-01: buffer authority has no issuer/control/grant accessors.
//! The arena pointer is private to this module. Field projections below never
//! form an &Root or &DomainRecord containing concurrently modified issuer Cells.
use super::*;

#[derive(Clone, Copy)]
pub(super) struct RemotePtr(NonNull<Root>);
impl RemotePtr {
    /// A control pins the initialized arena; an exposed Lease then retains a
    /// positive admitted charge until its final refund (SI-11).
    pub(super) fn from_issuer(root: RootPtr) -> Self {
        Self(root.0)
    }
    fn plan(&self) -> &Plan {
        // SAFETY MEM-01/AP-01: immutable initialized field, projected directly;
        // no shared reference to the enclosing issuer-only Root is created.
        unsafe { &*std::ptr::addr_of!((*self.0.as_ptr()).plan) }
    }
    pub(super) fn reference_limit(&self) -> usize {
        // SAFETY OWN-03/AP-01: immutable scalar set before any buffer exists.
        unsafe { (*self.0.as_ptr()).reference_limit }
    }
    pub(super) fn slot(&self, index: usize) -> &Slot {
        let plan = self.plan();
        assert!(index < plan.slot_count);
        // SAFETY MEM-01/OWN-03: checked initialized slot, pinned by this right.
        // Only the exclusive issuer writes origin after acquiring its free bit.
        unsafe {
            &*self
                .0
                .as_ptr()
                .cast::<u8>()
                .add(plan.slots)
                .cast::<Slot>()
                .add(index)
        }
    }
    pub(super) fn class(&self, index: usize) -> &Class {
        // SAFETY MEM-01/AP-01: immutable bound, not an issuer Cell.
        let count = unsafe { (*self.0.as_ptr()).class_count };
        assert!(index < count);
        // SAFETY MEM-01: checked prepared Class array; all fields are immutable.
        unsafe {
            &*self
                .0
                .as_ptr()
                .cast::<u8>()
                .add(self.plan().classes)
                .cast::<Class>()
                .add(index)
        }
    }
    pub(super) fn ledger(&self, index: usize) -> &protocol::DomainLedger {
        // SAFETY MEM-01/AP-01: immutable bound, not an issuer Cell.
        let count = unsafe { (*self.0.as_ptr()).domain_count };
        assert!(index < count);
        // SAFETY SI-11/AP-01: project only the atomic ledger. Never construct an
        // &DomainRecord or read its state/queue/grant Cells. Old positive charge
        // pins this ledger through final refund, independently of slot reuse.
        unsafe {
            let record = self
                .0
                .as_ptr()
                .cast::<u8>()
                .add(self.plan().domains)
                .cast::<DomainRecord>()
                .add(index);
            &*std::ptr::addr_of!((*record).ledger)
        }
    }
    pub(super) fn word(&self, index: usize) -> &Word {
        assert!(index < self.plan().word_count);
        // SAFETY SI-12: bounded initialized Word; contains only a bitmap atomic.
        unsafe {
            &*self
                .0
                .as_ptr()
                .cast::<u8>()
                .add(self.plan().words)
                .cast::<Word>()
                .add(index)
        }
    }
    pub(super) fn data(&self, index: usize) -> *mut u8 {
        // SAFETY OWN-03: checked immutable slot offset. A pointer is formed,
        // never a reference spanning neighboring payload or metadata.
        unsafe {
            self.0
                .as_ptr()
                .cast::<u8>()
                .add(self.slot(index).data_offset)
        }
    }
    #[cfg(feature = "local-branches-spike")]
    pub(super) fn branch_limit(&self) -> usize {
        self.plan().branches_per_slot
    }
    #[cfg(feature = "local-branches-spike")]
    pub(super) fn branch_record(&self, slot: usize, branch: usize) -> &branches::BranchRecord {
        let plan = self.plan();
        assert!(slot < plan.slot_count && branch < plan.branches_per_slot);
        // SAFETY BR-02/04: checked initialized prepared record; only exclusive
        // construction or this family's current thread may access its local Cell.
        // Its global right pins the descriptor through the final local release.
        unsafe {
            &*self
                .0
                .as_ptr()
                .cast::<u8>()
                .add(plan.branch_records)
                .cast::<branches::BranchRecord>()
                .add(slot * plan.branches_per_slot + branch)
        }
    }
}
