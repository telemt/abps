//! BR-01..06: prepared sidecars for explicit, uniquely transferable families.
//! Ordinary Lease/Buf return paths remain unchanged.
use super::*;

// AP-06/BR-04: align the complete descriptor, not an embedded 64-byte counter.
// These fields are immutable after prepare; only references changes on reuse.
#[repr(align(64))]
pub(super) struct BranchRecord {
    pub(super) references: protocol::LocalReferences,
    pub(super) root: RemotePtr,
    pub(super) slot: usize,
    pub(super) data: NonNull<u8>,
    pub(super) capacity: usize,
    pub(super) limit: usize,
}

/// Validation preserves the original mutable lease and the destination.
pub struct FreezeBranchesError {
    reason: Error,
    source: BufMut,
}
impl FreezeBranchesError {
    pub fn reason(&self) -> Error {
        self.reason
    }
    pub fn into_parts(self) -> (Error, BufMut) {
        (self.reason, self.source)
    }
}
impl fmt::Debug for FreezeBranchesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FreezeBranchesError")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for FreezeBranchesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.reason.fmt(f)
    }
}
impl std::error::Error for FreezeBranchesError {}

impl BufMut {
    /// BR-01: freeze into K prepared branch rights before publishing any handle.
    /// Empty output releases this lease. Errors preserve both bytes and output;
    /// recover the lease with FreezeBranchesError::into_parts. Work is O(K).
    pub fn freeze_branches_into(
        self,
        dst: &mut [Option<Branch>],
    ) -> Result<(), FreezeBranchesError> {
        let reason = if dst.iter().any(Option::is_some) {
            Some(Error::OutputNotEmpty)
        } else if dst.len() > self.lease.root.branch_limit() {
            Some(Error::BranchCapacityExceeded)
        } else if dst.len() > self.lease.root.reference_limit() {
            Some(Error::ReferenceLimit)
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(FreezeBranchesError {
                reason,
                source: self,
            });
        }
        if dst.is_empty() {
            drop(self);
            return Ok(());
        }
        let mut build = BranchBuild::new(self, dst);
        build.reserve();
        while build.built < build.dst.len() {
            build.materialize();
        }
        build.commit();
        Ok(())
    }
}

/// One uniquely transferable branch. Bind after receiving it on a worker.
/// ```compile_fail
/// fn sync<T: Sync>() {}
/// sync::<abps_reference::mosaic::Branch>();
/// ```
/// ```compile_fail
/// fn clone<T: Clone>() {}
/// clone::<abps_reference::mosaic::Branch>();
/// ```
pub struct Branch {
    inner: LocalBuf,
}
// SAFETY BR-02: construction or successful unique conversion provides exactly
// one local right. No other handle can access its Cell while it moves. A borrow
// of bytes prevents moving the Branch until that borrow ends. After synchronized
// transfer, only the receiving thread may bind/drop it. Branch remains !Sync.
unsafe impl Send for Branch {}
impl Branch {
    pub fn into_local(self) -> LocalBuf {
        self.inner
    }
    pub fn len(&self) -> usize {
        self.inner.len()
    }
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
    pub fn origin(&self) -> DomainIdentity {
        self.inner.origin()
    }
    pub fn backing_capacity(&self) -> usize {
        self.inner.backing_capacity()
    }
    pub fn try_view(&self, range: Range<usize>) -> Result<BufView<'_>, Error> {
        self.inner.try_view(range)
    }
    pub fn as_io_slice(&self) -> IoSlice<'_> {
        IoSlice::new(self.as_ref())
    }
}
impl AsRef<[u8]> for Branch {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}
impl fmt::Debug for Branch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Branch").field(&self.inner).finish()
    }
}

struct FamilyRight {
    record: NonNull<BranchRecord>,
    local: Local,
}
impl FamilyRight {
    fn record(&self) -> &BranchRecord {
        // SAFETY AP-06/BR-02: this local owner retains the family's global right,
        // which pins this prepared, aligned record. Only the family's current
        // thread accesses its Cell. Descriptor fields never change after prepare.
        unsafe { self.record.as_ref() }
    }
    fn try_clone(&self) -> Result<Self, Error> {
        let record = self.record();
        record.references.try_clone(record.limit)?;
        Ok(Self {
            record: self.record,
            local: PhantomData,
        })
    }
    fn data(&self) -> *mut u8 {
        self.record().data.as_ptr()
    }
}
impl Drop for FamilyRight {
    fn drop(&mut self) {
        let record = self.record();
        if record.references.release() {
            // AP-02/06, BR-03: copy the immutable return descriptor before the
            // global release. It may publish/reuse this record and destroy its
            // storage on another thread after refund. No record access follows.
            let root = record.root;
            let slot = record.slot;
            drop(Lease { root, slot });
        }
    }
}

/// Independently owned immutable views inside one thread/LocalSet.
/// Cloning/slicing is checked but never uses an atomic or returns Contended.
/// ```compile_fail
/// fn send<T: Send>() {}
/// send::<abps_reference::mosaic::LocalBuf>();
/// ```
/// ```compile_fail
/// fn sync<T: Sync>() {}
/// sync::<abps_reference::mosaic::LocalBuf>();
/// ```
pub struct LocalBuf {
    right: FamilyRight,
    range: Range<usize>,
}
impl LocalBuf {
    pub fn len(&self) -> usize {
        self.range.len()
    }
    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }
    pub fn origin(&self) -> DomainIdentity {
        // SAFETY BR-05/MEM-03: the family's global right excludes publication
        // and slot reuse. Origin was set before this immutable family existed.
        unsafe {
            let record = self.right.record();
            *record.root.slot(record.slot).origin.get()
        }
    }
    pub fn backing_capacity(&self) -> usize {
        self.right.record().capacity
    }
    pub fn as_io_slice(&self) -> IoSlice<'_> {
        IoSlice::new(self.as_ref())
    }
    pub fn try_view(&self, range: Range<usize>) -> Result<BufView<'_>, Error> {
        self.as_ref()
            .get(range)
            .map(BufView)
            .ok_or(Error::InvalidRange)
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
        Ok(Self {
            right: self.right.try_clone()?,
            range: start..end,
        })
    }
    pub fn try_into_branch(self) -> Result<Branch, Self> {
        if self.right.record().references.unique() {
            Ok(Branch { inner: self })
        } else {
            Err(self)
        }
    }
}
impl AsRef<[u8]> for LocalBuf {
    fn as_ref(&self) -> &[u8] {
        // SAFETY BR-02/05, OWN-01/03: freeze consumed all mutation rights; each
        // nested view is within the initialized filled prefix of this slot.
        // The borrowed self pins the local and global rights for this read.
        unsafe { std::slice::from_raw_parts(self.right.data().add(self.range.start), self.len()) }
    }
}
impl fmt::Debug for LocalBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalBuf")
            .field("origin", &self.origin())
            .field("len", &self.len())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BuildPhase {
    SourceOwned,
    BatchReserved,
    Committed,
}

struct BranchBuild<'a> {
    root: RemotePtr,
    slot: usize,
    filled: usize,
    dst: &'a mut [Option<Branch>],
    built: usize,
    pending: usize,
    staged: Option<Branch>,
    phase: BuildPhase,
}
impl<'a> BranchBuild<'a> {
    /// Caller preflights all limits/output. No other ownership exists yet.
    fn new(source: BufMut, dst: &'a mut [Option<Branch>]) -> Self {
        let BufMut { lease, filled, .. } = source;
        let root = lease.root;
        let slot = lease.slot;
        let build = Self {
            root,
            slot,
            filled,
            dst,
            built: 0,
            pending: 1,
            staged: None,
            phase: BuildPhase::SourceOwned,
        };
        // BR-01: install the guard while the original single right still exists.
        // Moving concrete fields and forgetting the replaced Lease cannot unwind.
        std::mem::forget(lease);
        build
    }

    fn reserve(&mut self) {
        assert_eq!(self.phase, BuildPhase::SourceOwned);
        assert!(!self.dst.is_empty());
        let slot = self.root.slot(self.slot);
        slot.frozen.store(true, Ordering::Release);
        slot.references.begin_branches(self.dst.len());
        // BR-01: only infallible scalar assignments separate the exclusive store
        // from updating the already installed guard. No indexing or callbacks.
        self.pending = self.dst.len();
        self.phase = BuildPhase::BatchReserved;
    }

    fn stage(&mut self) {
        assert_eq!(self.phase, BuildPhase::BatchReserved);
        assert!(self.pending > 0 && self.staged.is_none());
        assert!(self.dst[self.built].is_none());
        let record = self.root.branch_record(self.slot, self.built);
        record.references.begin();
        // BR-01/03: transfer one pending global right to a staged family. All
        // indexing/projections precede begin; construction, decrement (pending
        // was checked) and Option assignment cannot unwind. The guard owns the
        // family even if construction stops before installing the output slot.
        self.staged = Some(Branch {
            inner: LocalBuf {
                right: FamilyRight {
                    record: NonNull::from(record),
                    local: PhantomData,
                },
                range: 0..self.filled,
            },
        });
        self.pending -= 1;
    }

    fn install(&mut self) {
        assert!(self.staged.is_some());
        let out = &mut self.dst[self.built];
        assert!(out.is_none());
        *out = self.staged.take();
        self.built += 1;
    }

    fn materialize(&mut self) {
        self.stage();
        self.install();
    }

    fn commit(&mut self) {
        assert_eq!(self.phase, BuildPhase::BatchReserved);
        assert_eq!(self.pending, 0);
        assert!(self.staged.is_none());
        assert_eq!(self.built, self.dst.len());
        self.phase = BuildPhase::Committed;
    }
}
impl Drop for BranchBuild<'_> {
    fn drop(&mut self) {
        if self.phase != BuildPhase::Committed {
            drop(self.staged.take());
            for out in &mut self.dst[..self.built] {
                drop(out.take());
            }
            // Remaining rights have no local record/handle. Release each once;
            // after the last release the loop and this guard never read storage.
            for _ in 0..self.pending {
                drop(Lease {
                    root: self.root,
                    slot: self.slot,
                });
            }
        }
    }
}

#[cfg(test)]
mod tests;
