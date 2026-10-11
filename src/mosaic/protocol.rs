//! SI-11/12 atomic transitions, shared verbatim by native storage and Loom.
//! Callers enforce one issuer and exactly one return ticket per allocation.
#![forbid(unsafe_code)]

use crate::Error;
#[cfg(all(test, feature = "loom-model", not(abps_tsan)))]
use loom::sync::atomic::fence;
#[cfg(all(test, feature = "loom-model"))]
pub(super) use loom::sync::atomic::{AtomicUsize, Ordering};
#[cfg(all(not(all(test, feature = "loom-model")), not(abps_tsan)))]
use std::sync::atomic::fence;
#[cfg(not(all(test, feature = "loom-model")))]
pub(super) use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// SI-11: called only by the sole issuer after closing admission. Each zero
/// acquires the final refund for that domain; summing charges is insufficient.
/// Cold work is bounded by the prepared domain count. The control gate must
/// precede every ledger access.
pub(super) fn root_drained<'a>(
    controls: usize,
    mut ledgers: impl Iterator<Item = &'a DomainLedger>,
) -> bool {
    controls == 1 && ledgers.all(DomainLedger::drained)
}

#[repr(align(64))]
pub(super) struct DomainLedger {
    pub admitted: AtomicUsize,
}
impl DomainLedger {
    pub fn new() -> Self {
        Self {
            admitted: AtomicUsize::new(0),
        }
    }
    pub fn reserve(&self, charge: usize, limit: usize) -> Result<(), Error> {
        let old = self.admitted.load(Ordering::Relaxed);
        let new = old
            .checked_add(charge)
            .filter(|n| *n <= limit)
            .ok_or(Error::BudgetExceeded)?;
        self.admitted
            .compare_exchange(old, new, Ordering::Relaxed, Ordering::Relaxed)
            .map(|_| ())
            .map_err(|_| Error::Contended)
    }
    pub fn refund(&self, charge: usize) {
        refund_admitted(&self.admitted, charge);
    }
    pub fn drained(&self) -> bool {
        // Completion is published after the refund, and no issuer admission
        // overlaps this issuer-only drain decision for a closed domain.
        self.admitted.load(Ordering::Acquire) == 0
    }
}

/// SI-11: Release subtraction of a charge owned by the caller. A final refund
/// is the last storage access, so it borrows only the atomic word and uses
/// only the returned scalar afterwards (see PublishedReturn).
fn refund_admitted(admitted: &AtomicUsize, charge: usize) {
    let old = admitted.fetch_sub(charge, Ordering::Release);
    debug_assert!(old >= charge, "refund must own its full charge");
}

pub(super) struct Bitmap(pub AtomicUsize);
impl Bitmap {
    pub fn new(slots: usize) -> Self {
        assert!((1..=usize::BITS as usize).contains(&slots));
        Self(AtomicUsize::new(
            usize::MAX >> (usize::BITS as usize - slots),
        ))
    }
    pub fn confirm(&self) -> usize {
        self.0.fetch_or(0, Ordering::Acquire)
    }
    pub fn claim(&self, bit: usize) {
        self.0.fetch_and(!bit, Ordering::Acquire);
    }
    pub fn publish(&self, bit: usize) {
        self.0.fetch_or(bit, Ordering::Release);
    }
    pub fn load(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }
}

/// SI-12: only one issuer may clear; the accessor visits immutable eligible
/// masks. Returners can only add bits. `confirm` RMWs, unlike loads, read their
/// modification-order predecessor; zero across this monotone pass has an empty
/// witness at the first confirmation. No callback here is public/user supplied.
pub(super) fn take_words<'a>(
    count: usize,
    start: usize,
    mut word: impl FnMut(usize) -> (&'a Bitmap, usize),
    depleted: Error,
) -> Result<(usize, usize), Error> {
    for confirm in [false, true] {
        let mut index = start;
        for _ in 0..count {
            let (bitmap, mask) = word(index);
            let free = (if confirm {
                bitmap.confirm()
            } else {
                bitmap.load()
            }) & mask;
            if free != 0 {
                let bit = free & free.wrapping_neg();
                bitmap.claim(bit);
                return Ok((index, bit));
            }
            // AP-08: same bounded cyclic order in both passes; no division and
            // no change to the confirming RMW or monotone-publication proof.
            index += 1;
            if index == count {
                index = 0;
            }
        }
    }
    Err(depleted)
}

pub(super) struct References(pub AtomicUsize);
impl References {
    pub fn new() -> Self {
        Self(AtomicUsize::new(0))
    }
    /// Sole issuer, after an acquiring bitmap RMW; the previous last reader
    /// never touches this counter after publishing the free bit.
    pub fn begin(&self) {
        self.0.store(1, Ordering::Relaxed);
    }
    /// BR-01: exclusive BufMut, positive count already checked against the
    /// global limit. No other handle or observer may consume these rights yet.
    /// Caller transfers all rights into its guard without intervening panics.
    #[cfg(feature = "local-branches-spike")]
    pub fn begin_branches(&self, count: usize) {
        self.0.store(count, Ordering::Relaxed);
    }
    pub fn try_clone(&self, limit: usize) -> Result<(), Error> {
        self.try_add(1, limit)
    }
    fn try_add(&self, count: usize, limit: usize) -> Result<(), Error> {
        let old = self.0.load(Ordering::Relaxed);
        let new = old
            .checked_add(count)
            .filter(|n| *n <= limit)
            .ok_or(Error::ReferenceLimit)?;
        self.0
            .compare_exchange(old, new, Ordering::Relaxed, Ordering::Relaxed)
            .map(|_| ())
            .map_err(|_| Error::Contended)
    }
    /// SI-10: the caller must retain its original handle through this guard.
    pub fn try_reserve(&self, count: usize, limit: usize) -> Result<BatchReferences<'_>, Error> {
        if count != 0 {
            self.try_add(count, limit)?;
        }
        Ok(BatchReferences {
            references: self,
            pending: count,
        })
    }
    pub fn load(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }
    pub fn release(&self) -> bool {
        if self.0.fetch_sub(1, Ordering::Release) != 1 {
            return false;
        }
        // OWN-03/SI-11/BR-03: collect every prior reader's Release decrement
        // before slot/sidecar reuse or final refund permits storage destruction.
        // TSan cannot instrument fences; its load acquires the same RMW release
        // sequences. It must observe our zero: no live/reserved right remains
        // and the issuer cannot reinitialize the unpublished slot's counter.
        #[cfg(not(abps_tsan))]
        {
            fence(Ordering::Acquire);
        }
        #[cfg(abps_tsan)]
        {
            let _ = self.0.load(Ordering::Acquire);
        }
        true
    }
}

#[cfg(feature = "local-branches-spike")]
mod branches;
#[cfg(feature = "local-branches-spike")]
pub(super) use branches::LocalReferences;

/// Owns rights not yet represented by handles. The borrowed original prevents
/// final release, including when all other threads concurrently drop their views.
pub(super) struct BatchReferences<'a> {
    references: &'a References,
    pending: usize,
}
impl BatchReferences<'_> {
    #[cfg(test)]
    pub fn pending(&self) -> usize {
        self.pending
    }

    /// Transfer one reserved right into one handle, without synchronization.
    pub fn materialize(&mut self) {
        assert!(self.pending > 0);
        self.pending -= 1;
    }
}
impl Drop for BatchReferences<'_> {
    fn drop(&mut self) {
        if self.pending != 0 {
            // SI-10/OWN-03: none of these rights was published. The original
            // remains borrowed, so this subtraction can never be final return.
            let old = self.references.0.fetch_sub(self.pending, Ordering::Release);
            debug_assert!(old > self.pending);
        }
    }
}

/// Carries only stable root/class/domain records, never reusable slot metadata.
/// Positive admitted charge protects these borrows through the last refund.
pub(super) struct Return<'a> {
    class: &'a Bitmap,
    domain: &'a DomainLedger,
    bit: usize,
    charge: usize,
}
/// SI-11/AP-02: after publication only the atomic admitted word is borrowed.
/// The final refund lets the owner observe zero and free the arena while this
/// frame is still live. A borrow covering non-atomic bytes, such as the padding
/// of the 64-byte-aligned DomainLedger, would stay protected until `finish`
/// returns and that deallocation would be UB (cf. rust-lang/rust#55005).
pub(super) struct PublishedReturn<'a> {
    admitted: &'a AtomicUsize,
    charge: usize,
}
impl<'a> Return<'a> {
    pub fn new(class: &'a Bitmap, domain: &'a DomainLedger, bit: usize, charge: usize) -> Self {
        Self {
            class,
            domain,
            bit,
            charge,
        }
    }
    pub fn publish(self) -> PublishedReturn<'a> {
        self.class.publish(self.bit);
        PublishedReturn {
            admitted: &self.domain.admitted,
            charge: self.charge,
        }
    }
}
impl PublishedReturn<'_> {
    pub fn finish(self) {
        refund_admitted(self.admitted, self.charge);

        // SI-11: no storage access after this final refund.
    }
}

#[cfg(all(test, feature = "loom-model"))]
mod tests;
