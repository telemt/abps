//! SI-01/02 atomic transitions, shared verbatim by native storage and Loom.
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

pub(super) struct Claims(pub AtomicUsize);
impl Claims {
    pub fn new() -> Self {
        Self(AtomicUsize::new(1))
    }
    /// Issuer-only increment. Remote operations can only decrease the count,
    /// so the load/check can be conservative but cannot admit above the limit.
    pub fn pin(&self, adding: usize, limit: usize) -> Result<(), Error> {
        if self.load().checked_add(adding).is_none_or(|n| n > limit) {
            return Err(Error::ReferenceLimit);
        }
        self.0.fetch_add(adding, Ordering::Relaxed);
        Ok(())
    }
    pub fn load(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }
    /// Must be the final access protected by this claim. Never deallocates.
    pub fn release(&self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}

pub(super) struct DomainLedger {
    pub admitted: AtomicUsize,
    pub allocations: AtomicUsize,
}
impl DomainLedger {
    pub fn new() -> Self {
        Self {
            admitted: AtomicUsize::new(0),
            allocations: AtomicUsize::new(0),
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
    pub fn commit(&self) {
        self.allocations.fetch_add(1, Ordering::Relaxed);
    }
    pub fn refund(&self, charge: usize) {
        self.admitted.fetch_sub(charge, Ordering::Release);
    }
    pub fn drained(&self) -> bool {
        // Completion is published after the refund, and no issuer admission
        // overlaps this issuer-only drain decision for a closed domain.
        self.allocations.load(Ordering::Acquire) == 0 && self.admitted.load(Ordering::Acquire) == 0
    }
}

pub(super) struct Bitmap(pub AtomicUsize);
impl Bitmap {
    pub fn new(slots: usize) -> Self {
        assert!((1..=usize::BITS as usize).contains(&slots));
        Self(AtomicUsize::new(
            usize::MAX >> (usize::BITS as usize - slots),
        ))
    }
    /// Exactly one issuer clears bits. Returners only OR their own bits, hence
    /// an observed free bit cannot be stolen between this load and RMW.
    pub fn take(&self) -> Result<usize, Error> {
        self.take_masked(usize::MAX, Error::ClassDepleted)
    }
    pub fn take_masked(&self, mask: usize, depleted: Error) -> Result<usize, Error> {
        if mask == 0 {
            return Err(depleted);
        }
        self.take_observed(mask, self.0.load(Ordering::Relaxed), depleted)
    }
    // SI-01: private continuation of the actual admission path. Keeping the
    // advisory sample explicit lets tests supply a stale hint without replacing
    // the production atomic protocol or installing a user callback in admission.
    fn take_observed(&self, mask: usize, observed: usize, depleted: Error) -> Result<usize, Error> {
        let hinted = observed & mask;
        let free = if hinted == 0 {
            // SI-01/PROG-02: an identity RMW reads its immediately preceding
            // modification-order value. Zero here is the depletion witness;
            // the earlier load alone (even Acquire) cannot supply that proof.
            // Only this issuer clears eligible bits, so any positive result
            // remains claimable. One confirmation, no retry or hidden wait.
            self.0.fetch_or(0, Ordering::Acquire) & mask
        } else {
            hinted
        };
        if free == 0 {
            return Err(depleted);
        }
        let bit = free & free.wrapping_neg();
        self.0.fetch_and(!bit, Ordering::Acquire);
        Ok(bit)
    }
    pub fn publish(&self, bit: usize) {
        self.0.fetch_or(bit, Ordering::Release);
    }
    pub fn load(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }
}

/// SI-05: issuer-only rights transfer. Neither function touches the atomic free
/// bitmap; grants change only before issuance or after complete domain drain.
pub(super) fn assign_mask(
    unassigned: &std::cell::Cell<usize>,
    grant: &std::cell::Cell<usize>,
    count: usize,
) {
    let mut free = unassigned.get();
    assert_eq!(grant.get(), 0);
    assert!(count <= free.count_ones() as usize);
    let mut selected = 0;
    for _ in 0..count {
        let bit = free & free.wrapping_neg();
        selected |= bit;
        free &= !bit;
    }
    grant.set(selected);
    unassigned.set(free);
}

pub(super) fn release_mask(unassigned: &std::cell::Cell<usize>, grant: &std::cell::Cell<usize>) {
    let mask = grant.replace(0);
    assert_eq!(unassigned.get() & mask, 0);
    unassigned.set(unassigned.get() | mask);
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
        // OWN-03/SI-02: collect every prior reader's Release decrement before
        // publication permits slot reuse. TSan cannot instrument fences, so its
        // build acquires the same RMW release sequences by loading our zero.
        // No live/reserved right remains to increment, and the issuer cannot
        // reinitialize this word until we publish the free bit after returning.
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
/// The allocation's root/domain claims protect these borrows through completion.
pub(super) struct Return<'a> {
    root: &'a Claims,
    class: &'a Bitmap,
    domain: &'a DomainLedger,
    bit: usize,
    charge: usize,
}
pub(super) struct PublishedReturn<'a> {
    root: &'a Claims,
    domain: &'a DomainLedger,
    charge: usize,
}
impl<'a> Return<'a> {
    pub fn new(
        root: &'a Claims,
        class: &'a Bitmap,
        domain: &'a DomainLedger,
        bit: usize,
        charge: usize,
    ) -> Self {
        Self {
            root,
            class,
            domain,
            bit,
            charge,
        }
    }
    pub fn publish(self) -> PublishedReturn<'a> {
        self.class.publish(self.bit);
        PublishedReturn {
            root: self.root,
            domain: self.domain,
            charge: self.charge,
        }
    }
}
impl PublishedReturn<'_> {
    pub fn finish(self) {
        self.domain.refund(self.charge);
        self.domain.allocations.fetch_sub(1, Ordering::Release);
        self.root.release();
        // No root, domain, or slot access after releasing the lifetime claim.
    }
}

#[cfg(all(test, feature = "loom-model"))]
mod tests;
