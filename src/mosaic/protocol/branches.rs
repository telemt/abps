//! BR-02/03: actual per-family transitions, instrumented with Loom's Cell.
#![forbid(unsafe_code)]

use crate::Error;
#[cfg(all(test, feature = "loom-model"))]
use loom::cell::Cell;
#[cfg(not(all(test, feature = "loom-model")))]
use std::cell::Cell;

/// Layout is owned by the prepared storage record (AP-06); this is the protocol.
pub(in crate::mosaic) struct LocalReferences(Cell<usize>);
impl LocalReferences {
    pub fn new() -> Self {
        Self(Cell::new(0))
    }
    /// Exclusive slot construction; a reserved global right pins this record.
    pub fn begin(&self) {
        self.0.set(1);
    }
    pub fn try_clone(&self, limit: usize) -> Result<(), Error> {
        let next = self
            .0
            .get()
            .checked_add(1)
            .filter(|n| *n <= limit)
            .ok_or(Error::ReferenceLimit)?;
        self.0.set(next);
        Ok(())
    }
    pub fn unique(&self) -> bool {
        self.0.get() == 1
    }
    /// Caller owns one local right. On true it must release the family's one
    /// global right, with no further access to this record (BR-03).
    pub fn release(&self) -> bool {
        let old = self.0.get();
        debug_assert!(old > 0, "release must own one local right");
        self.0.set(old - 1);
        old == 1
    }
}
