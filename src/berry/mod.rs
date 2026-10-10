//! Berry: an experimental single-issuer pool with cross-thread buffer ownership.
//!
//! Control handles stay on their worker. Each class has at most `usize::BITS`
//! slots. Returns never block or allocate, but interrupted returns are not helped.
//! See CONTRIBUTING SI-01/02/03 for the intentionally narrower progress contract.

pub use crate::{
    CapacityPolicy, ClassConfig, ClassGrantSnapshot, ClassReservation, Config, DomainPhase, Error,
    OwnershipKind, RootPhase,
};
#[cfg(not(all(test, feature = "loom-model")))]
mod grants;
mod protocol;

#[cfg(not(all(test, feature = "loom-model")))]
#[allow(unsafe_code)]
mod raw;
#[cfg(not(all(test, feature = "loom-model")))]
pub use raw::*;
