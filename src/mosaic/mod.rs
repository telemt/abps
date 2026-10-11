//! Mosaic (SI-11..15): segmented capacity with worker-local single-issuer admission.
//! Exact domain limits; remote return publishes capacity before its last refund.
pub use crate::{
    CapacityPolicy, ClassConfig, ClassGrantSnapshot, ClassReservation, DomainPhase, Error,
    OwnershipKind, RootPhase,
};

/// SI-17: explicit startup work; neither policy guarantees page residency.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum PreparationPolicy {
    #[default]
    Lazy,
    /// Write all payload bytes during prepare. Leases still start uninitialized.
    ZeroPayload,
}

/// BR-01/04: fixed, root-charged sidecar records; no thread registration.
#[cfg(feature = "local-branches-spike")]
#[derive(Debug, Clone, Copy)]
pub struct LocalBranchConfig {
    pub max_branches_per_slot: usize,
    /// Independent of Config::reference_limit, which counts global rights.
    pub local_reference_limit: usize,
}

/// Build with `Config::new`, then assign fields. Non-exhaustive: the
/// feature-gated `local_branches` field must not break builds when Cargo
/// unifies the `local-branches-spike` feature for another crate in the graph.
///
/// ```compile_fail,E0639
/// let config = abps_reference::mosaic::Config::new(Vec::new(), 1, 1);
/// let _ = abps_reference::mosaic::Config { domain_slots: 2, ..config };
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Config {
    pub classes: Vec<ClassConfig>,
    pub domain_slots: usize,
    pub root_limit: usize,
    pub reference_limit: usize,
    /// Storage owner plus controls; allocations do not consume this limit.
    pub control_handle_limit: usize,
    pub capacity_policy: CapacityPolicy,
    pub max_bitmap_words_per_class: usize,
    pub preparation: PreparationPolicy,
    #[cfg(feature = "local-branches-spike")]
    pub local_branches: Option<LocalBranchConfig>,
}
impl Config {
    pub fn new(classes: Vec<ClassConfig>, domain_slots: usize, root_limit: usize) -> Self {
        Self {
            classes,
            domain_slots,
            root_limit,
            reference_limit: isize::MAX as usize,
            control_handle_limit: isize::MAX as usize,
            capacity_policy: CapacityPolicy::Unreserved,
            max_bitmap_words_per_class: 256,
            preparation: PreparationPolicy::Lazy,
            #[cfg(feature = "local-branches-spike")]
            local_branches: None,
        }
    }
}
mod grants;
mod protocol;
#[cfg(not(all(test, feature = "loom-model")))]
#[allow(unsafe_code)]
mod raw;
#[cfg(not(all(test, feature = "loom-model")))]
pub use raw::*;
