//! ABPS resource models. The default exports are the safe mutex reference.
//! `berry-spike` enables Berry, with one bitmap word per class.
//! `mosaic-spike` enables Mosaic, with segmented class bitmaps.
//! Both experimental backends use worker-local single-issuer admission.
#![deny(unsafe_code)]
#![deny(unsafe_op_in_unsafe_fn)]

mod reference;
pub use reference::*;

#[cfg(feature = "berry-spike")]
pub mod berry;

#[cfg(feature = "mosaic-spike")]
pub mod mosaic;

// Like Mosaic storage, the safe wrapper is outside the atomic-only Loom model.
#[cfg(all(feature = "serial-adapter", not(all(test, feature = "loom-model"))))]
pub mod serial;

#[cfg(all(test, feature = "loom-model"))]
#[path = "test_support/loom.rs"]
mod loom_support;

#[cfg(test)]
#[path = "test_support/config.rs"]
mod verification_config;
