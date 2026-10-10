//! MEM-01/OWN-01 configuration oracle shared with the bounded fuzz target.
#![forbid(unsafe_code)]
use abps_reference::{CapacityPolicy, ClassConfig, Error};
const ARENA_CAP: usize = 4 << 20;

macro_rules! backend {
    ($name:ident, $path:path, $limits:literal) => {
        #[cfg(feature = $limits)]
        pub mod $name {
            use super::*;
            use $path as api;
            pub fn check(
                classes: Vec<ClassConfig>,
                records: usize,
                reserved: bool,
                refs: usize,
                controls: usize,
                option: u8,
            ) -> bool {
                let mut c = api::Config::new(classes, records, usize::MAX);
                c.capacity_policy = if reserved {
                    CapacityPolicy::Reserved
                } else {
                    CapacityPolicy::Unreserved
                };
                c.reference_limit = refs;
                c.options(controls, option);
                let result = api::Reservation::for_config(&c);
                let Ok(r) = result else {
                    assert!(matches!(
                        result,
                        Err(Error::InvalidConfiguration | Error::LayoutOverflow)
                    ));
                    return false;
                };
                let bytes: u128 = c
                    .classes
                    .iter()
                    .map(|c| c.capacity as u128 * c.slots as u128)
                    .sum();
                assert_eq!(
                    r.backing_bytes as u128, bytes,
                    "MEM-01 wide payload arithmetic"
                );
                assert_eq!(
                    r.total_bytes as u128,
                    r.arena_bytes as u128 + r.diagnostic_bytes as u128
                );
                assert_eq!(r.total_bytes as u128, bytes + r.metadata_bytes as u128);
                assert!(r.arena_bytes <= isize::MAX as usize);
                c.root_limit = r.total_bytes - 1;
                assert_eq!(
                    api::Reservation::for_config(&c),
                    Err(Error::RootBudgetExceeded)
                );
                c.root_limit += 1;
                assert_eq!(api::Reservation::for_config(&c), Ok(r));
                // Layout-only for large successful plans: never ask the allocator
                // to materialize adversarial sizes, even if overcommit allows it.
                if r.total_bytes > ARENA_CAP {
                    return true;
                }
                let mut owner = api::StorageOwner::prepare(&c).unwrap();
                assert_eq!(owner.reservation(), Ok(r));
                owner.try_destroy().unwrap();
                true
            }
        }
    };
}
backend!(berry, abps_reference::berry, "berry-spike");
backend!(mosaic, abps_reference::mosaic, "mosaic-spike");

trait ConfigOptions {
    fn options(&mut self, controls: usize, option: u8);
}
#[cfg(feature = "berry-spike")]
impl ConfigOptions for abps_reference::berry::Config {
    fn options(&mut self, controls: usize, _: u8) {
        self.handle_limit = controls;
    }
}
// Configuration types differ; keep the adapter separate from the oracle.
#[cfg(feature = "mosaic-spike")]
impl ConfigOptions for abps_reference::mosaic::Config {
    fn options(&mut self, controls: usize, option: u8) {
        self.control_handle_limit = controls;
        self.max_bitmap_words_per_class = [0, 1, 2, 256, 257][usize::from(option) % 5];
        self.preparation = if option & 8 == 0 {
            abps_reference::mosaic::PreparationPolicy::Lazy
        } else {
            abps_reference::mosaic::PreparationPolicy::ZeroPayload
        };
        #[cfg(feature = "local-branches-spike")]
        if option & 16 != 0 {
            self.local_branches = Some(abps_reference::mosaic::LocalBranchConfig {
                max_branches_per_slot: usize::from(option % 4),
                local_reference_limit: 4,
            });
        }
    }
}

pub fn replay(data: &[u8]) {
    let byte = |i| data.get(i).copied().unwrap_or(0) as usize;
    let w = usize::BITS as usize;
    let sizes = [0, 1, 7, 8, 63, 64, 65, isize::MAX as usize, usize::MAX];
    let counts = [
        0,
        1,
        2,
        w - 1,
        w,
        w + 1,
        2 * w - 1,
        2 * w,
        2 * w + 1,
        256 * w,
        usize::MAX,
    ];
    let domains = [0, 1, 2, 3, usize::MAX];
    let limits = [0, 1, 2, isize::MAX as usize, usize::MAX];
    let length = [0, 1, 2, 64, 65][byte(0) % 5];
    let classes: Vec<_> = (0..length)
        .map(|i| ClassConfig {
            capacity: sizes[byte(1 + i % 2) % sizes.len()].saturating_add(i),
            slots: counts[byte(3 + i % 2) % counts.len()],
        })
        .collect();
    let records = domains[byte(5) % domains.len()];
    let reserved = byte(6) % 2 != 0;
    let refs = limits[byte(7) % limits.len()];
    let controls = limits[byte(8) % limits.len()];
    #[cfg(feature = "berry-spike")]
    berry::check(
        classes.clone(),
        records,
        reserved,
        refs,
        controls,
        byte(9) as u8,
    );
    #[cfg(feature = "mosaic-spike")]
    mosaic::check(classes, records, reserved, refs, controls, byte(9) as u8);
}
