//! SI-16 small worker-replacement example using the same adapter registry as TCP.
use crate::perf_support::Result;
use serde_json::Value;

pub fn run() -> Result<Value> {
    #[cfg(not(feature = "mosaic-spike"))]
    return Err("recovery requires mosaic-spike".into());
    #[cfg(feature = "mosaic-spike")]
    {
        use super::{backend::Fixture, supervisor::Registry};
        use abps_reference::{ClassConfig, mosaic as b};
        use serde_json::json;
        let classes = vec![ClassConfig {
            capacity: 64,
            slots: 2,
        }];
        let config = b::Config::new(classes.clone(), 1, usize::MAX);
        let bytes = b::Reservation::for_config(&config)?.total_bytes;
        let registry = Registry::new(2, bytes.checked_mul(2).ok_or("limit overflow")?, 1, 1)?;
        let old = registry.reserve(&[bytes], false)?.pop().unwrap();
        let first = Fixture::new("mosaic", classes.clone(), 1, None)?;
        old.active();
        let domain = first.open(128)?;
        let retained = domain.acquire(64)?.freeze();
        drop(domain);
        old.draining();
        let (destroyed, _) = first.shutdown(std::time::Duration::ZERO);
        assert!(!destroyed);
        old.abandoned();
        let replacement = registry.reserve(&[bytes], true)?.pop().unwrap();
        let next = Fixture::new("mosaic", classes, 1, None)?;
        replacement.active();
        let domain = next.open(128)?;
        let mut new_work = domain.acquire(1)?;
        new_work.put(&[42]);
        drop(new_work.freeze());
        drop(domain);
        let during = registry.snapshot();
        // Completion of the old buffer never inherits arena teardown/refund.
        drop(retained);
        assert_eq!(registry.snapshot()["charged_bytes"], bytes * 2);
        replacement.draining();
        let (destroyed, _) = next.shutdown(std::time::Duration::ZERO);
        assert!(destroyed);
        replacement.destroyed();
        let rejected = registry
            .reserve(&[bytes], true)
            .err()
            .ok_or("restart loop incorrectly admitted")?
            .to_string();
        Ok(
            json!({"new_work_completed":1,"replayed_sessions":0,"during_overlap":during,"after":registry.snapshot(),"next_replacement_rejected":rejected,"intentional_abandonment":true}),
        )
    }
}
#[cfg(all(test, feature = "mosaic-spike"))]
mod tests {
    #[test]
    fn bounded_replacement_only_admits_new_work() {
        let r = super::run().unwrap();
        assert_eq!(r["new_work_completed"], 1);
        assert_eq!(r["replayed_sessions"], 0);
        assert_eq!(r["after"]["records"][0]["phase"], "Abandoned");
    }
}
