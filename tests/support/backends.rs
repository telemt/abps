pub mod reference {
    mod backend {
        pub use abps_reference::*;
        pub fn class_counts(c: &ClassSnapshot) -> (usize, usize) {
            assert_eq!((c.acquiring, c.returning), (0, 0));
            (c.available, c.issued)
        }
        pub fn domain_bytes(d: &DomainSnapshot) -> (usize, usize) {
            assert_eq!((d.pending_acquire_bytes, d.pending_return_bytes), (0, 0));
            (d.admitted_bytes, d.live_backing_bytes)
        }
        pub fn allocation_count(_: &Snapshot, d: &DomainSnapshot) -> usize {
            d.allocations
        }
        pub const EXPLICIT_MAINTENANCE: bool = false;
        pub fn poll(_: &Pool, _: usize) -> (usize, usize, usize) {
            (0, 0, 0)
        }
        pub fn pending(_: &Snapshot) -> usize {
            0
        }
        pub fn prepare(config: Config) -> Result<StorageOwner, Error> {
            StorageOwner::prepare(config)
        }
    }
    pub mod oracle {
        include!("ledger_oracle.rs");
    }
    #[cfg(test)]
    mod reservations {
        include!("reservations.rs");
    }
}
#[cfg(feature = "berry-spike")]
pub mod berry {
    mod backend {
        pub use abps_reference::berry::*;
        pub fn class_counts(c: &ClassSnapshot) -> (usize, usize) {
            (c.observed_available_slots, c.observed_unpublished_slots)
        }
        pub fn domain_bytes(d: &DomainSnapshot) -> (usize, usize) {
            (
                d.observed_admitted_bytes,
                d.observed_unpublished_backing_bytes,
            )
        }
        pub fn allocation_count(_: &Snapshot, d: &DomainSnapshot) -> usize {
            d.allocations
        }
        pub const EXPLICIT_MAINTENANCE: bool = true;
        pub fn poll(pool: &Pool, n: usize) -> (usize, usize, usize) {
            let m = pool.poll_domains(n);
            (m.examined, m.reclaimed, m.pending)
        }
        pub fn pending(s: &Snapshot) -> usize {
            s.pending_domain_records
        }
        pub fn prepare(config: Config) -> Result<StorageOwner, Error> {
            StorageOwner::prepare(&config)
        }
    }
    pub mod oracle {
        include!("ledger_oracle.rs");
    }
    #[cfg(test)]
    mod reservations {
        include!("reservations.rs");
    }
}

#[cfg(feature = "mosaic-spike")]
pub mod mosaic {
    mod backend {
        pub use abps_reference::mosaic::*;
        pub fn class_counts(c: &ClassSnapshot) -> (usize, usize) {
            (c.observed_available_slots, c.observed_unpublished_slots)
        }
        pub fn domain_bytes(d: &DomainSnapshot) -> (usize, usize) {
            (
                d.observed_admitted_bytes,
                d.observed_unpublished_backing_bytes,
            )
        }
        pub fn allocation_count(s: &Snapshot, d: &DomainSnapshot) -> usize {
            s.slots
                .iter()
                .filter(|slot| slot.origin == Some(d.identity))
                .count()
        }
        pub const EXPLICIT_MAINTENANCE: bool = true;
        pub fn poll(pool: &Pool, n: usize) -> (usize, usize, usize) {
            let m = pool.poll_domains(n);
            (m.examined, m.reclaimed, m.pending)
        }
        pub fn pending(s: &Snapshot) -> usize {
            s.pending_domain_records
        }
        pub fn prepare(config: Config) -> Result<StorageOwner, Error> {
            StorageOwner::prepare(&config)
        }
    }
    pub mod oracle {
        include!("ledger_oracle.rs");
    }
    #[cfg(test)]
    mod reservations {
        include!("reservations.rs");
    }
}
