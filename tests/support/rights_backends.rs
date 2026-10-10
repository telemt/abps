#![allow(dead_code)]
macro_rules! backend {
    ($name:ident, $path:path, $explicit:expr, $prepare:expr, $available:ident, $admitted:ident, $pending:expr, $poll:expr) => {
        pub mod $name {
            use api::*;
            const BACKEND: &str = stringify!($name);
            use $path as api;
            const EXPLICIT_MAINTENANCE: bool = $explicit;
            fn prepare(c: Config) -> Result<StorageOwner, Error> {
                ($prepare)(c)
            }
            fn class_available(c: &ClassSnapshot) -> usize {
                c.$available
            }
            fn domain_admitted(d: &DomainSnapshot) -> usize {
                d.$admitted
            }
            fn pending(s: &Snapshot) -> usize {
                ($pending)(s)
            }
            fn poll(p: &Pool, n: usize) -> (usize, usize, usize) {
                ($poll)(p, n)
            }
            mod implementation {
                include!("rights_backend.rs");
            }
            pub use implementation::replay;
        }
    };
}
backend!(
    reference,
    abps_reference,
    false,
    StorageOwner::prepare,
    available,
    admitted_bytes,
    |_: &Snapshot| 0,
    |_: &Pool, _: usize| (0, 0, 0)
);
#[cfg(feature = "berry-spike")]
backend!(
    berry,
    abps_reference::berry,
    true,
    |c| StorageOwner::prepare(&c),
    observed_available_slots,
    observed_admitted_bytes,
    |s: &Snapshot| s.pending_domain_records,
    |p: &Pool, n| {
        let m = p.poll_domains(n);
        (m.examined, m.reclaimed, m.pending)
    }
);
#[cfg(feature = "mosaic-spike")]
backend!(
    mosaic,
    abps_reference::mosaic,
    true,
    |c| StorageOwner::prepare(&c),
    observed_available_slots,
    observed_admitted_bytes,
    |s: &Snapshot| s.pending_domain_records,
    |p: &Pool, n| {
        let m = p.poll_domains(n);
        (m.examined, m.reclaimed, m.pending)
    }
);
