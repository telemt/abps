//! Benchmark-only dispatch. All control objects are created inside their worker.
use crate::perf_support::{Result, backend as baseline};
#[cfg(feature = "mosaic-spike")]
use abps_reference::mosaic;
use abps_reference::{
    self as reference, CapacityPolicy, ClassConfig, ClassReservation, Config, Error, berry,
};
use serde_json::{Value, json};
use std::ops::Range;

pub enum Fixture {
    Reference {
        pool: reference::Pool,
        owner: Option<reference::StorageOwner>,
    },
    Berry {
        pool: berry::Pool,
        owner: berry::StorageOwner,
    },
    #[cfg(feature = "mosaic-spike")]
    Mosaic {
        pool: mosaic::Pool,
        owner: mosaic::StorageOwner,
    },
    Bytes(baseline::Fixture),
}
pub enum Origin {
    Reference {
        domain: reference::Domain,
        _owner: reference::DomainOwner,
    },
    Berry {
        domain: berry::Domain,
        _owner: berry::DomainOwner,
    },
    #[cfg(feature = "mosaic-spike")]
    Mosaic {
        domain: mosaic::Domain,
        _owner: mosaic::DomainOwner,
    },
    Bytes(baseline::Origin),
}
pub enum Lease {
    Reference(reference::BufMut),
    Berry(berry::BufMut),
    #[cfg(feature = "mosaic-spike")]
    Mosaic(mosaic::BufMut),
    Bytes(baseline::Lease),
}
pub enum Frame {
    Reference(reference::Buf),
    Berry(berry::Buf),
    #[cfg(feature = "mosaic-spike")]
    Mosaic(mosaic::Buf),
    Bytes(baseline::Frame),
}

impl Fixture {
    pub fn new(
        backend: &str,
        classes: Vec<ClassConfig>,
        records: usize,
        shared: Option<reference::Pool>,
    ) -> Result<Self> {
        Self::with_policy(
            backend,
            classes,
            records,
            shared,
            CapacityPolicy::Unreserved,
        )
    }
    pub fn with_policy(
        backend: &str,
        classes: Vec<ClassConfig>,
        records: usize,
        shared: Option<reference::Pool>,
        policy: CapacityPolicy,
    ) -> Result<Self> {
        Self::prepared(backend, classes, records, shared, policy, false)
    }
    pub fn prepared(
        backend: &str,
        classes: Vec<ClassConfig>,
        records: usize,
        shared: Option<reference::Pool>,
        policy: CapacityPolicy,
        zero: bool,
    ) -> Result<Self> {
        Self::prepared_branches(backend, classes, records, shared, policy, zero, 0)
    }
    pub fn prepared_branches(
        backend: &str,
        classes: Vec<ClassConfig>,
        records: usize,
        shared: Option<reference::Pool>,
        policy: CapacityPolicy,
        zero: bool,
        branches: usize,
    ) -> Result<Self> {
        Self::prepared_branch_layout(
            backend,
            classes,
            records,
            shared,
            policy,
            zero,
            (branches, isize::MAX as usize),
        )
    }
    /// Cold adapter configuration; (records per slot, local family limit).
    pub fn prepared_branch_layout(
        backend: &str,
        classes: Vec<ClassConfig>,
        records: usize,
        shared: Option<reference::Pool>,
        policy: CapacityPolicy,
        zero: bool,
        layout: (usize, usize),
    ) -> Result<Self> {
        let (branches, local_limit) = layout;
        let _ = local_limit; // feature-independent cold adapter entry point
        if branches != 0 && (backend != "mosaic" || !cfg!(feature = "local-branches-spike")) {
            return Err("local branches require local-branches-spike and mosaic backend".into());
        }
        if zero && backend != "mosaic" {
            return Err("ZeroPayload requires mosaic backend".into());
        }
        if let Some(pool) = shared {
            if backend != "abps" {
                return Err("shared root is reference-only in this harness".into());
            }
            return Ok(Self::Reference { owner: None, pool });
        }
        let mut config = Config::new(classes, records, isize::MAX as usize);
        config.capacity_policy = policy;
        Ok(match backend {
            "abps" => {
                config.root_limit = reference::Reservation::for_config(&config)?.total_bytes;
                let owner = reference::StorageOwner::prepare(config)?;
                let pool = owner.activate()?;
                Self::Reference {
                    owner: Some(owner),
                    pool,
                }
            }
            "berry" => {
                config.root_limit = berry::Reservation::for_config(&config)?.total_bytes;
                let owner = berry::StorageOwner::prepare(&config)?;
                let pool = owner.activate()?;
                Self::Berry { owner, pool }
            }
            #[cfg(feature = "mosaic-spike")]
            "mosaic" => {
                let mut c = mosaic::Config::new(config.classes, records, config.root_limit);
                c.capacity_policy = policy;
                #[cfg(feature = "local-branches-spike")]
                if branches != 0 {
                    c.local_branches = Some(mosaic::LocalBranchConfig {
                        max_branches_per_slot: branches,
                        local_reference_limit: local_limit,
                    });
                }
                if zero {
                    c.preparation = mosaic::PreparationPolicy::ZeroPayload;
                }
                c.root_limit = mosaic::Reservation::for_config(&c)?.total_bytes;
                let owner = mosaic::StorageOwner::prepare(&c)?;
                let pool = owner.activate()?;
                Self::Mosaic { owner, pool }
            }
            "bytes" => Self::Bytes(baseline::Fixture::with_policy(
                "bytes",
                config.classes,
                records,
                policy,
            )?),
            _ => return Err("backend must be abps, berry, enabled mosaic, or bytes".into()),
        })
    }
    pub fn open(&self, limit: usize) -> Result<Origin> {
        self.open_inner(limit, None)
    }
    pub fn open_reserved(&self, limit: usize, grants: &[ClassReservation]) -> Result<Origin> {
        self.open_inner(limit, Some(grants))
    }
    fn open_inner(&self, limit: usize, grants: Option<&[ClassReservation]>) -> Result<Origin> {
        Ok(match self {
            Self::Reference { pool, .. } => {
                let (_owner, domain) = match grants {
                    Some(g) => pool.try_open_reserved_domain(limit, g)?,
                    None => pool.try_open_domain(limit)?,
                };
                Origin::Reference { domain, _owner }
            }
            Self::Berry { pool, .. } => {
                let (_owner, domain) = match grants {
                    Some(g) => pool.try_open_reserved_domain(limit, g)?,
                    None => pool.try_open_domain(limit)?,
                };
                Origin::Berry { domain, _owner }
            }
            #[cfg(feature = "mosaic-spike")]
            Self::Mosaic { pool, .. } => {
                let (_owner, domain) = match grants {
                    Some(g) => pool.try_open_reserved_domain(limit, g)?,
                    None => pool.try_open_domain(limit)?,
                };
                Origin::Mosaic { domain, _owner }
            }
            Self::Bytes(fixture) => Origin::Bytes(match grants {
                Some(g) => fixture.open_reserved(limit, g)?,
                None => fixture.open(limit)?,
            }),
        })
    }
    /// SI-05: one adapter call examines at most 32 records. Retrying this call
    /// is explicit caller policy, never a hidden full FIFO traversal.
    pub async fn open_after_maintenance(&self, limit: usize) -> Result<Origin> {
        #[cfg(feature = "mosaic-spike")]
        if let Self::Mosaic { pool, .. } = self {
            return match pool.try_open_domain(limit) {
                Ok((_owner, domain)) => Ok(Origin::Mosaic { domain, _owner }),
                Err(Error::DomainMaintenanceRequired) => {
                    pool.poll_domains(32);
                    self.open(limit)
                }
                Err(e) => Err(e.into()),
            };
        }
        let Self::Berry { pool, .. } = self else {
            return self.open(limit);
        };
        match pool.try_open_domain(limit) {
            Ok((_owner, domain)) => Ok(Origin::Berry { domain, _owner }),
            Err(Error::DomainMaintenanceRequired) => {
                pool.poll_domains(32);
                self.open(limit)
            }
            Err(error) => Err(error.into()),
        }
    }
    pub fn sample(&self, full: bool) {
        match self {
            Self::Reference { pool, .. } => {
                std::hint::black_box(pool.snapshot());
            }
            Self::Berry { pool, .. } if !full => {
                std::hint::black_box(pool.metrics());
            }
            #[cfg(feature = "mosaic-spike")]
            Self::Mosaic { pool, .. } if !full => {
                std::hint::black_box(pool.metrics());
            }
            Self::Berry { pool, .. } => {
                std::hint::black_box(pool.snapshot());
            }
            #[cfg(feature = "mosaic-spike")]
            Self::Mosaic { pool, .. } => {
                std::hint::black_box(pool.snapshot());
            }
            Self::Bytes(fixture) => {
                std::hint::black_box(fixture.snapshot());
            }
        }
    }
    pub fn snapshot(&self) -> Value {
        match self {
            Self::Reference { pool, .. } => {
                let s = pool.snapshot();
                json!({"reserved_backing":s.reservation.backing_bytes,
                    "metadata":s.reservation.modeled_metadata_bytes,"metadata_is_modeled":true,
                    "admitted":s.domains.iter().map(|d|d.admitted_bytes).sum::<usize>(),
                    "available":s.classes.iter().map(|c|c.available).sum::<usize>(),
                    "assigned_slots":s.classes.iter().map(|c|c.reserved).sum::<usize>(),
                    "unassigned_slots":s.classes.iter().map(|c|c.unassigned).sum::<usize>(),
                    "idle_assigned_slots":s.domains.iter().flat_map(|d|&d.grants).map(|g|g.available).sum::<usize>(),
                    "total_slots":s.classes.iter().map(|c|c.total).sum::<usize>()})
            }
            Self::Berry { pool, .. } => {
                let s = pool.snapshot();
                json!({"reserved_backing":s.reservation.backing_bytes,"metadata":s.reservation.metadata_bytes,
                    "metadata_is_modeled":false,
                    "admitted":s.domains.iter().map(|d|d.observed_admitted_bytes).sum::<usize>(),
                    "available":s.classes.iter().map(|c|c.observed_available_slots).sum::<usize>(),
                    "assigned_slots":s.classes.iter().map(|c|c.reserved).sum::<usize>(),
                    "unassigned_slots":s.classes.iter().map(|c|c.unassigned).sum::<usize>(),
                    "idle_assigned_slots":s.domains.iter().flat_map(|d|&d.grants).map(|g|g.available).sum::<usize>(),
                    "total_slots":s.classes.iter().map(|c|c.total).sum::<usize>()})
            }
            #[cfg(feature = "mosaic-spike")]
            Self::Mosaic { pool, .. } => {
                let s = pool.snapshot();
                json!({"reserved_backing":s.reservation.backing_bytes,"metadata":s.reservation.metadata_bytes,
                    "metadata_is_modeled":false,
                    "admitted":s.domains.iter().map(|d|d.observed_admitted_bytes).sum::<usize>(),
                    "available":s.classes.iter().map(|c|c.observed_available_slots).sum::<usize>(),
                    "assigned_slots":s.classes.iter().map(|c|c.reserved).sum::<usize>(),
                    "unassigned_slots":s.classes.iter().map(|c|c.unassigned).sum::<usize>(),
                    "idle_assigned_slots":s.domains.iter().flat_map(|d|&d.grants).map(|g|g.available).sum::<usize>(),
                    "total_slots":s.classes.iter().map(|c|c.total).sum::<usize>()})
            }
            Self::Bytes(fixture) => fixture.snapshot(),
        }
    }
    pub fn close(&self) {
        match self {
            Self::Reference {
                owner: Some(owner), ..
            } => {
                let _ = owner.close();
            }
            Self::Berry { owner, .. } => {
                let _ = owner.close();
            }
            #[cfg(feature = "mosaic-spike")]
            Self::Mosaic { owner, .. } => {
                let _ = owner.close();
            }
            _ => {}
        }
    }
    pub fn maintenance(&self) -> usize {
        match self {
            Self::Berry { pool, .. } if pool.metrics().pending_domain_records != 0 => {
                pool.poll_domains(32).examined
            }
            #[cfg(feature = "mosaic-spike")]
            Self::Mosaic { pool, .. } if pool.metrics().pending_domain_records != 0 => {
                pool.poll_domains(32).examined
            }
            _ => 0,
        }
    }
    /// SI-16: owner is kept through Busy, then deliberately abandoned at deadline.
    /// No diagnostic handle escapes; the caller may refund only after this returns.
    pub fn shutdown(self, timeout: std::time::Duration) -> (bool, Value) {
        self.close();
        let mut snapshot = self.snapshot();
        let deadline = std::time::Instant::now() + timeout;
        macro_rules! destroy {
            ($owner:expr) => {{
                let mut owner = $owner;
                let destroyed = loop {
                    match owner.try_destroy() {
                        Ok(()) => break true,
                        Err(Error::Busy) if std::time::Instant::now() < deadline => {
                            std::thread::sleep(std::time::Duration::from_millis(1))
                        }
                        _ => break false,
                    }
                };
                drop(owner); // also releases the adapter's diagnostic allocation
                destroyed
            }};
        }
        let destroyed = match self {
            Self::Reference { owner, pool } => {
                drop(pool);
                owner.is_none_or(|o| destroy!(o))
            }
            Self::Berry { owner, pool } => {
                drop(pool);
                destroy!(owner)
            }
            #[cfg(feature = "mosaic-spike")]
            Self::Mosaic { owner, pool } => {
                drop(pool);
                destroy!(owner)
            }
            Self::Bytes(f) => f.finish().is_ok(),
        };
        if destroyed {
            snapshot["admitted"] = json!(0);
            snapshot["available"] = snapshot["total_slots"].clone();
        }
        snapshot["scope"] = json!(if destroyed {
            "confirmed drain"
        } else {
            "before abandonment"
        });
        (destroyed, snapshot)
    }
    pub fn finish(self) -> Result<Value> {
        let snapshot = self.snapshot();
        assert_eq!(snapshot["admitted"], 0);
        assert_eq!(snapshot["available"], snapshot["total_slots"]);
        match self {
            Self::Reference { owner, pool } => {
                drop(pool);
                if let Some(mut owner) = owner {
                    owner.try_destroy()?;
                }
            }
            Self::Berry { mut owner, pool } => {
                drop(pool);
                owner.try_destroy()?;
            }
            #[cfg(feature = "mosaic-spike")]
            Self::Mosaic { mut owner, pool } => {
                drop(pool);
                owner.try_destroy()?;
            }
            Self::Bytes(fixture) => {
                fixture.finish()?;
            }
        }
        Ok(snapshot)
    }
}
impl Origin {
    pub fn acquire(&self, size: usize) -> std::result::Result<Lease, Error> {
        match self {
            Self::Reference { domain, .. } => domain.try_acquire(size).map(Lease::Reference),
            Self::Berry { domain, .. } => domain.try_acquire(size).map(Lease::Berry),
            #[cfg(feature = "mosaic-spike")]
            Self::Mosaic { domain, .. } => domain.try_acquire(size).map(Lease::Mosaic),
            Self::Bytes(origin) => origin.acquire(size).map(Lease::Bytes),
        }
    }
}
impl Lease {
    pub fn put(&mut self, bytes: &[u8]) {
        match self {
            Self::Reference(b) => b.put_slice(bytes).unwrap(),
            Self::Berry(b) => b.put_slice(bytes).unwrap(),
            #[cfg(feature = "mosaic-spike")]
            Self::Mosaic(b) => b.put_slice(bytes).unwrap(),
            Self::Bytes(b) => b.put(bytes),
        }
    }
    pub fn initialize(&mut self, len: usize) -> &mut [u8] {
        match self {
            Self::Reference(b) => b.initialize_to(len).unwrap(),
            Self::Berry(b) => b.initialize_to(len).unwrap(),
            #[cfg(feature = "mosaic-spike")]
            Self::Mosaic(b) => b.initialize_to(len).unwrap(),
            Self::Bytes(b) => b.initialize(len),
        }
    }
    pub fn set_filled(&mut self, len: usize) {
        match self {
            Self::Reference(b) => b.set_filled_len(len).unwrap(),
            Self::Berry(b) => b.set_filled_len(len).unwrap(),
            #[cfg(feature = "mosaic-spike")]
            Self::Mosaic(b) => b.set_filled_len(len).unwrap(),
            Self::Bytes(b) => b.set_filled(len),
        }
    }
    pub fn freeze(self) -> Frame {
        match self {
            Self::Reference(b) => Frame::Reference(b.freeze()),
            Self::Berry(b) => Frame::Berry(b.freeze()),
            #[cfg(feature = "mosaic-spike")]
            Self::Mosaic(b) => Frame::Mosaic(b.freeze()),
            Self::Bytes(b) => Frame::Bytes(b.freeze()),
        }
    }
}
impl Frame {
    pub fn borrowed(&self, range: Range<usize>) -> std::result::Result<&[u8], Error> {
        #[cfg(feature = "mosaic-spike")]
        if let Self::Mosaic(b) = self {
            b.try_view(range.clone())?;
        }
        self.as_ref().get(range).ok_or(Error::InvalidRange)
    }

    /// SI-10 adapter: bounded scratch, no owning Bytes conversion. Destination
    /// validation precedes every backend and nothing is exposed on an error.
    pub fn try_clone_into(&self, dst: &mut [Option<Self>]) -> std::result::Result<(), Error> {
        if dst.iter().any(Option::is_some) {
            return Err(Error::OutputNotEmpty);
        }
        if dst.len() > 20 {
            return Err(Error::InvalidSize);
        }
        if dst.is_empty() {
            return Ok(());
        }
        if dst.len() == 1 {
            dst[0] = Some(self.view(0..self.as_ref().len())?);
            return Ok(());
        }
        macro_rules! batch {
            ($frame:expr, $ty:ty, $variant:ident) => {{
                let mut ready: [Option<$ty>; 20] = std::array::from_fn(|_| None);
                $frame.try_clone_into(&mut ready[..dst.len()])?;
                for (out, b) in dst.iter_mut().zip(&mut ready) {
                    *out = b.take().map(Self::$variant);
                }
            }};
        }
        match self {
            Self::Berry(b) => batch!(b, berry::Buf, Berry),
            #[cfg(feature = "mosaic-spike")]
            Self::Mosaic(b) => batch!(b, mosaic::Buf, Mosaic),
            Self::Reference(b) => batch!(b, reference::Buf, Reference),
            Self::Bytes(b) => {
                let mut ready: [Option<Self>; 20] = std::array::from_fn(|_| None);
                for out in &mut ready[..dst.len()] {
                    *out = Some(Self::Bytes(b.view(0..b.as_ref().len())?));
                }
                for (out, b) in dst.iter_mut().zip(&mut ready) {
                    *out = b.take();
                }
            }
        }
        Ok(())
    }
    pub fn view(&self, range: Range<usize>) -> std::result::Result<Self, Error> {
        match self {
            Self::Reference(b) => b.try_slice(range).map(Self::Reference),
            Self::Berry(b) => b.try_slice(range).map(Self::Berry),
            #[cfg(feature = "mosaic-spike")]
            Self::Mosaic(b) => b.try_slice(range).map(Self::Mosaic),
            Self::Bytes(b) => b.view(range).map(Self::Bytes),
        }
    }
}
impl AsRef<[u8]> for Frame {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Reference(b) => b.as_ref(),
            Self::Berry(b) => b.as_ref(),
            #[cfg(feature = "mosaic-spike")]
            Self::Mosaic(b) => b.as_ref(),
            Self::Bytes(b) => b.as_ref(),
        }
    }
}

#[cfg(test)]
mod maintenance_tests {
    use super::*;
    #[tokio::test]
    async fn explicit_adapter_stops_after_32_records_even_when_later_ready() {
        let fixture = Fixture::new(
            "berry",
            vec![ClassConfig {
                capacity: 16,
                slots: 64,
            }],
            40,
            None,
        )
        .unwrap();
        let mut retained = Vec::new();
        for _ in 0..40 {
            let origin = fixture.open(16).unwrap();
            retained.push(origin.acquire(1).unwrap());
            drop(origin);
        }
        retained.truncate(33);
        let error = fixture.open_after_maintenance(16).await.err().unwrap();
        assert_eq!(
            error.downcast_ref::<Error>(),
            Some(&Error::DomainMaintenanceRequired)
        );
        let Fixture::Berry { pool, .. } = &fixture else {
            unreachable!()
        };
        assert_eq!(pool.metrics().pending_domain_records, 40);
        let origin = fixture.open_after_maintenance(16).await.unwrap();
        assert!(fixture.open(16).is_ok());
        drop((origin, retained));
        fixture.finish().unwrap();
    }
}
