//! Resource/lifecycle soak, not a timing benchmark. One issuer, bounded remote
//! retention, repeated reserved-domain replacement, and cold periodic observations.
use super::pin_worker;
use crate::perf_support::{Args, Result};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    sync::mpsc,
    time::{Duration, Instant},
};

macro_rules! backend {
    ($name:ident, $backend:path) => {
fn $name(args: &Args) -> Result<Value> {
    use $backend as berry;
    use berry::{CapacityPolicy,ClassConfig,ClassReservation,Config,Error};
    pin_worker(args, 0)?;
    let mut config = Config::new(
        vec![
            ClassConfig {
                capacity: 512,
                slots: 32,
            },
            ClassConfig {
                capacity: 2 * 1024 * 1024,
                slots: 32,
            },
        ],
        16,
        usize::MAX,
    );
    config.capacity_policy = CapacityPolicy::Reserved;
    config.root_limit = berry::Reservation::for_config(&config)?.total_bytes;
    let mut owner = berry::StorageOwner::prepare(&config)?;
    let pool = owner.activate()?;
    let grants = [
        ClassReservation {
            capacity: 512,
            slots: 4,
        },
        ClassReservation {
            capacity: 2 * 1024 * 1024,
            slots: 4,
        },
    ];
    let limit = grants.iter().map(|g| g.capacity * g.slots).sum();
    let mut domains: Vec<_> = (0..8)
        .map(|_| pool.try_open_reserved_domain(limit, &grants).map(Some))
        .collect::<std::result::Result<_, _>>()?;
    let (tx, rx) = mpsc::sync_channel::<berry::Buf>(32);
    let remote_args = args.clone();
    let worker = std::thread::spawn(move || -> Result<u64> {
        pin_worker(&remote_args, 1)?;
        let mut retained = VecDeque::with_capacity(16);
        let mut checked = 0;
        loop {
            match rx.recv_timeout(Duration::from_millis(1)) {
                Ok(b) => {
                    assert_eq!(b.as_ref().first(), Some(&0x5a));
                    assert_eq!(b.as_ref().last(), Some(&0x5a));
                    if retained.len() == 16 {
                        retained.pop_front();
                    }
                    retained.push_back(b);
                    checked += 1;
                    if checked % 31 == 0 {
                        retained.clear();
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => retained.clear(),
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
        drop(retained);
        Ok(checked)
    });
    let payload = vec![0x5a; 2 * 1024 * 1024];
    let start = Instant::now();
    let duration = args.seconds("seconds", 1800.0);
    let rate = args.n("rate", 2000);
    if rate == 0 || rate > 100_000 {
        return Err("soak rate must be 1..100000".into());
    }
    let mut state = args.n("seed", 42) as u64 | 1;
    let (mut iterations, mut accepted, mut replacements, mut refused, mut queue_full) =
        (0u64, 0u64, 0u64, 0u64, 0u64);
    let mut samples = Vec::new();
    let mut next_sample = Duration::ZERO;
    while start.elapsed() < duration {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let index = state as usize % domains.len();
        if iterations % 17 == 0 {
            domains[index].take();
        }
        pool.poll_domains(32);
        if domains[index].is_none() {
            match pool.try_open_reserved_domain(limit, &grants) {
                Ok(d) => {
                    domains[index] = Some(d);
                    replacements += 1;
                }
                Err(Error::ReservationUnavailable | Error::DomainMaintenanceRequired) => {
                    refused += 1
                }
                Err(e) => return Err(e.into()),
            }
        }
        if let Some((_, domain)) = &domains[index] {
            let size = if state.is_multiple_of(20) {
                payload.len()
            } else {
                512
            };
            match domain.try_acquire(size) {
                Ok(mut b) => {
                    b.put_slice(&payload[..size])?;
                    let b = b.freeze();
                    let view = b.try_slice(0..size.min(128))?;
                    drop(b);
                    match tx.try_send(view) {
                        Ok(()) => accepted += 1,
                        Err(mpsc::TrySendError::Full(_)) => queue_full += 1,
                        Err(mpsc::TrySendError::Disconnected(_)) => {
                            return Err("soak consumer stopped".into());
                        }
                    }
                }
                Err(Error::BudgetExceeded | Error::DomainClassDepleted | Error::Contended) => {
                    refused += 1
                }
                Err(e) => return Err(e.into()),
            }
        }
        iterations += 1;
        let elapsed = start.elapsed();
        if elapsed >= next_sample {
            let s = pool.snapshot();
            assert!(s.domains.iter().all(|d| d.observed_admitted_bytes <= d.limit));
            assert!(
                s.classes
                    .iter()
                    .all(|c| c.reserved + c.unassigned == c.total)
            );
            assert_eq!(s.reservation.total_bytes, config.root_limit);
            let rss = std::fs::read_to_string("/proc/self/status")
                .ok()
                .and_then(|s| {
                    s.lines()
                        .find(|l| l.starts_with("VmRSS:"))
                        .map(str::to_owned)
                });
            samples.push(json!({"seconds":elapsed.as_secs_f64(),"rss":rss,"admitted":s.domains.iter().map(|d|d.observed_admitted_bytes).sum::<usize>(),"pending_domains":s.pending_domain_records,"free_slots":s.classes.iter().map(|c|c.observed_available_slots).sum::<usize>()}));
            next_sample = elapsed + Duration::from_secs(5);
        }
        let due = Duration::from_secs_f64(iterations as f64 / rate as f64);
        if let Some(wait) = due.checked_sub(start.elapsed()) {
            std::thread::sleep(wait);
        }
    }
    drop((domains, tx));
    let checked = worker.join().map_err(|_| "soak consumer panic")??;
    assert_eq!(checked, accepted);
    pool.poll_domains(32);
    let final_state = pool.snapshot();
    assert!(final_state.domains.iter().all(|d| d.observed_admitted_bytes == 0));
    assert!(
        final_state
            .classes
            .iter()
            .all(|c| c.observed_available_slots == c.total && c.unassigned == c.total)
    );
    assert_eq!(final_state.free_domain_records, 16);
    drop(pool);
    owner.try_destroy()?;
    Ok(
        json!({"backend_implementation":std::any::type_name::<berry::Buf>(),"resource_soak_only":true,"seconds":start.elapsed().as_secs_f64(),"iterations":iterations,"remote_returns":checked,"domain_replacements":replacements,"refused":refused,"queue_full":queue_full,"reservation_bytes":config.root_limit,"samples":samples,"drained":true}),
    )
}
    };
}
backend!(berry, abps_reference::berry);
#[cfg(feature = "mosaic-spike")]
backend!(mosaic, abps_reference::mosaic);
pub fn run(args: &Args) -> Result<Value> {
    match args.get("backend", "berry") {
        "berry" => berry(args),
        #[cfg(feature = "mosaic-spike")]
        "mosaic" => mosaic(args),
        _ => Err("soak supports only enabled berry or mosaic backends".into()),
    }
}
