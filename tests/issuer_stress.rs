//! Native scheduling evidence, not a progress or memory-order proof.
#![forbid(unsafe_code)]
#![cfg(any(feature = "berry-spike", feature = "mosaic-spike"))]
use std::{
    collections::VecDeque,
    sync::mpsc,
    time::{Duration, Instant},
};
fn rounds() -> usize {
    std::env::var("ABPS_STRESS_ROUNDS")
        .map(|v| v.parse().unwrap())
        .unwrap_or(if cfg!(miri) { 4 } else { 1000 })
}
macro_rules! suite {
    ($module:ident, $path:path, $slots:expr) => {
        mod $module {
            use super::*;
            use $path as b;
            fn run(slots: usize, returners: usize) {
                let mut cfg = b::Config::new(vec![b::ClassConfig {capacity:64, slots}, b::ClassConfig {capacity:256,slots}],128,4<<20);
                cfg.reference_limit = 32;
                let mut owner = b::StorageOwner::prepare(&cfg).unwrap();
                let pool = owner.activate().unwrap();
                let mut domains: Vec<_> = (0..100).map(|_| Some(pool.try_open_domain(512).unwrap())).collect();
                let reserved = owner.reservation().unwrap();
                let workers: Vec<_> = (0..returners).map(|id| {
                    let (tx, rx) = mpsc::sync_channel::<b::Buf>(4);
                    let worker = std::thread::spawn(move || {
                        let mut retained = VecDeque::with_capacity(3);
                        let mut checked = 0;
                        loop {
                            match rx.recv_timeout(Duration::from_millis(1)) {
                                Ok(frame) => {
                                    assert!(frame.as_ref().iter().all(|v| *v == frame.as_ref()[0]), "OWN-03 overwritten remote payload");
                                    match frame.try_slice(0..frame.len().min(8)) {
                                        Ok(view) => assert_eq!(view.as_ref(), &frame.as_ref()[..view.len()]),
                                        Err(b::Error::Contended) => {},
                                        other => panic!("unexpected slice: {other:?}"),
                                    }
                                    checked += 1;
                                    if id == 0 { if retained.len() == 3 { retained.pop_front(); } retained.push_back(frame); }
                                    if checked % 7 == 0 { retained.clear(); }
                                }
                                Err(mpsc::RecvTimeoutError::Timeout) => retained.clear(),
                                Err(mpsc::RecvTimeoutError::Disconnected) => break,
                            }
                        }
                        drop(retained);
                        checked
                    });
                    (tx,worker)
                }).collect();
                let deadline = Instant::now() + Duration::from_secs(120);
                let (mut enqueued,mut rejected,mut failures) = (0usize,0usize,0usize);
                let (mut i, mut completed) = (0usize,0usize);
                while completed < rounds() {
                    i += 1;
                    assert!(Instant::now() < deadline, "native stress deadline");
                    let d = i % domains.len();
                    if i % 13 == 0 { domains[d].take(); }
                    pool.poll_domains(32);
                    if domains[d].is_none() {
                        match pool.try_open_domain(512) {
                            Ok(domain) => domains[d] = Some(domain),
                            Err(b::Error::DomainMaintenanceRequired) => { failures+=1; continue; },
                            other => panic!("domain open: {:?}",other.err()),
                        }
                    }
                    let domain = &domains[d].as_ref().unwrap().1;
                    match domain.try_acquire(if i % 8 == 0 {256} else {64}) {
                        Ok(mut lease) => {
                            completed += 1;
                            let len = lease.capacity(); lease.put_slice(&vec![i as u8;len]).unwrap();
                            let frame = lease.freeze();
                            let mut views: Vec<_> = (0..returners).map(|_| None).collect();
                            frame.try_clone_into(&mut views).unwrap();
                            for ((tx,_), view) in workers.iter().zip(views) {
                                match tx.try_send(view.unwrap()) {
                                    Ok(()) => enqueued+=1,
                                    Err(mpsc::TrySendError::Full(_)) => rejected+=1,
                                    Err(mpsc::TrySendError::Disconnected(_)) => panic!("returner failed"),
                                }
                            }
                        }
                        Err(b::Error::BudgetExceeded | b::Error::ClassDepleted | b::Error::Contended) => { failures+=1; std::thread::yield_now(); },
                        Err(e) => panic!("unexpected acquire: {e:?}"),
                    }
                    assert!(domain.metrics().admitted_bytes<=512,"MEM-02");
                    assert_eq!(owner.reservation().unwrap(),reserved,"MEM-01");
                }
                let (txs,threads): (Vec<_>,Vec<_>) = workers.into_iter().unzip();
                drop((txs,domains));
                let consumed: usize = threads.into_iter().map(|t| t.join().unwrap()).sum();
                assert!(enqueued >= returners);
                assert_eq!(consumed,enqueued,"IO-03 accepted work accounted");
                for _ in 0..4 { pool.poll_domains(32); }
                let s=pool.snapshot();
                assert_eq!(s.free_domain_records,128,"LIFE-01 complete recycle");
                assert!(s.classes.iter().all(|c|c.observed_available_slots==c.total),"SI-01/12 no lost slot");
                assert!(s.domains.iter().all(|d|d.observed_admitted_bytes==0),"MEM-02 no lost charge");
                drop(pool); owner.try_destroy().unwrap();
                eprintln!("ABPS_STRESS backend={} slots={slots} returners={returners} accepted={enqueued} queue_rejected={rejected} admission_refused={failures} drained=true", stringify!($module));
            }
            #[test]
            fn own03_life02_repeated_slot_and_remote_domain_churn() {
                let counts: &[usize] = if cfg!(miri) { &[2] } else { &[1,2,4,8,16] };
                for &n in counts { run(1,n); run($slots,n); }
            }
        }
    };
}
#[cfg(feature = "berry-spike")]
suite!(berry, abps_reference::berry, usize::BITS as usize);
#[cfg(feature = "mosaic-spike")]
suite!(mosaic, abps_reference::mosaic, 2 * usize::BITS as usize + 1);

#[cfg(feature = "local-branches-spike")]
#[test]
fn br03_remote_families_repeatedly_reuse_one_sidecar_set() {
    use abps_reference::mosaic as b;
    let mut cfg = b::Config::new(
        vec![b::ClassConfig {
            capacity: 64,
            slots: 1,
        }],
        1,
        1 << 20,
    );
    cfg.local_branches = Some(b::LocalBranchConfig {
        max_branches_per_slot: 3,
        local_reference_limit: 8,
    });
    let mut owner = b::StorageOwner::prepare(&cfg).unwrap();
    let pool = owner.activate().unwrap();
    let (authority, domain) = pool.try_open_domain(64).unwrap();
    let workers: Vec<_> = (0..3)
        .map(|_| {
            let (tx, rx) = mpsc::sync_channel::<b::Branch>(2);
            let worker = std::thread::spawn(move || {
                let mut count = 0;
                while let Ok(token) = rx.recv() {
                    let local = token.into_local();
                    let child = local.try_clone().unwrap();
                    let tail = child.try_slice(1..63).unwrap();
                    drop((local, child));
                    assert!(
                        tail.as_ref().iter().all(|x| *x == tail.as_ref()[0]),
                        "BR-03 overwritten family payload"
                    );
                    let token = tail.try_into_branch().unwrap();
                    assert_eq!(token.as_ref().len(), 62);
                    drop(token);
                    count += 1;
                }
                count
            });
            (tx, worker)
        })
        .collect();
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut issued = 0;
    while issued < rounds() {
        assert!(Instant::now() < deadline, "family stress deadline");
        match domain.try_acquire(64) {
            Ok(mut lease) => {
                lease.put_slice(&[issued as u8; 64]).unwrap();
                let mut dst = [None, None, None];
                lease.freeze_branches_into(&mut dst).unwrap();
                for ((tx, _), token) in workers.iter().zip(dst) {
                    tx.send(token.unwrap()).unwrap();
                }
                issued += 1;
            }
            Err(b::Error::Contended | b::Error::ClassDepleted | b::Error::BudgetExceeded) => {
                std::thread::yield_now()
            }
            Err(e) => panic!("branch acquire: {e:?}"),
        }
    }
    let (txs, threads): (Vec<_>, Vec<_>) = workers.into_iter().unzip();
    drop((txs, authority, domain));
    let count: usize = threads.into_iter().map(|t| t.join().unwrap()).sum();
    assert_eq!(count, 3 * issued);
    pool.poll_domains(1);
    assert_eq!(pool.snapshot().classes[0].observed_available_slots, 1);
    drop(pool);
    owner.try_destroy().unwrap();
}
