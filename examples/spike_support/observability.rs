//! Cold diagnostics and saturated admission are separate measured operations.
//! Local smoke validates harness accounting, never a dedicated-host result.
use crate::perf_support::{
    Args, Result,
    metrics::{Counts, Latency},
};
use abps_reference::{CapacityPolicy, ClassConfig, ClassGrantSnapshot, ClassReservation, Error};
use serde_json::{Value, json};
use std::{hint::black_box, sync::mpsc, time::Instant};

macro_rules! backend {
    ($name:ident,$api:path) => {
        fn $name(args:&Args)->Result<Value> {
            use $api as api;
            super::pin_worker(args,0)?;
            let classes=args.n("classes",2); let records=args.n("records",256);
            let slots=args.n("class-slots",usize::BITS as usize); let seeds=args.n("active-domains",1);
            let count=args.n("iterations",10000); let page_size=args.n("page-size",32);
            let interval=args.n("diagnostic-interval",32);
            let fragmented=match args.get("fragmentation","contiguous") {
                "contiguous"=>false,"alternating"=>true,_=>return Err("invalid fragmentation".into())
            };
            let op=args.get("op","grants");
            if !(1..=64).contains(&classes) || !(2..=65536).contains(&records)
                || !(1..=api::MAX_SLOTS_PER_CLASS).contains(&slots) || seeds==0 || seeds>=records || seeds>slots
                || !(1..=4096).contains(&page_size) || count==0
                || !["grants","pages","busy-destroy","mixed"].contains(&op) {
                return Err("invalid observability dimensions".into());
            }
            let mut config=api::Config::new((0..classes).map(|i|ClassConfig{capacity:64*(i+1),slots}).collect(),records,usize::MAX);
            config.capacity_policy=CapacityPolicy::Reserved;
            let one:Vec<_>=config.classes.iter().map(|c|ClassReservation{capacity:c.capacity,slots:1}).collect();
            let mut owner=api::StorageOwner::prepare(&config)?; let pool=owner.activate()?;
            let mut neighbors=Vec::with_capacity(seeds);
            for _ in 0..seeds { neighbors.push(Some(pool.try_open_reserved_domain(usize::MAX,&one)?)); }
            for (i,d) in neighbors.iter_mut().enumerate() {
                if (fragmented && i%2==0) || (!fragmented && i==seeds-1) { drop(d.take()); }
            }
            let retained=neighbors.iter().flatten().count(); let grant_slots=slots-retained;
            let grants:Vec<_>=config.classes.iter().map(|c|ClassReservation{capacity:c.capacity,slots:grant_slots}).collect();
            let main=pool.try_open_reserved_domain(usize::MAX,&grants)?;
            let mut page=vec![api::DomainMetrics::default();page_size];
            let mut rows=vec![ClassGrantSnapshot{capacity:0,reserved:0,available:0};classes];
            let mut latency=Latency::default(); let mut counts=Counts::default();
            let mut cursor=0; let mut diagnostics=0u64; let mut queue_rejected=0u64;
            let mut final_drops=0u64;
            let start=Instant::now();
            if op=="busy-destroy" {
                let hold=main.1.try_acquire(1)?.freeze();
                drop((main,neighbors,pool)); // owner is the sole control; ledgers are actually inspected
                for _ in 0..count {
                    let at=Instant::now(); assert_eq!(owner.try_destroy(),Err(Error::Busy));
                    latency.record(at.elapsed());diagnostics+=1;
                }
                drop(hold);
            } else {
                let mut sample=|| -> Result<()> {
                    let at=Instant::now();
                    if op=="pages" {
                        let p=pool.domain_metrics_page(cursor,&mut page)?;
                        black_box(&page[..p.written]); cursor=if p.done {0} else {p.next_index};
                    } else {
                        assert_eq!(main.1.class_grants_into(&mut rows)?,classes);
                        black_box(&rows);
                    }
                    latency.record(at.elapsed());diagnostics+=1;Ok(())
                };
                if op=="mixed" {
                    let (tx,rx)=mpsc::sync_channel::<api::Buf>(slots);
                    let (ready_tx,ready_rx)=mpsc::sync_channel(1);
                    let worker=std::thread::spawn(move || {
                        let mut n=0;
                        for _ in 0..grant_slots { drop(rx.recv().expect("initial batch")); n+=1; }
                        ready_tx.send(()).expect("startup receiver");
                        while let Ok(b)=rx.recv() { drop(b);n+=1; } n
                    });
                    // Force a real capacity boundary before racing returns.
                    let held:Vec<_>=(0..grant_slots).map(|_|main.1.try_acquire(1).unwrap().freeze()).collect();
                    let at=Instant::now(); let exhausted=main.1.try_acquire(1);
                    assert_eq!(exhausted.err(),Some(Error::DomainClassDepleted));
                    counts.result(Err(Error::DomainClassDepleted),0,Some(at.elapsed()));
                    for b in held { tx.send(b)?; }
                    // Startup only: guarantee a completed return before measuring
                    // short loops. A ready worker may still be descheduled later.
                    ready_rx.recv_timeout(std::time::Duration::from_secs(10))?;
                    for i in 0..count {
                        if interval!=0 && i%interval==0 { sample()?; }
                        let at=Instant::now();
                        match main.1.try_acquire(1) {
                            Ok(b)=>{
                                counts.result(Ok(()),0,Some(at.elapsed()));
                                if let Err(e)=tx.try_send(b.freeze()) {
                                    match e {mpsc::TrySendError::Full(b)=>{queue_rejected+=1;drop(b);},
                                        mpsc::TrySendError::Disconnected(_)=>return Err("returner disconnected".into())}
                                }
                            },
                            Err(e)=>counts.result(Err(e),0,Some(at.elapsed()))
                        }
                    }
                    drop(tx); final_drops=worker.join().map_err(|_|"returner panicked")?;
                    assert!(counts.accepted>0,"mixed probe must exercise successful admission");
                    assert_eq!(final_drops+queue_rejected,counts.accepted+grant_slots as u64);
                    assert_eq!(main.1.metrics().admitted_bytes,0);
                } else { for _ in 0..count { sample()?; } }
                drop((main,neighbors,pool));
            }
            let elapsed=start.elapsed(); owner.try_destroy()?;
            Ok(json!({"scope":"local-harness-smoke","op":op,"prepared_domains":records,
                "active_domains":retained+1,"seed_domains":seeds,"classes":classes,"slots_per_class":slots,
                "fragmentation":if fragmented {"alternating"} else {"contiguous"},"main_grant_slots_per_class":grant_slots,
                "diagnostic_attempt_interval":interval,"page_size":page_size,"diagnostic_samples":diagnostics,
                "diagnostic_latency":latency.json(),"admission":counts.json(elapsed),
                "requested_bytes":counts.accepted,"charged_bytes":64*counts.accepted,
                "request_bytes":1,"charged_capacity":64,"traffic_class":0,"returner_threads":1,
                "queue_rejected":queue_rejected,"remote_final_drops":final_drops,
                "elapsed_seconds":elapsed.as_secs_f64(),"drained":true,
                "limits":"empty payload; startup, drain, bounded channel and clock overhead included; no dedicated-host or end-to-end claim"}))
        }
    }
}
backend!(berry, abps_reference::berry);
#[cfg(feature = "mosaic-spike")]
backend!(mosaic, abps_reference::mosaic);
pub fn run(args: &Args) -> Result<Value> {
    match args.get("backend", "berry") {
        "berry" => berry(args),
        #[cfg(feature = "mosaic-spike")]
        "mosaic" => mosaic(args),
        _ => Err("unsupported observability backend".into()),
    }
}
