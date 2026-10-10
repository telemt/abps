//! Explicit cold layout and worst-case depleted-path measurements. No allocations
//! occur inside the measured search; preparation and teardown are separate.
#![forbid(unsafe_code)]
#[allow(dead_code)]
mod perf_support;
use perf_support::{Args, Result, metrics::Latency};
use serde_json::{Value, json};
use std::{hint::black_box, time::Instant};

macro_rules! backend {
    ($name:ident, $path:path, $max_words:literal, $other_rmws:literal) => {
        fn $name(args: &Args) -> Result<Value> {
            use $path as b;
            let words=args.n("words",1);
            let classes=args.n("classes",1);
            let records=args.n("records",1);
            let slots=words.checked_mul(usize::BITS as usize).ok_or("slot overflow")?;
            let count=args.n("iterations",10000);
            if !(1..=$max_words).contains(&words) || !(1..=64).contains(&classes)
                || !(1..=65536).contains(&records) || !(1..=1000000).contains(&count) {
                return Err("invalid probe dimensions".into());
            }
            let mut cfg=b::Config::new((0..classes).map(|i|b::ClassConfig{capacity:64+i,slots}).collect(),records,usize::MAX);
            cfg.capacity_policy=match args.get("capacity-policy","unreserved") {
                "unreserved"=>b::CapacityPolicy::Unreserved,"reserved"=>b::CapacityPolicy::Reserved,
                _=>return Err("invalid capacity policy".into()),
            };
            let r=b::Reservation::for_config(&cfg)?;
            let layout=json!({"backing_bytes":r.backing_bytes,"metadata_bytes":r.metadata_bytes,"total_bytes":r.total_bytes});
            if args.get("case","empty")=="layout" {return Ok(layout);}
            if args.get("case","empty")!="empty" || classes!=1 {return Err("empty requires one class".into());}
            let mut owner=b::StorageOwner::prepare(&cfg)?;let pool=owner.activate()?;
            let limit=64*(slots+1);
            let (a,d)=if cfg.capacity_policy==b::CapacityPolicy::Reserved {
                pool.try_open_reserved_domain(limit,&[b::ClassReservation{capacity:64,slots}])?
            }else{pool.try_open_domain(limit)?};
            let held:Vec<_>=(0..slots).map(|_|d.try_acquire(1)).collect::<std::result::Result<_,_>>()?;
            let expected=if cfg.capacity_policy==b::CapacityPolicy::Reserved {b::Error::DomainClassDepleted}else{b::Error::ClassDepleted};
            for _ in 0..100 {assert_eq!(d.try_acquire(1).err(),Some(expected));}
            let mut latency=Latency::default();let start=Instant::now();
            for _ in 0..count {let at=Instant::now();assert_eq!(black_box(d.try_acquire(1)).err(),Some(expected));latency.record(at.elapsed());}
            let elapsed=start.elapsed();assert_eq!(d.metrics().admitted_bytes,slots*64);
            drop((held,d,a,pool));owner.try_destroy()?;
            Ok(json!({"layout":layout,"words":words,"ordinary_probe_bound":2*words,
                "ordinary_rmw_bound":words+$other_rmws,
                "rmw_bound_scope":"complete ordinary acquire, including admission and success commit or failure rollback; excludes injected rollback after claim",
                "operations":count,"latency":latency.json(),
                "attempts_per_s":count as f64/elapsed.as_secs_f64(),"drained":true}))
        }
    };
}
// SI-12: W confirmation RMWs plus admission and claim (or refusal refund).
backend!(mosaic, abps_reference::mosaic, 256, 2);
#[cfg(feature = "berry-spike")]
// SI-01/02: one confirmation plus root pin, admission and claim/commit on
// success, or admission refund/root release on failure. Berry has one word.
backend!(berry, abps_reference::berry, 1, 4);
// Linux probe only: counters include the small /proc observer cost, identically
// for both policies. No unsafe OS interface is introduced into the core.
fn process_memory() -> Result<(u64, u64, u64)> {
    let stat = std::fs::read_to_string("/proc/self/stat")?;
    let fields: Vec<_> = stat[stat.rfind(')').ok_or("invalid stat")? + 2..]
        .split_whitespace()
        .collect();
    let status = std::fs::read_to_string("/proc/self/status")?;
    let rss = status
        .lines()
        .find_map(|s| s.strip_prefix("VmRSS:"))
        .ok_or("missing RSS")?
        .split_whitespace()
        .next()
        .unwrap()
        .parse()?;
    Ok((fields[7].parse()?, fields[9].parse()?, rss))
}
fn preparation(args: &Args) -> Result<Value> {
    use abps_reference::mosaic as b;
    let size = args.n("size", 1 << 20);
    let slots = args.n("slots", 64);
    if size == 0 || slots == 0 || size.checked_mul(slots).is_none_or(|n| n > 1 << 30) {
        return Err("preparation probe backing must be 1 byte..1 GiB".into());
    }
    let mut config = b::Config::new(
        vec![b::ClassConfig {
            capacity: size,
            slots,
        }],
        1,
        usize::MAX,
    );
    config.preparation = match args.get("preparation", "lazy") {
        "lazy" => b::PreparationPolicy::Lazy,
        "zero-payload" => b::PreparationPolicy::ZeroPayload,
        _ => return Err("invalid preparation".into()),
    };
    let reservation = b::Reservation::for_config(&config)?;
    let mut held = Vec::with_capacity(slots);
    let before = process_memory()?;
    let at = Instant::now();
    let mut owner = b::StorageOwner::prepare(&config)?;
    let elapsed = at.elapsed();
    let after = process_memory()?;
    let report = |name: &str, ns: u128, a: (u64, u64, u64), z: (u64, u64, u64)| json!({"phase":name,"ns":ns,"minor_faults":z.0-a.0,"major_faults":z.1-a.1,"rss_before_kib":a.2,"rss_after_kib":z.2});
    let mut phases = vec![report("prepare", elapsed.as_nanos(), before, after)];
    let pool = owner.activate()?;
    let (a, d) = pool.try_open_domain(size * slots)?;
    for name in ["first_pass", "warm_pass"] {
        let before = process_memory()?;
        let at = Instant::now();
        for _ in 0..slots {
            let mut b = d.try_acquire(size)?;
            black_box(b.initialize_to(size)?);
            b.set_filled_len(size)?;
            held.push(b.freeze());
        }
        let elapsed = at.elapsed();
        let after = process_memory()?;
        phases.push(report(name, elapsed.as_nanos(), before, after));
        held.clear();
    }
    drop((d, a, pool));
    owner.try_destroy()?;
    Ok(
        json!({"preparation":format!("{:?}",config.preparation),"backing_bytes":reservation.backing_bytes,"total_bytes":reservation.total_bytes,"phases":phases,"drained":true,"fault_scope":"process counters including /proc observer overhead; not page residency proof"}),
    )
}
fn main() -> Result<()> {
    let args = Args::parse()?;
    let result = if args.get("case", "empty") == "preparation" {
        preparation(&args)?
    } else {
        match args.get("backend", "mosaic") {
            "mosaic" => mosaic(&args)?,
            #[cfg(feature = "berry-spike")]
            "berry" => berry(&args)?,
            _ => return Err("backend must be mosaic or enabled berry".into()),
        }
    };
    println!("{}", json!({"args":args.0,"result":result}));
    Ok(())
}
