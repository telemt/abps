//! AP-14 measurement through actual Bank/Handle admission, not a predicate model.
use super::*;
use std::hint::black_box;
pub fn run(args: &Args) -> Result<Value> {
    let classes = args.n("classes", 2);
    let streams = args.n("streams", 8);
    let iterations = args.n("iterations", 1000000);
    if ![2, 8, 32, 63, 64].contains(&classes)
        || ![1, 8, 32].contains(&streams)
        || !(1..=200000000).contains(&iterations)
    {
        return Err("unsupported memory-cycle dimensions".into());
    }
    let options = Options::parse(args, streams)?.ok_or("shared policy required")?;
    if options.mode != Mode::Shared {
        return Err("memory-cycle requires adaptive-shared".into());
    }
    let mut config = abps_reference::mosaic::Config::new(
        (1..=classes)
            .map(|i| ClassConfig {
                capacity: i * 64,
                slots: 32,
            })
            .collect(),
        streams,
        usize::MAX,
    );
    let reservation = abps_reference::mosaic::Reservation::for_config(&config)?;
    config.root_limit = reservation.total_bytes;
    let hard = reservation.backing_bytes;
    let plan = Plan {
        options,
        config,
        bytes: reservation.total_bytes,
        original_bytes: reservation.total_bytes,
        ceiling: reservation.total_bytes,
        initial: 1,
        standby: 0,
        hard: vec![hard; streams],
        extra_external: 0,
    };
    let bank = Bank::prepare(plan)?;
    let handles = (0..streams)
        .map(|_| Bank::open(&bank))
        .collect::<Result<Vec<_>>>()?;
    let start = Instant::now();
    let mut checksum = 0usize;
    for i in 0..iterations {
        let h = &handles[i % streams];
        let size = black_box((i % classes + 1) * 64);
        h.offer(size);
        let mut b = h
            .acquire(size)
            .map_err(|e| format!("unexpected admission: {e:?}"))?;
        b.put_slice(&[7; 8])?;
        checksum = checksum.wrapping_add(black_box(b.as_ref())[0] as usize);
        drop(b);
    }
    let elapsed = start.elapsed();
    assert_eq!(checksum, iterations * 7);
    drop(handles);
    assert!(bank.borrow_mut().shutdown());
    Ok(
        json!({"ns_per_frame":elapsed.as_nanos() as f64/iterations as f64,"iterations":iterations,"classes":classes,"streams":streams,"checksum":checksum,"drained":true,"bank":bank.borrow_mut().json()}),
    )
}
