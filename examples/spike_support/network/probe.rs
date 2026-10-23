//! AP-12 adapter costs, separate from TCP and from allocation primitive timings.
use super::*;
use std::hint::black_box;
use writer::{Delivery as _, Queue as _};

pub(super) fn run(args: &Args) -> Result<Value> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()?;
    let _entered = runtime.enter();
    let s = Settings::new(args)?;
    let iterations = args.n("iterations", 100_000);
    if iterations == 0 {
        return Err("iterations must be positive".into());
    }
    let fixture = Fixture::new(
        args.get("backend", "mosaic"),
        vec![ClassConfig {
            capacity: 1500,
            slots: 2,
        }],
        1,
        None,
    )?;
    let origin = fixture.open(3000)?;
    let (tx, mut rx, observer) = local_queue::channel(s.queue, s.freshness_min_age);
    if let Some(policy) = s.adaptive {
        tx.configure(policy);
    }
    let mut tails = Latency::default();
    let mut checksum = 0u64;
    let start = Instant::now();
    for id in 0..iterations {
        let sample = (id % 64 == 0).then(Instant::now);
        let permit = tx
            .reserve_frame(false)
            .map_err(|_| "unexpected empty-queue rejection")?;
        let mut lease = origin.acquire(512)?;
        lease.put(black_box(&[0xa5; 512]));
        permit.send(Message {
            header: Header {
                len: 512,
                id: id as u64,
                ..Header::default()
            },
            frame: lease.freeze().into(),
            deadline: None,
        });
        let mut batch = None;
        rx.try_recv()
            .ok_or("queued message disappeared")?
            .into_batch(&mut batch);
        let m = batch.take().unwrap();
        checksum += black_box(m.frame.as_ref()[0]) as u64;
        drop(m);
        if let Some(start) = sample {
            tails.record(start.elapsed());
        }
    }
    let elapsed = start.elapsed();
    drop((tx, rx, origin));
    fixture.finish()?;
    Ok(
        json!({"iterations":iterations,"ns_per_cycle":elapsed.as_nanos() as f64 / iterations as f64,
        "sample_every":64,"sampled_latency":tails.json(),"checksum":checksum,"drained":true,"queue":observer.json(),
        "writer_future":writer::future_layout(),"layout":{"header":std::mem::size_of::<Header>(),"message":std::mem::size_of::<Message>(),
        "writer_message":std::mem::size_of::<Message<WriterFrame>>(),"frame":std::mem::size_of::<Frame>(),
        "mosaic_buf":mosaic_buf_size(),"controller":std::mem::size_of::<adaptive::Controller>(),
        "sleep":std::mem::size_of::<tokio::time::Sleep>()}}),
    )
}
fn mosaic_buf_size() -> Option<usize> {
    #[cfg(feature = "mosaic-spike")]
    {
        Some(std::mem::size_of::<abps_reference::mosaic::Buf>())
    }
    #[cfg(not(feature = "mosaic-spike"))]
    {
        None
    }
}
