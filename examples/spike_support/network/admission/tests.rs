use super::*;
use std::future::Future;
fn options(mode: Mode, rate: u64, burst: usize) -> Options {
    Options {
        mode,
        rate,
        burst,
        return_workers: 1,
        slow_bytes: 1024,
    }
}
fn commit(e: &mut Engine, s: usize, c: usize, now: u64) {
    let t = e.register(s, c, u64::MAX).unwrap();
    assert!(e.begin(t, now));
    e.commit(t);
}
#[test]
fn ap16_bucket_all_intervals_conserve_successful_charges() {
    for burst in [1, 3, 17, 256] {
        let mut e = Engine::new(options(Mode::Bucket, 37, burst), 1);
        let mut seed = 5u64;
        let mut now = 0;
        let mut events = Vec::new();
        for i in 0..300 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            now += seed % 100_000_000;
            let c = 1 + (seed as usize % burst);
            let t = e.register(0, c, u64::MAX).unwrap();
            if e.begin(t, now) && i % 4 != 0 {
                e.commit(t);
                events.push((now, c));
            } else {
                e.cancel(t);
            }
        }
        // Independent oracle: successful charge sums on EVERY event interval.
        for i in 0..events.len() {
            let mut total = 0;
            for j in i..events.len() {
                total += events[j].1 as u128 * SCALE;
                assert!(total <= burst as u128 * SCALE + 37 * (events[j].0 - events[i].0) as u128);
            }
        }
        assert_eq!(e.counts.committed as usize, events.len());
    }
}
#[test]
fn ap16_fractional_refill_and_idle_cap() {
    let mut e = Engine::new(options(Mode::Bucket, 1, 1), 1);
    commit(&mut e, 0, 1, 0);
    let t = e.register(0, 1, u64::MAX).unwrap();
    for n in 1..1000 {
        assert!(!e.ready(t, n * 1_000_000));
    }
    assert!(e.begin(t, 1_000_000_000));
    e.commit(t);
    commit(&mut e, 0, 1, 100_000_000_000);
    let t = e.register(0, 1, u64::MAX).unwrap();
    assert!(!e.begin(t, 100_000_000_000));
    e.cancel(t);
}
#[test]
fn ap16_late_commit_cannot_bank_a_second_burst() {
    for mode in [Mode::Bucket, Mode::Tick] {
        let mut e = Engine::new(options(mode, 100, 100), 1);
        let t = e.register(0, 100, u64::MAX).unwrap();
        assert!(e.begin(t, 0));
        e.commit_at(t, 10_000_000_000);
        let next = e.register(0, 1, u64::MAX).unwrap();
        assert!(!e.begin(next, 10_000_000_000));
    }
}
#[test]
fn ap16_tick_window_not_an_arbitrary_interval_bucket() {
    let mut e = Engine::new(options(Mode::Tick, 10, 10), 1);
    commit(&mut e, 0, 10, 999_999_999);
    commit(&mut e, 0, 10, 1_000_000_000);
    assert_eq!(e.counts.bytes, 20);
    let t = e.register(0, 1, u64::MAX).unwrap();
    assert!(!e.begin(t, 1_000_000_000));
    e.cancel(t);
    commit(&mut e, 0, 10, 100_000_000_000);
    let t = e.register(0, 1, u64::MAX).unwrap();
    assert!(!e.begin(t, 100_000_000_000));
}
#[test]
fn ap16_fifo_large_charge_and_stale_cancellation() {
    let mut e = Engine::new(options(Mode::Bucket, 100, 100), 2);
    commit(&mut e, 0, 100, 0);
    let a = e.register(0, 100, u64::MAX).unwrap();
    let b = e.register(1, 1, u64::MAX).unwrap();
    assert!(!e.begin(b, 10_000_000));
    assert!(!e.begin(a, 10_000_000));
    assert!(e.begin(a, 1_000_000_000));
    e.commit(a);
    assert!(!e.begin(b, 1_000_000_000));
    assert!(e.begin(b, 1_010_000_000));
    e.commit(b);
    let next = e.register(0, 1, u64::MAX).unwrap();
    e.cancel(a);
    assert_eq!(e.state(next), Some(State::Waiting));
    e.cancel(next);
    e.cancel(next);
}
#[test]
fn ap16_bounded_expiry_and_generation_exhaustion() {
    let mut e = Engine::new(options(Mode::Bucket, 1, 1), 64);
    commit(&mut e, 0, 1, 0);
    let tickets = (0..64)
        .map(|s| e.register(s, 1, 100).unwrap())
        .collect::<Vec<_>>();
    assert!(e.register(0, 1, 200).is_err());
    for n in 1..=8 {
        let signals = e.service(100);
        assert_eq!(signals.into_iter().flatten().count(), 8);
        assert_eq!(e.counts.expired, n * 8);
    }
    for t in tickets {
        e.cancel(t);
    }
    assert!(e.queue.is_empty());
    e.entries[0].generation = u64::MAX;
    assert!(e.register(0, 1, 200).is_err());
}
#[test]
fn ap16_error_rollbacks_are_idempotent() {
    let mut e = Engine::new(options(Mode::Bucket, 1, 8), 1);
    for _ in 0..1000 {
        let t = e.register(0, 8, u64::MAX).unwrap();
        assert!(e.begin(t, 0));
        e.cancel(t);
        e.cancel(t);
    }
    commit(&mut e, 0, 8, 0);
    assert_eq!(e.counts.bytes, 8);
    assert_eq!(e.counts.rollback, 1000);
}
#[tokio::test(start_paused = true)]
async fn ap16_gate_cancellation_unwind_and_deadline() {
    let g = Gate::new(options(Mode::Bucket, 1, 1), 1);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut f = Box::pin(
        g.attempt(0, 1, deadline, || -> std::result::Result<(), ()> {
            panic!("after debit")
        }),
    );
    let mut cx = std::task::Context::from_waker(Waker::noop());
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f.as_mut().poll(&mut cx)))
            .is_err()
    );
    drop(f);
    assert_eq!(g.json()["rolled_back"], 1);
    assert!(
        g.attempt(0, 1, deadline, || Ok::<_, ()>(()))
            .await
            .unwrap()
            .is_ok()
    );
    let mut pending = Box::pin(g.attempt(0, 1, deadline, || Ok::<_, ()>(())));
    assert!(pending.as_mut().poll(&mut cx).is_pending());
    drop(pending);
    assert_eq!(g.json()["pending"], 0);
    let d = tokio::time::Instant::now();
    assert!(g.attempt(0, 1, d, || Ok::<_, ()>(())).await.is_err());
    assert_eq!(g.json()["expired"], 1);
}
#[tokio::test(start_paused = true)]
async fn ap16_shared_timer_makes_fifo_progress() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let g = Gate::new(options(Mode::Bucket, 10, 10), 2);
            let _driver = Driver(tokio::task::spawn_local(g.clone().drive()));
            let d = tokio::time::Instant::now() + Duration::from_secs(5);
            g.attempt(0, 10, d, || Ok::<_, ()>(()))
                .await
                .unwrap()
                .unwrap();
            let a = g.clone();
            let one =
                tokio::task::spawn_local(
                    async move { a.attempt(0, 10, d, || Ok::<_, ()>(())).await },
                );
            tokio::task::yield_now().await;
            let b = g.clone();
            let two =
                tokio::task::spawn_local(
                    async move { b.attempt(1, 1, d, || Ok::<_, ()>(())).await },
                );
            one.await.unwrap().unwrap().unwrap();
            two.await.unwrap().unwrap().unwrap();
            assert!(g.start.elapsed() >= Duration::from_millis(1100));
            assert_eq!(g.json()["pending"], 0);
        })
        .await;
}
#[tokio::test(start_paused = true)]
async fn ap16_expired_deadline_cannot_poll_a_ready_io() {
    let mut polled = false;
    let d = tokio::time::Instant::now();
    let r = before_deadline(Some(d), async {
        polled = true;
        Ok(())
    })
    .await;
    assert_eq!(r.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
    assert!(!polled);
}
#[tokio::test(start_paused = true)]
async fn ap16_wire_pacing_covers_header_and_payload() {
    let mut input = &[9u8; 2048][..];
    let mut out = [0u8; 2048];
    let mut at = 0;
    let mut p = Some(tcp::WirePacer::new(1024));
    let start = tokio::time::Instant::now();
    assert!(
        tcp::paced_prefix(&mut input, &mut out[..20], &mut at, &mut p)
            .await
            .unwrap()
    );
    at = 0;
    assert!(
        tcp::paced_prefix(&mut input, &mut out[20..], &mut at, &mut p)
            .await
            .unwrap()
    );
    assert!(start.elapsed() >= Duration::from_millis(1937));
    assert!(start.elapsed() < Duration::from_millis(2100));
    assert_eq!(out, [9; 2048]);
}

#[tokio::test(start_paused = true)]
async fn ap16_stale_guard_preserves_new_registration_waker() {
    let g = Gate::new(options(Mode::Bucket, 1, 1), 1);
    let old = g.state.borrow_mut().engine.register(0, 1, 100).unwrap();
    let guard = Registration {
        gate: &g,
        ticket: old,
        done: false,
    };
    g.state.borrow_mut().engine.cancel(old);
    let new = g.state.borrow_mut().engine.register(0, 1, 100).unwrap();
    g.state.borrow_mut().wakers[0] = Some(Waker::noop().clone());
    drop(guard);
    assert_eq!(g.state.borrow().engine.state(new), Some(State::Waiting));
    assert!(g.state.borrow().wakers[0].is_some());
}
