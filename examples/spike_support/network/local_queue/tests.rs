use super::*;
use std::{
    cell::Cell,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::atomic::AtomicUsize,
    task::Wake,
};

fn conserved<T>(o: &Observer<T>) {
    let s = o.json();
    let sum: u64 = ["free", "reserved", "queued", "transferring", "releasing"]
        .iter()
        .map(|k| s[k].as_u64().unwrap())
        .sum();
    assert_eq!(sum, s["capacity"]);
    assert!(s["occupancy_high_water"].as_u64().unwrap() <= sum);
}

#[tokio::test(start_paused = true)]
async fn ap10_age_fifo_transfer_and_cancel_conserve_credits() {
    let (tx, mut rx, o) = channel(2, Some(Duration::from_millis(2)));
    tx.try_reserve().unwrap().send(1);
    tx.try_reserve().unwrap().send(2);
    assert!(matches!(tx.try_reserve(), Err(Rejected::Full)));
    tokio::time::advance(Duration::from_millis(2)).await;
    let p = tx.try_reserve().unwrap();
    assert_eq!(o.json()["evicted"], 1);
    conserved(&o);
    p.send(3);
    let a = rx.pop().unwrap();
    let b = rx.pop().unwrap();
    assert_eq!(a.value, Some(2));
    assert_eq!(b.value, Some(3));
    // No queued frame can be evicted while all rights are transferring.
    assert!(matches!(tx.try_reserve(), Err(Rejected::Full)));
    assert_eq!(o.json()["transferring"], 2);
    drop((a, b));
    conserved(&o);
    assert_eq!(o.json()["free"], 2);
    assert_eq!(o.json()["cancelled"], 2);
}

#[test]
fn ap10_reserved_and_releasing_rights_survive_unwind() {
    struct Bomb {
        dropped: Rc<Cell<usize>>,
        explode: bool,
    }
    impl Drop for Bomb {
        fn drop(&mut self) {
            self.dropped.set(self.dropped.get() + 1);
            assert!(!self.explode, "injected payload drop");
        }
    }
    let drops = Rc::new(Cell::new(0));
    let (tx, rx, o) = channel(1, Some(Duration::ZERO));
    tx.try_reserve().unwrap().send(Bomb {
        dropped: drops.clone(),
        explode: true,
    });
    assert!(catch_unwind(AssertUnwindSafe(|| tx.try_reserve())).is_err());
    assert_eq!(drops.get(), 1);
    assert_eq!(o.json()["free"], 1);
    assert_eq!(o.json()["failed_replacements"], 1);
    tx.try_reserve().unwrap().send(Bomb {
        dropped: drops.clone(),
        explode: false,
    });
    let p = tx.try_reserve().unwrap();
    assert_eq!(drops.get(), 2, "old ownership ends before new admission");
    drop(p); // failed admission/input/view construction/cancellation all use this path
    assert_eq!(o.json()["failed_replacements"], 2);
    let p = tx.try_reserve().unwrap();
    drop(rx);
    p.send(Bomb {
        dropped: drops.clone(),
        explode: false,
    });
    assert_eq!(drops.get(), 3);
    assert!(matches!(tx.try_reserve(), Err(Rejected::Closed)));
    conserved(&o);
}

struct Signal(AtomicUsize);
impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}
#[test]
fn ap10_wakes_on_commit_close_and_last_reservation() {
    let (tx, mut rx, o) = channel(1, None);
    let signal = Arc::new(Signal(AtomicUsize::new(0)));
    let waker = Waker::from(signal.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(rx.poll(&mut cx).is_pending());
    let p = tx.try_reserve().unwrap();
    drop(tx);
    assert_eq!(signal.0.load(Ordering::Relaxed), 1);
    assert!(
        rx.poll(&mut cx).is_pending(),
        "outstanding reservation prevents EOF"
    );
    p.send(7);
    assert_eq!(signal.0.load(Ordering::Relaxed), 2);
    assert!(matches!(rx.poll(&mut cx), Poll::Ready(Some(_))));
    assert!(matches!(rx.poll(&mut cx), Poll::Ready(None)));
    conserved(&o);
    let (tx, mut rx, _) = channel::<usize>(1, None);
    let p = tx.try_reserve().unwrap();
    drop(tx);
    assert!(rx.poll(&mut cx).is_pending());
    drop(p);
    assert!(matches!(rx.poll(&mut cx), Poll::Ready(None)));
}

#[test]
fn ap10_independent_history_oracle_preserves_survivors() {
    for capacity in [1, 2, 8, 32] {
        for oldest in [false, true] {
            let (tx, mut rx, o) = channel(capacity, oldest.then_some(Duration::ZERO));
            let mut expected = Vec::new();
            let mut state = 0xabc51231u64;
            let mut evictions = 0;
            for id in 0..if cfg!(miri) { 64u64 } else { 1000 } {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                if state % 3 == 0 {
                    let actual = rx.pop().and_then(|v| v.value);
                    let want = if expected.is_empty() {
                        None
                    } else {
                        Some(expected.remove(0))
                    };
                    assert_eq!(actual, want);
                } else {
                    let allowed = expected.len() < capacity || oldest;
                    let attempt = tx.try_reserve();
                    assert_eq!(attempt.is_ok(), allowed);
                    if let Ok(p) = attempt {
                        if expected.len() == capacity {
                            expected.remove(0);
                            evictions += 1;
                        }
                        if state % 5 == 0 {
                            drop(p);
                        } else {
                            p.send(id);
                            expected.push(id);
                        }
                    }
                }
                conserved(&o);
                assert_eq!(o.json()["queued"], expected.len());
                assert_eq!(o.json()["evicted"], evictions);
            }
            drop((rx, tx));
            assert_eq!(o.json()["free"], capacity);
        }
    }
}

#[cfg(feature = "mosaic-spike")]
#[tokio::test]
async fn ap10_real_payload_partial_write_and_eviction_preserve_wire_and_budget() {
    use super::super::scripted_io::{ScriptedIo, Step};
    let fixture = Fixture::new(
        "mosaic",
        vec![ClassConfig {
            capacity: 16,
            slots: 3,
        }],
        1,
        None,
    )
    .unwrap();
    let origin = fixture.open(48).unwrap();
    let (tx, mut rx, o) = channel(1, Some(Duration::ZERO));
    let message = |id: u64| {
        let mut lease = origin.acquire(4).unwrap();
        lease.put(&[id as u8; 4]);
        Message {
            header: Header {
                id,
                len: 4,
                due: 0,
                ..Header::default()
            },
            frame: lease.freeze().into(),
            deadline: None,
        }
    };
    tx.try_reserve().unwrap().send(message(1));
    let mut batch = None;
    writer::Delivery::into_batch(rx.pop().unwrap(), &mut batch);
    assert_eq!(o.json()["free"], 1);
    tx.try_reserve().unwrap().send(message(2));
    let p = tx.try_reserve().unwrap();
    assert_eq!(
        fixture.snapshot()["admitted"],
        16,
        "eviction refunded only queued allocation"
    );
    p.send(message(3));
    assert_eq!(batch.as_ref().unwrap().frame.as_ref(), [1; 4]);
    drop(batch);
    drop(tx);
    let (_cancel, stop) = oneshot::channel();
    let socket = ScriptedIo::new(
        b"",
        [
            Step::Data(1),
            Step::Pending,
            Step::Data(HEADER),
            Step::Data(1),
            Step::Data(usize::MAX),
        ],
    );
    let output = socket.output.clone();
    let counts = Arc::new(writer::WriterCounts::default());
    writer::run_io(socket, rx, stop, counts.clone(), 8)
        .await
        .unwrap();
    let mut expected = Header {
        id: 3,
        len: 4,
        due: 0,
        ..Header::default()
    }
    .encode()
    .to_vec();
    expected.extend_from_slice(&[3; 4]);
    assert_eq!(*output.borrow(), expected);
    assert_eq!(counts.completed.load(Ordering::Relaxed), 1);
    assert_eq!(o.json()["evicted"], 1);
    assert_eq!(o.json()["transferred"], 2);
    assert_eq!(fixture.snapshot()["admitted"], 0);
    drop(origin);
    fixture.finish().unwrap();
}

#[test]
fn ap10_configuration_rejects_unsupported_combinations_before_resources() {
    let args = |pairs: &[(&str, &str)]| {
        Args(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    };
    let base = [
        ("backend", "mosaic"),
        ("queue-backend", "local"),
        ("return", "local"),
        ("overflow", "skip"),
        ("freshness-priority", "oldest"),
    ];
    for (k, v) in [
        ("backend", "berry"),
        ("return", "remote"),
        ("overflow", "disconnect"),
        ("queue-backend", "tokio"),
        ("fanout-api", "worker-branches"),
        ("freshness-min-age-ms", "3600001"),
        ("freshness-min-age-ms", "-1"),
    ] {
        let mut a = args(&base);
        a.0.insert(k.into(), v.into());
        assert!(Settings::new(&a).is_err(), "{k}={v}");
    }
    assert!(Settings::new(&args(&base)).is_ok());
    assert!(Settings::new(&args(&[("freshness-min-age-ms", "0")])).is_err());
}

#[cfg(feature = "mosaic-spike")]
#[tokio::test(start_paused = true)]
async fn ap10_writer_error_expiry_and_cancel_release_batch_and_queue() {
    use super::super::scripted_io::{ScriptedIo, Step};
    use std::io::ErrorKind;
    for failure in ["write", "expiry", "cancel"] {
        let fixture = Fixture::new(
            "mosaic",
            vec![ClassConfig {
                capacity: 16,
                slots: 3,
            }],
            1,
            None,
        )
        .unwrap();
        let origin = fixture.open(48).unwrap();
        let (tx, rx, observer) = channel(2, Some(Duration::ZERO));
        for id in 0..2 {
            let mut lease = origin.acquire(4).unwrap();
            lease.put(&[id as u8; 4]);
            tx.try_reserve().unwrap().send(Message {
                header: Header {
                    id,
                    len: 4,
                    due: 0,
                    ..Header::default()
                },
                frame: lease.freeze().into(),
                deadline: (failure == "expiry").then(|| Instant::now() + Duration::from_millis(1)),
            });
        }
        let io = ScriptedIo::new(
            b"",
            [
                Step::Data(1),
                if failure == "write" {
                    Step::Error(ErrorKind::BrokenPipe)
                } else {
                    Step::Stall
                },
            ],
        );
        let output = io.output.clone();
        let (cancel, stop) = oneshot::channel();
        let counts = Arc::new(writer::WriterCounts::default());
        let task = writer::run_io(io, rx, stop, counts.clone(), 1);
        tokio::pin!(task);
        let result = if failure == "cancel" {
            tokio::select! { _ = &mut task => panic!("writer should stall"), _ = tokio::task::yield_now() => {} }
            assert_eq!(observer.json()["transferred"], 1);
            assert_eq!(observer.json()["queued"], 1);
            cancel.send(()).unwrap();
            task.await
        } else {
            task.await
        };
        assert_eq!(result.is_ok(), failure == "cancel");
        assert_eq!(output.borrow().len(), 1, "partial frame is never replaced");
        assert_eq!(counts.completed.load(Ordering::Relaxed), 0);
        assert_eq!(
            counts.expired.load(Ordering::Relaxed),
            u64::from(failure == "expiry")
        );
        assert_eq!(observer.json()["free"], 2);
        assert_eq!(observer.json()["cancelled"], 1);
        assert_eq!(observer.json()["evicted"], 0);
        assert_eq!(fixture.snapshot()["admitted"], 0);
        drop((tx, origin));
        fixture.finish().unwrap();
    }
}

#[cfg(feature = "mosaic-spike")]
#[tokio::test]
async fn ap10_ingress_replacement_failure_and_cancellation_are_observable() {
    use super::super::scripted_io::{ScriptedIo, Step};
    for failure in ["none", "budget", "input", "cancel"] {
        let fixture = Fixture::new(
            "mosaic",
            vec![ClassConfig {
                capacity: 16,
                slots: 2,
            }],
            1,
            None,
        )
        .unwrap();
        let origin = fixture.open(16).unwrap();
        let (tx, rx, o) = channel(1, Some(Duration::ZERO));
        let old = origin.acquire(4).unwrap().freeze();
        // A different owner may retain the old backing after queue eviction.
        let retained = (failure == "budget").then(|| {
            let mut dst = [None];
            old.try_clone_into(&mut dst).unwrap();
            dst[0].take().unwrap()
        });
        tx.try_reserve().unwrap().send(Message {
            header: Header {
                id: 0,
                len: 0,
                due: 0,
                ..Header::default()
            },
            frame: old.into(),
            deadline: None,
        });
        let sub = Subscriber {
            queue: IngressSender::Local(tx),
            active: true,
            cancel: None,
        };
        let mut bytes = Header {
            id: 1,
            len: 4,
            due: 0,
            ..Header::default()
        }
        .encode()
        .to_vec();
        bytes.extend_from_slice(&[0x5a; 4]);
        let step = match failure {
            "input" => Step::Zero,
            "cancel" => Step::Stall,
            _ => Step::Data(4),
        };
        let io = ScriptedIo::new(
            &bytes,
            [Step::Data(1), Step::Data(HEADER), step, Step::Zero],
        );
        let settings = Settings::new(&Args(
            [
                ("backend", "mosaic"),
                ("queue-backend", "local"),
                ("freshness-priority", "oldest"),
                ("overflow", "skip"),
                ("fanout", "1"),
            ]
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect(),
        ))
        .unwrap();
        let task = super::super::ingress::run_io(
            io,
            origin,
            vec![sub],
            vec![0; 16].into_boxed_slice(),
            settings,
            0,
        );
        if failure == "cancel" {
            tokio::pin!(task);
            tokio::select! { _ = &mut task => panic!("input should be stalled"), _ = tokio::task::yield_now() => {} }
            assert_eq!(o.json()["reserved"], 1);
        } else {
            let stats = task.await.unwrap();
            assert_eq!(stats.accepted, u64::from(failure == "none"));
            assert_eq!(stats.budget_discards, u64::from(failure == "budget"));
            assert_eq!(stats.stop.is_some(), failure == "input");
        }
        assert_eq!(o.json()["evicted"], 1);
        assert_eq!(
            o.json()["failed_replacements"],
            u64::from(failure != "none")
        );
        drop((rx, retained));
        assert_eq!(fixture.snapshot()["admitted"], 0);
        assert_eq!(o.json()["free"], 1);
        fixture.finish().unwrap();
    }
}
