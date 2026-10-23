use super::*;
fn policy(keyed: bool) -> adaptive::AdaptivePolicy {
    adaptive::AdaptivePolicy {
        target: Duration::from_millis(20),
        grace: Duration::from_secs(2),
        stall: Duration::from_millis(30),
        key_wait: Duration::from_secs(5),
        keyed,
    }
}
fn conserved<T>(o: &Observer<T>) {
    let s = o.json();
    let sum: u64 = ["free", "reserved", "queued", "transferring", "releasing"]
        .iter()
        .map(|k| s[k].as_u64().unwrap())
        .sum();
    assert_eq!(sum, s["capacity"]);
    assert_eq!(
        s["enqueued"].as_u64().unwrap(),
        s["queued"].as_u64().unwrap()
            + s["transferring"].as_u64().unwrap()
            + s["transferred"].as_u64().unwrap()
            + s["evicted"].as_u64().unwrap()
            + s["cancelled"].as_u64().unwrap()
    );
}
#[tokio::test(start_paused = true)]
async fn ap11_window_contraction_has_eight_drop_budget_and_preserves_suffix() {
    let (tx, mut rx, o) = channel(32, None);
    tx.configure(policy(false));
    for n in 0..32 {
        tx.reserve_frame(false).unwrap().send(n);
    }
    tx.0.borrow_mut().adaptive.as_mut().unwrap().window = 1;
    assert!(matches!(tx.reserve_frame(false), Err(Rejected::Policy)));
    assert_eq!(o.json()["evicted"], 8);
    assert_eq!(o.json()["queued"], 24);
    conserved(&o);
    for _ in 0..3 {
        maintain(&tx.0, false);
        conserved(&o);
    }
    assert_eq!(o.json()["queued"], 1);
    let last = rx.pop().unwrap();
    assert_eq!(last.value, Some(31));
    drop(last);
    let p = tx.reserve_frame(false).unwrap();
    p.send(32);
    drop((rx, tx));
    conserved(&o);
    assert_eq!(o.json()["free"], 32);
}
#[tokio::test(start_paused = true)]
async fn ap11_outstanding_permits_survive_window_shrink_but_not_dependency_epoch() {
    let (tx, rx, o) = channel(2, None);
    tx.configure(policy(false));
    let a = tx.reserve_frame(false).unwrap();
    let b = tx.reserve_frame(false).unwrap();
    tx.0.borrow_mut().adaptive.as_mut().unwrap().window = 1;
    a.send(1);
    b.send(2);
    conserved(&o);
    assert_eq!(o.json()["queued"], 2);
    drop((rx, tx));
    conserved(&o);
    let (tx, mut rx, o) = channel(2, None);
    tx.configure(policy(true));
    assert!(matches!(tx.reserve_frame(false), Err(Rejected::Policy)));
    let key = tx.reserve_frame(true).unwrap();
    tx.gap();
    key.send(1);
    assert_eq!(o.json()["queued"], 0);
    assert_eq!(o.json()["adaptive"]["state"], "await_keyframe");
    let key = tx.reserve_frame(true).unwrap();
    key.send(2);
    tx.reserve_frame(false).unwrap().send(3);
    let key = rx.pop().unwrap();
    assert_eq!(key.value, Some(2));
    drop(key);
    tx.gap();
    assert!(matches!(tx.reserve_frame(false), Err(Rejected::Policy)));
    assert_eq!(o.json()["queued"], 0);
    tx.reserve_frame(true).unwrap().send(4);
    let key = rx.pop().unwrap();
    assert_eq!(key.value, Some(4));
    drop(key);
    drop((rx, tx));
    conserved(&o);
    assert_eq!(o.json()["free"], 2);
}
#[tokio::test(start_paused = true)]
async fn ap11_hard_deadline_precedes_pruning_and_failed_key_does_not_resume() {
    let (tx, rx, o) = channel(2, None);
    tx.configure(policy(true));
    drop(tx.reserve_frame(true).unwrap());
    assert_eq!(o.json()["adaptive"]["state"], "await_keyframe");
    let now = Instant::now();
    tx.reserve_frame(true)
        .unwrap()
        .send_at(1, now, Some(now + Duration::from_millis(10)));
    tokio::time::advance(Duration::from_millis(20)).await;
    maintain(&tx.0, false);
    assert_eq!(o.json()["adaptive"]["disconnect_reason"], "frame_deadline");
    assert_eq!(o.json()["evicted"], 0);
    assert!(matches!(tx.reserve_frame(true), Err(Rejected::Closed)));
    drop((rx, tx));
    conserved(&o);
}
#[cfg(feature = "mosaic-spike")]
#[tokio::test(start_paused = true)]
async fn ap11_real_writer_stall_keeps_partial_prefix_and_returns_all_rights() {
    use super::super::scripted_io::{ScriptedIo, Step};
    for cut in [0, 1, HEADER, HEADER + 1] {
        let fixture = Fixture::new(
            "mosaic",
            vec![ClassConfig {
                capacity: 16,
                slots: 4,
            }],
            1,
            None,
        )
        .unwrap();
        let origin = fixture.open(64).unwrap();
        let (tx, rx, o) = channel(4, None);
        tx.configure(policy(false));
        let mut expected = Vec::new();
        for id in 0..3 {
            let mut lease = origin.acquire(4).unwrap();
            lease.put(&[id as u8; 4]);
            let header = Header {
                id,
                len: 4,
                due: 0,
                ..Header::default()
            };
            expected.extend_from_slice(&header.encode());
            expected.extend_from_slice(&[id as u8; 4]);
            tx.reserve_frame(false).unwrap().send(Message {
                header,
                frame: lease.freeze().into(),
                deadline: None,
            });
        }
        let steps = if cut == 0 {
            vec![Step::Stall]
        } else {
            vec![Step::Data(cut), Step::Stall]
        };
        let socket = ScriptedIo::new(b"", steps);
        let output = socket.output.clone();
        let (_cancel, stop) = oneshot::channel();
        let counts = Arc::new(writer::WriterCounts::default());
        let result = writer::run_io(socket, rx, stop, counts.clone(), 8).await;
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
        assert_eq!(&*output.borrow(), &expected[..cut]);
        assert_eq!(
            counts.expired.load(Ordering::Relaxed),
            0,
            "stall is not a frame expiry"
        );
        assert_eq!(o.json()["adaptive"]["disconnect_reason"], "write_stall");
        assert_eq!(fixture.snapshot()["admitted"], 0);
        conserved(&o);
        drop((tx, origin));
        fixture.finish().unwrap();
    }
}
#[test]
fn ap11_pruning_unwind_returns_credit_and_drops_each_owner_once() {
    use std::{
        cell::Cell,
        panic::{AssertUnwindSafe, catch_unwind},
    };
    struct Bomb(Rc<Cell<usize>>);
    impl Drop for Bomb {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
            panic!("injected drop");
        }
    }
    let (tx, rx, o) = channel(1, None);
    tx.configure(policy(false));
    let drops = Rc::new(Cell::new(0));
    tx.reserve_frame(false).unwrap().send(Bomb(drops.clone()));
    assert!(catch_unwind(AssertUnwindSafe(|| tx.reserve_frame(false))).is_err());
    assert_eq!(drops.get(), 1);
    assert_eq!(
        o.json()["failed_replacements"],
        1,
        "unwind preserves eviction accounting"
    );
    conserved(&o);
    assert_eq!(o.json()["free"], 1);
    drop((rx, tx));
}

#[cfg(feature = "mosaic-spike")]
#[tokio::test(start_paused = true)]
async fn ap11_ingress_source_gap_and_recipient_recovery_use_real_guards() {
    use super::super::scripted_io::{ScriptedIo, Step};
    for duplicate in [false, true] {
        let args = Args(
            [
                ("backend", "mosaic"),
                ("queue-backend", "local"),
                ("return", "local"),
                ("overflow", "skip"),
                ("freshness-priority", "adaptive"),
                ("frame-dependencies", "keyframe"),
                ("fanout", "1"),
            ]
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect(),
        );
        let settings = Settings::new(&args).unwrap();
        let fixture = Fixture::new(
            "mosaic",
            vec![ClassConfig {
                capacity: 16,
                slots: 8,
            }],
            1,
            None,
        )
        .unwrap();
        let origin = fixture.open(128).unwrap();
        let (tx, mut rx, o) = channel(4, None);
        tx.configure(settings.adaptive.unwrap());
        let mut input = Vec::new();
        let mut steps = vec![Step::Data(1)];
        let sequence = if duplicate {
            vec![(0, true), (0, false)]
        } else {
            vec![(0, true), (1, false), (3, false), (4, true), (5, false)]
        };
        for (id, keyframe) in sequence {
            let h = Header {
                id,
                len: 4,
                due: 0,
                sequence: Some(id),
                keyframe,
            };
            input.extend_from_slice(&h.wire());
            input.extend_from_slice(&[id as u8; 4]);
            steps.push(Step::Data(HEADER_V2));
            steps.push(Step::Data(4));
        }
        steps.push(Step::Zero);
        let (cancel, _) = oneshot::channel();
        let subscribers = vec![Subscriber {
            queue: IngressSender::Local(tx),
            active: true,
            cancel: Some(cancel),
        }];
        let stats = ingress::run_io(
            ScriptedIo::new(&input, steps),
            origin,
            subscribers,
            vec![0; 16].into_boxed_slice(),
            settings,
            0,
        )
        .await
        .unwrap();
        if duplicate {
            assert_eq!(stats.stop.as_deref(), Some("invalid_sequence"));
        } else {
            assert_eq!(stats.received, 5);
            assert_eq!(o.json()["adaptive"]["dependency_skips"], 1);
            assert_eq!(o.json()["adaptive"]["dependency_evictions"], 2);
            assert_eq!(fixture.snapshot()["admitted"], 32);
            for seq in [4, 5] {
                let d = rx.pop().unwrap();
                assert_eq!(d.value.as_ref().unwrap().header.sequence, Some(seq));
                assert_eq!(d.value.as_ref().unwrap().frame.as_ref(), &[seq as u8; 4]);
                drop(d);
            }
        }
        drop(rx);
        conserved(&o);
        assert_eq!(fixture.snapshot()["admitted"], 0);
        fixture.finish().unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn ap11_stale_permit_cannot_invalidate_committed_new_keyframe() {
    let (tx, mut rx, o) = channel(3, None);
    tx.configure(policy(true));
    let stale = tx.reserve_frame(true).unwrap();
    tx.gap();
    tx.reserve_frame(true).unwrap().send(2);
    let epoch = o.json()["adaptive"]["recovery_generation"].clone();
    stale.send(1);
    assert_eq!(o.json()["adaptive"]["recovery_generation"], epoch);
    assert_eq!(o.json()["adaptive"]["state"], "streaming");
    tx.reserve_frame(false).unwrap().send(3);
    for expected in [2, 3] {
        let d = rx.pop().unwrap();
        assert_eq!(d.value, Some(expected));
        drop(d);
    }
    drop((tx, rx));
    conserved(&o);
}

#[tokio::test(start_paused = true)]
async fn ap11_independent_rights_oracle_and_subscriber_isolation() {
    use std::collections::BTreeSet;
    struct Owned {
        id: u64,
        live: Rc<RefCell<BTreeSet<u64>>>,
    }
    impl Drop for Owned {
        fn drop(&mut self) {
            assert!(self.live.borrow_mut().remove(&self.id));
        }
    }
    for capacity in [1, 2, 32] {
        let (tx, mut rx, o) = channel(capacity, None);
        tx.configure(policy(false));
        let (other, other_rx, other_o) = channel::<u64>(2, None);
        other.configure(policy(false));
        other.reserve_frame(false).unwrap().send(999);
        let live = Rc::new(RefCell::new(BTreeSet::new()));
        let mut random = 17u64;
        for id in 0..if cfg!(miri) { 64 } else { 1024 } {
            random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
            match random % 5 {
                0 => {
                    drop(rx.pop());
                }
                1 => {
                    tokio::time::advance(Duration::from_millis(1)).await;
                    maintain(&tx.0, false);
                }
                _ => {
                    if let Ok(p) = tx.reserve_frame(false) {
                        assert!(live.borrow_mut().insert(id));
                        p.send(Owned {
                            id,
                            live: live.clone(),
                        });
                    }
                }
            }
            conserved(&o);
            assert_eq!(
                live.borrow().len(),
                o.json()["queued"].as_u64().unwrap() as usize
            );
            assert_eq!(other_o.json()["queued"], 1);
            assert_eq!(other_o.json()["adaptive"]["window"], 2);
            assert_eq!(other_o.json()["adaptive"]["debt_ms"], 0);
        }
        drop((tx, rx));
        assert!(live.borrow().is_empty());
        conserved(&o);
        drop((other, other_rx));
        conserved(&other_o);
    }
}
