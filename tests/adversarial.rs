//! ABPS-001..032: rights, not implementation counters, predict each result.
#![forbid(unsafe_code)]
#[path = "support/rights_backends.rs"]
mod backends;
#[path = "support/rights.rs"]
mod rights;
#[path = "support/rights_trace.rs"]
mod rights_trace;
use rights::{Action::*, Configuration, InvariantOracle};
use rights_trace::{Trace, critical_trace};

fn replay_issuers(trace: &Trace) -> Vec<String> {
    assert!(trace.valid());
    #[cfg(feature = "berry-spike")]
    let berry = trace
        .config
        .counts
        .iter()
        .all(|n| *n <= usize::BITS as usize)
        .then(|| backends::berry::replay(trace));
    #[cfg(feature = "mosaic-spike")]
    let mosaic = backends::mosaic::replay(trace);
    #[cfg(all(feature = "berry-spike", feature = "mosaic-spike"))]
    if let Some(ref berry) = berry {
        assert_eq!(*berry, mosaic, "ABPS-032 differential history");
    }
    #[cfg(all(feature = "berry-spike", not(feature = "mosaic-spike")))]
    {
        berry.expect("wide trace requires the Mosaic backend")
    }
    #[cfg(feature = "mosaic-spike")]
    {
        mosaic
    }
    #[cfg(not(any(feature = "berry-spike", feature = "mosaic-spike")))]
    {
        Vec::new()
    }
}

#[cfg(feature = "mosaic-spike")]
#[test]
fn abps_wide_concrete_trace_selects_only_applicable_backend() {
    let mut trace = portable_trace();
    trace.config.counts[0] = usize::BITS as usize + 1;
    assert_eq!(
        Trace::parse(&serde_json::to_vec(&trace.json()).unwrap()).unwrap(),
        trace
    );
    checked_replay(&trace);
}
fn failure_message(p: &(dyn std::any::Any + Send)) -> String {
    if let Some(failure) = p.downcast_ref::<rights_trace::ReplayFailure>() {
        return format!(
            "{}\nABPS_REPLAY_CONTEXT {}",
            failure.message, failure.context
        );
    }
    p.downcast_ref::<String>()
        .cloned()
        .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_else(|| "non-string panic".into())
}
fn checked_replay(trace: &Trace) {
    if let Err(panic) = std::panic::catch_unwind(|| replay_issuers(trace)) {
        let failure = failure_message(&*panic);
        if !cfg!(miri) {
            let key = panic
                .downcast_ref::<rights_trace::ReplayFailure>()
                .map(|f| &f.key);
            let minimized = key.map_or_else(
                || trace.clone(),
                |key| {
                    rights_trace::minimize_failure(trace.clone(), key, |candidate| {
                        replay_issuers(candidate);
                    })
                },
            );
            let dir = std::env::var_os("ABPS_TRACE_OUTPUT")
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| "target/verification-traces".into());
            std::fs::create_dir_all(&dir).unwrap();
            let mut artifact = minimized.json();
            artifact["failure"] = serde_json::json!({"message":failure,"key":key.map(|k|k.json()),
                "source_sha256":std::env::var("ABPS_SOURCE_SHA256").ok(),
                "original":trace.json(),"backends":[cfg!(feature="berry-spike"),cfg!(feature="mosaic-spike")]});
            // One named record per seed; proptest's final minimized input replaces
            // the earlier reproduction. The complete concrete history is kept.
            std::fs::write(
                dir.join(format!("rights-{}.json", trace.seed)),
                serde_json::to_vec_pretty(&artifact).unwrap(),
            )
            .unwrap();
        }
        std::panic::resume_unwind(panic);
    }
}

#[test]
fn abps_critical_pressure_states_and_explicit_maintenance() {
    for reserved in [false, true] {
        let trace = critical_trace(reserved);
        let mut model = InvariantOracle::new(trace.config.clone(), true);
        let mut hits = std::collections::BTreeSet::new();
        for action in &trace.actions {
            model.apply(action).unwrap().ok();
            hits.extend(rights_trace::pressure(&model));
        }
        for required in [
            "last-slot",
            "reference-limit",
            "closing",
            "pending-maintenance",
        ] {
            assert!(
                hits.contains(required),
                "vacuous boundary scenario: {required}"
            );
        }
        assert_eq!(hits.contains("grant-pressure"), reserved);
        assert!(model.pending.is_empty());
        assert_eq!(model.free_records(), 1);
        checked_replay(&trace);
    }
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config {
        cases: if cfg!(miri) {3} else {std::env::var("ABPS_PROPTEST_CASES").ok().map(|n|n.parse().unwrap()).unwrap_or(128)},
        failure_persistence: if cfg!(miri) {None} else {Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource("proptest-regressions")))},
        ..Default::default()
    })]
    #[test]
    fn abps_biased_concrete_histories(input in proptest::collection::vec(proptest::prelude::any::<u8>(), 9..5+4*trace_operations())) {
        let seed = input.iter().fold(0xabf50021u64, |seed,b| seed.rotate_left(5)^u64::from(*b));
        let (trace,_) = rights_trace::generated(seed,&input,trace_operations());
        checked_replay(&trace);
    }
}
fn trace_operations() -> usize {
    if cfg!(miri) {
        20
    } else {
        std::env::var("ABPS_TRACE_OPERATIONS")
            .ok()
            .map(|s| s.parse::<usize>().unwrap().clamp(1, 1024))
            .unwrap_or(250)
    }
}

#[test]
fn abps_shrinking_preserves_defect_identity_not_panic_text() {
    use rights_trace::{FailureKey, ReplayFailure};
    let trace = Trace {
        seed: 42,
        config: Configuration::default(),
        actions: vec![Snapshot, Poll { budget: 0 }, Snapshot],
    };
    let key = FailureKey::from_assertion("berry", "MEM-02 charge", &Drop { id: 7 }).unwrap();
    let mut alternatives = Vec::new();
    for part in 0..4 {
        let mut other = key.clone();
        match part {
            0 => other.backend = "mosaic".into(),
            1 => other.check = "OWN-01 payload".into(),
            2 => other.operation = "acquire".into(),
            _ => other.owners = vec![8],
        }
        alternatives.push(Some(other));
    }
    alternatives.push(None);
    for alternative in alternatives {
        let reduced = rights_trace::minimize_failure(trace.clone(), &key, |candidate| {
            let selected = if candidate.actions.contains(&Snapshot) {
                Some(key.clone())
            } else {
                alternative.clone()
            };
            if let Some(key) = selected {
                std::panic::panic_any(ReplayFailure {
                    key,
                    message: "identical diagnostic".into(),
                    context: serde_json::Value::Null,
                });
            }
            panic!("identical diagnostic");
        });
        assert_eq!(
            reduced.actions,
            [Snapshot],
            "shrinking must retain the original violated postcondition"
        );
    }
    assert!(FailureKey::from_assertion("berry", "unrelated panic", &Snapshot).is_none());
}

#[test]
fn abps_boundary_histories_preserve_errors_isolation_and_recycling() {
    let word = usize::BITS as usize;
    let counts: &[usize] = if cfg!(miri) {
        &[1, 2]
    } else {
        &[
            1,
            2,
            word - 1,
            word,
            word + 1,
            2 * word - 1,
            2 * word,
            2 * word + 1,
        ]
    };
    for &count in counts {
        if count > word && !cfg!(feature = "mosaic-spike") {
            continue;
        }
        for reserved in [false, true] {
            let mut trace = Trace {
                seed: count as u64,
                config: Configuration {
                    counts: [count, 1],
                    records: 2,
                    reserved,
                    reference_limit: 3,
                },
                actions: vec![
                    Open {
                        id: 1,
                        limit: 8 * count,
                        grants: [count, 0],
                    },
                    Open {
                        id: 2,
                        limit: 32,
                        grants: [0, 1],
                    },
                    Acquire {
                        id: 3,
                        domain: 2,
                        size: 32,
                    },
                    Write {
                        id: 3,
                        bytes: vec![0xa7; 32],
                    },
                    Freeze { id: 3 },
                ],
            };
            for id in 10..10 + count {
                trace.actions.push(Acquire {
                    id,
                    domain: 1,
                    size: 1,
                });
            }
            trace.actions.extend([
                Acquire {
                    id: 1000,
                    domain: 1,
                    size: 1,
                }, // exact byte limit, no state change
                Write {
                    id: 10,
                    bytes: vec![0xa5; 8],
                },
                Freeze { id: 10 },
                Slice {
                    source: 10,
                    id: 1001,
                    start: 8,
                    end: 8,
                },
                Slice {
                    source: 10,
                    id: 1002,
                    start: 0,
                    end: usize::MAX,
                },
                Close { domain: 1 },
                Acquire {
                    id: 1003,
                    domain: 1,
                    size: 1,
                },
                DropControls { domain: 1 },
                Poll { budget: 0 },
                Poll { budget: 1 },
            ]);
            for id in 10..10 + count {
                trace.actions.push(Drop { id });
            }
            trace.actions.extend([
                Poll { budget: 1 },
                Snapshot,
                Drop { id: 1001 },
                Poll { budget: 1 },
                Open {
                    id: 4,
                    limit: 8 * count,
                    grants: [count, 0],
                },
                Acquire {
                    id: 5,
                    domain: 4,
                    size: 1,
                },
                SetFilled { id: 5, len: 1 },
                Initialize { id: 5, len: 1 },
                SetFilled { id: 5, len: 1 },
                Freeze { id: 5 },
                Snapshot,
                Drop { id: 5 },
                Drop { id: 3 },
                DropControls { domain: 4 },
                DropControls { domain: 2 },
                Poll { budget: 2 },
            ]);
            let mut model = InvariantOracle::new(trace.config.clone(), true);
            let mut hits = std::collections::BTreeSet::new();
            for action in &trace.actions {
                model.apply(action).unwrap().ok();
                hits.extend(rights_trace::pressure(&model));
            }
            for state in ["last-slot", "closing", "pending-maintenance"] {
                assert!(hits.contains(state), "unreached {state}");
            }
            assert_eq!(hits.contains("grant-pressure"), reserved);
            assert_eq!(model.free_records(), 2);
            checked_replay(&trace);
        }
    }
}

#[test]
fn abps_concrete_trace_roundtrip_replay_and_dependency_preserving_shrink() {
    let trace = critical_trace(true);
    assert_eq!(
        Trace::parse(&serde_json::to_vec(&trace.json()).unwrap()).unwrap(),
        trace
    );
    let minimized = rights_trace::minimize(trace.clone(), |t| {
        t.actions
            .iter()
            .any(|a| matches!(a, Slice { source: 4, .. }))
    });
    assert!(minimized.valid());
    assert!(minimized.actions.len() < trace.actions.len());
    assert!(
        minimized
            .actions
            .iter()
            .any(|a| matches!(a, Slice { id: 4, .. })),
        "producer identity must survive shrinking"
    );
    let mut invalid = trace.clone();
    invalid
        .actions
        .retain(|a| !matches!(a, Slice { id: 4, .. }));
    assert!(
        !invalid.valid(),
        "a missing ID must not select another handle"
    );
    let mut json = trace.json();
    json["version"] = 2.into();
    assert!(Trace::parse(&serde_json::to_vec(&json).unwrap()).is_err());
    if !cfg!(miri)
        && let Some(path) = std::env::var_os("ABPS_RIGHTS_TRACE_FILE")
    {
        checked_replay(&Trace::parse(&std::fs::read(path).unwrap()).unwrap());
    }
    if !cfg!(miri)
        && let Some(path) = std::env::var_os("ABPS_RIGHTS_BYTES_FILE")
    {
        let input = std::fs::read(path).unwrap();
        let seed = input
            .iter()
            .fold(0xabf50021u64, |seed, b| seed.rotate_left(5) ^ u64::from(*b));
        let (trace, _) = rights_trace::generated(seed, &input, 1024);
        checked_replay(&trace);
    }
}

#[test]
fn abps_oracle_negative_controls_reject_lost_rights_and_early_refund() {
    let mut model = InvariantOracle::new(Configuration::default(), true);
    model
        .apply(&Open {
            id: 0,
            limit: 40,
            grants: [1, 1],
        })
        .unwrap()
        .unwrap();
    model
        .apply(&Acquire {
            id: 0,
            domain: 0,
            size: 1,
        })
        .unwrap()
        .unwrap();
    let mut bad = model.clone();
    bad.handles.clear();
    assert!(bad.validate().unwrap_err().contains("orphan"));
    let mut bad = model.clone();
    bad.domains.get_mut(&0).unwrap().recycled = true;
    assert!(bad.validate().is_err());
    let mut bad = model.clone();
    bad.domains.get_mut(&0).unwrap().limit = 0;
    assert!(bad.validate().unwrap_err().contains("budget"));
    model.apply(&DropControls { domain: 0 }).unwrap().unwrap();
    let mut bad = model.clone();
    bad.pending.push_back(0);
    assert!(bad.validate().unwrap_err().contains("duplicate FIFO"));
    model.pending.clear();
    assert!(model.validate().unwrap_err().contains("missing FIFO"));
    let mut ledger = rights::ReturnLedger::default();
    ledger.issue(0, 10, 0, 8);
    ledger.publish(0, 10);
    ledger.issue(0, 11, 1, 8);
    assert_eq!((ledger.admitted(0), ledger.admitted(1)), (8, 8));
    ledger.finish(10);
    assert_eq!(ledger.slots[&0], 11);
    assert_eq!((ledger.admitted(0), ledger.admitted(1)), (0, 8));
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| ledger.finish(11))).is_err());
    ledger.publish(0, 11);
    ledger.finish(11);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| ledger.finish(11))).is_err());
}

fn portable_trace() -> Trace {
    Trace {
        seed: 1,
        config: Configuration {
            reference_limit: 4,
            ..Default::default()
        },
        actions: vec![
            Open {
                id: 0,
                limit: 64,
                grants: [2, 2],
            },
            Acquire {
                id: 0,
                domain: 0,
                size: 8,
            },
            Write {
                id: 0,
                bytes: b"ab".to_vec(),
            },
            Write {
                id: 0,
                bytes: b"cd".to_vec(),
            },
            Freeze { id: 0 },
            Batch {
                source: 0,
                ids: vec![1, 2, 3],
            },
            Drop { id: 1 },
            Drop { id: 2 },
            Drop { id: 3 },
            Snapshot,
            Drop { id: 0 },
            DropControls { domain: 0 },
            Poll { budget: 2 },
        ],
    }
}
fn end(trace: &Trace) -> String {
    let reference = backends::reference::replay(trace);
    let issuers = replay_issuers(trace);
    if !issuers.is_empty() {
        assert_eq!(reference, issuers, "ABPS-032 portable histories");
    }
    reference.last().unwrap().clone()
}
#[test]
fn abps_metamorphic_snapshots_zero_poll_batch_split_and_write_composition() {
    let original = portable_trace();
    let expected = end(&original);
    let mut observed = original.clone();
    observed.actions = original
        .actions
        .iter()
        .flat_map(|a| [a.clone(), Snapshot, Poll { budget: 0 }])
        .collect();
    assert_eq!(end(&observed), expected);
    let mut split = original.clone();
    let index = split
        .actions
        .iter()
        .position(|a| matches!(a, Batch { .. }))
        .unwrap();
    split.actions.splice(
        index..=index,
        [
            Batch {
                source: 0,
                ids: vec![1],
            },
            Batch {
                source: 0,
                ids: vec![2, 3],
            },
        ],
    );
    assert_eq!(end(&split), expected);
    let mut joined = original.clone();
    joined.actions.splice(
        2..4,
        [Write {
            id: 0,
            bytes: b"abcd".to_vec(),
        }],
    );
    assert_eq!(end(&joined), expected);
    // Compare the payload while it is still observable, not just after drain.
    let mut a = original.clone();
    a.actions.truncate(5);
    let mut b = joined.clone();
    b.actions.truncate(4);
    assert_eq!(end(&a), end(&b));
}

#[test]
fn abps_drop_permutations_and_budget_monotonic_prefixes() {
    let source = portable_trace();
    for a in 0..4 {
        for b in 0..4 {
            for c in 0..4 {
                if a == b || a == c || b == c {
                    continue;
                }
                let d = (0..4).find(|n| ![a, b, c].contains(n)).unwrap();
                let mut trace = source.clone();
                trace.actions.truncate(6);
                trace.actions.extend([a, b, c, d].map(|id| Drop { id }));
                trace
                    .actions
                    .extend([DropControls { domain: 0 }, Poll { budget: 2 }]);
                assert_eq!(end(&trace), end(&source));
            }
        }
    }
    // Fixed successful prefixes, not histories where a newly accepted request
    // steals a slot from a later request. Limits are configured on fresh pools.
    for limit in [0, 8, 16] {
        let mut low = portable_trace();
        low.actions = vec![Open {
            id: 0,
            limit,
            grants: [2, 2],
        }];
        low.actions.extend((0..limit / 8).map(|id| Acquire {
            id,
            domain: 0,
            size: 8,
        }));
        let mut high = low.clone();
        high.actions[0] = Open {
            id: 0,
            limit: 64,
            grants: [2, 2],
        };
        let mut model = InvariantOracle::new(low.config.clone(), true);
        for a in &low.actions {
            model.apply(a).unwrap().unwrap();
        }
        assert_eq!(end(&low), end(&high));
    }
}
