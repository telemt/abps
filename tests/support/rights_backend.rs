// Included in each backend adapter. The specification itself lives in rights.rs.
use super::*;
use crate::rights::{Action, CAPACITIES, InvariantOracle};
use crate::rights_trace::Trace;
use std::collections::{BTreeMap, HashSet};

enum ActualHandle {
    Mutable(BufMut),
    Frozen(Buf),
}
struct ActualDomain {
    owner: Option<DomainOwner>,
    handle: Option<Domain>,
    identity: DomainIdentity,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Observation {
    pub available: Vec<usize>,
    pub admitted: Vec<(usize, usize)>,
    pub payloads: Vec<(usize, Vec<u8>)>,
    pub free_records: usize,
    pub pending: usize,
}

fn observe(
    pool: &Pool,
    model: &InvariantOracle,
    domains: &BTreeMap<usize, ActualDomain>,
    handles: &BTreeMap<usize, ActualHandle>,
) -> Observation {
    let snapshot = pool.snapshot();
    for (class, c) in snapshot.classes.iter().enumerate() {
        assert_eq!(
            class_available(c),
            model.available(class),
            "PROG-01 available"
        );
        assert_eq!(c.total, model.config.counts[class], "MEM-01 fixed capacity");
        assert_eq!(c.reserved, model.assigned(class), "SI-05 pinned grants");
        assert_eq!(
            c.unassigned,
            model.config.counts[class] - model.assigned(class),
            "SI-05 unassigned"
        );
    }
    assert_eq!(
        snapshot.free_domain_records,
        model.free_records(),
        "LIFE-02 free records"
    );
    assert_eq!(
        pending(&snapshot),
        model.pending.len(),
        "LIFE-02 explicit maintenance"
    );
    let mut admitted = Vec::new();
    for (&id, expected) in &model.domains {
        if expected.recycled {
            continue;
        }
        let actual = snapshot
            .domains
            .iter()
            .find(|d| d.identity == domains[&id].identity)
            .expect("LIFE-02 missing domain incarnation");
        assert_eq!(domain_admitted(actual), model.admitted(id), "MEM-02 charge");
        assert_eq!(
            actual.phase,
            if expected.closed {
                DomainPhase::Closing
            } else {
                DomainPhase::Active
            },
            "LIFE-02 phase"
        );
        assert_eq!(actual.owner, expected.controls);
        assert_eq!(actual.handles, usize::from(expected.controls));
        assert_eq!(actual.limit, expected.limit);
        admitted.push((id, domain_admitted(actual)));
    }
    // Match independently predicted allocation incarnations, never infer an
    // expected count from the implementation's refcount or bitmap.
    let mut predicted: Vec<_> = model
        .allocations
        .iter()
        .map(|(&id, a)| {
            let mutable = model
                .handles
                .values()
                .any(|h| h.allocation == id && h.mutable);
            (
                a.class,
                Some(domains[&a.domain].identity),
                model.references(id),
                if mutable {
                    OwnershipKind::Mutable
                } else {
                    OwnershipKind::Immutable
                },
            )
        })
        .collect();
    for slot in snapshot
        .slots
        .iter()
        .filter(|s| s.ownership != OwnershipKind::Free)
    {
        let i = predicted
            .iter()
            .position(|p| *p == (slot.class, slot.origin, slot.references, slot.ownership))
            .expect("OWN-01/02 allocation rights");
        predicted.swap_remove(i);
    }
    assert!(predicted.is_empty(), "OWN-02 lost allocation");
    assert_eq!(handles.len(), model.handles.len());
    let payloads = handles
        .iter()
        .map(|(&id, h)| {
            let bytes = match h {
                ActualHandle::Mutable(b) => {
                    let a = &model.allocations[&model.handles[&id].allocation];
                    assert_eq!(
                        (b.filled_len(), b.initialized_len()),
                        (a.filled, a.initialized.len()),
                        "OWN-01 prefix"
                    );
                    b.as_ref()
                }
                ActualHandle::Frozen(b) => b.as_ref(),
            };
            assert_eq!(bytes, model.payload(id), "OWN-01 payload");
            (id, bytes.to_vec())
        })
        .collect();
    Observation {
        available: snapshot.classes.iter().map(class_available).collect(),
        admitted,
        payloads,
        free_records: snapshot.free_domain_records,
        pending: pending(&snapshot),
    }
}

pub fn replay(trace: &Trace) -> Vec<String> {
    let mut config = Config::new(
        CAPACITIES
            .into_iter()
            .zip(trace.config.counts)
            .map(|(capacity, slots)| ClassConfig { capacity, slots })
            .collect(),
        trace.config.records,
        4 << 20,
    );
    config.reference_limit = trace.config.reference_limit;
    if trace.config.reserved {
        config.capacity_policy = CapacityPolicy::Reserved;
    }
    let mut owner = prepare(config).unwrap();
    let pool = owner.activate().unwrap();
    let reservation = pool.snapshot().reservation;
    let mut domains: BTreeMap<usize, ActualDomain> = BTreeMap::new();
    let mut handles: BTreeMap<usize, ActualHandle> = BTreeMap::new();
    let mut identities = HashSet::new();
    let mut model = InvariantOracle::new(trace.config.clone(), EXPLICIT_MAINTENANCE);
    let mut observations = Vec::new();
    for (step, action) in trace.actions.iter().enumerate() {
        let before = model.clone();
        let expected = model
            .apply(action)
            .unwrap_or_else(|e| panic!("TRACE dependency at {step}: {e}"));
        if expected.is_err() {
            assert_eq!(model, before, "oracle mutated on API failure");
        }
        use Action::*;
        let mut observed_result = None;
        let checked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let actual = match action {
                Open { id, limit, grants } => {
                    let grants: Vec<_> = CAPACITIES
                        .into_iter()
                        .zip(*grants)
                        .filter(|(_, slots)| *slots > 0)
                        .map(|(capacity, slots)| ClassReservation { capacity, slots })
                        .collect();
                    let result = if trace.config.reserved {
                        pool.try_open_reserved_domain(*limit, &grants)
                    } else {
                        pool.try_open_domain(*limit)
                    };
                    result.map(|(owner, domain)| {
                        assert!(
                            identities.insert(domain.identity()),
                            "LIFE-02 repeated incarnation"
                        );
                        domains.insert(
                            *id,
                            ActualDomain {
                                identity: domain.identity(),
                                owner: Some(owner),
                                handle: Some(domain),
                            },
                        );
                    })
                }
                Acquire { id, domain, size } => domains[domain]
                    .handle
                    .as_ref()
                    .unwrap()
                    .try_acquire(*size)
                    .map(|b| {
                        handles.insert(*id, ActualHandle::Mutable(b));
                    }),
                Write { id, bytes } => match handles.get_mut(id).unwrap() {
                    ActualHandle::Mutable(b) => b.put_slice(bytes),
                    _ => unreachable!(),
                },
                Initialize { id, len } => match handles.get_mut(id).unwrap() {
                    ActualHandle::Mutable(b) => b.initialize_to(*len).map(|bytes| {
                        assert_eq!(
                            bytes,
                            &model.allocations[&model.handles[id].allocation].initialized[..*len],
                            "OWN-01 stale bytes"
                        );
                    }),
                    _ => unreachable!(),
                },
                SetFilled { id, len } => match handles.get_mut(id).unwrap() {
                    ActualHandle::Mutable(b) => b.set_filled_len(*len),
                    _ => unreachable!(),
                },
                Freeze { id } => {
                    let ActualHandle::Mutable(b) = handles.remove(id).unwrap() else {
                        unreachable!()
                    };
                    handles.insert(*id, ActualHandle::Frozen(b.freeze()));
                    Ok(())
                }
                Clone { source, id } | Slice { source, id, .. } => {
                    let ActualHandle::Frozen(b) = &handles[source] else {
                        unreachable!()
                    };
                    let result = if let Slice { start, end, .. } = action {
                        b.try_slice(*start..*end)
                    } else {
                        b.try_clone()
                    };
                    result.map(|b| {
                        handles.insert(*id, ActualHandle::Frozen(b));
                    })
                }
                Batch { source, ids } => {
                    let ActualHandle::Frozen(b) = &handles[source] else {
                        unreachable!()
                    };
                    let mut output: Vec<_> = (0..ids.len()).map(|_| None).collect();
                    let result = b.try_clone_into(&mut output);
                    if result.is_err() {
                        assert!(
                            output.iter().all(Option::is_none),
                            "SI-10 partial error output"
                        );
                    } else {
                        for (&id, b) in ids.iter().zip(output) {
                            handles.insert(id, ActualHandle::Frozen(b.unwrap()));
                        }
                    }
                    result
                }
                Drop { id } => {
                    drop(handles.remove(id).unwrap());
                    Ok(())
                }
                Close { domain } => {
                    domains[domain].owner.as_ref().unwrap().close();
                    Ok(())
                }
                DropControls { domain } => {
                    let d = domains.get_mut(domain).unwrap();
                    d.owner.take();
                    d.handle.take();
                    Ok(())
                }
                Poll { budget } => {
                    let (examined, reclaimed, pending) = poll(&pool, *budget);
                    assert_eq!(
                        examined,
                        before.pending.len().min(*budget),
                        "LIFE-02 bounded examination"
                    );
                    assert_eq!(pending, model.pending.len());
                    assert_eq!(reclaimed, model.free_records() - before.free_records());
                    Ok(())
                }
                Snapshot => Ok(()),
            };
            observed_result = Some(format!("{actual:?}"));
            assert_eq!(actual, expected, "API outcome at action {step}: {action:?}");
            assert_eq!(
                pool.snapshot().reservation,
                reservation,
                "MEM-01 fixed reservation"
            );
            let observation = observe(&pool, &model, &domains, &handles);
            (actual, observation)
        }));
        let (actual, observation) = checked.unwrap_or_else(|failure| {
            let message = failure
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| failure.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "non-string assertion".into());
            let context = serde_json::json!({
                "backend":BACKEND,"step":step,"action":format!("{action:?}"),
                "expected":format!("{expected:?}"),"observed":observed_result,
                "completed":observations});
            if let Some(key) = crate::rights_trace::FailureKey::from_assertion(BACKEND, &message, action) {
                eprintln!("{message}\nABPS_REPLAY_CONTEXT {context}\nABPS_FAILURE_KEY {}",key.json());
                std::panic::panic_any(crate::rights_trace::ReplayFailure { key, message, context });
            }
            // Unexpected panics are not allowed to masquerade as a known
            // postcondition during minimization.
            panic!("{message}\nABPS_REPLAY_CONTEXT {context}");
        });
        observations.push(format!("{actual:?} {observation:?}"));
    }
    drop(handles);
    drop(domains);
    poll(&pool, trace.config.records);
    drop(pool);
    owner.try_destroy().unwrap();
    observations
}
