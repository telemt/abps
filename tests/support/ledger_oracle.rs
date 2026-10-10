
use std::collections::{HashMap, VecDeque};

use super::backend::*;
#[cfg(test)]
use proptest::prelude::*;
use crate::trace::{Trace, Observation};

const CAPACITIES: [usize; 2] = [8, 32];

struct Session {
    record_alive: bool,
    closed: bool,
    owner: Option<DomainOwner>,
    handle: Option<Domain>,
    identity: DomainIdentity,
    limit: usize,
    grants: [usize; 2],
}

enum Actual {
    Mutable(BufMut),
    Frozen(Buf),
}
struct Handle {
    actual: Actual,
    allocation: usize,
    bytes: Vec<u8>,
    initialized: Vec<u8>,
}

// Independent oracle: allocation IDs, copied logical bytes, and plain counters.
// It never calls production admission/recycle helpers or uses observed counters
// to predict success. Only the opaque domain identity comes from the public API.
struct ModelAllocation {
    domain: usize,
    class: usize,
    references: usize,
    frozen: bool,
}

#[allow(clippy::too_many_arguments)]
fn verify(
    pool: &Pool,
    reservation: Reservation,
    sessions: &[Session],
    model: &HashMap<usize, ModelAllocation>,
    handles: &[Handle],
    candidates: &VecDeque<usize>,
    reserved: bool,
    counts: [usize; 2],
) -> Observation {
    let snapshot = pool.snapshot(); // quiescent ownership counts only
    assert_eq!(
        snapshot.reservation, reservation,
        "MEM-01 reservation ledger"
    );
    for (index, class) in snapshot.classes.iter().enumerate() {
        let issued = model.values().filter(|a| a.class == index).count();
        assert_eq!(class.total, counts[index]);
        assert_eq!(class_counts(class).0, counts[index] - issued);
        assert_eq!(class_counts(class).1, issued);
        let assigned = if reserved { sessions.iter().filter(|s| s.record_alive).map(|s| s.grants[index]).sum() } else { 0 };
        assert_eq!(class.reserved, assigned, "SI-05 assignment ledger");
        assert_eq!(class.unassigned, counts[index] - assigned);
        assert_eq!(
            class.total,
            class_counts(class).0 + class_counts(class).1,
            "PROG-01 capacity ledger"
        );
    }
    assert_eq!(pending(&snapshot), candidates.len(), "SI-01 pending records");
    let mut occupied_domains = 0;
    for (index, session) in sessions.iter().enumerate() {
        let allocations: Vec<_> = model.values().filter(|a| a.domain == index).collect();
        if !session.record_alive {
            assert!(session.owner.is_none() && session.handle.is_none() && allocations.is_empty());
            continue;
        }
        occupied_domains += 1;
        let domain = snapshot
            .domains
            .iter()
            .find(|d| d.identity == session.identity)
            .unwrap();
        let bytes: usize = allocations.iter().map(|a| CAPACITIES[a.class]).sum();
        assert_eq!(domain.limit, session.limit);
        assert_eq!(
            domain.phase,
            if session.owner.is_some() && !session.closed {
                DomainPhase::Active
            } else {
                DomainPhase::Closing
            }
        );
        assert_eq!(domain.owner, session.owner.is_some());
        assert_eq!(domain.handles, usize::from(session.handle.is_some()));
        assert_eq!(allocation_count(&snapshot, domain), allocations.len());
        assert_eq!(domain_bytes(domain).0, bytes, "MEM-02 admission ledger");
        assert_eq!(domain_bytes(domain).1, bytes);
        assert!(bytes <= session.limit);
        if reserved {
            for (class, grant) in domain.grants.iter().enumerate() {
                let used = allocations.iter().filter(|a| a.class == class).count();
                assert_eq!(grant.reserved, session.grants[class]);
                assert_eq!(grant.available, session.grants[class] - used);
            }
        } else { assert!(domain.grants.is_empty()); }
    }
    assert_eq!(snapshot.free_domain_records, 3 - occupied_domains);
    assert_eq!(snapshot.retired_domain_records, 0);
    let mut rights: Vec<_> = model
        .values()
        .map(|a| {
            (
                a.class,
                Some(sessions[a.domain].identity),
                if a.frozen {
                    OwnershipKind::Immutable
                } else {
                    OwnershipKind::Mutable
                },
                a.references,
            )
        })
        .collect();
    for slot in snapshot
        .slots
        .iter()
        .filter(|s| s.ownership != OwnershipKind::Free)
    {
        let index = rights
            .iter()
            .position(|v| *v == (slot.class, slot.origin, slot.ownership, slot.references))
            .expect("OWN-01/02 ownership ledger");
        rights.swap_remove(index);
    }
    assert!(rights.is_empty());
    for handle in handles {
        let actual = match &handle.actual {
            Actual::Mutable(b) => {
                assert_eq!(b.filled_len(), handle.bytes.len(), "OWN-01 filled prefix");
                assert_eq!(b.initialized_len(), handle.initialized.len(), "OWN-01 initialized prefix");
                b.as_ref()
            },
            Actual::Frozen(b) => b.as_ref(),
        };
        assert_eq!(actual, handle.bytes);
    }
    Observation {
        available: snapshot.classes.iter().map(|c| class_counts(c).0).collect(),
        admitted: sessions.iter().map(|s| snapshot.domains.iter().find(|d| d.identity == s.identity).map_or(0, |d| domain_bytes(d).0)).collect(),
        payloads: handles.iter().map(|h| match &h.actual { Actual::Mutable(b) => b.as_ref().to_vec(), Actual::Frozen(b) => b.as_ref().to_vec() }).collect(),
    }

}

pub fn replay(trace: &Trace) -> Vec<Observation> {
    let reserved = trace.reserved;
    let counts = trace.counts;
        let mut config = Config::new(CAPACITIES.into_iter().zip(counts).map(|(capacity, slots)| ClassConfig { capacity, slots }).collect(), 3, 4 << 20);
        config.reference_limit = 4;
        if reserved { config.capacity_policy = CapacityPolicy::Reserved; }
        let mut owner = prepare(config).unwrap();
        let pool = owner.activate().unwrap();
        let reservation = pool.snapshot().reservation;
        let mut sessions: Vec<Session> = Vec::new();
        let mut model: HashMap<usize, ModelAllocation> = HashMap::new();
        let mut handles: Vec<Handle> = Vec::new();
        let mut next_allocation = 0;
        let mut candidates = VecDeque::new();
        let mut observations = Vec::new();
        for action in &trace.actions {
            let (op, x, y, z) = (action.kind as u8, action.x, action.y, action.z);
            match op {
                0 => {
                    let occupied = sessions.iter().filter(|s| s.record_alive).count();
                    let limit = [0, 8, 16, 32, 64, 128][x as usize % 6];
                    let grants = [y as usize % (counts[0] + 2), z as usize % (counts[1] + 2)];
                    let requests: Vec<_> = CAPACITIES.iter().zip(grants).filter(|(_, n)| *n > 0).map(|(&capacity, slots)| ClassReservation { capacity, slots }).collect();
                    let unavailable = reserved && (0..2).any(|c| sessions.iter().filter(|s| s.record_alive).map(|s| s.grants[c]).sum::<usize>() + grants[c] > counts[c]);
                    let result = if reserved { pool.try_open_reserved_domain(limit, &requests) } else { pool.try_open_domain(limit) };
                    if unavailable { assert_eq!(result.err(), Some(Error::ReservationUnavailable)); }
                    else if occupied == 3 {
                        let expected = if candidates.is_empty() { Error::DomainCapacityExhausted } else { Error::DomainMaintenanceRequired };
                        assert_eq!(result.err(), Some(expected));
                    }
                    else {
                        let (authority, domain) = result.unwrap();
                        let identity = domain.identity();
                        assert!(sessions.iter().all(|s| s.identity != identity), "LIFE-01 no identity reuse");
                        sessions.push(Session { record_alive: true, closed: false, owner: Some(authority), handle: Some(domain), identity, limit, grants });
                    }
                }
                1 | 2 if !sessions.is_empty() => {
                    let index = x as usize % sessions.len();
                    let session = &mut sessions[index];
                    let had_control = session.owner.is_some() || session.handle.is_some();
                    if op == 1 { session.owner.take(); } else { session.handle.take(); }
                    if EXPLICIT_MAINTENANCE && had_control && session.owner.is_none() && session.handle.is_none() {
                        if model.values().any(|a| a.domain == index) { candidates.push_back(index); }
                        else { session.record_alive = false; }
                    }
                }
                3 if !sessions.is_empty() => {
                    let domain_index = x as usize % sessions.len();
                    let session = &sessions[domain_index];
                    if let Some(domain) = &session.handle {
                        let size = [0, 1, 7, 8, 9, 31, 32, 33, usize::MAX][y as usize % 9];
                        let class = CAPACITIES.iter().position(|c| *c >= size);
                        let admitted: usize = model.values().filter(|a| a.domain == domain_index).map(|a| CAPACITIES[a.class]).sum();
                        let expected = if size == 0 { Some(Error::InvalidSize) }
                            else if class.is_none() { Some(Error::TooLarge) }
                            else if session.owner.is_none() || session.closed { Some(Error::Closed) }
                            else {
                                let class = class.unwrap();
                                if admitted + CAPACITIES[class] > session.limit { Some(Error::BudgetExceeded) }
                                else if reserved && model.values().filter(|a| a.class == class && a.domain == domain_index).count() == session.grants[class] { Some(Error::DomainClassDepleted) }
                                else if !reserved && model.values().filter(|a| a.class == class).count() == counts[class] { Some(Error::ClassDepleted) }
                                else { None }
                            };
                        match (expected, domain.try_acquire(size)) {
                            (Some(expected), Err(actual)) => assert_eq!(actual, expected),
                            (None, Ok(buffer)) => {
                                model.insert(next_allocation, ModelAllocation { domain: domain_index, class: class.unwrap(), references: 1, frozen: false });
                                handles.push(Handle { actual: Actual::Mutable(buffer), allocation: next_allocation, bytes: Vec::new(), initialized: Vec::new() });
                                next_allocation += 1;
                            }
                            (expected, actual) => panic!("acquire mismatch: expected {expected:?}, actual error {:?}", actual.err()),
                        }
                    }
                }
                4 if !handles.is_empty() => {
                    let index = x as usize % handles.len();
                    let handle = &mut handles[index];
                    if let Actual::Mutable(buffer) = &mut handle.actual {
                        let bytes = vec![z; y as usize % 12];
                        let expected = handle.bytes.len() + bytes.len() <= CAPACITIES[model[&handle.allocation].class];
                        let result = buffer.put_slice(&bytes);
                        if expected {
                            result.unwrap();
                            let start = handle.bytes.len(); let end = start + bytes.len();
                            handle.initialized.resize(handle.initialized.len().max(end), 0);
                            handle.initialized[start..end].copy_from_slice(&bytes);
                            handle.bytes = handle.initialized[..end].to_vec();
                        }
                        else { assert_eq!(result, Err(Error::InvalidRange)); }
                    }
                }
                9 | 13 if !handles.is_empty() => {
                    let index = x as usize % handles.len();
                    let h = &mut handles[index];
                    if let Actual::Mutable(b) = &mut h.actual {
                        let len = if y == 255 { usize::MAX } else { y as usize % 35 };
                        if op == 9 {
                            if len > CAPACITIES[model[&h.allocation].class] { assert_eq!(b.initialize_to(len).err(), Some(Error::InvalidRange)); }
                            else {
                                h.initialized.resize(h.initialized.len().max(len), 0);
                                let prefix = b.initialize_to(len).unwrap();
                                assert_eq!(prefix, &h.initialized[..len], "OWN-01 stale bytes");
                                prefix.fill(z); h.initialized[..len].fill(z);
                                h.bytes = h.initialized[..h.bytes.len()].to_vec();
                            }
                        } else if len > h.initialized.len() { assert_eq!(b.set_filled_len(len), Err(Error::NotInitialized)); }
                        else { b.set_filled_len(len).unwrap(); h.bytes = h.initialized[..len].to_vec(); }
                    }
                }
                14 if !sessions.is_empty() => {
                    let i = x as usize % sessions.len();
                    if let Some(a) = &sessions[i].owner { a.close(); sessions[i].closed = true; }
                }
                15 if !handles.is_empty() => {
                    let i = x as usize % handles.len();
                    if let Actual::Frozen(b) = &handles[i].actual {
                        // A live destination must survive OutputNotEmpty unchanged.
                        match b.try_clone() {
                            Ok(existing) => {
                                assert!(model[&handles[i].allocation].references < 4);
                                let mut dst = [Some(existing)];
                                assert_eq!(b.try_clone_into(&mut dst), Err(Error::OutputNotEmpty));
                                assert_eq!(dst[0].as_ref().unwrap().as_ref(), handles[i].bytes);
                            }
                            Err(e) => {
                                assert_eq!(model[&handles[i].allocation].references, 4);
                                assert_eq!(e, Error::ReferenceLimit);
                            }
                        }
                    }
                }
                5 if !handles.is_empty() => {
                    let index = x as usize % handles.len();
                    let mut handle = handles.swap_remove(index);
                    if let Actual::Mutable(buffer) = handle.actual {
                        handle.actual = Actual::Frozen(buffer.freeze());
                        model.get_mut(&handle.allocation).unwrap().frozen = true;
                    }
                    handles.push(handle);
                }
                6 | 7 if !handles.is_empty() => {
                    let handle = &handles[x as usize % handles.len()];
                    if let Actual::Frozen(buffer) = &handle.actual {
                        let range = if op == 6 { 0..handle.bytes.len() } else { y as usize % 35..z as usize % 35 };
                        let allocation = model.get_mut(&handle.allocation).unwrap();
                        let expected = if range.start > range.end || range.end > handle.bytes.len() { Some(Error::InvalidRange) }
                            else if allocation.references == 4 { Some(Error::ReferenceLimit) } else { None };
                        match (expected, if op == 6 { buffer.try_clone() } else { buffer.try_slice(range.clone()) }) {
                            (Some(expected), Err(actual)) => assert_eq!(actual, expected),
                            (None, Ok(view)) => {
                                allocation.references += 1;
                                let next = Handle { actual: Actual::Frozen(view), allocation: handle.allocation, bytes: handle.bytes[range].to_vec(), initialized: Vec::new() };
                                handles.push(next);
                            }
                            (expected, actual) => panic!("view mismatch {expected:?}, {actual:?}"),
                        }
                    }
                }
                12 if !handles.is_empty() => {
                    let handle = &handles[x as usize % handles.len()];
                    if let Actual::Frozen(buffer) = &handle.actual {
                        let id = handle.allocation;
                        let bytes = handle.bytes.clone();
                        let count = y as usize % 6;
                        let mut out: Vec<_> = (0..count).map(|_| None).collect();
                        let allocation = model.get_mut(&id).unwrap();
                        let result = buffer.try_clone_into(&mut out);
                        if allocation.references + count > 4 {
                            assert_eq!(result, Err(Error::ReferenceLimit));
                            assert!(out.iter().all(Option::is_none));
                        } else {
                            result.unwrap();
                            allocation.references += count;
                            for b in out { handles.push(Handle { actual:Actual::Frozen(b.unwrap()), allocation:id, bytes:bytes.clone(), initialized: Vec::new() }); }
                        }
                    }
                }
                8 | 10 if !handles.is_empty() => {
                    let handle = handles.swap_remove(x as usize % handles.len());
                    let allocation = model.get_mut(&handle.allocation).unwrap();
                    allocation.references -= 1;
                    if allocation.references == 0 { model.remove(&handle.allocation); }
                    drop(handle);
                }
                11 => {
                    let n = x as usize % 5;
                    let examined = n.min(candidates.len());
                    let mut reclaimed = 0;
                    for _ in 0..examined {
                        let i = candidates.pop_front().unwrap();
                        if model.values().any(|a| a.domain == i) { candidates.push_back(i); }
                        else { sessions[i].record_alive = false; reclaimed += 1; }
                    }
                    assert_eq!(poll(&pool, n), (examined, reclaimed, candidates.len()));
                }
                _ => {}
            }
            if !EXPLICIT_MAINTENANCE {
                for (i, s) in sessions.iter_mut().enumerate() {
                    if s.owner.is_none() && s.handle.is_none() && !model.values().any(|a| a.domain == i) {
                        s.record_alive = false;
                    }
                }
            }
            // A common maintenance barrier makes immediate reference recycle
            // and explicit issuer recycle comparable without inventing semantics.
            if EXPLICIT_MAINTENANCE {
                let examined = candidates.len(); let mut reclaimed = 0;
                for _ in 0..examined {
                    let i = candidates.pop_front().unwrap();
                    if model.values().any(|a| a.domain == i) { candidates.push_back(i); }
                    else { sessions[i].record_alive = false; reclaimed += 1; }
                }
                assert_eq!(poll(&pool, 3), (examined, reclaimed, candidates.len()));
            }
            observations.push(verify(&pool, reservation, &sessions, &model, &handles, &candidates, reserved, counts));
        }
        owner.close().unwrap();
        drop((handles, sessions, pool));
        owner.try_destroy().unwrap();
        observations
}

#[cfg(test)]
proptest! {
    #![proptest_config(crate::property_config())]
    #[test]
    fn four_ledgers_follow_generated_ownership_and_domain_sequences(input in prop::collection::vec(any::<u8>(), 3..crate::max_trace_bytes())) {
        replay(&Trace::decode(&input, false));
    }
}
