//! BR-01..05: independent family/range model shared by tests and libFuzzer.
#![forbid(unsafe_code)]
use abps_reference::mosaic::*;
use std::ops::Range;
pub fn replay(input: &[u8]) {
    let families = 1 + input.first().copied().unwrap_or(0) as usize % 4;
    let limit = 1 + input.get(1).copied().unwrap_or(0) as usize % 6;
    let ops = input
        .get(2..)
        .unwrap_or_default()
        .chunks_exact(5)
        .take(1024)
        .map(|a| {
            (
                a[0] % 5,
                a[1] as usize % families,
                a[2] as usize,
                a[3],
                a[4],
            )
        });
    let mut c = Config::new(
        vec![ClassConfig {
            capacity: 16,
            slots: 1,
        }],
        1,
        1 << 20,
    );
    c.local_branches = Some(LocalBranchConfig {
        max_branches_per_slot: families,
        local_reference_limit: limit,
    });
    let mut owner = StorageOwner::prepare(&c).unwrap();
    let pool = owner.activate().unwrap();
    let (authority, domain) = pool.try_open_domain(16).unwrap();
    let reservation = owner.reservation().unwrap();
    let mut actual: Vec<Vec<LocalBuf>> = (0..families).map(|_| Vec::new()).collect();
    let mut model: Vec<Vec<Range<usize>>> = (0..families).map(|_| Vec::new()).collect();
    let mut generation = 0u8;
    let mut payload = [0u8; 8];
    for (op, branch, pick, a, z) in ops {
        if model.iter().all(Vec::is_empty) {
            let mut lease = domain.try_acquire(8).unwrap();
            assert_eq!(lease.initialized_len(), 0);
            assert!(lease.as_ref().is_empty());
            generation = generation.wrapping_add(1);
            payload = std::array::from_fn(|i| generation.wrapping_add(i as u8));
            lease.put_slice(&payload).unwrap();
            let active = 1 + usize::from(a) % families;
            let mut out: Vec<_> = (0..active).map(|_| None).collect();
            lease.freeze_branches_into(&mut out).unwrap();
            for i in 0..active {
                actual[i].push(out[i].take().unwrap().into_local());
                model[i].push(0..8);
            }
        }
        if !model[branch].is_empty() {
            let index = pick % model[branch].len();
            let range = model[branch][index].clone();
            match op {
                0 | 1 => {
                    let start = if op == 0 { 0 } else { a as usize % 11 };
                    let end = if op == 0 {
                        range.len()
                    } else {
                        z as usize % 11
                    };
                    let expected = if start > end || end > range.len() {
                        Some(Error::InvalidRange)
                    } else if model[branch].len() == limit {
                        Some(Error::ReferenceLimit)
                    } else {
                        None
                    };
                    match actual[branch][index].try_slice(start..end) {
                        Ok(view) => {
                            assert_eq!(expected, None);
                            actual[branch].push(view);
                            model[branch].push(range.start + start..range.start + end);
                        }
                        Err(error) => assert_eq!(Some(error), expected),
                    }
                }
                2 => {
                    actual[branch].remove(index);
                    model[branch].remove(index);
                }
                3 => {
                    let local = actual[branch].remove(index);
                    let restored = match local.try_into_branch() {
                        Ok(token) => {
                            assert_eq!(model[branch].len(), 1);
                            token.into_local()
                        }
                        Err(local) => {
                            assert!(model[branch].len() > 1);
                            local
                        }
                    };
                    actual[branch].insert(index, restored);
                }
                _ => {
                    let view = actual[branch][index].try_view(0..range.len()).unwrap();
                    assert_eq!(view.as_ref(), &payload[range]);
                }
            }
        }
        let groups = model.iter().filter(|v| !v.is_empty()).count();
        let snapshot = pool.snapshot();
        assert_eq!(snapshot.slots[0].references, groups);
        assert_eq!(
            snapshot.classes[0].observed_available_slots,
            usize::from(groups == 0)
        );
        assert_eq!(
            domain.metrics().admitted_bytes,
            if groups == 0 { 0 } else { 16 }
        );
        assert_eq!(snapshot.reservation.total_bytes, reservation.total_bytes);
        for i in 0..families {
            for (v, r) in actual[i].iter().zip(&model[i]) {
                assert_eq!(v.as_ref(), &payload[r.clone()]);
            }
        }
    }
    drop(actual);
    assert_eq!(domain.metrics().admitted_bytes, 0);
    drop((authority, domain, pool));
    owner.try_destroy().unwrap();
}
