// SI-05/SI-13 logical slot rights, independent of bitmap words and node lists.
// Included in each backend's raw tests. The adapter only reads representation;
// expected ownership uses sets of integer slot identities and domain incarnations.
use std::collections::{BTreeMap, BTreeSet};

type Rights = Vec<BTreeSet<usize>>;
struct GrantOracle {
    free: Rights,
    owned: BTreeMap<usize, (DomainIdentity, Rights)>,
}
impl GrantOracle {
    fn new(counts: [usize; 2]) -> Self {
        Self {
            free: counts.into_iter().map(|n| (0..n).collect()).collect(),
            owned: BTreeMap::new(),
        }
    }
    fn assign(&mut self, key: DomainIdentity, counts: [usize; 2]) {
        let rights: Rights = counts
            .into_iter()
            .enumerate()
            .map(|(c, n)| {
                assert!(self.free[c].len() >= n);
                let selected: BTreeSet<_> = self.free[c].iter().copied().take(n).collect();
                for slot in &selected {
                    assert!(self.free[c].remove(slot));
                }
                selected
            })
            .collect();
        assert!(self.owned.insert(key.index, (key, rights)).is_none());
    }
    fn recycle(&mut self, key: DomainIdentity) {
        let (old, rights) = self.owned.remove(&key.index).unwrap();
        assert_eq!(old, key, "LIFE-01 grant incarnation");
        for (c, slots) in rights.into_iter().enumerate() {
            for slot in slots {
                assert!(self.free[c].insert(slot), "SI-13 double return");
            }
        }
    }
    fn check(&self, p: &Pool) {
        audit_structure(p);
        for index in 0..p.root.root().domain_count {
            let expected = self
                .owned
                .get(&index)
                .map(|(_, rights)| rights.clone())
                .unwrap_or_else(|| vec![BTreeSet::new(); 2]);
            assert_eq!(
                read_rights(p, index),
                expected,
                "SI-13 exact rights for record {index}"
            );
        }
        assert_eq!(
            read_unassigned(p),
            self.free,
            "SI-13 unassigned logical rights"
        );
    }
}
fn reservations(counts: [usize; 2]) -> Vec<ClassReservation> {
    [8, 32]
        .into_iter()
        .zip(counts)
        .filter_map(|(capacity, slots)| {
            (slots != 0).then_some(ClassReservation { capacity, slots })
        })
        .collect()
}

fn history(counts: [usize; 2], requests: &[[u8; 2]]) {
    let mut config = Config::new(
        [8, 32]
            .into_iter()
            .zip(counts)
            .map(|(capacity, slots)| crate::ClassConfig { capacity, slots })
            .collect(),
        3,
        4 << 20,
    );
    config.capacity_policy = CapacityPolicy::Reserved;
    let mut owner = StorageOwner::prepare(&config).unwrap();
    let pool = owner.activate().unwrap();
    let mut model = GrantOracle::new(counts);
    let mut generations = BTreeMap::new();
    for (incarnation, request) in requests.iter().enumerate() {
        // One neighbor remains live across close, pending return, recycling and
        // reopening. Each iteration fully drains, so shrinking cannot rebind a
        // reference from a removed incarnation to a different live domain.
        let a_count = std::array::from_fn(|c| 1 + usize::from(request[c]) % (counts[c] - 1));
        let b_count = std::array::from_fn(|c| counts[c] - a_count[c]);
        let a = pool
            .try_open_reserved_domain(usize::MAX, &reservations(a_count))
            .unwrap();
        let old = a.1.identity();
        if let Some(previous) = generations.insert(old.index, old.generation) {
            assert!(old.generation > previous, "LIFE-01 generation advance");
        }
        model.assign(old, a_count);
        model.check(&pool);
        let b = pool
            .try_open_reserved_domain(usize::MAX, &reservations(b_count))
            .unwrap();
        model.assign(b.1.identity(), b_count);
        model.check(&pool);
        // An empty grant has no row/nodes and still consumes a domain record.
        let empty = pool.try_open_reserved_domain(0, &[]).unwrap();
        model.assign(empty.1.identity(), [0, 0]);
        model.check(&pool);
        assert_eq!(empty.1.try_acquire(8).err(), Some(Error::BudgetExceeded));
        model.check(&pool);
        let empty_key = empty.1.identity();
        drop(empty);
        model.recycle(empty_key);
        model.check(&pool);
        assert_eq!(
            pool.try_open_reserved_domain(usize::MAX, &reservations([1, 1]))
                .err(),
            Some(Error::ReservationUnavailable)
        );
        model.check(&pool);

        let mut left = Vec::new();
        let mut right = Vec::new();
        let mut identities = BTreeSet::new();
        for (pair, requested, dst) in [(&a, a_count, &mut left), (&b, b_count, &mut right)] {
            for (c, n) in requested.into_iter().enumerate() {
                for _ in 0..n {
                    let mut buffer = pair.1.try_acquire([8, 32][c]).unwrap();
                    let slot = buffer.lease.slot - pool.root.class(c).first;
                    assert!(model.owned[&pair.1.identity().index].1[c].contains(&slot));
                    assert!(
                        identities.insert((c, slot)),
                        "OWN-03 duplicate physical slot"
                    );
                    let pattern = if pair.1.identity() == old { 0xa5 } else { 0x37 };
                    buffer.put_slice(&[pattern; 8]).unwrap();
                    dst.push(buffer.freeze());
                    model.check(&pool);
                }
                assert_eq!(
                    pair.1.try_acquire([8, 32][c]).err(),
                    Some(Error::DomainClassDepleted)
                );
                model.check(&pool);
            }
            assert_eq!(
                pair.1.metrics().admitted_bytes,
                requested[0] * 8 + requested[1] * 32
            );
        }
        let last = left.pop().unwrap();
        let clone = last.try_clone().unwrap();
        model.check(&pool);
        drop(left);
        model.check(&pool);
        a.0.close();
        assert_eq!(a.1.try_acquire(8).err(), Some(Error::Closed));
        model.check(&pool);
        drop(a);
        model.check(&pool);
        assert_eq!(pool.poll_domains(0).examined, 0);
        model.check(&pool);
        assert_eq!(pool.poll_domains(1).reclaimed, 0);
        model.check(&pool);
        drop(last);
        model.check(&pool);
        assert_eq!(pool.poll_domains(2).reclaimed, 0);
        model.check(&pool);
        drop(clone);
        model.check(&pool); // complete refund alone does not recycle
        assert_eq!(pool.poll_domains(1).reclaimed, 1);
        model.recycle(old);
        model.check(&pool);
        let next = pool
            .try_open_reserved_domain(usize::MAX, &reservations(a_count))
            .unwrap();
        let key = next.1.identity();
        assert_eq!(key.index, old.index);
        assert!(
            key.generation > old.generation,
            "LIFE-01 incarnation {incarnation}"
        );
        model.assign(key, a_count);
        model.check(&pool);
        for retained in &right {
            assert_eq!(retained.as_ref(), &[0x37; 8], "OWN-01 neighbor payload");
        }
        drop(next);
        model.recycle(key);
        model.check(&pool);
        drop(right);
        model.check(&pool);
        let key = b.1.identity();
        drop(b);
        model.recycle(key);
        model.check(&pool);
        assert_eq!(pool.poll_domains(3).examined, 0);
    }
    drop(pool);
    owner.try_destroy().unwrap();
}

#[test]
fn grant_histories_cover_word_edges_and_record_reuse() {
    for n in boundary_counts() {
        history([n, 3], &[[0, 0], [1, 2], [255, 127]]);
    }
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config {
        cases: if cfg!(miri) {2} else { std::env::var("ABPS_PROPTEST_CASES").ok().map(|n| n.parse().unwrap()).unwrap_or(128) },
        failure_persistence: if cfg!(miri) {None} else {Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource("proptest-regressions")))},
        ..Default::default()
    })]
    #[test]
    fn grant_histories_match_logical_slot_sets(
        requests in proptest::collection::vec(proptest::prelude::any::<[u8;2]>(), 1..=if cfg!(miri) {2} else {8}),
        count in 2usize..=8
    ) {
        // Proptest persists the concrete request vector and shrinks complete,
        // independent incarnations. General interleaved histories/shrinking
        // remain in the shared public rights oracle.
        history([count, 3], &requests);
    }
}
