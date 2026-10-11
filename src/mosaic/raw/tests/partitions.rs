//! SI-13 independent partition audit. Never use production list traversal.
use super::*;
use std::collections::BTreeSet;

#[derive(Clone)]
struct Partition {
    valid: Vec<usize>,
    unassigned: Vec<usize>,
    classes: Vec<std::ops::Range<usize>>,
    nodes: Vec<MaskNode>,
    rows: Vec<Vec<GrantEntry>>,
    owners: Vec<Option<usize>>,
    free_rows: Vec<usize>,
    free_node: usize,
    free_nodes: usize,
}
impl Partition {
    fn read(pool: &Pool) -> Self {
        let ptr = pool.root;
        let r = ptr.root();
        let w = usize::BITS as usize;
        let mut valid = Vec::new();
        let mut classes = Vec::new();
        for class in 0..r.class_count {
            let c = ptr.class(class);
            classes.push(c.word_first..c.word_first + c.words);
            for word in 0..c.words {
                let count = (c.slots - word * w).min(w);
                valid.push(usize::MAX >> (w - count));
            }
        }
        Self {
            valid,
            classes,
            unassigned: (0..r.plan.word_count)
                .map(|i| ptr.unassigned(i).get())
                .collect(),
            nodes: (0..r.plan.node_count).map(|i| ptr.node(i).get()).collect(),
            rows: (0..r.plan.row_count)
                .map(|i| (0..r.class_count).map(|c| ptr.entry(i, c).get()).collect())
                .collect(),
            owners: (0..r.domain_count)
                .map(|i| {
                    let row = ptr.domain(i).grant_row.get();
                    (row != NONE).then_some(row)
                })
                .collect(),
            free_rows: (0..r.free_rows.get())
                .map(|i| ptr.row_id(i).get())
                .collect(),
            free_node: r.free_node.get(),
            free_nodes: r.free_nodes.get(),
        }
    }
    fn validate(&self) -> Result<(), &'static str> {
        macro_rules! require {
            ($condition:expr,$message:literal) => {
                if !$condition {
                    return Err($message);
                }
            };
        }
        require!(self.valid.len() == self.unassigned.len(), "word count");
        let mut coverage = self.unassigned.clone();
        let mut nodes = BTreeSet::new();
        let mut rows = BTreeSet::new();
        for row in self.owners.iter().flatten() {
            require!(
                *row < self.rows.len() && rows.insert(*row),
                "duplicate/out-of-range row"
            );
            for (class, entry) in self.rows[*row].iter().enumerate() {
                let mut id = entry.head;
                let mut count = 0;
                let mut slots = 0;
                let mut members = BTreeSet::new();
                while id != NONE {
                    require!(
                        id < self.nodes.len() && nodes.insert(id),
                        "duplicate node or cycle"
                    );
                    members.insert(id);
                    let node = self.nodes[id];
                    require!(
                        self.classes[class].contains(&node.word),
                        "node in wrong class"
                    );
                    require!(
                        node.mask != 0 && node.mask & !self.valid[node.word] == 0,
                        "invalid grant mask"
                    );
                    require!(coverage[node.word] & node.mask == 0, "overlapping grants");
                    coverage[node.word] |= node.mask;
                    slots += node.mask.count_ones() as usize;
                    count += 1;
                    id = node.next;
                }
                require!(
                    entry.count == count && entry.reserved == slots,
                    "grant count"
                );
                require!(
                    if count == 0 {
                        entry.cursor == NONE
                    } else {
                        members.contains(&entry.cursor)
                    },
                    "cursor outside grant"
                );
            }
        }
        require!(
            coverage == self.valid,
            "partition does not cover valid slots"
        );
        for row in &self.free_rows {
            require!(
                *row < self.rows.len() && rows.insert(*row),
                "free row overlaps ownership"
            );
            require!(
                self.rows[*row]
                    .iter()
                    .all(|e| e.head == NONE && e.cursor == NONE && e.count == 0 && e.reserved == 0),
                "free row retains grants"
            );
        }
        require!(rows.len() == self.rows.len(), "lost row");
        let mut id = self.free_node;
        let mut free = 0;
        while id != NONE {
            require!(
                id < self.nodes.len() && nodes.insert(id),
                "duplicate free node or cycle"
            );
            free += 1;
            id = self.nodes[id].next;
        }
        require!(free == self.free_nodes, "free node count");
        require!(nodes.len() == self.nodes.len(), "lost node");
        Ok(())
    }
}
fn audit(pool: &Pool) {
    assert_eq!(
        Partition::read(pool).validate(),
        Ok(()),
        "SI-13 exact rights partition"
    );
}

#[test]
fn abps_sparse_partition_at_node_capacity_and_across_pending_returns() {
    let slots = if cfg!(miri) {
        4
    } else {
        usize::BITS as usize + 1
    };
    let mut c = config(slots, slots);
    c.capacity_policy = CapacityPolicy::Reserved;
    let mut owner = StorageOwner::prepare(&c).unwrap();
    let pool = owner.activate().unwrap();
    let grant = [ClassReservation {
        capacity: 16,
        slots: 1,
    }];
    let mut domains = Vec::new();
    audit(&pool);
    // One bit per domain attains the mathematical node/row bound without
    // inventing a metadata-exhaustion error that SI-13 does not permit.
    for _ in 0..slots {
        domains.push(pool.try_open_reserved_domain(32, &grant).unwrap());
        audit(&pool);
    }
    assert_eq!(
        (
            pool.root.root().free_nodes.get(),
            pool.root.root().free_rows.get()
        ),
        (0, 0)
    );
    for (a, d) in domains {
        let b = std::mem::ManuallyDrop::new(d.try_acquire(1).unwrap().freeze());
        let returning = b.lease.begin_return().unwrap().publish();
        drop((a, d));
        audit(&pool);
        assert_eq!(pool.poll_domains(1).reclaimed, 0);
        audit(&pool);
        returning.finish();
        audit(&pool);
        assert_eq!(pool.poll_domains(1).reclaimed, 1);
        audit(&pool);
        let replacement = pool.try_open_reserved_domain(32, &grant).unwrap();
        audit(&pool);
        drop(replacement);
        audit(&pool);
    }
    assert_eq!(pool.root.root().free_nodes.get(), slots);
    drop(pool);
    owner.try_destroy().unwrap();
}

#[test]
fn abps_fragmented_multiclass_partition_transaction_and_negative_controls() {
    let w = usize::BITS as usize;
    let mut c = Config::new(
        vec![
            ClassConfig {
                capacity: 8,
                slots: w + 1,
            },
            ClassConfig {
                capacity: 32,
                slots: 3,
            },
        ],
        3,
        1 << 20,
    );
    c.capacity_policy = CapacityPolicy::Reserved;
    let mut owner = StorageOwner::prepare(&c).unwrap();
    let pool = owner.activate().unwrap();
    for generation in 0..if cfg!(miri) { 2 } else { 32 } {
        let take = 1 + generation % w;
        let grant = [
            ClassReservation {
                capacity: 8,
                slots: take,
            },
            ClassReservation {
                capacity: 32,
                slots: 1,
            },
        ];
        let first = pool.try_open_reserved_domain(1024, &grant).unwrap();
        audit(&pool);
        let second = pool
            .try_open_reserved_domain(
                1024,
                &[
                    ClassReservation {
                        capacity: 8,
                        slots: w + 1 - take,
                    },
                    ClassReservation {
                        capacity: 32,
                        slots: 1,
                    },
                ],
            )
            .unwrap();
        audit(&pool);
        let before = pool.snapshot();
        assert_eq!(
            pool.try_open_reserved_domain(
                1024,
                &[
                    ClassReservation {
                        capacity: 8,
                        slots: 1
                    },
                    ClassReservation {
                        capacity: 32,
                        slots: 2
                    }
                ]
            )
            .err(),
            Some(Error::ReservationUnavailable)
        );
        assert_eq!(
            pool.snapshot().free_domain_records,
            before.free_domain_records
        );
        audit(&pool);
        let sample = Partition::read(&pool);
        let row = sample.owners.iter().flatten().next().copied().unwrap();
        let node = sample.rows[row][0].head;
        let mut bad = sample.clone();
        bad.nodes[node].next = node;
        assert!(bad.validate().is_err());
        let mut bad = sample.clone();
        bad.unassigned[bad.nodes[node].word] |= bad.nodes[node].mask;
        assert_eq!(bad.validate(), Err("overlapping grants"));
        let mut bad = sample.clone();
        bad.free_rows.push(row);
        assert!(bad.validate().is_err());
        if generation % 2 == 0 {
            drop(first);
            audit(&pool);
            drop(second);
        } else {
            drop(second);
            audit(&pool);
            drop(first);
        }
        audit(&pool);
    }
    drop(pool);
    owner.try_destroy().unwrap();
}

#[test]
fn abps_lazy_and_zero_preparation_have_identical_safe_observations() {
    fn observations(mode: super::super::super::PreparationPolicy) -> Vec<Vec<u8>> {
        let mut c = config(1, 1);
        c.preparation = mode;
        let mut owner = StorageOwner::prepare(&c).unwrap();
        let pool = owner.activate().unwrap();
        let (a, d) = pool.try_open_domain(16).unwrap();
        let mut out = Vec::new();
        for value in [0xa5, 1, 0xff, 0] {
            let mut b = d.try_acquire(16).unwrap();
            assert_eq!((b.filled_len(), b.initialized_len()), (0, 0));
            assert!(b.as_ref().is_empty());
            assert_eq!(b.set_filled_len(1), Err(Error::NotInitialized));
            out.push(b.initialize_to(3).unwrap().to_vec());
            b.put_slice(&[value; 2]).unwrap();
            out.push(b.freeze().as_ref().to_vec());
        }
        drop((a, d, pool));
        owner.try_destroy().unwrap();
        out
    }
    use crate::mosaic::PreparationPolicy;
    assert_eq!(
        observations(PreparationPolicy::Lazy),
        observations(PreparationPolicy::ZeroPayload)
    );
}

mod histories {
    use super::*;
    fn boundary_counts() -> Vec<usize> {
        let w = usize::BITS as usize;
        if cfg!(miri) {
            // Every step audits all records: O(slots^2) per history, beyond a
            // Miri budget from w+1 slots up. Native runs cover every word edge;
            // Miri covers multi-word grants in the fragmented/sparse tests.
            vec![2]
        } else {
            vec![2, w - 1, w, w + 1, 2 * w - 1, 2 * w, 2 * w + 1]
        }
    }
    fn read_rights(p: &Pool, d: usize) -> Vec<BTreeSet<usize>> {
        let part = Partition::read(p);
        assert_eq!(part.validate(), Ok(()));
        (0..part.classes.len())
            .map(|c| {
                let mut slots = BTreeSet::new();
                if let Some(row) = part.owners[d] {
                    let mut id = part.rows[row][c].head;
                    for _ in 0..part.rows[row][c].count {
                        let node = part.nodes[id];
                        for bit in 0..usize::BITS as usize {
                            if node.mask & (1 << bit) != 0 {
                                slots.insert(
                                    (node.word - part.classes[c].start) * usize::BITS as usize
                                        + bit,
                                );
                            }
                        }
                        id = node.next;
                    }
                }
                slots
            })
            .collect()
    }
    fn read_unassigned(p: &Pool) -> Vec<BTreeSet<usize>> {
        (0..p.root.root().class_count)
            .map(|c| {
                let class = p.root.class(c);
                (0..class.slots)
                    .filter(|&s| {
                        p.root
                            .unassigned(class.word_first + s / usize::BITS as usize)
                            .get()
                            & (1 << (s % usize::BITS as usize))
                            != 0
                    })
                    .collect()
            })
            .collect()
    }
    fn audit_structure(p: &Pool) {
        audit(p);
    }
    include!("../../../test_support/grant_histories.rs");
}

#[test]
#[cfg(debug_assertions)]
fn sparse_preflight_assertions_precede_all_mutation() {
    let mut c = config(2, 2);
    c.capacity_policy = CapacityPolicy::Reserved;
    let mut storage = StorageOwner::prepare(&c).unwrap();
    let pool = storage.activate().unwrap();
    for rows in [true, false] {
        let counter = if rows {
            &pool.root.root().free_rows
        } else {
            &pool.root.root().free_nodes
        };
        let saved = counter.replace(0);
        let controls = pool.root.root().controls.get();
        let free = pool.root.root().free_count.get();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            pool.try_open_reserved_domain(
                16,
                &[ClassReservation {
                    capacity: 16,
                    slots: 1,
                }],
            )
        }));
        counter.set(saved);
        if cfg!(debug_assertions) {
            assert!(result.is_err());
            assert_eq!(pool.root.root().controls.get(), controls);
            assert_eq!(pool.root.root().free_count.get(), free);
            audit(&pool);
        } else {
            unreachable!("debug-only detector");
        }
    }
    drop(pool);
    storage.try_destroy().unwrap();
}
