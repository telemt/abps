// MEM-01: independent wide range arithmetic, never Layout::extend/array.
fn range<T>(offset: usize, count: usize) -> (u128, u128) {
    assert_eq!(offset % align_of::<T>(), 0, "MEM-01 typed alignment");
    (
        offset as u128,
        offset as u128 + count as u128 * size_of::<T>() as u128,
    )
}
#[test]
fn layout_ranges_are_disjoint_aligned_and_fully_charged() {
    for classes in 1..=2 {
        for slots in 1..=3 {
            for records in 1..=3 {
                for reserved in [false, true] {
                    let mut c = Config::new(
                        (0..classes)
                            .map(|i| crate::ClassConfig {
                                capacity: 3 + 14 * i,
                                slots,
                            })
                            .collect(),
                        records,
                        4 << 20,
                    );
                    c.capacity_policy = if reserved {
                        CapacityPolicy::Reserved
                    } else {
                        CapacityPolicy::Unreserved
                    };
                    configure_layout(&mut c);
                    let p = Plan::new(&c).unwrap();
                    let mut ranges = vec![
                        range::<Root>(0, 1),
                        range::<Class>(p.classes, classes),
                        range::<DomainRecord>(p.domains, records),
                        range::<Slot>(p.slots, slots * classes),
                        range::<usize>(p.free_ids, records),
                    ];
                    additional_ranges(&p, &c, &mut ranges);
                    let mut backing = 0u128;
                    for (i, class) in c.classes.iter().enumerate() {
                        let bytes = class.capacity as u128 * class.slots as u128;
                        ranges.push((p.payload[i] as u128, p.payload[i] as u128 + bytes));
                        backing += bytes;
                    }
                    ranges.retain(|(start, end)| start != end);
                    ranges.sort_unstable();
                    for pair in ranges.windows(2) {
                        assert!(pair[0].1 <= pair[1].0, "MEM-01 overlapping ranges {pair:?}");
                    }
                    assert!(ranges.last().unwrap().1 <= p.layout.size() as u128);
                    assert_eq!(p.layout.size() % p.layout.align(), 0);
                    assert_eq!(p.reservation.backing_bytes as u128, backing);
                    assert_eq!(
                        p.reservation.total_bytes as u128,
                        p.layout.size() as u128 + size_of::<DiagnosticRecord>() as u128
                    );
                    assert_eq!(
                        p.reservation.metadata_bytes as u128,
                        p.reservation.total_bytes as u128 - backing
                    );
                    c.root_limit = p.reservation.total_bytes;
                    let mut owner = StorageOwner::prepare(&c).unwrap();
                    owner.try_destroy().unwrap();
                }
            }
        }
    }
}
