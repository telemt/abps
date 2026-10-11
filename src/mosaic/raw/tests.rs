use super::*;
use crate::ClassConfig;
mod conservation {
    use super::*;
    fn stages() -> &'static [Stage] {
        &[Stage::Charged, Stage::Bit, Stage::Origin, Stage::Ready]
    }
    fn control_lifetime(p: &Pool) -> usize {
        p.root.root().controls.get()
    }
    include!("../../test_support/issuer_rights.rs");
}
mod partitions;

fn config(slots: usize, records: usize) -> Config {
    Config::new(
        vec![ClassConfig {
            capacity: 16,
            slots,
        }],
        records,
        usize::MAX,
    )
}
fn fixture(slots: usize) -> (StorageOwner, Pool, DomainOwner, Domain) {
    let owner = StorageOwner::prepare(&config(slots, 2)).unwrap();
    let pool = owner.activate().unwrap();
    let (a, d) = pool.try_open_domain(16 * (slots + 1)).unwrap();
    (owner, pool, a, d)
}
fn available(pool: &Pool) -> usize {
    pool.snapshot()
        .classes
        .iter()
        .map(|c| c.observed_available_slots)
        .sum()
}
fn finish(mut owner: StorageOwner, pool: Pool, a: DomainOwner, d: Domain) {
    assert_eq!(d.metrics().admitted_bytes, 0);
    assert_eq!(available(&pool), pool.metrics().total_slots);
    drop((d, a, pool));
    owner.try_destroy().unwrap();
}

#[test]
fn si12_cursor_retains_winning_word_then_wraps_without_losing_capacity() {
    // SI-12 deliberately reuses the winning word. No per-word fairness promise;
    // both direct words and sparse grant nodes must wrap when that word empties.
    let word = usize::BITS as usize;
    for reserved in [false, true] {
        let slots = 2 * word + 1;
        let mut c = config(slots, 1);
        if reserved {
            c.capacity_policy = CapacityPolicy::Reserved;
        }
        let mut owner = StorageOwner::prepare(&c).unwrap();
        let pool = owner.activate().unwrap();
        let (a, d) = if reserved {
            pool.try_open_reserved_domain(
                16 * (slots + 1),
                &[ClassReservation {
                    capacity: 16,
                    slots,
                }],
            )
        } else {
            pool.try_open_domain(16 * (slots + 1))
        }
        .unwrap();
        let mut held: Vec<_> = (0..=word).map(|_| d.try_acquire(1).unwrap()).collect();
        assert_eq!(held.last().unwrap().lease.slot, word);
        drop(held.pop());
        drop(held.remove(0)); // free capacity in both word 0 and the winning word 1
        for _ in 0..8 {
            let b = d.try_acquire(1).unwrap();
            assert_eq!(b.lease.slot, word, "SI-12 keeps the winning word");
            drop(b);
        }
        let mut seen: std::collections::BTreeSet<_> = held.iter().map(|b| b.lease.slot).collect();
        for _ in held.len()..slots {
            let b = d.try_acquire(1).unwrap();
            assert!(seen.insert(b.lease.slot), "one exclusive owner per slot");
            held.push(b);
        }
        assert_eq!(seen, (0..slots).collect());
        assert_eq!(
            d.try_acquire(1).err(),
            Some(if reserved {
                Error::DomainClassDepleted
            } else {
                Error::ClassDepleted
            })
        );
        drop(held);
        assert_eq!(d.metrics().admitted_bytes, 0);
        drop((a, d, pool));
        owner.try_destroy().unwrap();
    }
}
#[test]
fn si12_word_boundaries_exact_exhaustion_and_no_padding_slot() {
    let w = usize::BITS as usize;
    let counts = if cfg!(miri) {
        vec![1, w - 1, w, w + 1, 2 * w - 1, 2 * w, 2 * w + 1]
    } else {
        vec![
            1,
            w - 1,
            w,
            w + 1,
            2 * w - 1,
            2 * w,
            2 * w + 1,
            10272,
            16384,
        ]
    };
    for slots in counts {
        let (owner, pool, a, d) = fixture(slots);
        let mut held: Vec<_> = (0..slots).map(|_| d.try_acquire(1).unwrap()).collect();
        assert_eq!(d.try_acquire(1).err(), Some(Error::ClassDepleted));
        assert_eq!(d.metrics().admitted_bytes, 16 * slots);
        assert_eq!(available(&pool), 0);
        drop(held.pop());
        held.push(d.try_acquire(1).unwrap());
        assert_eq!(d.try_acquire(0).err(), Some(Error::InvalidSize));
        assert_eq!(d.try_acquire(usize::MAX).err(), Some(Error::TooLarge));
        drop(held);
        finish(owner, pool, a, d);
    }
}
#[test]
fn si11_rollback_stages_conserve_all_ledgers() {
    for reserved in [false, true] {
        let mut c = config(65, 1);
        if reserved {
            c.capacity_policy = CapacityPolicy::Reserved;
        }
        let mut owner = StorageOwner::prepare(&c).unwrap();
        let pool = owner.activate().unwrap();
        let (a, d) = if reserved {
            pool.try_open_reserved_domain(
                1040,
                &[ClassReservation {
                    capacity: 16,
                    slots: 65,
                }],
            )
        } else {
            pool.try_open_domain(1040)
        }
        .unwrap();
        for stage in [Stage::Charged, Stage::Bit, Stage::Ready] {
            assert_eq!(
                d.acquire(1, Some(Fault::Error(stage))).err(),
                Some(Error::Contended)
            );
            assert_eq!(d.metrics().admitted_bytes, 0);
            assert_eq!(available(&pool), 65);
            drop(d.try_acquire(1).unwrap());
        }
        drop((a, d, pool));
        owner.try_destroy().unwrap();
    }
}
#[test]
fn si11_reissue_before_refund_cannot_credit_new_origin_or_destroy_root() {
    let (mut owner, pool, a, d) = fixture(1);
    let (b, e) = pool.try_open_domain(16).unwrap();
    let old = d.try_acquire(1).unwrap().freeze();
    let old = std::mem::ManuallyDrop::new(old);
    let return_right = old.lease.begin_return().unwrap();
    assert_eq!(available(&pool), 0);
    let pending = return_right.publish();
    assert_eq!(available(&pool), 1);
    assert_eq!(d.metrics().admitted_bytes, 16);
    let next = e.try_acquire(1).unwrap();
    assert_eq!(next.lease.origin(), e.identity());
    drop((a, d, b, e, pool));
    assert_eq!(owner.try_destroy(), Err(Error::Busy));
    pending.finish();
    assert_eq!(owner.try_destroy(), Err(Error::Busy));
    assert_eq!(
        next.lease
            .root
            .ledger(next.lease.origin().index)
            .admitted
            .load(Ordering::Acquire),
        16
    );
    drop(next);
    owner.try_destroy().unwrap();
}
#[test]
fn si11_teardown_after_refund_without_join_synchronization() {
    let (mut owner, pool, a, d) = fixture(2);
    let diagnostic = owner.diagnostics().unwrap();
    let buffers: Vec<_> = (0..2).map(|_| d.try_acquire(1).unwrap()).collect();
    drop((d, a, pool));
    let workers: Vec<_> = buffers
        .into_iter()
        .map(|b| std::thread::spawn(move || drop(b)))
        .collect();
    while owner.try_destroy() == Err(Error::Busy) {
        std::thread::yield_now();
    }
    assert!(diagnostic.snapshot().destroyed);
    for w in workers {
        w.join().unwrap();
    }
}
#[test]
fn si11_final_refund_races_teardown_across_many_schedules() {
    // SI-11/AP-02, LIFE-02: the owner may free the arena once it acquires a
    // zero charge, while the returner is still inside its final refund. Miri
    // reports a protected borrow of non-atomic storage across that refund only
    // in schedules preempting the returner there (about 1% per race), so one
    // seed runs hundreds of races. A persistent returner keeps races cheap and
    // the busy owner loop keeps observation and deallocation in one slice.
    // The channel orders only the handoff, never the final refund.
    let rounds = if cfg!(miri) { 256 } else { 512 };
    let (tx, rx) = std::sync::mpsc::channel::<Buf>();
    let returner = std::thread::spawn(move || rx.into_iter().for_each(drop));
    for _ in 0..rounds {
        let mut owner = StorageOwner::prepare(&config(1, 1)).unwrap();
        let pool = owner.activate().unwrap();
        let (a, d) = pool.try_open_domain(16).unwrap();
        let frame = d.try_acquire(1).unwrap().freeze();
        drop((d, a, pool));
        tx.send(frame).unwrap();
        loop {
            match owner.try_destroy() {
                Ok(()) => break,
                Err(Error::Busy) if cfg!(miri) => {}
                Err(Error::Busy) => std::thread::yield_now(),
                result => panic!("unexpected destroy: {result:?}"),
            }
        }
    }
    drop(tx);
    returner.join().unwrap();
}
#[test]
fn si11_controls_are_separate_from_allocation_and_ref_limits() {
    let mut c = config(2, 1);
    c.control_handle_limit = 4;
    c.reference_limit = 2;
    let owner = StorageOwner::prepare(&c).unwrap();
    let pool = owner.activate().unwrap();
    let (a, d) = pool.try_open_domain(32).unwrap();
    assert_eq!(d.try_clone().err(), Some(Error::ReferenceLimit));
    let x = d.try_acquire(1).unwrap().freeze();
    let y = x.try_clone().unwrap();
    drop(d.try_acquire(1).unwrap());
    assert_eq!(x.try_clone().err(), Some(Error::ReferenceLimit));
    assert_eq!(pool.metrics().control_handles, 4);
    drop((x, y));
    finish(owner, pool, a, d);
}
#[test]
fn si13_fragmented_grants_recycle_only_after_pending_refund() {
    let mut c = config(130, 3);
    c.capacity_policy = CapacityPolicy::Reserved;
    let mut owner = StorageOwner::prepare(&c).unwrap();
    let pool = owner.activate().unwrap();
    let grants = |slots| {
        [ClassReservation {
            capacity: 16,
            slots,
        }]
    };
    let (a, d) = pool.try_open_reserved_domain(4096, &grants(65)).unwrap();
    let (b, e) = pool.try_open_reserved_domain(4096, &grants(65)).unwrap();
    assert_eq!(
        pool.try_open_reserved_domain(4096, &grants(1)).err(),
        Some(Error::ReservationUnavailable)
    );
    let buffers: Vec<_> = (0..65).map(|_| d.try_acquire(1).unwrap()).collect();
    assert_eq!(d.try_acquire(1).err(), Some(Error::DomainClassDepleted));
    drop(buffers);
    let frame = std::mem::ManuallyDrop::new(e.try_acquire(1).unwrap().freeze());
    let pending = frame.lease.begin_return().unwrap().publish();
    drop((b, e));
    assert_eq!(pool.poll_domains(32).reclaimed, 0);
    assert_eq!(
        pool.try_open_reserved_domain(4096, &grants(1)).err(),
        Some(Error::ReservationUnavailable)
    );
    pending.finish();
    assert_eq!(pool.poll_domains(32).reclaimed, 1);
    let (f, g) = pool.try_open_reserved_domain(4096, &grants(65)).unwrap();
    let buffers: Vec<_> = (0..65).map(|_| g.try_acquire(1).unwrap()).collect();
    drop(buffers);
    drop((f, g, a, d));
    assert_eq!(pool.root.root().free_nodes.get(), 130);
    assert_eq!(pool.root.root().free_rows.get(), 3);
    drop(pool);
    owner.try_destroy().unwrap();
}
#[test]
fn si13_sparse_metadata_bound_and_transactional_open() {
    let mut c = config(2, 65536);
    c.capacity_policy = CapacityPolicy::Reserved;
    let plan = Plan::new(&c).unwrap();
    assert_eq!(plan.row_count, 2);
    assert_eq!(plan.node_count, 2);
    c.domain_slots = 4;
    let mut owner = StorageOwner::prepare(&c).unwrap();
    let pool = owner.activate().unwrap();
    let empty = pool.try_open_reserved_domain(100, &[]).unwrap();
    assert_eq!(
        empty.1.try_acquire(1).err(),
        Some(Error::DomainClassDepleted)
    );
    assert_eq!(pool.root.root().free_rows.get(), 2);
    let before = pool.metrics().control_handles;
    for g in [
        vec![ClassReservation {
            capacity: 17,
            slots: 1,
        }],
        vec![ClassReservation {
            capacity: 16,
            slots: 0,
        }],
    ] {
        assert_eq!(
            pool.try_open_reserved_domain(100, &g).err(),
            Some(Error::InvalidConfiguration)
        );
    }
    assert_eq!(
        pool.try_open_reserved_domain(
            100,
            &[ClassReservation {
                capacity: 16,
                slots: 3
            }]
        )
        .err(),
        Some(Error::ReservationUnavailable)
    );
    assert_eq!(pool.metrics().control_handles, before);
    let x = pool
        .try_open_reserved_domain(
            100,
            &[ClassReservation {
                capacity: 16,
                slots: 1,
            }],
        )
        .unwrap();
    let y = pool
        .try_open_reserved_domain(
            100,
            &[ClassReservation {
                capacity: 16,
                slots: 1,
            }],
        )
        .unwrap();
    drop((empty, x, y));
    assert_eq!(pool.root.root().free_nodes.get(), 2);
    drop(pool);
    owner.try_destroy().unwrap();
}
#[test]
fn si14_borrow_initialization_neighbor_bounds_and_batch_unwind() {
    let (owner, pool, a, d) = fixture(3);
    let mut x = d.try_acquire(16).unwrap();
    x.put_slice(b"abcdefghijklmnop").unwrap();
    let x = x.freeze();
    let borrowed = x.try_view(1..4).unwrap();
    let mut y = d.try_acquire(16).unwrap();
    assert!(y.as_ref().is_empty());
    assert_eq!(y.set_filled_len(1), Err(Error::NotInitialized));
    y.initialize_to(16).unwrap().fill(7);
    y.set_filled_len(16).unwrap();
    assert_eq!(borrowed.as_ref(), b"bcd");
    assert_eq!(borrowed.as_io_slice().as_ref(), b"bcd");
    assert_eq!(borrowed.try_as_io_slice().unwrap().as_ref(), b"bcd");
    assert_eq!(x.try_as_io_slice().unwrap().as_ref(), x.as_ref());
    assert_eq!(x.try_view(0..17).err(), Some(Error::InvalidRange));
    assert_eq!(x.lease.slot().references.load(), 1);
    for k in [0, 1, 20] {
        let mut dst: Vec<_> = (0..k).map(|_| None).collect();
        x.try_clone_into(&mut dst).unwrap();
        assert_eq!(x.lease.slot().references.load(), k + 1);
        drop(dst);
    }
    let mut dst = [None, None, None];
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut batch = BatchBuild::new(&x, &mut dst).unwrap();
        batch.materialize();
        panic!("injected batch unwind");
    }));
    assert!(result.is_err());
    assert!(dst.iter().all(Option::is_none));
    assert_eq!(x.lease.slot().references.load(), 1);
    dst[2] = Some(x.try_clone().unwrap());
    assert_eq!(x.try_clone_into(&mut dst), Err(Error::OutputNotEmpty));
    drop((dst, x, y));
    finish(owner, pool, a, d);
}
#[test]
fn si14_pages_include_free_records_without_maintenance() {
    let (owner, pool, a, d) = fixture(65);
    let mut page = [DomainMetrics::default(); 4];
    let p = pool.domain_metrics_page(0, &mut page).unwrap();
    assert_eq!(p.written, 2);
    let mut words = [BitmapMetrics::default(); 1];
    assert!(!pool.bitmap_metrics_page(0, &mut words).unwrap().done);
    assert_eq!(words[0].available, 64);
    assert!(pool.bitmap_metrics_page(1, &mut words).unwrap().done);
    assert_eq!(words[0].available, 1);
    assert_eq!(
        pool.bitmap_metrics_page(3, &mut words),
        Err(Error::InvalidRange)
    );
    finish(owner, pool, a, d);
}
#[test]
fn life02_root_identity_is_opaque_and_survives_storage_reuse() {
    // LIFE-02: diagnostics outlive their owner, so a destroyed root's identity
    // must not reappear when the allocator reuses its arena for a new root.
    let first = StorageOwner::prepare(&config(1, 1)).unwrap();
    let observer = first.diagnostics().unwrap();
    let address = first.root.unwrap().0.as_ptr().addr();
    drop(first);
    let second = StorageOwner::prepare(&config(1, 1)).unwrap();
    let (old, new) = (
        observer.snapshot(),
        second.diagnostics().unwrap().snapshot(),
    );
    assert!(old.destroyed);
    assert!(old.root_identity != 0 && new.root_identity > old.root_identity);
    assert_ne!(old.root_identity, address, "no heap address is exported");
}
#[test]
fn si11_forget_and_early_owner_drop_are_observable_not_reclamation() {
    let (owner, pool, a, d) = fixture(1);
    let diagnostic = owner.diagnostics().unwrap();
    let frame = d.try_acquire(1).unwrap().freeze();
    std::mem::forget(frame);
    a.close();
    assert_eq!(d.try_acquire(1).err(), Some(Error::Closed));
    drop((a, d));
    assert_eq!(pool.poll_domains(32).reclaimed, 0);
    drop(owner);
    let s = diagnostic.snapshot();
    assert_eq!(s.abps_abandoned_roots, 1);
    assert_eq!(s.admitted_bytes, 16);
    assert!(!s.destroyed);
    assert_eq!(available(&pool), 0);
    drop(pool);
}
#[test]
fn si11_generation_retires_without_reassigning_old_identity() {
    let mut owner = StorageOwner::prepare(&config(1, 1)).unwrap();
    let pool = owner.activate().unwrap();
    let record = pool.root.domain(0);
    let mut state = record.state.get();
    state.generation = u64::MAX - 1;
    record.state.set(state);
    let (a, d) = pool.try_open_domain(16).unwrap();
    drop(d.try_acquire(1).unwrap());
    drop((a, d));
    assert_eq!(pool.snapshot().retired_domain_records, 1);
    assert_eq!(
        pool.try_open_domain(16).err(),
        Some(Error::DomainCapacityExhausted)
    );
    drop(pool);
    owner.try_destroy().unwrap();
}
#[test]
fn si12_search_work_bound_includes_confirmation() {
    for count in [1, 2, 161, 256] {
        let words: Vec<_> = (0..count)
            .map(|_| {
                let w = protocol::Bitmap::new(1);
                w.claim(1);
                w
            })
            .collect();
        let mut probes = Vec::new();
        assert_eq!(
            protocol::take_words(
                count,
                count - 1,
                |i| {
                    probes.push(i);
                    (&words[i], 1)
                },
                Error::ClassDepleted
            ),
            Err(Error::ClassDepleted)
        );
        assert_eq!(
            probes,
            (0..count * 2)
                .map(|n| (count - 1 + n) % count)
                .collect::<Vec<_>>()
        );
        words[0].publish(1);
        assert_eq!(
            protocol::take_words(count, count - 1, |i| (&words[i], 1), Error::ClassDepleted),
            Ok((0, 1))
        );
    }
}

#[test]
fn abps_search_success_and_masked_failure_have_bounded_work() {
    // SI-12/AP-08: probe counts cover cyclic success, empty grants and the
    // confirming pass. Timing cannot establish this algorithmic bound.
    for count in if cfg!(miri) {
        vec![1, 2, 3]
    } else {
        vec![1, 2, 3, 161, 256]
    } {
        for start in [0, count / 2, count - 1] {
            for offset in [0, count / 2, count - 1] {
                let selected = (start + offset) % count;
                let words: Vec<_> = (0..count)
                    .map(|i| {
                        let word = protocol::Bitmap::new(1);
                        if i != selected {
                            word.claim(1);
                        }
                        word
                    })
                    .collect();
                let mut probes = Vec::new();
                assert_eq!(
                    protocol::take_words(
                        count,
                        start,
                        |i| {
                            probes.push(i);
                            (&words[i], 1)
                        },
                        Error::ClassDepleted
                    ),
                    Ok((selected, 1))
                );
                assert_eq!(probes.len(), offset + 1, "SI-12 first-pass success work");
                assert_eq!(
                    probes,
                    (0..=offset)
                        .map(|n| (start + n) % count)
                        .collect::<Vec<_>>()
                );
                let mut probes = 0;
                assert_eq!(
                    protocol::take_words(
                        count,
                        start,
                        |i| {
                            probes += 1;
                            (&words[i], 0)
                        },
                        Error::DomainClassDepleted
                    ),
                    Err(Error::DomainClassDepleted)
                );
                assert_eq!(probes, 2 * count, "SI-12 bounded empty-mask confirmation");
            }
        }
    }
}
#[test]
fn si15_large_layout_is_checked_without_backing_allocation() {
    let mut c = Config::new(
        vec![
            ClassConfig {
                capacity: 65536,
                slots: 10272,
            },
            ClassConfig {
                capacity: 2097152,
                slots: 10272,
            },
        ],
        32,
        usize::MAX,
    );
    c.capacity_policy = CapacityPolicy::Reserved;
    if usize::BITS == 64 {
        let p = Plan::new(&c).unwrap();
        assert_eq!(p.word_count, 322);
        assert_eq!(p.reservation.backing_bytes as u64 * 2, 44_430_262_272);
    }
    c.max_bitmap_words_per_class = 1;
    assert_eq!(
        Reservation::for_config(&c),
        Err(Error::InvalidConfiguration)
    );
    c.max_bitmap_words_per_class = 257;
    assert_eq!(
        Reservation::for_config(&c),
        Err(Error::InvalidConfiguration)
    );
    c.max_bitmap_words_per_class = 256;
    c.domain_slots = usize::MAX;
    assert_eq!(Reservation::for_config(&c), Err(Error::LayoutOverflow));
}
#[cfg(feature = "bytes-interop")]
#[test]
fn si14_bytes_cursor_preserves_tail_and_initialization() {
    use bytes::BufMut as _;
    let (owner, pool, a, d) = fixture(2);
    let mut x = d.try_acquire(16).unwrap();
    bytes::BufMut::put_slice(&mut x, b"packet");
    assert_eq!(x.as_ref(), b"packet");
    assert_eq!(x.remaining_mut(), 10);
    let x = x.freeze();
    assert_eq!(x.try_view(0..6).unwrap().as_ref(), b"packet");
    drop(x);
    finish(owner, pool, a, d);
}

#[test]
fn si17_zero_payload_checked_ranges_and_fresh_lease_state() {
    let word = std::mem::size_of::<usize>();
    for offset in 0..word {
        for len in 0..3 * word + 1 {
            let mut bytes = vec![0xa5; 6 * word];
            // SAFETY SI-17: a unique initialized test allocation; the prepared
            // subrange fits. Guards detect prefix/tail writes outside the range.
            unsafe {
                prepare_payload(bytes.as_mut_ptr().add(offset), len);
            }
            assert!(bytes[..offset].iter().all(|&b| b == 0xa5));
            assert!(bytes[offset..offset + len].iter().all(|&b| b == 0));
            assert!(bytes[offset + len..].iter().all(|&b| b == 0xa5));
        }
    }
    let mut c = Config::new(
        vec![
            ClassConfig {
                capacity: 3,
                slots: 3,
            },
            ClassConfig {
                capacity: 19,
                slots: 5,
            },
        ],
        2,
        usize::MAX,
    );
    let lazy = Reservation::for_config(&c).unwrap();
    c.preparation = super::super::PreparationPolicy::ZeroPayload;
    assert_eq!(Reservation::for_config(&c).unwrap(), lazy);
    let mut owner = StorageOwner::prepare(&c).unwrap();
    let ptr = owner.root.unwrap();
    let plan = ptr.root().plan;
    for (i, class) in c.classes.iter().enumerate() {
        // SAFETY SI-17: ZeroPayload initialized this exact checked payload range;
        // no handles or mutable payload references have been issued yet.
        let bytes = unsafe {
            std::slice::from_raw_parts(
                ptr.0.as_ptr().cast::<u8>().add(plan.payload[i]),
                class.capacity * class.slots,
            )
        };
        assert!(bytes.iter().all(|&b| b == 0));
    }
    let pool = owner.activate().unwrap();
    let (a, d) = pool.try_open_domain(200).unwrap();
    let mut b = d.try_acquire(19).unwrap();
    assert_eq!((b.initialized_len(), b.filled_len()), (0, 0));
    assert_eq!(b.set_filled_len(1), Err(Error::NotInitialized));
    b.put_slice(&[7; 19]).unwrap();
    drop(b);
    let b = d.try_acquire(19).unwrap();
    assert_eq!((b.initialized_len(), b.filled_len()), (0, 0));
    drop((b, d, a, pool));
    owner.try_destroy().unwrap();
}

#[test]
fn si16_maintenance_is_bounded_fair_and_not_a_snapshot_side_effect() {
    let mut owner = StorageOwner::prepare(&config(2, 3)).unwrap();
    let pool = owner.activate().unwrap();
    let (ao, a) = pool.try_open_domain(16).unwrap();
    let (bo, b) = pool.try_open_domain(16).unwrap();
    let (co, c) = pool.try_open_domain(16).unwrap();
    let stuck = a.try_acquire(16).unwrap();
    let remote = b.try_acquire(16).unwrap();
    let old = b.identity();
    drop((ao, a, bo, b));
    assert_eq!(pool.poll_domains(0).examined, 0);
    std::thread::spawn(move || drop(remote)).join().unwrap();
    assert_eq!(pool.snapshot().pending_domain_records, 2);
    assert!(matches!(
        pool.try_open_domain(16),
        Err(Error::DomainMaintenanceRequired)
    ));
    let first = pool.poll_domains(1);
    assert_eq!((first.examined, first.reclaimed, first.pending), (1, 0, 2));
    let second = pool.poll_domains(1);
    assert_eq!(
        (second.examined, second.reclaimed, second.pending),
        (1, 1, 1)
    );
    let (no, n) = pool.try_open_domain(16).unwrap();
    assert_ne!(n.identity(), old);
    assert_eq!(n.identity().index, old.index);
    assert_eq!(pool.poll_domains(32).examined, 1);
    drop((no, n, co, c, stuck, pool));
    owner.try_destroy().unwrap();
}

// AP-02/03/05: execute the production transition one phase at a time. These
// samples describe publication, not an invented partition of pending phases.
#[test]
fn ap05_zero_references_and_published_pending_refund_are_honest_samples() {
    let (owner, pool, a, d) = fixture(1);
    let frame = std::mem::ManuallyDrop::new(d.try_acquire(1).unwrap().freeze());
    let ticket = frame.lease.begin_return().unwrap();
    let sample = pool.snapshot();
    assert_eq!(sample.slots[0].references, 0);
    assert_eq!(sample.classes[0].observed_available_slots, 0);
    assert_eq!(sample.classes[0].observed_unpublished_slots, 1);
    assert_eq!(
        sample.domains[d.identity().index].observed_unpublished_backing_bytes,
        16
    );
    assert_eq!(
        sample.domains[d.identity().index].observed_admitted_bytes,
        16
    );
    let pending = ticket.publish();
    let sample = pool.snapshot();
    assert_eq!(sample.classes[0].observed_available_slots, 1);
    assert_eq!(sample.classes[0].observed_unpublished_slots, 0);
    assert_eq!(
        sample.domains[d.identity().index].observed_unpublished_backing_bytes,
        0
    );
    assert_eq!(
        sample.domains[d.identity().index].observed_admitted_bytes,
        16
    );
    pending.finish();
    finish(owner, pool, a, d);
}

#[test]
fn ap01_remote_authority_publish_reuse_refund_preserves_new_metadata() {
    // AP-01/02/03/05: the actual remote Lease path, paused at both return
    // boundaries. Channels control the accounting schedule; visibility ordering
    // itself is tested separately without observer synchronization by Loom.
    assert_eq!(
        std::mem::size_of::<RemotePtr>(),
        std::mem::size_of::<RootPtr>()
    );
    assert_eq!(
        std::mem::size_of::<Lease>(),
        2 * std::mem::size_of::<usize>()
    );
    let (mut owner, pool, a, d) = fixture(1);
    let diagnostic = owner.diagnostics().unwrap();
    let (b, e) = pool.try_open_domain(16).unwrap();
    let old_key = d.identity();
    let new_key = e.identity();
    let old = d.try_acquire(1).unwrap();
    let (stage_tx, stage_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let mut old = old;
        assert_eq!(old.origin(), old_key);
        assert_eq!(old.capacity(), 16);
        old.initialize_to(8).unwrap().copy_from_slice(b"abcdefgh");
        old.set_filled_len(8).unwrap();
        let old = old.freeze();
        let mut batch = [None, None];
        old.try_clone_into(&mut batch).unwrap();
        let slice = old.try_slice(1..7).unwrap();
        assert_eq!(slice.try_view(1..3).unwrap().as_ref(), b"cd");
        assert_eq!(slice.origin(), old_key);
        assert_eq!(slice.backing_capacity(), 16);
        drop((slice, batch));
        let old = std::mem::ManuallyDrop::new(old);
        let ticket = old.lease.begin_return().unwrap();
        stage_tx.send(()).unwrap(); // refs=0, old bit still unpublished
        resume_rx.recv().unwrap();
        let pending = ticket.publish();
        stage_tx.send(()).unwrap(); // old positive charge still protects ledger
        resume_rx.recv().unwrap();
        pending.finish(); // LAST arena access on this thread
        stage_tx.send(()).unwrap();
    });
    a.close();
    drop((a, d));
    let wait = || {
        stage_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap()
    };
    wait();
    let s = pool.snapshot();
    assert_eq!(s.slots[0].references, 0);
    assert_eq!(s.slots[0].origin, Some(old_key));
    assert_eq!(s.classes[0].observed_unpublished_slots, 1);
    assert_eq!(s.domains[old_key.index].observed_admitted_bytes, 16);
    assert_eq!(pool.poll_domains(1).reclaimed, 0);
    resume_tx.send(()).unwrap();
    wait();
    assert_eq!(available(&pool), 1);
    assert_eq!(pool.poll_domains(1).reclaimed, 0);
    let mut fresh = e.try_acquire(1).unwrap();
    assert_eq!(fresh.origin(), new_key);
    assert_eq!((fresh.initialized_len(), fresh.filled_len()), (0, 0));
    assert!(fresh.as_ref().is_empty());
    assert_eq!(fresh.initialize_to(4).unwrap(), &[0; 4]);
    fresh.put_slice(b"WXYZ").unwrap();
    let fresh = fresh.freeze();
    let peer = fresh.try_clone().unwrap();
    let slice = fresh.try_slice(1..3).unwrap();
    let check_new = || {
        let s = pool.snapshot();
        assert_eq!(s.slots[0].origin, Some(new_key));
        assert_eq!(s.slots[0].references, 3);
        assert_eq!(s.slots[0].ownership, OwnershipKind::Immutable);
        assert_eq!(fresh.as_ref(), b"WXYZ");
        assert_eq!(slice.as_ref(), b"XY");
        assert_eq!(e.metrics().admitted_bytes, 16);
    };
    check_new();
    resume_tx.send(()).unwrap();
    wait();
    check_new(); // BEFORE join: old refund cannot alter new slot metadata
    assert_eq!(pool.poll_domains(1).reclaimed, 1);
    let (_, reopened) = pool.try_open_domain(16).unwrap();
    assert_eq!(reopened.identity().index, old_key.index);
    assert!(reopened.identity().generation > old_key.generation);
    drop((reopened, b, e, pool));
    assert_eq!(owner.try_destroy(), Err(Error::Busy));
    drop((fresh, peer, slice));
    owner.try_destroy().unwrap();
    assert_eq!(owner.snapshot().err(), Some(Error::Destroyed));
    assert!(diagnostic.snapshot().destroyed);
    worker.join().unwrap(); // no arena access follows final refund on worker
}

#[test]
fn ap04_owner_drop_races_final_return_without_delegating_teardown() {
    for _ in 0..if cfg!(miri) { 2 } else { 16 } {
        let (owner, pool, a, d) = fixture(1);
        let diagnostics = owner.diagnostics().unwrap();
        let frame = d.try_acquire(1).unwrap().freeze();
        drop((a, d, pool));
        let worker = std::thread::spawn(move || drop(frame));
        drop(owner);
        worker.join().unwrap();
        let sample = diagnostics.snapshot();
        // Either owner observed complete drain, or abandonment is permanent.
        assert_eq!(
            usize::from(sample.destroyed) + sample.abps_abandoned_roots,
            1
        );
        assert_eq!(sample.abps_abandoned_bytes > 0, !sample.destroyed);
    }
}

#[test]
fn mem02_own03_every_guarded_admission_fault_restores_resources() {
    // The actual acquisition runs through every guarded phase, including origin
    // overwrite on the same slot after earlier successful incarnations.
    for reserved in [false, true] {
        let mut c = config(1, 1);
        if reserved {
            c.capacity_policy = CapacityPolicy::Reserved;
        }
        let mut owner = StorageOwner::prepare(&c).unwrap();
        let pool = owner.activate().unwrap();
        let (a, d) = if reserved {
            pool.try_open_reserved_domain(
                16,
                &[ClassReservation {
                    capacity: 16,
                    slots: 1,
                }],
            )
        } else {
            pool.try_open_domain(16)
        }
        .unwrap();
        let reservation = owner.reservation().unwrap();
        for stage in [Stage::Charged, Stage::Bit, Stage::Origin, Stage::Ready] {
            for unwind in [false, true] {
                let mut old = d.try_acquire(16).unwrap();
                old.put_slice(&[0x5a; 16]).unwrap();
                drop(old.freeze());
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    d.acquire(
                        16,
                        Some(if unwind {
                            Fault::Panic(stage)
                        } else {
                            Fault::Error(stage)
                        }),
                    )
                }));
                if unwind {
                    assert!(outcome.is_err(), "{stage:?}");
                } else {
                    assert_eq!(outcome.unwrap().err(), Some(Error::Contended), "{stage:?}");
                }
                assert_eq!(d.metrics().admitted_bytes, 0, "MEM-02 {stage:?}");
                assert_eq!(owner.reservation().unwrap(), reservation, "MEM-01");
                let mut next = d.try_acquire(16).unwrap();
                assert_eq!(
                    (next.filled_len(), next.initialized_len()),
                    (0, 0),
                    "OWN-01"
                );
                assert_eq!(next.initialize_to(16).unwrap(), &[0; 16]);
                next.set_filled_len(16).unwrap();
                drop(next.freeze());
            }
        }
        drop((a, d, pool));
        owner.try_destroy().unwrap(); // LIFE-02: no leaked claim/charge at any phase.
    }
}

mod layouts {
    use super::*;
    fn configure_layout(c: &mut Config) {
        #[cfg(feature = "local-branches-spike")]
        {
            c.local_branches = Some(crate::mosaic::LocalBranchConfig {
                max_branches_per_slot: 2,
                local_reference_limit: 3,
            });
        }
        let _ = c;
    }
    fn additional_ranges(p: &Plan, c: &Config, ranges: &mut Vec<(u128, u128)>) {
        ranges.extend([
            range::<Cell<GrantEntry>>(p.grants, p.row_count * c.classes.len()),
            range::<Cell<usize>>(p.row_ids, p.row_count),
            range::<Cell<MaskNode>>(p.nodes, p.node_count),
            range::<Word>(p.words, p.word_count),
            range::<Cell<usize>>(
                p.unassigned,
                if c.capacity_policy == CapacityPolicy::Reserved {
                    p.word_count
                } else {
                    0
                },
            ),
            range::<Cell<usize>>(p.cursors, c.classes.len()),
        ]);
        #[cfg(feature = "local-branches-spike")]
        ranges.push(range::<branches::BranchRecord>(
            p.branch_records,
            p.slot_count * p.branches_per_slot,
        ));
    }
    include!("../../test_support/layout_ranges.rs");
}
