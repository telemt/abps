use super::*;
use crate::{ClassConfig, mosaic::LocalBranchConfig};

fn config() -> Config {
    let mut c = Config::new(
        vec![ClassConfig {
            capacity: 16,
            slots: 2,
        }],
        2,
        usize::MAX,
    );
    c.local_branches = Some(LocalBranchConfig {
        max_branches_per_slot: 3,
        local_reference_limit: 3,
    });
    c
}
fn fixture() -> (StorageOwner, Pool, DomainOwner, Domain) {
    let owner = StorageOwner::prepare(&config()).unwrap();
    let pool = owner.activate().unwrap();
    let (a, d) = pool.try_open_domain(32).unwrap();
    (owner, pool, a, d)
}
fn finish(mut o: StorageOwner, p: Pool, a: DomainOwner, d: Domain) {
    assert_eq!(d.metrics().admitted_bytes, 0);
    assert_eq!(p.snapshot().classes[0].observed_available_slots, 2);
    drop((d, a, p));
    o.try_destroy().unwrap();
}
fn filled(d: &Domain) -> BufMut {
    let mut b = d.try_acquire(4).unwrap();
    b.put_slice(b"abcd").unwrap();
    b
}

#[test]
fn br02_checked_cell_transition_and_limit() {
    // Test the actual counter without live handles, so a deliberately broken
    // increment cannot cause a second destructor panic during negative controls.
    let r = protocol::LocalReferences::new();
    r.begin();
    assert!(r.unique());
    r.try_clone(2).unwrap();
    assert!(!r.unique());
    assert_eq!(r.try_clone(2), Err(Error::ReferenceLimit));
    assert!(!r.release());
    assert!(r.unique());
    assert!(r.release());
}

#[test]
fn br04_prepared_records_are_charged_and_layout_checked() {
    assert_eq!(size_of::<LocalBuf>(), 3 * size_of::<usize>());
    assert_eq!(size_of::<BranchRecord>(), 64);
    assert_eq!(align_of::<BranchRecord>(), 64);
    let mut c = config();
    let with = Reservation::for_config(&c).unwrap();
    c.local_branches = None;
    let without = Reservation::for_config(&c).unwrap();
    assert_eq!(with.backing_bytes, without.backing_bytes);
    assert_eq!(with.metadata_bytes - without.metadata_bytes, 2 * 3 * 64);
    let mut c = config();
    c.root_limit = with.total_bytes - 1;
    assert_eq!(
        StorageOwner::prepare(&c).err(),
        Some(Error::RootBudgetExceeded)
    );
    for (b, l) in [(0, 1), (1, 0), (1, usize::MAX), (usize::MAX, 1)] {
        let mut c = config();
        c.local_branches = Some(crate::mosaic::LocalBranchConfig {
            max_branches_per_slot: b,
            local_reference_limit: l,
        });
        assert_eq!(
            Reservation::for_config(&c).err(),
            Some(Error::InvalidConfiguration)
        );
    }
    let mut c = config();
    c.local_branches.as_mut().unwrap().max_branches_per_slot = isize::MAX as usize;
    assert_eq!(
        Reservation::for_config(&c).err(),
        Some(Error::LayoutOverflow)
    );
}

#[test]
fn br01_errors_restore_source_and_destination_with_fixed_precedence() {
    let (o, p, a, d) = fixture();
    let mut out = [None, None, None, None];
    let err = filled(&d).freeze_branches_into(&mut out).unwrap_err();
    assert_eq!(err.reason(), Error::BranchCapacityExceeded);
    assert!(out.iter().all(Option::is_none));
    let (_, mut source) = err.into_parts();
    assert_eq!(source.as_ref(), b"abcd");
    source.put_slice(b"e").unwrap();
    source.freeze_branches_into(&mut out[..1]).unwrap();
    let before = out[0].as_ref().unwrap().as_ref().as_ptr();
    let err = filled(&d).freeze_branches_into(&mut out).unwrap_err();
    assert_eq!(err.reason(), Error::OutputNotEmpty);
    assert_eq!(out[0].as_ref().unwrap().as_ref().as_ptr(), before);
    assert_eq!(out[0].as_ref().unwrap().as_ref(), b"abcde");
    let (_, source) = err.into_parts();
    source.freeze_branches_into(&mut []).unwrap();
    drop(out);
    finish(o, p, a, d);

    let mut c = config();
    c.reference_limit = 1;
    let mut o = StorageOwner::prepare(&c).unwrap();
    let p = o.activate().unwrap();
    let (a, d) = p.try_open_domain(32).unwrap();
    let err = filled(&d)
        .freeze_branches_into(&mut [None, None])
        .unwrap_err();
    assert_eq!(err.reason(), Error::ReferenceLimit);
    drop(err);
    drop((a, d, p));
    o.try_destroy().unwrap();

    c.local_branches = None;
    let mut o = StorageOwner::prepare(&c).unwrap();
    let p = o.activate().unwrap();
    let (a, d) = p.try_open_domain(32).unwrap();
    let err = filled(&d).freeze_branches_into(&mut [None]).unwrap_err();
    assert_eq!(err.reason(), Error::BranchCapacityExceeded);
    err.into_parts().1.freeze_branches_into(&mut []).unwrap();
    drop((a, d, p));
    o.try_destroy().unwrap();
}

#[test]
fn br02_local_views_preserve_global_rights_ranges_and_unique_migration() {
    fn send<T: Send>() {}
    send::<Branch>();
    let (o, p, a, d) = fixture();
    let mut out = [None, None];
    filled(&d).freeze_branches_into(&mut out).unwrap();
    let mut local = out[0].take().unwrap().into_local();
    let x = local.try_slice(1..4).unwrap();
    let y = x.try_slice(1..2).unwrap();
    assert_eq!(y.as_ref(), b"c");
    assert_eq!(local.try_clone().err(), Some(Error::ReferenceLimit));
    assert_eq!(
        local.try_slice(0..usize::MAX).err(),
        Some(Error::InvalidRange)
    );
    assert_eq!(
        p.snapshot()
            .slots
            .iter()
            .map(|s| s.references)
            .sum::<usize>(),
        2
    );
    assert_eq!(d.metrics().admitted_bytes, 16);
    local = local.try_into_branch().unwrap_err();
    drop((x, y));
    let unique = local.try_into_branch().unwrap();
    let returned = std::thread::spawn(move || {
        let local = unique.into_local();
        assert_eq!(local.try_view(1..3).unwrap().as_ref(), b"bc");
        let nested = local.try_slice(2..4).unwrap();
        drop(local);
        nested.try_into_branch().unwrap()
    })
    .join()
    .unwrap();
    assert_eq!(returned.as_ref(), b"cd");
    std::thread::spawn(move || drop(returned)).join().unwrap();
    assert_eq!(d.metrics().admitted_bytes, 16);
    drop(out);
    finish(o, p, a, d);
}

#[test]
fn br05_borrowed_io_views_keep_origin_and_filled_bounds_through_reuse() {
    // BR-03/05, MEM-03, OWN-01: the immutable sidecar descriptor survives
    // allocation reuse, but each family must read its current origin and range.
    // Borrowed I/O descriptors consume no local/global reference rights.
    let mut c = config();
    c.classes[0].slots = 1;
    c.domain_slots = 1;
    let mut owner = StorageOwner::prepare(&c).unwrap();
    let pool = owner.activate().unwrap();
    let mut previous = None;
    for value in 1..=8 {
        let (authority, domain) = pool.try_open_domain(16).unwrap();
        let identity = domain.identity();
        assert_ne!(previous, Some(identity));
        let mut lease = domain.try_acquire(4).unwrap();
        lease.put_slice(&[value; 4]).unwrap();
        lease.initialize_to(16).unwrap(); // initialized tail is not payload
        let mut out = [None];
        lease.freeze_branches_into(&mut out).unwrap();
        let branch = out[0].take().unwrap();
        authority.close();
        assert_eq!(branch.origin(), identity);
        assert_eq!(branch.backing_capacity(), 16);
        assert_eq!(branch.len(), 4);
        assert!(!branch.is_empty());
        assert_eq!(branch.as_io_slice().as_ref(), &[value; 4]);
        assert_eq!(branch.try_view(1..3).unwrap().as_ref(), &[value; 2]);
        assert_eq!(branch.try_view(0..5).err(), Some(Error::InvalidRange));
        assert_eq!(pool.snapshot().slots[0].references, 1);
        let local = branch.into_local();
        let empty = local.try_slice(4..4).unwrap();
        assert!(empty.is_empty());
        assert_eq!(empty.origin(), identity);
        assert_eq!(empty.backing_capacity(), 16);
        assert!(empty.as_io_slice().is_empty());
        assert_eq!(local.as_io_slice().as_ref(), &[value; 4]);
        assert_eq!(local.try_view(0..16).err(), Some(Error::InvalidRange));
        drop(local);
        assert_eq!(domain.metrics().admitted_bytes, 16);
        drop((domain, authority));
        pool.poll_domains(1);
        assert_eq!(
            pool.try_open_domain(16).err(),
            Some(Error::DomainMaintenanceRequired)
        );
        drop(empty);
        pool.poll_domains(1);
        previous = Some(identity);
    }
    drop(pool);
    owner.try_destroy().unwrap();
}

#[test]
fn br01_unwind_at_every_materialization_prefix_releases_all_rights() {
    let (o, p, a, d) = fixture();
    for prefix in 0..=3 {
        let mut out = [None, None, None];
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut guard = BranchBuild::new(filled(&d), &mut out);
            guard.reserve();
            for _ in 0..prefix {
                guard.materialize();
            }
            panic!("injected construction unwind");
        }));
        assert!(result.is_err());
        assert!(out.iter().all(Option::is_none));
        assert_eq!(d.metrics().admitted_bytes, 0);
        filled(&d).freeze_branches_into(&mut out).unwrap();
        drop(out);
    }
    finish(o, p, a, d);
}

#[test]
fn br01_source_reserved_staged_and_installed_boundaries_conserve_rights() {
    // BR-01/03, SI-11: inject cancellation/unwind only at complete production
    // transitions. The scalar-only transfer intervals have no injection callback.
    for reserved in [false, true] {
        let mut c = config();
        c.classes[0].slots = 1;
        c.domain_slots = 1;
        if reserved {
            c.capacity_policy = CapacityPolicy::Reserved;
        }
        let mut owner = StorageOwner::prepare(&c).unwrap();
        let pool = owner.activate().unwrap();
        let open = || {
            if reserved {
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
        };
        for count in 1..=3 {
            for stop in 0..(2 + 2 * count) {
                for unwind in [false, true] {
                    let (authority, domain) = open().unwrap();
                    let old_origin = domain.identity();
                    let mut output: Vec<Option<Branch>> = (0..count).map(|_| None).collect();
                    let source = filled(&domain);
                    let slot = source.lease.slot;
                    let controls = pool.root.root().controls.get();
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let mut build = BranchBuild::new(source, &mut output);
                        if stop > 0 {
                            build.reserve();
                        }
                        for index in 0..count {
                            if stop >= 2 + 2 * index {
                                build.stage();
                            }
                            if stop >= 3 + 2 * index {
                                build.install();
                            }
                        }
                        let installed = build.dst.iter().flatten().count();
                        let staged = usize::from(build.staged.is_some());
                        assert_eq!(installed, build.built);
                        assert_eq!(
                            build.root.slot(slot).references.load(),
                            build.pending + staged + installed
                        );
                        assert_eq!(
                            build.pending + staged + installed,
                            if stop == 0 { 1 } else { count }
                        );
                        for branch in build.dst.iter().flatten().chain(build.staged.iter()) {
                            assert_eq!(branch.as_ref(), b"abcd");
                            assert_eq!(branch.origin(), old_origin);
                            assert!(branch.inner.right.record().references.unique());
                        }
                        assert_eq!(domain.metrics().admitted_bytes, 16);
                        assert_eq!(pool.snapshot().classes[0].observed_available_slots, 0);
                        if unwind {
                            panic!("BR-01 injected unwind at {stop}/{count}");
                        }
                        // A normal early return cancels the same guarded state.
                    }));
                    assert_eq!(result.is_err(), unwind);
                    assert!(output.iter().all(Option::is_none));
                    assert_eq!(pool.root.slot(slot).references.load(), 0);
                    assert_eq!(domain.metrics().admitted_bytes, 0);
                    assert_eq!(pool.root.root().controls.get(), controls);
                    assert_eq!(pool.snapshot().classes[0].observed_available_slots, 1);
                    drop((authority, domain));
                    let (authority, domain) = open().unwrap();
                    assert_eq!(domain.identity().index, old_origin.index);
                    assert!(domain.identity().generation > old_origin.generation);
                    let mut reused = domain.try_acquire(1).unwrap();
                    assert_eq!(reused.lease.slot, slot);
                    assert_eq!(reused.lease.slot().references.load(), 1);
                    assert!(reused.as_ref().is_empty());
                    assert_eq!(reused.initialized_len(), 0);
                    reused.put_slice(b"new").unwrap();
                    reused.freeze_branches_into(&mut output).unwrap();
                    for branch in output.iter().flatten() {
                        assert_eq!(branch.as_ref(), b"new");
                        assert_eq!(branch.origin(), domain.identity());
                    }
                    drop(output);
                    assert_eq!(domain.metrics().admitted_bytes, 0);
                    drop((authority, domain));
                }
            }
        }
        drop(pool);
        owner.try_destroy().unwrap();
    }
}

#[test]
fn br03_stopped_local_release_and_reuse_before_refund_pin_old_origin() {
    let (mut o, p, a, d) = fixture();
    let (b, e) = p.try_open_domain(32).unwrap();
    let mut out = [None];
    filled(&d).freeze_branches_into(&mut out).unwrap();
    let local = std::mem::ManuallyDrop::new(out[0].take().unwrap().into_local());
    let root = local.right.record().root;
    let slot = local.right.record().slot;
    assert!(local.right.record().references.release()); // interrupted just before shared decrement
    assert_eq!(root.slot(slot).references.load(), 1);
    assert_eq!(d.metrics().admitted_bytes, 16);
    let lease = std::mem::ManuallyDrop::new(Lease { root, slot });
    let pending = lease.begin_return().unwrap().publish();
    assert_eq!(d.metrics().admitted_bytes, 16);
    let mut next = [None, None];
    filled(&e).freeze_branches_into(&mut next).unwrap();
    assert_eq!(next[0].as_ref().unwrap().origin(), e.identity());
    drop((a, d, b, e, p));
    assert_eq!(o.try_destroy(), Err(Error::Busy));
    pending.finish(); // must never touch the reused local record or global refcount
    let x = next[0].take().unwrap().into_local();
    let view = x.try_clone().unwrap();
    drop((x, next));
    assert_eq!(view.as_ref(), b"abcd");
    assert_eq!(o.try_destroy(), Err(Error::Busy));
    drop(view);
    o.try_destroy().unwrap();
}

#[test]
fn br05_forget_pins_full_charge_and_owner_drop_abandons() {
    let (mut o, p, a, d) = fixture();
    let diagnostics = o.diagnostics().unwrap();
    let mut out = [None];
    filled(&d).freeze_branches_into(&mut out).unwrap();
    let local = out[0].take().unwrap().into_local();
    std::mem::forget(local.try_slice(0..0).unwrap());
    drop(local);
    assert_eq!(d.metrics().admitted_bytes, 16);
    drop((a, d, p));
    assert_eq!(o.try_destroy(), Err(Error::Busy));
    drop(o);
    assert_eq!(diagnostics.snapshot().abps_abandoned_roots, 1);
}

#[test]
fn br03_remote_families_allow_teardown_before_worker_join() {
    let (mut o, p, a, d) = fixture();
    let mut out = [None, None, None];
    filled(&d).freeze_branches_into(&mut out).unwrap();
    drop((d, a, p));
    let threads: Vec<_> = out
        .into_iter()
        .map(|b| {
            std::thread::spawn(move || {
                let local = b.unwrap().into_local();
                let other = local.try_slice(1..3).unwrap();
                drop(local);
                assert_eq!(other.as_ref(), b"bc");
                drop(other);
            })
        })
        .collect();
    while o.try_destroy() == Err(Error::Busy) {
        std::thread::yield_now();
    }
    for t in threads {
        t.join().unwrap();
    }
}

#[test]
fn br05_close_and_early_owner_drop_preserve_readers_and_neighbor_slot() {
    let (o, p, a, d) = fixture();
    let diagnostics = o.diagnostics().unwrap();
    let mut out = [None, None];
    filled(&d).freeze_branches_into(&mut out).unwrap();
    let mut neighbor = filled(&d);
    let bytes = neighbor.initialize_to(16).unwrap();
    bytes.fill(77);
    neighbor.set_filled_len(16).unwrap();
    a.close();
    drop((a, d, p, o));
    let local = out[0].take().unwrap().into_local();
    let child = local.try_clone().unwrap();
    drop(local);
    assert_eq!(child.as_ref(), b"abcd");
    assert!(neighbor.as_ref().iter().all(|b| *b == 77));
    drop((child, out, neighbor));
    assert_eq!(diagnostics.snapshot().abps_abandoned_roots, 1);
    assert!(!diagnostics.snapshot().destroyed);
}

#[test]
fn branch_record_incarnations_one_two_one_preserve_lifetime_and_initialization() {
    let mut c = config();
    c.classes[0].slots = 1;
    let mut owner = StorageOwner::prepare(&c).unwrap();
    let pool = owner.activate().unwrap();
    let (authority, domain) = pool.try_open_domain(16).unwrap();
    for (generation, families) in [1, 2, 1, 3, 2, 1].into_iter().enumerate() {
        let mut buffer = domain.try_acquire(8).unwrap();
        assert_eq!(buffer.initialized_len(), 0);
        assert_eq!(buffer.filled_len(), 0);
        assert!(buffer.as_ref().is_empty());
        assert_eq!(buffer.set_filled_len(1), Err(Error::NotInitialized));
        let bytes = [generation as u8; 8];
        buffer.put_slice(&bytes).unwrap();
        let mut out: Vec<_> = (0..families).map(|_| None).collect();
        buffer.freeze_branches_into(&mut out).unwrap();
        for (i, branch) in out.iter_mut().enumerate() {
            let local = branch.take().unwrap().into_local();
            let view = local.try_slice(1..7).unwrap();
            drop(local);
            assert_eq!(view.as_ref(), &bytes[1..7]);
            assert_eq!(domain.try_acquire(8).err(), Some(Error::BudgetExceeded));
            assert_eq!(pool.snapshot().slots[0].references, families - i);
            drop(view);
        }
        assert_eq!(domain.metrics().admitted_bytes, 0);
        assert_eq!(pool.snapshot().classes[0].observed_available_slots, 1);
    }
    drop((authority, domain, pool));
    owner.try_destroy().unwrap();
}
