//! AP-15 / OWN-01/02 / MEM-02 / LIFE-02: frames borrow, allocations retain rights.
#![forbid(unsafe_code)]
#![cfg(feature = "serial-adapter")]
use abps_reference::{
    Error, mosaic as m,
    serial::{SerialBuffer, SerialFrame, SerialWrite},
};
use std::panic::{AssertUnwindSafe, catch_unwind};

fn fixture(slots: usize) -> (m::StorageOwner, m::Pool, m::DomainOwner, m::Domain) {
    let config = m::Config::new(
        vec![m::ClassConfig {
            capacity: 64,
            slots,
        }],
        1,
        usize::MAX,
    );
    let storage = m::StorageOwner::prepare(&config).unwrap();
    let pool = storage.activate().unwrap();
    let (owner, domain) = pool.try_open_domain(64 * slots).unwrap();
    (storage, pool, owner, domain)
}

#[test]
fn ap15_transfer_and_serial_frames_preserve_origin_and_full_charge() {
    let (mut storage, pool, owner, domain) = fixture(1);
    let mut raw = domain.try_acquire(64).unwrap();
    raw.put_slice(b"existing").unwrap();
    let raw = SerialBuffer::from_buffer(raw).into_buffer();
    assert_eq!(raw.as_ref(), b"existing");
    let origin = raw.origin();
    let mut serial = SerialBuffer::from_buffer(raw);
    for len in [64, 1, 0, 63, 2, 32] {
        let mut w = serial.begin();
        assert_eq!(w.filled_len(), 0);
        w.put_slice(&vec![len as u8; len]).unwrap();
        let frame = w.finish();
        assert_eq!(frame.as_ref(), vec![len as u8; len]);
        assert_eq!(frame.origin(), origin);
        assert_eq!(frame.capacity(), 64);
        assert_eq!(frame.filled_len(), len);
        assert_eq!(frame.initialized_len(), 64);
        assert_eq!(domain.metrics().admitted_bytes, 64);
        assert!(domain.try_acquire(1).is_err());
    }
    assert_eq!(serial.origin(), origin);
    assert_eq!(serial.capacity(), 64);
    let raw = serial.into_buffer();
    assert_eq!(raw.filled_len(), 0);
    assert_eq!(raw.initialized_len(), 64);
    drop(raw);
    assert_eq!(domain.metrics().admitted_bytes, 0);
    let raw = domain.try_acquire(64).unwrap();
    assert_eq!((raw.filled_len(), raw.initialized_len()), (0, 0));
    drop((raw, domain, owner, pool));
    storage.try_destroy().unwrap();
}

#[test]
fn ap15_error_paths_preserve_lengths_and_valid_prefix() {
    let (mut storage, pool, owner, domain) = fixture(1);
    let mut serial = SerialBuffer::from_buffer(domain.try_acquire(64).unwrap());
    let mut w = serial.begin();
    assert_eq!(w.origin(), serial_origin(&domain));
    assert_eq!(w.set_filled_len(1), Err(Error::NotInitialized));
    w.put_slice(b"abc").unwrap();
    assert_eq!(w.put_slice(&[1; 64]), Err(Error::InvalidRange));
    assert_eq!(w.initialize_to(usize::MAX).err(), Some(Error::InvalidRange));
    assert_eq!(w.set_filled_len(usize::MAX), Err(Error::NotInitialized));
    assert_eq!(
        (w.filled_len(), w.initialized_len(), w.capacity()),
        (3, 3, 64)
    );
    assert_eq!(w.finish().as_ref(), b"abc");
    drop((serial, domain, owner, pool));
    storage.try_destroy().unwrap();
}

fn serial_origin(domain: &m::Domain) -> m::DomainIdentity {
    domain.identity()
}

#[test]
fn ap15_initialized_bytes_persist_without_exposing_neighbor() {
    let (mut storage, pool, owner, domain) = fixture(2);
    let mut other = domain.try_acquire(64).unwrap();
    other.put_slice(&[0xcc; 64]).unwrap();
    let other = other.freeze();
    let mut serial = SerialBuffer::from_buffer(domain.try_acquire(64).unwrap());
    for len in 1..=64 {
        let mut w = serial.begin();
        let initialized = w.initialize_to(len).unwrap();
        assert_eq!(initialized[len - 1], 0); // only this newly initialized tail
        initialized.fill(len as u8);
        w.set_filled_len(len).unwrap();
        assert_eq!(w.finish().as_ref(), vec![len as u8; len]);
        assert_eq!(other.as_ref(), &[0xcc; 64]);
    }
    assert_eq!(serial.initialized_len(), 64);
    let mut w = serial.begin();
    assert_eq!(w.initialize_to(64).unwrap(), &[64; 64]);
    assert_eq!(w.filled_len(), 0);
    drop(w);
    drop((serial, other, domain, owner, pool));
    storage.try_destroy().unwrap();
}

#[test]
fn ap15_unwind_and_forgotten_guards_do_not_publish_a_previous_frame() {
    let (mut storage, pool, owner, domain) = fixture(1);
    let mut serial = SerialBuffer::from_buffer(domain.try_acquire(64).unwrap());
    for finish in [false, true] {
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let mut w = serial.begin();
                w.put_slice(b"aborted").unwrap();
                if finish {
                    let _frame = w.finish();
                    panic!("frame interrupted");
                }
                panic!("write interrupted");
            }))
            .is_err()
        );
        // Check Drop itself: a new begin must not hide a failed cleanup.
        let raw = serial.into_buffer();
        assert_eq!(raw.filled_len(), 0);
        serial = SerialBuffer::from_buffer(raw);
        assert!(serial.begin().finish().as_ref().is_empty());
        let mut w = serial.begin();
        w.put_slice(b"forgotten").unwrap();
        if finish {
            std::mem::forget(w.finish());
        } else {
            std::mem::forget(w);
        }
        assert!(serial.begin().finish().as_ref().is_empty());
        assert_eq!(domain.metrics().admitted_bytes, 64);
    }
    drop((serial, domain, owner, pool));
    storage.try_destroy().unwrap();
}

#[test]
fn ap15_close_and_remote_return_keep_teardown_owner_side() {
    let (mut storage, pool, owner, domain) = fixture(1);
    let mut serial = SerialBuffer::from_buffer(domain.try_acquire(64).unwrap());
    drop((domain, owner, pool));
    assert!(storage.try_destroy().is_err());
    serial.begin().put_slice(b"retained rights").unwrap();
    std::thread::spawn(move || {
        assert_eq!(serial.capacity(), 64);
        drop(serial);
    })
    .join()
    .unwrap();
    storage.try_destroy().unwrap();
}

#[test]
fn ap15_exclusive_types_are_send_not_sync_or_clone() {
    macro_rules! not_impl {
        ($ty:ty,$bound:path) => {{
            trait Ambiguous<A> {
                fn witness() {}
            }
            impl<T: ?Sized> Ambiguous<()> for T {}
            impl<T: ?Sized + $bound> Ambiguous<u8> for T {}
            let _ = <$ty as Ambiguous<_>>::witness;
        }};
    }
    fn send<T: Send>() {}
    send::<SerialBuffer>();
    send::<SerialWrite<'static>>();
    send::<SerialFrame<'static>>();
    not_impl!(SerialBuffer, Sync);
    not_impl!(SerialBuffer, Clone);
    not_impl!(SerialWrite<'static>, Sync);
    not_impl!(SerialWrite<'static>, Clone);
    not_impl!(SerialFrame<'static>, Sync);
    not_impl!(SerialFrame<'static>, Clone);
}

#[tokio::test]
async fn ap15_partial_io_eof_and_cancellation_retain_only_the_backing_right() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (mut storage, pool, owner, domain) = fixture(1);
    let mut serial = SerialBuffer::from_buffer(domain.try_acquire(64).unwrap());
    let (mut tx, mut rx) = tokio::io::duplex(2);
    let producer = async {
        for part in [b"ab", b"cd", b"ef"] {
            tx.write_all(part).await.unwrap();
        }
        tx.shutdown().await.unwrap();
    };
    let consumer = async {
        let mut w = serial.begin();
        rx.read_exact(w.initialize_to(6).unwrap()).await.unwrap();
        w.set_filled_len(6).unwrap();
        let frame = w.finish();
        assert_eq!(frame.as_ref(), b"abcdef");
        let (mut out, mut input) = tokio::io::duplex(1);
        let mut bytes = [0; 6];
        let (sent, read) =
            tokio::join!(out.write_all(frame.as_ref()), input.read_exact(&mut bytes));
        sent.unwrap();
        read.unwrap();
        assert_eq!(&bytes, b"abcdef");
    };
    tokio::join!(producer, consumer);
    let mut w = serial.begin();
    assert_eq!(
        rx.read_exact(w.initialize_to(1).unwrap())
            .await
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::UnexpectedEof
    );
    drop(w);
    let (_tx, mut rx) = tokio::io::duplex(1);
    let pending = async {
        let mut w = serial.begin();
        rx.read_exact(w.initialize_to(4).unwrap()).await.unwrap();
        w.set_filled_len(4).unwrap();
    };
    {
        let mut pending = std::pin::pin!(pending);
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(std::future::Future::poll(
                pending.as_mut(),
                cx
            )))
            .await
            .is_pending()
        );
    }
    assert!(serial.begin().finish().as_ref().is_empty());
    assert_eq!(domain.metrics().admitted_bytes, 64);
    drop((serial, domain, owner, pool));
    storage.try_destroy().unwrap();
}

use proptest::prelude::*;
proptest! {
    #![proptest_config(ProptestConfig {
        cases: if cfg!(miri) {8} else {128},
        failure_persistence: if cfg!(miri) {None} else {ProptestConfig::default().failure_persistence},
        ..ProptestConfig::default()
    })]
    #[test]
    fn ap15_model_preserves_bytes_and_one_right(actions in prop::collection::vec((0usize..80, any::<u8>(), any::<bool>()),1..64)) {
        let (mut storage,pool,owner,domain)=fixture(1);
        let mut serial=SerialBuffer::from_buffer(domain.try_acquire(64).unwrap());
        let mut initialized=0;
        for (len,value,finish) in actions {
            let mut w=serial.begin();let bytes=vec![value;len];
            if len<=64 {
                w.put_slice(&bytes).unwrap();initialized=initialized.max(len);
                if finish {let frame=w.finish();prop_assert_eq!(frame.as_ref(),bytes);} else {drop(w);}
            } else {prop_assert_eq!(w.put_slice(&bytes),Err(Error::InvalidRange));drop(w);}
            prop_assert_eq!(serial.initialized_len(),initialized);
            prop_assert_eq!(domain.metrics().admitted_bytes,64);
            prop_assert!(serial.begin().finish().as_ref().is_empty());
        }
        drop(serial);prop_assert_eq!(domain.metrics().admitted_bytes,0);
        drop((domain,owner,pool));storage.try_destroy().unwrap();
    }
}
