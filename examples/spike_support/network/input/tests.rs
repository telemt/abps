use super::super::scripted_io::{ScriptedIo, Step};
use super::*;
use std::future::Future;
fn fixture() -> Fixture {
    Fixture::new(
        if cfg!(feature = "mosaic-spike") {
            "mosaic"
        } else {
            "berry"
        },
        vec![ClassConfig {
            capacity: 64,
            slots: 1,
        }],
        1,
        None,
    )
    .unwrap()
}
#[tokio::test]
async fn ap12_staged_split_headers_payload_skip_and_prefetch_keep_framing() {
    for keyed in [false, true] {
        let mut wire = Vec::new();
        for id in 0..3 {
            let h = Header {
                len: 7,
                id,
                due: 99,
                sequence: keyed.then_some(id),
                keyframe: id == 0,
            };
            wire.extend_from_slice(&h.wire()[..h.wire_len()]);
            wire.extend_from_slice(&[id as u8; 7]);
        }
        for capacity in [1, 2, 19, 20, 31, 32, 63, 128, 1500] {
            let f = fixture();
            let origin = f.open(64).unwrap();
            let reader = ScriptedIo::new(
                &wire,
                std::iter::repeat_n(Step::Data(usize::MAX), wire.len() + 1),
            );
            let mut input = Input::new(reader, vec![0; capacity].into_boxed_slice(), true);
            for id in 0..3 {
                let h = Header::read_mode(&mut input, keyed).await.unwrap();
                assert_eq!((h.id, h.len, h.sequence), (id, 7, keyed.then_some(id)));
                if id == 1 {
                    input.discard(h.len).await.unwrap();
                    continue;
                }
                let mut lease = origin.acquire(h.len).unwrap();
                input.fill(&mut lease, h.len, false).await.unwrap();
                assert_eq!(lease.freeze().as_ref(), &[id as u8; 7]);
            }
            assert_eq!(input.socket.consumed, wire.len());
            drop(origin);
            f.finish().unwrap();
        }
    }
}
#[tokio::test]
async fn ap12_staged_eof_and_cancellation_commit_only_initialized_prefix() {
    for cancel in [false, true] {
        let f = fixture();
        let origin = f.open(64).unwrap();
        let reader = ScriptedIo::new(
            b"abc",
            [Step::Data(3), if cancel { Step::Stall } else { Step::Zero }],
        );
        let mut input = Input::new(reader, vec![0xa5; 8].into_boxed_slice(), true);
        let mut lease = origin.acquire(8).unwrap();
        {
            let future = input.fill(&mut lease, 8, false);
            tokio::pin!(future);
            if cancel {
                assert!(
                    std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx)))
                        .await
                        .is_pending()
                );
            } else {
                assert_eq!(
                    future.await.unwrap_err().kind(),
                    std::io::ErrorKind::UnexpectedEof
                );
            }
        }
        assert_eq!(lease.freeze().as_ref(), b"abc");
        assert_eq!(f.snapshot()["admitted"], 0);
        let next = origin.acquire(8).unwrap().freeze();
        assert!(next.as_ref().is_empty());
        drop(next);
        drop(origin);
        f.finish().unwrap();
    }
}
#[tokio::test]
async fn ap12_staged_coalesces_ready_reads_without_extra_storage() {
    let f = fixture();
    let origin = f.open(64).unwrap();
    let mut wire = Vec::new();
    for id in 0..3 {
        wire.extend_from_slice(
            &Header {
                len: 4,
                id,
                ..Header::default()
            }
            .encode(),
        );
        wire.extend_from_slice(b"abcd");
    }
    let mut counts = Vec::new();
    for staged in [false, true] {
        let reader = ScriptedIo::new(&wire, std::iter::repeat_n(Step::Data(usize::MAX), 6));
        let mut input = Input::new(reader, vec![0; 128].into_boxed_slice(), staged);
        for _ in 0..3 {
            let h = Header::read(&mut input).await.unwrap();
            let mut lease = origin.acquire(h.len).unwrap();
            input.fill(&mut lease, h.len, false).await.unwrap();
            assert_eq!(lease.freeze().as_ref(), b"abcd");
        }
        counts.push(input.socket.calls);
    }
    assert_eq!(counts, vec![6, 1]);
    drop(origin);
    f.finish().unwrap();
}
#[tokio::test]
async fn ap12_staged_discard_eof_and_invalid_keyed_header_stop_without_admission() {
    let mut input = Input::new(&b"abc"[..], vec![0; 8].into_boxed_slice(), true);
    assert_eq!(
        input.discard(4).await.unwrap_err().kind(),
        std::io::ErrorKind::UnexpectedEof
    );
    let mut wire = Header {
        len: 4,
        sequence: Some(0),
        ..Header::default()
    }
    .wire();
    wire[31] = 1;
    let mut input = Input::new(&wire[..], vec![0; 64].into_boxed_slice(), true);
    assert!(Header::read_mode(&mut input, true).await.is_err());
}
