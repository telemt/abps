//! AP-09 / IO-01/02: negative controls use the real consumer and scripted I/O.
use super::scripted_io::{ScriptedIo, Step};
use super::*;

fn validation() -> Validation {
    Validation {
        settings: Settings::new(&Args(std::collections::BTreeMap::from([
            ("fanout".into(), "1".into()),
            ("streams".into(), "2".into()),
        ])))
        .unwrap(),
        rate: 1000,
        planned: 100,
        skew: false,
    }
}

#[test]
fn abps_consumer_rejects_extreme_identity_before_schedule_conversion() {
    let mut v = validation();
    v.rate = 1;
    let header = Header {
        id: u64::MAX,
        len: 1,
        ..Header::default()
    };
    assert_eq!(v.header_error(header, 0, None), Some("invalid_identity"));
}

fn frame(v: &Validation, id: u64) -> Vec<u8> {
    let len = v
        .settings
        .payload_size(0, loadgen::sequence_before(id, 0, 2, false));
    let mut wire = Header {
        len,
        id,
        due: Duration::from_secs_f64(id as f64 / v.rate as f64).as_nanos() as u64,
        ..Header::default()
    }
    .encode()
    .to_vec();
    wire.resize(HEADER + len, 0);
    payload::fill_payload(&mut wire[HEADER..], v.settings.seed, 0, id);
    wire
}
async fn read(v: Validation, reader: impl tokio::io::AsyncRead + Unpin) -> Consumer {
    let scratch = vec![0; v.settings.max_payload()];
    consume(
        reader,
        Consumer::new(0, 0),
        Instant::now(),
        tokio::time::Instant::now() + Duration::from_millis(20),
        scratch,
        Duration::ZERO,
        v,
    )
    .await
}

#[tokio::test]
async fn abps_scripted_consumer_corruption_identity_and_skip_gaps() {
    let v = validation();
    let wire = frame(&v, 0);
    for i in HEADER..wire.len() {
        let mut bad = wire.clone();
        bad[i] ^= 1;
        let stats = read(v.clone(), &bad[..]).await;
        assert_eq!(
            (stats.delivered, stats.invalid, stats.terminal),
            (0, 1, "invalid_payload"),
            "offset {i}"
        );
    }
    for (offset, bytes, reason) in [
        (0, 0u32.to_be_bytes().to_vec(), "invalid_length"),
        (0, u32::MAX.to_be_bytes().to_vec(), "invalid_length"),
        (4, 1u64.to_be_bytes().to_vec(), "invalid_identity"),
        (4, 100u64.to_be_bytes().to_vec(), "invalid_identity"),
        (12, 1u64.to_be_bytes().to_vec(), "invalid_timestamp"),
    ] {
        let mut bad = wire.clone();
        bad[offset..offset + bytes.len()].copy_from_slice(&bytes);
        let stats = read(v.clone(), &bad[..]).await;
        assert_eq!(
            (stats.delivered, stats.invalid, stats.terminal),
            (0, 1, reason)
        );
    }
    let mut skipped = wire.clone();
    skipped.extend(frame(&v, 8));
    let stats = read(v.clone(), &skipped[..]).await;
    assert_eq!(
        (stats.delivered, stats.invalid, stats.terminal),
        (2, 0, "clean_eof")
    );
    skipped.extend(frame(&v, 8));
    let stats = read(v, &skipped[..]).await;
    assert_eq!(
        (stats.delivered, stats.invalid, stats.terminal),
        (2, 1, "invalid_order")
    );
    assert_eq!(stats.latency.json()["samples"], 2);
}

#[tokio::test]
async fn abps_scripted_consumer_partial_reads_and_terminal_outcomes() {
    let v = validation();
    let wire = frame(&v, 0);
    for end in 0..wire.len() {
        let stats = read(v.clone(), &wire[..end]).await;
        let reason = if end == 0 {
            "clean_eof"
        } else if end < HEADER {
            "truncated_header"
        } else {
            "truncated_payload"
        };
        assert_eq!(
            (stats.delivered, stats.invalid, stats.terminal),
            (0, 0, reason)
        );
        assert_eq!(
            stats.partial_bytes,
            if end < HEADER { end } else { end - HEADER }
        );
    }
    let mut reader = ScriptedIo::new(
        &wire,
        [
            Step::Data(3),
            Step::Pending,
            Step::Data(17),
            Step::Pending,
            Step::Data(wire.len()),
            Step::Zero,
        ],
    );
    let stats = read(v.clone(), &mut reader).await;
    assert_eq!((stats.delivered, stats.invalid), (1, 0));
    for (step, reason) in [
        (Step::Stall, "read_deadline"),
        (
            Step::Error(std::io::ErrorKind::ConnectionReset),
            "connection_reset",
        ),
        (Step::Error(std::io::ErrorKind::Other), "read_error"),
    ] {
        let mut bytes = wire.clone();
        bytes.extend(frame(&v, 2));
        let reader = ScriptedIo::new(
            &bytes,
            [
                Step::Data(HEADER),
                Step::Data(wire.len() - HEADER),
                Step::Data(HEADER),
                Step::Data(3),
                step,
            ],
        );
        let stats = read(v.clone(), reader).await;
        assert_eq!(
            (
                stats.delivered,
                stats.invalid,
                stats.partial_bytes,
                stats.terminal
            ),
            (1, 0, 3, reason)
        );
    }
}
