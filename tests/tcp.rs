#![forbid(unsafe_code)]

#[path = "../examples/support/mod.rs"]
mod support;

use abps_reference::*;
use support::{Experiment, Failure, Overflow, SUBSCRIBERS};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn experiment(config: Experiment) -> support::Report {
    let report = tokio::time::timeout(std::time::Duration::from_secs(30), support::run(config))
        .await
        .unwrap()
        .unwrap();
    assert!(report.destroyed);
    assert!(report.domain_reused);
    assert_eq!(report.headroom_error, Error::BudgetExceeded);
    assert_eq!(report.drained.free_domain_records, 2);
    assert!(report.drained.domains.iter().all(|d| d.admitted_bytes == 0));
    assert!(
        report
            .drained
            .classes
            .iter()
            .all(|c| c.available == c.total)
    );
    assert_eq!(report.planned_frames, config.frames);
    assert!(report.offered_frames <= config.frames);
    report
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn io03_twenty_subscribers_default_disconnect_contains_slow_writer() {
    let config = Experiment::default();
    let report = experiment(config).await;
    assert_eq!(report.ingress_failure, None);
    assert_eq!(report.disconnected, 1);
    assert_eq!(report.rejected_full, 1);
    assert_eq!(report.skipped, 0);
    assert_eq!(report.read_frames, config.frames);
    assert_eq!(report.offered_frames, config.frames);
    assert_eq!(report.enqueued, (SUBSCRIBERS - 1) * config.frames + 2);
    assert_eq!(report.delivered, (SUBSCRIBERS - 1) * config.frames);
    assert_eq!(report.undelivered_enqueued, 2);
    for ids in &report.delivered_ids[1..] {
        assert_eq!(ids, &(0..config.frames as u64).collect::<Vec<_>>());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn io03_skip_is_explicit_and_preserves_accepted_frames() {
    let config = Experiment {
        overflow: Overflow::Skip,
        ..Experiment::default()
    };
    let report = experiment(config).await;
    assert_eq!(report.ingress_failure, None);
    assert_eq!(report.disconnected, 0);
    assert_eq!(report.rejected_full, 0);
    assert!(report.skipped >= 4);
    assert_eq!(
        report.enqueued + report.skipped,
        SUBSCRIBERS * config.frames
    );
    assert_eq!(report.enqueued, report.delivered);
    assert_eq!(report.undelivered_enqueued, 0);
    assert!(report.delivered_ids[0].starts_with(&[0, 1]));
    assert!(
        report.delivered_ids[0]
            .windows(2)
            .any(|pair| pair[1] > pair[0] + 1)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn own02_view_limit_is_a_separate_failure_before_any_enqueue() {
    let report = experiment(Experiment {
        reference_limit: 4,
        ..Experiment::default()
    })
    .await;
    assert_eq!(
        report.ingress_failure,
        Some(Failure::Views(Error::ReferenceLimit))
    );
    assert_eq!(report.enqueued, 0);
    assert_eq!(report.offered_frames, 1);
    assert_eq!(report.disconnected, 0);
    assert_eq!(report.skipped, 0);
    assert_eq!(report.rejected_full, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn io03_normal_fanout_drains_every_delivery() {
    let config = Experiment {
        slow_subscriber: false,
        ..Experiment::default()
    };
    let report = experiment(config).await;
    assert_eq!(report.ingress_failure, None);
    assert_eq!(report.delivered, SUBSCRIBERS * config.frames);
    assert_eq!(report.disconnected + report.skipped, 0);
}

#[tokio::test]
async fn io02_vectored_short_writes_cross_header_and_payload_boundaries() {
    let (mut writer, mut reader) = tokio::io::duplex(3);
    let send = tokio::spawn(async move {
        support::write_frame(&mut writer, 17, &[23; 127])
            .await
            .unwrap();
        writer.shutdown().await.unwrap();
    });
    assert_eq!(
        support::read_header(&mut reader).await.unwrap(),
        Some((17, 127))
    );
    let mut payload = [0u8; 127];
    reader.read_exact(&mut payload).await.unwrap();
    assert_eq!(payload, [23; 127]);
    assert_eq!(support::read_header(&mut reader).await.unwrap(), None);
    send.await.unwrap();
}

#[tokio::test]
async fn io01_truncated_header_is_not_clean_eof() {
    let (mut writer, mut reader) = tokio::io::duplex(16);
    writer.write_all(&[1, 2]).await.unwrap();
    drop(writer);
    assert_eq!(
        support::read_header(&mut reader).await.unwrap_err().kind(),
        std::io::ErrorKind::UnexpectedEof
    );
}

#[tokio::test]
async fn io02_cancel_pending_write_then_join_before_storage_drain() {
    let mut owner = StorageOwner::prepare(Config::new(
        vec![ClassConfig {
            capacity: 512,
            slots: 1,
        }],
        1,
        1 << 20,
    ))
    .unwrap();
    let pool = owner.activate().unwrap();
    let (authority, domain) = pool.try_open_domain(512).unwrap();
    let mut lease = domain.try_acquire(512).unwrap();
    lease.put_slice(&[42; 512]).unwrap();
    let buffer = lease.freeze();
    let (mut writer, _unread) = tokio::io::duplex(1);
    let (entered, running) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        entered.send(()).unwrap();
        support::write_frame(&mut writer, 0, buffer.as_ref())
            .await
            .unwrap();
    });
    running.await.unwrap();
    assert_eq!(pool.snapshot().classes[0].issued, 1);
    drop((authority, domain, pool));
    assert_eq!(owner.try_destroy(), Err(Error::Busy));
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(owner.snapshot().unwrap().classes[0].available, 1);
    owner.try_destroy().unwrap();
}

#[tokio::test]
async fn io01_cancel_real_tcp_ingress_returns_exclusive_lease() {
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let _silent_peer = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (mut ingress, _) = listener.accept().await.unwrap();
    let mut owner = StorageOwner::prepare(Config::new(
        vec![ClassConfig {
            capacity: 512,
            slots: 1,
        }],
        1,
        1 << 20,
    ))
    .unwrap();
    let pool = owner.activate().unwrap();
    let (authority, domain) = pool.try_open_domain(512).unwrap();
    let mut lease = domain.try_acquire(512).unwrap();
    let (entered, running) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        entered.send(()).unwrap();
        ingress
            .read_exact(lease.initialize_to(512).unwrap())
            .await
            .unwrap();
        lease.set_filled_len(512).unwrap();
    });
    running.await.unwrap();
    assert_eq!(pool.snapshot().domains[0].admitted_bytes, 512);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(pool.snapshot().domains[0].admitted_bytes, 0);
    drop((authority, domain, pool));
    owner.try_destroy().unwrap();
}
