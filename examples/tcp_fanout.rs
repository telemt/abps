#![forbid(unsafe_code)]

mod support;

use support::{Experiment, Overflow};

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut experiment = Experiment::default();
    for argument in std::env::args().skip(1) {
        match argument.as_str() {
            "--overflow=disconnect" => experiment.overflow = Overflow::Disconnect,
            "--overflow=skip" => experiment.overflow = Overflow::Skip,
            _ => {
                return Err(format!(
                    "unknown argument {argument}; use --overflow=disconnect or --overflow=skip"
                )
                .into());
            }
        }
    }
    let report = support::run(experiment).await?;
    println!("Reference experiment (mutex/wrapper allocations; ACK-paced, not a benchmark)");
    println!(
        "overflow={:?} planned_frames={} offered_frames={} read_frames={} enqueued_deliveries={} delivered={} undelivered_enqueued={} rejected_full={} skipped={} disconnected={}",
        experiment.overflow,
        report.planned_frames,
        report.offered_frames,
        report.read_frames,
        report.enqueued,
        report.delivered,
        report.undelivered_enqueued,
        report.rejected_full,
        report.skipped,
        report.disconnected
    );
    println!("subscriber_0_frame_ids={:?}", report.delivered_ids.first());
    println!(
        "ingress_failure={:?} domain_reused={} headroom_error={:?} destroyed={}",
        report.ingress_failure, report.domain_reused, report.headroom_error, report.destroyed
    );
    println!(
        "final_ledgers: reservation={:?} classes={:?} admitted_bytes={} read_rights={} free_domains={} retired_domains={}",
        report.drained.reservation,
        report.drained.classes,
        report
            .drained
            .domains
            .iter()
            .map(|d| d.admitted_bytes)
            .sum::<usize>(),
        report
            .drained
            .slots
            .iter()
            .map(|s| s.references)
            .sum::<usize>(),
        report.drained.free_domain_records,
        report.drained.retired_domain_records,
    );
    if report.ingress_failure.is_some() {
        return Err("ingress terminated; see distinct failure above".into());
    }
    Ok(())
}
