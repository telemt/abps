use abps_reference::Error;
use hdrhistogram::Histogram;
use serde_json::{Value, json};
use std::time::Duration;

pub const ERRORS: [Error; 22] = [
    Error::InvalidConfiguration,
    Error::LayoutOverflow,
    Error::RootBudgetExceeded,
    Error::AllocationFailed,
    Error::InvalidSize,
    Error::TooLarge,
    Error::BudgetExceeded,
    Error::ClassDepleted,
    Error::DomainClassDepleted,
    Error::ReservationUnavailable,
    Error::Contended,
    Error::Closed,
    Error::ReferenceLimit,
    Error::OutputNotEmpty,
    Error::BranchCapacityExceeded,
    Error::DomainCapacityExhausted,
    Error::DomainMaintenanceRequired,
    Error::InvalidRange,
    Error::NotInitialized,
    Error::Busy,
    Error::AlreadyActive,
    Error::Destroyed,
];
pub struct Latency {
    histogram: Histogram<u64>,
    overflow: u64,
}
impl Default for Latency {
    fn default() -> Self {
        // Fixed capacity: no histogram resizing or allocation during recording.
        let mut histogram = Histogram::new_with_bounds(1, 120_000_000_000, 3).unwrap();
        histogram.auto(false);
        Self {
            histogram,
            overflow: 0,
        }
    }
}
impl Latency {
    pub fn record(&mut self, duration: Duration) {
        let ns = duration.as_nanos().min(u64::MAX as u128) as u64;
        if self.histogram.record(ns.max(1)).is_err() {
            self.overflow += 1;
        }
    }
    pub fn merge(&mut self, other: &Self) {
        self.histogram.add(&other.histogram).unwrap();
        self.overflow += other.overflow;
    }
    pub fn json(&self) -> Value {
        let n = self.histogram.len();
        if n == 0 {
            return json!({"samples": 0, "out_of_range": self.overflow});
        }
        json!({
            "samples": n, "out_of_range": self.overflow,
            "p50_ns": self.histogram.value_at_quantile(0.50),
            "p95_ns": self.histogram.value_at_quantile(0.95),
            "p99_ns": self.histogram.value_at_quantile(0.99),
            "p99_9_ns": self.histogram.value_at_quantile(0.999),
            "p99_99_ns": if n >= 1_000_000 { Some(self.histogram.value_at_quantile(0.9999)) } else { None },
            "max_ns": self.histogram.max(),
            "p99_99_status": if n >= 1_000_000 { "reported" } else { "insufficient_samples" },
        })
    }
}
#[derive(Default)]
pub struct Counts {
    pub attempted: u64,
    pub accepted: u64,
    pub bytes: u64,
    pub errors: [u64; ERRORS.len()],
    pub success: Latency,
    pub failure: Latency,
}
impl Counts {
    pub fn result(
        &mut self,
        result: std::result::Result<(), Error>,
        bytes: usize,
        elapsed: Option<Duration>,
    ) {
        self.attempted += 1;
        match result {
            Ok(()) => {
                self.accepted += 1;
                self.bytes += bytes as u64;
                if let Some(elapsed) = elapsed {
                    self.success.record(elapsed);
                }
            }
            Err(error) => {
                self.errors[ERRORS
                    .iter()
                    .position(|e| *e == error)
                    .expect("every Error has a metric bucket")] += 1;
                if let Some(elapsed) = elapsed {
                    self.failure.record(elapsed);
                }
            }
        }
    }
    pub fn merge(&mut self, other: Self) {
        self.attempted += other.attempted;
        self.accepted += other.accepted;
        self.bytes += other.bytes;
        for (a, b) in self.errors.iter_mut().zip(other.errors) {
            *a += b;
        }
        self.success.merge(&other.success);
        self.failure.merge(&other.failure);
    }
    pub fn json(&self, elapsed: Duration) -> Value {
        let errors: serde_json::Map<_, _> = self
            .errors
            .iter()
            .enumerate()
            .filter(|(_, n)| **n != 0)
            .map(|(i, n)| (format!("{:?}", ERRORS[i]), json!(n)))
            .collect();
        json!({
            "elapsed_s": elapsed.as_secs_f64(), "attempted": self.attempted,
            "accepted": self.accepted, "errors": errors, "useful_bytes": self.bytes,
            "accepted_per_s": self.accepted as f64 / elapsed.as_secs_f64(),
            "attempted_per_s": self.attempted as f64 / elapsed.as_secs_f64(),
            "useful_bytes_per_s": self.bytes as f64 / elapsed.as_secs_f64(),
            "success_latency": self.success.json(), "failure_latency": self.failure.json(),
        })
    }
}
