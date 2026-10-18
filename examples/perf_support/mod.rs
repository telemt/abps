//! Measurement-only adapters. They make no production allocator promises.
pub mod backend;
pub mod core;
pub mod metrics;
pub mod payload;
pub mod tcp;

use std::collections::BTreeMap;
use std::time::Duration;

pub type AnyError = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, AnyError>;

#[derive(Clone, Debug)]
pub struct Args(pub BTreeMap<String, String>);

impl Args {
    pub fn parse() -> Result<Self> {
        let mut values = BTreeMap::new();
        for arg in std::env::args().skip(1) {
            let (key, value) = arg
                .strip_prefix("--")
                .and_then(|s| s.split_once('='))
                .ok_or("arguments must be --key=value; see docs/performance.md")?;
            if values.insert(key.to_string(), value.to_string()).is_some() {
                return Err(format!("duplicate argument: {key}").into());
            }
        }
        Ok(Self(values))
    }
    pub fn get<'a>(&'a self, key: &str, default: &'a str) -> &'a str {
        self.0.get(key).map(String::as_str).unwrap_or(default)
    }
    pub fn n(&self, key: &str, default: usize) -> usize {
        self.0
            .get(key)
            .map(|s| s.parse().expect("nonnegative integer argument"))
            .unwrap_or(default)
    }
    pub fn seconds(&self, key: &str, default: f64) -> Duration {
        Duration::from_secs_f64(
            self.0
                .get(key)
                .map(|s| s.parse().expect("seconds argument"))
                .unwrap_or(default),
        )
    }
    pub fn validate(&self) -> Result<()> {
        const KEYS: &[&str] = &[
            "case",
            "op",
            "backend",
            "size",
            "seconds",
            "warmup",
            "threads",
            "roots",
            "classes",
            "records",
            "observe-hz",
            "latency",
            "fanout",
            "rate",
            "distribution",
            "slow",
            "overflow",
            "iterations",
        ];
        for key in self.0.keys() {
            if !KEYS.contains(&key.as_str()) {
                return Err(format!("unknown argument: {key}").into());
            }
        }
        Ok(())
    }
}
