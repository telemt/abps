#![cfg(feature = "local-branches-spike")]
#![forbid(unsafe_code)]
#[path = "support/branch_trace.rs"]
mod branch_trace;
use proptest::prelude::*;
fn property_config() -> ProptestConfig {
    ProptestConfig {
        cases: if cfg!(miri) {
            3
        } else {
            std::env::var("ABPS_PROPTEST_CASES")
                .map(|v| v.parse().unwrap())
                .unwrap_or(128)
        },
        failure_persistence: if cfg!(miri) {
            None
        } else {
            Some(Box::new(
                proptest::test_runner::FileFailurePersistence::WithSource("proptest-regressions"),
            ))
        },
        ..Default::default()
    }
}
fn max_bytes() -> usize {
    2 + 5 * if cfg!(miri) {
        20
    } else {
        std::env::var("ABPS_TRACE_OPERATIONS")
            .map(|v| v.parse::<usize>().unwrap().clamp(1, 1024))
            .unwrap_or(250)
    }
}
proptest! {
    #![proptest_config(property_config())]
    #[test]
    fn four_ledgers_survive_local_sequences(input in prop::collection::vec(any::<u8>(), 2..max_bytes())) {
        branch_trace::replay(&input);
    }
}
#[test]
fn br_verification_corpus_replays() {
    if let Ok(path) = std::env::var("ABPS_BRANCH_TRACE_FILE") {
        branch_trace::replay(&std::fs::read(path).unwrap());
        return;
    }
    for entry in std::fs::read_dir(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/verification/corpus/branches"
    ))
    .unwrap()
    {
        branch_trace::replay(&std::fs::read(entry.unwrap().path()).unwrap());
    }
}
