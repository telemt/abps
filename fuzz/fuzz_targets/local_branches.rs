#![no_main]
#![forbid(unsafe_code)]
#[path = "../../tests/support/branch_trace.rs"]
mod branch_trace;
libfuzzer_sys::fuzz_target!(|data: &[u8]| branch_trace::replay(data));
