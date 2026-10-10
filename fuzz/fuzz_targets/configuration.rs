#![no_main]
#![forbid(unsafe_code)]
#[path="../../tests/support/configuration.rs"]
mod configuration;
libfuzzer_sys::fuzz_target!(|data:&[u8]| configuration::replay(data));
