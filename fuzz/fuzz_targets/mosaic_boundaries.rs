#![no_main]
#![forbid(unsafe_code)]
#[path = "../../tests/support/backends.rs"]
#[allow(dead_code)]
mod backends;
#[path = "../../tests/support/trace.rs"]
mod trace;
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let t = trace::Trace::decode(data, true);
    assert_eq!(
        backends::mosaic::oracle::replay(&t),
        backends::reference::oracle::replay(&t)
    );
});
