#![no_main]
#![forbid(unsafe_code)]
#[path = "../../tests/support/backends.rs"]
mod backends;
#[path = "../../tests/support/trace.rs"]
mod trace;
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let t = trace::Trace::decode(data, false);
    let expected = backends::reference::oracle::replay(&t);
    assert_eq!(backends::berry::oracle::replay(&t), expected);
    assert_eq!(backends::mosaic::oracle::replay(&t), expected);
});
