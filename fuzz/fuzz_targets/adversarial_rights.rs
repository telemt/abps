#![no_main]
#![forbid(unsafe_code)]
#[path = "../../tests/support/rights.rs"]
mod rights;
#[path = "../../tests/support/rights_trace.rs"]
mod rights_trace;
#[path = "../../tests/support/rights_backends.rs"]
mod backends;

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    let seed=input.iter().fold(0xabf50021u64,|seed,b|seed.rotate_left(5)^u64::from(*b));
    let (trace,_)=rights_trace::generated(seed,input,1024);
    // Native replay emits a versioned concrete JSON reproduction and shrinks it
    // without rebinding IDs. The fuzzer retains its original byte artifact too.
    assert_eq!(backends::berry::replay(&trace),backends::mosaic::replay(&trace));
});
