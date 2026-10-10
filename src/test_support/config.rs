//! AP-09: report the compiled synchronization path, not just requested flags.
#[test]
fn reference_acquire_mode_matches_campaign() {
    let mode = if cfg!(abps_tsan) {
        "acquire-load"
    } else {
        "fence"
    };
    let sanitizers = env!("ABPS_TARGET_SANITIZERS");
    let sanitizers = if sanitizers.is_empty() {
        "none"
    } else {
        sanitizers
    };
    if sanitizers.split(',').any(|name| name == "thread") {
        assert_eq!(mode, "acquire-load", "TSan must select Acquire-load");
    }
    if let Ok(expected) = std::env::var("ABPS_EXPECT_REFERENCE_ACQUIRE") {
        assert_eq!(mode, expected);
    }
    if let Ok(expected) = std::env::var("ABPS_EXPECT_SANITIZER") {
        assert_eq!(sanitizers, expected);
    }
    eprintln!("ABPS_REFERENCE_ACQUIRE={mode} ABPS_TARGET_SANITIZERS={sanitizers}");
}
