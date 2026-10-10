fn main() {
    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rustc-check-cfg=cfg(abps_tsan)");

    // Cargo supplies the package's target cfg, not the build-script host cfg.
    // This also works when ABPS is a dependency of a sanitizer-enabled project.
    let sanitizers = std::env::var("CARGO_CFG_SANITIZE").unwrap_or_default();
    if sanitizers.split(',').any(|name| name == "thread") {
        println!("cargo::rustc-cfg=abps_tsan");
    }
    // Verification reads this at compile time; no runtime/public API switch.
    println!("cargo::rustc-env=ABPS_TARGET_SANITIZERS={sanitizers}");
}
