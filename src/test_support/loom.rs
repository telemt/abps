//! Test-only exploration policy. These bounds do not change the protocol.
use std::env;

pub(crate) fn model(f: impl Fn() + Send + Sync + 'static) {
    check(false, f);
}

pub(crate) fn exhaustive(f: impl Fn() + Send + Sync + 'static) {
    check(true, f);
}

fn check(small: bool, f: impl Fn() + Send + Sync + 'static) {
    // Builder silently returns success on duration/permutation caps. Disallow
    // those caps: an external runner timeout must be reported as incomplete.
    for key in [
        "LOOM_MAX_PERMUTATIONS",
        "LOOM_MAX_DURATION",
        "LOOM_MAX_PREEMPTIONS",
        "LOOM_MAX_BRANCHES",
        "LOOM_CHECKPOINT_FILE",
    ] {
        assert!(
            env::var_os(key).is_none(),
            "{key} conflicts with the explicit ABPS Loom profile"
        );
    }
    let profile = env::var("ABPS_LOOM_PROFILE").unwrap_or_else(|_| "smoke".into());
    let bound = match profile.as_str() {
        "smoke" => Some(2),
        "deep" => Some(3),
        "exhaustive" => None,
        _ => panic!("unknown ABPS_LOOM_PROFILE: {profile}"),
    };
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = if small { None } else { bound };
    builder.max_branches = 10_000;
    builder.max_permutations = None;
    builder.max_duration = None;
    eprintln!(
        "ABPS_LOOM profile={profile} preemptions={:?} branches={} permutations=unlimited duration=unlimited raw_storage=false reference_acquire={}",
        builder.preemption_bound,
        builder.max_branches,
        if cfg!(abps_tsan) {
            "acquire-load"
        } else {
            "fence"
        }
    );
    builder.check(f);
    eprintln!(
        "ABPS_LOOM completed exploration={}",
        if builder.preemption_bound.is_none() {
            "exhaustive-within-model"
        } else {
            "preemption-bounded"
        }
    );
}
