use super::*;
fn policy() -> AdaptivePolicy {
    AdaptivePolicy {
        target: Duration::from_millis(20),
        grace: Duration::from_secs(2),
        stall: Duration::from_millis(500),
        key_wait: Duration::from_secs(5),
        keyed: false,
    }
}
#[test]
fn ap11_pressure_hysteresis_burst_sustained_and_recovery() {
    for (burst, closes) in [(0, false), (100, false), (6000, true)] {
        let start = Instant::now();
        let mut c = Controller::new(policy(), 32, start);
        let mut minimum = 32;
        for ms in (10..=6000).step_by(10) {
            let now = start + Duration::from_millis(ms);
            if ms <= burst {
                c.age(now - Duration::from_millis(20), now);
            }
            c.sample(now, 0);
            assert!((1..=32).contains(&c.window));
            assert!(c.fast <= SCALE);
            minimum = minimum.min(c.window);
            if c.closed.is_some() {
                assert!((2000..=2200).contains(&ms));
                break;
            }
        }
        assert_eq!(c.closed.is_some(), closes);
        if burst == 100 {
            assert!(minimum < 32);
            assert_eq!(c.window, 32);
            assert_eq!(c.debt_ms, 0);
        }
        if closes {
            assert_eq!(c.closed, Some(CloseReason::Pressure));
            assert_eq!(minimum, 1);
        }
    }
}
#[test]
fn ap11_idle_progress_and_terminal_reasons_are_distinct() {
    let start = Instant::now();
    let mut c = Controller::new(policy(), 2, start);
    c.sample(start + Duration::from_secs(20), 0);
    assert_eq!(c.closed, None, "idle is not stalled output");
    let now = start + Duration::from_secs(20);
    c.batch(Some(now), None, now);
    c.progress(0, now + Duration::from_millis(499));
    c.sample(now + Duration::from_millis(500), 0);
    assert_eq!(c.closed, Some(CloseReason::WriteStall));
    c.close(CloseReason::Pressure);
    assert_eq!(c.closed, Some(CloseReason::WriteStall));
    let mut c = Controller::new(policy(), 2, start);
    c.batch(Some(start), None, start);
    c.progress(1, start + Duration::from_millis(499));
    c.sample(start + Duration::from_millis(500), 0);
    assert_eq!(c.closed, None);
    c.batch(None, None, start + Duration::from_millis(501));
    c.sample(start + Duration::from_secs(2), 0);
    assert_eq!(c.closed, None);
}
#[test]
fn ap11_keyframe_wait_is_not_extended_by_more_gaps_or_epoch_wrap() {
    let start = Instant::now();
    let mut p = policy();
    p.keyed = true;
    let mut c = Controller::new(p, 2, start);
    c.offer(start);
    c.gap(start + Duration::from_secs(4));
    c.sample(start + Duration::from_secs(5), 0);
    assert_eq!(c.closed, Some(CloseReason::KeyframeTimeout));
    let mut c = Controller::new(p, 2, start);
    c.epoch = u64::MAX;
    c.gap(start);
    assert_eq!(c.epoch, u64::MAX);
    assert_eq!(c.closed, Some(CloseReason::GenerationExhausted));
}
#[test]
fn ap11_wire_v1_is_unchanged_and_v2_rejects_invalid_metadata() {
    let h = Header {
        id: 7,
        len: 4,
        due: 123,
        ..Header::default()
    };
    assert_eq!(&h.wire()[..20], &h.encode());
    assert_eq!(Header::decode(&h.wire()[..20]).unwrap().sequence, None);
    let h = Header {
        sequence: Some(0),
        keyframe: true,
        ..h
    };
    let wire = h.wire();
    assert_eq!(Header::decode(&wire).unwrap().sequence, Some(0));
    assert!(Header::decode(&wire).unwrap().keyframe);
    for i in 28..32 {
        let mut bad = wire;
        bad[i] = 2;
        assert!(Header::decode(&bad).is_err());
    }
    for n in 0..32 {
        if n != 20 {
            assert!(Header::decode(&wire[..n]).is_err());
        }
    }
}
#[test]
fn ap11_config_rejects_incompatible_and_invalid_policy_before_resources() {
    let valid = [
        ("backend", "mosaic"),
        ("queue-backend", "local"),
        ("return", "local"),
        ("overflow", "skip"),
        ("freshness-priority", "adaptive"),
    ];
    let args = || {
        Args(
            valid
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    };
    let mut cli = args();
    for (key, value) in [
        ("freshness-target-ms", "20"),
        ("slow-grace-ms", "2000"),
        ("write-stall-ms", "500"),
        ("frame-dependencies", "keyframe"),
        ("keyframe-wait-ms", "5000"),
        ("keyframe-interval", "20"),
        ("slow-profile", "pause"),
    ] {
        cli.0.insert(key.into(), value.into());
    }
    crate::spike_support::validate(&cli).unwrap();
    assert!(Settings::new(&cli).unwrap().keyed());
    let s = Settings::new(&args()).unwrap();
    assert_eq!(s.adaptive.unwrap().target, Duration::from_millis(20));
    for (key, val) in [
        ("freshness-target-ms", "0"),
        ("slow-grace-ms", "3600001"),
        ("write-stall-ms", "bad"),
        ("keyframe-wait-ms", "100"),
        ("freshness-min-age-ms", "0"),
        ("queue-backend", "tokio"),
        ("return", "remote"),
        ("overflow", "disconnect"),
        ("fanout-api", "worker-branches"),
    ] {
        let mut a = args();
        a.0.insert(key.into(), val.into());
        assert!(Settings::new(&a).is_err(), "{key}");
    }
}

#[test]
fn ap11_consumer_checks_dependency_metadata_against_source_schedule() {
    let args = Args(
        [
            ("backend", "mosaic"),
            ("queue-backend", "local"),
            ("return", "local"),
            ("overflow", "skip"),
            ("freshness-priority", "adaptive"),
            ("frame-dependencies", "keyframe"),
        ]
        .into_iter()
        .map(|(k, v)| (k.into(), v.into()))
        .collect(),
    );
    let settings = Settings::new(&args).unwrap();
    let len = settings.payload_size(0, 0);
    let validation = Validation {
        settings,
        rate: 1000,
        planned: 100,
        skew: false,
    };
    let h = Header {
        len,
        id: 0,
        due: 0,
        sequence: Some(0),
        keyframe: true,
    };
    assert_eq!(validation.header_error(h, 0, None), None);
    for bad in [
        Header {
            sequence: Some(1),
            ..h
        },
        Header {
            keyframe: false,
            ..h
        },
    ] {
        assert_eq!(
            validation.header_error(bad, 0, None),
            Some("invalid_dependency_metadata")
        );
    }
}
