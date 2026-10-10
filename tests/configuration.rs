#![cfg(any(feature = "berry-spike", feature = "mosaic-spike"))]
#![forbid(unsafe_code)]
#[path = "support/configuration.rs"]
mod configuration;

#[test]
fn checked_configuration_small_cartesian_and_boundaries() {
    use abps_reference::ClassConfig;
    for classes in 1..=2 {
        for a in 1..=3 {
            for b in 1..=3 {
                for records in 1..=3 {
                    for reserved in [false, true] {
                        let config = (0..classes)
                            .map(|i| ClassConfig {
                                capacity: 3 + 14 * i,
                                slots: if i == 0 { a } else { b },
                            })
                            .collect::<Vec<_>>();
                        #[cfg(feature = "berry-spike")]
                        assert!(configuration::berry::check(
                            config.clone(),
                            records,
                            reserved,
                            3,
                            64,
                            0
                        ));
                        #[cfg(feature = "mosaic-spike")]
                        for option in [1, 9, 17, 18] {
                            configuration::mosaic::check(
                                config.clone(),
                                records,
                                reserved,
                                3,
                                64,
                                option,
                            );
                        }
                    }
                }
            }
        }
    }
    for index in 0..10 {
        for value in 0..=10 {
            let mut input = [1, 1, 2, 1, 1, 1, 1, 3, 3, 1];
            input[index] = value;
            configuration::replay(&input);
        }
    }
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config { cases:if cfg!(miri) {4} else {std::env::var("ABPS_PROPTEST_CASES").ok().map(|v|v.parse().unwrap()).unwrap_or(128)}, failure_persistence: if cfg!(miri) {None} else {Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource("proptest-regressions")))}, ..Default::default() })]
    #[test]
    fn configuration_arithmetic_and_exact_budget(data in proptest::array::uniform10(proptest::prelude::any::<u8>())) {
        configuration::replay(&data);
    }
}
