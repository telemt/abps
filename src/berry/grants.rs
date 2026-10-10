//! SI-09: resolve once before any admission or rights mutation.
#![forbid(unsafe_code)]
use crate::{ClassReservation, Error};

pub(super) fn resolve(
    grants: &[ClassReservation],
    classes: usize,
    mut capacity: impl FnMut(usize) -> usize,
    indices: &mut [usize; 64],
) -> Result<(), Error> {
    if grants.len() > classes || classes > indices.len() {
        return Err(Error::InvalidConfiguration);
    }
    let mut previous = 0;
    let mut class = 0;
    for (grant, out) in grants.iter().zip(indices) {
        if grant.capacity <= previous || grant.slots == 0 {
            return Err(Error::InvalidConfiguration);
        }
        loop {
            if class == classes {
                return Err(Error::InvalidConfiguration);
            }
            let size = capacity(class);
            if size > grant.capacity {
                return Err(Error::InvalidConfiguration);
            }
            if size == grant.capacity {
                *out = class;
                class += 1;
                break;
            }
            class += 1;
        }
        previous = grant.capacity;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn si09_rejects_oversized_input_before_reading_classes_or_writing_scratch() {
        // SI-09/MEM-01: invalid dimensions must not even invoke the capacity
        // accessor. Resolution cannot write past or truncate the bounded scratch.
        let grants = [ClassReservation {
            capacity: 1,
            slots: 1,
        }; 3];
        for (input, classes) in [(&grants[..], 2), (&grants[..0], 65)] {
            let mut scratch = [usize::MAX; 64];
            assert_eq!(
                resolve(
                    input,
                    classes,
                    |_| panic!("invalid preflight read"),
                    &mut scratch
                ),
                Err(Error::InvalidConfiguration)
            );
            assert_eq!(scratch, [usize::MAX; 64]);
        }
    }

    #[test]
    fn si09_invalid_grants_fail_with_bounded_class_reads() {
        // SI-09: validate each independent error and bound work even for bad
        // inputs. The accessor is an independent work monitor, so a search
        // mutation that fails to advance is caught rather than hanging a test.
        for grants in [
            vec![(0, 1)],
            vec![(1, 0)],
            vec![(1, 1), (1, 1)],
            vec![(4, 1), (1, 1)],
            vec![(2, 1)],
            vec![(9, 1)],
        ] {
            let grants: Vec<_> = grants
                .into_iter()
                .map(|(capacity, slots)| ClassReservation { capacity, slots })
                .collect();
            let mut reads = [false; 3];
            let mut scratch = [usize::MAX; 64];
            assert_eq!(
                resolve(
                    &grants,
                    3,
                    |i| {
                        assert!(i < 3, "read beyond prepared classes");
                        assert!(!reads[i], "unbounded repeated class read");
                        reads[i] = true;
                        [1, 4, 8][i]
                    },
                    &mut scratch
                ),
                Err(Error::InvalidConfiguration)
            );
        }
    }

    #[test]
    fn si09_resolution_visits_each_class_at_most_once() {
        for count in [1, 4, 16, 64] {
            for stride in [1, 2, 7] {
                let grants: Vec<_> = (1..=count)
                    .step_by(stride)
                    .map(|capacity| ClassReservation { capacity, slots: 1 })
                    .collect();
                let mut visited = [false; 64];
                let mut work = 0;
                let mut indices = [usize::MAX; 64];
                resolve(
                    &grants,
                    count,
                    |i| {
                        assert!(!visited[i], "class searched repeatedly");
                        visited[i] = true;
                        work += 1;
                        i + 1
                    },
                    &mut indices,
                )
                .unwrap();
                assert!(work <= count);
                for (g, i) in grants.iter().zip(indices) {
                    assert_eq!(g.capacity, i + 1);
                }
            }
        }
    }
}
