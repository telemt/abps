//! Measurement payload v1: deterministic identity in every byte, not a checksum
//! of only the frame edges. This is a corruption detector, not authentication.
use tokio::io::{AsyncRead, AsyncReadExt};

pub const VERSION: &str = "splitmix64-v1";

fn mix(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

fn word(seed: u64, stream: usize, id: u64, len: usize, block: usize) -> [u8; 8] {
    mix(seed
        ^ mix(stream as u64)
        ^ mix(id.wrapping_add(0x9e3779b97f4a7c15))
        ^ mix(len as u64)
        ^ mix((block as u64).wrapping_add(0xd1b54a32d192ed03)))
    .to_le_bytes()
}

pub fn fill_payload(dst: &mut [u8], seed: u64, stream: usize, id: u64) {
    let len = dst.len();
    for (block, bytes) in dst.chunks_mut(8).enumerate() {
        bytes.copy_from_slice(&word(seed, stream, id, len, block)[..bytes.len()]);
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct PayloadMismatch {
    pub offset: usize,
    pub expected: u8,
    pub observed: u8,
}

pub fn validate_payload(
    src: &[u8],
    seed: u64,
    stream: usize,
    id: u64,
) -> Result<(), PayloadMismatch> {
    for (block, bytes) in src.chunks(8).enumerate() {
        let expected = word(seed, stream, id, src.len(), block);
        if let Some(offset) = bytes.iter().zip(expected).position(|(a, b)| *a != b) {
            return Err(PayloadMismatch {
                offset: block * 8 + offset,
                expected: expected[offset],
                observed: bytes[offset],
            });
        }
    }
    Ok(())
}

/// Keep the initialized prefix outside the future so cancellation reports the
/// actual partial read. False means EOF, including an empty frame boundary.
pub async fn read_prefix(
    reader: &mut (impl AsyncRead + Unpin),
    dst: &mut [u8],
    filled: &mut usize,
) -> std::io::Result<bool> {
    while *filled < dst.len() {
        let n = reader.read(&mut dst[*filled..]).await?;
        if n == 0 {
            return Ok(false);
        }
        *filled += n;
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn payload_all_offsets_and_identity_are_checked() {
        for len in [1, 7, 8, 9, 63, 64, 65, 1500, 65536] {
            let mut data = vec![0; len];
            fill_payload(&mut data, 42, 3, 91);
            assert_eq!(validate_payload(&data, 42, 3, 91), Ok(()));
            for i in (0..len).filter(|i| len <= 1500 || i % 997 == 0 || *i == len - 1) {
                data[i] ^= 1;
                assert_eq!(validate_payload(&data, 42, 3, 91).unwrap_err().offset, i);
                data[i] ^= 1;
            }
            for (seed, stream, id) in [(43, 3, 91), (42, 4, 91), (42, 3, 92)] {
                assert!(validate_payload(&data, seed, stream, id).is_err());
            }
        }
    }
    #[test]
    fn payload_golden_vectors() {
        let mut data = [0; 17];
        fill_payload(&mut data, 42, 3, 91);
        assert_eq!(
            data,
            [
                107, 16, 163, 30, 246, 136, 219, 20, 18, 108, 45, 236, 173, 247, 99, 3, 52
            ]
        );
    }
}
