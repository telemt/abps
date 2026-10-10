//! Stable, bounded input shared by proptest, corpus replay and libFuzzer.
#![allow(dead_code)]
pub const MAX_OPERATIONS: usize = 1024;
#[derive(Clone, Copy, Debug)]
#[repr(u8)]
pub enum Kind {
    Open,
    DropOwner,
    DropDomain,
    Acquire,
    Write,
    Freeze,
    Clone,
    Slice,
    Drop,
    Initialize,
    DropAgain,
    Poll,
    Batch,
    SetFilled,
    Close,
    OccupiedBatch,
}
#[derive(Clone, Copy, Debug)]
pub struct Action {
    pub kind: Kind,
    pub x: u8,
    pub y: u8,
    pub z: u8,
}
#[derive(Clone, Debug)]
pub struct Trace {
    pub reserved: bool,
    pub counts: [usize; 2],
    pub actions: Vec<Action>,
}
#[derive(Debug, PartialEq, Eq)]
pub struct Observation {
    pub available: Vec<usize>,
    pub admitted: Vec<usize>,
    pub payloads: Vec<Vec<u8>>,
}
impl Trace {
    pub fn decode(input: &[u8], wide: bool) -> Self {
        let w = usize::BITS as usize;
        let sizes = [1, 2, 3, w - 1, w, w + 1, 2 * w - 1, 2 * w, 2 * w + 1];
        let choices = if wide { sizes.len() } else { 5 };
        let byte = |i| input.get(i).copied().unwrap_or(0) as usize;
        let kinds = [
            Kind::Open,
            Kind::DropOwner,
            Kind::DropDomain,
            Kind::Acquire,
            Kind::Write,
            Kind::Freeze,
            Kind::Clone,
            Kind::Slice,
            Kind::Drop,
            Kind::Initialize,
            Kind::DropAgain,
            Kind::Poll,
            Kind::Batch,
            Kind::SetFilled,
            Kind::Close,
            Kind::OccupiedBatch,
        ];
        Self {
            reserved: byte(0) & 1 != 0,
            counts: [sizes[byte(1) % choices], sizes[byte(2) % choices]],
            actions: input
                .get(3..)
                .unwrap_or_default()
                .chunks_exact(4)
                .take(MAX_OPERATIONS)
                .map(|a| Action {
                    kind: kinds[a[0] as usize % kinds.len()],
                    x: a[1],
                    y: a[2],
                    z: a[3],
                })
                .collect(),
        }
    }
}
