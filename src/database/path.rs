use std::ops::Index;

use super::bytes::Bytes;
use super::direction::Direction;

#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord)]
pub(crate) struct Path(Bytes);

impl Path {
    pub fn empty() -> Self {
        Path(Bytes::empty())
    }

    pub fn set(&mut self, index: u8, value: Direction) {
        let (byte, bit) = Path::split(index);

        if value == Direction::Left {
            self.0 .0[byte] |= 1 << (7 - bit);
        } else {
            self.0 .0[byte] &= !(1 << (7 - bit));
        }
    }

    pub fn deepeq(lho: &Path, rho: &Path, depth: u8) -> bool {
        let (full, overflow) = Path::split(depth);

        if lho.0 .0[0..full] != rho.0 .0[0..full] {
            return false;
        }

        if overflow > 0 {
            let shift = 8 - overflow;
            (lho.0 .0[full] >> shift) == (rho.0 .0[full] >> shift)
        } else {
            true
        }
    }

    fn split(index: u8) -> (usize, u8) {
        ((index / 8) as usize, index % 8)
    }
}

impl From<Bytes> for Path {
    fn from(bytes: Bytes) -> Path {
        Path(bytes)
    }
}

impl Index<u8> for Path {
    type Output = Direction;

    fn index(&self, index: u8) -> &Self::Output {
        let (byte, bit) = Path::split(index);
        let mask = 1 << (7 - bit);

        if self.0 .0[byte] & mask != 0 {
            &Direction::Left
        } else {
            &Direction::Right
        }
    }
}
