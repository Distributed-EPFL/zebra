use drop::crypto::Digest;

use std::ops::Index;

use super::bytes::Bytes;
use super::direction::Direction;
use super::field::Field;
use super::wrap::Wrap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Path(Bytes);

impl Path {
    pub fn empty() -> Self {
        Path(Bytes::empty())
    }

    pub fn reaches<Key>(&self, key: &Wrap<Key>) -> bool
    where
        Key: Field,
    {
        self.0 == *key.digest()
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

impl From<Digest> for Path {
    fn from(digest: Digest) -> Path {
        Path::from(Bytes::from(digest))
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

#[cfg(test)]
mod tests {
    use super::*;

    use drop::crypto::hash;
    use drop::crypto::hash::SIZE;

    use std::iter;
    use std::vec::Vec;

    fn path_from_directions(directions: &Vec<Direction>) -> Path {
        let mut path = Path::empty();

        for index in 0..directions.len() {
            path.set(index as u8, directions[index]);
        }

        path
    }

    fn directions_from_path(path: &Path, until: u8) -> Vec<Direction> {
        (0..until).map(|index| path[index]).collect()
    }

    #[test]
    fn path() {
        use Direction::{Left as L, Right as R};
        let reference = vec![L, L, L, R, L, L, R, R, R, R, L, R, L, R, L, L];

        assert_eq!(
            directions_from_path(&Path::empty(), (8 * SIZE - 1) as u8),
            iter::repeat(Direction::Right)
                .take(8 * SIZE - 1)
                .collect::<Vec<Direction>>()
        );
        assert_eq!(
            directions_from_path(
                &Path::from(hash(&0u32).unwrap()),
                reference.len() as u8
            ),
            reference
        );
        assert_eq!(
            directions_from_path(
                &path_from_directions(&reference),
                reference.len() as u8
            ),
            reference
        );
    }

    #[test]
    fn ordering() {
        use Direction::{Left as L, Right as R};

        assert!(
            &path_from_directions(&vec![R]) < &path_from_directions(&vec![L])
        );
        assert!(
            &path_from_directions(&vec![R])
                < &path_from_directions(&vec![R, L])
        );
        assert!(
            &path_from_directions(&vec![L, R, L])
                < &path_from_directions(&vec![L, L, L, L, L])
        );

        let lesser = vec![L, L, L, R, L, L, R, R, R, R, L, R, L, R, L, L];

        let mut greater = lesser.clone();
        greater.push(L);

        assert!(
            &path_from_directions(&lesser) < &path_from_directions(&greater)
        );
    }
}
