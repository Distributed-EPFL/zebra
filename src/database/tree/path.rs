use crate::{
    common::{
        data::{bytes::EMPTY, Bytes},
        store::Field,
    },
    database::{store::Wrap, tree::Direction},
};

use drop::crypto::Digest;

use std::ops::Index;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Path(Bytes);

impl Path {
    pub fn empty() -> Self {
        Path(EMPTY)
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

impl Into<Bytes> for Path {
    fn into(self) -> Bytes {
        self.0
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

pub(crate) struct PathIterator {
    path: Path,
    cursor: usize,
}

impl Iterator for PathIterator {
    type Item = Direction;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor < 256 {
            self.cursor += 1;
            Some(self.path[(self.cursor - 1) as u8])
        } else {
            None
        }
    }
}

impl IntoIterator for Path {
    type Item = Direction;
    type IntoIter = PathIterator;

    fn into_iter(self) -> Self::IntoIter {
        PathIterator {
            cursor: 0,
            path: self,
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

    impl Path {
        pub fn from_directions<I>(directions: I) -> Self
        where
            I: IntoIterator<Item = Direction>,
        {
            let mut path = Path::empty();

            for (index, direction) in directions.into_iter().enumerate() {
                path.set(index as u8, direction);
            }

            path
        }

        pub fn into_vec(self, len: usize) -> Vec<Direction> {
            self.into_iter().take(len).collect()
        }
    }

    #[test]
    fn path() {
        use Direction::{Left as L, Right as R};
        let reference = vec![L, L, L, R, L, L, R, R, R, R, L, R, L, R, L, L];

        assert_eq!(
            Path::empty().into_vec(8 * SIZE - 1),
            iter::repeat(Direction::Right)
                .take(8 * SIZE - 1)
                .collect::<Vec<Direction>>()
        );

        assert_eq!(
            Path::from(hash(&0u32).unwrap()).into_vec(reference.len()),
            reference
        );

        assert_eq!(
            Path::from_directions(reference.clone()).into_vec(reference.len()),
            reference
        );
    }

    #[test]
    fn ordering() {
        use Direction::{Left as L, Right as R};

        assert!(
            &Path::from_directions(vec![R]) < &Path::from_directions(vec![L])
        );

        assert!(
            &Path::from_directions(vec![R])
                < &Path::from_directions(vec![R, L])
        );

        assert!(
            &Path::from_directions(vec![L, R, L])
                < &Path::from_directions(vec![L, L, L, L, L])
        );

        let lesser = vec![L, L, L, R, L, L, R, R, R, R, L, R, L, R, L, L];

        let mut greater = lesser.clone();
        greater.push(L);

        assert!(
            &Path::from_directions(lesser) < &Path::from_directions(greater)
        );
    }
}
