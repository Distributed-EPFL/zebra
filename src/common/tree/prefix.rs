use crate::common::tree::{Direction, Path, PathIterator};

use std::{iter::Take, ops::Index};

#[derive(Debug, Clone, Copy)]
pub(crate) struct Prefix {
    path: Path,
    depth: u8,
}

impl Prefix {
    pub fn root() -> Self {
        Prefix {
            path: Path::empty(),
            depth: 0,
        }
    }

    pub fn common(lho: Path, rho: Path) -> Self {
        let depth = lho
            .into_iter()
            .zip(rho)
            .take_while(|(left, right)| left == right)
            .count();

        Prefix {
            path: lho,
            depth: depth as u8,
        }
    }

    pub fn depth(&self) -> u8 {
        self.depth
    }

    pub fn ancestor(&self, generations: u8) -> Self {
        if self.depth < generations {
            panic!("`ancestor` does not exist (would be above root)");
        }

        Prefix {
            path: self.path,
            depth: self.depth - generations,
        }
    }

    pub fn left(&self) -> Self {
        self.child(Direction::Left)
    }

    pub fn right(&self) -> Self {
        self.child(Direction::Right)
    }

    fn child(&self, direction: Direction) -> Self {
        let mut path = self.path;
        path.set(self.depth, direction);

        Prefix {
            path,
            depth: self.depth + 1,
        }
    }

    pub fn contains(&self, path: &Path) -> bool {
        Path::deepeq(&self.path, path, self.depth)
    }
}

impl Index<u8> for Prefix {
    type Output = Direction;

    fn index(&self, index: u8) -> &Self::Output {
        debug_assert!(index < self.depth);
        &self.path[index]
    }
}

impl PartialEq for Prefix {
    fn eq(&self, rho: &Self) -> bool {
        self.depth == rho.depth && Path::deepeq(&self.path, &rho.path, self.depth)
    }
}

impl IntoIterator for Prefix {
    type Item = Direction;
    type IntoIter = Take<PathIterator>;

    fn into_iter(self) -> Self::IntoIter {
        self.path.into_iter().take(self.depth as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::vec::Vec;

    impl Prefix {
        pub fn new(path: Path, depth: u8) -> Self {
            Prefix { path, depth }
        }

        pub fn from_directions<I>(directions: I) -> Self
        where
            I: IntoIterator<Item = Direction>,
        {
            let mut prefix = Prefix::root();

            for direction in directions {
                prefix = if direction == Direction::Left {
                    prefix.left()
                } else {
                    prefix.right()
                };
            }

            prefix
        }

        pub fn into_vec(self) -> Vec<Direction> {
            self.path.into_vec(self.depth as usize)
        }
    }

    #[test]
    fn prefix() {
        use Direction::{Left as L, Right as R};
        let reference = vec![L, L, L, R, L, L, R, R, R, R, L, R, L, R, L, L];

        let path = Path::from_directions(reference.clone());

        assert_eq!(
            Prefix::new(path, reference.len() as u8).into_vec(),
            reference
        );

        assert_eq!(Prefix::root().into_vec(), vec![]);

        assert_eq!(
            Prefix::from_directions(reference.clone()).into_vec(),
            reference
        );

        assert!(Prefix::root().contains(&Path::from_directions(vec![L])));
        assert!(Prefix::root().contains(&Path::from_directions(vec![R])));

        assert!(Prefix::root().left().contains(&path));
        assert!(!Prefix::root().right().contains(&path));

        assert!(Prefix::from_directions(vec![L, L, L, R, L, L, R]).contains(&path));

        assert!(!Prefix::from_directions(vec![L, L, L, R, L, L, L]).contains(&path));

        assert!(Prefix::new(path, reference.len() as u8).contains(&path));

        assert!(Prefix::new(path, reference.len() as u8)
            .right()
            .contains(&path));

        assert!(!Prefix::new(path, reference.len() as u8)
            .left()
            .contains(&path));

        assert_eq!(Prefix::root(), Prefix::root());
        assert_eq!(Prefix::root().left(), Prefix::root().left());
        assert_ne!(Prefix::root().left(), Prefix::root().right());
        assert_ne!(Prefix::root(), Prefix::root().left());

        assert_eq!(
            Prefix::from_directions(vec![L, L, L, R, L, L, L]),
            Prefix::from_directions(vec![L, L, L, R, L, L, L])
        );

        assert_ne!(
            Prefix::from_directions(vec![L, L, L, R, L, L, L]),
            Prefix::from_directions(vec![L, L, L, R, L, L, R])
        );

        assert_ne!(
            Prefix::from_directions(vec![L, L, L, R, L, L, L]),
            Prefix::from_directions(vec![L, L, L, R, L, L])
        );
    }

    #[test]
    fn iterator() {
        use Direction::{Left as L, Right as R};

        assert_eq!(
            Prefix::from_directions(vec![])
                .into_iter()
                .collect::<Vec<Direction>>(),
            vec![]
        );

        assert_eq!(
            Prefix::from_directions(vec![L])
                .into_iter()
                .collect::<Vec<Direction>>(),
            vec![L]
        );

        assert_eq!(
            Prefix::from_directions(vec![L, R])
                .into_iter()
                .collect::<Vec<Direction>>(),
            vec![L, R]
        );

        assert_eq!(
            Prefix::from_directions(vec![L, R, L, L, R, L])
                .into_iter()
                .collect::<Vec<Direction>>(),
            vec![L, R, L, L, R, L]
        );
    }

    #[test]
    fn common() {
        use Direction::{Left as L, Right as R};

        assert_eq!(
            Prefix::common(Path::from_directions(vec![]), Path::from_directions(vec![])),
            Prefix::from_directions(vec![])
        );

        assert_eq!(
            Prefix::common(
                Path::from_directions(vec![]),
                Path::from_directions(vec![L, R, L])
            ),
            Prefix::from_directions(vec![])
        );

        assert_eq!(
            Prefix::common(
                Path::from_directions(vec![L, L]),
                Path::from_directions(vec![L, R, L])
            ),
            Prefix::from_directions(vec![L])
        );

        assert_eq!(
            Prefix::common(
                Path::from_directions(vec![R]),
                Path::from_directions(vec![L, R, L])
            ),
            Prefix::from_directions(vec![])
        );

        assert_eq!(
            Prefix::common(
                Path::from_directions(vec![R, R, L]),
                Path::from_directions(vec![L])
            ),
            Prefix::from_directions(vec![])
        );

        assert_eq!(
            Prefix::common(
                Path::from_directions(vec![L, R, L, L]),
                Path::from_directions(vec![L, R, L])
            ),
            Prefix::from_directions(vec![L, R, L])
        );

        assert_eq!(
            Prefix::common(
                Path::from_directions(vec![L, R, L, R]),
                Path::from_directions(vec![L, R, L, R, L, L])
            ),
            Prefix::from_directions(vec![L, R, L, R])
        );

        assert_eq!(
            Prefix::common(
                Path::from_directions(vec![L, R, L, R, L, L, R, L]),
                Path::from_directions(vec![R, R, L, R, L, L, R, L])
            ),
            Prefix::from_directions(vec![])
        );

        assert_eq!(
            Prefix::common(
                Path::from_directions(vec![L, R, L, R, L, L, R, L, R, R, R, R]),
                Path::from_directions(vec![L, R, L, R, L, L, R, L, R, R, R, L])
            ),
            Prefix::from_directions(vec![L, R, L, R, L, L, R, L, R, R, R])
        );

        assert_eq!(
            Prefix::common(
                Path::from_directions(vec![L, R, L, L, R, L, L, L, L, L]),
                Path::from_directions(vec![L, R, L, L, R, L, R, R, R, R])
            ),
            Prefix::from_directions(vec![L, R, L, L, R, L])
        );
    }
}
