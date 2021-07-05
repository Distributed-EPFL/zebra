use crate::database::tree::{Direction, Path, PathIterator};

use std::iter::Take;
use std::ops::Index;

#[derive(Debug, Clone, Copy)]
pub(crate) struct Prefix {
    path: Path,
    depth: u8,
}

impl Prefix {
    #[cfg(test)]
    pub fn new(path: Path, depth: u8) -> Self {
        Prefix { path, depth }
    }

    pub fn root() -> Self {
        Prefix {
            path: Path::empty(),
            depth: 0,
        }
    }

    pub fn common(lho: Path, rho: Path) -> Self {
        let depth = lho
            .into_iter()
            .zip(rho.into_iter())
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
        self.depth == rho.depth
            && Path::deepeq(&self.path, &rho.path, self.depth)
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

    fn prefix_from_directions(directions: &Vec<Direction>) -> Prefix {
        let mut prefix = Prefix::root();

        for &direction in directions {
            prefix = if direction == Direction::Left {
                prefix.left()
            } else {
                prefix.right()
            };
        }

        prefix
    }

    fn directions_from_prefix(prefix: &Prefix) -> Vec<Direction> {
        directions_from_path(&prefix.path, prefix.depth())
    }

    #[test]
    fn prefix() {
        use Direction::{Left as L, Right as R};
        let reference = vec![L, L, L, R, L, L, R, R, R, R, L, R, L, R, L, L];

        let path = path_from_directions(&reference);

        assert_eq!(
            directions_from_prefix(&Prefix::new(path, reference.len() as u8)),
            reference
        );
        assert_eq!(directions_from_prefix(&Prefix::root()), vec![]);
        assert_eq!(
            directions_from_prefix(&prefix_from_directions(&reference)),
            reference
        );

        assert!(Prefix::root().contains(&path_from_directions(&vec![L])));
        assert!(Prefix::root().contains(&path_from_directions(&vec![R])));

        assert!(Prefix::root().left().contains(&path));
        assert!(!Prefix::root().right().contains(&path));

        assert!(
            prefix_from_directions(&vec![L, L, L, R, L, L, R]).contains(&path)
        );
        assert!(
            !prefix_from_directions(&vec![L, L, L, R, L, L, L]).contains(&path)
        );

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
            prefix_from_directions(&vec![L, L, L, R, L, L, L]),
            prefix_from_directions(&vec![L, L, L, R, L, L, L])
        );
        assert_ne!(
            prefix_from_directions(&vec![L, L, L, R, L, L, L]),
            prefix_from_directions(&vec![L, L, L, R, L, L, R])
        );
        assert_ne!(
            prefix_from_directions(&vec![L, L, L, R, L, L, L]),
            prefix_from_directions(&vec![L, L, L, R, L, L])
        );
    }

    #[test]
    fn iterator() {
        use Direction::{Left as L, Right as R};

        assert_eq!(
            prefix_from_directions(&vec![])
                .into_iter()
                .collect::<Vec<Direction>>(),
            vec![]
        );
        assert_eq!(
            prefix_from_directions(&vec![L])
                .into_iter()
                .collect::<Vec<Direction>>(),
            vec![L]
        );
        assert_eq!(
            prefix_from_directions(&vec![L, R])
                .into_iter()
                .collect::<Vec<Direction>>(),
            vec![L, R]
        );
        assert_eq!(
            prefix_from_directions(&vec![L, R, L, L, R, L])
                .into_iter()
                .collect::<Vec<Direction>>(),
            vec![L, R, L, L, R, L]
        );
    }

    #[test]
    fn common() {
        use Direction::{Left as L, Right as R};

        assert_eq!(
            Prefix::common(
                path_from_directions(&vec![]),
                path_from_directions(&vec![])
            ),
            prefix_from_directions(&vec![])
        );

        assert_eq!(
            Prefix::common(
                path_from_directions(&vec![]),
                path_from_directions(&vec![L, R, L])
            ),
            prefix_from_directions(&vec![])
        );

        assert_eq!(
            Prefix::common(
                path_from_directions(&vec![L, L]),
                path_from_directions(&vec![L, R, L])
            ),
            prefix_from_directions(&vec![L])
        );

        assert_eq!(
            Prefix::common(
                path_from_directions(&vec![R]),
                path_from_directions(&vec![L, R, L])
            ),
            prefix_from_directions(&vec![])
        );

        assert_eq!(
            Prefix::common(
                path_from_directions(&vec![R, R, L]),
                path_from_directions(&vec![L])
            ),
            prefix_from_directions(&vec![])
        );

        assert_eq!(
            Prefix::common(
                path_from_directions(&vec![L, R, L, L]),
                path_from_directions(&vec![L, R, L])
            ),
            prefix_from_directions(&vec![L, R, L])
        );

        assert_eq!(
            Prefix::common(
                path_from_directions(&vec![L, R, L, R]),
                path_from_directions(&vec![L, R, L, R, L, L])
            ),
            prefix_from_directions(&vec![L, R, L, R])
        );

        assert_eq!(
            Prefix::common(
                path_from_directions(&vec![L, R, L, R, L, L, R, L]),
                path_from_directions(&vec![R, R, L, R, L, L, R, L])
            ),
            prefix_from_directions(&vec![])
        );

        assert_eq!(
            Prefix::common(
                path_from_directions(&vec![L, R, L, R, L, L, R, L, R, R, R, R]),
                path_from_directions(&vec![L, R, L, R, L, L, R, L, R, R, R, L])
            ),
            prefix_from_directions(&vec![L, R, L, R, L, L, R, L, R, R, R])
        );

        assert_eq!(
            prefix_from_directions(&vec![L, R, L, L, R, L]),
            Prefix::common(
                path_from_directions(&vec![L, R, L, L, R, L, L, L, L, L]),
                path_from_directions(&vec![L, R, L, L, R, L, R, R, R, R])
            )
        );
    }
}
