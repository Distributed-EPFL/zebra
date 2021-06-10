use std::ops::Index;

use super::direction::Direction;
use super::path::Path;

#[derive(Debug, Clone, Copy)]
pub(crate) struct Prefix {
    path: Path,
    depth: u8,
}

impl Prefix {
    pub fn new(path: Path, depth: u8) -> Self {
        Prefix { path, depth }
    }

    pub fn root() -> Self {
        Prefix {
            path: Path::empty(),
            depth: 0,
        }
    }

    pub fn depth(&self) -> u8 {
        self.depth
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
