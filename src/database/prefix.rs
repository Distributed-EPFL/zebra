use super::direction::Direction;
use super::path::Path;

#[derive(Debug, Clone, Copy)]
pub(super) struct Prefix {
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
