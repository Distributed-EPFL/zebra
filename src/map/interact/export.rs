use crate::{
    common::{
        store::Field,
        tree::{Direction, Path},
    },
    map::{
        errors::MapError,
        store::{Internal, Leaf, Node},
    },
};

use doomstack::{here, Doom, ResultExt, Top};

fn split(paths: &[Path], depth: u8) -> (&[Path], &[Path]) {
    let partition = paths.partition_point(|path| path[depth] == Direction::Right); // This is because `Direction::Right < Direction::Left`

    let right = &paths[..partition];
    let left = &paths[partition..];

    (left, right)
}

pub(crate) fn recur<Key, Value>(
    node: &Node<Key, Value>,
    depth: u8,
    paths: &[Path],
) -> Result<Node<Key, Value>, Top<MapError>>
where
    Key: Field + Clone,
    Value: Field + Clone,
{
    match node {
        Node::Internal(internal) if !paths.is_empty() => {
            let (left_paths, right_paths) = split(paths, depth);

            let left = recur(internal.left(), depth + 1, left_paths)?; // Clone relevant subtrees under `left`..
            let right = recur(internal.right(), depth + 1, right_paths)?; // .. and `right`

            Ok(Node::Internal(Internal::raw(internal.hash(), left, right))) // `internal.hash()` is guaranteed to be correct, no need to recompute
        }
        Node::Leaf(leaf) if !paths.is_empty() => Ok(Node::Leaf(Leaf::raw(
            // If some `path` in `paths` does not reach `leaf.key().digest()`,
            // then `leaf` is a proof of exclusion for `path`, and needs to be cloned
            leaf.hash(),
            leaf.key().clone(),
            leaf.value().clone(),
        ))),
        Node::Stub(_) if !paths.is_empty() => MapError::BranchUnknown.fail().spot(here!()),

        Node::Empty => Ok(Node::Empty), // `Node::Empty` is cheaper to clone than `Node::Stub`

        node => Ok(Node::stub(node.hash())),
    }
}

pub(crate) fn export<Key, Value>(
    root: &Node<Key, Value>,
    paths: &[Path],
) -> Result<Node<Key, Value>, Top<MapError>>
where
    Key: Field + Clone,
    Value: Field + Clone,
{
    recur(root, 0, paths)
}
