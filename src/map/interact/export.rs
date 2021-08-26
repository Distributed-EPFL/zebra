use crate::{
    common::{
        data::Bytes,
        store::Field,
        tree::{Direction, Path},
    },
    map::{
        errors::{BranchUnknown, HashError, MapError},
        store::{Internal, Leaf, Node},
    },
};

use drop::crypto::hash;

use snafu::ResultExt;

use std::borrow::Borrow;

fn split(depth: u8, paths: &[Path]) -> (&[Path], &[Path]) {
    let partition =
        paths.partition_point(|path| path[depth] == Direction::Right); // This is because `Direction::Right < Direction::Left`

    let right = &paths[..partition];
    let left = &paths[partition..];

    (left, right)
}

pub(crate) fn recur<Key, Value>(
    node: &Node<Key, Value>,
    depth: u8,
    paths: &[Path],
) -> Result<Node<Key, Value>, MapError>
where
    Key: Field + Clone,
    Value: Field + Clone,
{
    match node {
        Node::Internal(internal) if !paths.is_empty() => {
            let (left_paths, right_paths) = split(depth, paths);

            let left = recur(internal.left(), depth + 1, left_paths)?;
            let right = recur(internal.right(), depth + 1, right_paths)?;

            Ok(Node::Internal(Internal::raw(internal.hash(), left, right)))
        }
        Node::Leaf(leaf) if !paths.is_empty() => Ok(Node::Leaf(Leaf::raw(
            leaf.hash(),
            leaf.key().clone(),
            leaf.value().clone(),
        ))),
        Node::Stub(_) if !paths.is_empty() => BranchUnknown.fail(),

        Node::Empty => Ok(Node::Empty),

        node => Ok(Node::stub(node.hash())),
    }
}

pub(crate) fn export<Key, Value, I, K>(
    root: &Node<Key, Value>,
    keys: I,
) -> Result<Node<Key, Value>, MapError>
where
    Key: Field + Clone,
    Value: Field + Clone,
    I: IntoIterator<Item = K>,
    K: Borrow<Key>,
{
    let paths: Result<Vec<Path>, MapError> = keys
        .into_iter()
        .map(|key| {
            hash::hash(key.borrow())
                .map(|digest| Path::from(Bytes::from(digest)))
                .context(HashError)
        })
        .collect();

    let mut paths = paths?;
    paths.sort();

    recur(root, 0, &paths)
}
