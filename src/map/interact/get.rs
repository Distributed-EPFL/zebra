use crate::{
    common::{store::Field, tree::Direction},
    map::{errors::MapError, interact::Query, store::Node},
};

use doomstack::{here, Doom, ResultExt, Top};

fn recur<Key, Value>(
    node: &Node<Key, Value>,
    depth: u8,
    query: Query,
) -> Result<Option<&Value>, Top<MapError>>
where
    Key: Field,
    Value: Field,
{
    match node {
        Node::Empty => Ok(None),
        Node::Internal(internal) => {
            if query.path[depth] == Direction::Left {
                recur(internal.left(), depth + 1, query)
            } else {
                recur(internal.right(), depth + 1, query)
            }
        }
        Node::Leaf(leaf) => {
            if query.path.reaches(leaf.key().digest()) {
                Ok(Some(leaf.value().inner()))
            } else {
                Ok(None)
            }
        }
        Node::Stub(_) => MapError::BranchUnknown.fail().spot(here!()),
    }
}

pub(crate) fn get<Key, Value>(
    root: &Node<Key, Value>,
    query: Query,
) -> Result<Option<&Value>, Top<MapError>>
where
    Key: Field,
    Value: Field,
{
    recur(root, 0, query)
}
