use crate::{
    common::{
        store::Field,
        tree::{Path, Prefix},
    },
    map::{
        errors::TopologyError,
        store::{Internal, Leaf, Node},
    },
};

use doomstack::{here, Doom, ResultExt, Top};

fn check_internal<Key, Value>(internal: &Internal<Key, Value>) -> Result<(), Top<TopologyError>>
where
    Key: Field,
    Value: Field,
{
    match (internal.left(), internal.right()) {
        (Node::Empty, Node::Empty)
        | (Node::Empty, Node::Leaf(..))
        | (Node::Leaf(..), Node::Empty) => TopologyError::CompactnessViolation.fail().spot(here!()),
        _ => Ok(()),
    }
}

fn check_leaf<Key, Value>(
    leaf: &Leaf<Key, Value>,
    location: Prefix,
) -> Result<(), Top<TopologyError>>
where
    Key: Field,
    Value: Field,
{
    if !location.contains(&Path::from(leaf.key().digest())) {
        TopologyError::PathViolation.fail().spot(here!())
    } else {
        Ok(())
    }
}

fn recursion<Key, Value>(
    node: &Node<Key, Value>,
    location: Prefix,
) -> Result<(), Top<TopologyError>>
where
    Key: Field,
    Value: Field,
{
    match node {
        Node::Internal(internal) => {
            check_internal(internal)?;

            recursion(internal.left(), location.left())?;
            recursion(internal.right(), location.right())
        }
        Node::Leaf(leaf) => check_leaf(leaf, location),
        Node::Empty | Node::Stub(_) => Ok(()),
    }
}

pub(crate) fn check<Key, Value>(node: &Node<Key, Value>) -> Result<(), Top<TopologyError>>
where
    Key: Field,
    Value: Field,
{
    recursion(&node, Prefix::root())
}
