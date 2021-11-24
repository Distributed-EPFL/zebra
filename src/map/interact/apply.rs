use crate::{
    common::{
        store::Field,
        tree::{Direction, Path},
    },
    map::{
        errors::MapError,
        interact::{Action, Update},
        store::Node,
    },
};

use doomstack::{here, Doom, ResultExt, Top};

fn branch<Key, Value>(
    left: Node<Key, Value>,
    right: Node<Key, Value>,
    depth: u8,
    update: Update<Key, Value>,
) -> (Node<Key, Value>, Result<Option<Value>, Top<MapError>>)
where
    Key: Field,
    Value: Field,
{
    let (left, right, get) = if update.path[depth] == Direction::Left {
        let (left, get) = recur(left, depth + 1, update);
        (left, right, get)
    } else {
        let (right, get) = recur(right, depth + 1, update);
        (left, right, get)
    };

    let node = match (&left, &right) {
        (Node::Empty, Node::Empty) => Node::Empty,
        (Node::Leaf { .. }, Node::Empty) => left,
        (Node::Empty, Node::Leaf { .. }) => right,
        _ => Node::internal(left, right),
    };

    (node, get)
}

fn recur<Key, Value>(
    node: Node<Key, Value>,
    depth: u8,
    update: Update<Key, Value>,
) -> (Node<Key, Value>, Result<Option<Value>, Top<MapError>>)
where
    Key: Field,
    Value: Field,
{
    match (node, update) {
        (
            Node::Empty,
            Update {
                action: Action::Remove,
                ..
            },
        ) => (Node::Empty, Ok(None)),
        (
            Node::Empty,
            Update {
                action: Action::Insert(key, value),
                ..
            },
        ) => (Node::leaf(key, value), Ok(None)),

        (Node::Internal(internal), update) => {
            let (left, right) = internal.children();
            branch(left, right, depth, update)
        }

        (
            Node::Leaf(leaf),
            Update {
                path,
                action: Action::Insert(_, new_value),
            },
        ) if path.reaches(leaf.key().digest()) => {
            let (key, old_value) = leaf.fields();
            (Node::leaf(key, new_value), Ok(Some(old_value.take())))
        }
        (
            Node::Leaf(leaf),
            Update {
                path,
                action: Action::Remove,
            },
        ) if path.reaches(leaf.key().digest()) => (Node::Empty, Ok(Some(leaf.fields().1.take()))),
        (
            Node::Leaf(leaf),
            Update {
                action: Action::Remove,
                ..
            },
        ) => (Node::Leaf(leaf), Ok(None)),
        (Node::Leaf(leaf), update) => {
            if Path::from(leaf.key().digest())[depth] == Direction::Left {
                branch(Node::Leaf(leaf), Node::Empty, depth, update)
            } else {
                branch(Node::Empty, Node::Leaf(leaf), depth, update)
            }
        }

        (Node::Stub(stub), _) => (
            Node::Stub(stub),
            MapError::BranchUnknown.fail().spot(here!()),
        ),
    }
}

pub(crate) fn apply<Key, Value>(
    root: Node<Key, Value>,
    update: Update<Key, Value>,
) -> (Node<Key, Value>, Result<Option<Value>, Top<MapError>>)
where
    Key: Field,
    Value: Field,
{
    recur(root, 0, update)
}
