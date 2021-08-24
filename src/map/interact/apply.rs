use crate::{
    common::{
        store::Field,
        tree::{Direction, Path},
    },
    map::{
        errors::MapError,
        interact::{Action, Operation},
        store::Node,
    },
};

use std::rc::Rc;

fn branch<Key, Value>(
    left: Node<Key, Value>,
    right: Node<Key, Value>,
    depth: u8,
    operation: Operation<Key, Value>,
) -> (Node<Key, Value>, Result<Option<Rc<Value>>, MapError>)
where
    Key: Field,
    Value: Field,
{
    let (left, right, get) = if operation.path[depth] == Direction::Left {
        let (left, get) = recur(left, depth + 1, operation);
        (left, right, get)
    } else {
        let (right, get) = recur(right, depth + 1, operation);
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
    operation: Operation<Key, Value>,
) -> (Node<Key, Value>, Result<Option<Rc<Value>>, MapError>)
where
    Key: Field,
    Value: Field,
{
    match (node, operation) {
        (
            Node::Empty,
            Operation {
                action: Action::Get | Action::Remove,
                ..
            },
        ) => (Node::Empty, Ok(None)),
        (
            Node::Empty,
            Operation {
                action: Action::Set(key, value),
                ..
            },
        ) => (Node::leaf(key, value), Ok(None)),

        (Node::Internal(internal), operation) => {
            let (left, right) = internal.children();
            branch(left, right, depth, operation)
        }

        (
            Node::Leaf(leaf),
            Operation {
                path,
                action: Action::Get,
            },
        ) if path.reaches(*leaf.key().digest()) => {
            let get = Some(leaf.value().inner().clone());
            (Node::Leaf(leaf), Ok(get))
        }
        (
            Node::Leaf(leaf),
            Operation {
                path,
                action: Action::Set(_, value),
            },
        ) if path.reaches(*leaf.key().digest()) => {
            let (key, _) = leaf.fields();
            (Node::leaf(key, value), Ok(None))
        }
        (
            Node::Leaf(leaf),
            Operation {
                path,
                action: Action::Remove,
            },
        ) if path.reaches(*leaf.key().digest()) => (Node::Empty, Ok(None)),
        (
            Node::Leaf(leaf),
            Operation {
                action: Action::Get | Action::Remove,
                ..
            },
        ) => (Node::Leaf(leaf), Ok(None)),
        (Node::Leaf(leaf), operation) => {
            if Path::from(*leaf.key().digest())[depth] == Direction::Left {
                branch(Node::Leaf(leaf), Node::Empty, depth, operation)
            } else {
                branch(Node::Empty, Node::Leaf(leaf), depth, operation)
            }
        }

        (Node::Stub(stub), _) => {
            (Node::Stub(stub), Err(MapError::BranchUnknown))
        }
    }
}

pub(crate) fn apply<Key, Value>(
    root: Node<Key, Value>,
    operation: Operation<Key, Value>,
) -> (Node<Key, Value>, Result<Option<Rc<Value>>, MapError>)
where
    Key: Field,
    Value: Field,
{
    recur(root, 0, operation)
}
