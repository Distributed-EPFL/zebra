use crate::{
    common::{
        store::Field,
        tree::{Direction, Path},
    },
    map::{
        interact::{Action, Operation},
        store::Node,
    },
};

use std::rc::Rc;

fn branch<Key, Value>(
    left: Box<Node<Key, Value>>,
    right: Box<Node<Key, Value>>,
    depth: u8,
    operation: Operation<Key, Value>,
) -> (Box<Node<Key, Value>>, Result<Option<Rc<Value>>, ()>)
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

    let node = match (&*left, &*right) {
        (Node::Empty, Node::Empty) => left,
        (Node::Leaf { .. }, Node::Empty) => left,
        (Node::Empty, Node::Leaf { .. }) => right,
        _ => Node::internal(left, right),
    };

    (node, get)
}

fn recur<Key, Value>(
    node: Box<Node<Key, Value>>,
    depth: u8,
    operation: Operation<Key, Value>,
) -> (Box<Node<Key, Value>>, Result<Option<Rc<Value>>, ()>)
where
    Key: Field,
    Value: Field,
{
    match (*node, operation) {
        (
            Node::Empty,
            Operation {
                action: Action::Get,
                ..
            },
        ) => (Node::empty(), Ok(None)),
        (
            Node::Empty,
            Operation {
                action: Action::Set(key, value),
                ..
            },
        ) => (Node::leaf(key, value), Ok(None)),
        (
            Node::Empty,
            Operation {
                action: Action::Remove,
                ..
            },
        ) => (Node::empty(), Ok(None)),

        (Node::Internal { left, right, .. }, operation) => {
            branch(left, right, depth, operation)
        }

        (
            Node::Leaf {
                key,
                value: original_value,
                ..
            },
            Operation { path, action },
        ) if path.reaches(*key.digest()) => match action {
            Action::Get => {
                let get = Some(original_value.inner().clone());
                let node = Node::leaf(key, original_value);

                (node, Ok(get))
            }
            Action::Set(_, new_value) => (Node::leaf(key, new_value), Ok(None)),
            Action::Remove => (Node::empty(), Ok(None)),
        },
        (
            Node::Leaf { key, value, .. },
            Operation {
                action: Action::Get,
                ..
            },
        ) => (Node::leaf(key, value), Ok(None)),
        (Node::Leaf { key, value, .. }, operation) => {
            if Path::from(*key.digest())[depth] == Direction::Left {
                branch(Node::leaf(key, value), Node::empty(), depth, operation)
            } else {
                branch(Node::empty(), Node::leaf(key, value), depth, operation)
            }
        }

        (Node::Stub { hash }, _) => (Node::stub(hash), Err(())),
    }
}

pub(crate) fn apply<Key, Value>(
    root: Box<Node<Key, Value>>,
    operation: Operation<Key, Value>,
) -> (Box<Node<Key, Value>>, Result<Option<Rc<Value>>, ()>)
// TODO: Fill `Err` with appropriate error type
where
    Key: Field,
    Value: Field,
{
    recur(root, 0, operation)
}
