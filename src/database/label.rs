use drop::crypto::hash;

use serde::Serialize;

use super::bytes::Bytes;
use super::node::Node;

#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) enum Label {
    Internal(Bytes),
    Leaf(Bytes),
    Empty,
}

impl Label {
    pub fn is_empty(&self) -> bool {
        *self == Label::Empty
    }

    pub fn bytes(&self) -> &Bytes {
        match self {
            Label::Internal(bytes) => bytes,
            Label::Leaf(bytes) => bytes,
            Label::Empty => {
                panic!("called `Label::bytes()` on an `Empty` value")
            }
        }
    }
}

pub(crate) fn label<Key, Value>(node: &Node<Key, Value>) -> Label
where
    Key: Serialize,
    Value: Serialize,
{
    match node {
        Node::Empty => Label::Empty,
        Node::Internal(..) => {
            Label::Internal(hash::hash(&node).unwrap().into())
        }
        Node::Leaf(..) => Label::Leaf(hash::hash(&node).unwrap().into()),
    }
}
