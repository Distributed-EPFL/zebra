use drop::crypto::hash;

use serde::Serialize;

use super::bytes::Bytes;
use super::map_id::MapId;
use super::node::Node;

#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) enum Label {
    Internal(Bytes),
    Leaf(MapId, Bytes),
    Empty,
}

impl Label {
    pub fn is_empty(&self) -> bool {
        *self == Label::Empty
    }

    pub fn bytes(&self) -> &Bytes {
        match self {
            Label::Internal(bytes) => bytes,
            Label::Leaf(_, bytes) => bytes,
            Label::Empty => {
                panic!("called `Label::bytes()` on an `Empty` value")
            }
        }
    }
}

pub(crate) fn label<Key, Value>(node: &Node<Key, Value>) -> Label
where
    Key: Serialize + Sync,
    Value: Serialize + Sync,
{
    match node {
        Node::Empty => Label::Empty,
        Node::Internal(..) => {
            Label::Internal(hash::hash(&node).unwrap().into())
        }
        Node::Leaf(..) => {
            let hash: Bytes = hash::hash(&node).unwrap().into();
            let map = MapId::read(&hash);
            Label::Leaf(map, hash)
        }
    }
}
