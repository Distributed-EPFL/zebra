use crate::{
    common::{
        data::Bytes,
        store::{hash, Field},
    },
    map::store::Wrap,
};

pub(crate) enum Node<Key: Field, Value: Field> {
    Empty,
    Internal {
        hash: Bytes,
        left: Box<Node<Key, Value>>,
        right: Box<Node<Key, Value>>,
    },
    Leaf {
        hash: Bytes,
        key: Wrap<Key>,
        value: Wrap<Value>,
    },
    Stub {
        hash: Bytes,
    },
}

impl<Key, Value> Node<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub fn empty() -> Box<Self> {
        Box::new(Node::Empty)
    }

    pub fn internal(
        left: Box<Node<Key, Value>>,
        right: Box<Node<Key, Value>>,
    ) -> Box<Self> {
        let hash = hash::internal(left.hash(), right.hash());
        Box::new(Node::Internal { hash, left, right })
    }

    pub fn leaf(key: Wrap<Key>, value: Wrap<Value>) -> Box<Self> {
        let hash = hash::leaf(*key.digest(), *value.digest());
        Box::new(Node::Leaf { hash, key, value })
    }

    pub fn stub(hash: Bytes) -> Box<Self> {
        Box::new(Node::Stub { hash })
    }

    pub fn hash(&self) -> Bytes {
        match self {
            Node::Empty => hash::empty(),
            Node::Internal { hash, .. } => *hash,
            Node::Leaf { hash, .. } => *hash,
            Node::Stub { hash } => *hash,
        }
    }
}
