use crate::{
    common::{
        data::{bytes::EMPTY, Bytes},
        store::Field,
    },
    map::store::Wrap,
};

use drop::crypto::hash;
use drop::crypto::hash::HashError;

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
        let hash = hash::hash(&(left.hash(), right.hash())).unwrap().into();
        Box::new(Node::Internal { hash, left, right })
    }

    pub fn leaf(key: Key, value: Value) -> Result<Box<Self>, HashError> {
        let key = Wrap::new(key)?;
        let value = Wrap::new(value)?;

        let hash = hash::hash(&(*key.digest(), *value.digest()))
            .unwrap()
            .into();

        Ok(Box::new(Node::Leaf { hash, key, value }))
    }

    pub fn stub(hash: Bytes) -> Box<Self> {
        Box::new(Node::Stub { hash })
    }

    pub fn hash(&self) -> Bytes {
        match self {
            Node::Empty => EMPTY,
            Node::Internal { hash, .. } => *hash,
            Node::Leaf { hash, .. } => *hash,
            Node::Stub { hash } => *hash,
        }
    }
}
