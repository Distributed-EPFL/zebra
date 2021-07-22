use crate::common::{
    data::{bytes::EMPTY, Bytes},
    store::Field,
};

use drop::crypto::hash;
use drop::crypto::hash::HashError;

pub(crate) enum Node<Key, Value>
where
    Key: Field,
    Value: Field,
{
    Empty,
    Internal(Bytes, Box<Node<Key, Value>>, Box<Node<Key, Value>>),
    Leaf(Bytes, Key, Value),
    Stub(Bytes),
}

impl<Key, Value> Node<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub fn empty() -> Self {
        Node::Empty
    }

    pub fn internal(
        left: Box<Node<Key, Value>>,
        right: Box<Node<Key, Value>>,
    ) -> Self {
        let hash = hash::hash(&(left.hash(), right.hash())).unwrap().into();
        Node::Internal(hash, left, right)
    }

    pub fn leaf(key: Key, value: Value) -> Result<Self, HashError> {
        let key_hash: Bytes = hash::hash(&key)?.into();
        let value_hash: Bytes = hash::hash(&value)?.into();

        let hash = hash::hash(&(key_hash, value_hash)).unwrap().into();
        Ok(Node::Leaf(hash, key, value))
    }

    pub fn stub(hash: Bytes) -> Self {
        Node::Stub(hash)
    }

    pub fn hash(&self) -> Bytes {
        match self {
            Node::Empty => EMPTY,
            Node::Internal(hash, ..) => *hash,
            Node::Leaf(hash, ..) => *hash,
            Node::Stub(hash) => *hash,
        }
    }
}
