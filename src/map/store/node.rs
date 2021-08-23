use crate::{
    common::{
        data::Bytes,
        store::{hash, Field},
    },
    map::store::Wrap,
};

pub(crate) enum Node<Key: Field, Value: Field> {
    Empty,
    Internal(Internal<Key, Value>),
    Leaf(Leaf<Key, Value>),
    Stub(Stub),
}

pub(crate) struct Internal<Key: Field, Value: Field> {
    hash: Bytes,
    left: Box<Node<Key, Value>>,
    right: Box<Node<Key, Value>>,
}

pub(crate) struct Leaf<Key: Field, Value: Field> {
    hash: Bytes,
    key: Wrap<Key>,
    value: Wrap<Value>,
}

pub(crate) struct Stub {
    hash: Bytes,
}

impl<Key, Value> Internal<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub fn new(left: Node<Key, Value>, right: Node<Key, Value>) -> Self {
        let hash = hash::internal(left.hash(), right.hash());
        Internal {
            hash,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    pub fn hash(&self) -> Bytes {
        self.hash
    }

    pub fn children(self) -> (Node<Key, Value>, Node<Key, Value>) {
        (*self.left, *self.right)
    }
}

impl<Key, Value> Leaf<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub fn new(key: Wrap<Key>, value: Wrap<Value>) -> Self {
        let hash = hash::leaf(*key.digest(), *value.digest());
        Leaf { hash, key, value }
    }

    pub fn hash(&self) -> Bytes {
        self.hash
    }

    pub fn fields(self) -> (Wrap<Key>, Wrap<Value>) {
        (self.key, self.value)
    }

    pub fn key(&self) -> &Wrap<Key> {
        &self.key
    }

    pub fn value(&self) -> &Wrap<Value> {
        &self.value
    }
}

impl Stub {
    pub fn new(hash: Bytes) -> Self {
        Stub { hash }
    }

    pub fn hash(&self) -> Bytes {
        self.hash
    }
}

impl<Key, Value> Node<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub fn internal(left: Node<Key, Value>, right: Node<Key, Value>) -> Self {
        Node::Internal(Internal::new(left, right))
    }

    pub fn leaf(key: Wrap<Key>, value: Wrap<Value>) -> Self {
        Node::Leaf(Leaf::new(key, value))
    }

    pub fn stub(hash: Bytes) -> Self {
        Node::Stub(Stub::new(hash))
    }

    pub fn hash(&self) -> Bytes {
        match self {
            Node::Empty => hash::empty(),
            Node::Internal(internal) => internal.hash(),
            Node::Leaf(leaf) => leaf.hash(),
            Node::Stub(stub) => stub.hash(),
        }
    }
}
