use crate::{common::store::Field, map::store::Node};

pub struct Map<Key, Value>
where
    Key: Field,
    Value: Field,
{
    root: Node<Key, Value>,
}

impl<Key, Value> Map<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub fn new() -> Self {
        Map { root: Node::Empty }
    }
}
