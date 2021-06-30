use crate::database::store::{Field, Node};

#[derive(Clone)]
pub(crate) struct Entry<Key: Field, Value: Field> {
    pub node: Node<Key, Value>,
    pub references: usize,
}
