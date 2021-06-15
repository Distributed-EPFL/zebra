use super::field::Field;
use super::node::Node;

#[derive(Clone)]
pub(crate) struct Entry<Key: Field, Value: Field> {
    pub node: Node<Key, Value>,
    pub references: usize,
}
