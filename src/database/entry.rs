use serde::Serialize;

use super::node::Node;

#[derive(Clone)]
pub(crate) struct Entry<Key: Serialize, Value: Serialize> {
    pub node: Node<Key, Value>,
    pub references: usize,
}
