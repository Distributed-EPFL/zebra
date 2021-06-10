use serde::Serialize;

use super::node::Node;

pub(crate) struct Entry<Key: Serialize, Value: Serialize> {
    pub node: Node<Key, Value>,
    pub references: usize,
}
