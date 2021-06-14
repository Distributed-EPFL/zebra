use serde::Serialize;

use super::node::Node;

#[derive(Clone)]
pub(crate) struct Entry<Key: Serialize + Sync, Value: Serialize + Sync> {
    pub node: Node<Key, Value>,
    pub references: usize,
}
