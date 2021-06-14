use serde::Serialize;

use super::node::Node;

#[derive(Clone)]
pub(crate) struct Entry<
    Key: 'static + Serialize + Send + Sync,
    Value: 'static + Serialize + Send + Sync,
> {
    pub node: Node<Key, Value>,
    pub references: usize,
}
