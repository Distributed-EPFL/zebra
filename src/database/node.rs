use serde::Serialize;

use super::field::Field;
use super::label::Label;
use super::wrap::Wrap;

#[derive(Debug, Serialize)]
pub(crate) enum Node<Key: Field, Value: Field> {
    Empty,
    Internal(Label, Label),
    Leaf(Wrap<Key>, Wrap<Value>),
}

impl<Key, Value> Clone for Node<Key, Value>
where
    Key: Field,
    Value: Field,
{
    fn clone(&self) -> Self {
        match self {
            Node::Empty => Node::Empty,
            Node::Internal(left, right) => Node::Internal(*left, *right),
            Node::Leaf(key, value) => Node::Leaf(key.clone(), value.clone()),
        }
    }
}

impl<Key, Value> PartialEq for Node<Key, Value>
where
    Key: Field,
    Value: Field,
{
    fn eq(&self, rho: &Node<Key, Value>) -> bool {
        match (self, rho) {
            (Node::Empty, Node::Empty) => true,
            (
                Node::Internal(self_left, self_right),
                Node::Internal(rho_left, rho_right),
            ) => (self_left == rho_left) && (self_right == rho_right),
            (
                Node::Leaf(self_key, self_value),
                Node::Leaf(rho_key, rho_value),
            ) => (self_key == rho_key) && (self_value == rho_value),
            _ => false,
        }
    }
}

impl<Key, Value> Eq for Node<Key, Value>
where
    Key: Field,
    Value: Field,
{
}
