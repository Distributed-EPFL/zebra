use crate::database::store::{Field, Node};

use std::vec::Vec;

pub struct Answer<Key: Field, Value: Field>(pub(crate) Vec<Node<Key, Value>>);
