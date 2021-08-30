use crate::{common::store::Field, map::store::Wrap};

#[derive(Debug)]
pub(crate) enum Action<Key: Field, Value: Field> {
    Insert(Wrap<Key>, Wrap<Value>),
    Remove,
}
