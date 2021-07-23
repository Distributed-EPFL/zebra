use crate::{common::store::Field, map::store::Wrap};

#[derive(Debug)]
pub(crate) enum Action<Key: Field, Value: Field> {
    Get,
    Set(Wrap<Key>, Wrap<Value>),
    Remove,
}
