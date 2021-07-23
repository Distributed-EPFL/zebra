use crate::{common::store::Field, map::store::Wrap};

#[derive(Debug)]
pub(crate) enum Action<'a, Key: Field, Value: Field> {
    Get(Option<&'a Value>),
    Set(Wrap<Key>, Wrap<Value>),
    Remove,
}
