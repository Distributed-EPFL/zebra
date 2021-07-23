use crate::{common::store::Field, map::store::Wrap};

use std::rc::Rc;

#[derive(Debug)]
pub(crate) enum Action<Key: Field, Value: Field> {
    Get(Option<Rc<Value>>),
    Set(Wrap<Key>, Wrap<Value>),
    Remove,
}
