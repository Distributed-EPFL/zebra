use crate::database::store::{Field, Store};

pub(crate) enum Split<Key: Field, Value: Field> {
    Split(Store<Key, Value>, Store<Key, Value>),
    Unsplittable(Store<Key, Value>),
}
