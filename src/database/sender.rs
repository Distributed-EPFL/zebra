use crate::database::store::{Field, Handle};

pub struct Sender<Key: Field, Value: Field>(Handle<Key, Value>);

impl<Key, Value> Sender<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub(crate) fn new(handle: Handle<Key, Value>) -> Self {
        Sender(handle)
    }
}
