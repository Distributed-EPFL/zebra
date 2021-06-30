use crate::database::store::{Field, Label, Node, Store};

pub(crate) fn drop<Key, Value>(store: &mut Store<Key, Value>, label: Label)
where
    Key: Field,
    Value: Field,
{
    match store.decref(label, false) {
        Some(Node::Internal(left, right)) => {
            drop(store, left);
            drop(store, right);
        }
        _ => (),
    }
}
