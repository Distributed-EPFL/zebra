use crate::{
    common::store::Field,
    map::{errors::MapError, store::Node},
};

use doomstack::{here, Doom, ResultExt, Top};

fn recur<Key, Value>(destination: &mut Node<Key, Value>, source: Node<Key, Value>)
where
    Key: Field,
    Value: Field,
{
    match (destination, source) {
        (destination, source) if destination.is_stub() => {
            *destination = source;
        }
        (Node::Internal(destination), Node::Internal(source)) => {
            let (source_left, source_right) = source.children();
            recur(destination.left_mut(), source_left);
            recur(destination.right_mut(), source_right);
        }
        _ => (),
    }
}

pub(crate) fn import<Key, Value>(
    destination_root: &mut Node<Key, Value>,
    source_root: Node<Key, Value>,
) -> Result<(), Top<MapError>>
where
    Key: Field,
    Value: Field,
{
    if source_root.hash() == destination_root.hash() {
        recur(destination_root, source_root);
        Ok(())
    } else {
        MapError::MapIncompatible.fail().spot(here!())
    }
}
