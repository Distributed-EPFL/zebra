use crate::database::{
    store::{Field, Label, Node, Store},
    tree::{Path, Prefix},
};

use std::collections::hash_map::Entry::{Occupied, Vacant};

enum Recursion {
    Recur(Label),
    Stop(Label, Label),
}

fn recur<Key, Value>(
    store: &mut Store<Key, Value>,
    label: Label,
) -> (u8, (Label, Label))
where
    Key: Field,
    Value: Field,
{
    let recursion = match store.entry(label) {
        Occupied(entry) => match entry.get().node {
            Node::Internal(Label::Internal(map, hash), _)
            | Node::Internal(_, Label::Internal(map, hash)) => {
                Recursion::Recur(Label::Internal(map, hash))
            }
            Node::Internal(left, right) => Recursion::Stop(left, right),
            _ => panic!("called `locate` on a non-`Internal` node"),
        },
        Vacant(..) => unreachable!(),
    };

    match recursion {
        Recursion::Recur(child) => {
            let (dive, (left, right)) = recur(store, child);
            (dive + 1, (left, right))
        }
        Recursion::Stop(left, right) => (0, (left, right)),
    }
}

fn path<Key, Value>(store: &mut Store<Key, Value>, label: Label) -> Path
where
    Key: Field,
    Value: Field,
{
    match store.entry(label) {
        Occupied(entry) => match &entry.get().node {
            Node::Leaf(key, _) => Path::from(*key.digest()),
            _ => unreachable!(),
        },
        Vacant(..) => unreachable!(),
    }
}

pub(crate) fn locate<Key, Value>(
    store: &mut Store<Key, Value>,
    label: Label,
) -> Prefix
where
    Key: Field,
    Value: Field,
{
    let (dive, (left, right)) = recur(store, label);
    let common = Prefix::common(path(store, left), path(store, right));
    common.ancestor(dive)
}
