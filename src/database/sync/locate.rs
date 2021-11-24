use crate::{
    common::{
        store::Field,
        tree::{Path, Prefix},
    },
    database::store::{Label, Node, Store},
};

use std::collections::hash_map::Entry::{Occupied, Vacant};

enum Recursion {
    Recur(Label),
    Stop(Label, Label),
}

fn get_siblings<Key, Value>(store: &mut Store<Key, Value>, label: Label) -> (u8, (Label, Label))
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
            let (dive, (left, right)) = get_siblings(store, child);
            (dive + 1, (left, right))
        }
        Recursion::Stop(left, right) => (0, (left, right)),
    }
}

fn leaf_path<Key, Value>(store: &mut Store<Key, Value>, label: Label) -> Path
where
    Key: Field,
    Value: Field,
{
    match store.entry(label) {
        Occupied(entry) => match &entry.get().node {
            Node::Leaf(key, _) => Path::from(key.digest()),
            _ => unreachable!(),
        },
        Vacant(..) => unreachable!(),
    }
}

pub(crate) fn locate<Key, Value>(store: &mut Store<Key, Value>, label: Label) -> Prefix
where
    Key: Field,
    Value: Field,
{
    let (dive, (left, right)) = get_siblings(store, label);
    let common = Prefix::common(leaf_path(store, left), leaf_path(store, right));
    common.ancestor(dive)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        common::tree::Direction,
        database::interact::{apply, Batch},
    };

    #[test]
    fn tree() {
        use Direction::{Left as L, Right as R};

        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (mut store, root, _) = apply::apply(store, Label::Empty, batch);

        let l = store.fetch_label_at(root, Prefix::from_directions([L]));
        assert_eq!(locate(&mut store, l), Prefix::from_directions([L]));

        let r = store.fetch_label_at(root, Prefix::from_directions([R]));
        assert_eq!(locate(&mut store, r), Prefix::from_directions([R]));

        let ll = store.fetch_label_at(root, Prefix::from_directions([L, L]));
        assert_eq!(locate(&mut store, ll), Prefix::from_directions([L, L]));

        let lr = store.fetch_label_at(root, Prefix::from_directions([L, R]));
        assert_eq!(locate(&mut store, lr), Prefix::from_directions([L, R]));

        let rl = store.fetch_label_at(root, Prefix::from_directions([R, L]));
        assert_eq!(locate(&mut store, rl), Prefix::from_directions([R, L]));

        let rr = store.fetch_label_at(root, Prefix::from_directions([R, R]));
        assert_eq!(locate(&mut store, rr), Prefix::from_directions([R, R]));

        let lll = store.fetch_label_at(root, Prefix::from_directions([L, L, R]));
        assert_eq!(locate(&mut store, lll), Prefix::from_directions([L, L, R]));
    }

    #[test]
    fn full() {
        fn recursion(store: &mut Store<u32, u32>, prefix: Prefix, label: Label) {
            if !label.is_empty() {
                match store.fetch_node(label) {
                    Node::Internal(left, right) => {
                        assert_eq!(locate(store, label), prefix);
                        recursion(store, prefix.left(), left);
                        recursion(store, prefix.right(), right);
                    }
                    _ => {}
                }
            }
        }

        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (mut store, root, _) = apply::apply(store, Label::Empty, batch);

        recursion(&mut store, Prefix::root(), root);
    }
}
