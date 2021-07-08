use crate::database::{
    store::{Field, Label, Node, Store},
    tree::{Path, Prefix},
};

use std::collections::hash_map::Entry::{Occupied, Vacant};

enum Recursion {
    Recur(Label),
    Stop(Label, Label),
}

fn get_siblings<Key, Value>(
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
    let (dive, (left, right)) = get_siblings(store, label);
    let common =
        Prefix::common(leaf_path(store, left), leaf_path(store, right));
    common.ancestor(dive)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::database::{
        interact::{apply, Batch, Operation},
        tree::Direction::{self, Left as L, Right as R},
    };

    fn get_recursive(
        store: &mut Store<u32, u32>,
        prefix: Prefix,
        label: Label,
    ) -> Label {
        let mut next = label;
        for i in prefix {
            next = match store.fetch_node(next) {
                Node::Internal(left, right) => {
                    if i == L {
                        left
                    } else {
                        right
                    }
                }
                _ => unreachable!(),
            };
        }

        next
    }

    fn op_set(key: u32, value: u32) -> Operation<u32, u32> {
        Operation::set(key, value).unwrap()
    }

    fn check_recursion(
        store: &mut Store<u32, u32>,
        prefix: Prefix,
        label: Label,
    ) {
        if !label.is_empty() {
            match store.fetch_node(label) {
                Node::Internal(left, right) => {
                    assert_eq!(locate(store, label), prefix);
                    check_recursion(store, prefix.left(), left);
                    check_recursion(store, prefix.right(), right);
                }
                _ => {}
            }
        }
    }

    fn prefix_from_directions(directions: &Vec<Direction>) -> Prefix {
        let mut prefix = Prefix::root();

        for &direction in directions {
            prefix = if direction == Direction::Left {
                prefix.left()
            } else {
                prefix.right()
            };
        }

        prefix
    }

    #[tokio::test]
    async fn tree() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| op_set(i, i)).collect());
        let (mut store, root, _) =
            apply::apply(store, Label::Empty, batch).await;

        let l =
            get_recursive(&mut store, prefix_from_directions(&vec![L]), root);
        assert_eq!(locate(&mut store, l), prefix_from_directions(&vec![L]));

        let r =
            get_recursive(&mut store, prefix_from_directions(&vec![R]), root);
        assert_eq!(locate(&mut store, r), prefix_from_directions(&vec![R]));

        let ll = get_recursive(
            &mut store,
            prefix_from_directions(&vec![L, L]),
            root,
        );
        assert_eq!(locate(&mut store, ll), prefix_from_directions(&vec![L, L]));

        let lr = get_recursive(
            &mut store,
            prefix_from_directions(&vec![L, R]),
            root,
        );
        assert_eq!(locate(&mut store, lr), prefix_from_directions(&vec![L, R]));

        let rl = get_recursive(
            &mut store,
            prefix_from_directions(&vec![R, L]),
            root,
        );
        assert_eq!(locate(&mut store, rl), prefix_from_directions(&vec![R, L]));

        let rr = get_recursive(
            &mut store,
            prefix_from_directions(&vec![R, R]),
            root,
        );
        assert_eq!(locate(&mut store, rr), prefix_from_directions(&vec![R, R]));

        let lll = get_recursive(
            &mut store,
            prefix_from_directions(&vec![L, L, R]),
            root,
        );
        assert_eq!(
            locate(&mut store, lll),
            prefix_from_directions(&vec![L, L, R])
        );
    }

    #[tokio::test]
    async fn full() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| op_set(i, i)).collect());
        let (mut store, root, _) =
            apply::apply(store, Label::Empty, batch).await;

        check_recursion(&mut store, Prefix::root(), root);
    }
}
