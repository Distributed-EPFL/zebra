use crate::{
    common::store::Field,
    database::store::{Label, Node, Split, Store, Wrap},
};

use std::collections::{
    hash_map::Entry::{Occupied, Vacant},
    LinkedList,
};

fn get<Key, Value>(store: &mut Store<Key, Value>, label: Label) -> Node<Key, Value>
where
    Key: Field,
    Value: Field,
{
    if !label.is_empty() {
        match store.entry(label) {
            Occupied(entry) => {
                let value = entry.get();
                value.node.clone()
            }
            Vacant(..) => unreachable!(),
        }
    } else {
        Node::Empty
    }
}

pub(crate) fn branch<Key, Value>(
    store: Store<Key, Value>,
    lho_recursion: Option<(Label, Label)>,
    rho_recursion: Option<(Label, Label)>,
) -> (
    Store<Key, Value>,
    LinkedList<(Wrap<Key>, Wrap<Value>)>,
    LinkedList<(Wrap<Key>, Wrap<Value>)>,
)
where
    Key: Field,
    Value: Field,
{
    let (lho_left, lho_right) = match lho_recursion {
        Some((lho_left, lho_right)) => (Some(lho_left), Some(lho_right)),
        None => (None, None),
    };

    let (rho_left, rho_right) = match rho_recursion {
        Some((rho_left, rho_right)) => (Some(rho_left), Some(rho_right)),
        None => (None, None),
    };

    let (
        store,
        mut left_lho_candidates,
        mut left_rho_candidates,
        mut right_lho_candidates,
        mut right_rho_candidates,
    ) = match store.split() {
        Split::Split(left_store, right_store) => {
            let (
                (left_store, left_lho_candidates, left_rho_candidates),
                (right_store, right_lho_candidates, right_rho_candidates),
            ) = rayon::join(
                move || recur(left_store, lho_left, rho_left),
                move || recur(right_store, lho_right, rho_right),
            );

            let store = Store::merge(left_store, right_store);

            (
                store,
                left_lho_candidates,
                left_rho_candidates,
                right_lho_candidates,
                right_rho_candidates,
            )
        }
        Split::Unsplittable(store) => {
            let (store, left_lho_candidates, left_rho_candidates) =
                recur(store, lho_left, rho_left);

            let (store, right_lho_candidates, right_rho_candidates) =
                recur(store, lho_right, rho_right);

            (
                store,
                left_lho_candidates,
                left_rho_candidates,
                right_lho_candidates,
                right_rho_candidates,
            )
        }
    };

    let mut lho_candidates = LinkedList::new();
    lho_candidates.append(&mut left_lho_candidates);
    lho_candidates.append(&mut right_lho_candidates);

    let mut rho_candidates = LinkedList::new();
    rho_candidates.append(&mut left_rho_candidates);
    rho_candidates.append(&mut right_rho_candidates);

    (store, lho_candidates, rho_candidates)
}

pub(crate) fn recur<Key, Value>(
    mut store: Store<Key, Value>,
    lho_node: Option<Label>,
    rho_node: Option<Label>,
) -> (
    Store<Key, Value>,
    LinkedList<(Wrap<Key>, Wrap<Value>)>,
    LinkedList<(Wrap<Key>, Wrap<Value>)>,
)
where
    Key: Field,
    Value: Field,
{
    if lho_node != rho_node {
        let mut lho_collector = LinkedList::new();
        let mut rho_collector = LinkedList::new();

        let lho_recursion = lho_node.and_then(|node| match get(&mut store, node) {
            Node::Internal(left, right) => Some((left, right)),
            Node::Leaf(key, value) => {
                lho_collector.push_back((key, value));
                None
            }
            Node::Empty => None,
        });

        let rho_recursion = rho_node.and_then(|node| match get(&mut store, node) {
            Node::Internal(left, right) => Some((left, right)),
            Node::Leaf(key, value) => {
                rho_collector.push_back((key, value));
                None
            }
            Node::Empty => None,
        });

        let store = if lho_recursion.is_some() || rho_recursion.is_some() {
            let (store, mut lho_candidates, mut rho_candidates) =
                branch(store, lho_recursion, rho_recursion);

            lho_collector.append(&mut lho_candidates);
            rho_collector.append(&mut rho_candidates);

            store
        } else {
            store
        };

        (store, lho_collector, rho_collector)
    } else {
        (store, LinkedList::new(), LinkedList::new())
    }
}

pub(crate) fn diff<Key, Value>(
    store: Store<Key, Value>,
    lho_root: Label,
    rho_root: Label,
) -> (
    Store<Key, Value>,
    LinkedList<(Wrap<Key>, Wrap<Value>)>,
    LinkedList<(Wrap<Key>, Wrap<Value>)>,
)
where
    Key: Field,
    Value: Field,
{
    recur(store, Some(lho_root), Some(rho_root))
}
