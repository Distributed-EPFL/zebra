use crate::{
    common::{
        store::Field,
        tree::{Direction, Path},
    },
    database::store::{Label, Node, Split, Store},
    map::store::{Internal as MapInternal, Leaf as MapLeaf, Node as MapNode, Wrap as MapWrap},
};

use oh_snap::Snap;

use std::collections::hash_map::Entry::{Occupied, Vacant};

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

fn split(paths: Snap<Path>, depth: u8) -> (Snap<Path>, Snap<Path>) {
    let partition = paths.partition_point(|path| path[depth] == Direction::Right); // This is because `Direction::Right < Direction::Left`

    let (right, left) = paths.snap(partition);
    (left, right)
}

fn branch<Key, Value>(
    store: Store<Key, Value>,
    depth: u8,
    paths: Snap<Path>,
    left: Label,
    right: Label,
) -> (Store<Key, Value>, MapNode<Key, Value>, MapNode<Key, Value>)
where
    Key: Field + Clone,
    Value: Field + Clone,
{
    let (left_paths, right_paths) = split(paths, depth);

    match store.split() {
        Split::Split(left_store, right_store) => {
            let ((left_store, left), (right_store, right)) = rayon::join(
                move || recur(left_store, left, depth + 1, left_paths),
                move || recur(right_store, right, depth + 1, right_paths),
            );

            let store = Store::merge(left_store, right_store);
            (store, left, right)
        }
        Split::Unsplittable(store) => {
            let (store, left) = recur(store, left, depth + 1, left_paths);
            let (store, right) = recur(store, right, depth + 1, right_paths);

            (store, left, right)
        }
    }
}

fn recur<Key, Value>(
    mut store: Store<Key, Value>,
    node: Label,
    depth: u8,
    paths: Snap<Path>,
) -> (Store<Key, Value>, MapNode<Key, Value>)
where
    Key: Field + Clone,
    Value: Field + Clone,
{
    let hash = node.hash();

    match get(&mut store, node) {
        Node::Internal(left, right) if !paths.is_empty() => {
            let (store, left, right) = branch(store, depth, paths, left, right);

            (
                store,
                MapNode::Internal(MapInternal::raw(hash, left, right)),
            )
        }
        Node::Leaf(key, value) if !paths.is_empty() => {
            let key = MapWrap::raw(key.digest(), (**key.inner()).clone());
            let value = MapWrap::raw(value.digest(), (**value.inner()).clone());

            (store, MapNode::Leaf(MapLeaf::raw(hash, key, value)))
        }

        Node::Empty => (store, MapNode::Empty),

        node => (store, MapNode::stub(node.hash())),
    }
}

pub(crate) fn export<Key, Value>(
    store: Store<Key, Value>,
    root: Label,
    paths: Snap<Path>,
) -> (Store<Key, Value>, MapNode<Key, Value>)
where
    Key: Field + Clone,
    Value: Field + Clone,
{
    recur(store, root, 0, paths)
}
