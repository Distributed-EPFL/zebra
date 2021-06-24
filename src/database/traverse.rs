use async_recursion::async_recursion;

use std::collections::hash_map::Entry::{Occupied, Vacant};

use super::action::Action;
use super::batch::Batch;
use super::chunk::Chunk;
use super::direction::Direction;
use super::entry::Entry as StoreEntry;
use super::field::Field;
use super::label::Label;
use super::node::Node;
use super::operation::Operation;
use super::path::Path;
use super::store::{Split, Store};
use super::task::Task;

#[derive(Eq, PartialEq)]
enum References {
    Applicable(usize),
    NotApplicable,
}

impl References {
    fn multiple(&self) -> bool {
        match self {
            References::Applicable(references) => *references > 1,
            References::NotApplicable => false,
        }
    }
}

struct Entry<Key: Field, Value: Field> {
    label: Label,
    node: Node<Key, Value>,
    references: References,
}

impl<Key, Value> Entry<Key, Value>
where
    Key: Field,
    Value: Field,
{
    fn empty() -> Self {
        Entry {
            label: Label::Empty,
            node: Node::Empty,
            references: References::NotApplicable,
        }
    }
}

fn get<Key, Value>(
    store: &mut Store<Key, Value>,
    label: Label,
) -> Entry<Key, Value>
where
    Key: Field,
    Value: Field,
{
    if !label.is_empty() {
        match store.entry(label) {
            Occupied(entry) => {
                let value = entry.get();
                Entry {
                    label,
                    node: value.node.clone(),
                    references: References::Applicable(value.references),
                }
            }
            Vacant(..) => unreachable!(),
        }
    } else {
        Entry::empty()
    }
}

fn populate<Key, Value>(
    store: &mut Store<Key, Value>,
    label: Label,
    node: Node<Key, Value>,
) -> bool
where
    Key: Field,
    Value: Field,
{
    if !label.is_empty() {
        match store.entry(label) {
            Vacant(entry) => {
                entry.insert(StoreEntry {
                    node,
                    references: 0,
                });

                true
            }
            Occupied(..) => false,
        }
    } else {
        false
    }
}

fn incref<Key, Value>(store: &mut Store<Key, Value>, label: Label)
where
    Key: Field,
    Value: Field,
{
    if !label.is_empty() {
        match store.entry(label) {
            Occupied(mut entry) => {
                entry.get_mut().references += 1;
            }
            Vacant(..) => panic!("called `incref` on non-existing node"),
        }
    }
}

fn decref<Key, Value>(
    store: &mut Store<Key, Value>,
    label: Label,
    preserve: bool,
) where
    Key: Field,
    Value: Field,
{
    if !label.is_empty() {
        match store.entry(label) {
            Occupied(mut entry) => {
                let value = entry.get_mut();
                value.references -= 1;

                if value.references == 0 && !preserve {
                    entry.remove_entry();
                }
            }
            Vacant(..) => panic!("called `decref` on non-existing node"),
        }
    }
}

#[async_recursion]
async fn branch<Key, Value>(
    store: Store<Key, Value>,
    original: Option<&'async_recursion Entry<Key, Value>>,
    preserve: bool,
    depth: u8,
    batch: Batch<Key, Value>,
    chunk: Chunk,
    left: Entry<Key, Value>,
    right: Entry<Key, Value>,
) -> (Store<Key, Value>, Option<Batch<Key, Value>>, Label)
where
    Key: Field,
    Value: Field,
{
    let preserve_branches = preserve
        || if let Some(original) = original {
            original.references.multiple()
        } else {
            false
        };

    let (mut store, batch, new_left, new_right) = match store.split() {
        Split::Split(left_store, right_store) => {
            let (left_batch, left_chunk, right_batch, right_chunk) =
                chunk.snap(batch);

            let left_task = tokio::spawn(async move {
                recur(
                    left_store,
                    left,
                    preserve_branches,
                    depth + 1,
                    left_batch,
                    left_chunk,
                )
                .await
            });

            let right_task = tokio::spawn(async move {
                recur(
                    right_store,
                    right,
                    preserve_branches,
                    depth + 1,
                    right_batch,
                    right_chunk,
                )
                .await
            });

            let (left_join, right_join) = tokio::join!(left_task, right_task);

            let (left_store, _, left_label) = left_join.unwrap();
            let (right_store, _, right_label) = right_join.unwrap();

            let store = Store::merge(left_store, right_store);
            (store, None, left_label, right_label)
        }
        Split::Unsplittable(store) => {
            let (left_chunk, right_chunk) = chunk.split(&batch);

            let (store, batch, left_label) = recur(
                store,
                left,
                preserve_branches,
                depth + 1,
                batch,
                left_chunk,
            )
            .await;

            let (store, batch, right_label) = recur(
                store,
                right,
                preserve_branches,
                depth + 1,
                batch.unwrap(),
                right_chunk,
            )
            .await;

            (store, batch, left_label, right_label)
        }
    };

    let (new_label, adopt) = match (new_left, new_right) {
        (Label::Empty, Label::Empty) => (Label::Empty, false),
        (Label::Empty, Label::Leaf(map, hash))
        | (Label::Leaf(map, hash), Label::Empty) => {
            (Label::Leaf(map, hash), false)
        }
        (new_left, new_right) => {
            let node = Node::<Key, Value>::Internal(new_left, new_right);
            let label = store.label(&node);
            let adopt = populate(&mut store, label, node);

            (label, adopt)
        }
    };

    if match original {
        // This `match` is `true` iff `original` has changed
        // (`None` `original` changes implicitly)
        Some(original) => new_label != original.label,
        _ => true,
    } {
        if adopt {
            // If `adopt`, then `node` is guaranteed to be
            // `Internal(new_left, new_right)` (see above)
            incref(&mut store, new_left);
            incref(&mut store, new_right);
        }

        if let Some(original) = original {
            if !preserve && original.references == References::Applicable(1) {
                if let Node::Internal(old_left, old_right) = original.node {
                    // If `original` has only one reference, then its parent
                    // will `decref` it to 0 references and remove it,
                    // hence its children need to be `decref`-ed

                    // If `new_label == old_child`, then a `Leaf` is being pulled up,
                    // and will eventually be adopted either by an `Internal` node
                    // or by a root handle. Hence, it is left on the `store` to be
                    // `incref`-ed (adopted) later, even if its references
                    // are temporarily 0.
                    decref(&mut store, old_left, new_label == old_left);
                    decref(&mut store, old_right, new_label == old_right);
                }
            }
        }
    }

    (store, batch, new_label)
}

#[async_recursion]
async fn recur<Key, Value>(
    mut store: Store<Key, Value>,
    target: Entry<Key, Value>,
    preserve: bool,
    depth: u8,
    mut batch: Batch<Key, Value>,
    chunk: Chunk,
) -> (Store<Key, Value>, Option<Batch<Key, Value>>, Label)
where
    Key: Field,
    Value: Field,
{
    match (&target.node, chunk.task(&mut batch)) {
        (_, Task::Pass) => (store, Some(batch), target.label),

        (Node::Empty, Task::Do(operation)) => match &mut operation.action {
            Action::Get(sender) => {
                let sender = sender.take().unwrap();
                let _ = sender.send(None);

                (store, Some(batch), Label::Empty)
            }
            Action::Set(value) => {
                let node = Node::Leaf(operation.key.clone(), value.clone());
                let label = store.label(&node);

                populate(&mut store, label, node);
                (store, Some(batch), label)
            }
            Action::Remove => (store, Some(batch), Label::Empty),
        },
        (Node::Empty, Task::Split) => {
            branch(
                store,
                None,
                preserve,
                depth,
                batch,
                chunk,
                Entry::empty(),
                Entry::empty(),
            )
            .await
        }

        (Node::Leaf(key, original_value), Task::Do(operation))
            if *key == operation.key =>
        {
            match &mut operation.action {
                Action::Get(sender) => {
                    let sender = sender.take().unwrap();
                    let _ = sender.send(Some(original_value.clone()));

                    (store, Some(batch), target.label)
                }
                Action::Set(new_value) if new_value != original_value => {
                    let node =
                        Node::Leaf(operation.key.clone(), new_value.clone());
                    let label = store.label(&node);
                    populate(&mut store, label, node);

                    (store, Some(batch), label)
                }
                Action::Set(_) => (store, Some(batch), target.label),
                Action::Remove => (store, Some(batch), Label::Empty),
            }
        }
        (
            Node::Leaf(..),
            Task::Do(Operation {
                action: Action::Get(sender),
                ..
            }),
        ) => {
            let sender = sender.take().unwrap();
            let _ = sender.send(None);

            (store, Some(batch), target.label)
        }
        (Node::Leaf(key, _), _) => {
            let (left, right) =
                if Path::from(*key.digest())[depth] == Direction::Left {
                    (target, Entry::empty())
                } else {
                    (Entry::empty(), target)
                };

            branch(store, None, preserve, depth, batch, chunk, left, right)
                .await
        }

        (Node::Internal(left, right), _) => {
            let left = get(&mut store, *left);
            let right = get(&mut store, *right);

            branch(
                store,
                Some(&target),
                preserve,
                depth,
                batch,
                chunk,
                left,
                right,
            )
            .await
        }
    }
}

pub(super) async fn traverse<Key, Value>(
    mut store: Store<Key, Value>,
    root: Label,
    batch: Batch<Key, Value>,
) -> (Store<Key, Value>, Label)
where
    Key: Field,
    Value: Field,
{
    let root_node = get(&mut store, root);
    let root_chunk = Chunk::root(&batch);

    let (mut store, _, new_root) =
        recur(store, root_node, false, 0, batch, root_chunk).await;

    let old_root = root;
    if new_root != old_root {
        incref(&mut store, new_root);
        decref(&mut store, old_root, false);
    }

    (store, new_root)
}

#[cfg(test)]
mod tests {
    use super::*;

    use rand::seq::IteratorRandom;
    use rand::Rng;

    use super::super::operation::Operation;
    use super::super::prefix::Prefix;
    use super::super::wrap::Wrap;

    use std::collections::{HashMap, HashSet};

    fn get(store: &mut Store<u32, u32>, label: Label) -> Node<u32, u32> {
        match store.entry(label) {
            Occupied(entry) => entry.get().node.clone(),
            Vacant(..) => panic!("get: node not found"),
        }
    }

    fn get_internal(
        store: &mut Store<u32, u32>,
        label: Label,
    ) -> (Label, Label) {
        match get(store, label) {
            Node::Internal(left, right) => (left, right),
            _ => panic!("get_internal: node not internal"),
        }
    }

    fn get_leaf(
        store: &mut Store<u32, u32>,
        label: Label,
    ) -> (Wrap<u32>, Wrap<u32>) {
        match get(store, label) {
            Node::Leaf(key, value) => (key.clone(), value.clone()),
            _ => panic!("get_leaf: node not leaf"),
        }
    }

    fn leaf(key: u32, value: u32) -> Node<u32, u32> {
        Node::Leaf(Wrap::new(key).unwrap(), Wrap::new(value).unwrap())
    }

    fn set(key: u32, value: u32) -> Operation<u32, u32> {
        Operation::set(key, value).unwrap()
    }

    fn remove(key: u32) -> Operation<u32, u32> {
        Operation::remove(key).unwrap()
    }

    fn check_internal(store: &mut Store<u32, u32>, label: Label) {
        let (left, right) = get_internal(store, label);

        match (left, right) {
            (Label::Empty, Label::Empty)
            | (Label::Empty, Label::Leaf(..))
            | (Label::Leaf(..), Label::Empty) => {
                panic!("check_internal: children violate topology")
            }
            _ => {}
        }

        for &child in [left, right].iter() {
            if child != Label::Empty {
                if let Vacant(..) = store.entry(child) {
                    panic!("check_internal: child not found");
                }
            }
        }
    }

    fn check_leaf(store: &mut Store<u32, u32>, label: Label, prefix: Prefix) {
        let (key, _) = get_leaf(store, label);
        if !prefix.contains(&Path::from(*key.digest())) {
            panic!("check_leaf: leaf outside of path")
        }
    }

    fn check_tree(store: &mut Store<u32, u32>, label: Label, prefix: Prefix) {
        match label {
            Label::Internal(..) => {
                check_internal(store, label);

                let (left, right) = get_internal(store, label);
                check_tree(store, left, prefix.left());
                check_tree(store, right, prefix.right());
            }
            Label::Leaf(..) => {
                check_leaf(store, label, prefix);
            }
            Label::Empty => {}
        }
    }

    fn read_labels(
        store: &mut Store<u32, u32>,
        label: Label,
        collector: &mut HashSet<Label>,
    ) {
        if !label.is_empty() {
            collector.insert(label);
        }

        match label {
            Label::Internal(..) => {
                let (left, right) = get_internal(store, label);
                read_labels(store, left, collector);
                read_labels(store, right, collector);
            }
            _ => {}
        }
    }

    fn check_size(store: &mut Store<u32, u32>, roots: Vec<Label>) {
        let mut labels = HashSet::new();

        for root in roots {
            read_labels(store, root, &mut labels);
        }

        assert_eq!(store.size(), labels.len());
    }

    fn read_records(
        store: &mut Store<u32, u32>,
        label: Label,
        collector: &mut HashMap<u32, u32>,
    ) {
        match label {
            Label::Internal(..) => {
                let (left, right) = get_internal(store, label);
                read_records(store, left, collector);
                read_records(store, right, collector);
            }
            Label::Leaf(..) => {
                let (key, value) = get_leaf(store, label);
                collector.insert(**key.inner(), **value.inner());
            }
            Label::Empty => {}
        }
    }

    fn check_records(
        store: &mut Store<u32, u32>,
        root: Label,
        expected: &HashMap<u32, u32>,
    ) {
        let mut actual = HashMap::new();
        read_records(store, root, &mut actual);

        let actual: HashSet<(u32, u32)> =
            actual.iter().map(|(k, v)| (*k, *v)).collect();
        let expected: HashSet<(u32, u32)> =
            expected.iter().map(|(k, v)| (*k, *v)).collect();

        let differences: HashSet<(u32, u32)> =
            expected.symmetric_difference(&actual).map(|r| *r).collect();
        assert_eq!(differences, HashSet::new());
    }

    #[tokio::test]
    async fn single_static_tree() {
        let mut store = Store::<u32, u32>::new();
        check_size(&mut store, vec![Label::Empty]);

        // {0: 0, 1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6, 7: 7}

        let batch = Batch::new(vec![
            set(0, 0),
            set(1, 1),
            set(2, 2),
            set(3, 3),
            set(4, 4),
            set(5, 5),
            set(6, 6),
            set(7, 7),
        ]);

        let (mut store, root) = traverse(store, Label::Empty, batch).await;
        check_tree(&mut store, root, Prefix::root());
        check_size(&mut store, vec![root]);

        let (l, r) = get_internal(&mut store, root);
        assert_eq!(get(&mut store, r), leaf(4, 4));

        let (ll, lr) = get_internal(&mut store, l);
        assert_eq!(get(&mut store, lr), leaf(3, 3));

        let (lll, llr) = get_internal(&mut store, ll);
        assert_eq!(get(&mut store, llr), leaf(1, 1));

        let (llll, lllr) = get_internal(&mut store, lll);

        let (lllll, llllr) = get_internal(&mut store, llll);
        assert_eq!(lllll, Label::Empty);

        let (llllrl, llllrr) = get_internal(&mut store, llllr);
        assert_eq!(llllrl, Label::Empty);

        let (llllrrl, llllrrr) = get_internal(&mut store, llllrr);
        assert_eq!(get(&mut store, llllrrl), leaf(7, 7));
        assert_eq!(get(&mut store, llllrrr), leaf(2, 2));

        let (lllrl, lllrr) = get_internal(&mut store, lllr);
        assert_eq!(lllrr, Label::Empty);

        let (lllrll, lllrlr) = get_internal(&mut store, lllrl);
        assert_eq!(get(&mut store, lllrlr), leaf(5, 5));

        let (lllrlll, lllrllr) = get_internal(&mut store, lllrll);
        assert_eq!(lllrlll, Label::Empty);

        let (lllrllrl, lllrllrr) = get_internal(&mut store, lllrllr);
        assert_eq!(get(&mut store, lllrllrl), leaf(6, 6));
        assert_eq!(get(&mut store, lllrllrr), leaf(0, 0));
    }

    #[tokio::test]
    async fn single_dynamic_tree() {
        let store = Store::<u32, u32>::new();

        // {0: 1}

        let batch = Batch::new(vec![set(0, 1)]);
        let (mut store, root) = traverse(store, Label::Empty, batch).await;

        check_tree(&mut store, root, Prefix::root());
        check_size(&mut store, vec![root]);

        assert_eq!(get(&mut store, root), leaf(0, 1));

        // {0: 0}

        let batch = Batch::new(vec![set(0, 0)]);
        let (mut store, root) = traverse(store, root, batch).await;

        check_tree(&mut store, root, Prefix::root());
        check_size(&mut store, vec![root]);

        assert_eq!(get(&mut store, root), leaf(0, 0));

        // {0: 0, 1: 0}

        let batch = Batch::new(vec![set(1, 0)]);
        let (mut store, root) = traverse(store, root, batch).await;

        check_tree(&mut store, root, Prefix::root());
        check_size(&mut store, vec![root]);

        let (l, r) = get_internal(&mut store, root);
        assert_eq!(r, Label::Empty);

        let (ll, lr) = get_internal(&mut store, l);
        assert_eq!(lr, Label::Empty);

        let (lll, llr) = get_internal(&mut store, ll);
        assert_eq!(get(&mut store, lll), leaf(0, 0));
        assert_eq!(get(&mut store, llr), leaf(1, 0));

        // {1: 1}

        let batch = Batch::new(vec![set(1, 1), remove(0)]);
        let (mut store, root) = traverse(store, root, batch).await;

        check_tree(&mut store, root, Prefix::root());
        check_size(&mut store, vec![root]);

        assert_eq!(get(&mut store, root), leaf(1, 1));

        // {}

        let batch = Batch::new(vec![remove(1)]);
        let (mut store, root) = traverse(store, root, batch).await;

        check_tree(&mut store, root, Prefix::root());
        check_size(&mut store, vec![root]);

        assert_eq!(root, Label::Empty);
    }

    #[tokio::test]
    async fn single_insert() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set(i, i)).collect());
        let (mut store, root) = traverse(store, Label::Empty, batch).await;

        check_tree(&mut store, root, Prefix::root());
        check_records(
            &mut store,
            root,
            &mut (0..128).map(|i| (i, i)).collect(),
        );
        check_size(&mut store, vec![root]);
    }

    #[tokio::test]
    async fn single_modify() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set(i, i)).collect());
        let (store, root) = traverse(store, Label::Empty, batch).await;

        let batch = Batch::new((0..128).map(|i| set(i, i + 1)).collect());
        let (mut store, root) = traverse(store, root, batch).await;

        check_tree(&mut store, root, Prefix::root());
        check_records(
            &mut store,
            root,
            &mut (0..128).map(|i| (i, i + 1)).collect(),
        );
        check_size(&mut store, vec![root]);
    }

    #[tokio::test]
    async fn single_remove_all() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set(i, i)).collect());
        let (store, root) = traverse(store, Label::Empty, batch).await;

        let batch = Batch::new((0..128).map(|i| remove(i)).collect());
        let (mut store, root) = traverse(store, root, batch).await;

        assert_eq!(root, Label::Empty);
        check_size(&mut store, vec![root]);
    }

    #[tokio::test]
    async fn single_remove_half() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set(i, i)).collect());
        let (store, root) = traverse(store, Label::Empty, batch).await;

        let batch = Batch::new((0..64).map(|i| remove(i)).collect());
        let (mut store, root) = traverse(store, root, batch).await;

        check_tree(&mut store, root, Prefix::root());
        check_records(
            &mut store,
            root,
            &mut (64..128).map(|i| (i, i)).collect(),
        );
        check_size(&mut store, vec![root]);
    }

    #[tokio::test]
    async fn single_remove_all_but_one() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set(i, i)).collect());
        let (store, root) = traverse(store, Label::Empty, batch).await;

        let batch = Batch::new((0..127).map(|i| remove(i)).collect());
        let (mut store, root) = traverse(store, root, batch).await;

        check_tree(&mut store, root, Prefix::root());
        check_records(
            &mut store,
            root,
            &mut (127..128).map(|i| (i, i)).collect(),
        );
        check_size(&mut store, vec![root]);
    }

    #[tokio::test]
    async fn single_remove_half_insert_half() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..64).map(|i| set(i, i)).collect());
        let (store, root) = traverse(store, Label::Empty, batch).await;

        let batch = Batch::new(
            (0..128)
                .map(|i| if i < 64 { remove(i) } else { set(i, i) })
                .collect(),
        );
        let (mut store, root) = traverse(store, root, batch).await;

        check_tree(&mut store, root, Prefix::root());
        check_records(
            &mut store,
            root,
            &mut (64..128).map(|i| (i, i)).collect(),
        );
        check_size(&mut store, vec![root]);
    }

    #[tokio::test]
    async fn single_remove_half_modify_half() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set(i, i)).collect());
        let (store, root) = traverse(store, Label::Empty, batch).await;

        let batch = Batch::new(
            (0..128)
                .map(|i| if i < 64 { remove(i) } else { set(i, i + 1) })
                .collect(),
        );
        let (mut store, root) = traverse(store, root, batch).await;

        check_tree(&mut store, root, Prefix::root());
        check_records(
            &mut store,
            root,
            &mut (64..128).map(|i| (i, i + 1)).collect(),
        );
        check_size(&mut store, vec![root]);
    }

    #[tokio::test]
    async fn single_remove_quarter_modify_quarter_insert_half() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..64).map(|i| set(i, i)).collect());
        let (store, root) = traverse(store, Label::Empty, batch).await;

        let batch = Batch::new(
            (0..128)
                .map(|i| if i < 32 { remove(i) } else { set(i, i + 1) })
                .collect(),
        );
        let (mut store, root) = traverse(store, root, batch).await;

        check_tree(&mut store, root, Prefix::root());
        check_records(
            &mut store,
            root,
            &mut (32..128).map(|i| (i, i + 1)).collect(),
        );
        check_size(&mut store, vec![root]);
    }

    #[tokio::test]
    async fn single_stress() {
        let mut reference = HashMap::new();

        let mut store = Store::<u32, u32>::new();
        let mut root = Label::Empty;

        let mut rng = rand::thread_rng();

        for round in 0..32 {
            let keys = (0..1024).choose_multiple(&mut rng, 128);

            let operations: Vec<Operation<u32, u32>> = keys
                .iter()
                .map(|&key| {
                    if rng.gen::<bool>() {
                        reference.insert(key, round);
                        set(key, round)
                    } else {
                        reference.remove(&key);
                        remove(key)
                    }
                })
                .collect();

            let batch = Batch::new(operations);
            let next = traverse(store, root, batch).await;

            store = next.0;
            root = next.1;

            check_tree(&mut store, root, Prefix::root());
            check_records(&mut store, root, &reference);
            check_size(&mut store, vec![root]);
        }
    }

    #[tokio::test]
    async fn multiple_distinct() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set(i, i)).collect());
        let (store, first_root) = traverse(store, Label::Empty, batch).await;

        let batch = Batch::new((128..256).map(|i| set(i, i)).collect());
        let (mut store, second_root) =
            traverse(store, Label::Empty, batch).await;

        check_tree(&mut store, first_root, Prefix::root());
        check_records(
            &mut store,
            first_root,
            &mut (0..128).map(|i| (i, i)).collect(),
        );

        check_tree(&mut store, second_root, Prefix::root());
        check_records(
            &mut store,
            second_root,
            &mut (128..256).map(|i| (i, i)).collect(),
        );

        check_size(&mut store, vec![first_root, second_root]);
    }

    #[tokio::test]
    async fn multiple_insert_then_match() {
        let store = Store::<u32, u32>::new();

        let batch = || Batch::new((0..128).map(|i| set(i, i)).collect());
        let (store, first_root) = traverse(store, Label::Empty, batch()).await;
        let (mut store, second_root) =
            traverse(store, Label::Empty, batch()).await;

        check_tree(&mut store, first_root, Prefix::root());
        check_records(
            &mut store,
            first_root,
            &mut (0..128).map(|i| (i, i)).collect(),
        );

        check_tree(&mut store, second_root, Prefix::root());
        check_records(
            &mut store,
            second_root,
            &mut (0..128).map(|i| (i, i)).collect(),
        );

        check_size(&mut store, vec![first_root, second_root]);
    }

    #[tokio::test]
    async fn multiple_insert_then_overflow_by_one() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set(i, i)).collect());
        let (store, first_root) = traverse(store, Label::Empty, batch).await;

        let batch = Batch::new((0..129).map(|i| set(i, i)).collect());
        let (mut store, second_root) =
            traverse(store, Label::Empty, batch).await;

        check_tree(&mut store, first_root, Prefix::root());
        check_records(
            &mut store,
            first_root,
            &mut (0..128).map(|i| (i, i)).collect(),
        );

        check_tree(&mut store, second_root, Prefix::root());
        check_records(
            &mut store,
            second_root,
            &mut (0..129).map(|i| (i, i)).collect(),
        );

        check_size(&mut store, vec![first_root, second_root]);
    }

    #[tokio::test]
    async fn multiple_insert_then_double() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set(i, i)).collect());
        let (store, first_root) = traverse(store, Label::Empty, batch).await;

        let batch = Batch::new((0..256).map(|i| set(i, i)).collect());
        let (mut store, second_root) =
            traverse(store, Label::Empty, batch).await;

        check_tree(&mut store, first_root, Prefix::root());
        check_records(
            &mut store,
            first_root,
            &mut (0..128).map(|i| (i, i)).collect(),
        );

        check_tree(&mut store, second_root, Prefix::root());
        check_records(
            &mut store,
            second_root,
            &mut (0..256).map(|i| (i, i)).collect(),
        );

        check_size(&mut store, vec![first_root, second_root]);
    }

    #[tokio::test]
    async fn multiple_match_then_empty() {
        let store = Store::<u32, u32>::new();

        let batch = || Batch::new((0..128).map(|i| set(i, i)).collect());
        let (store, first_root) = traverse(store, Label::Empty, batch()).await;
        let (store, second_root) = traverse(store, Label::Empty, batch()).await;

        let batch = Batch::new((0..128).map(|i| remove(i)).collect());
        let (mut store, second_root) =
            traverse(store, second_root, batch).await;

        check_tree(&mut store, first_root, Prefix::root());
        check_records(
            &mut store,
            first_root,
            &mut (0..128).map(|i| (i, i)).collect(),
        );

        check_tree(&mut store, second_root, Prefix::root());
        check_records(&mut store, second_root, &mut HashMap::new());

        check_size(&mut store, vec![first_root, second_root]);
    }

    #[tokio::test]
    async fn multiple_match_then_leave_one() {
        let store = Store::<u32, u32>::new();

        let batch = || Batch::new((0..128).map(|i| set(i, i)).collect());
        let (store, first_root) = traverse(store, Label::Empty, batch()).await;
        let (store, second_root) = traverse(store, Label::Empty, batch()).await;

        let batch = Batch::new((0..127).map(|i| remove(i)).collect());
        let (mut store, second_root) =
            traverse(store, second_root, batch).await;

        check_tree(&mut store, first_root, Prefix::root());
        check_records(
            &mut store,
            first_root,
            &mut (0..128).map(|i| (i, i)).collect(),
        );

        check_tree(&mut store, second_root, Prefix::root());
        check_records(
            &mut store,
            second_root,
            &mut (127..128).map(|i| (i, i)).collect(),
        );

        check_size(&mut store, vec![first_root, second_root]);
    }

    #[tokio::test]
    async fn multiple_match_then_leave_half() {
        let store = Store::<u32, u32>::new();

        let batch = || Batch::new((0..128).map(|i| set(i, i)).collect());
        let (store, first_root) = traverse(store, Label::Empty, batch()).await;
        let (store, second_root) = traverse(store, Label::Empty, batch()).await;

        let batch = Batch::new((0..64).map(|i| remove(i)).collect());
        let (mut store, second_root) =
            traverse(store, second_root, batch).await;

        check_tree(&mut store, first_root, Prefix::root());
        check_records(
            &mut store,
            first_root,
            &mut (0..128).map(|i| (i, i)).collect(),
        );

        check_tree(&mut store, second_root, Prefix::root());
        check_records(
            &mut store,
            second_root,
            &mut (64..128).map(|i| (i, i)).collect(),
        );

        check_size(&mut store, vec![first_root, second_root]);
    }

    #[tokio::test]
    async fn multiple_match_then_split() {
        let store = Store::<u32, u32>::new();

        let batch = || Batch::new((0..128).map(|i| set(i, i)).collect());
        let (store, first_root) = traverse(store, Label::Empty, batch()).await;
        let (store, second_root) = traverse(store, Label::Empty, batch()).await;

        let batch = Batch::new((64..128).map(|i| remove(i)).collect());
        let (store, first_root) = traverse(store, first_root, batch).await;

        let batch = Batch::new((0..64).map(|i| remove(i)).collect());
        let (mut store, second_root) =
            traverse(store, second_root, batch).await;

        check_tree(&mut store, first_root, Prefix::root());
        check_records(
            &mut store,
            first_root,
            &mut (0..64).map(|i| (i, i)).collect(),
        );

        check_tree(&mut store, second_root, Prefix::root());
        check_records(
            &mut store,
            second_root,
            &mut (64..128).map(|i| (i, i)).collect(),
        );

        check_size(&mut store, vec![first_root, second_root]);
    }
}
