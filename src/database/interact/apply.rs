use async_recursion::async_recursion;

use crate::database::{
    interact::{Action, Batch, Chunk, Operation, Task},
    store::{Field, Label, Node, Split, Store},
    tree::{Direction, Path},
};

use std::collections::hash_map::Entry::{Occupied, Vacant};

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
) -> (Store<Key, Value>, Batch<Key, Value>, Label)
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

            let (left_store, left_batch, left_label) = left_join.unwrap();
            let (right_store, right_batch, right_label) = right_join.unwrap();

            let store = Store::merge(left_store, right_store);
            let batch = Batch::merge(left_batch, right_batch);

            (store, batch, left_label, right_label)
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
                batch,
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
            let adopt = store.populate(label, node);

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
            store.incref(new_left);
            store.incref(new_right);
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
                    store.decref(old_left, new_label == old_left);
                    store.decref(old_right, new_label == old_right);
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
) -> (Store<Key, Value>, Batch<Key, Value>, Label)
where
    Key: Field,
    Value: Field,
{
    match (&target.node, chunk.task(&mut batch)) {
        (_, Task::Pass) => (store, batch, target.label),

        (Node::Empty, Task::Do(operation)) => match &mut operation.action {
            Action::Get(..) => (store, batch, Label::Empty),
            Action::Set(key, value) => {
                let node = Node::Leaf(key.clone(), value.clone());
                let label = store.label(&node);

                store.populate(label, node);
                (store, batch, label)
            }
            Action::Remove => (store, batch, Label::Empty),
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
            if operation.path.reaches(key) =>
        {
            match &mut operation.action {
                Action::Get(holder) => {
                    *holder = Some(original_value.inner().clone());
                    (store, batch, target.label)
                }
                Action::Set(_, new_value) if new_value != original_value => {
                    let node = Node::Leaf(key.clone(), new_value.clone());
                    let label = store.label(&node);
                    store.populate(label, node);

                    (store, batch, label)
                }
                Action::Set(..) => (store, batch, target.label),
                Action::Remove => (store, batch, Label::Empty),
            }
        }
        (
            Node::Leaf(..),
            Task::Do(Operation {
                action: Action::Get(..),
                ..
            }),
        ) => (store, batch, target.label),
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

pub(crate) async fn apply<Key, Value>(
    mut store: Store<Key, Value>,
    root: Label,
    batch: Batch<Key, Value>,
) -> (Store<Key, Value>, Label, Batch<Key, Value>)
where
    Key: Field,
    Value: Field,
{
    let root_node = get(&mut store, root);
    let root_chunk = Chunk::root(&batch);

    let (mut store, batch, new_root) =
        recur(store, root_node, false, 0, batch, root_chunk).await;

    let old_root = root;
    if new_root != old_root {
        store.incref(new_root);
        store.decref(old_root, false);
    }

    (store, new_root, batch)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::database::{data::Bytes, interact::Operation, tree::Prefix};

    use drop::crypto::hash;

    use rand::seq::IteratorRandom;
    use rand::Rng;

    use std::collections::{HashMap, HashSet};

    fn check_records(
        store: &mut Store<u32, u32>,
        root: Label,
        reference: &HashMap<u32, u32>,
    ) {
        let actual = store.collect_records(root);

        let actual: HashSet<(u32, u32)> =
            actual.iter().map(|(k, v)| (*k, *v)).collect();
        let reference: HashSet<(u32, u32)> =
            reference.iter().map(|(k, v)| (*k, *v)).collect();

        let differences: HashSet<(u32, u32)> = reference
            .symmetric_difference(&actual)
            .map(|r| *r)
            .collect();
        assert_eq!(differences, HashSet::new());
    }

    fn read_gets(batch: &Batch<u32, u32>) -> HashMap<Bytes, Option<u32>> {
        batch
            .operations()
            .iter()
            .filter_map(|operation| match &operation.action {
                Action::Get(holder) => Some((
                    operation.path.into(),
                    holder.clone().map(|value| *value),
                )),
                _ => None,
            })
            .collect()
    }

    fn check_gets(
        batch: &Batch<u32, u32>,
        reference: &HashMap<u32, Option<u32>>,
    ) {
        let preimage: HashMap<Bytes, u32> = reference
            .iter()
            .map(|(k, _)| (Bytes::from(hash::hash(k).unwrap()), *k))
            .collect();

        let actual = read_gets(batch);

        let actual: HashSet<(Bytes, Option<u32>)> =
            actual.iter().map(|(k, v)| (*k, *v)).collect();
        let reference: HashSet<(Bytes, Option<u32>)> = reference
            .iter()
            .map(|(k, v)| (Bytes::from(hash::hash(k).unwrap()), *v))
            .collect();

        #[derive(Debug, Hash, PartialEq, Eq)]
        enum DiffKey {
            Known(u32),
            Unknown(Bytes),
        }

        let differences: HashSet<(DiffKey, Option<u32>)> = reference
            .symmetric_difference(&actual)
            .map(|(hash, value)| {
                (
                    if let Some(key) = preimage.get(hash) {
                        DiffKey::Known(*key)
                    } else {
                        DiffKey::Unknown(*hash)
                    },
                    value.clone(),
                )
            })
            .collect();

        assert_eq!(differences, HashSet::new());
    }

    #[tokio::test]
    async fn single_static_tree() {
        let mut store = Store::<u32, u32>::new();
        store.check_leaks([Label::Empty]);

        // {0: 0, 1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6, 7: 7}

        let batch = Batch::new(vec![
            set!(0, 0),
            set!(1, 1),
            set!(2, 2),
            set!(3, 3),
            set!(4, 4),
            set!(5, 5),
            set!(6, 6),
            set!(7, 7),
        ]);

        let (mut store, root, _) = apply(store, Label::Empty, batch).await;
        store.check_tree(root, Prefix::root());
        store.check_leaks([root]);

        let (l, r) = store.fetch_internal(root);
        assert_eq!(store.fetch_node(r), leaf!(4, 4));

        let (ll, lr) = store.fetch_internal(l);
        assert_eq!(store.fetch_node(lr), leaf!(3, 3));

        let (lll, llr) = store.fetch_internal(ll);
        assert_eq!(store.fetch_node(llr), leaf!(1, 1));

        let (llll, lllr) = store.fetch_internal(lll);

        let (lllll, llllr) = store.fetch_internal(llll);
        assert_eq!(lllll, Label::Empty);

        let (llllrl, llllrr) = store.fetch_internal(llllr);
        assert_eq!(llllrl, Label::Empty);

        let (llllrrl, llllrrr) = store.fetch_internal(llllrr);
        assert_eq!(store.fetch_node(llllrrl), leaf!(7, 7));
        assert_eq!(store.fetch_node(llllrrr), leaf!(2, 2));

        let (lllrl, lllrr) = store.fetch_internal(lllr);
        assert_eq!(lllrr, Label::Empty);

        let (lllrll, lllrlr) = store.fetch_internal(lllrl);
        assert_eq!(store.fetch_node(lllrlr), leaf!(5, 5));

        let (lllrlll, lllrllr) = store.fetch_internal(lllrll);
        assert_eq!(lllrlll, Label::Empty);

        let (lllrllrl, lllrllrr) = store.fetch_internal(lllrllr);
        assert_eq!(store.fetch_node(lllrllrl), leaf!(6, 6));
        assert_eq!(store.fetch_node(lllrllrr), leaf!(0, 0));
    }

    #[tokio::test]
    async fn single_dynamic_tree() {
        let store = Store::<u32, u32>::new();

        // {0: 1}

        let batch = Batch::new(vec![set!(0, 1)]);
        let (mut store, root, _) = apply(store, Label::Empty, batch).await;

        store.check_tree(root, Prefix::root());
        store.check_leaks([root]);

        assert_eq!(store.fetch_node(root), leaf!(0, 1));

        // {0: 0}

        let batch = Batch::new(vec![set!(0, 0)]);
        let (mut store, root, _) = apply(store, root, batch).await;

        store.check_tree(root, Prefix::root());
        store.check_leaks([root]);

        assert_eq!(store.fetch_node(root), leaf!(0, 0));

        // {0: 0, 1: 0}

        let batch = Batch::new(vec![set!(1, 0)]);
        let (mut store, root, _) = apply(store, root, batch).await;

        store.check_tree(root, Prefix::root());
        store.check_leaks([root]);

        let (l, r) = store.fetch_internal(root);
        assert_eq!(r, Label::Empty);

        let (ll, lr) = store.fetch_internal(l);
        assert_eq!(lr, Label::Empty);

        let (lll, llr) = store.fetch_internal(ll);
        assert_eq!(store.fetch_node(lll), leaf!(0, 0));
        assert_eq!(store.fetch_node(llr), leaf!(1, 0));

        // {1: 1}

        let batch = Batch::new(vec![set!(1, 1), remove!(0)]);
        let (mut store, root, _) = apply(store, root, batch).await;

        store.check_tree(root, Prefix::root());
        store.check_leaks([root]);

        assert_eq!(store.fetch_node(root), leaf!(1, 1));

        // {}

        let batch = Batch::new(vec![remove!(1)]);
        let (mut store, root, _) = apply(store, root, batch).await;

        store.check_tree(root, Prefix::root());
        store.check_leaks([root]);

        assert_eq!(root, Label::Empty);
    }

    #[tokio::test]
    async fn single_insert() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (mut store, root, _) = apply(store, Label::Empty, batch).await;

        store.check_tree(root, Prefix::root());
        check_records(
            &mut store,
            root,
            &mut (0..128).map(|i| (i, i)).collect(),
        );
        store.check_leaks([root]);
    }

    #[tokio::test]
    async fn single_insert_read_all() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (store, root, _) = apply(store, Label::Empty, batch).await;

        let batch = Batch::new((0..128).map(|i| get!(i)).collect());
        let (_, _, batch) = apply(store, root, batch).await;

        check_gets(&batch, &(0..128).map(|i| (i, Some(i))).collect());
    }

    #[tokio::test]
    async fn single_insert_read_half() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (store, root, _) = apply(store, Label::Empty, batch).await;

        let batch = Batch::new((0..64).map(|i| get!(i)).collect());
        let (_, _, batch) = apply(store, root, batch).await;

        check_gets(&batch, &(0..64).map(|i| (i, Some(i))).collect());
    }

    #[tokio::test]
    async fn single_insert_read_missing() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (store, root, _) = apply(store, Label::Empty, batch).await;

        let batch = Batch::new((128..256).map(|i| get!(i)).collect());
        let (_, _, batch) = apply(store, root, batch).await;

        check_gets(&batch, &(128..256).map(|i| (i, None)).collect());
    }

    #[tokio::test]
    async fn single_insert_read_overlap() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (store, root, _) = apply(store, Label::Empty, batch).await;

        let batch = Batch::new((64..192).map(|i| get!(i)).collect());
        let (_, _, batch) = apply(store, root, batch).await;

        check_gets(
            &batch,
            &(64..192)
                .map(|i| (i, if i < 128 { Some(i) } else { None }))
                .collect(),
        );
    }

    #[tokio::test]
    async fn single_modify() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (store, root, _) = apply(store, Label::Empty, batch).await;

        let batch = Batch::new((0..128).map(|i| set!(i, i + 1)).collect());
        let (mut store, root, _) = apply(store, root, batch).await;

        store.check_tree(root, Prefix::root());
        check_records(
            &mut store,
            root,
            &mut (0..128).map(|i| (i, i + 1)).collect(),
        );
        store.check_leaks([root]);
    }

    #[tokio::test]
    async fn single_modify_read_overlap() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (store, root, _) = apply(store, Label::Empty, batch).await;

        let batch = Batch::new((0..128).map(|i| set!(i, i + 1)).collect());
        let (store, root, _) = apply(store, root, batch).await;

        let batch = Batch::new((64..192).map(|i| get!(i)).collect());
        let (_, _, batch) = apply(store, root, batch).await;

        check_gets(
            &batch,
            &(64..192)
                .map(|i| (i, if i < 128 { Some(i + 1) } else { None }))
                .collect(),
        );
    }

    #[tokio::test]
    async fn single_insert_hybrid_read_set() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..192).map(|i| set!(i, i)).collect());
        let (store, root, _) = apply(store, Label::Empty, batch).await;

        let batch = Batch::new(
            (0..128)
                .map(|i| set!(i, i + 1))
                .chain((128..256).map(|i| get!(i)))
                .collect(),
        );

        let (mut store, root, batch) = apply(store, root, batch).await;

        store.check_tree(root, Prefix::root());
        check_records(
            &mut store,
            root,
            &mut (0..192)
                .map(|i| (i, if i < 128 { i + 1 } else { i }))
                .collect(),
        );
        store.check_leaks([root]);

        check_gets(
            &batch,
            &(128..256)
                .map(|i| (i, if i < 192 { Some(i) } else { None }))
                .collect(),
        );
    }

    #[tokio::test]
    async fn single_remove_all() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (store, root, _) = apply(store, Label::Empty, batch).await;

        let batch = Batch::new((0..128).map(|i| remove!(i)).collect());
        let (mut store, root, _) = apply(store, root, batch).await;

        assert_eq!(root, Label::Empty);
        store.check_leaks([root]);
    }

    #[tokio::test]
    async fn single_remove_half() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (store, root, _) = apply(store, Label::Empty, batch).await;

        let batch = Batch::new((0..64).map(|i| remove!(i)).collect());
        let (mut store, root, _) = apply(store, root, batch).await;

        store.check_tree(root, Prefix::root());
        check_records(
            &mut store,
            root,
            &mut (64..128).map(|i| (i, i)).collect(),
        );
        store.check_leaks([root]);
    }

    #[tokio::test]
    async fn single_remove_all_but_one() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (store, root, _) = apply(store, Label::Empty, batch).await;

        let batch = Batch::new((0..127).map(|i| remove!(i)).collect());
        let (mut store, root, _) = apply(store, root, batch).await;

        store.check_tree(root, Prefix::root());
        check_records(
            &mut store,
            root,
            &mut (127..128).map(|i| (i, i)).collect(),
        );
        store.check_leaks([root]);
    }

    #[tokio::test]
    async fn single_remove_half_insert_half() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..64).map(|i| set!(i, i)).collect());
        let (store, root, _) = apply(store, Label::Empty, batch).await;

        let batch = Batch::new(
            (0..128)
                .map(|i| if i < 64 { remove!(i) } else { set!(i, i) })
                .collect(),
        );
        let (mut store, root, _) = apply(store, root, batch).await;

        store.check_tree(root, Prefix::root());
        check_records(
            &mut store,
            root,
            &mut (64..128).map(|i| (i, i)).collect(),
        );
        store.check_leaks([root]);
    }

    #[tokio::test]
    async fn single_remove_half_modify_half() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (store, root, _) = apply(store, Label::Empty, batch).await;

        let batch = Batch::new(
            (0..128)
                .map(|i| if i < 64 { remove!(i) } else { set!(i, i + 1) })
                .collect(),
        );
        let (mut store, root, _) = apply(store, root, batch).await;

        store.check_tree(root, Prefix::root());
        check_records(
            &mut store,
            root,
            &mut (64..128).map(|i| (i, i + 1)).collect(),
        );
        store.check_leaks([root]);
    }

    #[tokio::test]
    async fn single_remove_quarter_modify_quarter_insert_half() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..64).map(|i| set!(i, i)).collect());
        let (store, root, _) = apply(store, Label::Empty, batch).await;

        let batch = Batch::new(
            (0..128)
                .map(|i| if i < 32 { remove!(i) } else { set!(i, i + 1) })
                .collect(),
        );
        let (mut store, root, _) = apply(store, root, batch).await;

        store.check_tree(root, Prefix::root());
        check_records(
            &mut store,
            root,
            &mut (32..128).map(|i| (i, i + 1)).collect(),
        );
        store.check_leaks([root]);
    }

    #[tokio::test]
    async fn single_stress() {
        let mut record_reference = HashMap::new();

        let mut store = Store::<u32, u32>::new();
        let mut root = Label::Empty;

        let mut rng = rand::thread_rng();

        for round in 0..32 {
            let keys = (0..1024).choose_multiple(&mut rng, 128);
            let mut get_reference = HashMap::new();

            let operations: Vec<Operation<u32, u32>> = keys
                .iter()
                .map(|&key| {
                    if rng.gen::<bool>() {
                        if rng.gen::<bool>() {
                            record_reference.insert(key, round);
                            set!(key, round)
                        } else {
                            record_reference.remove(&key);
                            remove!(key)
                        }
                    } else {
                        get_reference.insert(
                            key,
                            record_reference.get(&key).map(|value| *value),
                        );
                        get!(key)
                    }
                })
                .collect();

            let batch = Batch::new(operations);
            let next = apply(store, root, batch).await;

            store = next.0;
            root = next.1;
            let batch = next.2;

            store.check_tree(root, Prefix::root());
            check_records(&mut store, root, &record_reference);
            store.check_leaks([root]);

            check_gets(&batch, &get_reference);
        }
    }

    #[tokio::test]
    async fn multiple_distinct() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (store, first_root, _) = apply(store, Label::Empty, batch).await;

        let batch = Batch::new((128..256).map(|i| set!(i, i)).collect());
        let (mut store, second_root, _) =
            apply(store, Label::Empty, batch).await;

        store.check_tree(first_root, Prefix::root());
        check_records(
            &mut store,
            first_root,
            &mut (0..128).map(|i| (i, i)).collect(),
        );

        store.check_tree(second_root, Prefix::root());
        check_records(
            &mut store,
            second_root,
            &mut (128..256).map(|i| (i, i)).collect(),
        );

        store.check_leaks([first_root, second_root]);
    }

    #[tokio::test]
    async fn multiple_insert_then_match() {
        let store = Store::<u32, u32>::new();

        let batch = || Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (store, first_root, _) = apply(store, Label::Empty, batch()).await;
        let (mut store, second_root, _) =
            apply(store, Label::Empty, batch()).await;

        store.check_tree(first_root, Prefix::root());
        check_records(
            &mut store,
            first_root,
            &mut (0..128).map(|i| (i, i)).collect(),
        );

        store.check_tree(second_root, Prefix::root());
        check_records(
            &mut store,
            second_root,
            &mut (0..128).map(|i| (i, i)).collect(),
        );

        store.check_leaks([first_root, second_root]);
    }

    #[tokio::test]
    async fn multiple_insert_then_overflow_by_one() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (store, first_root, _) = apply(store, Label::Empty, batch).await;

        let batch = Batch::new((0..129).map(|i| set!(i, i)).collect());
        let (mut store, second_root, _) =
            apply(store, Label::Empty, batch).await;

        store.check_tree(first_root, Prefix::root());
        check_records(
            &mut store,
            first_root,
            &mut (0..128).map(|i| (i, i)).collect(),
        );

        store.check_tree(second_root, Prefix::root());
        check_records(
            &mut store,
            second_root,
            &mut (0..129).map(|i| (i, i)).collect(),
        );

        store.check_leaks([first_root, second_root]);
    }

    #[tokio::test]
    async fn multiple_insert_then_double() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (store, first_root, _) = apply(store, Label::Empty, batch).await;

        let batch = Batch::new((0..256).map(|i| set!(i, i)).collect());
        let (mut store, second_root, _) =
            apply(store, Label::Empty, batch).await;

        store.check_tree(first_root, Prefix::root());
        check_records(
            &mut store,
            first_root,
            &mut (0..128).map(|i| (i, i)).collect(),
        );

        store.check_tree(second_root, Prefix::root());
        check_records(
            &mut store,
            second_root,
            &mut (0..256).map(|i| (i, i)).collect(),
        );

        store.check_leaks([first_root, second_root]);
    }

    #[tokio::test]
    async fn multiple_match_then_empty() {
        let store = Store::<u32, u32>::new();

        let batch = || Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (store, first_root, _) = apply(store, Label::Empty, batch()).await;
        let (store, second_root, _) = apply(store, Label::Empty, batch()).await;

        let batch = Batch::new((0..128).map(|i| remove!(i)).collect());
        let (mut store, second_root, _) =
            apply(store, second_root, batch).await;

        store.check_tree(first_root, Prefix::root());
        check_records(
            &mut store,
            first_root,
            &mut (0..128).map(|i| (i, i)).collect(),
        );

        store.check_tree(second_root, Prefix::root());
        check_records(&mut store, second_root, &mut HashMap::new());

        store.check_leaks([first_root, second_root]);
    }

    #[tokio::test]
    async fn multiple_match_then_leave_one() {
        let store = Store::<u32, u32>::new();

        let batch = || Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (store, first_root, _) = apply(store, Label::Empty, batch()).await;
        let (store, second_root, _) = apply(store, Label::Empty, batch()).await;

        let batch = Batch::new((0..127).map(|i| remove!(i)).collect());
        let (mut store, second_root, _) =
            apply(store, second_root, batch).await;

        store.check_tree(first_root, Prefix::root());
        check_records(
            &mut store,
            first_root,
            &mut (0..128).map(|i| (i, i)).collect(),
        );

        store.check_tree(second_root, Prefix::root());
        check_records(
            &mut store,
            second_root,
            &mut (127..128).map(|i| (i, i)).collect(),
        );

        store.check_leaks([first_root, second_root]);
    }

    #[tokio::test]
    async fn multiple_match_then_leave_half() {
        let store = Store::<u32, u32>::new();

        let batch = || Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (store, first_root, _) = apply(store, Label::Empty, batch()).await;
        let (store, second_root, _) = apply(store, Label::Empty, batch()).await;

        let batch = Batch::new((0..64).map(|i| remove!(i)).collect());
        let (mut store, second_root, _) =
            apply(store, second_root, batch).await;

        store.check_tree(first_root, Prefix::root());
        check_records(
            &mut store,
            first_root,
            &mut (0..128).map(|i| (i, i)).collect(),
        );

        store.check_tree(second_root, Prefix::root());
        check_records(
            &mut store,
            second_root,
            &mut (64..128).map(|i| (i, i)).collect(),
        );

        store.check_leaks([first_root, second_root]);
    }

    #[tokio::test]
    async fn multiple_match_then_split() {
        let store = Store::<u32, u32>::new();

        let batch = || Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (store, first_root, _) = apply(store, Label::Empty, batch()).await;
        let (store, second_root, _) = apply(store, Label::Empty, batch()).await;

        let batch = Batch::new((64..128).map(|i| remove!(i)).collect());
        let (store, first_root, _) = apply(store, first_root, batch).await;

        let batch = Batch::new((0..64).map(|i| remove!(i)).collect());
        let (mut store, second_root, _) =
            apply(store, second_root, batch).await;

        store.check_tree(first_root, Prefix::root());
        check_records(
            &mut store,
            first_root,
            &mut (0..64).map(|i| (i, i)).collect(),
        );

        store.check_tree(second_root, Prefix::root());
        check_records(
            &mut store,
            second_root,
            &mut (64..128).map(|i| (i, i)).collect(),
        );

        store.check_leaks([first_root, second_root]);
    }

    #[tokio::test]
    async fn multiple_stress() {
        let mut first_record_reference = HashMap::new();
        let mut second_record_reference = HashMap::new();

        let mut store = Store::<u32, u32>::new();

        let mut first_root = Label::Empty;
        let mut second_root = Label::Empty;

        let mut rng = rand::thread_rng();

        for round in 0..32 {
            for (record_reference, root) in vec![
                (&mut first_record_reference, &mut first_root),
                (&mut second_record_reference, &mut second_root),
            ] {
                let keys = (0..1024).choose_multiple(&mut rng, 128);
                let mut get_reference = HashMap::new();

                let operations: Vec<Operation<u32, u32>> = keys
                    .iter()
                    .map(|&key| {
                        if rng.gen::<bool>() {
                            if rng.gen::<bool>() {
                                record_reference.insert(key, round);
                                set!(key, round)
                            } else {
                                record_reference.remove(&key);
                                remove!(key)
                            }
                        } else {
                            get_reference.insert(
                                key,
                                record_reference.get(&key).map(|value| *value),
                            );
                            get!(key)
                        }
                    })
                    .collect();

                let batch = Batch::new(operations);
                let next = apply(store, *root, batch).await;

                store = next.0;
                *root = next.1;
                let batch = next.2;

                store.check_tree(*root, Prefix::root());
                check_records(&mut store, *root, &record_reference);

                check_gets(&batch, &get_reference);
            }

            store.check_leaks([first_root, second_root]);
        }
    }
}
