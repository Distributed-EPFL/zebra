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
use super::path::Path;
use super::store::{Split, Store};
use super::task::Task;

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

fn incref<Key, Value>(
    store: &mut Store<Key, Value>,
    label: Label,
    node: Node<Key, Value>,
) where
    Key: Field,
    Value: Field,
{
    if !label.is_empty() {
        match store.entry(label) {
            Occupied(mut entry) => {
                entry.get_mut().references += 1;

                // This `match` is tied to the traversal of a `MerkleTable`'s tree:
                // increfing an internal node implies a previous incref of its children,
                // which needs to be correct upon deduplication.
                // A normal `incref` method would not have this.
                match node {
                    Node::Internal(left, right) => {
                        decref(store, left);
                        decref(store, right);
                    }
                    _ => {}
                }
            }
            Vacant(entry) => {
                entry.insert(StoreEntry {
                    node,
                    references: 1,
                });
            }
        }
    }
}

fn decref<Key, Value>(store: &mut Store<Key, Value>, label: Label)
where
    Key: Field,
    Value: Field,
{
    if !label.is_empty() {
        match store.entry(label) {
            Occupied(mut entry) => {
                let value = entry.get_mut();
                value.references -= 1;

                if value.references == 0 {
                    entry.remove_entry();
                }
            }
            Vacant(_) => unreachable!(),
        }
    }
}

#[async_recursion]
async fn branch<Key, Value>(
    store: Store<Key, Value>,
    original: Option<&'async_recursion Entry<Key, Value>>,
    preserve: bool,
    depth: u8,
    batch: &Batch<Key, Value>,
    chunk: Chunk,
    left: Entry<Key, Value>,
    right: Entry<Key, Value>,
) -> (Store<Key, Value>, Label)
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

    let (mut store, left, right) = match store.split() {
        Split::Split(left_store, right_store) => {
            let (left_chunk, right_chunk) =
                (chunk.left(batch), chunk.right(batch));
            let (left_batch, right_batch) = (batch.clone(), batch.clone());

            let left_task = tokio::spawn(async move {
                recur(
                    left_store,
                    left,
                    preserve_branches,
                    depth + 1,
                    &left_batch,
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
                    &right_batch,
                    right_chunk,
                )
                .await
            });

            let (left_join, right_join) = tokio::join!(left_task, right_task);

            let (left_store, left_label) = left_join.unwrap();
            let (right_store, right_label) = right_join.unwrap();

            let store = Store::merge(left_store, right_store);
            (store, left_label, right_label)
        }
        Split::Unsplittable(store) => {
            let (store, left_label) = recur(
                store,
                left,
                preserve_branches,
                depth + 1,
                batch,
                chunk.left(batch),
            )
            .await;

            let (store, right_label) = recur(
                store,
                right,
                preserve_branches,
                depth + 1,
                batch,
                chunk.right(batch),
            )
            .await;

            (store, left_label, right_label)
        }
    };

    let new = match (left, right) {
        (Label::Empty, Label::Empty) => Label::Empty,
        (Label::Empty, Label::Leaf(map, hash))
        | (Label::Leaf(map, hash), Label::Empty) => Label::Leaf(map, hash),
        (left, right) => {
            let node = Node::<Key, Value>::Internal(left, right);
            match original {
                Some(original) if node == original.node => {
                    // Unchanged `original`
                    original.label
                }
                _ => {
                    // New or modified `original`

                    let label = store.label(&node);
                    incref(&mut store, label, node);
                    label
                }
            }
        }
    };

    if let Some(original) = original {
        if new != original.label && !preserve {
            decref(&mut store, original.label);
        }
    }

    (store, new)
}

#[async_recursion]
async fn recur<Key, Value>(
    mut store: Store<Key, Value>,
    target: Entry<Key, Value>,
    preserve: bool,
    depth: u8,
    batch: &Batch<Key, Value>,
    chunk: Chunk,
) -> (Store<Key, Value>, Label)
where
    Key: Field,
    Value: Field,
{
    match (&target.node, chunk.task(batch)) {
        (_, Task::Pass) => (store, target.label),

        (Node::Empty, Task::Do(operation)) => match &operation.action {
            Action::Set(value) => {
                let node = Node::Leaf(operation.key.clone(), value.clone());
                let label = store.label(&node);

                incref(&mut store, label, node);
                (store, label)
            }
            Action::Remove => (store, Label::Empty),
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
            match &operation.action {
                Action::Set(new_value) if new_value != original_value => {
                    let node =
                        Node::Leaf(operation.key.clone(), new_value.clone());
                    let label = store.label(&node);
                    incref(&mut store, label, node);

                    if !preserve {
                        decref(&mut store, target.label);
                    }

                    (store, label)
                }
                Action::Set(_) => (store, target.label),
                Action::Remove => {
                    if !preserve {
                        decref(&mut store, target.label);
                    }

                    (store, Label::Empty)
                }
            }
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
    batch: &Batch<Key, Value>,
) -> (Store<Key, Value>, Label)
where
    Key: Field,
    Value: Field,
{
    let root = get(&mut store, root);
    recur(store, root, false, 0, batch, Chunk::root(batch)).await
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::operation::Operation;
    use super::super::wrap::Wrap;

    fn get(store: &mut Store<u32, u32>, label: Label) -> Node<u32, u32> {
        match store.entry(label) {
            Occupied(entry) => entry.get().node.clone(),
            Vacant(..) => unreachable!(),
        }
    }

    fn get_internal(
        store: &mut Store<u32, u32>,
        label: Label,
    ) -> (Label, Label) {
        match get(store, label) {
            Node::Internal(left, right) => (left, right),
            _ => unreachable!(),
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

    #[tokio::test]
    async fn single_map() {
        let store = Store::<u32, u32>::new();

        // {0: 1}

        let batch = Batch::new(vec![set(0, 1)]);
        let (mut store, root) = traverse(store, Label::Empty, &batch).await;
        assert_eq!(get(&mut store, root), leaf(0, 1));

        // {0: 0}

        let batch = Batch::new(vec![set(0, 0)]);
        let (mut store, root) = traverse(store, root, &batch).await;
        assert_eq!(get(&mut store, root), leaf(0, 0));

        // {0: 0, 1: 0}

        let batch = Batch::new(vec![set(1, 0)]);
        let (mut store, root) = traverse(store, root, &batch).await;

        let (l, r) = get_internal(&mut store, root);
        assert_eq!(r, Label::Empty);

        let (ll, lr) = get_internal(&mut store, l);
        assert_eq!(lr, Label::Empty);

        let (lll, llr) = get_internal(&mut store, ll);
        assert_eq!(get(&mut store, lll), leaf(0, 0));
        assert_eq!(get(&mut store, llr), leaf(1, 0));

        // {1: 1}

        let batch = Batch::new(vec![set(1, 1), remove(0)]);
        let (mut store, root) = traverse(store, root, &batch).await;
        assert_eq!(get(&mut store, root), leaf(1, 1));

        // {}

        let batch = Batch::new(vec![remove(1)]);
        let (store, root) = traverse(store, root, &batch).await;
        assert_eq!(root, Label::Empty);

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
        let (mut store, root) = traverse(store, root, &batch).await;

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
}
