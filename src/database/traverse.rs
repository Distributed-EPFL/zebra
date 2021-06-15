use async_recursion::async_recursion;

use std::collections::hash_map::Entry::{Occupied, Vacant};

use super::action::Action;
use super::batch::Batch;
use super::chunk::Chunk;
use super::direction::Direction;
use super::entry::Entry as StoreEntry;
use super::field::Field;
use super::label;
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

            let (left_store, left_label) = left_task.await.unwrap();
            let (right_store, right_label) = right_task.await.unwrap();

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

                    let label = label::label(&node);
                    incref(&mut store, label, node);
                    Label::Internal(*label.bytes())
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
                let label = label::label(&node);

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
                    let label = label::label(&node);
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
        (Node::Leaf(..), _) => {
            let (left, right) = if Path::from(*target.label.bytes())[depth]
                == Direction::Left
            {
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

pub(super) fn traverse<Key, Value>(
    mut store: Store<Key, Value>,
    root: Label,
    batch: &Batch<Key, Value>,
) where
    Key: Field,
    Value: Field,
{
    let root = get(&mut store, root);
    recur(store, root, false, 0, batch, Chunk::root(batch));
}
