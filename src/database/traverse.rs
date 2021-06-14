use serde::Serialize;

use std::collections::hash_map::Entry::{Occupied, Vacant};

use super::batch::Batch;
use super::entry::Entry as StoreEntry;
use super::label::Label;
use super::node::Node;
use super::store::{Split, Store};

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

struct Entry<Key: Serialize, Value: Serialize> {
    label: Label,
    node: Node<Key, Value>,
    references: References,
}

impl<Key, Value> Entry<Key, Value>
where
    Key: Serialize,
    Value: Serialize,
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
    Key: Serialize,
    Value: Serialize,
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
    Key: Serialize,
    Value: Serialize,
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
    Key: Serialize,
    Value: Serialize,
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

async fn branch<Key, Value>(
    mut store: Store<Key, Value>,
    original: Option<&Entry<Key, Value>>,
    preserve: bool,
    depth: u8,
    batch: Batch<'_, Key, Value>,
    left: Entry<Key, Value>,
    right: Entry<Key, Value>,
) where
    Key: Serialize,
    Value: Serialize,
{
    let preserve_branches = preserve
        || if let Some(original) = original {
            original.references.multiple()
        } else {
            false
        };

    match store.split() {
        Split::Split(left_store, right_store) => {
            let left_task = tokio::spawn(async move {
                left_store;
            });
        }
        Split::Unsplittable(store) => {}
    }
}

async fn recur<Key, Value>(
    mut store: Store<Key, Value>,
    target: Entry<Key, Value>,
    preserve: bool,
    depth: u8,
    batch: Batch<'_, Key, Value>,
) where
    Key: Serialize,
    Value: Serialize,
{
}
