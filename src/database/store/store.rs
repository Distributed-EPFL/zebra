use crate::database::{
    data::Bytes,
    store::{Entry, Field, Label, MapId, Node, Split},
};

use drop::crypto::hash;

use oh_snap::Snap;

use std::collections::hash_map::Entry as HashMapEntry;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::iter;

pub(crate) type EntryMap<Key, Value> = HashMap<Bytes, Entry<Key, Value>>;
pub(crate) type EntryMapEntry<'a, Key, Value> =
    HashMapEntry<'a, Bytes, Entry<Key, Value>>;

pub(crate) const DEPTH: u8 = 8;

pub(crate) struct Store<Key: Field, Value: Field> {
    maps: Snap<EntryMap<Key, Value>>,
    splits: u8,
}

impl<Key, Value> Store<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub fn new() -> Self {
        Store {
            maps: Snap::new(
                iter::repeat_with(|| EntryMap::new())
                    .take(1 << DEPTH)
                    .collect(),
            ),
            splits: 0,
        }
    }

    pub fn merge(left: Self, right: Self) -> Self {
        Store {
            maps: Snap::merge(right.maps, left.maps),
            splits: left.splits - 1,
        }
    }

    pub fn split(self) -> Split<Key, Value> {
        if self.splits < DEPTH {
            let mid = 1 << (DEPTH - self.splits - 1);

            let (right_maps, left_maps) = self.maps.snap(mid); // `oh-snap` stores the lowest-index elements in `left`, while `zebra` stores them in `right`, hence the swap

            let left = Store {
                maps: left_maps,
                splits: self.splits + 1,
            };

            let right = Store {
                maps: right_maps,
                splits: self.splits + 1,
            };

            Split::Split(left, right)
        } else {
            Split::Unsplittable(self)
        }
    }

    #[cfg(test)]
    pub fn size(&self) -> usize {
        debug_assert!(self.maps.is_complete());
        self.maps.iter().map(|map| map.len()).sum()
    }

    pub fn entry(&mut self, label: Label) -> EntryMapEntry<Key, Value> {
        let map = label.map().id() - self.maps.range().start;
        let hash = *label.hash();
        self.maps[map].entry(hash)
    }

    pub fn label(&self, node: &Node<Key, Value>) -> Label {
        match node {
            Node::Empty => Label::Empty,
            Node::Internal(..) => {
                let map = MapId::internal(self.maps.range().start);
                let hash = hash::hash(&node).unwrap().into();
                Label::Internal(map, hash)
            }
            Node::Leaf(key, _) => {
                let map = MapId::leaf(&key.digest());
                let hash: Bytes = hash::hash(&node).unwrap().into();
                Label::Leaf(map, hash)
            }
        }
    }

    pub fn populate(&mut self, label: Label, node: Node<Key, Value>) -> bool
    where
        Key: Field,
        Value: Field,
    {
        if !label.is_empty() {
            match self.entry(label) {
                Vacant(entry) => {
                    entry.insert(Entry {
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

    pub fn incref(&mut self, label: Label)
    where
        Key: Field,
        Value: Field,
    {
        if !label.is_empty() {
            match self.entry(label) {
                Occupied(mut entry) => {
                    entry.get_mut().references += 1;
                }
                Vacant(..) => panic!("called `incref` on non-existing node"),
            }
        }
    }

    pub fn decref(
        &mut self,
        label: Label,
        preserve: bool,
    ) -> Option<Node<Key, Value>>
    where
        Key: Field,
        Value: Field,
    {
        if !label.is_empty() {
            match self.entry(label) {
                Occupied(mut entry) => {
                    let value = entry.get_mut();
                    value.references -= 1;

                    if value.references == 0 && !preserve {
                        let (_, entry) = entry.remove_entry();
                        Some(entry.node)
                    } else {
                        None
                    }
                }
                Vacant(..) => panic!("called `decref` on non-existing node"),
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::database::{
        store::{Entry, Node, Wrap},
        tree::{Direction, Path},
    };

    fn store_with_records(
        mut keys: Vec<u32>,
        mut values: Vec<u32>,
    ) -> (Store<u32, u32>, Vec<Label>) {
        let mut store = Store::<u32, u32>::new();

        let labels = keys
            .drain(..)
            .zip(values.drain(..))
            .map(|(key, value)| {
                let key = Wrap::new(key).unwrap();
                let value = Wrap::new(value).unwrap();

                let node = Node::Leaf(key, value);
                let label = store.label(&node);

                let entry = Entry {
                    node,
                    references: 1,
                };

                match store.entry(label) {
                    EntryMapEntry::Vacant(entrymapentry) => {
                        entrymapentry.insert(entry);
                    }
                    _ => {
                        unreachable!();
                    }
                }

                label
            })
            .collect();

        (store, labels)
    }

    #[test]
    fn split() {
        let mut store = Store::<u32, u32>::new();

        let key = Wrap::new(0u32).unwrap();
        let value = Wrap::new(1u32).unwrap();

        let path: Path = (*key.digest()).into();
        let node = Node::Leaf(key, value);
        let label = store.label(&node);

        let entry = Entry {
            node,
            references: 1,
        };

        match store.entry(label) {
            EntryMapEntry::Vacant(entrymapentry) => {
                entrymapentry.insert(entry);
            }
            _ => {
                unreachable!();
            }
        }

        for splits in 0..DEPTH {
            store = match store.split() {
                Split::Split(left, right) => {
                    if path[splits] == Direction::Left {
                        left
                    } else {
                        right
                    }
                }
                Split::Unsplittable(_) => unreachable!(),
            };

            match store.entry(label) {
                EntryMapEntry::Occupied(..) => {}
                _ => {
                    unreachable!();
                }
            }
        }

        for _ in DEPTH..=255 {
            store = match store.split() {
                Split::Split(_, _) => unreachable!(),
                Split::Unsplittable(store) => store,
            };

            match store.entry(label) {
                EntryMapEntry::Occupied(..) => {}
                _ => {
                    unreachable!();
                }
            }
        }
    }

    #[test]
    fn merge() {
        let keys = vec![0, 1, 2, 3, 4, 5, 6, 7, 8];
        let values = keys.clone();

        let (store, labels) = store_with_records(keys.clone(), values.clone());

        let (l, r) = match store.split() {
            Split::Split(l, r) => (l, r),
            Split::Unsplittable(..) => unreachable!(),
        };

        let (ll, lr) = match l.split() {
            Split::Split(l, r) => (l, r),
            Split::Unsplittable(..) => unreachable!(),
        };

        let (rl, rr) = match r.split() {
            Split::Split(l, r) => (l, r),
            Split::Unsplittable(..) => unreachable!(),
        };

        let l = Store::merge(ll, lr);
        let r = Store::merge(rl, rr);

        let mut store = Store::merge(l, r);

        for i in 0..=8 {
            match store.entry(labels[i]) {
                EntryMapEntry::Occupied(entry) => match &entry.get().node {
                    Node::Leaf(key, value) => {
                        assert_eq!(*key, Wrap::new(keys[i]).unwrap());
                        assert_eq!(*value, Wrap::new(values[i]).unwrap());
                    }
                    _ => unreachable!(),
                },
                _ => {
                    unreachable!();
                }
            }
        }
    }

    #[test]
    fn size() {
        let store = Store::<u32, u32>::new();
        assert_eq!(store.size(), 0);

        let keys = vec![0, 1, 2, 3, 4, 5, 6, 7, 8];
        let values = keys.clone();

        let (store, _) = store_with_records(keys.clone(), values.clone());
        assert_eq!(store.size(), 9);
    }
}
