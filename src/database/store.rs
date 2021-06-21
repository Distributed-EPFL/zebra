use drop::crypto::hash;

use std::collections::hash_map::Entry as HashMapEntry;
use std::collections::HashMap;
use std::iter;
use std::ops::Range;
use std::sync::Arc;

use super::bytes::Bytes;
use super::entry::Entry;
use super::field::Field;
use super::label::Label;
use super::map_id::MapId;
use super::node::Node;

pub(crate) type EntryMap<Key, Value> = HashMap<Bytes, Entry<Key, Value>>;
pub(crate) type EntryMapEntry<'a, Key, Value> =
    HashMapEntry<'a, Bytes, Entry<Key, Value>>;

pub(crate) const DEPTH: u8 = 8;

pub(crate) struct Store<Key: Field, Value: Field> {
    maps: Arc<Vec<EntryMap<Key, Value>>>,
    splits: u8,
    range: Range<usize>,
}

pub(crate) enum Split<Key: Field, Value: Field> {
    Split(Store<Key, Value>, Store<Key, Value>),
    Unsplittable(Store<Key, Value>),
}

impl<Key, Value> Store<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub fn new() -> Self {
        Store {
            maps: Arc::new(
                iter::repeat_with(|| EntryMap::new())
                    .take(1 << DEPTH)
                    .collect(),
            ),
            splits: 0,
            range: 0..(1 << DEPTH),
        }
    }

    pub fn merge(left: Self, right: Self) -> Self {
        debug_assert_eq!(left.splits, right.splits);
        debug_assert_eq!(right.range.end, left.range.start);

        Store {
            maps: left.maps,
            splits: left.splits - 1,
            range: right.range.start..left.range.end,
        }
    }

    pub fn split(self) -> Split<Key, Value> {
        if self.splits < DEPTH {
            let start = self.range.start;
            let end = self.range.end;
            let mid = start + (1 << (DEPTH - self.splits - 1));

            let right = Store {
                maps: self.maps.clone(),
                splits: self.splits + 1,
                range: start..mid,
            };

            let left = Store {
                maps: self.maps.clone(),
                splits: self.splits + 1,
                range: mid..end,
            };

            Split::Split(left, right)
        } else {
            Split::Unsplittable(self)
        }
    }

    pub fn size(&self) -> usize {
        debug_assert_eq!(self.splits, 0);
        self.maps.iter().map(|map| map.len()).sum()
    }

    pub fn entry(&mut self, label: Label) -> EntryMapEntry<Key, Value> {
        let map = label.map().id();
        let hash = *label.hash();

        debug_assert!(self.range.contains(&map));

        unsafe {
            let map = &self.maps[map];
            let map =
                map as *const EntryMap<Key, Value> as *mut EntryMap<Key, Value>;
            let map = &mut *map;

            map.entry(hash)
        }
    }

    pub fn label(&self, node: &Node<Key, Value>) -> Label {
        match node {
            Node::Empty => Label::Empty,
            Node::Internal(..) => {
                let map = MapId::internal(self.range.start);
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
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::direction::Direction;
    use super::super::entry::Entry;
    use super::super::node::Node;
    use super::super::path::Path;
    use super::super::wrap::Wrap;

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
