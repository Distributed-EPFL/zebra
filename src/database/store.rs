use std::collections::hash_map::Entry as HashMapEntry;
use std::collections::HashMap;
use std::iter;
use std::ops::Range;
use std::sync::Arc;

use super::bytes::Bytes;
use super::entry::Entry;
use super::field::Field;
use super::label::Label;

pub(crate) type EntryMap<Key, Value> = HashMap<Bytes, Entry<Key, Value>>;
pub(crate) type EntryMapEntry<'a, Key, Value> =
    HashMapEntry<'a, Bytes, Entry<Key, Value>>;

pub(crate) struct Store<Key: Field, Value: Field> {
    depth: u8,
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
    pub fn with_depth(depth: u8) -> Self {
        Store {
            depth,
            maps: Arc::new(
                iter::repeat_with(|| EntryMap::new())
                    .take(1 << depth)
                    .collect(),
            ),
            splits: 0,
            range: 0..(1 << depth),
        }
    }

    pub fn merge(left: Self, right: Self) -> Self {
        debug_assert_eq!(left.depth, right.depth);
        debug_assert_eq!(left.splits, right.splits);
        debug_assert_eq!(right.range.end, left.range.start);

        Store {
            depth: left.depth,
            maps: left.maps,
            splits: left.splits - 1,
            range: right.range.start..left.range.end,
        }
    }

    pub fn split(self) -> Split<Key, Value> {
        if self.splits < self.depth {
            let start = self.range.start;
            let end = self.range.end;
            let mid = start + (1 << (self.depth - self.splits - 1));

            let right = Store {
                depth: self.depth,
                maps: self.maps.clone(),
                splits: self.splits + 1,
                range: start..mid,
            };

            let left = Store {
                depth: self.depth,
                maps: self.maps.clone(),
                splits: self.splits + 1,
                range: mid..end,
            };

            Split::Split(left, right)
        } else {
            Split::Unsplittable(self)
        }
    }

    pub fn entry(&mut self, label: Label) -> EntryMapEntry<Key, Value> {
        let (map, hash) = match label {
            Label::Internal(hash) => (0, hash),
            Label::Leaf(map, hash) => (map.crop(self.depth, self.splits), hash),
            Label::Empty => {
                panic!("called `Store::entry()` on an `Empty` value");
            }
        };

        unsafe {
            let map = &self.maps[self.range.start + map];
            let map =
                map as *const EntryMap<Key, Value> as *mut EntryMap<Key, Value>;
            let map = &mut *map;

            map.entry(hash)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::direction::Direction;
    use super::super::entry::Entry;
    use super::super::label;
    use super::super::node::Node;
    use super::super::path::Path;
    use super::super::wrap::Wrap;

    fn store_with_records(
        mut keys: Vec<u32>,
        mut values: Vec<u32>,
    ) -> (Store<u32, u32>, Vec<Label>) {
        let mut store = Store::<u32, u32>::with_depth(8);

        let labels = keys
            .drain(..)
            .zip(values.drain(..))
            .map(|(key, value)| {
                let key = Wrap::new(key).unwrap();
                let value = Wrap::new(value).unwrap();

                let node = Node::Leaf(key, value);
                let label = label::label(&node);

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
    fn leaf_consistency() {
        let key = Wrap::new(0u32).unwrap();
        let value = Wrap::new(1u32).unwrap();

        let node = Node::Leaf(key, value);
        let label = label::label(&node);
        let path: Path = (*label.bytes()).into();

        let entry = Entry {
            node,
            references: 1,
        };

        let mut store = Store::<u32, u32>::with_depth(8);

        match store.entry(label) {
            EntryMapEntry::Vacant(entrymapentry) => {
                entrymapentry.insert(entry);
            }
            _ => {
                unreachable!();
            }
        }

        for depth in 0..=255 {
            store = match store.split() {
                Split::Split(left, right) => {
                    if path[depth] == Direction::Left {
                        left
                    } else {
                        right
                    }
                }
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
    fn merge_safety() {
        let (store, labels) = store_with_records(
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8],
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8],
        );
        let (left, right) = match store.split() {
            Split::Split(l, r) => (l, r),
            Split::Unsplittable(..) => unreachable!(),
        };

        let mut store = Store::merge(left, right);

        for i in 0..=8 {
            match store.entry(labels[i]) {
                EntryMapEntry::Occupied(..) => {}
                _ => {
                    unreachable!();
                }
            }
        }
    }
}
