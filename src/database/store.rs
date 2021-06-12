use serde::Serialize;

use std::collections::hash_map::Entry as HashMapEntry;
use std::collections::HashMap;
use std::iter;
use std::vec::Vec;

use super::bytes::Bytes;
use super::entry::Entry;
use super::label::Label;

pub(crate) type EntryMap<Key, Value> = HashMap<Bytes, Entry<Key, Value>>;
pub(crate) type EntryMapEntry<'a, Key, Value> =
    HashMapEntry<'a, Bytes, Entry<Key, Value>>;

pub(crate) struct Store<Key: Serialize, Value: Serialize> {
    depth: u8,
    maps: Vec<EntryMap<Key, Value>>,
    splits: u8,
}

pub(crate) enum Split<Key: Serialize, Value: Serialize> {
    Split(Store<Key, Value>, Store<Key, Value>),
    Unsplittable(Store<Key, Value>),
}

impl<Key, Value> Store<Key, Value>
where
    Key: Serialize,
    Value: Serialize,
{
    pub fn with_depth(depth: u8) -> Self {
        Store {
            depth,
            maps: iter::repeat_with(|| EntryMap::new())
                .take(1 << depth)
                .collect(),
            splits: 0,
        }
    }

    pub fn split(mut self) -> Split<Key, Value> {
        if self.splits < self.depth {
            let left = Store {
                depth: self.depth,
                maps: self.maps.split_off(self.maps.len() >> 1),
                splits: self.splits + 1,
            };

            let right = Store {
                depth: self.depth,
                maps: self.maps,
                splits: self.splits + 1,
            };

            Split::Split(left, right)
        } else {
            Split::Unsplittable(self)
        }
    }

    pub fn entry(&mut self, label: &Label) -> EntryMapEntry<Key, Value> {
        let (map, hash) = match label {
            Label::Internal(hash) => (0, hash),
            Label::Leaf(map, hash) => (map.crop(self.depth, self.splits), hash),
            Label::Empty => {
                panic!("called `Store::entry()` on an `Empty` value");
            }
        };

        self.maps[map].entry(*hash)
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

        match store.entry(&label) {
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

            match store.entry(&label) {
                EntryMapEntry::Occupied(..) => {}
                _ => {
                    unreachable!();
                }
            }
        }
    }
}
