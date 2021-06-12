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

impl<Key, Value> Store<Key, Value>
where
    Key: Serialize,
    Value: Serialize,
{
    fn with_depth(depth: u8) -> Self {
        Store {
            depth,
            maps: iter::repeat_with(|| EntryMap::new())
                .take(1 << depth)
                .collect(),
            splits: 0,
        }
    }

    fn split(mut self) -> (Self, Self) {
        let right = Store {
            depth: self.depth,
            maps: self.maps.split_off(self.maps.len() >> 1),
            splits: self.splits + 1,
        };

        let left = Store {
            depth: self.depth,
            maps: self.maps,
            splits: self.splits + 1,
        };

        (left, right)
    }

    fn entry(&mut self, label: &Label) -> EntryMapEntry<Key, Value> {
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
