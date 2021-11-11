use crate::{
    common::store::Field,
    map::{errors::MapError, Map},
};

use doomstack::Top;

use talk::crypto::primitives::hash::Hash;

pub struct Set<Item: Field>(Map<Item, ()>);

impl<Item> Set<Item>
where
    Item: Field,
{
    pub fn new() -> Self {
        Set(Map::new())
    }

    pub fn commit(&self) -> Hash {
        self.0.commit()
    }

    pub fn contains(&self, item: &Item) -> Result<bool, Top<MapError>> {
        Ok(self.0.get(item)?.is_some())
    }

    pub fn insert(&mut self, item: Item) -> Result<bool, Top<MapError>> {
        Ok(self.0.insert(item, ())?.is_none())
    }

    pub fn remove(&mut self, item: &Item) -> Result<bool, Top<MapError>> {
        Ok(self.0.remove(item)?.is_some())
    }
}
