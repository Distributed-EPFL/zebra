use crate::{
    common::store::Field,
    map::{errors::MapError, Map},
};

use doomstack::Top;

use serde::{Deserialize, Serialize};

use std::{
    borrow::Borrow,
    fmt::{Debug, Error, Formatter},
};

use talk::crypto::primitives::hash::Hash;

#[derive(Clone, Serialize, Deserialize)]
pub struct Set<Item: Field>(Map<Item, ()>);

impl<Item> Set<Item>
where
    Item: Field,
{
    pub fn new() -> Self {
        Set(Map::new())
    }

    pub fn root_stub(commitment: Hash) -> Self {
        Set(Map::root_stub(commitment))
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

    pub fn export<I, K>(&self, keys: I) -> Result<Set<Item>, Top<MapError>>
    where
        Item: Clone,
        I: IntoIterator<Item = K>,
        K: Borrow<Item>,
    {
        Ok(Set(self.0.export(keys)?))
    }

    pub fn import(&mut self, other: Set<Item>) -> Result<(), Top<MapError>> {
        self.0.import(other.0)
    }
}

impl<Item> Debug for Set<Item>
where
    Item: Field,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "Set(commitment: {:?})", self.commit())
    }
}
