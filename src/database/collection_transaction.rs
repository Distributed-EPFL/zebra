use crate::{
    common::store::Field,
    database::{errors::QueryError, CollectionQuery, DatabaseTransaction},
};

use doomstack::Top;

pub struct CollectionTransaction<Item: Field>(DatabaseTransaction<Item, ()>);

impl<Item> CollectionTransaction<Item>
where
    Item: Field,
{
    pub fn new() -> Self {
        CollectionTransaction(DatabaseTransaction::new())
    }

    pub fn contains(
        &mut self,
        item: &Item,
    ) -> Result<CollectionQuery, Top<QueryError>> {
        Ok(CollectionQuery(self.0.get(item)?))
    }

    pub fn insert(&mut self, item: Item) -> Result<(), Top<QueryError>> {
        self.0.set(item, ())
    }

    pub fn remove(&mut self, item: &Item) -> Result<(), Top<QueryError>> {
        self.0.remove(item)
    }
}
