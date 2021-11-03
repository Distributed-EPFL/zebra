use crate::{
    common::store::Field,
    database::{Collection, CollectionReceiver, Database},
};

#[derive(Clone)]
pub struct Family<Item: Field>(pub(crate) Database<Item, ()>);

impl<Item> Family<Item>
where
    Item: Field,
{
    pub fn new() -> Self {
        Family(Database::new())
    }

    pub fn empty_collection(&self) -> Collection<Item> {
        Collection(self.0.empty_table())
    }

    pub fn receive(&self) -> CollectionReceiver<Item> {
        CollectionReceiver(self.0.receive())
    }
}
