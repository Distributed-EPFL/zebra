use crate::{
    common::store::Field,
    database::{Collection, Database},
};

#[derive(Clone)]
pub struct Family<Item: Field>(Database<Item, ()>);

impl<Item> Family<Item>
where
    Item: Field,
{
    pub fn new() -> Self {
        Family(Database::new())
    }

    pub fn empty_collection(&self) -> Collection<Item> {
        Collection::from_table(self.0.empty_table())
    }
}
