use crate::{
    common::{store::Field, Commitment},
    database::Table,
};

pub struct Collection<Item: Field>(Table<Item, ()>);

impl<Item> Collection<Item>
where
    Item: Field,
{
    pub(crate) fn from_table(table: Table<Item, ()>) -> Self {
        Collection(table)
    }

    pub fn commit(&self) -> Commitment {
        self.0.commit()
    }
}
