use crate::{
    common::{store::Field, Commitment},
    database::Table,
};

pub struct Collection<Item: Field>(pub(crate) Table<Item, ()>);

impl<Item> Collection<Item>
where
    Item: Field,
{
    pub fn commit(&self) -> Commitment {
        self.0.commit()
    }
}
