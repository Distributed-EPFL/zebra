use crate::{
    common::store::Field,
    database::{errors::SyncError, Collection, CollectionAnswer, Question, TableSender},
};

use doomstack::Top;

pub struct CollectionSender<Item: Field>(pub(crate) TableSender<Item, ()>);

impl<Item> CollectionSender<Item>
where
    Item: Field,
{
    pub fn hello(&mut self) -> CollectionAnswer<Item> {
        self.0.hello()
    }

    pub fn answer(
        &mut self,
        question: &Question,
    ) -> Result<CollectionAnswer<Item>, Top<SyncError>> {
        self.0.answer(question)
    }

    pub fn end(self) -> Collection<Item> {
        Collection(self.0.end())
    }
}
