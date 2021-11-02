use crate::{
    common::{store::Field, Commitment},
    database::{
        CollectionResponse, CollectionSender, CollectionTransaction, Table,
    },
};

pub struct Collection<Item: Field>(pub(crate) Table<Item, ()>);

impl<Item> Collection<Item>
where
    Item: Field,
{
    pub fn commit(&self) -> Commitment {
        self.0.commit()
    }

    pub async fn execute(
        &mut self,
        transaction: CollectionTransaction<Item>,
    ) -> CollectionResponse<Item> {
        CollectionResponse(self.0.execute(transaction.0).await)
    }

    pub fn send(self) -> CollectionSender<Item> {
        self.0.send().into()
    }
}

impl<Item> Clone for Collection<Item>
where
    Item: Field,
{
    fn clone(&self) -> Self {
        Collection(self.0.clone())
    }
}
