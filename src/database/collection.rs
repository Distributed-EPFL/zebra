use crate::{
    common::{store::Field, Commitment},
    database::{
        CollectionResponse, CollectionSender, CollectionTransaction, Table,
    },
};

use std::collections::HashSet;
use std::hash::Hash;

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

    pub async fn diff(
        lho: &mut Collection<Item>,
        rho: &mut Collection<Item>,
    ) -> HashSet<Item>
    where Item: Clone + Eq + Hash {
        Table::diff(&mut lho.0, &mut rho.0).await.into_iter().map(|(key,_)| key).collect()
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
