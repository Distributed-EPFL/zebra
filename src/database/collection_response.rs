use crate::{
    common::store::Field,
    database::{CollectionQuery, DatabaseResponse},
};

pub struct CollectionResponse<Item: Field>(
    pub(crate) DatabaseResponse<Item, ()>,
);

impl<Item> CollectionResponse<Item>
where
    Item: Field,
{
    pub fn contains(&self, query: &CollectionQuery) -> bool {
        self.0.get(&query.0).is_some()
    }
}
