use crate::{
    common::store::Field,
    database::{Query, TableResponse},
};

pub struct CollectionResponse<Item: Field>(pub(crate) TableResponse<Item, ()>);

impl<Item> CollectionResponse<Item>
where
    Item: Field,
{
    pub fn contains(&self, query: &Query) -> bool {
        self.0.get(&query).is_some()
    }
}
