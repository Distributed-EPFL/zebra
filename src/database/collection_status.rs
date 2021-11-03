use crate::{
    common::store::Field,
    database::{Collection, CollectionReceiver, Question},
};

pub enum CollectionStatus<Item: Field> {
    Complete(Collection<Item>),
    Incomplete(CollectionReceiver<Item>, Question),
}
