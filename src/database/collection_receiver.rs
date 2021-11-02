use crate::database::{TableReceiver, TableStatus};

pub type CollectionReceiver<Item> = TableReceiver<Item, ()>;
pub type CollectionStatus<Item> = TableStatus<Item, ()>;
