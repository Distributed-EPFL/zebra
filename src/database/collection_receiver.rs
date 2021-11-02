use crate::database::TableReceiver;

pub type CollectionReceiver<Item> = TableReceiver<Item, ()>;
