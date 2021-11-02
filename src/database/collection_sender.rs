use crate::database::TableSender;

pub type CollectionSender<Item> = TableSender<Item, ()>;
