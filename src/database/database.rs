use super::field::Field;
use super::store::Store;
use super::table::Table;

use std::sync::{Arc, Mutex};

pub struct Database<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub(crate) store: Arc<Mutex<Option<Store<Key, Value>>>>,
}

impl<Key, Value> Database<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub fn new() -> Self {
        Database {
            store: Arc::new(Mutex::new(Some(Store::new()))),
        }
    }

    pub fn empty_table(&self) -> Table<Key, Value> {
        Table::new(self)
    }
}

impl<Key, Value> Clone for Database<Key, Value>
where
    Key: Field,
    Value: Field,
{
    fn clone(&self) -> Self {
        Database {
            store: self.store.clone(),
        }
    }
}
