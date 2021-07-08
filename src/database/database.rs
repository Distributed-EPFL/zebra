use crate::database::{
    store::{Cell, Field, Store},
    Receiver, Table,
};

pub struct Database<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub(crate) store: Cell<Key, Value>,
}

impl<Key, Value> Database<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub fn new() -> Self {
        Database {
            store: Cell::new(Store::new()),
        }
    }

    pub fn empty_table(&self) -> Table<Key, Value> {
        Table::empty(self.store.clone())
    }

    pub fn receive(&self) -> Receiver<Key, Value> {
        Receiver::new(self.store.clone())
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::database::Transaction;

    impl<Key, Value> Database<Key, Value>
    where
        Key: Field,
        Value: Field,
    {
        pub(crate) async fn table_with_records<I>(
            &self,
            records: I,
        ) -> Table<Key, Value>
        where
            I: IntoIterator<Item = (Key, Value)>,
        {
            let mut table = self.empty_table();
            let mut transaction = Transaction::new();

            for (key, value) in records {
                transaction.set(key, value).unwrap();
            }

            table.execute(transaction).await;
            table
        }
    }
}
