use crate::{
    common::store::Field,
    database::{
        store::{Cell, Store},
        Receiver, Table,
    },
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

    use crate::database::{store::Label, Transaction};

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

        pub(crate) fn check<'a, I, J>(&self, tables: I, receivers: J)
        where
            I: IntoIterator<Item = &'a Table<Key, Value>>,
            J: IntoIterator<Item = &'a Receiver<Key, Value>>,
        {
            let tables: Vec<&'a Table<Key, Value>> =
                tables.into_iter().collect();

            let receivers: Vec<&'a Receiver<Key, Value>> =
                receivers.into_iter().collect();

            for table in &tables {
                table.check_tree();
            }

            let table_held = tables.iter().map(|table| table.root());

            let receiver_held =
                receivers.iter().map(|receiver| receiver.held()).flatten();

            let held: Vec<Label> = table_held.chain(receiver_held).collect();

            let mut store = self.store.take();
            store.check_leaks(held.clone());
            store.check_references(held.clone());
            self.store.restore(store);
        }
    }
}
