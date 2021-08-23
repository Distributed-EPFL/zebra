use crate::{
    common::store::Field,
    database::{
        store::{Cell, Store},
        Receiver, Table,
    },
};

/// A datastrucure for memory-efficient storage and transfer of maps with a
/// large degree of similarity (% of key-pairs in common).
///
/// A database maintains a collection of [`Table`]s which in turn represent
/// a collection of key-value pairs. A [`Table`] can be read and modified by
/// creating and executing a [`Transaction`].
///
/// We optimize for the following use cases:
/// 1) Storing multiple maps with a lot of similarities (e.g. snapshots in a system)
/// 2) Transfering maps to databases with similar maps
/// 3) Applying large batches of operations (read, write, remove) to a single map
/// ([`Table`]). In particular, within a batch, we apply operations concurrently
/// and with minimal synchronization between threads.
///
/// The default hashing algorithm is currently Blake3, though this is
/// subject to change at any point in the future.
///
/// It is required that the keys implement `'static` and the [`Serialize`],
/// [`Send`] and [`Sync`] traits.
///
/// [`Field`]: crate::common::store::Field
/// [`Table`]: crate::database::Table
/// [`Transaction`]: crate::database::Transaction
/// [`Serialize`]: serde::Serialize
/// [`Send`]: Send
/// [`Sync`]: Sync
///
/// # Examples
///
/// ```rust
///
/// use zebra::database::{Database, Table, Transaction, Response, Query};
///
/// #[tokio::main]
/// async fn main() {
///     // Type inference lets us omit an explicit type signature (which
///     // would be `Database<&str, integer>` in this example).
///     let database = Database::new();
///
///     // We create a new transaction. See [`Transaction`] for more details.
///     let mut modify = Transaction::new();
///     modify.set("Alice", 42).unwrap();
///
///     let mut table = database.empty_table();
///     let _ = table.execute(modify).await;
///
///     let mut read = Transaction::new();
///     let query_key = read.get(&"Alice").unwrap();
///     let response = table.execute(read).await;
///
///     assert_eq!(response.get(&query_key), Some(&42));
///
///     // Let's remove "Alice" and set "Bob".
///     let mut modify = Transaction::new();
///     modify.remove(&"Alice").unwrap();
///     modify.set(&"Bob", 23).unwrap();
///
///     // Ignore the response (modify only)
///     let _ = table.execute(modify).await;
///
///     let mut read = Transaction::new();
///     let query_key_alice = read.get(&"Alice").unwrap();
///     let query_key_bob = read.get(&"Bob").unwrap();
///     let response = table.execute(read).await;
///
///     assert_eq!(response.get(&query_key_alice), None);
///     assert_eq!(response.get(&query_key_bob), Some(&23));
/// }
/// ```

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
    /// Creates an empty `Database`.
    ///
    /// # Examples
    ///
    /// ```
    /// use zebra::database::Database;
    /// let mut database: Database<&str, i32> = Database::new();
    /// ```
    pub fn new() -> Self {
        Database {
            store: Cell::new(Store::new()),
        }
    }

    /// Creates and assigns an empty [`Table`] to the `Database`.
    ///
    /// # Examples
    ///
    /// ```
    /// use zebra::database::Database;
    /// let mut database: Database<&str, i32> = Database::new();
    ///
    /// let table = database.empty_table();
    /// ```
    pub fn empty_table(&self) -> Table<Key, Value> {
        Table::empty(self.store.clone())
    }

    /// Creates a [`Receiver`] assigned to this `Database`. The
    /// receiver is used to efficiently receive a [`Table`]
    /// from other databases and add them this one.
    ///
    /// See [`Receiver`] for more details on its operation.
    ///
    /// # Examples
    ///
    /// ```
    /// use zebra::database::Database;
    /// let mut database: Database<&str, i32> = Database::new();
    ///
    /// let mut receiver = database.receive();
    ///
    /// // Do things with receiver...
    ///
    /// ```
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

    #[tokio::test]
    async fn modify_basic() {
        let database: Database<u32, u32> = Database::new();

        let mut table =
            database.table_with_records((0..256).map(|i| (i, i))).await;

        let mut transaction = Transaction::new();
        for i in 128..256 {
            transaction.set(i, i + 1).unwrap();
        }
        let _ = table.execute(transaction).await;
        table.assert_records(
            (0..256).map(|i| (i, if i < 128 { i } else { i + 1 })),
        );

        database.check([&table], []);
    }

    #[tokio::test]
    async fn clone_modify_original() {
        let database: Database<u32, u32> = Database::new();

        let mut table =
            database.table_with_records((0..256).map(|i| (i, i))).await;
        let table_clone = table.clone();

        let mut transaction = Transaction::new();
        for i in 128..256 {
            transaction.set(i, i + 1).unwrap();
        }
        let _response = table.execute(transaction).await;
        table.assert_records(
            (0..256).map(|i| (i, if i < 128 { i } else { i + 1 })),
        );
        table_clone.assert_records((0..256).map(|i| (i, i)));

        database.check([&table, &table_clone], []);
        drop(table_clone);

        table.assert_records(
            (0..256).map(|i| (i, if i < 128 { i } else { i + 1 })),
        );
        database.check([&table], []);
    }

    #[tokio::test]
    async fn clone_modify_drop() {
        let database: Database<u32, u32> = Database::new();

        let table = database.table_with_records((0..256).map(|i| (i, i))).await;
        let mut table_clone = table.clone();

        let mut transaction = Transaction::new();
        for i in 128..256 {
            transaction.set(i, i + 1).unwrap();
        }
        let _response = table_clone.execute(transaction).await;
        table_clone.assert_records(
            (0..256).map(|i| (i, if i < 128 { i } else { i + 1 })),
        );
        table.assert_records((0..256).map(|i| (i, i)));

        database.check([&table, &table_clone], []);
        drop(table_clone);

        table.assert_records((0..256).map(|i| (i, i)));
        database.check([&table], []);
    }
}
