use crate::{
    common::store::Field,
    database::{
        store::{Cell, Handle, Label},
        Response, Sender, Transaction,
    },
};

use drop::crypto::Digest;

// Documentation links
#[allow(unused_imports)]
use crate::database::{Database, Receiver};

/// A map implemented using Merkle Patricia Trees.
///
/// Allows for:
/// 1) Concurrent processing of operations on different keys with minimal
/// thread synchronization.
/// 2) Cheap cloning (O(1)).
/// 3) Efficient sending to [`Database`]s containing similar maps (high % of
/// key-value pairs in common)
///
/// [`Database`]: crate::database::Database
/// [`Table`]: crate::database::Table
/// [`Transaction`]: crate::database::Transaction
/// [`Sender`]: crate::database::Sender
/// [`Receiver`]: crate::database::Receiver

pub struct Table<Key: Field, Value: Field>(Handle<Key, Value>);

impl<Key, Value> Table<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub(crate) fn empty(cell: Cell<Key, Value>) -> Self {
        Table(Handle::empty(cell))
    }

    pub(crate) fn new(cell: Cell<Key, Value>, root: Label) -> Self {
        Table(Handle::new(cell, root))
    }

    /// Returns a cryptographic commitment to the contents of the `Table`.
    pub fn commitment(&self) -> Digest {
        self.0.commitment()
    }

    /// Executes a [`Transaction`] returning a [`Response`]
    /// (see their respective documentations for more details).
    ///
    /// # Examples
    ///
    /// ```
    /// use zebra::database::{Database, Transaction};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///
    ///     let mut database = Database::new();
    ///
    ///     // Create a new transaction.
    ///     let mut transaction = Transaction::new();
    ///
    ///     // Set (key = 0, value = 0)
    ///     transaction.set(0, 0).unwrap();
    ///
    ///     // Remove record with (key = 1)
    ///     transaction.remove(&1).unwrap();
    ///
    ///     // Read records with (key = 2)
    ///     let read_key = transaction.get(&2).unwrap();
    ///
    ///     let mut table = database.empty_table();
    ///     
    ///     // Executes the transaction, returning a response.
    ///     let response = table.execute(transaction).await;
    ///
    ///     let value_read = response.get(&read_key);
    ///     assert_eq!(value_read, None);
    /// }
    /// ```
    pub async fn execute(
        &mut self,
        transaction: Transaction<Key, Value>,
    ) -> Response<Key, Value> {
        let (tid, batch) = transaction.finalize();
        let batch = self.0.apply(batch).await;
        Response::new(tid, batch)
    }

    /// Transforms the table into a [`Sender`], preparing it for sending to
    /// to a [`Receiver`] of another [`Database`]. For details on how to use
    /// Senders and Receivers check their respective documentation.
    /// ```
    /// use zebra::database::Database;
    ///
    /// let mut database: Database<u32, u32> = Database::new();
    /// let original = database.empty_table();
    ///
    /// // Sending consumes the copy so we typically clone first, which is cheap.
    /// let copy = original.clone();
    /// let sender = copy.send();
    ///
    /// // Use sender...
    /// ```
    pub fn send(self) -> Sender<Key, Value> {
        Sender::new(self.0)
    }
}

impl<Key, Value> Clone for Table<Key, Value>
where
    Key: Field,
    Value: Field,
{
    fn clone(&self) -> Self {
        Table(self.0.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fmt::Debug;
    use std::hash::Hash;

    impl<Key, Value> Table<Key, Value>
    where
        Key: Field,
        Value: Field,
    {
        pub(crate) fn root(&self) -> Label {
            self.0.root
        }

        pub(crate) fn check_tree(&self) {
            let mut store = self.0.cell.take();
            store.check_tree(self.0.root);
            self.0.cell.restore(store);
        }

        pub(crate) fn assert_records<I>(&self, reference: I)
        where
            Key: Debug + Clone + Eq + Hash,
            Value: Debug + Clone + Eq + Hash,
            I: IntoIterator<Item = (Key, Value)>,
        {
            let mut store = self.0.cell.take();
            store.assert_records(self.0.root, reference);
            self.0.cell.restore(store);
        }
    }
}
