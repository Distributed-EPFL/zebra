use crate::database::{
    store::{Cell, Field, Handle, Label},
    Response, Sender, Transaction,
};

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

    pub async fn execute(
        &mut self,
        transaction: Transaction<Key, Value>,
    ) -> Response<Key, Value> {
        let (tid, batch) = transaction.finalize();
        let batch = self.0.apply(batch).await;
        Response::new(tid, batch)
    }

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
        pub(crate) fn check_tree<I>(&mut self) {
            let mut store = self.0.cell.take();
            store.check_tree(self.0.root);
            self.0.cell.restore(store);
        }

        pub(crate) fn assert_records<I>(&mut self, reference: I)
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
