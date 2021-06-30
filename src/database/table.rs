use crate::database::{
    interact::{apply, Batch},
    store::{Field, Label},
    Database, Response, Transaction,
};

pub struct Table<Key, Value>
where
    Key: Field,
    Value: Field,
{
    database: Database<Key, Value>,
    root: Label,
}

impl<Key, Value> Table<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub(crate) fn empty(database: &Database<Key, Value>) -> Self {
        Table {
            database: database.clone(),
            root: Label::Empty,
        }
    }

    pub async fn execute(
        &mut self,
        transaction: Transaction<Key, Value>,
    ) -> Response<Key, Value> {
        let (tid, batch) = transaction.finalize();
        let batch = self.apply(batch).await;
        Response::new(tid, batch)
    }

    pub(crate) async fn apply(
        &mut self,
        batch: Batch<Key, Value>,
    ) -> Batch<Key, Value> {
        let store = self.database.store.take();

        let (store, root, batch) = apply::apply(store, self.root, batch).await;

        self.database.store.restore(store);
        self.root = root;

        batch
    }
}
