use super::apply;
use super::batch::Batch;
use super::database::Database;
use super::field::Field;
use super::label::Label;

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

    pub(crate) async fn apply(
        &mut self,
        batch: Batch<Key, Value>,
    ) -> Batch<Key, Value> {
        let mut guard = self.database.store.lock().unwrap();
        let store = guard.take().unwrap();

        let (store, root, batch) = apply::apply(store, self.root, batch).await;

        *guard = Some(store);
        self.root = root;

        batch
    }
}
