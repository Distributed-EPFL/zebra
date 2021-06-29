use super::apply;
use super::batch::Batch;
use super::database::Database;
use super::field::Field;
use super::label::Label;
use super::response::Response;
use super::transaction::Transaction;

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
        let mut guard = self.database.store.lock().unwrap();
        let store = guard.take().unwrap();

        let (store, root, batch) = apply::apply(store, self.root, batch).await;

        *guard = Some(store);
        self.root = root;

        batch
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rand::prelude::*;

    use super::super::database::Database;

    #[tokio::test]
    async fn develop() {
        let database: Database<u32, u32> = Database::new();
        let mut table = database.empty_table();

        for _ in 0..100 {
            let mut transaction: Transaction<u32, u32> = Transaction::new();

            for i in 0..65536 {
                let _ = transaction.set(i, random());
            }

            let start = std::time::Instant::now();
            table.execute(transaction).await;
            println!("Elapsed total: {}", start.elapsed().as_secs_f64());
        }
    }
}
