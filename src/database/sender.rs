use crate::database::{
    errors::{MalformedQuestion, SyncError},
    store::{Field, Handle, Label, Node, Store},
    sync::ANSWER_DEPTH,
    Answer, Question,
};

use std::collections::hash_map::Entry::{Occupied, Vacant};

pub struct Sender<Key: Field, Value: Field>(Handle<Key, Value>);

impl<Key, Value> Sender<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub(crate) fn new(handle: Handle<Key, Value>) -> Self {
        Sender(handle)
    }

    pub fn hello(&mut self) -> Answer<Key, Value> {
        self.answer(&Question(vec![self.0.root])).unwrap()
    }

    pub fn answer(
        &mut self,
        question: &Question,
    ) -> Result<Answer<Key, Value>, SyncError> {
        let mut collector: Vec<Node<Key, Value>> = Vec::new();
        let mut store = self.0.cell.take();

        for label in &question.0 {
            Sender::grab(&mut store, &mut collector, *label, ANSWER_DEPTH)?;
        }

        self.0.cell.restore(store);
        Ok(Answer(collector))
    }

    fn grab(
        store: &mut Store<Key, Value>,
        collector: &mut Vec<Node<Key, Value>>,
        label: Label,
        ttl: u8,
    ) -> Result<(), SyncError> {
        if !label.is_empty() {
            let node = match store.entry(label) {
                Occupied(entry) => {
                    let node = entry.get().node.clone();
                    Ok(node)
                }
                Vacant(..) => MalformedQuestion.fail(),
            }?;

            let recur = match node {
                Node::Internal(left, right) if ttl > 0 => Some((left, right)),
                _ => None,
            };

            collector.push(node);

            if let Some((left, right)) = recur {
                Sender::grab(store, collector, left, ttl - 1)?;
                Sender::grab(store, collector, right, ttl - 1)?;
            }

            Ok(())
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::database::{store::Field, Database, Table, Transaction};

    use std::collections::hash_map::Entry::Occupied;

    async fn new_table<Key, Value, I>(
        database: &Database<Key, Value>,
        sets: I,
    ) -> Table<Key, Value>
    where
        Key: Field,
        Value: Field,
        I: IntoIterator<Item = (Key, Value)>,
    {
        let mut table = database.empty_table();
        let mut transaction = Transaction::new();

        for (k, v) in sets.into_iter() {
            transaction.set(k, v).unwrap();
        }

        table.execute(transaction).await;
        table
    }

    #[tokio::test]
    async fn answer_empty() {
        let database: Database<u32, u32> = Database::new();
        let table = database.empty_table();

        let mut send = table.send();

        let answer = send.answer(&Question(vec![Label::Empty])).unwrap();

        assert_eq!(answer, Answer(vec!()));
    }

    #[tokio::test]
    async fn grab_one() {
        let database: Database<u32, u32> = Database::new();
        let table = new_table(&database, [(0u32, 0u32)]).await;

        let mut send = table.send();
        let label = send.0.root;

        let mut store = database.store.take();
        let node = match store.entry(label) {
            Occupied(entry) => entry.get().node.clone(),
            _ => unreachable!(),
        };
        database.store.restore(store);

        let answer = send.answer(&Question(vec![label])).unwrap();

        assert_eq!(answer, Answer(vec!(node)));
    }

    #[tokio::test]
    async fn grab_three() {
        let database: Database<u32, u32> = Database::new();
        let table =
            new_table(&database, [(0u32, 0u32), (4u32, 4u32)]).await;

        let mut send = table.send();
        let label0 = send.0.root;

        let mut store = database.store.take();
        let n0 = match store.entry(label0) {
            Occupied(entry) => entry.get().node.clone(),
            _ => unreachable!(),
        };
        let (n1, n2) = match n0 {
            Node::Internal(label1, label2) => {
                let n1 = match store.entry(label1) {
                    Occupied(entry) => entry.get().node.clone(),
                    _ => unreachable!(),
                };
                let n2 = match store.entry(label2) {
                    Occupied(entry) => entry.get().node.clone(),
                    _ => unreachable!(),
                };
                (n1, n2)
            }
            _ => unreachable!(),
        };
        database.store.restore(store);

        let answer = send.answer(&Question(vec![label0])).unwrap();

        assert_eq!(answer, Answer(vec!(n0, n1, n2)));
    }
}
