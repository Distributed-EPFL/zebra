use crate::{
    common::store::Field,
    database::{
        errors::SyncError,
        store::{Handle, Label, Node, Store},
        sync::ANSWER_DEPTH,
        Question, Table, TableAnswer,
    },
};

use doomstack::{here, Doom, ResultExt, Top};

use std::collections::hash_map::Entry::{Occupied, Vacant};

pub struct TableSender<Key: Field, Value: Field>(Handle<Key, Value>);

impl<Key, Value> TableSender<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub(crate) fn from_handle(handle: Handle<Key, Value>) -> Self {
        TableSender(handle)
    }

    pub fn hello(&mut self) -> TableAnswer<Key, Value> {
        self.answer(&Question(vec![self.0.root])).unwrap()
    }

    pub fn answer(
        &mut self,
        question: &Question,
    ) -> Result<TableAnswer<Key, Value>, Top<SyncError>> {
        let mut collector: Vec<Node<Key, Value>> = Vec::new();
        let mut store = self.0.cell.take();

        for label in &question.0 {
            if let Err(e) = TableSender::grab(&mut store, &mut collector, *label, ANSWER_DEPTH) {
                self.0.cell.restore(store);
                return Err(e);
            }
        }

        self.0.cell.restore(store);
        Ok(TableAnswer(collector))
    }

    pub fn end(self) -> Table<Key, Value> {
        Table::from_handle(self.0)
    }

    fn grab(
        store: &mut Store<Key, Value>,
        collector: &mut Vec<Node<Key, Value>>,
        label: Label,
        ttl: u8,
    ) -> Result<(), Top<SyncError>> {
        if !label.is_empty() {
            let node = match store.entry(label) {
                Occupied(entry) => {
                    let node = entry.get().node.clone();
                    Ok(node)
                }
                Vacant(..) => SyncError::MalformedQuestion.fail().spot(here!()),
            }?;

            let recur = match node {
                Node::Internal(left, right) if ttl > 0 => Some((left, right)),
                _ => None,
            };

            collector.push(node);

            if let Some((left, right)) = recur {
                TableSender::grab(store, collector, left, ttl - 1)?;
                TableSender::grab(store, collector, right, ttl - 1)?;
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

    use crate::database::{store::MapId, Database};

    use std::collections::hash_map::Entry::Occupied;

    #[test]
    fn answer_empty() {
        let database: Database<u32, u32> = Database::new();
        let table = database.empty_table();

        let mut send = table.send();

        let answer = send.answer(&Question(vec![Label::Empty])).unwrap();

        assert_eq!(answer, TableAnswer(vec!()));
    }

    #[test]
    fn answer_non_existant() {
        let database: Database<u32, u32> = Database::new();
        let table = database.empty_table();

        let mut send = table.send();
        let leaf = leaf!(1u32, 1u32);
        let leaf_label = Label::Leaf(MapId::leaf(&wrap!(1u32).digest()), leaf.hash());

        let question = Question(vec![leaf_label]);
        let answer = send.answer(&question);

        match answer {
            Err(e) if *e.top() == SyncError::MalformedQuestion => (),
            Err(x) => panic!("Expected `SyncError::MalformedQuestion` but got {:?}", x),
            _ => panic!("Expected `SyncError::MalformedQuestion` but got a valid answer"),
        };
    }

    #[test]
    fn grab_one() {
        let database: Database<u32, u32> = Database::new();
        let table = database.table_with_records([(0u32, 0u32)]);

        let mut send = table.send();
        let label = send.0.root;

        let mut store = database.store.take();
        let node = match store.entry(label) {
            Occupied(entry) => entry.get().node.clone(),
            _ => unreachable!(),
        };
        database.store.restore(store);

        let answer = send.answer(&Question(vec![label])).unwrap();

        assert_eq!(answer, TableAnswer(vec!(node)));
    }

    #[test]
    fn grab_three() {
        let database: Database<u32, u32> = Database::new();
        let table = database.table_with_records([(0u32, 0u32), (4u32, 4u32)]);

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

        assert_eq!(answer, TableAnswer(vec!(n0, n1, n2)));
    }
}
