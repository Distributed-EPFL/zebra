use crate::database::{
    errors::{MalformedQuestion, SyncError},
    store::{Field, Handle, Label, Node, Store},
    Answer, Question,
};

use std::collections::hash_map::Entry::{Occupied, Vacant};

pub struct Sender<Key: Field, Value: Field>(Handle<Key, Value>);

const DEPTH: u8 = 2;

impl<Key, Value> Sender<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub(crate) fn new(handle: Handle<Key, Value>) -> Self {
        Sender(handle)
    }

    pub fn answer(
        &mut self,
        question: &Question,
    ) -> Result<Answer<Key, Value>, SyncError> {
        let mut collector: Vec<Node<Key, Value>> = Vec::new();
        let mut store = self.0.cell.take();

        for label in &question.0 {
            Sender::grab(&mut store, &mut collector, *label, DEPTH)?;
        }

        Ok(Answer(collector))
    }

    fn grab(
        store: &mut Store<Key, Value>,
        collector: &mut Vec<Node<Key, Value>>,
        label: Label,
        ttl: u8,
    ) -> Result<(), SyncError> {
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
    }
}
