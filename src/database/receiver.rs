use crate::database::{
    data::Bytes,
    errors::{MalformedAnswer, SyncError},
    store::{Cell, Field, Label, MapId, Node, Store},
    sync::{locate, Severity},
    tree::Prefix,
    Answer, Question, Table,
};

use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::{HashMap, HashSet};

const DEFAULT_WINDOW: usize = 128;

pub struct Receiver<Key: Field, Value: Field> {
    cell: Cell<Key, Value>,
    root: Option<Label>,
    held: HashSet<Label>,
    frontier: HashMap<Bytes, Context>,
    acquired: HashMap<Label, Node<Key, Value>>,
    pub settings: Settings,
}

pub struct Settings {
    pub window: usize,
}

pub enum Status<Key: Field, Value: Field> {
    Complete(Table<Key, Value>),
    Incomplete(Receiver<Key, Value>, Question),
}

struct Context {
    location: Prefix,
    remote_label: Label,
}

impl<Key, Value> Receiver<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub(crate) fn new(cell: Cell<Key, Value>) -> Self {
        Receiver {
            cell,
            root: None,
            held: HashSet::new(),
            frontier: HashMap::new(),
            acquired: HashMap::new(),
            settings: Settings {
                window: DEFAULT_WINDOW,
            },
        }
    }

    pub fn learn(
        mut self,
        answer: Answer<Key, Value>,
    ) -> Result<Status<Key, Value>, SyncError> {
        let mut store = self.cell.take();
        let mut severity = Severity::Benign(0);

        for node in answer.0.into_iter() {
            severity = match self.update(&mut store, node) {
                Ok(()) => Severity::Benign(0),
                Err(offence) => severity + offence,
            };

            if severity.is_malicious() {
                break;
            }
        }

        if severity.is_benign() {
            if self.frontier.is_empty() {
                self.flush(&mut store, self.root.unwrap());
                self.cell.restore(store);

                Ok(Status::Complete(Table::new(
                    self.cell.clone(),
                    self.root.unwrap(),
                )))
            } else {
                self.cell.restore(store);
                let question = self.ask();

                Ok(Status::Incomplete(self, question))
            }
        } else {
            self.cell.restore(store);
            MalformedAnswer.fail()
        }
    }

    fn update(
        &mut self,
        store: &mut Store<Key, Value>,
        node: Node<Key, Value>,
    ) -> Result<(), Severity> {
        let hash = node.hash().into();

        let location = if self.root.is_some() {
            // Check if `hash` is in `frontier`. If so, retrieve `location`.
            Ok(self
                .frontier
                .get(&hash)
                .ok_or(Severity::Benign(1))?
                .location)
        } else {
            // This is the first `node` fed in `update`. By convention, `node` is the root.
            Ok(Prefix::root())
        }?;

        // Check if `node` preserves topology invariants:
        // - If `node` is `Internal`, its children must preserve compactness.
        // - If `node` is `Leaf`, it must lie along its `key` path.
        // If so, compute `node`'s `label`.
        let label = match node {
            Node::Internal(left, right) => match (left, right) {
                (Label::Empty, Label::Empty)
                | (Label::Empty, Label::Leaf(..))
                | (Label::Leaf(..), Label::Empty) => Err(Severity::Malicious),
                _ => Ok(Label::Internal(MapId::internal(location), hash)),
            },
            Node::Leaf(ref key, _) => {
                if location.contains(&(*key.digest()).into()) {
                    Ok(Label::Leaf(MapId::leaf(key.digest()), hash))
                } else {
                    Err(Severity::Malicious)
                }
            }
            Node::Empty => Err(Severity::Malicious),
        }?;

        // Fill `root` if necessary.

        if self.root.is_none() {
            self.root = Some(label);
        }

        // Check if `label` is already in `store`.
        let hold = match store.entry(label) {
            Occupied(..) => true,
            Vacant(..) => false,
        };

        if hold {
            // If `node` is `Internal`, its position in `store` must match `location`.
            if let Node::Internal(..) = node {
                if locate::locate(store, label) == location {
                    Ok(())
                } else {
                    Err(Severity::Malicious)
                }
            } else {
                Ok(())
            }?;

            store.incref(label);
            self.held.insert(label);
        } else {
            if let Node::Internal(ref left, ref right) = node {
                self.sight(left, location.left());
                self.sight(right, location.right());
            }

            self.acquired.insert(label, node);
        }

        self.frontier.remove(&hash);
        Ok(())
    }

    fn sight(&mut self, label: &Label, location: Prefix) {
        if !label.is_empty() {
            self.frontier.insert(
                *label.hash(),
                Context {
                    location,
                    remote_label: *label,
                },
            );
        }
    }

    fn ask(&self) -> Question {
        Question(
            self.frontier
                .iter()
                .map(|(_, context)| context.remote_label)
                .take(self.settings.window)
                .collect(),
        )
    }

    fn flush(&mut self, store: &mut Store<Key, Value>, label: Label) {
        if !label.is_empty() {
            let stored = match store.entry(label) {
                Occupied(..) => true,
                Vacant(..) => false,
            };

            let recursion = if stored {
                None
            } else {
                let node = self.acquired.get(&label).unwrap();
                store.populate(label, node.clone());

                match node {
                    Node::Internal(left, right) => Some((*left, *right)),
                    _ => None,
                }
            };

            if self.held.contains(&label) {
                self.held.remove(&label);
            } else {
                store.incref(label);
            }

            if let Some((left, right)) = recursion {
                self.flush(store, left);
                self.flush(store, right);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::database::{Database, Query, Sender, Transaction};

    use std::fmt::Debug;

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

    async fn check_table<Key, Value, I>(
        table: &mut Table<Key, Value>,
        values: I,
    ) where
        Key: Field,
        Value: Field + Debug + Eq,
        I: IntoIterator<Item = (Key, Value)>,
    {
        let mut transaction: Transaction<Key, Value> = Transaction::new();
        let expected: Vec<(Query, Value)> = values
            .into_iter()
            .map(|(key, value)| (transaction.get(&key).unwrap(), value))
            .collect();

        let response = table.execute(transaction).await;

        for (query, value) in expected {
            assert_eq!(*response.get(&query).unwrap(), value);
        }
    }

    fn exchange(
        sender: &mut Sender<u32, u32>,
        mut receiver: Receiver<u32, u32>,
    ) -> (Table<u32, u32>, usize) {
        let mut rounds: usize = 0;
        let mut answer = sender.hello();

        loop {
            rounds += 1;
            match receiver.learn(answer).unwrap() {
                Status::Complete(table) => {
                    return (table, rounds);
                }
                Status::Incomplete(new_receiver, question) => {
                    receiver = new_receiver;
                    answer = sender.answer(&question).unwrap();
                }
            }
        }
    }

    #[tokio::test]
    async fn develop() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = new_table(&alice, (0..256).map(|i| (i, i))).await;
        let mut sender = original.send();

        let (mut first, _) = exchange(&mut sender, bob.receive());
        check_table(&mut first, (0..256).map(|i| (i, i))).await;

        let (mut second, rounds) = exchange(&mut sender, bob.receive());
        check_table(&mut second, (0..256).map(|i| (i, i))).await;

        assert_eq!(rounds, 1);
    }
}
