use crate::{
    common::{data::Bytes, store::Field, tree::Prefix},
    database::{
        errors::SyncError,
        interact::drop,
        store::{Cell, Label, MapId, Node, Store},
        sync::{locate, Severity},
        Question, Table, TableAnswer, TableStatus,
    },
};

use doomstack::{here, Doom, ResultExt, Top};

use std::collections::{
    hash_map::Entry::{Occupied, Vacant},
    HashMap, HashSet,
};

const DEFAULT_WINDOW: usize = 128;

pub struct TableReceiver<Key: Field, Value: Field> {
    cell: Cell<Key, Value>,
    root: Option<Label>,
    held: HashSet<Label>,
    frontier: HashMap<Bytes, Context>,
    acquired: HashMap<Bytes, Node<Key, Value>>,
    pub settings: Settings,
}

pub struct Settings {
    pub window: usize,
}

struct Context {
    location: Prefix,
    remote_label: Label,
}

impl<Key, Value> TableReceiver<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub(crate) fn new(cell: Cell<Key, Value>) -> Self {
        TableReceiver {
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
        answer: TableAnswer<Key, Value>,
    ) -> Result<TableStatus<Key, Value>, Top<SyncError>> {
        let mut store = self.cell.take();
        let mut severity = Severity::ok();

        for node in answer.0 {
            severity = match self.update(&mut store, node) {
                Ok(()) => Severity::ok(),
                Err(offence) => severity + offence,
            };

            if severity.is_malicious() {
                break;
            }
        }

        if severity.is_benign() {
            if self.frontier.is_empty() {
                // Receive complete, flush if necessary
                match self.root {
                    Some(root) => {
                        // At least one node was received: flush
                        self.flush(&mut store, root);
                        self.cell.restore(store);

                        Ok(TableStatus::Complete(Table::new(self.cell.clone(), root)))
                    }
                    None => {
                        // No node received: the new table's `root` should be `Empty`
                        self.cell.restore(store);
                        Ok(TableStatus::Complete(Table::new(
                            self.cell.clone(),
                            Label::Empty,
                        )))
                    }
                }
            } else {
                // Receive incomplete, carry on with new `Question`
                self.cell.restore(store);
                let question = self.ask();

                Ok(TableStatus::Incomplete(self, question))
            }
        } else {
            self.cell.restore(store);
            SyncError::MalformedAnswer.fail().spot(here!())
        }
    }

    fn update(
        &mut self,
        store: &mut Store<Key, Value>,
        node: Node<Key, Value>,
    ) -> Result<(), Severity> {
        let hash = node.hash();

        let location = if self.root.is_some() {
            // Check if `hash` is in `frontier`. If so, retrieve `location`.
            Ok(self.frontier.get(&hash).ok_or(Severity::benign())?.location)
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
                | (Label::Leaf(..), Label::Empty) => Err(Severity::malicious()),
                _ => Ok(Label::Internal(MapId::internal(location), hash)),
            },
            Node::Leaf(ref key, _) => {
                if location.contains(&key.digest().into()) {
                    Ok(Label::Leaf(MapId::leaf(&key.digest()), hash))
                } else {
                    Err(Severity::malicious())
                }
            }
            Node::Empty => Err(Severity::malicious()),
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
                    Err(Severity::malicious())
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

            self.acquired.insert(label.hash(), node);
        }

        self.frontier.remove(&hash);
        Ok(())
    }

    fn sight(&mut self, label: &Label, location: Prefix) {
        if !label.is_empty() {
            self.frontier.insert(
                label.hash(),
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
                let node = self.acquired.get(&label.hash()).unwrap();
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

impl<Key, Value> Drop for TableReceiver<Key, Value>
where
    Key: Field,
    Value: Field,
{
    fn drop(&mut self) {
        let mut store = self.cell.take();

        for label in self.held.iter() {
            drop::drop(&mut store, *label);
        }

        self.cell.restore(store);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::database::{sync::ANSWER_DEPTH, Database, TableSender};

    enum Transfer<'a, Key, Value>
    where
        Key: Field,
        Value: Field,
    {
        Complete(Table<Key, Value>),
        Incomplete(
            &'a mut TableSender<Key, Value>,
            TableReceiver<Key, Value>,
            TableAnswer<Key, Value>,
        ),
    }

    fn run_for<Key, Value>(
        mut receiver: TableReceiver<Key, Value>,
        sender: &mut TableSender<Key, Value>,
        mut answer: TableAnswer<Key, Value>,
        steps: usize,
    ) -> Transfer<Key, Value>
    where
        Key: Field,
        Value: Field,
    {
        for _ in 0..steps {
            let status = receiver.learn(answer).unwrap();

            match status {
                TableStatus::Complete(table) => {
                    return Transfer::Complete(table);
                }
                TableStatus::Incomplete(receiver_t, question) => {
                    answer = sender.answer(&question).unwrap();
                    receiver = receiver_t;
                }
            };
        }

        Transfer::Incomplete(sender, receiver, answer)
    }

    impl<Key, Value> TableReceiver<Key, Value>
    where
        Key: Field,
        Value: Field,
    {
        pub(crate) fn held(&self) -> Vec<Label> {
            self.held.iter().map(|label| *label).collect()
        }
    }

    fn run<'a, Key, Value, I, const N: usize>(
        database: &Database<Key, Value>,
        tables: I,
        transfers: [(&mut TableSender<Key, Value>, TableReceiver<Key, Value>); N],
    ) -> ([Table<Key, Value>; N], usize)
    where
        Key: Field,
        Value: Field,
        I: IntoIterator<Item = &'a Table<Key, Value>>,
    {
        let mut transfers: [Transfer<Key, Value>; N] = array_init::from_iter(
            IntoIterator::into_iter(transfers).map(|(sender, receiver)| {
                let hello = sender.hello();
                Transfer::Incomplete(sender, receiver, hello)
            }),
        )
        .unwrap();

        let tables: Vec<&Table<Key, Value>> = tables.into_iter().collect();

        let mut steps: usize = 0;

        loop {
            steps += 1;

            transfers = array_init::from_iter(IntoIterator::into_iter(transfers).map(|transfer| {
                match transfer {
                    Transfer::Incomplete(sender, receiver, answer) => {
                        run_for(receiver, sender, answer, 1)
                    }
                    complete => complete,
                }
            }))
            .unwrap();

            let receivers = transfers.iter().filter_map(|transfer| match transfer {
                Transfer::Complete(..) => None,
                Transfer::Incomplete(_, receiver, _) => Some(receiver),
            });

            let received = transfers.iter().filter_map(|transfer| match transfer {
                Transfer::Complete(table) => Some(table),
                Transfer::Incomplete(..) => None,
            });

            let tables = tables.clone().into_iter().chain(received);

            database.check(tables, receivers);

            if transfers.iter().all(|transfer| {
                if let Transfer::Complete(..) = transfer {
                    true
                } else {
                    false
                }
            }) {
                break;
            }
        }

        let received: [Table<Key, Value>; N] = array_init::from_iter(
            IntoIterator::into_iter(transfers).map(|transfer| match transfer {
                Transfer::Complete(table) => table,
                Transfer::Incomplete(..) => unreachable!(),
            }),
        )
        .unwrap();

        (received, steps)
    }

    #[test]
    fn empty() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.empty_table();
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([received], steps) = run(&bob, [], [(&mut sender, receiver)]);

        assert_eq!(steps, 1);
        received.assert_records([]);
    }

    #[test]
    fn single() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records([(0, 1)]);
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([received], steps) = run(&bob, [], [(&mut sender, receiver)]);

        assert_eq!(steps, 1);
        received.assert_records([(0, 1)]);
    }

    #[test]
    fn tree() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..8).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([received], steps) = run(&bob, [], [(&mut sender, receiver)]);

        assert_eq!(steps, 3);
        received.assert_records((0..8).map(|i| (i, i)));
    }

    #[test]
    fn multiple() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..256).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([received], _) = run(&bob, [], [(&mut sender, receiver)]);

        received.assert_records((0..256).map(|i| (i, i)));
    }

    #[test]
    fn single_then_single() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records([(0, 1)]);
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([first], steps) = run(&bob, [], [(&mut sender, receiver)]);

        assert_eq!(steps, 1);
        first.assert_records([(0, 1)]);

        let original = alice.table_with_records([(2, 3)]);
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([second], steps) = run(&bob, [&first], [(&mut sender, receiver)]);

        assert_eq!(steps, 1);
        second.assert_records([(2, 3)]);
    }

    #[test]
    fn single_then_same() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records([(0, 1)]);
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([first], steps) = run(&bob, [], [(&mut sender, receiver)]);

        assert_eq!(steps, 1);
        first.assert_records([(0, 1)]);

        let receiver = bob.receive();
        let ([second], steps) = run(&bob, [&first], [(&mut sender, receiver)]);

        assert_eq!(steps, 1);
        second.assert_records([(0, 1)]);
    }

    #[test]
    fn tree_then_same() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..8).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([first], steps) = run(&bob, [], [(&mut sender, receiver)]);

        assert_eq!(steps, 3);
        first.assert_records((0..8).map(|i| (i, i)));

        let receiver = bob.receive();
        let ([second], steps) = run(&bob, [&first], [(&mut sender, receiver)]);

        assert_eq!(steps, 1);
        second.assert_records((0..8).map(|i| (i, i)));
    }

    #[test]
    fn multiple_then_multiple() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..256).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([first], _) = run(&bob, [], [(&mut sender, receiver)]);

        first.assert_records((0..256).map(|i| (i, i)));

        let original = alice.table_with_records((256..512).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([second], _) = run(&bob, [&first], [(&mut sender, receiver)]);

        second.assert_records((256..512).map(|i| (i, i)));
    }

    #[test]
    fn multiple_then_same() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..256).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([first], _) = run(&bob, [], [(&mut sender, receiver)]);

        first.assert_records((0..256).map(|i| (i, i)));

        let receiver = bob.receive();
        let ([second], steps) = run(&bob, [&first], [(&mut sender, receiver)]);

        assert_eq!(steps, 1);
        second.assert_records((0..256).map(|i| (i, i)));
    }

    #[test]
    fn multiple_then_subset() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..256).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([first], first_steps) = run(&bob, [], [(&mut sender, receiver)]);

        first.assert_records((0..256).map(|i| (i, i)));

        let original = alice.table_with_records((0..128).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([second], second_steps) = run(&bob, [&first], [(&mut sender, receiver)]);

        second.assert_records((0..128).map(|i| (i, i)));
        assert!(second_steps < first_steps);
    }

    #[test]
    fn multiple_then_superset() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..256).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([first], _) = run(&bob, [], [(&mut sender, receiver)]);

        first.assert_records((0..256).map(|i| (i, i)));

        let original = alice.table_with_records((0..512).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([second], _) = run(&bob, [&first], [(&mut sender, receiver)]);

        second.assert_records((0..512).map(|i| (i, i)));
    }

    #[test]
    fn multiple_then_overlap() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..256).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([first], _) = run(&bob, [], [(&mut sender, receiver)]);

        first.assert_records((0..256).map(|i| (i, i)));

        let original = alice.table_with_records((128..384).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([second], _) = run(&bob, [&first], [(&mut sender, receiver)]);

        second.assert_records((128..384).map(|i| (i, i)));
    }

    #[test]
    fn multiple_then_multiple_then_overlap() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..256).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([first], _) = run(&bob, [], [(&mut sender, receiver)]);

        first.assert_records((0..256).map(|i| (i, i)));

        let original = alice.table_with_records((256..512).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([second], _) = run(&bob, [&first], [(&mut sender, receiver)]);

        second.assert_records((256..512).map(|i| (i, i)));

        let original = alice.table_with_records((128..384).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([third], _) = run(&bob, [&first, &second], [(&mut sender, receiver)]);

        third.assert_records((128..384).map(|i| (i, i)));
    }

    #[test]
    fn multiple_interleave_multiple() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let first_original = alice.table_with_records((0..256).map(|i| (i, i)));
        let mut first_sender = first_original.send();

        let second_original = alice.table_with_records((256..512).map(|i| (i, i)));
        let mut second_sender = second_original.send();

        let first_receiver = bob.receive();
        let second_receiver = bob.receive();

        let ([first, second], _) = run(
            &bob,
            [],
            [
                (&mut first_sender, first_receiver),
                (&mut second_sender, second_receiver),
            ],
        );

        first.assert_records((0..256).map(|i| (i, i)));
        second.assert_records((256..512).map(|i| (i, i)));
    }

    #[test]
    fn multiple_interleave_same() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let first_original = alice.table_with_records((0..256).map(|i| (i, i)));
        let mut first_sender = first_original.send();

        let second_original = alice.table_with_records((0..256).map(|i| (i, i)));
        let mut second_sender = second_original.send();

        let first_receiver = bob.receive();
        let second_receiver = bob.receive();

        let ([first, second], _) = run(
            &bob,
            [],
            [
                (&mut first_sender, first_receiver),
                (&mut second_sender, second_receiver),
            ],
        );

        first.assert_records((0..256).map(|i| (i, i)));
        second.assert_records((0..256).map(|i| (i, i)));
    }

    #[test]
    fn multiple_interleave_overlap() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let first_original = alice.table_with_records((0..256).map(|i| (i, i)));
        let mut first_sender = first_original.send();

        let second_original = alice.table_with_records((128..384).map(|i| (i, i)));
        let mut second_sender = second_original.send();

        let first_receiver = bob.receive();
        let second_receiver = bob.receive();

        let ([first, second], _) = run(
            &bob,
            [],
            [
                (&mut first_sender, first_receiver),
                (&mut second_sender, second_receiver),
            ],
        );

        first.assert_records((0..256).map(|i| (i, i)));
        second.assert_records((128..384).map(|i| (i, i)));
    }

    #[test]
    fn multiple_then_double_overlap() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..256).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([received], _) = run(&bob, [], [(&mut sender, receiver)]);

        received.assert_records((0..256).map(|i| (i, i)));

        let first_original = alice.table_with_records((128..384).map(|i| (i, i)));
        let mut first_sender = first_original.send();

        let second_original = alice.table_with_records((128..384).map(|i| (i, i)));
        let mut second_sender = second_original.send();

        let first_receiver = bob.receive();
        let second_receiver = bob.receive();

        let ([first, second], _) = run(
            &bob,
            [&received],
            [
                (&mut first_sender, first_receiver),
                (&mut second_sender, second_receiver),
            ],
        );

        first.assert_records((128..384).map(|i| (i, i)));
        second.assert_records((128..384).map(|i| (i, i)));
    }

    #[test]
    fn multiple_then_overlap_drop_received_midway() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..256).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();
        let ([received], _) = run(&bob, [], [(&mut sender, receiver)]);

        received.assert_records((0..256).map(|i| (i, i)));

        let first_original = alice.table_with_records((128..384).map(|i| (i, i)));
        let mut first_sender = first_original.send();

        let first_receiver = bob.receive();
        let answer = first_sender.hello();

        let (first_receiver, answer) = match run_for(first_receiver, &mut sender, answer, 2) {
            Transfer::Incomplete(_, receiver, answer) => (receiver, answer),
            Transfer::Complete(_) => {
                panic!("Should take longer than 2 steps")
            }
        };

        drop(received);

        let first = match run_for(first_receiver, &mut first_sender, answer, 100) {
            Transfer::Incomplete(..) => {
                panic!("Transfer does not complete")
            }
            Transfer::Complete(table) => table,
        };

        bob.check([&first], []);
        first.assert_records((128..384).map(|i| (i, i)));
    }

    #[test]
    fn multiple_acceptable_benign() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..256).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();

        let mut answer = sender.hello();

        let max_benign = (1 << (ANSWER_DEPTH + 1)) - 2;

        answer = TableAnswer(
            (0..max_benign + 1)
                .map(|_| answer.0[0].clone())
                .collect::<Vec<Node<_, _>>>(),
        );

        let first = match run_for(receiver, &mut sender, answer, 100) {
            Transfer::Incomplete(..) => {
                panic!("Transfer does not complete")
            }
            Transfer::Complete(table) => table,
        };

        bob.check([&first], []);
        first.assert_records((0..256).map(|i| (i, i)));
    }

    #[test]
    fn multiple_unacceptable_benign() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..256).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();

        let mut answer = sender.hello();

        let max_benign = (1 << (ANSWER_DEPTH + 1)) - 2;

        answer = TableAnswer(
            (0..max_benign + 2)
                .map(|_| answer.0[0].clone())
                .collect::<Vec<Node<_, _>>>(),
        );

        match receiver.learn(answer) {
            Err(e) if *e.top() == SyncError::MalformedAnswer => (),
            Err(x) => {
                panic!("Expected `SyncError::MalformedAnswer` but got {:?}", x)
            }
            _ => panic!("Receiver accepts too many benign faults from sender"),
        }
    }

    #[test]
    fn multiple_malicious_internal_topology_empty_leaf() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..100).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();

        let mut answer = sender.hello();

        // Malicious tampering of Internal node's right child label ((empty, leaf) -> bad topology)
        let fake_leaf = Node::Leaf(wrap!(u32::MAX), wrap!(u32::MAX - 4));
        let fake_internal = Node::Internal(
            Label::Empty,
            Label::Leaf(MapId::leaf(&wrap!(u32::MAX).digest()), fake_leaf.hash()),
        );
        let fake_internal_label =
            Label::Internal(MapId::internal(Prefix::root().left()), fake_internal.hash());
        if let Node::<_, _>::Internal(_, r) = answer.0[0].clone() {
            answer.0[0] = Node::Internal(fake_internal_label, r);
        }
        answer.0[1] = fake_internal;

        match receiver.learn(answer) {
            Err(e) if *e.top() == SyncError::MalformedAnswer => (),
            Err(x) => {
                panic!("Expected `SyncError::MalformedAnswer` but got {:?}", x)
            }
            _ => panic!("Receiver accepts too many benign faults from sender"),
        }
    }

    #[test]
    fn multiple_malicious_internal_topology_leaf_empty() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..100).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();

        let mut answer = sender.hello();

        // Malicious tampering of Internal node's right child label ((leaf, empty) -> bad topology)
        let fake_leaf = Node::Leaf(wrap!(u32::MAX), wrap!(u32::MAX - 10));
        let fake_internal = Node::Internal(
            Label::Leaf(MapId::leaf(&wrap!(u32::MAX).digest()), fake_leaf.hash()),
            Label::Empty,
        );
        let fake_internal_label =
            Label::Internal(MapId::internal(Prefix::root().left()), fake_internal.hash());
        if let Node::<_, _>::Internal(_, r) = answer.0[0].clone() {
            answer.0[0] = Node::Internal(fake_internal_label, r);
        }
        answer.0[1] = fake_internal;

        match receiver.learn(answer) {
            Err(e) if *e.top() == SyncError::MalformedAnswer => (),
            Err(x) => {
                panic!("Expected `SyncError::MalformedAnswer` but got {:?}", x)
            }
            _ => panic!("Receiver accepts too many benign faults from sender"),
        }
    }

    #[test]
    fn multiple_malicious_internal_topology_empty_empty() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..100).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();

        let mut answer = sender.hello();

        // Malicious tampering of Internal node's right child label ((empty, empty) -> bad topology)
        let fake_internal = Node::Internal(Label::Empty, Label::Empty);
        let fake_internal_label =
            Label::Internal(MapId::internal(Prefix::root().left()), fake_internal.hash());
        if let Node::<_, _>::Internal(_, r) = answer.0[0].clone() {
            answer.0[0] = Node::Internal(fake_internal_label, r);
        }
        answer.0[1] = fake_internal;

        match receiver.learn(answer) {
            Err(e) if *e.top() == SyncError::MalformedAnswer => (),
            Err(x) => {
                panic!("Expected `SyncError::MalformedAnswer` but got {:?}", x)
            }
            _ => panic!("Receiver accepts too many benign faults from sender"),
        }
    }

    #[test]
    fn multiple_malicious_internal_map_id() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..256).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();

        let answer = sender.hello();

        let (receiver, mut answer) = match run_for(receiver, &mut sender, answer, 1) {
            Transfer::Incomplete(_, receiver, answer) => (receiver, answer),
            Transfer::Complete(_) => {
                panic!("Should take longer than 1 step to complete")
            }
        };

        // Malicious tampering of Internal node's right child map_id
        for (i, v) in answer.0.clone().iter().enumerate() {
            if let Node::<_, _>::Internal(l, Label::Internal(_, bytes)) = v {
                let fake_map_id = MapId::internal(Prefix::root());
                let n = Node::Internal(*l, Label::Internal(fake_map_id, *bytes));
                answer.0[i] = n;
                break;
            }
        }

        let first = match run_for(receiver, &mut sender, answer, 100) {
            Transfer::Incomplete(..) => {
                panic!("Transfer does not complete")
            }
            Transfer::Complete(table) => table,
        };

        bob.check([&first], []);
        first.assert_records((0..256).map(|i| (i, i)));
    }

    #[test]
    fn multiple_malicious_leaf_key_recover() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..256).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();

        let answer = sender.hello();

        let (receiver, mut answer) = match run_for(receiver, &mut sender, answer, 2) {
            Transfer::Incomplete(_, receiver, answer) => (receiver, answer),
            Transfer::Complete(_) => {
                panic!("Should take longer than 2 steps to complete")
            }
        };

        // Malicious tampering of Leaf node's key
        for (i, v) in answer.0.clone().iter().enumerate() {
            if let Node::<_, _>::Leaf(_, value) = v {
                let fake_key = wrap!(u32::MAX);
                let n = Node::Leaf(fake_key, value.clone());
                answer.0[i] = n;
                break;
            }
        }

        let first = match run_for(receiver, &mut sender, answer, 100) {
            Transfer::Incomplete(..) => {
                panic!("Transfer does not complete")
            }
            Transfer::Complete(table) => table,
        };

        bob.check([&first], []);
        first.assert_records((0..256).map(|i| (i, i)));
    }

    #[test]
    fn swapped_leaf_positions() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((4..=5).map(|i| (i, i)));
        let mut sender = original.send();
        let receiver = bob.receive();
        let mut answer = sender.hello();

        if let Node::<_, _>::Internal(l, r) = answer.0[0].clone() {
            answer.0[0] = Node::Internal(r, l);
        };

        match receiver.learn(answer) {
            Err(e) if *e.top() == SyncError::MalformedAnswer => (),
            Err(x) => {
                panic!("Expected `SyncError::MalformedAnswer` but got {:?}", x)
            }
            _ => panic!("Receiver accepts too many benign faults from sender"),
        }
    }

    #[test]
    fn empty_node_hello() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..1).map(|i| (i, i)));
        let mut sender = original.send();
        let receiver = bob.receive();
        let mut answer = sender.hello();

        answer.0[0] = Node::Empty;

        match receiver.learn(answer) {
            Err(e) if *e.top() == SyncError::MalformedAnswer => (),
            Err(e) => {
                panic!("Expected `SyncError::MalformedAnswer` but got {:?}", e)
            }
            _ => panic!("Receiver accepts too many benign faults from sender"),
        }
    }

    #[test]
    fn malicious_internal_swap_location_root() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..256).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();

        let answer = sender.hello();
        let right_label = match answer.0[0] {
            Node::Internal(_, r) => r,
            _ => unreachable!(),
        };
        let right = sender.answer(&Question(vec![right_label])).unwrap().0[0].clone();

        let first = match run_for(receiver, &mut sender, answer, 100) {
            Transfer::Incomplete(..) => {
                panic!("Transfer does not complete")
            }
            Transfer::Complete(table) => table,
        };

        let original = alice.table_with_records((0..128).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();
        let mut answer = sender.hello();

        // Malicious swap. Swapping root with right child.
        answer.0[0] = right;

        match receiver.learn(answer) {
            Err(e) if *e.top() == SyncError::MalformedAnswer => (),
            Err(x) => {
                panic!("Expected `SyncError::MalformedAnswer` but got {:?}", x)
            }
            _ => panic!("Receiver accepts too many benign faults from sender"),
        }

        bob.check([&first], []);
        first.assert_records((0..256).map(|i| (i, i)));
    }

    #[test]
    fn malicious_internal_swap_location_deep() {
        let alice: Database<u32, u32> = Database::new();
        let bob: Database<u32, u32> = Database::new();

        let original = alice.table_with_records((0..256).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();

        let answer = sender.hello();
        let right_label = match answer.0[0] {
            Node::Internal(_, r) => r,
            _ => unreachable!(),
        };
        let right_label = match sender.answer(&Question(vec![right_label])).unwrap().0[0].clone() {
            Node::Internal(_, r) => r,
            _ => unreachable!(),
        };
        let right = sender.answer(&Question(vec![right_label])).unwrap().0[0].clone();

        let first = match run_for(receiver, &mut sender, answer, 100) {
            Transfer::Incomplete(..) => {
                panic!("Transfer does not complete")
            }
            Transfer::Complete(table) => table,
        };

        let original = alice.table_with_records((0..128).map(|i| (i, i)));
        let mut sender = original.send();

        let receiver = bob.receive();
        let mut answer = sender.hello();

        // Malicious swap. Swapping RRR with RR (same map_id)
        match answer.0[0] {
            Node::Internal(l, _) => {
                answer.0[0] = Node::Internal(l, right_label);
            }
            _ => unreachable!(),
        };
        answer.0[1] = right;

        match receiver.learn(answer) {
            Err(e) if *e.top() == SyncError::MalformedAnswer => (),
            Err(x) => {
                panic!("Expected `SyncError::MalformedAnswer` but got {:?}", x)
            }
            _ => panic!("Receiver accepts too many benign faults from sender"),
        }

        bob.check([&first], []);
        first.assert_records((0..256).map(|i| (i, i)));
    }
}
