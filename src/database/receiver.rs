use crate::database::{
    data::Bytes,
    store::{Cell, Field, Label, MapId, Node, Store},
    sync::{locate, Severity},
    tree::Prefix,
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
            self.frontier.remove(&hash);
        }

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
}
