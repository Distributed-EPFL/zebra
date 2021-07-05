use crate::database::{
    data::Bytes,
    store::{Cell, Field, Label, MapId, Node, Store},
    sync::{locate, Severity},
    tree::Prefix,
};

use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::{HashMap, HashSet};

pub struct Receiver<Key: Field, Value: Field> {
    cell: Cell<Key, Value>,
    root: Option<Label>,
    held: HashSet<Label>,
    frontier: HashMap<Bytes, Context>,
    acquired: HashMap<Label, Node<Key, Value>>,
}

struct Context {
    location: Prefix,
    remote_map_id: MapId,
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
        }
    }

    fn update(
        &mut self,
        store: &mut Store<Key, Value>,
        node: Node<Key, Value>,
    ) -> Result<(), Severity> {
        let hash = node.hash().into();
        let location = self
            .frontier
            .get(&hash)
            .ok_or(Severity::Benign(1))?
            .location;

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

        let hold = match store.entry(label) {
            Occupied(..) => true,
            Vacant(..) => false,
        };

        if hold {
            match node {
                Node::Internal(..) => {
                    if locate::locate(store, label) == location {
                        Ok(())
                    } else {
                        Err(Severity::Malicious)
                    }
                }
                _ => Ok(()),
            }?;

            store.incref(label);
            self.held.insert(label);
        } else {
            match node {
                Node::Internal(left, right) => {
                    if !left.is_empty() {
                        self.frontier.insert(
                            *left.hash(),
                            Context {
                                location: location.left(),
                                remote_map_id: *left.map(),
                            },
                        );
                    }

                    if !right.is_empty() {
                        self.frontier.insert(
                            *left.hash(),
                            Context {
                                location: location.left(),
                                remote_map_id: *left.map(),
                            },
                        );
                    }
                }
                _ => {}
            }

            self.acquired.insert(label, node);
            self.frontier.remove(&hash);
        }

        Ok(())
    }
}
