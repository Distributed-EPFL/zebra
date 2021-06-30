use crate::database::{data::Bytes, store::MapId};

use serde::Serialize;

#[derive(Debug, Clone, Hash, Copy, PartialEq, Eq, Serialize)]
pub(crate) enum Label {
    Internal(MapId, Bytes),
    Leaf(MapId, Bytes),
    Empty,
}

impl Label {
    pub fn is_empty(&self) -> bool {
        *self == Label::Empty
    }

    pub fn map(&self) -> &MapId {
        match self {
            Label::Internal(map, _) => map,
            Label::Leaf(map, _) => map,
            Label::Empty => {
                panic!("called `Label::map()` on an `Empty` value")
            }
        }
    }

    pub fn hash(&self) -> &Bytes {
        match self {
            Label::Internal(_, hash) => hash,
            Label::Leaf(_, hash) => hash,
            Label::Empty => {
                panic!("called `Label::hash()` on an `Empty` value")
            }
        }
    }
}
