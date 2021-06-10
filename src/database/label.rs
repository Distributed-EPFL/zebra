use serde::Serialize;

use super::bytes::Bytes;

#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) enum Label {
    Internal(Bytes),
    Leaf(Bytes),
    Empty,
}

impl Label {
    pub fn is_empty(&self) -> bool {
        *self == Label::Empty
    }

    pub fn bytes(&self) -> &Bytes {
        match self {
            Label::Internal(bytes) => bytes,
            Label::Leaf(bytes) => bytes,
            Label::Empty => {
                panic!("called `Label::bytes()` on an `Empty` value")
            }
        }
    }
}
