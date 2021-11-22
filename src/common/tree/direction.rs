use serde::{Deserialize, Serialize};

#[repr(u8)]
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum Direction {
    Left,
    Right,
}
