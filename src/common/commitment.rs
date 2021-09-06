use crate::common::data::Bytes;

use drop::crypto::hash::SIZE;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct Commitment([u8; SIZE]);

impl From<Bytes> for Commitment {
    fn from(bytes: Bytes) -> Commitment {
        Commitment(bytes.0)
    }
}

impl Into<[u8; 32]> for Commitment {
    fn into(self) -> [u8; 32] {
        self.0
    }
}
