use crate::common::data::Bytes;

use serde::{Deserialize, Serialize};

use talk::crypto::primitives::hash::HASH_LENGTH;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct Commitment([u8; HASH_LENGTH]);

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
