use crate::common::data::Bytes;

use talk::crypto::primitives::{hash, hash::HASH_LENGTH};

const INTERNAL_FLAG: u8 = 0;
const LEAF_FLAG: u8 = 1;
const EMPTY_HASH: Bytes = Bytes([0; HASH_LENGTH]);

pub(crate) fn empty() -> Bytes {
    EMPTY_HASH
}

pub(crate) fn internal(left: Bytes, right: Bytes) -> Bytes {
    hash::hash(&(INTERNAL_FLAG, left, right)).unwrap().into()
}

pub(crate) fn leaf(key: Bytes, value: Bytes) -> Bytes {
    hash::hash(&(LEAF_FLAG, key, value)).unwrap().into()
}
