use crate::common::data::Bytes;

use drop::crypto::hash;
use drop::crypto::hash::SIZE;

const INTERNAL_FLAG: u8 = 0;
const LEAF_FLAG: u8 = 1;
const EMPTY_HASH: Bytes = Bytes([0; SIZE]);

pub(crate) fn empty() -> Bytes {
    EMPTY_HASH
}

pub(crate) fn internal(left: Bytes, right: Bytes) -> Bytes {
    hash::hash(&(INTERNAL_FLAG, left, right)).unwrap().into()
}

pub(crate) fn leaf(key: Bytes, value: Bytes) -> Bytes {
    hash::hash(&(LEAF_FLAG, key, value)).unwrap().into()
}