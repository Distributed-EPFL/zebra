use drop::crypto::hash::{Digest, SIZE};

use serde::Serialize;

#[derive(
    Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize,
)]
pub(crate) struct Bytes(pub [u8; SIZE]);

impl Bytes {
    pub fn empty() -> Self {
        Bytes([0; SIZE])
    }
}

impl From<Digest> for Bytes {
    fn from(digest: Digest) -> Bytes {
        Bytes(*digest.as_bytes())
    }
}
