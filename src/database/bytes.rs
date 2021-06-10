use drop::crypto::hash::{Digest, SIZE};

#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord)]
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
