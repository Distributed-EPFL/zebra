use drop::crypto::hash::{Digest, SIZE};

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
