use drop::crypto::hash::{Digest, SIZE};

use serde::Serialize;

use std::fmt::{Debug, Error, Formatter, LowerHex};

#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(crate) struct Bytes(pub [u8; SIZE]);

impl From<Digest> for Bytes {
    fn from(digest: Digest) -> Bytes {
        Bytes(*digest.as_bytes())
    }
}

impl LowerHex for Bytes {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        for byte in &self.0 {
            write!(f, "{:x}", byte)?;
        }

        Ok(())
    }
}

impl Debug for Bytes {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "Bytes({:x})", self)?;
        Ok(())
    }
}
