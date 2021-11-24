use crate::common::{data::Bytes, store::Field, tree::Path};

use doomstack::Top;

use talk::crypto::primitives::{hash, hash::HashError};

#[derive(Debug)]
pub(crate) struct Query {
    pub path: Path,
}

impl Query {
    pub fn new<Key>(key: &Key) -> Result<Self, Top<HashError>>
    where
        Key: Field,
    {
        let hash: Bytes = hash::hash(key)?.into();

        Ok(Query {
            path: Path::from(hash),
        })
    }
}
