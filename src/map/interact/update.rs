use crate::{
    common::{data::Bytes, store::Field, tree::Path},
    map::{interact::Action, store::Wrap},
};

use doomstack::Top;

use talk::crypto::primitives::{hash, hash::HashError};

#[derive(Debug)]
pub(crate) struct Update<Key: Field, Value: Field> {
    pub path: Path,
    pub action: Action<Key, Value>,
}

impl<Key, Value> Update<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub fn insert(key: Key, value: Value) -> Result<Self, Top<HashError>> {
        let key = Wrap::new(key)?;
        let value = Wrap::new(value)?;

        Ok(Update {
            path: Path::from(key.digest()),
            action: Action::Insert(key, value),
        })
    }

    pub fn remove(key: &Key) -> Result<Self, Top<HashError>> {
        let hash: Bytes = hash::hash(key)?.into();

        Ok(Update {
            path: Path::from(hash),
            action: Action::Remove,
        })
    }
}
