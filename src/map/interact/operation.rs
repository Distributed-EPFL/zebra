use crate::{
    common::{data::Bytes, store::Field, tree::Path},
    map::{interact::Action, store::Wrap},
};

use drop::crypto::hash;
use drop::crypto::hash::HashError;

#[derive(Debug)]
pub(crate) struct Operation<Key: Field, Value: Field> {
    pub path: Path,
    pub action: Action<Key, Value>,
}

impl<Key, Value> Operation<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub fn get(key: &Key) -> Result<Self, HashError> {
        let hash: Bytes = hash::hash(key)?.into();

        Ok(Operation {
            path: Path::from(hash),
            action: Action::Get,
        })
    }

    pub fn set(key: Key, value: Value) -> Result<Self, HashError> {
        let key = Wrap::new(key)?;
        let value = Wrap::new(value)?;

        Ok(Operation {
            path: Path::from(*key.digest()),
            action: Action::Set(key, value),
        })
    }

    pub fn remove(key: &Key) -> Result<Self, HashError> {
        let hash: Bytes = hash::hash(key)?.into();

        Ok(Operation {
            path: Path::from(hash),
            action: Action::Remove,
        })
    }
}