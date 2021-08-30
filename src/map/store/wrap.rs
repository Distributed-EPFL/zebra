use crate::common::{data::Bytes, store::Field};

use drop::crypto::hash;
use drop::crypto::hash::HashError;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone)]
pub(crate) struct Wrap<Inner: Field> {
    digest: Bytes,
    inner: Inner,
}

impl<Inner> Wrap<Inner>
where
    Inner: Field,
{
    pub fn new(inner: Inner) -> Result<Self, HashError> {
        Ok(Wrap {
            digest: hash::hash(&inner)?.into(),
            inner,
        })
    }

    pub fn take(self) -> Inner {
        self.inner
    }

    pub fn digest(&self) -> Bytes {
        self.digest
    }

    pub fn inner(&self) -> &Inner {
        &self.inner
    }
}

impl<Inner> PartialEq for Wrap<Inner>
where
    Inner: Field,
{
    fn eq(&self, rho: &Wrap<Inner>) -> bool {
        self.digest == rho.digest
    }
}

impl<Inner> Eq for Wrap<Inner> where Inner: Field {}

impl<Inner> Serialize for Wrap<Inner>
where
    Inner: Field,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.inner.serialize(serializer)
    }
}

impl<'de, Inner> Deserialize<'de> for Wrap<Inner>
where
    Inner: Field + Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let inner = Inner::deserialize(deserializer)?;
        Wrap::new(inner).map_err(|err| DeError::custom(err))
    }
}
