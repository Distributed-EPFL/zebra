use crate::common::{data::Bytes, store::Field};

use doomstack::Top;

use serde::{Deserialize, Serialize};

use talk::crypto::primitives::{hash, hash::HashError};

use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Wrap<Inner: Field> {
    digest: Bytes,
    inner: Arc<Inner>,
}

impl<Inner> Wrap<Inner>
where
    Inner: Field,
{
    pub fn new(inner: Inner) -> Result<Self, Top<HashError>> {
        Ok(Wrap {
            digest: hash::hash(&inner)?.into(),
            inner: Arc::new(inner),
        })
    }

    pub fn digest(&self) -> Bytes {
        self.digest
    }

    pub fn inner(&self) -> &Arc<Inner> {
        &self.inner
    }
}

impl<Inner> Clone for Wrap<Inner>
where
    Inner: Field,
{
    fn clone(&self) -> Self {
        Wrap {
            digest: self.digest,
            inner: self.inner.clone(),
        }
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
