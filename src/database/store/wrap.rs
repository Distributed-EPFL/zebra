use crate::database::{data::Bytes, store::Field};

use drop::crypto::hash;
use drop::crypto::hash::HashError;

use serde::Serialize;

use std::sync::Arc;

#[derive(Debug, Serialize)]
pub(crate) struct Wrap<Inner: Field> {
    digest: Bytes,
    #[serde(skip)]
    inner: Arc<Inner>,
}

impl<Inner> Wrap<Inner>
where
    Inner: Field,
{
    pub fn new(inner: Inner) -> Result<Self, HashError> {
        Ok(Wrap {
            digest: hash::hash(&inner)?.into(),
            inner: Arc::new(inner),
        })
    }

    pub fn digest(&self) -> &Bytes {
        &self.digest
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
