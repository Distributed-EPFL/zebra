use crate::common::{data::Bytes, store::Field};

use drop::crypto::hash;
use drop::crypto::hash::HashError;

use std::rc::Rc;

#[derive(Debug)]
pub(crate) struct Wrap<Inner: Field> {
    digest: Bytes,
    inner: Rc<Inner>,
}

impl<Inner> Wrap<Inner>
where
    Inner: Field,
{
    pub fn new(inner: Inner) -> Result<Self, HashError> {
        Ok(Wrap {
            digest: hash::hash(&inner)?.into(),
            inner: Rc::new(inner),
        })
    }

    pub fn digest(&self) -> &Bytes {
        &self.digest
    }

    pub fn inner(&self) -> &Rc<Inner> {
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
