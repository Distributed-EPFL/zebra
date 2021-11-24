use crate::{
    common::{store::Field, tree::Path},
    database::{
        errors::QueryError,
        interact::{Batch, Operation},
        Query,
    },
};

use doomstack::{here, Doom, ResultExt, Top};

use std::{
    collections::HashSet,
    sync::atomic::{AtomicUsize, Ordering},
    vec::Vec,
};

pub(crate) type Tid = usize;

static TID: AtomicUsize = AtomicUsize::new(0);

pub struct TableTransaction<Key: Field, Value: Field> {
    tid: Tid,
    operations: Vec<Operation<Key, Value>>,
    paths: HashSet<Path>,
}

impl<Key, Value> TableTransaction<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub fn new() -> Self {
        TableTransaction {
            tid: TID.fetch_add(1, Ordering::Relaxed),
            operations: Vec::new(),
            paths: HashSet::new(),
        }
    }

    pub fn get(&mut self, key: &Key) -> Result<Query, Top<QueryError>> {
        let operation = Operation::<Key, Value>::get(key).pot(QueryError::HashError, here!())?;

        if self.paths.insert(operation.path) {
            let query = Query {
                tid: self.tid,
                path: operation.path,
            };

            self.operations.push(operation);
            Ok(query)
        } else {
            QueryError::KeyCollision.fail().spot(here!())
        }
    }

    pub fn set(&mut self, key: Key, value: Value) -> Result<(), Top<QueryError>> {
        let operation = Operation::set(key, value).pot(QueryError::HashError, here!())?;

        if self.paths.insert(operation.path) {
            self.operations.push(operation);
            Ok(())
        } else {
            QueryError::KeyCollision.fail().spot(here!())
        }
    }

    pub fn remove(&mut self, key: &Key) -> Result<(), Top<QueryError>> {
        let operation = Operation::remove(key).pot(QueryError::HashError, here!())?;

        if self.paths.insert(operation.path) {
            self.operations.push(operation);
            Ok(())
        } else {
            QueryError::KeyCollision.fail().spot(here!())
        }
    }

    pub(crate) fn finalize(self) -> (Tid, Batch<Key, Value>) {
        (self.tid, Batch::new(self.operations))
    }
}
