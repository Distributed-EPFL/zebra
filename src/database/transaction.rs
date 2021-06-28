use snafu::ResultExt;

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::vec::Vec;

use super::errors::{HashError, KeyCollision, QueryError};
use super::field::Field;
use super::operation::Operation;
use super::path::Path;
use super::query::Query;

static TID: AtomicUsize = AtomicUsize::new(0);

pub struct Transaction<Key: Field, Value: Field> {
    tid: usize,
    operations: Vec<Operation<Key, Value>>,
    paths: HashSet<Path>,
}

impl<Key, Value> Transaction<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub fn new() -> Self {
        Transaction {
            tid: TID.fetch_add(1, Ordering::Relaxed),
            operations: Vec::new(),
            paths: HashSet::new(),
        }
    }

    pub fn get(&mut self, key: &Key) -> Result<Query, QueryError> {
        let operation = Operation::<Key, Value>::get(key).context(HashError)?;

        if self.paths.insert(operation.path) {
            let query = Query {
                tid: self.tid,
                path: operation.path,
            };

            self.operations.push(operation);
            Ok(query)
        } else {
            KeyCollision.fail()
        }
    }

    pub fn set(&mut self, key: Key, value: Value) -> Result<(), QueryError> {
        let operation = Operation::set(key, value).context(HashError)?;

        if self.paths.insert(operation.path) {
            self.operations.push(operation);
            Ok(())
        } else {
            KeyCollision.fail()
        }
    }

    pub fn remove(&mut self, key: &Key) -> Result<(), QueryError> {
        let operation = Operation::remove(key).context(HashError)?;

        if self.paths.insert(operation.path) {
            self.operations.push(operation);
            Ok(())
        } else {
            KeyCollision.fail()
        }
    }
}
