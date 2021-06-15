use std::sync::Arc;
use std::vec::Vec;

use super::field::Field;
use super::operation::Operation;

pub(crate) struct Batch<Key: Field, Value: Field> {
    operations: Arc<Vec<Operation<Key, Value>>>,
}

impl<Key, Value> Batch<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub fn new(mut operations: Vec<Operation<Key, Value>>) -> Self {
        operations.sort_unstable_by(|lho, rho| lho.path.cmp(&rho.path)); // TODO: Replace with `rayon`'s parallel sort if this becomes a bottleneck.
        Batch {
            operations: Arc::new(operations),
        }
    }

    pub fn operations(&self) -> &[Operation<Key, Value>] {
        &self.operations
    }
}
