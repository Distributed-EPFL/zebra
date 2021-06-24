use oh_snap::Snap;

use std::vec::Vec;

use super::field::Field;
use super::operation::Operation;

pub(crate) struct Batch<Key: Field, Value: Field> {
    operations: Snap<Operation<Key, Value>>,
}

impl<Key, Value> Batch<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub fn new(mut operations: Vec<Operation<Key, Value>>) -> Self {
        operations.sort_unstable_by(|lho, rho| lho.path.cmp(&rho.path)); // TODO: Replace with `rayon`'s parallel sort if this becomes a bottleneck.
        Batch {
            operations: Snap::new(operations),
        }
    }

    pub fn snap(self, at: usize) -> (Self, Self) {
        let (right, left) = self.operations.snap(at); // `oh-snap` stores the lowest-index elements in `left`, while `zebra` stores them in `right`, hence the swap
        (Batch { operations: left }, Batch { operations: right })
    }

    pub fn operations(&self) -> &[Operation<Key, Value>] {
        &self.operations
    }

    pub fn operations_mut(&mut self) -> &mut [Operation<Key, Value>] {
        &mut self.operations
    }
}
