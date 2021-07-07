use oh_snap::Snap;

use crate::database::{interact::Operation, store::Field};

use rayon::prelude::*;

use std::vec::Vec;

pub(crate) struct Batch<Key: Field, Value: Field> {
    operations: Snap<Operation<Key, Value>>,
}

impl<Key, Value> Batch<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub fn new(mut operations: Vec<Operation<Key, Value>>) -> Self {
        operations.par_sort_unstable_by(|lho, rho| lho.path.cmp(&rho.path));
        Batch {
            operations: Snap::new(operations),
        }
    }

    pub fn snap(self, at: usize) -> (Self, Self) {
        let (right, left) = self.operations.snap(at); // `oh-snap` stores the lowest-index elements in `left`, while `zebra` stores them in `right`, hence the swap
        (Batch { operations: left }, Batch { operations: right })
    }

    pub fn merge(left: Self, right: Self) -> Self {
        Batch {
            operations: Snap::merge(right.operations, left.operations), // `oh-snap` stores the lowest-index elements in `left`, while `zebra` stores them in `right`, hence the swap
        }
    }

    pub fn operations(&self) -> &[Operation<Key, Value>] {
        &self.operations
    }

    pub fn operations_mut(&mut self) -> &mut [Operation<Key, Value>] {
        &mut self.operations
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::database::tree::Path;

    #[test]
    fn snap_merge() {
        let operations: Vec<Operation<u32, u32>> =
            (0..128).map(|i| Operation::set(i, i).unwrap()).collect();

        let batch = Batch::new(operations);

        let reference: Vec<Path> = batch
            .operations()
            .iter()
            .map(|operation| operation.path)
            .collect();

        let (l, r) = batch.snap(64);

        let (ll, lr) = l.snap(32);
        let (rl, rr) = r.snap(32);

        let l = Batch::merge(ll, lr);
        let r = Batch::merge(rl, rr);

        let batch = Batch::merge(l, r);

        assert!(batch
            .operations()
            .iter()
            .zip(reference.iter())
            .all(|(operation, reference)| operation.path == *reference));
    }
}
