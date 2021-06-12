use serde::Serialize;

use super::operation::Operation;
use super::prefix::Prefix;
use super::task::Task;

pub(super) struct Batch<'a, Key: Serialize, Value: Serialize> {
    prefix: Prefix,
    operations: &'a [Operation<Key, Value>],
}

impl<'a, Key, Value> Batch<'a, Key, Value>
where
    Key: Serialize,
    Value: Serialize,
{
    pub fn new(operations: &'a mut [Operation<Key, Value>]) -> Self {
        operations.sort_unstable_by(|lho, rho| lho.path.cmp(&rho.path)); // TODO: Replace with `rayon`'s parallel sort if this becomes a bottleneck.
        Batch {
            prefix: Prefix::root(),
            operations,
        }
    }

    pub fn prefix(&self) -> &Prefix {
        &self.prefix
    }

    pub fn task(&self) -> Task<Key, Value> {
        match self.operations.len() {
            0 => Task::Pass,
            1 => Task::Do(&self.operations[0]),
            _ => Task::Split,
        }
    }

    pub fn left(&self) -> Self {
        Batch {
            prefix: self.prefix.left(),
            operations: &self.operations[self.partition()..],
        }
    }

    pub fn right(&self) -> Self {
        Batch {
            prefix: self.prefix.right(),
            operations: &self.operations[..self.partition()],
        }
    }

    fn partition(&self) -> usize {
        let right = self.prefix.right();
        self.operations
            .partition_point(|operation| right.contains(&operation.path))
    }
}
