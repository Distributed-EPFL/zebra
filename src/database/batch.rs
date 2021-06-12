use serde::Serialize;

use super::operation::Operation;
use super::prefix::Prefix;
use super::task::Task;

pub(crate) struct Batch<'a, Key: Serialize, Value: Serialize> {
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

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::prefix::Prefix;
    use super::super::action::Action;
    use super::super::wrap::Wrap;

    use std::vec::Vec;

    fn split_recursion(batch: &Batch<u32, u32>) -> (u32, bool) {
        match batch.task() {
            Task::Pass => (0, true),
            Task::Do(operation) => {
                if batch.prefix().contains(&operation.path) {
                    (1, true)
                } else {
                    (1, false)
                }
            }
            Task::Split => {
                let (lcount, lpass) = split_recursion(&batch.left());
                let (rcount, rpass) = split_recursion(&batch.right());

                (lcount + rcount, lpass && rpass)
            }
        }
    }

    #[test]
    fn operation() {
        let prefix = Prefix::root()
            .left()
            .left()
            .left()
            .right()
            .left()
            .left()
            .right()
            .right()
            .right()
            .right()
            .left()
            .right()
            .left()
            .right()
            .left()
            .left();

        let set = Operation::set(0u32, 8u32).unwrap();
        assert!(prefix.contains(&set.path));
        assert_eq!(set.key, Wrap::new(0u32).unwrap());
        assert_eq!(set.action, Action::Set(Wrap::new(8u32).unwrap()));

        let remove = Operation::remove(0u32).unwrap();
        assert_eq!(remove.path, set.path);
        assert_eq!(remove.key, set.key);
        assert_eq!(remove.action, Action::<u32>::Remove);
    }

    #[test]
    fn prefix() {
        let mut operations: Vec<Operation<u32, u32>> = Vec::new();
        let batch = Batch::new(&mut operations);

        assert_eq!(batch.prefix(), &Prefix::root());
        assert_eq!(batch.left().prefix(), &Prefix::root().left());
        assert_eq!(
            batch
                .right()
                .right()
                .right()
                .left()
                .right()
                .right()
                .right()
                .prefix(),
            &Prefix::root()
                .right()
                .right()
                .right()
                .left()
                .right()
                .right()
                .right()
        );
    }

    #[test]
    fn task_develop() {
        let mut operations: Vec<Operation<u32, u32>> = (0u32..4u32)
            .map(|index| Operation::set(index, index).unwrap())
            .collect();
        let batch = Batch::new(&mut operations);

        assert_eq!(batch.task(), Task::Split);

        assert_eq!(batch.left().task(), Task::Split);
        assert_eq!(batch.right().task(), Task::Pass);

        assert_eq!(batch.left().left().task(), Task::Split);
        assert_eq!(
            batch.left().right().task(),
            Task::Do(&Operation::set(3u32, 3u32).unwrap())
        );

        assert_eq!(batch.left().left().left().task(), Task::Split);
        assert_eq!(
            batch.left().left().right().task(),
            Task::Do(&Operation::set(1u32, 1u32).unwrap())
        );

        assert_eq!(
            batch.left().left().left().left().task(),
            Task::Do(&Operation::set(2u32, 2u32).unwrap())
        );
        assert_eq!(
            batch.left().left().left().right().task(),
            Task::Do(&Operation::set(0u32, 0u32).unwrap())
        );
    }

    #[test]
    fn distribution() {
        let mut operations: Vec<Operation<u32, u32>> = (0u32..64u32)
            .map(|index| Operation::set(index, index).unwrap())
            .collect();
        let batch = Batch::new(&mut operations);

        assert_eq!(split_recursion(&batch), (64, true));
    }
}
