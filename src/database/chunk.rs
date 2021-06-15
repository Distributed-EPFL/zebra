use std::ops::Range;

use super::batch::Batch;
use super::field::Field;
use super::operation::Operation;
use super::prefix::Prefix;
use super::task::Task;

#[derive(Clone)]
pub(crate) struct Chunk {
    prefix: Prefix,
    range: Range<usize>,
}

impl Chunk {
    pub fn root<Key, Value>(batch: &Batch<Key, Value>) -> Self
    where
        Key: Field,
        Value: Field,
    {
        Chunk {
            prefix: Prefix::root(),
            range: 0..batch.operations().len(),
        }
    }

    pub fn prefix(&self) -> &Prefix {
        &self.prefix
    }

    pub fn operations<'a, Key, Value>(
        &self,
        batch: &'a Batch<Key, Value>,
    ) -> &'a [Operation<Key, Value>]
    where
        Key: Field,
        Value: Field,
    {
        &batch.operations()[self.range.clone()]
    }

    pub fn task<'a, Key, Value>(
        &self,
        batch: &'a Batch<Key, Value>,
    ) -> Task<'a, Key, Value>
    where
        Key: Field,
        Value: Field,
    {
        let operations = self.operations(batch);

        match operations.len() {
            0 => Task::Pass,
            1 => Task::Do(&operations[0]),
            _ => Task::Split,
        }
    }

    fn partition<Key, Value>(&self, batch: &Batch<Key, Value>) -> usize
    where
        Key: Field,
        Value: Field,
    {
        let right = self.prefix.right();
        self.operations(batch)
            .partition_point(|operation| right.contains(&operation.path))
    }

    pub fn left<Key, Value>(&self, batch: &Batch<Key, Value>) -> Self
    where
        Key: Field,
        Value: Field,
    {
        Chunk {
            prefix: self.prefix.left(),
            range: (self.range.start + self.partition(batch))..self.range.end,
        }
    }

    pub fn right<Key, Value>(&self, batch: &Batch<Key, Value>) -> Self
    where
        Key: Field,
        Value: Field,
    {
        Chunk {
            prefix: self.prefix.right(),
            range: self.range.start..(self.range.start + self.partition(batch)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::direction::Direction;

    fn prefix_from_directions(directions: &Vec<Direction>) -> Prefix {
        let mut prefix = Prefix::root();

        for &direction in directions {
            prefix = if direction == Direction::Left {
                prefix.left()
            } else {
                prefix.right()
            };
        }

        prefix
    }

    fn chunk_from_directions<Key, Value>(
        batch: &Batch<Key, Value>,
        directions: &Vec<Direction>,
    ) -> Chunk
    where
        Key: Field,
        Value: Field,
    {
        let mut chunk = Chunk::root(batch);

        for &direction in directions {
            chunk = if direction == Direction::Left {
                chunk.left(batch)
            } else {
                chunk.right(batch)
            };
        }

        chunk
    }

    fn split_recursion(batch: &Batch<u32, u32>, chunk: Chunk) -> (u32, bool) {
        match chunk.task(batch) {
            Task::Pass => (0, true),
            Task::Do(operation) => {
                if chunk.prefix().contains(&operation.path) {
                    (1, true)
                } else {
                    (1, false)
                }
            }
            Task::Split => {
                let (lcount, lpass) = split_recursion(batch, chunk.left(batch));
                let (rcount, rpass) =
                    split_recursion(batch, chunk.right(batch));

                (lcount + rcount, lpass && rpass)
            }
        }
    }

    #[test]
    fn prefix() {
        use Direction::{Left as L, Right as R};

        let operations: Vec<Operation<u32, u32>> = Vec::new();
        let batch = Batch::new(operations);

        assert_eq!(Chunk::root(&batch).prefix(), &Prefix::root());
        assert_eq!(
            Chunk::root(&batch).left(&batch).prefix(),
            &Prefix::root().left()
        );
        assert_eq!(
            *chunk_from_directions(&batch, &vec![R, R, R, L, R, R, R]).prefix(),
            prefix_from_directions(&vec![R, R, R, L, R, R, R])
        );
    }

    #[test]
    fn task() {
        let operations: Vec<Operation<u32, u32>> = (0u32..4u32)
            .map(|index| Operation::set(index, index).unwrap())
            .collect();
        let batch = Batch::new(operations);

        assert_eq!(Chunk::root(&batch).task(&batch), Task::Split);

        assert_eq!(Chunk::root(&batch).left(&batch).task(&batch), Task::Split);
        assert_eq!(Chunk::root(&batch).right(&batch).task(&batch), Task::Pass);

        assert_eq!(
            Chunk::root(&batch).left(&batch).left(&batch).task(&batch),
            Task::Split
        );
        assert_eq!(
            Chunk::root(&batch).left(&batch).right(&batch).task(&batch),
            Task::Do(&Operation::set(3u32, 3u32).unwrap())
        );

        assert_eq!(
            Chunk::root(&batch)
                .left(&batch)
                .left(&batch)
                .left(&batch)
                .task(&batch),
            Task::Split
        );
        assert_eq!(
            Chunk::root(&batch)
                .left(&batch)
                .left(&batch)
                .right(&batch)
                .task(&batch),
            Task::Do(&Operation::set(1u32, 1u32).unwrap())
        );

        assert_eq!(
            Chunk::root(&batch)
                .left(&batch)
                .left(&batch)
                .left(&batch)
                .left(&batch)
                .task(&batch),
            Task::Do(&Operation::set(2u32, 2u32).unwrap())
        );
        assert_eq!(
            Chunk::root(&batch)
                .left(&batch)
                .left(&batch)
                .left(&batch)
                .right(&batch)
                .task(&batch),
            Task::Do(&Operation::set(0u32, 0u32).unwrap())
        );
    }

    #[test]
    fn distribution() {
        let operations: Vec<Operation<u32, u32>> = (0u32..64u32)
            .map(|index| Operation::set(index, index).unwrap())
            .collect();
        let batch = Batch::new(operations);

        assert_eq!(split_recursion(&batch, Chunk::root(&batch)), (64, true));
    }
}
