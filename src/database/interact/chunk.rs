use crate::{
    common::{store::Field, tree::Prefix},
    database::interact::{Batch, Operation, Task},
};

use std::ops::Range;

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

    fn operations<'a, Key, Value>(
        &self,
        batch: &'a Batch<Key, Value>,
    ) -> &'a [Operation<Key, Value>]
    where
        Key: Field,
        Value: Field,
    {
        &batch.operations()[self.range.clone()]
    }

    fn operations_mut<'a, Key, Value>(
        &self,
        batch: &'a mut Batch<Key, Value>,
    ) -> &'a mut [Operation<Key, Value>]
    where
        Key: Field,
        Value: Field,
    {
        &mut batch.operations_mut()[self.range.clone()]
    }

    pub fn task<'a, Key, Value>(&self, batch: &'a mut Batch<Key, Value>) -> Task<'a, Key, Value>
    where
        Key: Field,
        Value: Field,
    {
        let operations = self.operations_mut(batch);

        match operations.len() {
            0 => Task::Pass,
            1 => Task::Do(&mut operations[0]),
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

    pub fn split<Key, Value>(&self, batch: &Batch<Key, Value>) -> (Self, Self)
    where
        Key: Field,
        Value: Field,
    {
        let partition = self.partition(batch);

        let left = Chunk {
            prefix: self.prefix.left(),
            range: (self.range.start + partition)..self.range.end,
        };

        let right = Chunk {
            prefix: self.prefix.right(),
            range: self.range.start..(self.range.start + partition),
        };

        (left, right)
    }

    pub fn snap<Key, Value>(
        &self,
        batch: Batch<Key, Value>,
    ) -> (Batch<Key, Value>, Self, Batch<Key, Value>, Self)
    where
        Key: Field,
        Value: Field,
    {
        debug_assert_eq!(self.range, 0..batch.operations().len());

        let partition = self.partition(&batch);
        let (left_batch, right_batch) = batch.snap(partition);

        let left_chunk = Chunk {
            prefix: self.prefix.left(),
            range: 0..left_batch.operations().len(),
        };

        let right_chunk = Chunk {
            prefix: self.prefix.right(),
            range: 0..right_batch.operations().len(),
        };

        (left_batch, left_chunk, right_batch, right_chunk)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::common::tree::Direction;

    impl Chunk {
        pub fn from_directions<Key, Value, I, J>(
            mut batch: Batch<Key, Value>,
            snaps: I,
            splits: J,
        ) -> (Batch<Key, Value>, Chunk)
        where
            Key: Field,
            Value: Field,
            I: IntoIterator<Item = Direction>,
            J: IntoIterator<Item = Direction>,
        {
            let mut chunk = Chunk::root(&batch);

            for direction in snaps {
                let (left_batch, left_chunk, right_batch, right_chunk) = chunk.snap(batch);

                if direction == Direction::Left {
                    batch = left_batch;
                    chunk = left_chunk;
                } else {
                    batch = right_batch;
                    chunk = right_chunk;
                };
            }

            for direction in splits {
                let (left, right) = chunk.split(&batch);
                chunk = if direction == Direction::Left {
                    left
                } else {
                    right
                };
            }

            (batch, chunk)
        }
    }

    #[test]
    fn prefix() {
        use Direction::{Left as L, Right as R};

        let new_batch = || Batch::<u32, u32>::new(Vec::new());

        assert_eq!(
            Chunk::from_directions(new_batch(), vec![], vec![]).1.prefix,
            Prefix::from_directions(vec![])
        );

        assert_eq!(
            Chunk::from_directions(new_batch(), vec![], vec![L])
                .1
                .prefix,
            Prefix::from_directions(vec![L])
        );

        assert_eq!(
            Chunk::from_directions(new_batch(), vec![L], vec![])
                .1
                .prefix,
            Prefix::from_directions(vec![L])
        );

        assert_eq!(
            Chunk::from_directions(new_batch(), vec![], vec![R])
                .1
                .prefix,
            Prefix::from_directions(vec![R])
        );

        assert_eq!(
            Chunk::from_directions(new_batch(), vec![R], vec![])
                .1
                .prefix,
            Prefix::from_directions(vec![R])
        );

        assert_eq!(
            Chunk::from_directions(new_batch(), vec![L], vec![L])
                .1
                .prefix,
            Prefix::from_directions(vec![L, L])
        );

        assert_eq!(
            Chunk::from_directions(new_batch(), vec![L], vec![R])
                .1
                .prefix,
            Prefix::from_directions(vec![L, R])
        );

        assert_eq!(
            Chunk::from_directions(new_batch(), vec![R], vec![L])
                .1
                .prefix,
            Prefix::from_directions(vec![R, L])
        );

        assert_eq!(
            Chunk::from_directions(new_batch(), vec![R], vec![R])
                .1
                .prefix,
            Prefix::from_directions(vec![R, R])
        );

        assert_eq!(
            Chunk::from_directions(new_batch(), vec![], vec![R, R, R, L, R, R, R])
                .1
                .prefix,
            Prefix::from_directions(vec![R, R, R, L, R, R, R])
        );

        assert_eq!(
            Chunk::from_directions(new_batch(), vec![R,], vec![R, R, L, R, R, R])
                .1
                .prefix,
            Prefix::from_directions(vec![R, R, R, L, R, R, R])
        );

        assert_eq!(
            Chunk::from_directions(new_batch(), vec![R, R,], vec![R, L, R, R, R])
                .1
                .prefix,
            Prefix::from_directions(vec![R, R, R, L, R, R, R])
        );

        assert_eq!(
            Chunk::from_directions(new_batch(), vec![R, R, R,], vec![L, R, R, R])
                .1
                .prefix,
            Prefix::from_directions(vec![R, R, R, L, R, R, R])
        );

        assert_eq!(
            Chunk::from_directions(new_batch(), vec![R, R, R, L,], vec![R, R, R])
                .1
                .prefix,
            Prefix::from_directions(vec![R, R, R, L, R, R, R])
        );

        assert_eq!(
            Chunk::from_directions(new_batch(), vec![R, R, R, L, R,], vec![R, R])
                .1
                .prefix,
            Prefix::from_directions(vec![R, R, R, L, R, R, R])
        );

        assert_eq!(
            Chunk::from_directions(new_batch(), vec![R, R, R, L, R, R,], vec![R])
                .1
                .prefix,
            Prefix::from_directions(vec![R, R, R, L, R, R, R])
        );

        assert_eq!(
            Chunk::from_directions(new_batch(), vec![R, R, R, L, R, R, R], vec![])
                .1
                .prefix,
            Prefix::from_directions(vec![R, R, R, L, R, R, R])
        );
    }

    #[test]
    fn tree() {
        use Direction::{Left as L, Right as R};

        let new_batch = || {
            let operations: Vec<Operation<u32, u32>> =
                (0u32..4u32).map(|index| set!(index, index)).collect();

            Batch::new(operations)
        };

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![], vec![]);
        assert_eq!(chunk.task(&mut batch), Task::Split);

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![], vec![L]);
        assert_eq!(chunk.task(&mut batch), Task::Split);

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![L], vec![]);
        assert_eq!(chunk.task(&mut batch), Task::Split);

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![], vec![R]);
        assert_eq!(chunk.task(&mut batch), Task::Pass);

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![R], vec![]);
        assert_eq!(chunk.task(&mut batch), Task::Pass);

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![], vec![L, L]);
        assert_eq!(chunk.task(&mut batch), Task::Split);

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![L], vec![L]);
        assert_eq!(chunk.task(&mut batch), Task::Split);

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![L, L], vec![]);
        assert_eq!(chunk.task(&mut batch), Task::Split);

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![], vec![L, R]);
        assert_eq!(chunk.task(&mut batch), Task::Do(&mut set!(3u32, 3u32)));

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![L], vec![R]);
        assert_eq!(chunk.task(&mut batch), Task::Do(&mut set!(3u32, 3u32)));

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![L, R], vec![]);
        assert_eq!(chunk.task(&mut batch), Task::Do(&mut set!(3u32, 3u32)));

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![], vec![L, L, L]);
        assert_eq!(chunk.task(&mut batch), Task::Split);

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![L], vec![L, L]);
        assert_eq!(chunk.task(&mut batch), Task::Split);

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![L, L], vec![L]);
        assert_eq!(chunk.task(&mut batch), Task::Split);

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![L, L, L], vec![]);
        assert_eq!(chunk.task(&mut batch), Task::Split);

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![], vec![L, L, R]);
        assert_eq!(chunk.task(&mut batch), Task::Do(&mut set!(1u32, 1u32)));

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![L], vec![L, R]);
        assert_eq!(chunk.task(&mut batch), Task::Do(&mut set!(1u32, 1u32)));

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![L, L], vec![R]);
        assert_eq!(chunk.task(&mut batch), Task::Do(&mut set!(1u32, 1u32)));

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![L, L, R], vec![]);
        assert_eq!(chunk.task(&mut batch), Task::Do(&mut set!(1u32, 1u32)));

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![], vec![L, L, L, L]);
        assert_eq!(chunk.task(&mut batch), Task::Do(&mut set!(2u32, 2u32)));

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![L], vec![L, L, L]);
        assert_eq!(chunk.task(&mut batch), Task::Do(&mut set!(2u32, 2u32)));

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![L, L], vec![L, L]);
        assert_eq!(chunk.task(&mut batch), Task::Do(&mut set!(2u32, 2u32)));

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![L, L, L], vec![L]);
        assert_eq!(chunk.task(&mut batch), Task::Do(&mut set!(2u32, 2u32)));

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![L, L, L, L], vec![]);
        assert_eq!(chunk.task(&mut batch), Task::Do(&mut set!(2u32, 2u32)));

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![], vec![L, L, L, R]);
        assert_eq!(chunk.task(&mut batch), Task::Do(&mut set!(0u32, 0u32)));

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![L], vec![L, L, R]);
        assert_eq!(chunk.task(&mut batch), Task::Do(&mut set!(0u32, 0u32)));

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![L, L], vec![L, R]);
        assert_eq!(chunk.task(&mut batch), Task::Do(&mut set!(0u32, 0u32)));

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![L, L, L], vec![R]);
        assert_eq!(chunk.task(&mut batch), Task::Do(&mut set!(0u32, 0u32)));

        let (mut batch, chunk) = Chunk::from_directions(new_batch(), vec![L, L, L, R], vec![]);
        assert_eq!(chunk.task(&mut batch), Task::Do(&mut set!(0u32, 0u32)));
    }

    #[test]
    fn distribution() {
        fn check_tree(batch: Batch<u32, u32>, chunk: Chunk, snap_ttl: usize) -> (u32, bool) {
            fn recursion(
                mut batch: Batch<u32, u32>,
                chunk: Chunk,
                snap_ttl: usize,
            ) -> (Option<Batch<u32, u32>>, u32, bool) {
                match chunk.task(&mut batch) {
                    Task::Pass => (Some(batch), 0, true),
                    Task::Do(operation) => {
                        if chunk.prefix.contains(&operation.path) {
                            (Some(batch), 1, true)
                        } else {
                            (Some(batch), 1, false)
                        }
                    }
                    Task::Split => {
                        if snap_ttl > 0 {
                            let (left_batch, left_chunk, right_batch, right_chunk) =
                                chunk.snap(batch);

                            let (_, lcount, lpass) =
                                recursion(left_batch, left_chunk, snap_ttl - 1);

                            let (_, rcount, rpass) =
                                recursion(right_batch, right_chunk, snap_ttl - 1);

                            (None, lcount + rcount, lpass && rpass)
                        } else {
                            let (left_chunk, right_chunk) = chunk.split(&batch);

                            let (batch, lcount, lpass) = recursion(batch, left_chunk, 0);

                            let (batch, rcount, rpass) = recursion(batch.unwrap(), right_chunk, 0);

                            (batch, lcount + rcount, lpass && rpass)
                        }
                    }
                }
            }

            let (_, count, pass) = recursion(batch, chunk, snap_ttl);
            (count, pass)
        }

        let new_batch = || {
            let operations: Vec<Operation<u32, u32>> =
                (0u32..64u32).map(|index| set!(index, index)).collect();

            Batch::new(operations)
        };

        for snap_ttl in 0..8 {
            let (count, pass) = check_tree(new_batch(), Chunk::root(&new_batch()), snap_ttl);

            assert_eq!((count, pass), (64, true));
        }
    }
}
