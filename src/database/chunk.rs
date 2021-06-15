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
