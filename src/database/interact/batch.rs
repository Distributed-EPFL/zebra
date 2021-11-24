use oh_snap::Snap;

use crate::{common::store::Field, database::interact::Operation};

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

    use crate::{
        common::{data::Bytes, tree::Path},
        database::interact::Action,
    };

    use std::{
        collections::{HashMap, HashSet},
        fmt::Debug,
        hash::Hash,
    };

    use talk::crypto::primitives::hash;

    impl<Key, Value> Batch<Key, Value>
    where
        Key: Field,
        Value: Field + Clone,
    {
        pub fn collect_raw_gets(&self) -> HashMap<Bytes, Option<Value>> {
            self.operations
                .iter()
                .filter_map(|operation| match &operation.action {
                    Action::Get(holder) => Some((
                        operation.path.into(),
                        holder.clone().map(|value| (*value).clone()),
                    )),
                    _ => None,
                })
                .collect()
        }

        pub fn assert_gets<I>(&self, reference: I)
        where
            Key: Debug + Clone + Eq + Hash,
            Value: Debug + Clone + Eq + Hash,
            I: IntoIterator<Item = (Key, Option<Value>)>,
        {
            let reference: HashMap<Key, Option<Value>> = reference.into_iter().collect();

            let preimage: HashMap<Bytes, Key> = reference
                .iter()
                .map(|(k, _)| (Bytes::from(hash::hash(k).unwrap()), k.clone()))
                .collect();

            let actual: HashSet<(Bytes, Option<Value>)> =
                self.collect_raw_gets().into_iter().collect();

            let reference: HashSet<(Bytes, Option<Value>)> = reference
                .iter()
                .map(|(k, v)| (Bytes::from(hash::hash(k).unwrap()), v.clone()))
                .collect();

            #[derive(Debug, Hash, PartialEq, Eq)]
            enum DiffKey<Key> {
                Known(Key),
                Unknown(Bytes),
            }

            let differences: HashSet<(DiffKey<Key>, Option<Value>)> = reference
                .symmetric_difference(&actual)
                .map(|(hash, value)| {
                    (
                        if let Some(key) = preimage.get(hash) {
                            DiffKey::Known(key.clone())
                        } else {
                            DiffKey::Unknown(*hash)
                        },
                        value.clone(),
                    )
                })
                .collect();

            assert_eq!(differences, HashSet::new());
        }
    }

    #[test]
    fn snap_merge() {
        let operations: Vec<Operation<u32, u32>> = (0..128).map(|i| set!(i, i)).collect();

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
