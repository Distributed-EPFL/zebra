use bit_vec::BitVec;

use crate::{
    common::tree::Direction,
    vector::{errors::ProofError, Node},
};

use doomstack::{here, Doom, ResultExt, Top};

use serde::{Deserialize, Serialize};

use serde_bytes::ByteBuf;

use talk::crypto::primitives::{hash, hash::Hash};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Proof {
    path: BitVec,
    proof: Vec<Hash>,
    siblings: Option<(Vec<ByteBuf>, usize)>,
}

impl Proof {
    pub(in crate::vector) fn new<I, Item: Serialize>(
        path: I,
        proof: Vec<Hash>,
        siblings: Option<(Vec<&Item>, usize)>,
    ) -> Self
    where
        I: IntoIterator<Item = Direction>,
    {
        let path = path
            .into_iter()
            .map(|direction| direction == Direction::Left)
            .collect::<BitVec>();

        let siblings = match siblings {
            None => None,
            Some((vec, pos)) => {
                let vec: Vec<ByteBuf> = vec
                    .into_iter()
                    .map(|item| ByteBuf::from(bincode::serialize(item).unwrap()))
                    .collect();
                Some((vec, pos))
            }
        };

        Proof {
            path,
            proof,
            siblings,
        }
    }

    pub fn verify<Item: Serialize + for<'de> Deserialize<'de>>(
        &self,
        root: Hash,
        item: &Item,
    ) -> Result<(), Top<ProofError>> {
        let mut hash = match &self.siblings {
            Some((vec, pos)) => {
                let vec: Vec<Item> = vec
                    .iter()
                    .map(|item| bincode::deserialize::<Item>(item.as_ref()).unwrap())
                    .collect();
                let mut vec: Vec<&Item> = vec.iter().collect();
                vec.insert(*pos, item);
                hash::hash(&Node::<&[&Item]>::Item(vec.as_slice()))
                    .pot(ProofError::HashError, here!())?
            }
            None => hash::hash(&Node::<&Item>::Item(item)).pot(ProofError::HashError, here!())?,
        };

        for (direction, sibling_hash) in self.path.iter().zip(self.proof.iter().cloned()) {
            let parent = match direction {
                true => Node::<Item>::Internal(hash, sibling_hash),
                false => Node::<Item>::Internal(sibling_hash, hash),
            };

            hash = hash::hash(&parent).unwrap();
        }

        if root != hash {
            return ProofError::RootMismatch.fail().spot(here!());
        }

        Ok(())
    }
}
