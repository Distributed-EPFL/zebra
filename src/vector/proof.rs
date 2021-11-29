use bit_vec::BitVec;

use crate::{
    common::tree::Direction,
    vector::{errors::ProofError, Node},
};

use doomstack::{here, Doom, ResultExt, Top};

use serde::{Deserialize, Serialize};

use talk::crypto::primitives::{hash, hash::Hash};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Proof {
    path: BitVec,
    proof: Vec<Hash>,
}

impl Proof {
    pub(in crate::vector) fn new<I>(path: I, proof: Vec<Hash>) -> Self
    where
        I: IntoIterator<Item = Direction>,
    {
        let path = path
            .into_iter()
            .map(|direction| direction == Direction::Left)
            .collect::<BitVec>();

        Proof { path, proof }
    }

    pub fn verify<Item>(&self, root: Hash, item: &Item) -> Result<(), Top<ProofError>>
    where
        Item: Serialize,
    {
        let hash = hash::hash(&item).pot(ProofError::HashError, here!())?;
        let mut hash = hash::hash(&Node::Item(hash)).pot(ProofError::HashError, here!())?;

        for (direction, sibling_hash) in self.path.iter().zip(self.proof.iter().cloned()) {
            let parent = match direction {
                true => Node::Internal(hash, sibling_hash),
                false => Node::Internal(sibling_hash, hash),
            };

            hash = hash::hash(&parent).unwrap();
        }

        if root != hash {
            return ProofError::RootMismatch.fail().spot(here!());
        }

        Ok(())
    }
}
