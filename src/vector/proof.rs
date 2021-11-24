use crate::{
    common::tree::Direction,
    vector::{errors::ProofError, Node},
};

use doomstack::{here, Doom, ResultExt, Top};

use serde::{Deserialize, Serialize};

use talk::crypto::primitives::{hash, hash::Hash};

#[derive(Debug, Serialize, Deserialize)]
pub struct Proof {
    path: Vec<Direction>,
    proof: Vec<Hash>,
}

impl Proof {
    pub(in crate::vector) fn new(path: Vec<Direction>, proof: Vec<Hash>) -> Self {
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
                Direction::Left => Node::Internal(hash, sibling_hash),
                Direction::Right => Node::Internal(sibling_hash, hash),
            };

            hash = hash::hash(&parent).unwrap();
        }

        if root != hash {
            return ProofError::RootMismatch.fail().spot(here!());
        }

        Ok(())
    }
}
