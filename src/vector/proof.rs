use crate::vector::{errors::ProofError, Children, Node};

use doomstack::{here, Doom, ResultExt, Top};

use serde::{Deserialize, Serialize};

use talk::crypto::primitives::hash;
use talk::crypto::primitives::hash::Hash;

#[derive(Serialize, Deserialize)]
pub struct Proof {
    width: u8,
    index: u64,
    branch: Vec<Children>,
}

impl Proof {
    pub(in crate::vector) fn new(
        width: u8,
        index: u64,
        branch: Vec<Children>,
    ) -> Self {
        Proof {
            width,
            index,
            branch,
        }
    }

    pub fn verify<Item>(
        &self,
        root: Hash,
        item: &Item,
    ) -> Result<(), Top<ProofError>>
    where
        Item: Serialize,
    {
        if self.branch.len() > 0 {
            if root
                != hash::hash(&Node::Internal::<Item>(&self.branch[0])).unwrap()
            {
                return ProofError::RootMismatch.fail().spot(here!());
            }

            for depth in 0..(self.branch.len() - 1) {
                let label = self.label(depth)?;

                if label
                    != hash::hash(&Node::Internal::<Item>(
                        &self.branch[depth + 1],
                    ))
                    .unwrap()
                {
                    return ProofError::Mislabled.fail().spot(here!());
                }
            }

            let label = self.label(self.branch.len() - 1)?;

            if label
                != hash::hash(&Node::Item(item))
                    .pot(ProofError::HashError, here!())?
            {
                return ProofError::ItemMismatch.fail().spot(here!());
            }
        } else {
            if root != hash::hash(&Node::Item(item)).unwrap() {
                return ProofError::ItemMismatch.fail().spot(here!());
            }
        }

        Ok(())
    }

    fn label(&self, depth: usize) -> Result<Hash, Top<ProofError>> {
        let shift = (63 - self.width) - (depth as u8);
        let mask = 1 << shift;
        let direction = self.index & mask > 0;

        match (&self.branch[depth], direction) {
            (Children::Only(only), false) => Ok(*only),
            (Children::Siblings(left, _), false) => Ok(*left),
            (Children::Siblings(_, right), true) => Ok(*right),
            _ => return ProofError::OutOfPath.fail().spot(here!()),
        }
    }
}
