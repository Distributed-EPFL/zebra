use crate::vector::{errors::VectorError, Children, Node, Proof};

use doomstack::{here, ResultExt, Top};

use serde::Serialize;

use talk::crypto::primitives::hash;
use talk::crypto::primitives::hash::Hash;

pub struct Vector<Item: Serialize> {
    layers: Vec<Vec<Hash>>,
    items: Vec<Item>,
}

impl<Item> Vector<Item>
where
    Item: Serialize,
{
    pub fn new(items: Vec<Item>) -> Result<Self, Top<VectorError>> {
        if items.is_empty() {
            panic!("called `Vector::new` with an empty `items`");
        }

        let mut layers = Vec::new();

        let mut layer = items
            .iter()
            .map(|element| {
                hash::hash(&Node::Item(element))
                    .pot(VectorError::HashError, here!())
            })
            .collect::<Result<Vec<Hash>, Top<VectorError>>>()?;

        while layer.len() > 1 {
            layer = {
                let next = layer
                    .chunks(2)
                    .map(Into::into)
                    .map(|children| {
                        hash::hash(&Node::Internal::<Item>(&children)).unwrap()
                    })
                    .collect::<Vec<_>>();

                layers.push(layer);
                next
            };
        }

        layers.push(layer);
        layers.reverse();

        Ok(Vector { layers, items })
    }

    pub fn root(&self) -> Hash {
        self.layers[0][0]
    }

    pub fn items(&self) -> &[Item] {
        &self.items
    }

    pub fn prove(&self, index: usize) -> Proof {
        let index = index as u64;
        let width = (self.items.len() - 1).leading_zeros() as u8;

        let branch = (0..(self.layers.len() - 1))
            .map(|depth| {
                let shift = (64 - width) - (depth as u8);
                let parent = index >> shift;

                if (parent * 2 + 1) as usize >= self.layers[depth + 1].len() {
                    Children::Only(
                        self.layers[depth + 1][(parent * 2) as usize],
                    )
                } else {
                    Children::Siblings(
                        self.layers[depth + 1][(parent * 2) as usize],
                        self.layers[depth + 1][(parent * 2 + 1) as usize],
                    )
                }
            })
            .collect();

        Proof::new(width, index, branch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic]
    fn empty() {
        Vector::<()>::new(vec![]).unwrap();
    }

    #[test]
    fn one_item() {
        let vector = Vector::new(vec![0u32]).unwrap();

        assert_eq!(vector.layers.len(), 1);
        assert_eq!(vector.layers[0].len(), 1);

        assert_eq!(
            vector.layers[0][0],
            hash::hash(&Node::Item(&0u32)).unwrap()
        );
    }

    #[test]
    fn two_items() {
        let vector = Vector::new(vec![0u32, 1u32]).unwrap();

        assert_eq!(vector.layers.len(), 2);
        assert_eq!(vector.layers[0].len(), 1);
        assert_eq!(vector.layers[1].len(), 2);

        assert_eq!(
            vector.layers[0][0],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[1][0],
                vector.layers[1][1]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][0],
            hash::hash(&Node::Item(&0u32)).unwrap()
        );

        assert_eq!(
            vector.layers[1][1],
            hash::hash(&Node::Item(&1u32)).unwrap()
        );
    }

    #[test]
    fn three_items() {
        let vector = Vector::new(vec![0u32, 1u32, 2u32]).unwrap();

        assert_eq!(vector.layers.len(), 3);
        assert_eq!(vector.layers[0].len(), 1);
        assert_eq!(vector.layers[1].len(), 2);
        assert_eq!(vector.layers[2].len(), 3);

        assert_eq!(
            vector.layers[0][0],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[1][0],
                vector.layers[1][1]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][0],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[2][0],
                vector.layers[2][1]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][1],
            hash::hash(&Node::Internal::<u32>(&Children::Only(
                vector.layers[2][2]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][0],
            hash::hash(&Node::Item(&0u32)).unwrap()
        );

        assert_eq!(
            vector.layers[2][1],
            hash::hash(&Node::Item(&1u32)).unwrap()
        );

        assert_eq!(
            vector.layers[2][2],
            hash::hash(&Node::Item(&2u32)).unwrap()
        );
    }

    #[test]
    fn four_items() {
        let vector = Vector::new(vec![0u32, 1u32, 2u32, 3u32]).unwrap();

        assert_eq!(vector.layers.len(), 3);
        assert_eq!(vector.layers[0].len(), 1);
        assert_eq!(vector.layers[1].len(), 2);
        assert_eq!(vector.layers[2].len(), 4);

        assert_eq!(
            vector.layers[0][0],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[1][0],
                vector.layers[1][1]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][0],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[2][0],
                vector.layers[2][1]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][1],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[2][2],
                vector.layers[2][3]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][0],
            hash::hash(&Node::Item(&0u32)).unwrap()
        );

        assert_eq!(
            vector.layers[2][1],
            hash::hash(&Node::Item(&1u32)).unwrap()
        );

        assert_eq!(
            vector.layers[2][2],
            hash::hash(&Node::Item(&2u32)).unwrap()
        );

        assert_eq!(
            vector.layers[2][3],
            hash::hash(&Node::Item(&3u32)).unwrap()
        );
    }

    #[test]
    fn five_items() {
        let vector = Vector::new(vec![0u32, 1u32, 2u32, 3u32, 4u32]).unwrap();

        assert_eq!(vector.layers.len(), 4);
        assert_eq!(vector.layers[0].len(), 1);
        assert_eq!(vector.layers[1].len(), 2);
        assert_eq!(vector.layers[2].len(), 3);
        assert_eq!(vector.layers[3].len(), 5);

        assert_eq!(
            vector.layers[0][0],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[1][0],
                vector.layers[1][1]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][0],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[2][0],
                vector.layers[2][1]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][1],
            hash::hash(&Node::Internal::<u32>(&Children::Only(
                vector.layers[2][2]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][0],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[3][0],
                vector.layers[3][1]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][1],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[3][2],
                vector.layers[3][3]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][2],
            hash::hash(&Node::Internal::<u32>(&Children::Only(
                vector.layers[3][4]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[3][0],
            hash::hash(&Node::Item(&0u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][1],
            hash::hash(&Node::Item(&1u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][2],
            hash::hash(&Node::Item(&2u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][3],
            hash::hash(&Node::Item(&3u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][4],
            hash::hash(&Node::Item(&4u32)).unwrap()
        );
    }

    #[test]
    fn six_items() {
        let vector =
            Vector::new(vec![0u32, 1u32, 2u32, 3u32, 4u32, 5u32]).unwrap();

        assert_eq!(vector.layers.len(), 4);
        assert_eq!(vector.layers[0].len(), 1);
        assert_eq!(vector.layers[1].len(), 2);
        assert_eq!(vector.layers[2].len(), 3);
        assert_eq!(vector.layers[3].len(), 6);

        assert_eq!(
            vector.layers[0][0],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[1][0],
                vector.layers[1][1]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][0],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[2][0],
                vector.layers[2][1]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][1],
            hash::hash(&Node::Internal::<u32>(&Children::Only(
                vector.layers[2][2]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][0],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[3][0],
                vector.layers[3][1]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][1],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[3][2],
                vector.layers[3][3]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][2],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[3][4],
                vector.layers[3][5]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[3][0],
            hash::hash(&Node::Item(&0u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][1],
            hash::hash(&Node::Item(&1u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][2],
            hash::hash(&Node::Item(&2u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][3],
            hash::hash(&Node::Item(&3u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][4],
            hash::hash(&Node::Item(&4u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][5],
            hash::hash(&Node::Item(&5u32)).unwrap()
        );
    }

    #[test]
    fn seven_items() {
        let vector =
            Vector::new(vec![0u32, 1u32, 2u32, 3u32, 4u32, 5u32, 6u32])
                .unwrap();

        assert_eq!(vector.layers.len(), 4);
        assert_eq!(vector.layers[0].len(), 1);
        assert_eq!(vector.layers[1].len(), 2);
        assert_eq!(vector.layers[2].len(), 4);
        assert_eq!(vector.layers[3].len(), 7);

        assert_eq!(
            vector.layers[0][0],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[1][0],
                vector.layers[1][1]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][0],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[2][0],
                vector.layers[2][1]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][1],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[2][2],
                vector.layers[2][3]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][0],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[3][0],
                vector.layers[3][1]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][1],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[3][2],
                vector.layers[3][3]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][2],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[3][4],
                vector.layers[3][5]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][3],
            hash::hash(&Node::Internal::<u32>(&Children::Only(
                vector.layers[3][6]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[3][0],
            hash::hash(&Node::Item(&0u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][1],
            hash::hash(&Node::Item(&1u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][2],
            hash::hash(&Node::Item(&2u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][3],
            hash::hash(&Node::Item(&3u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][4],
            hash::hash(&Node::Item(&4u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][5],
            hash::hash(&Node::Item(&5u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][6],
            hash::hash(&Node::Item(&6u32)).unwrap()
        );
    }

    #[test]
    fn eight_items() {
        let vector =
            Vector::new(vec![0u32, 1u32, 2u32, 3u32, 4u32, 5u32, 6u32, 7u32])
                .unwrap();

        assert_eq!(vector.layers.len(), 4);
        assert_eq!(vector.layers[0].len(), 1);
        assert_eq!(vector.layers[1].len(), 2);
        assert_eq!(vector.layers[2].len(), 4);
        assert_eq!(vector.layers[3].len(), 8);

        assert_eq!(
            vector.layers[0][0],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[1][0],
                vector.layers[1][1]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][0],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[2][0],
                vector.layers[2][1]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][1],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[2][2],
                vector.layers[2][3]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][0],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[3][0],
                vector.layers[3][1]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][1],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[3][2],
                vector.layers[3][3]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][2],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[3][4],
                vector.layers[3][5]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][3],
            hash::hash(&Node::Internal::<u32>(&Children::Siblings(
                vector.layers[3][6],
                vector.layers[3][7]
            )))
            .unwrap()
        );

        assert_eq!(
            vector.layers[3][0],
            hash::hash(&Node::Item(&0u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][1],
            hash::hash(&Node::Item(&1u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][2],
            hash::hash(&Node::Item(&2u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][3],
            hash::hash(&Node::Item(&3u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][4],
            hash::hash(&Node::Item(&4u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][5],
            hash::hash(&Node::Item(&5u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][6],
            hash::hash(&Node::Item(&6u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][7],
            hash::hash(&Node::Item(&7u32)).unwrap()
        );
    }

    #[test]
    fn build_stress() {
        for len in 1..256 {
            let vector = Vector::new((0u32..len).collect()).unwrap();

            let mut log2 = 0;

            while (1 << log2) < len {
                log2 += 1;
            }

            assert_eq!(vector.layers.len(), log2 + 1);

            for l in 0..(vector.layers.len() - 1) {
                assert_eq!(
                    vector.layers[l].len(),
                    (vector.layers[l + 1].len() + 1) / 2
                );

                for i in 0..(vector.layers[l].len() - 1) {
                    assert_eq!(
                        vector.layers[l][i],
                        hash::hash(&Node::Internal::<u32>(&if 2 * i + 1
                            >= vector.layers[l + 1].len()
                        {
                            Children::Only(vector.layers[l + 1][2 * i])
                        } else {
                            Children::Siblings(
                                vector.layers[l + 1][2 * i],
                                vector.layers[l + 1][2 * i + 1],
                            )
                        }))
                        .unwrap()
                    )
                }
            }

            for i in 0..(vector.layers.last().unwrap().len()) {
                assert_eq!(
                    vector.layers.last().unwrap()[i],
                    hash::hash(&Node::Item(&(i as u32))).unwrap()
                );
            }
        }
    }

    #[test]
    fn proof_stress() {
        for len in 1..128 {
            let vector = Vector::new((0..len).collect()).unwrap();

            for item in 0..len {
                let proof = vector.prove(item);
                proof.verify(vector.root(), &item).unwrap();
            }
        }
    }
}
