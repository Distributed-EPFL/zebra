use crate::common::tree::Direction;
use crate::vector::{errors::VectorError, Node, Proof};

use doomstack::{here, ResultExt, Top};

use serde::Serialize;

use talk::crypto::primitives::hash;
use talk::crypto::primitives::hash::Hash;

#[derive(Debug)]
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

        let mut nodes = items
            .iter()
            .map(|element| {
                let hash = hash::hash(&element)
                    .pot(VectorError::HashError, here!())?;
                hash::hash(&Node::Item(hash))
                    .pot(VectorError::HashError, here!())
            })
            .collect::<Result<Vec<Hash>, Top<VectorError>>>()?;

        let pow = std::cmp::max(
            1,
            items
                .len()
                .checked_next_power_of_two()
                .unwrap_or(usize::MAX)
                / 2,
        );
        
        let last_layer = std::cmp::max(1, 2 * (items.len() - pow));

        let mut layer = if items.len() - last_layer > 0 {
            let last = nodes.split_off(last_layer);

            let mut penultimate_layer: Vec<Hash> = nodes
                .chunks(2)
                .map(|pair| {
                    hash::hash(&Node::Internal(pair[0], pair[1])).unwrap()
                })
                .collect();

            layers.push(nodes);

            penultimate_layer.extend(last);

            penultimate_layer
        } else {
            nodes
        };

        while layer.len() > 1 {
            layer = {
                let next = layer
                    .chunks(2)
                    .map(|pair| {
                        hash::hash(&Node::Internal(pair[0], pair[1])).unwrap()
                    })
                    .collect::<Vec<_>>();

                layers.push(layer);
                next
            };
        }

        layers.push(layer);

        Ok(Vector { layers, items })
    }

    pub fn root(&self) -> Hash {
        self.layers.last().unwrap()[0]
    }

    pub fn items(&self) -> &[Item] {
        &self.items
    }

    pub fn prove(&self, index: usize) -> Proof {
        let mut path: Vec<Direction> = Vec::new();
        let mut proof: Vec<Hash> = Vec::new();

        let mut layers = self.layers.iter();

        let mut layer_index = if index < self.layers[0].len() {
            index
        } else {
            layers.next();
            index - self.layers[0].len() / 2
        };

        for layer in layers {
            if layer.len() > 1 {
                let (direction, sibling) = if layer_index % 2 == 0 {
                    (Direction::Left, layer[layer_index + 1])
                } else {
                    (Direction::Right, layer[layer_index - 1])
                };

                path.push(direction);
                proof.push(sibling);
            }

            layer_index = layer_index / 2;
        }

        Proof::new(path, proof)
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
            vector.layers.last().unwrap()[0],
            hash::hash(&Node::Item(hash::hash(&0u32).unwrap())).unwrap()
        );
    }

    #[test]
    fn two_items() {
        let vector = Vector::new(vec![0u32, 1u32]).unwrap();

        assert_eq!(vector.layers.len(), 2);
        assert_eq!(vector.layers[0].len(), 2);
        assert_eq!(vector.layers[1].len(), 1);

        assert_eq!(
            vector.layers.last().unwrap()[0],
            hash::hash(&Node::Internal(
                hash::hash(&Node::Item(hash::hash(&0u32).unwrap())).unwrap(),
                hash::hash(&Node::Item(hash::hash(&1u32).unwrap())).unwrap()
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[0][0],
            hash::hash(&Node::Item(hash::hash(&0u32).unwrap())).unwrap(),
        );

        assert_eq!(
            vector.layers[0][1],
            hash::hash(&Node::Item(hash::hash(&1u32).unwrap())).unwrap(),
        );
    }

    #[test]
    fn three_items() {
        let vector = Vector::new(vec![0u32, 1u32, 2u32]).unwrap();

        assert_eq!(vector.layers.len(), 3);
        assert_eq!(vector.layers[2].len(), 1);
        assert_eq!(vector.layers[1].len(), 2);
        assert_eq!(vector.layers[0].len(), 2);

        assert_eq!(
            vector.layers[2][0],
            hash::hash(&Node::Internal(
                hash::hash(&Node::Internal(
                    hash::hash(&Node::Item(hash::hash(&0u32).unwrap()))
                        .unwrap(),
                    hash::hash(&Node::Item(hash::hash(&1u32).unwrap()))
                        .unwrap()
                ))
                .unwrap(),
                hash::hash(&Node::Item(hash::hash(&2u32).unwrap())).unwrap()
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][0],
            hash::hash(&Node::Internal(
                hash::hash(&Node::Item(hash::hash(&0u32).unwrap())).unwrap(),
                hash::hash(&Node::Item(hash::hash(&1u32).unwrap())).unwrap()
            ))
            .unwrap(),
        );

        assert_eq!(
            vector.layers[1][1],
            hash::hash(&Node::Item(hash::hash(&2u32).unwrap())).unwrap(),
        );

        assert_eq!(
            vector.layers[0][0],
            hash::hash(&Node::Item(hash::hash(&0u32).unwrap())).unwrap(),
        );

        assert_eq!(
            vector.layers[0][1],
            hash::hash(&Node::Item(hash::hash(&1u32).unwrap())).unwrap(),
        );
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
