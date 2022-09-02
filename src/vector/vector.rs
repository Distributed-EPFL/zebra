use crate::{
    common::tree::Direction,
    vector::{errors::VectorError, Node, Proof},
};

use doomstack::{here, ResultExt, Top};

use serde::{de::Error as DeError, Deserialize, Deserializer, Serialize, Serializer};

use talk::crypto::primitives::{hash, hash::Hash};

#[derive(Debug, Clone)]
pub struct Vector<Item: Serialize, const PACKING: usize = 1> {
    layers: Vec<Vec<Hash>>,
    items: Vec<Item>,
}

impl<Item, const PACKING: usize> Vector<Item, PACKING>
where
    Item: Serialize,
{
    pub fn new(items: Vec<Item>) -> Result<Self, Top<VectorError>> {
        assert!(PACKING > 0);

        Self::with_packing(items, PACKING)
    }

    fn with_packing(items: Vec<Item>, packing: usize) -> Result<Self, Top<VectorError>> {
        assert!(packing > 0);

        if items.is_empty() {
            panic!("called `PackedVector::new` with an empty `items`");
        }

        let mut layers = Vec::new();

        let mut nodes = items
            .iter()
            .collect::<Vec<&Item>>()
            .chunks(packing)
            .map(|chunk| {
                if packing == 1 {
                    hash::hash(&Node::<&Item>::Item(chunk.get(0).unwrap()))
                        .pot(VectorError::HashError, here!())
                } else {
                    hash::hash(&Node::<&[&Item]>::Item(chunk)).pot(VectorError::HashError, here!())
                }
            })
            .collect::<Result<Vec<Hash>, Top<VectorError>>>()?;

        let pow = std::cmp::max(
            1,
            nodes
                .len()
                .checked_next_power_of_two()
                .unwrap_or(usize::MAX)
                / 2,
        );

        let last_layer = std::cmp::max(1, 2 * (nodes.len() - pow));

        let mut layer = if nodes.len() - last_layer > 0 {
            let last = nodes.split_off(last_layer);

            let mut penultimate_layer: Vec<Hash> = nodes
                .chunks(2)
                .map(|pair| hash::hash(&Node::<Item>::Internal(pair[0], pair[1])).unwrap())
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
                    .map(|pair| hash::hash(&Node::<Item>::Internal(pair[0], pair[1])).unwrap())
                    .collect::<Vec<_>>();

                layers.push(layer);
                next
            };
        }

        layers.push(layer);

        Ok(Vector { layers, items })
    }

    pub fn set(&mut self, index: usize, item: Item) -> Result<(), Top<VectorError>> {
        assert!(index < self.items.len());

        self.items[index] = item;

        let mut node_hash = if PACKING == 1 {
            hash::hash(&Node::<&Item>::Item(self.items.get(index).unwrap()))
                .pot(VectorError::HashError, here!())?
        } else {
            let chunk = ((index - index % PACKING)
                ..std::cmp::min(index - index % PACKING + PACKING, self.items.len()))
                .map(|index| self.items.get(index).unwrap())
                .collect::<Vec<_>>();

            hash::hash(&Node::<&[&Item]>::Item(chunk.as_slice()))
                .pot(VectorError::HashError, here!())?
        };

        let node_index = index / PACKING;

        let first_layer_len = self.layers[0].len();
        let mut layers = self.layers.iter_mut();

        let mut layer_index = if node_index < first_layer_len {
            node_index
        } else {
            layers.next();
            node_index - first_layer_len / 2
        };

        for layer in layers {
            layer[layer_index] = node_hash;

            if layer.len() > 1 {
                node_hash = if layer_index % 2 == 0 {
                    hash::hash(&Node::<Item>::Internal(node_hash, layer[layer_index + 1])).unwrap()
                } else {
                    hash::hash(&Node::<Item>::Internal(layer[layer_index - 1], node_hash)).unwrap()
                };
            }

            layer_index = layer_index / 2;
        }

        Ok(())
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn root(&self) -> Hash {
        self.layers.last().unwrap()[0]
    }

    pub fn items(&self) -> &[Item] {
        &self.items
    }

    pub fn prove(&self, index: usize) -> Proof {
        assert!(index < self.items.len());

        let mut path: Vec<Direction> = Vec::new();
        let mut proof: Vec<Hash> = Vec::new();

        let mut layers = self.layers.iter();

        let index_shift = index / PACKING;

        let mut layer_index = if index_shift < self.layers[0].len() {
            index_shift
        } else {
            layers.next();
            index_shift - self.layers[0].len() / 2
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

        let siblings = if PACKING == 1 {
            None
        } else {
            let mut siblings = vec![];
            for i in (index - index % PACKING)
                ..std::cmp::min(index - index % PACKING + PACKING, self.items.len())
            {
                if i != index {
                    siblings.push(&self.items()[i])
                }
            }
            Some((siblings, index % PACKING))
        };

        Proof::new(path, proof, siblings)
    }
}

impl<Item, const PACKING: usize> Serialize for Vector<Item, PACKING>
where
    Item: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.items.serialize(serializer)
    }
}

impl<'de, Item, const PACKING: usize> Deserialize<'de> for Vector<Item, PACKING>
where
    Item: Serialize + Deserialize<'de> + Clone,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let items = Vec::<Item>::deserialize(deserializer)?;
        Ok(Vector::new(items).map_err(|err| DeError::custom(err))?)
    }
}

impl<Item, const PACKING: usize> From<Vector<Item, PACKING>> for Vec<Item>
where
    Item: Serialize,
{
    fn from(vector: Vector<Item, PACKING>) -> Self {
        vector.items
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
        let vector = Vector::<_>::new(vec![0u32]).unwrap();

        assert_eq!(vector.layers.len(), 1);
        assert_eq!(vector.layers[0].len(), 1);

        assert_eq!(
            vector.layers.last().unwrap()[0],
            hash::hash(&Node::Item(0u32)).unwrap()
        );
    }

    #[test]
    fn one_item_2packed() {
        let vector = Vector::<_, 2>::new(vec![0u32]).unwrap();

        assert_eq!(vector.layers.len(), 1);
        assert_eq!(vector.layers[0].len(), 1);

        assert_eq!(
            vector.layers.last().unwrap()[0],
            hash::hash(&Node::<&[u32]>::Item(&[0u32])).unwrap()
        );
    }

    #[test]
    fn one_item_3packed() {
        let vector = Vector::<_, 3>::new(vec![0u32]).unwrap();

        assert_eq!(vector.layers.len(), 1);
        assert_eq!(vector.layers[0].len(), 1);

        assert_eq!(
            vector.layers.last().unwrap()[0],
            hash::hash(&Node::<&[u32]>::Item(&[0u32])).unwrap()
        );
    }

    #[test]
    fn two_items() {
        let vector = Vector::<_>::new(vec![0u32, 1u32]).unwrap();

        assert_eq!(vector.layers.len(), 2);
        assert_eq!(vector.layers[0].len(), 2);
        assert_eq!(vector.layers[1].len(), 1);

        assert_eq!(
            vector.layers.last().unwrap()[0],
            hash::hash(&Node::<u32>::Internal(
                hash::hash(&Node::Item(0u32)).unwrap(),
                hash::hash(&Node::Item(1u32)).unwrap()
            ))
            .unwrap()
        );

        assert_eq!(vector.layers[0][0], hash::hash(&Node::Item(0u32)).unwrap(),);

        assert_eq!(vector.layers[0][1], hash::hash(&Node::Item(1u32)).unwrap(),);
    }

    #[test]
    fn two_items_2packed() {
        let vector = Vector::<_, 2>::new(vec![0u32, 1u32]).unwrap();

        assert_eq!(vector.layers.len(), 1);

        assert_eq!(
            vector.layers.last().unwrap()[0],
            hash::hash(&Node::<&[u32]>::Item(&[0u32, 1u32])).unwrap()
        );
    }

    #[test]
    fn two_items_3packed() {
        let vector = Vector::<_, 2>::new(vec![0u32, 1u32]).unwrap();

        assert_eq!(vector.layers.len(), 1);

        assert_eq!(
            vector.layers.last().unwrap()[0],
            hash::hash(&Node::<&[u32]>::Item(&[0u32, 1u32])).unwrap()
        );
    }

    #[test]
    fn three_items() {
        let vector = Vector::<_>::new(vec![0u32, 1u32, 2u32]).unwrap();

        assert_eq!(vector.layers.len(), 3);
        assert_eq!(vector.layers[2].len(), 1);
        assert_eq!(vector.layers[1].len(), 2);
        assert_eq!(vector.layers[0].len(), 2);

        assert_eq!(
            vector.layers[2][0],
            hash::hash(&Node::<u32>::Internal(
                hash::hash(&Node::<u32>::Internal(
                    hash::hash(&Node::Item(0u32)).unwrap(),
                    hash::hash(&Node::Item(1u32)).unwrap()
                ))
                .unwrap(),
                hash::hash(&Node::Item(2u32)).unwrap()
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][0],
            hash::hash(&Node::<u32>::Internal(
                hash::hash(&Node::Item(0u32)).unwrap(),
                hash::hash(&Node::Item(1u32)).unwrap()
            ))
            .unwrap(),
        );

        assert_eq!(vector.layers[1][1], hash::hash(&Node::Item(2u32)).unwrap(),);

        assert_eq!(vector.layers[0][0], hash::hash(&Node::Item(0u32)).unwrap(),);

        assert_eq!(vector.layers[0][1], hash::hash(&Node::Item(1u32)).unwrap(),);
    }

    #[test]
    fn three_items_2packed() {
        let vector = Vector::<_, 2>::new(vec![0u32, 1u32, 2u32]).unwrap();

        assert_eq!(vector.layers.len(), 2);
        assert_eq!(vector.layers[1].len(), 1);
        assert_eq!(vector.layers[0].len(), 2);

        assert_eq!(
            vector.layers[1][0],
            hash::hash(&Node::<&[u32]>::Internal(
                hash::hash(&Node::<&[u32]>::Item(&[0u32, 1u32])).unwrap(),
                hash::hash(&Node::<&[u32]>::Item(&[2u32])).unwrap()
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[0][1],
            hash::hash(&Node::<&[u32]>::Item(&[2u32])).unwrap()
        );

        assert_eq!(
            vector.layers[0][0],
            hash::hash(&Node::<&[u32]>::Item(&[0u32, 1u32])).unwrap()
        );
    }

    #[test]
    fn three_items_3packed() {
        let vector = Vector::<_, 3>::new(vec![0u32, 1u32, 2u32]).unwrap();

        assert_eq!(vector.layers.len(), 1);
        assert_eq!(vector.layers[0].len(), 1);

        assert_eq!(
            vector.layers[0][0],
            hash::hash(&Node::<&[u32]>::Item(&[0u32, 1u32, 2u32])).unwrap(),
        );
    }

    #[test]
    fn proof_stress() {
        for len in 1..128 {
            let vector = Vector::<_>::new((0..len).collect()).unwrap();

            for item in 0..len {
                let proof = vector.prove(item);
                proof.verify(vector.root(), &item).unwrap();
            }
        }
    }

    #[test]
    fn proof_stress_2packed() {
        for len in 1..128 {
            let vector = Vector::<_, 2>::new((0..len).collect()).unwrap();

            for item in 0..len {
                let proof = vector.prove(item);
                proof.verify(vector.root(), &item).unwrap();
            }
        }
    }

    #[test]
    fn proof_stress_3packed() {
        for len in 1..128 {
            let vector = Vector::<_, 3>::new((0..len).collect()).unwrap();

            for item in 0..len {
                let proof = vector.prove(item);
                proof.verify(vector.root(), &item).unwrap();
            }
        }
    }

    #[test]
    fn proof_stress_4packed() {
        for len in 1..128 {
            let vector = Vector::<_, 4>::new((0..len).collect()).unwrap();

            for item in 0..len {
                let proof = vector.prove(item);
                proof.verify(vector.root(), &item).unwrap();
            }
        }
    }

    #[test]
    fn set_stress() {
        for len in 1..128 {
            let control = Vector::<_, 1>::new((0..len).collect()).unwrap();
            let mut vector = Vector::<_, 1>::new(std::iter::repeat(0).take(len).collect()).unwrap();

            for index in 0..len {
                vector.set(index, index).unwrap();
            }

            assert_eq!(vector.root(), control.root());
        }
    }

    #[test]
    fn serde() {
        let original = Vector::<_>::new((0..128).collect()).unwrap();
        let serialized = bincode::serialize(&original).unwrap();
        let deserialized = bincode::deserialize::<Vector<u32>>(&serialized).unwrap();

        assert_eq!(original.items(), deserialized.items());
        assert_eq!(original.root(), deserialized.root());
    }

    #[test]
    fn serde_2packed() {
        let original = Vector::<_, 2>::new((0..128).collect()).unwrap();
        let serialized = bincode::serialize(&original).unwrap();
        let deserialized = bincode::deserialize::<Vector<u32, 2>>(&serialized).unwrap();

        assert_eq!(original.items(), deserialized.items());
        assert_eq!(original.root(), deserialized.root());
    }

    #[test]
    fn serde_3packed() {
        let original = Vector::<_, 3>::new((0..128).collect()).unwrap();
        let serialized = bincode::serialize(&original).unwrap();
        let deserialized = bincode::deserialize::<Vector<u32, 3>>(&serialized).unwrap();

        assert_eq!(original.items(), deserialized.items());
        assert_eq!(original.root(), deserialized.root());
    }
}
