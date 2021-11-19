use crate::vector::Children;

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
    pub fn new(items: Vec<Item>) -> Self {
        if items.is_empty() {
            panic!("called `Vector::new` with an empty `items`");
        }

        let mut layers = Vec::new();

        let mut layer = items
            .iter()
            .map(|element| hash::hash(&Children::Item(element)).unwrap())
            .collect::<Vec<_>>();

        while layer.len() > 1 {
            layer = {
                let next = layer
                    .chunks(2)
                    .map(Into::<Children<Item>>::into)
                    .map(|children| hash::hash(&children).unwrap())
                    .collect::<Vec<_>>();

                layers.push(layer);
                next
            };
        }

        layers.push(layer);
        layers.reverse();

        Vector { layers, items }
    }

    pub fn root(&self) -> Hash {
        self.layers[0][0]
    }

    pub fn items(&self) -> &[Item] {
        &self.items
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic]
    fn empty() {
        Vector::<()>::new(vec![]);
    }

    #[test]
    fn one_item() {
        let vector = Vector::new(vec![0u32]);

        assert_eq!(vector.layers.len(), 1);
        assert_eq!(vector.layers[0].len(), 1);

        assert_eq!(
            vector.layers[0][0],
            hash::hash(&Children::Item(0u32)).unwrap()
        );
    }

    #[test]
    fn two_items() {
        let vector = Vector::new(vec![0u32, 1u32]);

        assert_eq!(vector.layers.len(), 2);
        assert_eq!(vector.layers[0].len(), 1);
        assert_eq!(vector.layers[1].len(), 2);

        assert_eq!(
            vector.layers[0][0],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[1][0],
                vector.layers[1][1]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][0],
            hash::hash(&Children::Item(0u32)).unwrap()
        );

        assert_eq!(
            vector.layers[1][1],
            hash::hash(&Children::Item(1u32)).unwrap()
        );
    }

    #[test]
    fn three_items() {
        let vector = Vector::new(vec![0u32, 1u32, 2u32]);

        assert_eq!(vector.layers.len(), 3);
        assert_eq!(vector.layers[0].len(), 1);
        assert_eq!(vector.layers[1].len(), 2);
        assert_eq!(vector.layers[2].len(), 3);

        assert_eq!(
            vector.layers[0][0],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[1][0],
                vector.layers[1][1]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][0],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[2][0],
                vector.layers[2][1]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][1],
            hash::hash(&Children::Only::<u32>(vector.layers[2][2])).unwrap()
        );

        assert_eq!(
            vector.layers[2][0],
            hash::hash(&Children::Item(0u32)).unwrap()
        );

        assert_eq!(
            vector.layers[2][1],
            hash::hash(&Children::Item(1u32)).unwrap()
        );

        assert_eq!(
            vector.layers[2][2],
            hash::hash(&Children::Item(2u32)).unwrap()
        );
    }

    #[test]
    fn four_items() {
        let vector = Vector::new(vec![0u32, 1u32, 2u32, 3u32]);

        assert_eq!(vector.layers.len(), 3);
        assert_eq!(vector.layers[0].len(), 1);
        assert_eq!(vector.layers[1].len(), 2);
        assert_eq!(vector.layers[2].len(), 4);

        assert_eq!(
            vector.layers[0][0],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[1][0],
                vector.layers[1][1]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][0],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[2][0],
                vector.layers[2][1]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][1],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[2][2],
                vector.layers[2][3]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][0],
            hash::hash(&Children::Item(0u32)).unwrap()
        );

        assert_eq!(
            vector.layers[2][1],
            hash::hash(&Children::Item(1u32)).unwrap()
        );

        assert_eq!(
            vector.layers[2][2],
            hash::hash(&Children::Item(2u32)).unwrap()
        );

        assert_eq!(
            vector.layers[2][3],
            hash::hash(&Children::Item(3u32)).unwrap()
        );
    }

    #[test]
    fn five_items() {
        let vector = Vector::new(vec![0u32, 1u32, 2u32, 3u32, 4u32]);

        assert_eq!(vector.layers.len(), 4);
        assert_eq!(vector.layers[0].len(), 1);
        assert_eq!(vector.layers[1].len(), 2);
        assert_eq!(vector.layers[2].len(), 3);
        assert_eq!(vector.layers[3].len(), 5);

        assert_eq!(
            vector.layers[0][0],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[1][0],
                vector.layers[1][1]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][0],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[2][0],
                vector.layers[2][1]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][1],
            hash::hash(&Children::Only::<u32>(vector.layers[2][2])).unwrap()
        );

        assert_eq!(
            vector.layers[2][0],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[3][0],
                vector.layers[3][1]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][1],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[3][2],
                vector.layers[3][3]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][2],
            hash::hash(&Children::Only::<u32>(vector.layers[3][4])).unwrap()
        );

        assert_eq!(
            vector.layers[3][0],
            hash::hash(&Children::Item(0u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][1],
            hash::hash(&Children::Item(1u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][2],
            hash::hash(&Children::Item(2u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][3],
            hash::hash(&Children::Item(3u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][4],
            hash::hash(&Children::Item(4u32)).unwrap()
        );
    }

    #[test]
    fn six_items() {
        let vector = Vector::new(vec![0u32, 1u32, 2u32, 3u32, 4u32, 5u32]);

        assert_eq!(vector.layers.len(), 4);
        assert_eq!(vector.layers[0].len(), 1);
        assert_eq!(vector.layers[1].len(), 2);
        assert_eq!(vector.layers[2].len(), 3);
        assert_eq!(vector.layers[3].len(), 6);

        assert_eq!(
            vector.layers[0][0],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[1][0],
                vector.layers[1][1]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][0],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[2][0],
                vector.layers[2][1]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][1],
            hash::hash(&Children::Only::<u32>(vector.layers[2][2])).unwrap()
        );

        assert_eq!(
            vector.layers[2][0],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[3][0],
                vector.layers[3][1]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][1],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[3][2],
                vector.layers[3][3]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][2],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[3][4],
                vector.layers[3][5]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[3][0],
            hash::hash(&Children::Item(0u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][1],
            hash::hash(&Children::Item(1u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][2],
            hash::hash(&Children::Item(2u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][3],
            hash::hash(&Children::Item(3u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][4],
            hash::hash(&Children::Item(4u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][5],
            hash::hash(&Children::Item(5u32)).unwrap()
        );
    }

    #[test]
    fn seven_items() {
        let vector =
            Vector::new(vec![0u32, 1u32, 2u32, 3u32, 4u32, 5u32, 6u32]);

        assert_eq!(vector.layers.len(), 4);
        assert_eq!(vector.layers[0].len(), 1);
        assert_eq!(vector.layers[1].len(), 2);
        assert_eq!(vector.layers[2].len(), 4);
        assert_eq!(vector.layers[3].len(), 7);

        assert_eq!(
            vector.layers[0][0],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[1][0],
                vector.layers[1][1]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][0],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[2][0],
                vector.layers[2][1]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][1],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[2][2],
                vector.layers[2][3]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][0],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[3][0],
                vector.layers[3][1]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][1],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[3][2],
                vector.layers[3][3]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][2],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[3][4],
                vector.layers[3][5]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][3],
            hash::hash(&Children::Only::<u32>(vector.layers[3][6])).unwrap()
        );

        assert_eq!(
            vector.layers[3][0],
            hash::hash(&Children::Item(0u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][1],
            hash::hash(&Children::Item(1u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][2],
            hash::hash(&Children::Item(2u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][3],
            hash::hash(&Children::Item(3u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][4],
            hash::hash(&Children::Item(4u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][5],
            hash::hash(&Children::Item(5u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][6],
            hash::hash(&Children::Item(6u32)).unwrap()
        );
    }

    #[test]
    fn eight_items() {
        let vector =
            Vector::new(vec![0u32, 1u32, 2u32, 3u32, 4u32, 5u32, 6u32, 7u32]);

        assert_eq!(vector.layers.len(), 4);
        assert_eq!(vector.layers[0].len(), 1);
        assert_eq!(vector.layers[1].len(), 2);
        assert_eq!(vector.layers[2].len(), 4);
        assert_eq!(vector.layers[3].len(), 8);

        assert_eq!(
            vector.layers[0][0],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[1][0],
                vector.layers[1][1]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][0],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[2][0],
                vector.layers[2][1]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[1][1],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[2][2],
                vector.layers[2][3]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][0],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[3][0],
                vector.layers[3][1]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][1],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[3][2],
                vector.layers[3][3]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][2],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[3][4],
                vector.layers[3][5]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[2][3],
            hash::hash(&Children::Siblings::<u32>(
                vector.layers[3][6],
                vector.layers[3][7]
            ))
            .unwrap()
        );

        assert_eq!(
            vector.layers[3][0],
            hash::hash(&Children::Item(0u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][1],
            hash::hash(&Children::Item(1u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][2],
            hash::hash(&Children::Item(2u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][3],
            hash::hash(&Children::Item(3u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][4],
            hash::hash(&Children::Item(4u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][5],
            hash::hash(&Children::Item(5u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][6],
            hash::hash(&Children::Item(6u32)).unwrap()
        );

        assert_eq!(
            vector.layers[3][7],
            hash::hash(&Children::Item(7u32)).unwrap()
        );
    }

    #[test]
    fn stress() {
        for len in 1..256 {
            let vector = Vector::new((0u32..len).collect());

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
                        hash::hash(
                            &if 2 * i + 1 >= vector.layers[l + 1].len() {
                                Children::Only::<u32>(
                                    vector.layers[l + 1][2 * i],
                                )
                            } else {
                                Children::Siblings::<u32>(
                                    vector.layers[l + 1][2 * i],
                                    vector.layers[l + 1][2 * i + 1],
                                )
                            }
                        )
                        .unwrap()
                    )
                }
            }

            for i in 0..(vector.layers.last().unwrap().len()) {
                assert_eq!(
                    vector.layers.last().unwrap()[i],
                    hash::hash(&Children::Item(i as u32)).unwrap()
                );
            }
        }
    }
}
