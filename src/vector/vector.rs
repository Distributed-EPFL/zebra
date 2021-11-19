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
            .map(|element| hash::hash(&element).unwrap())
            .collect::<Vec<_>>();

        while layer.len() > 1 {
            layer = {
                let next = layer
                    .chunks(2)
                    .map(|chunk| hash::hash(&chunk).unwrap())
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
        let vector = Vector::new(vec![0]);

        assert_eq!(vector.layers.len(), 1);
        assert_eq!(vector.layers[0].len(), 1);

        assert_eq!(vector.layers[0][0], hash::hash(&[0]).unwrap());
    }

    #[test]
    fn two_items() {
        let vector = Vector::new(vec![0, 1]);

        assert_eq!(vector.layers.len(), 2);
        assert_eq!(vector.layers[0].len(), 1);
        assert_eq!(vector.layers[1].len(), 2);

        assert_eq!(
            vector.layers[0][0],
            hash::hash(&&[vector.layers[1][0], vector.layers[1][1]][..])
                .unwrap()
        );

        assert_eq!(vector.layers[1][0], hash::hash(&[0]).unwrap());
        assert_eq!(vector.layers[1][1], hash::hash(&[1]).unwrap());
    }

    #[test]
    fn three_items() {
        let vector = Vector::new(vec![0, 1, 2]);

        assert_eq!(vector.layers.len(), 3);
        assert_eq!(vector.layers[0].len(), 1);
        assert_eq!(vector.layers[1].len(), 2);
        assert_eq!(vector.layers[2].len(), 3);

        assert_eq!(
            vector.layers[0][0],
            hash::hash(&&[vector.layers[1][0], vector.layers[1][1]][..])
                .unwrap()
        );

        assert_eq!(
            vector.layers[1][0],
            hash::hash(&&[vector.layers[2][0], vector.layers[2][1]][..])
                .unwrap()
        );

        assert_eq!(
            vector.layers[1][1],
            hash::hash(&&[vector.layers[2][2]][..]).unwrap()
        );

        assert_eq!(vector.layers[2][0], hash::hash(&[0]).unwrap());
        assert_eq!(vector.layers[2][1], hash::hash(&[1]).unwrap());
        assert_eq!(vector.layers[2][2], hash::hash(&[2]).unwrap());
    }

    #[test]
    fn four_items() {
        let vector = Vector::new(vec![0, 1, 2, 3]);

        assert_eq!(vector.layers.len(), 3);
        assert_eq!(vector.layers[0].len(), 1);
        assert_eq!(vector.layers[1].len(), 2);
        assert_eq!(vector.layers[2].len(), 4);

        assert_eq!(
            vector.layers[0][0],
            hash::hash(&&[vector.layers[1][0], vector.layers[1][1]][..])
                .unwrap()
        );

        assert_eq!(
            vector.layers[1][0],
            hash::hash(&&[vector.layers[2][0], vector.layers[2][1]][..])
                .unwrap()
        );

        assert_eq!(
            vector.layers[1][1],
            hash::hash(&&[vector.layers[2][2], vector.layers[2][3]][..])
                .unwrap()
        );

        assert_eq!(vector.layers[2][0], hash::hash(&[0]).unwrap());
        assert_eq!(vector.layers[2][1], hash::hash(&[1]).unwrap());
        assert_eq!(vector.layers[2][2], hash::hash(&[2]).unwrap());
        assert_eq!(vector.layers[2][3], hash::hash(&[3]).unwrap());
    }

    #[test]
    fn five_items() {
        let vector = Vector::new(vec![0, 1, 2, 3, 4]);

        assert_eq!(vector.layers.len(), 4);
        assert_eq!(vector.layers[0].len(), 1);
        assert_eq!(vector.layers[1].len(), 2);
        assert_eq!(vector.layers[2].len(), 3);
        assert_eq!(vector.layers[3].len(), 5);

        assert_eq!(
            vector.layers[0][0],
            hash::hash(&&[vector.layers[1][0], vector.layers[1][1]][..])
                .unwrap()
        );

        assert_eq!(
            vector.layers[1][0],
            hash::hash(&&[vector.layers[2][0], vector.layers[2][1]][..])
                .unwrap()
        );

        assert_eq!(
            vector.layers[1][1],
            hash::hash(&&[vector.layers[2][2]][..]).unwrap()
        );

        assert_eq!(
            vector.layers[2][0],
            hash::hash(&&[vector.layers[3][0], vector.layers[3][1]][..])
                .unwrap()
        );

        assert_eq!(
            vector.layers[2][1],
            hash::hash(&&[vector.layers[3][2], vector.layers[3][3]][..])
                .unwrap()
        );

        assert_eq!(
            vector.layers[2][2],
            hash::hash(&&[vector.layers[3][4]][..]).unwrap()
        );

        assert_eq!(vector.layers[3][0], hash::hash(&[0]).unwrap());
        assert_eq!(vector.layers[3][1], hash::hash(&[1]).unwrap());
        assert_eq!(vector.layers[3][2], hash::hash(&[2]).unwrap());
        assert_eq!(vector.layers[3][3], hash::hash(&[3]).unwrap());
        assert_eq!(vector.layers[3][4], hash::hash(&[4]).unwrap());
    }

    #[test]
    fn six_items() {
        let vector = Vector::new(vec![0, 1, 2, 3, 4, 5]);

        assert_eq!(vector.layers.len(), 4);
        assert_eq!(vector.layers[0].len(), 1);
        assert_eq!(vector.layers[1].len(), 2);
        assert_eq!(vector.layers[2].len(), 3);
        assert_eq!(vector.layers[3].len(), 6);

        assert_eq!(
            vector.layers[0][0],
            hash::hash(&&[vector.layers[1][0], vector.layers[1][1]][..])
                .unwrap()
        );

        assert_eq!(
            vector.layers[1][0],
            hash::hash(&&[vector.layers[2][0], vector.layers[2][1]][..])
                .unwrap()
        );

        assert_eq!(
            vector.layers[1][1],
            hash::hash(&&[vector.layers[2][2]][..]).unwrap()
        );

        assert_eq!(
            vector.layers[2][0],
            hash::hash(&&[vector.layers[3][0], vector.layers[3][1]][..])
                .unwrap()
        );

        assert_eq!(
            vector.layers[2][1],
            hash::hash(&&[vector.layers[3][2], vector.layers[3][3]][..])
                .unwrap()
        );

        assert_eq!(
            vector.layers[2][2],
            hash::hash(&&[vector.layers[3][4], vector.layers[3][5]][..])
                .unwrap()
        );

        assert_eq!(vector.layers[3][0], hash::hash(&[0]).unwrap());
        assert_eq!(vector.layers[3][1], hash::hash(&[1]).unwrap());
        assert_eq!(vector.layers[3][2], hash::hash(&[2]).unwrap());
        assert_eq!(vector.layers[3][3], hash::hash(&[3]).unwrap());
        assert_eq!(vector.layers[3][4], hash::hash(&[4]).unwrap());
        assert_eq!(vector.layers[3][5], hash::hash(&[5]).unwrap());
    }

    #[test]
    fn seven_items() {
        let vector = Vector::new(vec![0, 1, 2, 3, 4, 5, 6]);

        assert_eq!(vector.layers.len(), 4);
        assert_eq!(vector.layers[0].len(), 1);
        assert_eq!(vector.layers[1].len(), 2);
        assert_eq!(vector.layers[2].len(), 4);
        assert_eq!(vector.layers[3].len(), 7);

        assert_eq!(
            vector.layers[0][0],
            hash::hash(&&[vector.layers[1][0], vector.layers[1][1]][..])
                .unwrap()
        );

        assert_eq!(
            vector.layers[1][0],
            hash::hash(&&[vector.layers[2][0], vector.layers[2][1]][..])
                .unwrap()
        );

        assert_eq!(
            vector.layers[1][1],
            hash::hash(&&[vector.layers[2][2], vector.layers[2][3]][..])
                .unwrap()
        );

        assert_eq!(
            vector.layers[2][0],
            hash::hash(&&[vector.layers[3][0], vector.layers[3][1]][..])
                .unwrap()
        );

        assert_eq!(
            vector.layers[2][1],
            hash::hash(&&[vector.layers[3][2], vector.layers[3][3]][..])
                .unwrap()
        );

        assert_eq!(
            vector.layers[2][2],
            hash::hash(&&[vector.layers[3][4], vector.layers[3][5]][..])
                .unwrap()
        );

        assert_eq!(
            vector.layers[2][3],
            hash::hash(&&[vector.layers[3][6]][..]).unwrap()
        );

        assert_eq!(vector.layers[3][0], hash::hash(&[0]).unwrap());
        assert_eq!(vector.layers[3][1], hash::hash(&[1]).unwrap());
        assert_eq!(vector.layers[3][2], hash::hash(&[2]).unwrap());
        assert_eq!(vector.layers[3][3], hash::hash(&[3]).unwrap());
        assert_eq!(vector.layers[3][4], hash::hash(&[4]).unwrap());
        assert_eq!(vector.layers[3][5], hash::hash(&[5]).unwrap());
        assert_eq!(vector.layers[3][6], hash::hash(&[6]).unwrap());
    }

    #[test]
    fn eight_items() {
        let vector = Vector::new(vec![0, 1, 2, 3, 4, 5, 6, 7]);

        assert_eq!(vector.layers.len(), 4);
        assert_eq!(vector.layers[0].len(), 1);
        assert_eq!(vector.layers[1].len(), 2);
        assert_eq!(vector.layers[2].len(), 4);
        assert_eq!(vector.layers[3].len(), 8);

        assert_eq!(
            vector.layers[0][0],
            hash::hash(&&[vector.layers[1][0], vector.layers[1][1]][..])
                .unwrap()
        );

        assert_eq!(
            vector.layers[1][0],
            hash::hash(&&[vector.layers[2][0], vector.layers[2][1]][..])
                .unwrap()
        );

        assert_eq!(
            vector.layers[1][1],
            hash::hash(&&[vector.layers[2][2], vector.layers[2][3]][..])
                .unwrap()
        );

        assert_eq!(
            vector.layers[2][0],
            hash::hash(&&[vector.layers[3][0], vector.layers[3][1]][..])
                .unwrap()
        );

        assert_eq!(
            vector.layers[2][1],
            hash::hash(&&[vector.layers[3][2], vector.layers[3][3]][..])
                .unwrap()
        );

        assert_eq!(
            vector.layers[2][2],
            hash::hash(&&[vector.layers[3][4], vector.layers[3][5]][..])
                .unwrap()
        );

        assert_eq!(
            vector.layers[2][3],
            hash::hash(&&[vector.layers[3][6], vector.layers[3][7]][..])
                .unwrap()
        );

        assert_eq!(vector.layers[3][0], hash::hash(&[0]).unwrap());
        assert_eq!(vector.layers[3][1], hash::hash(&[1]).unwrap());
        assert_eq!(vector.layers[3][2], hash::hash(&[2]).unwrap());
        assert_eq!(vector.layers[3][3], hash::hash(&[3]).unwrap());
        assert_eq!(vector.layers[3][4], hash::hash(&[4]).unwrap());
        assert_eq!(vector.layers[3][5], hash::hash(&[5]).unwrap());
        assert_eq!(vector.layers[3][6], hash::hash(&[6]).unwrap());
        assert_eq!(vector.layers[3][7], hash::hash(&[7]).unwrap());
    }
}
