use serde::{Deserialize, Serialize};

use talk::crypto::primitives::hash::Hash;

#[derive(Serialize, Deserialize)]
pub(in crate::vector) enum Children<Item> {
    Only(Hash),
    Siblings(Hash, Hash),
    Item(Item),
}

impl<Item> From<&[Hash]> for Children<Item>
where
    Item: Serialize,
{
    fn from(slice: &[Hash]) -> Children<Item> {
        match slice.len() {
            1 => Children::Only(slice[0]),
            2 => Children::Siblings(slice[0], slice[1]),
            _ => {
                panic!("called `Children::from` with an unexpected size slice")
            }
        }
    }
}
