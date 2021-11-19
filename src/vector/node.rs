use crate::vector::Children;

use serde::Serialize;

#[derive(Serialize)]
pub(in crate::vector) enum Node<'n, Item> {
    Internal(&'n Children),
    Item(&'n Item),
}
