use serde::Serialize;
use talk::crypto::primitives::hash::Hash;

#[derive(Serialize)]
pub(in crate::vector) enum Node<I: Serialize> {
    Internal(Hash, Hash),
    Item(I),
}
