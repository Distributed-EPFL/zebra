use serde::Serialize;
use talk::crypto::primitives::hash::Hash;

#[derive(Serialize)]
pub(in crate::vector) enum Node {
    Internal(Hash, Hash),
    Item(Hash),
}
