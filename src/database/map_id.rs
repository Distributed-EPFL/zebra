use serde::Serialize;

use super::bytes::Bytes;
use super::store::DEPTH;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize)]
pub(crate) struct MapId(u8);

impl MapId {
    pub fn internal(id: usize) -> Self {
        if DEPTH > 0 {
            MapId((id as u8) << (8 - DEPTH))
        } else {
            MapId(0)
        }
    }

    pub fn leaf(key_hash: &Bytes) -> Self {
        MapId(key_hash.0[0])
    }

    pub fn id(&self) -> usize {
        if DEPTH > 0 {
            (self.0 >> (8 - DEPTH)) as usize
        } else {
            0
        }
    }
}
