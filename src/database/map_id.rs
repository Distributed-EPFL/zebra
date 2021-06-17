use byteorder::{BigEndian, ByteOrder};

use serde::Serialize;

use super::bytes::Bytes;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize)]
pub(crate) struct MapId(u16);

impl MapId {
    pub fn internal(depth: u8, map: usize) -> Self {
        if depth > 0 {
            MapId((map as u16) << (16 - depth))
        } else {
            MapId(0)
        }
    }

    pub fn leaf(key_hash: &Bytes) -> Self {
        MapId(BigEndian::read_u16(&key_hash.0))
    }

    pub fn map(&self, depth: u8) -> usize {
        if depth > 0 {
            (self.0 >> (16 - depth)) as usize
        } else {
            0
        }
    }
}
