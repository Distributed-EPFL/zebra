use byteorder::{BigEndian, ByteOrder};

use serde::Serialize;

use super::bytes::Bytes;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize)]
pub(crate) struct MapId(u16);

impl MapId {
    pub fn read(bytes: &Bytes) -> Self {
        MapId(BigEndian::read_u16(&bytes.0))
    }

    pub fn map(&self, depth: u8) -> usize {
        if depth > 0 {
            (self.0 >> (16 - depth)) as usize
        } else {
            0
        }
    }
}

impl From<usize> for MapId {
    fn from(map: usize) -> MapId {
        MapId(map as u16)
    }
}
