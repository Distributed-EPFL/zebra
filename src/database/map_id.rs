use byteorder::{BigEndian, ByteOrder};

use serde::Serialize;

use super::bytes::Bytes;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize)]
pub(crate) struct MapId(u16);

impl MapId {
    pub fn read(bytes: &Bytes) -> MapId {
        MapId(BigEndian::read_u16(&bytes.0))
    }

    pub fn crop(&self, depth: u8, splits: u8) -> usize {
        let value = (if splits < depth {
            (self.0 << splits) >> (16 + splits - depth)
        } else {
            0
        }) as usize;

        value
    }
}
