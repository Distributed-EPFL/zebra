use byteorder::{BigEndian, ByteOrder};

use serde::Serialize;

use super::bytes::Bytes;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize)]
pub(crate) struct MapId(u16);

impl MapId {
    pub fn read(bytes: &Bytes) -> MapId {
        MapId(BigEndian::read_u16(&bytes.0))
    }
}
