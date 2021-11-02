use crate::{
    common::{
        data::Bytes,
        tree::{Direction, Prefix},
    },
    database::store::DEPTH,
};

use serde::{Deserialize, Serialize};

use std::fmt::{Debug, Error, Formatter, LowerHex};

#[derive(Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MapId(u8);

impl MapId {
    pub fn internal(position: Prefix) -> Self {
        let mut id = 0;

        for (bit, direction) in (0..DEPTH).zip(position) {
            if direction == Direction::Left {
                id |= 1 << (7 - bit);
            }
        }

        MapId(id)
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

impl LowerHex for MapId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{:x}", self.0)?;
        Ok(())
    }
}

impl Debug for MapId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "MapId({:x})", self)?;
        Ok(())
    }
}
