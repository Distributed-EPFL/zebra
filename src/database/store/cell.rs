use crate::{common::data::AtomicLender, database::store::Store};

use std::sync::Arc;

pub(crate) type Cell<Key, Value> = Arc<AtomicLender<Store<Key, Value>>>;
