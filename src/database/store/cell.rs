use crate::database::store::Store;

use std::sync::Arc;

use talk::sync::lenders::AtomicLender;

pub(crate) type Cell<Key, Value> = Arc<AtomicLender<Store<Key, Value>>>;
