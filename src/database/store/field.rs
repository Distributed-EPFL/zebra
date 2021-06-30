use serde::Serialize;

pub trait Field: 'static + Serialize + Send + Sync {}

impl<T> Field for T where T: 'static + Serialize + Send + Sync {}
