use serde::Serialize;

use super::wrap::Wrap;

#[derive(Debug)]
pub(crate) enum Action<Value: 'static + Serialize + Send + Sync> {
    Set(Wrap<Value>),
    Remove,
}

impl<Value> PartialEq for Action<Value>
where
    Value: 'static + Serialize + Send + Sync,
{
    fn eq(&self, rho: &Self) -> bool {
        match (self, rho) {
            (Action::Set(self_value), Action::Set(rho_value)) => {
                self_value == rho_value
            }
            (Action::Remove, Action::Remove) => true,
            _ => false,
        }
    }
}

impl<Value> Eq for Action<Value> where Value: 'static + Serialize + Send + Sync {}
