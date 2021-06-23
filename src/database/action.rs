use std::sync::Mutex;

use super::field::Field;
use super::wrap::Wrap;

use tokio::sync::oneshot::Sender;

#[derive(Debug)]
pub(crate) enum Action<Value: Field> {
    Get(Mutex<Option<Sender<Option<Wrap<Value>>>>>),
    Set(Wrap<Value>),
    Remove,
}

impl<Value> PartialEq for Action<Value>
where
    Value: Field,
{
    fn eq(&self, rho: &Self) -> bool {
        match (self, rho) {
            (Action::Get(..), Action::Get(..)) => true,
            (Action::Set(self_value), Action::Set(rho_value)) => {
                self_value == rho_value
            }
            (Action::Remove, Action::Remove) => true,
            _ => false,
        }
    }
}

impl<Value> Eq for Action<Value> where Value: Field {}
