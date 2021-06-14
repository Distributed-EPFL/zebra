use serde::Serialize;

use super::operation::Operation;

#[derive(Debug)]
pub(crate) enum Task<'a, Key: Serialize + Sync, Value: Serialize + Sync> {
    Pass,
    Do(&'a Operation<Key, Value>),
    Split,
}

impl<'a, Key, Value> PartialEq for Task<'a, Key, Value>
where
    Key: Serialize + Sync,
    Value: Serialize + Sync,
{
    fn eq(&self, rho: &Self) -> bool {
        match (self, rho) {
            (Task::Pass, Task::Pass) => true,
            (Task::Do(self_op), Task::Do(rho_op)) => self_op == rho_op,
            (Task::Split, Task::Split) => true,
            _ => false,
        }
    }
}

impl<'a, Key, Value> Eq for Task<'a, Key, Value>
where
    Key: Serialize + Sync,
    Value: Serialize + Sync,
{
}
