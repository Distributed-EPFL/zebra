use crate::database::{interact::Operation, store::Field};

#[derive(Debug)]
pub(crate) enum Task<'a, Key: Field, Value: Field> {
    Pass,
    Do(&'a mut Operation<Key, Value>),
    Split,
}

impl<'a, Key, Value> PartialEq for Task<'a, Key, Value>
where
    Key: Field,
    Value: Field,
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
    Key: Field,
    Value: Field,
{
}