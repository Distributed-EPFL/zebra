use drop::crypto::hash::HashError;

use super::action::Action;
use super::field::Field;
use super::path::Path;
use super::wrap::Wrap;

use tokio::sync::oneshot;
use tokio::sync::oneshot::Receiver;

#[derive(Debug)]
pub(crate) struct Operation<Key: Field, Value: Field> {
    pub path: Path,
    pub key: Wrap<Key>,
    pub action: Action<Value>,
}

impl<Key, Value> Operation<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub fn get(
        key: Key,
    ) -> Result<(Self, Receiver<Option<Wrap<Value>>>), HashError> {
        let (sender, receiver) = oneshot::channel();
        let key = Wrap::new(key)?;

        Ok((
            Operation {
                path: Path::from(*key.digest()),
                key,
                action: Action::Get(Some(sender)),
            },
            receiver,
        ))
    }

    pub fn set(key: Key, value: Value) -> Result<Self, HashError> {
        let key = Wrap::new(key)?;
        let value = Wrap::new(value)?;

        Ok(Operation {
            path: Path::from(*key.digest()),
            key,
            action: Action::Set(value),
        })
    }

    pub fn remove(key: Key) -> Result<Self, HashError> {
        let key = Wrap::new(key)?;
        Ok(Operation {
            path: Path::from(*key.digest()),
            key,
            action: Action::Remove,
        })
    }
}

impl<Key, Value> PartialEq for Operation<Key, Value>
where
    Key: Field,
    Value: Field,
{
    fn eq(&self, rho: &Self) -> bool {
        (self.key == rho.key) && (self.action == rho.action) // `path` is uniquely determined by `key`
    }
}

impl<Key, Value> Eq for Operation<Key, Value>
where
    Key: Field,
    Value: Field,
{
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::direction::Direction;
    use super::super::prefix::Prefix;

    fn prefix_from_directions(directions: &Vec<Direction>) -> Prefix {
        let mut prefix = Prefix::root();

        for &direction in directions {
            prefix = if direction == Direction::Left {
                prefix.left()
            } else {
                prefix.right()
            };
        }

        prefix
    }

    #[test]
    fn operation() {
        use Direction::{Left as L, Right as R};

        let prefix = prefix_from_directions(&vec![
            L, L, L, R, L, L, R, R, R, R, L, R, L, R, L, L,
        ]);

        let set = Operation::set(0u32, 8u32).unwrap();
        assert!(prefix.contains(&set.path));
        assert_eq!(set.key, Wrap::new(0u32).unwrap());
        assert_eq!(set.action, Action::Set(Wrap::new(8u32).unwrap()));

        let remove = Operation::remove(0u32).unwrap();
        assert_eq!(remove.path, set.path);
        assert_eq!(remove.key, set.key);
        assert_eq!(remove.action, Action::<u32>::Remove);
    }
}
