use crate::database::{
    data::Bytes,
    interact::Action,
    store::{Field, Wrap},
    tree::Path,
};

use drop::crypto::hash;
use drop::crypto::hash::HashError;

#[derive(Debug)]
pub(crate) struct Operation<Key: Field, Value: Field> {
    pub path: Path,
    pub action: Action<Key, Value>,
}

impl<Key, Value> Operation<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub fn get(key: &Key) -> Result<Self, HashError> {
        let hash: Bytes = hash::hash(key)?.into();

        Ok(Operation {
            path: Path::from(hash),
            action: Action::Get(None),
        })
    }

    pub fn set(key: Key, value: Value) -> Result<Self, HashError> {
        let key = Wrap::new(key)?;
        let value = Wrap::new(value)?;

        Ok(Operation {
            path: Path::from(*key.digest()),
            action: Action::Set(key, value),
        })
    }

    pub fn remove(key: &Key) -> Result<Self, HashError> {
        let hash: Bytes = hash::hash(key)?.into();

        Ok(Operation {
            path: Path::from(hash),
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
        (self.path == rho.path) && (self.action == rho.action)
    }
}

impl<Key, Value> Eq for Operation<Key, Value>
where
    Key: Field,
    Value: Field,
{
}

#[cfg(test)]
#[macro_use]
mod tests {
    use super::*;

    use crate::database::tree::{Direction, Prefix};

    #[macro_use]
    mod macros {
        macro_rules! get {
            ($key: expr) => {
                crate::database::interact::Operation::get(&$key).unwrap()
            };
        }

        macro_rules! set {
            ($key: expr, $value: expr) => {
                crate::database::interact::Operation::set($key, $value).unwrap()
            };
        }

        macro_rules! remove {
            ($key: expr) => {
                crate::database::interact::Operation::remove(&$key).unwrap()
            };
        }
    }

    #[test]
    fn operation() {
        use Direction::{Left as L, Right as R};

        let prefix = Prefix::from_directions(vec![
            L, L, L, R, L, L, R, R, R, R, L, R, L, R, L, L,
        ]);

        let set = set!(0u32, 8u32);

        assert!(prefix.contains(&set.path));
        assert_eq!(set.path, Path::from(hash(&0u32).unwrap()));

        assert_eq!(
            set.action,
            Action::Set(Wrap::new(0u32).unwrap(), Wrap::new(8u32).unwrap())
        );

        let remove = remove!(0u32);
        assert_eq!(remove.path, set.path);
        assert_eq!(remove.action, Action::<u32, u32>::Remove);
    }
}
