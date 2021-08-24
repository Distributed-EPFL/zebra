use crate::{
    common::store::Field,
    map::{
        errors::{HashError, MapError},
        interact::{self, Query, Update},
        store::Node,
    },
};

use snafu::ResultExt;

use std::rc::Rc;

pub struct Map<Key, Value>
where
    Key: Field,
    Value: Field,
{
    root: Option<Node<Key, Value>>,
}

impl<Key, Value> Map<Key, Value>
where
    Key: Field,
    Value: Field,
{
    /// Creates an empty `Map`
    ///
    /// # Examples
    ///
    /// ```
    /// use zebra::map::Map;
    ///
    /// let mut tree: Map<&str, i32> = Map::new();
    /// ```
    pub fn new() -> Self {
        Map {
            root: Some(Node::Empty),
        }
    }

    /// Returns an `Rc` to the value corresponding to the key, if it is present in the `Map`.
    ///
    /// [`Eq`]: https://doc.rust-lang.org/std/cmp/trait.Eq.html
    /// [`Serialize`]: https://docs.serde.rs/serde/trait.Serialize.html
    ///
    /// # Return
    /// If the map did not have the key present and it is guaranteed to not exist,
    /// `None` is returned.
    ///
    /// # Errors
    /// If the map did not have the key present but it cannot determine if association exists or not
    /// (e.g. locally part of the tree is missing, replaced by a `Stub`), [`BranchUnknown`] is returned.
    ///
    /// If the `Key` or `Value` cannot be hashed (via `drop::crypto::hash`), [`HashError`] is returned
    ///
    /// [`BranchUnknown`]: error/enum.MapError.html
    /// [`HashError`]: error/enum.MapError.html
    ///
    /// # Examples
    ///
    /// ```
    /// use zebra::map::Map;
    /// use zebra::map::errors::MapError;
    ///
    /// // let mut map = Map::new();
    /// // map.insert(1, "a");
    /// // assert_eq!(map.get(&1), Ok(Some("a")));
    /// // assert_eq!(map.get(&2), Err(MapError));
    /// ```
    pub fn get(&mut self, key: &Key) -> Result<Option<Rc<Value>>, MapError> {
        // let operation = Operation::get(key).context(HashError)?;

        // let root = self.root.take().unwrap();
        // let (root, result) = interact::apply(root, operation);
        // self.root = Some(root);

        // result
        unimplemented!();
    }
}
