use crate::{
    common::store::Field,
    map::{
        errors::{HashError, MapError},
        interact::{self, Query, Update},
        store::Node,
    },
};

use snafu::ResultExt;

pub struct Map<Key, Value>
where
    Key: Field,
    Value: Field,
{
    root: Node<Key, Value>,
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
        Map { root: Node::Empty }
    }

    /// Returns a reference to the value corresponding to the key.
    ///
    /// # Errors
    ///
    /// If the map did not have the key present but it cannot determine if the association exists or not
    /// (e.g. locally part of the tree is missing, replaced by a `Stub`), [`BranchUnknown`] is returned.
    ///
    /// If the `Key` or `Value` cannot be hashed (via `drop::crypto::hash`), [`HashError`] is returned
    ///
    /// [`Stub`]: store/node/enum.Node.html
    /// [`BranchUnknown`]: errors/enum.MapError.html
    /// [`HashError`]: errors/enum.MapError.html
    ///
    /// # Examples
    ///
    /// ```
    /// use zebra::map::Map;
    ///
    /// let mut map = Map::new();
    /// map.insert(1, "a");
    /// assert_eq!(map.get(&1).unwrap(), Some(&"a"));
    /// assert_eq!(map.get(&2).unwrap(), None);
    /// ```
    pub fn get(&self, key: &Key) -> Result<Option<&Value>, MapError> {
        let query = Query::new(key).context(HashError)?;

        interact::get(&self.root, query)
    }

    /// Inserts a key-value pair into the map.
    ///
    /// If the map did not have this key present, [`None`] is returned.
    ///
    /// If the map did have this key present, the value is updated, and the old value is returned.
    ///
    /// [`None`]: https://doc.rust-lang.org/std/option/enum.Option.html#variant.None
    /// [`Stub`]: store/node/enum.Node.html
    ///
    /// # Errors
    ///
    /// If the portion of the map pertaining to the key is incomplete, i.e. there is a [`Stub`]
    /// on the key's path), [`BranchUnknown`] is returned.
    ///
    /// # Examples
    ///
    /// ```
    /// use zebra::map::Map;
    ///
    /// let mut map = Map::new();
    /// assert_eq!(map.insert("Alice", 1).unwrap(), None);
    ///
    /// map.insert("Alice", 2);
    /// assert_eq!(map.insert("Alice", 3).unwrap(), Some(2));
    /// assert_eq!(map.get(&"Alice").unwrap(), Some(&3));
    /// ```
    pub fn insert(
        &mut self,
        key: Key,
        value: Value,
    ) -> Result<Option<Value>, MapError> {
        let update = Update::set(key, value).context(HashError)?;

        self.update(update)
    }

    /// Removes a key from the map, returning the value at the key if the
    /// key was previously in the map.
    ///
    /// If the map did not have this key present, [`None`] is returned.
    ///
    /// [`None`]: https://doc.rust-lang.org/std/option/enum.Option.html#variant.None
    /// [`Stub`]: store/node/enum.Node.html
    ///
    /// # Errors
    ///
    /// If the portion of the map pertaining to the key is incomplete, i.e. there is a [`Stub`]
    /// on the key's path, [`BranchUnknown`] is returned.
    ///
    /// # Examples
    ///
    /// ```
    /// use zebra::map::Map;
    ///
    /// let mut map = Map::new();
    ///
    /// map.insert(1, "a");
    /// assert_eq!(map.remove(&1).unwrap(), Some("a"));
    /// assert_eq!(map.remove(&1).unwrap(), None);
    /// ```
    pub fn remove(&mut self, key: &Key) -> Result<Option<Value>, MapError> {
        let update = Update::remove(key).context(HashError)?;

        self.update(update)
    }

    fn update(
        &mut self,
        update: Update<Key, Value>,
    ) -> Result<Option<Value>, MapError> {
        let mut root = Node::Empty;
        std::mem::swap(&mut root, &mut self.root); // `interact::apply` needs ownership of `self.root`:
                                                   // swap `self.root` with a `Node::Empty` placeholder,
                                                   // to be restored as soon as `interact::apply` returns.

        let (mut new_root, result) = interact::apply(root, update);

        std::mem::swap(&mut new_root, &mut self.root); // Restore `self.root` to guarantee a consistent state
                                                       // when this method returns.

        result
    }
}
