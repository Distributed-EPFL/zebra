use crate::{
    common::store::Field,
    map::{
        errors::{HashError, MapError},
        interact::{self, Query, Update},
        store::Node,
    },
};

use snafu::ResultExt;

/// A map based on Merkle-prefix trees supporting both existence and deniability proofs.
///
/// Due to the way key-value pairs are stored in the merkle tree, there is a one-to-one
/// mapping between any set of key-value pairs and a [`Map`]. This permits the
/// creation of deniability proofs, i.e. proofs that show that a key-value pair with a
/// specific key doesn't (yet) exist. Please refer to the end of this documentation for
/// a brief explanation.
///
/// Note that this is unlike normal merkle trees following the [RFC6962](https://tools.ietf.org/html/rfc6962)
/// standard which are generally balanced, and for which the order of key-value pairs
/// in the tree is typically determined by the order of insertion.
///
/// The default hashing algorithm is currently Blake3, though this is
/// subject to change at any point in the future.
///
/// It is required that the keys implement `'static` and the [`Serialize`],
/// [`Send`] and [`Sync`] traits.
///
/// [`Field`]: crate::common::store::Field
/// [`Table`]: crate::database::Table
/// [`Transaction`]: crate::database::Transaction
/// [`Serialize`]: serde::Serialize
/// [`Send`]: Send
/// [`Sync`]: Sync
///
/// # Examples
///
/// ```
/// use zebra::map::Map;
///
/// // Type inference lets us omit an explicit type signature (which
/// // would be `Map<str, str>` in this example).
/// let mut color_preferences = Map::new();
///
/// // Add some preferences.
/// color_preferences.insert(
///     "Alice",
///     "red",
/// );
/// color_preferences.insert(
///     "Bob",
///     "green",
/// );
/// color_preferences.insert(
///     "Charlie",
///     "blue",
/// );
///
/// // We can get the corresponding value of Alice.
/// assert_eq!(color_preferences.get(&"Alice").unwrap(), Some(&"red"));
///
/// // Bob wants to remove his preference.
/// // When maps store owned values (String), they can still be
/// // queried using references (&str).
/// let old_preference = color_preferences.remove(&"Bob");
/// assert_eq!(old_preference.unwrap(), Some("green"));
///
/// // Charlie actually preferes 'cyan'. Let's change his preference.
/// let old_preference = color_preferences.insert(
///     "Charlie",
///     "cyan",
/// );
/// assert_eq!(old_preference.unwrap(), Some("blue"));
///
/// ```
///
/// [`Eq`]: https://doc.rust-lang.org/std/cmp/trait.Eq.html
/// [`Serialize`]: https://docs.serde.rs/serde/trait.Serialize.html
/// [`Deserialize`]: https://docs.serde.rs/serde/trait.Deserialize.html
/// [`Clone`]: https://doc.rust-lang.org/std/clone/trait.Clone.html
/// [`RefCell`]: https://doc.rust-lang.org/std/cell/struct.RefCell.html
/// [`Cell`]: https://doc.rust-lang.org/std/cell/struct.Cell.html
/// [`hash`]: ../drop/crypto/hash/fn.hash.html
///
/// # Caching and Hash recomputation
///
/// For trees with large numbers of records, computing the root hash from
/// scratch after every modification can be very costly.
///
/// To avoid unnecessary recomputation after each modification, each
/// internal node in a tree caches its digest. This digest is only
/// updated on an as-needed basis, e.g. when the node lies along the path
/// of a modification, such as an insertion or removal.
///
/// It is important to note that all cached values in a tree are skipped
/// on serialization ([`Serialize`]) and recomputed on deserialization
/// ([`Deserialize`]), thus ensuring that they are locally valid at all
/// times, in spite of any prior malicious tampering that might have happened.
///
/// # One-to-one mapping of key-value pairs.
///
/// Key-Value pairs are placed in the tree along the path corresponding to the hash of their keys.
/// E.g. {Key: "Alice", Value: 3} -> hash("Alice")
/// -> b00100110... -> Left (0), Left (0), Right (1), Left (0)...
///
/// Any two keys (e.g. k1, k2) with respective hashes having the first
/// N bits in common (i.e. hash(k1)[bit0..bitN] == hash(k1)[bit0..bitN]), will share
/// the same branch/path in the tree until depth N.
///
/// By proving that some other key-value pair is present along the path of the our key, we prove
/// that that key is not present in the tree; if it was, then the branch would run deeper until it
/// forked and we found our key.
///
/// E.g.:
/// ```text
/// hash(k1) = b000..
/// hash(k2) = b110..
/// hash(k3) = b111..
/// ```
///
/// Tree without k3:
/// ```text
///         o
///        / \
///      k1   k2
/// ```
/// Tree after k3 is inserted:
/// ```text
///         o
///        / \
///      k1   o
///            \
///             o
///            / \
///          k2   k3
/// ```

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
