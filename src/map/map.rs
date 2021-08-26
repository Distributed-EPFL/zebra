use crate::{
    common::{data::Lender, store::Field},
    map::{
        errors::{HashError, MapError},
        interact::{self, Query, Update},
        store::Node,
    },
};

use serde::{Serialize, Serializer};

use snafu::ResultExt;

use std::borrow::Borrow;

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

pub struct Map<Key: Field, Value: Field> {
    root: Lender<Node<Key, Value>>,
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
            root: Lender::new(Node::Empty),
        }
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
        interact::get(self.root.borrow(), query)
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
        let root = self.root.take();
        let (root, result) = interact::apply(root, update);
        self.root.restore(root);

        result
    }
}

impl<Key, Value> Serialize for Map<Key, Value>
where
    Key: Field,
    Value: Field,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.root.serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        common::tree::{Path, Prefix},
        map::store::{Internal, Leaf},
    };

    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::fmt::Debug;
    use std::hash::Hash;

    impl<Key, Value> Map<Key, Value>
    where
        Key: Field,
        Value: Field,
    {
        pub(crate) fn check_internal(internal: &Internal<Key, Value>) {
            match (internal.left(), internal.right()) {
                (Node::Empty, Node::Empty)
                | (Node::Empty, Node::Leaf(..))
                | (Node::Leaf(..), Node::Empty) => {
                    panic!("`check_internal`: children violate compactness")
                }
                _ => {}
            }
        }

        pub(crate) fn check_leaf(leaf: &Leaf<Key, Value>, location: Prefix) {
            if !location.contains(&Path::from(*leaf.key().digest())) {
                panic!("`check_leaf`: leaf outside of its key path");
            }
        }

        pub(crate) fn check_tree(&self) {
            fn recursion<Key, Value>(node: &Node<Key, Value>, location: Prefix)
            where
                Key: Field,
                Value: Field,
            {
                match node {
                    Node::Internal(internal) => {
                        Map::check_internal(internal);

                        recursion(internal.left(), location.left());
                        recursion(internal.right(), location.right());
                    }
                    Node::Leaf(leaf) => {
                        Map::check_leaf(leaf, location);
                    }
                    Node::Empty | Node::Stub(_) => {}
                }
            }

            recursion(self.root.borrow(), Prefix::root());
        }

        pub(crate) fn collect_records(&self) -> HashMap<Key, Value>
        where
            Key: Field + Clone + Eq + Hash,
            Value: Field + Clone,
        {
            fn recursion<Key, Value>(
                node: &Node<Key, Value>,
                collector: &mut HashMap<Key, Value>,
            ) where
                Key: Field + Clone + Eq + Hash,
                Value: Field + Clone,
            {
                match node {
                    Node::Internal(internal) => {
                        recursion(internal.left(), collector);
                        recursion(internal.right(), collector);
                    }
                    Node::Leaf(leaf) => {
                        collector.insert(
                            leaf.key().inner().clone(),
                            leaf.value().inner().clone(),
                        );
                    }
                    Node::Empty | Node::Stub(_) => {}
                }
            }

            let mut collector = HashMap::new();
            recursion(self.root.borrow(), &mut collector);
            collector
        }

        pub fn assert_records<I>(&self, reference: I)
        where
            Key: Field + Debug + Clone + Eq + Hash,
            Value: Field + Debug + Clone + Eq + Hash,
            I: IntoIterator<Item = (Key, Value)>,
        {
            let actual: HashSet<(Key, Value)> =
                self.collect_records().into_iter().collect();

            let reference: HashSet<(Key, Value)> =
                reference.into_iter().collect();

            let differences: HashSet<(Key, Value)> = reference
                .symmetric_difference(&actual)
                .map(|r| r.clone())
                .collect();

            assert_eq!(differences, HashSet::new());
        }
    }

    #[test]
    fn stress() {
        let mut map: Map<u32, u32> = Map::new();
        map.check_tree();
        map.assert_records([]);

        for (key, value) in (0..1024).map(|i| (i, i)) {
            assert_eq!(map.insert(key, value).unwrap(), None);

            map.check_tree();
            map.assert_records((0..=key).map(|i| (i, i)));
        }

        for key in 0..2048 {
            if key < 1024 {
                assert_eq!(map.get(&key).unwrap(), Some(&key))
            } else {
                assert_eq!(map.get(&key).unwrap(), None)
            }
        }

        for key in 512..1024 {
            assert_eq!(map.remove(&key).unwrap(), Some(key));
        }

        for key in 0..2048 {
            if key < 512 {
                assert_eq!(map.get(&key).unwrap(), Some(&key))
            } else {
                assert_eq!(map.get(&key).unwrap(), None)
            }
        }

        for (key, value) in (0..512).map(|i| (i, i + 1)) {
            assert_eq!(map.insert(key, value).unwrap(), Some(key));

            map.check_tree();
            map.assert_records((0..512).map(|i| {
                if i <= key {
                    (i, i + 1)
                } else {
                    (i, i)
                }
            }));
        }

        for key in 0..2048 {
            if key < 512 {
                assert_eq!(map.get(&key).unwrap(), Some(&(key + 1)))
            } else {
                assert_eq!(map.get(&key).unwrap(), None)
            }
        }

        for key in 0..512 {
            assert_eq!(map.remove(&key).unwrap(), Some(key + 1));
        }

        map.check_tree();
        map.assert_records([]);
    }
}
