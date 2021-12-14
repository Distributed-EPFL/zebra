use crate::{
    common::{data::Bytes, store::Field, tree::Path},
    map::{
        errors::MapError,
        interact::{self, Query, Update},
        store::{self, Node},
    },
};

use doomstack::{here, ResultExt, Top};

use serde::{de::Error as DeError, Deserialize, Deserializer, Serialize, Serializer};

use std::{
    borrow::{Borrow, BorrowMut},
    fmt::{Debug, Error, Formatter},
};

use talk::{
    crypto::primitives::{hash, hash::Hash},
    sync::lenders::Lender,
};

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
/// [`Transaction`]: crate::database::TableTransaction
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

    pub fn root_stub(commitment: Hash) -> Self {
        Map {
            root: Lender::new(Node::stub(commitment.into())),
        }
    }

    pub(crate) fn raw(root: Node<Key, Value>) -> Self {
        Map {
            root: Lender::new(root),
        }
    }

    /// Returns a cryptographic commitment to the contents of the `Map`.
    /// Exporting a `Map`, even partially, preserves its commitment.
    /// A `Map` can be imported only by another `Map` with matching
    /// commitment.
    ///
    /// # Examples
    ///
    /// ```
    /// use zebra::map::Map;
    ///
    /// let mut map: Map<&str, i32> = Map::new();
    /// map.insert("alice", 31);
    /// map.insert("bob", 44);
    ///
    /// let export = map.export(["alice"]).unwrap();
    /// assert_eq!(map.commit(), export.commit());
    /// ```
    pub fn commit(&self) -> Hash {
        let root: &Node<Key, Value> = self.root.borrow();
        root.hash().into()
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
    pub fn get(&self, key: &Key) -> Result<Option<&Value>, Top<MapError>> {
        let query = Query::new(key).pot(MapError::HashError, here!())?;
        interact::get(self.root.borrow(), query)
    }

    /// Inserts a key-value pair into the map.
    ///
    /// If the map did not have this key present, [`None`] is returned.
    ///
    /// If the map did have this key present, the value is updated, and the old value is returned.
    ///
    /// [`None`]: https://doc.rust-lang.org/std/option/enum.Option.html#variant.None
    ///
    /// # Errors
    ///
    /// If the portion of the map pertaining to the key is incomplete, i.e. there is a `Stub`
    /// on the key's path), [`BranchUnknown`] is returned.
    ///
    /// [`BranchUnknown`]: errors/enum.MapError.html
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
    pub fn insert(&mut self, key: Key, value: Value) -> Result<Option<Value>, Top<MapError>> {
        let update = Update::insert(key, value).pot(MapError::HashError, here!())?;
        self.update(update)
    }

    /// Removes a key from the map, returning the value at the key if the
    /// key was previously in the map.
    ///
    /// If the map did not have this key present, [`None`] is returned.
    ///
    /// [`None`]: https://doc.rust-lang.org/std/option/enum.Option.html#variant.None
    ///
    /// # Errors
    ///
    /// If the portion of the map pertaining to the key is incomplete, i.e. there is a `Stub`
    /// on the key's path, [`BranchUnknown`] is returned.
    ///
    /// [`BranchUnknown`]: errors/enum.MapError.html
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
    pub fn remove(&mut self, key: &Key) -> Result<Option<Value>, Top<MapError>> {
        let update = Update::remove(key).pot(MapError::HashError, here!())?;
        self.update(update)
    }

    fn update(&mut self, update: Update<Key, Value>) -> Result<Option<Value>, Top<MapError>> {
        let root = self.root.take();
        let (root, result) = interact::apply(root, update);
        self.root.restore(root);

        result
    }

    /// Exports a subset of the map containing only branches along the given keys.
    /// Excluded branches are replaced by `Stub`s.
    ///
    /// The keys may be any borrowed form of the tree's key type, but
    /// [`Serialize`] on the borrowed form *must* match that of
    /// the key type.
    ///
    /// [`Serialize`]: https://docs.serde.rs/serde/trait.Serialize.html
    ///
    /// # Errors
    /// If the it cannot be determined if the key does or does not exist
    /// (e.g. locally part of the map is missing, replaced by a `Stub`), [`BranchUnknown`] is returned.
    ///
    /// [`BranchUnknown`]: errors/enum.MapError.html
    /// # Examples
    ///
    /// ```
    /// use zebra::map::Map;
    /// use zebra::map::errors::MapError;
    ///
    /// let mut map = Map::new();
    ///
    /// map.insert(1, "a");
    /// map.insert(2, "b");
    /// map.insert(3, "c");
    ///
    /// let submap = map.export([&1]).unwrap();
    ///
    /// assert_eq!(submap.get(&1).unwrap(), Some(&"a"));
    /// assert!(submap.get(&2).is_err()); // MapError::BranchUnknown
    /// assert!(submap.get(&3).is_err()); // MapError::BranchUnknown
    ///
    /// assert_eq!(map.get(&1).unwrap(), Some(&"a"));
    /// assert_eq!(map.get(&2).unwrap(), Some(&"b"));
    /// assert_eq!(map.get(&3).unwrap(), Some(&"c"));
    /// ```
    pub fn export<I, K>(&self, keys: I) -> Result<Map<Key, Value>, Top<MapError>>
    where
        Key: Clone,
        Value: Clone,
        I: IntoIterator<Item = K>,
        K: Borrow<Key>,
    {
        let paths: Result<Vec<Path>, Top<MapError>> = keys
            .into_iter()
            .map(|key| {
                hash::hash(key.borrow())
                    .map(|digest| Path::from(Bytes::from(digest)))
                    .pot(MapError::HashError, here!())
            })
            .collect();

        let mut paths = paths?;
        paths.sort();

        let root = interact::export(self.root.borrow(), &paths)?;

        Ok(Map {
            root: Lender::new(root),
        })
    }

    /// Computes the union of two *compatible* maps.
    /// Two `Map`s are compatible if they share the same underlying key-value associations.
    ///
    /// Concretely, it replaces `Stub`s in the first map with the concrete information
    /// in the second map. The first map is therefore extended with the missing information
    /// (key-value associations) that the second map possesses.
    ///
    /// This can be used as a method to merge (and condense) multiple maps into one.
    ///
    /// # Errors
    /// If the maps are not compatible, [`MapIncompatible`] is returned.
    ///
    /// [`MapIncompatible`]: errors/enum.MapError.html
    ///
    /// # Examples
    ///
    /// ```
    /// use zebra::map::Map;
    /// use zebra::map::errors::MapError;
    ///
    /// let mut map = Map::new();
    ///
    /// map.insert(1, "a");
    /// map.insert(2, "b");
    /// map.insert(3, "c");
    ///
    /// let mut first_submap = map.export([&1]).unwrap();
    /// let second_submap = map.export([&2]).unwrap();
    ///
    /// first_submap.import(second_submap).unwrap();
    ///
    /// assert_eq!(first_submap.get(&1).unwrap(), Some(&"a"));
    /// assert_eq!(first_submap.get(&2).unwrap(), Some(&"b"));
    /// assert!(first_submap.get(&3).is_err());
    ///
    /// let mut incompatible_map = Map::new();
    ///
    /// incompatible_map.insert(3, "c");
    ///
    /// // MapError::MapIncompatible
    /// assert!(first_submap.import(incompatible_map).is_err())
    /// ```
    pub fn import(&mut self, mut other: Map<Key, Value>) -> Result<(), Top<MapError>> {
        interact::import(self.root.borrow_mut(), other.root.take())
    }
}

impl<Key, Value> Debug for Map<Key, Value>
where
    Key: Field,
    Value: Field,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "Map(commitment: {:?})", self.commit())
    }
}

impl<Key, Value> Clone for Map<Key, Value>
where
    Key: Field + Clone,
    Value: Field + Clone,
{
    fn clone(&self) -> Self {
        let root: &Node<Key, Value> = self.root.borrow();
        Map::raw(root.clone())
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

impl<'de, Key, Value> Deserialize<'de> for Map<Key, Value>
where
    Key: Field + Deserialize<'de>,
    Value: Field + Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let root = Node::deserialize(deserializer)?; // Deserializes and computes node hashes

        store::check(&root) // Checks correctness of tree topology
            .map_err(|err| DeError::custom(err))?;

        Ok(Map {
            root: Lender::new(root),
        }) // If a `Map` is `Deserialize`d, then it is correct
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        common::store::hash,
        map::store::{self, Internal},
    };

    use std::{
        collections::{HashMap, HashSet},
        fmt::Debug,
        hash::Hash,
    };

    impl<Key, Value> Map<Key, Value>
    where
        Key: Field,
        Value: Field,
    {
        pub(crate) fn check_tree(&self) {
            store::check(self.root.borrow()).unwrap();
        }

        pub(crate) fn collect_records(&self) -> HashMap<Key, Value>
        where
            Key: Field + Clone + Eq + Hash,
            Value: Field + Clone,
        {
            fn recursion<Key, Value>(node: &Node<Key, Value>, collector: &mut HashMap<Key, Value>)
            where
                Key: Field + Clone + Eq + Hash,
                Value: Field + Clone,
            {
                match node {
                    Node::Internal(internal) => {
                        recursion(internal.left(), collector);
                        recursion(internal.right(), collector);
                    }
                    Node::Leaf(leaf) => {
                        collector.insert(leaf.key().inner().clone(), leaf.value().inner().clone());
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
            let actual: HashSet<(Key, Value)> = self.collect_records().into_iter().collect();

            let reference: HashSet<(Key, Value)> = reference.into_iter().collect();

            let differences: HashSet<(Key, Value)> = reference
                .symmetric_difference(&actual)
                .map(|r| r.clone())
                .collect();

            assert_eq!(differences, HashSet::new());
        }
    }

    #[test]
    fn empty() {
        let map: Map<u32, u32> = Map::new();

        map.check_tree();
        map.assert_records([]);
    }

    #[test]
    fn insert() {
        let mut map: Map<u32, u32> = Map::new();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            assert_eq!(map.insert(key, value).unwrap(), None);

            map.check_tree();
            map.assert_records((0..=key).map(|i| (i, i)));
        }
    }

    #[test]
    fn insert_then_get() {
        let mut map: Map<u32, u32> = Map::new();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            map.insert(key, value).unwrap();
        }

        for key in 0..2048 {
            if key < 1024 {
                assert_eq!(map.get(&key).unwrap(), Some(&key))
            } else {
                assert_eq!(map.get(&key).unwrap(), None)
            }
        }
    }

    #[test]
    fn insert_then_remove_half() {
        let mut map: Map<u32, u32> = Map::new();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            map.insert(key, value).unwrap();
        }

        for key in 512..1024 {
            assert_eq!(map.remove(&key).unwrap(), Some(key));

            map.check_tree();
            map.assert_records(
                (0..512)
                    .map(|i| (i, i))
                    .chain(((key + 1)..1024).map(|i| (i, i))),
            );
        }
    }

    #[test]
    fn insert_then_remove_half_then_get() {
        let mut map: Map<u32, u32> = Map::new();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            map.insert(key, value).unwrap();
        }

        for key in 512..1024 {
            map.remove(&key).unwrap();
        }

        for key in 0..2048 {
            if key < 512 {
                assert_eq!(map.get(&key).unwrap(), Some(&key))
            } else {
                assert_eq!(map.get(&key).unwrap(), None)
            }
        }
    }

    #[test]
    fn insert_then_remove_half_then_increment() {
        let mut map: Map<u32, u32> = Map::new();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            map.insert(key, value).unwrap();
        }

        for key in 512..1024 {
            map.remove(&key).unwrap();
        }

        for (key, value) in (0..512).map(|i| (i, i + 1)) {
            assert_eq!(map.insert(key, value).unwrap(), Some(key));

            map.check_tree();
            map.assert_records((0..512).map(|i| if i <= key { (i, i + 1) } else { (i, i) }));
        }
    }

    #[test]
    fn insert_then_remove_half_then_increment_then_get() {
        let mut map: Map<u32, u32> = Map::new();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            map.insert(key, value).unwrap();
        }

        for key in 512..1024 {
            map.remove(&key).unwrap();
        }

        for (key, value) in (0..512).map(|i| (i, i + 1)) {
            map.insert(key, value).unwrap();
        }

        for key in 0..2048 {
            if key < 512 {
                assert_eq!(map.get(&key).unwrap(), Some(&(key + 1)))
            } else {
                assert_eq!(map.get(&key).unwrap(), None)
            }
        }
    }

    #[test]
    fn insert_then_remove_half_then_remove_other_half() {
        let mut map: Map<u32, u32> = Map::new();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            map.insert(key, value).unwrap();
        }

        for key in 512..1024 {
            map.remove(&key).unwrap();
        }

        for key in 0..512 {
            assert_eq!(map.remove(&key).unwrap(), Some(key));

            map.check_tree();
            map.assert_records(((key + 1)..512).map(|i| (i, i)));
        }
    }

    #[test]
    fn export_none() {
        let mut map: Map<u32, u32> = Map::new();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            map.insert(key, value).unwrap();
        }

        let export = map.export::<[u32; 0], u32>([]).unwrap(); // Explicit type arguments are to aid type inference on an empty array

        assert_eq!(map.commit(), export.commit());
        export.check_tree();
        export.assert_records([]);
    }

    #[test]
    fn export_single() {
        let mut map: Map<u32, u32> = Map::new();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            map.insert(key, value).unwrap();
        }

        let export = map.export([33]).unwrap();

        assert_eq!(map.commit(), export.commit());
        export.check_tree();
        export.assert_records([(33, 33)]);
    }

    #[test]
    fn export_single_then_get() {
        let mut map: Map<u32, u32> = Map::new();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            map.insert(key, value).unwrap();
        }

        let export = map.export([33]).unwrap();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            if key == 33 {
                assert_eq!(export.get(&key).unwrap(), Some(&value));
            } else {
                export.get(&key).unwrap_err();
            }
        }
    }

    #[test]
    fn export_outsider_then_get() {
        let mut map: Map<u32, u32> = Map::new();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            map.insert(key, value).unwrap();
        }

        let export = map.export([1025]).unwrap();

        assert_eq!(export.get(&1025).unwrap(), None);
    }

    #[test]
    fn export_half() {
        let mut map: Map<u32, u32> = Map::new();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            map.insert(key, value).unwrap();
        }

        let export = map.export(0..512).unwrap();

        assert_eq!(map.commit(), export.commit());
        export.check_tree();
        export.assert_records((0..512).map(|i| (i, i)));
    }

    #[test]
    fn export_half_then_get() {
        let mut map: Map<u32, u32> = Map::new();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            map.insert(key, value).unwrap();
        }

        let export = map.export(0..512).unwrap();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            if key < 512 {
                assert_eq!(export.get(&key).unwrap(), Some(&value));
            } else {
                export.get(&key).unwrap_err();
            }
        }
    }

    #[test]
    fn export_all() {
        let mut map: Map<u32, u32> = Map::new();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            map.insert(key, value).unwrap();
        }

        let export = map.export(0..1024).unwrap();

        assert_eq!(map.commit(), export.commit());
        export.check_tree();
        export.assert_records((0..1024).map(|i| (i, i)));
    }

    #[test]
    fn export_all_then_get() {
        let mut map: Map<u32, u32> = Map::new();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            map.insert(key, value).unwrap();
        }

        let export = map.export(0..1024).unwrap();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            assert_eq!(export.get(&key).unwrap(), Some(&value));
        }
    }

    #[test]
    fn export_all_then_get_outsider() {
        let mut map: Map<u32, u32> = Map::new();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            map.insert(key, value).unwrap();
        }

        let export = map.export(0..1024).unwrap();

        assert_eq!(export.get(&1025).unwrap(), None);
    }

    #[test]
    fn export_overlap_then_get() {
        let mut map: Map<u32, u32> = Map::new();

        for (key, value) in (0..512).map(|i| (i, i)) {
            map.insert(key, value).unwrap();
        }

        let export = map.export(256..1024).unwrap();

        assert_eq!(map.commit(), export.commit());
        export.check_tree();

        for (key, value) in (256..1024).map(|i| (i, i)) {
            if key < 512 {
                assert_eq!(export.get(&key).unwrap(), Some(&value));
            } else {
                assert_eq!(export.get(&key).unwrap(), None);
            }
        }
    }

    #[test]
    fn import_disjoint_singles() {
        let mut map: Map<u32, u32> = Map::new();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            map.insert(key, value).unwrap();
        }

        let mut main = map.export([33]).unwrap();
        let secondary = map.export([34]).unwrap();

        main.import(secondary).unwrap();

        assert_eq!(map.commit(), main.commit());
        main.check_tree();
        main.assert_records([(33, 33), (34, 34)]);
    }

    #[test]
    fn import_same_singles() {
        let mut map: Map<u32, u32> = Map::new();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            map.insert(key, value).unwrap();
        }

        let mut main = map.export([33]).unwrap();
        let secondary = map.export([33]).unwrap();

        main.import(secondary).unwrap();

        assert_eq!(map.commit(), main.commit());
        main.check_tree();
        main.assert_records([(33, 33)]);
    }

    #[test]
    fn import_disjoint_halves() {
        let mut map: Map<u32, u32> = Map::new();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            map.insert(key, value).unwrap();
        }

        let mut main = map.export(0..512).unwrap();
        let secondary = map.export(512..1024).unwrap();

        main.import(secondary).unwrap();

        assert_eq!(map.commit(), main.commit());
        main.check_tree();
        main.assert_records((0..1024).map(|i| (i, i)));
    }

    #[test]
    fn import_overlapping_halves() {
        let mut map: Map<u32, u32> = Map::new();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            map.insert(key, value).unwrap();
        }

        let mut main = map.export(0..512).unwrap();
        let secondary = map.export(256..768).unwrap();

        main.import(secondary).unwrap();

        assert_eq!(map.commit(), main.commit());
        main.check_tree();
        main.assert_records((0..768).map(|i| (i, i)));
    }

    #[test]
    fn import_same_halves() {
        let mut map: Map<u32, u32> = Map::new();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            map.insert(key, value).unwrap();
        }

        let mut main = map.export(0..512).unwrap();
        let secondary = map.export(0..512).unwrap();

        main.import(secondary).unwrap();

        assert_eq!(map.commit(), main.commit());
        main.check_tree();
        main.assert_records((0..512).map(|i| (i, i)));
    }

    #[test]
    fn import_mismatched() {
        let mut first: Map<u32, u32> = Map::new();
        let mut second: Map<u32, u32> = Map::new();

        for (key, value) in (0..128).map(|i| (i, i)) {
            first.insert(key, value).unwrap();
        }

        for (key, value) in (64..192).map(|i| (i, i)) {
            second.insert(key, value).unwrap();
        }

        let mut first_export = first.export([1]).unwrap();
        let second_export = second.export([2]).unwrap();

        assert!(first_export.import(second_export).is_err());
    }

    #[test]
    fn double_export() {
        let mut map: Map<u32, u32> = Map::new();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            map.insert(key, value).unwrap();
        }

        let mut main = map.export(0..128).unwrap();
        let secondary = map.export(128..256).unwrap();

        main.import(secondary).unwrap();

        let export = main.export(64..192).unwrap();

        assert_eq!(map.commit(), export.commit());
        export.check_tree();
        export.assert_records((64..192).map(|i| (i, i)));
    }

    #[test]
    fn serialize_empty() {
        let original: Map<u32, u32> = Map::new();
        let serialized = bincode::serialize(&original).unwrap();

        let deserialized: Map<u32, u32> = bincode::deserialize(&serialized).unwrap();

        assert_eq!(original.commit(), deserialized.commit());
        deserialized.check_tree();
        deserialized.assert_records([]);
    }

    #[test]
    fn serialize_full() {
        let mut original: Map<u32, u32> = Map::new();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            original.insert(key, value).unwrap();
        }

        let serialized = bincode::serialize(&original).unwrap();
        let deserialized: Map<u32, u32> = bincode::deserialize(&serialized).unwrap();

        assert_eq!(original.commit(), deserialized.commit());
        deserialized.check_tree();
        deserialized.assert_records((0..1024).map(|i| (i, i)));
    }

    #[test]
    fn serialize_half() {
        let mut original: Map<u32, u32> = Map::new();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            original.insert(key, value).unwrap();
        }

        let export = original.export(0..512).unwrap();
        let serialized = bincode::serialize(&export).unwrap();
        let deserialized: Map<u32, u32> = bincode::deserialize(&serialized).unwrap();

        assert_eq!(original.commit(), deserialized.commit());
        deserialized.check_tree();
        deserialized.assert_records((0..512).map(|i| (i, i)));
    }

    #[test]
    fn serialize_mislabled_small() {
        let mut original: Map<u32, u32> = Map::new();

        original.insert(3, 3).unwrap();
        original.insert(4, 4).unwrap();

        let original_commitment = original.commit();

        let root = match original.root.take() {
            Node::Internal(internal) => {
                let (left, right) = internal.children();
                Node::Internal(Internal::raw(hash::empty(), left, right))
            }
            _ => unreachable!(),
        };

        original.root.restore(root);

        let serialized = bincode::serialize(&original).unwrap();
        let deserialized: Map<u32, u32> = bincode::deserialize(&serialized).unwrap();

        assert_eq!(original_commitment, deserialized.commit());
        deserialized.check_tree();
        deserialized.assert_records([(3, 3), (4, 4)]);
    }

    #[test]
    fn serialize_flawed_small() {
        let mut original: Map<u32, u32> = Map::new();

        original.insert(3, 3).unwrap();
        original.insert(4, 4).unwrap();

        let root = match original.root.take() {
            Node::Internal(internal) => {
                let (left, right) = internal.children();
                Node::internal(right, left)
            }
            _ => unreachable!(),
        };

        original.root.restore(root);

        let serialized = bincode::serialize(&original).unwrap();
        assert!(bincode::deserialize::<Map<u32, u32>>(&serialized).is_err());
    }

    #[test]
    fn serialize_flawed_mislabled_small() {
        let mut original: Map<u32, u32> = Map::new();

        original.insert(3, 3).unwrap();
        original.insert(4, 4).unwrap();

        let root = match original.root.take() {
            Node::Internal(internal) => {
                let hash = internal.hash();
                let (left, right) = internal.children();
                Node::Internal(Internal::raw(hash, right, left))
            }
            _ => unreachable!(),
        };

        original.root.restore(root);

        let serialized = bincode::serialize(&original).unwrap();
        assert!(bincode::deserialize::<Map<u32, u32>>(&serialized).is_err());
    }

    #[test]
    fn serialize_mislabled_big() {
        let mut original: Map<u32, u32> = Map::new();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            original.insert(key, value).unwrap();
        }

        let original_commitment = original.commit();

        let root = match original.root.take() {
            Node::Internal(internal) => {
                let (left, right) = internal.children();
                let left = match left {
                    Node::Internal(internal) => {
                        let (left, right) = internal.children();
                        let left = match left {
                            Node::Internal(internal) => {
                                let (left, right) = internal.children();
                                let left = match left {
                                    Node::Internal(internal) => {
                                        let (left, right) = internal.children();
                                        Node::Internal(Internal::raw(hash::empty(), left, right))
                                    }
                                    _ => unreachable!(),
                                };
                                Node::internal(left, right)
                            }
                            _ => unreachable!(),
                        };
                        Node::internal(left, right)
                    }
                    _ => unreachable!(),
                };
                Node::internal(left, right)
            }
            _ => unreachable!(),
        };

        original.root.restore(root);

        let serialized = bincode::serialize(&original).unwrap();
        let deserialized = bincode::deserialize::<Map<u32, u32>>(&serialized).unwrap();

        assert_eq!(original_commitment, deserialized.commit());
        deserialized.check_tree();
        deserialized.assert_records((0..1024).map(|i| (i, i)));
    }

    #[test]
    fn serialize_flawed_big() {
        let mut original: Map<u32, u32> = Map::new();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            original.insert(key, value).unwrap();
        }

        let root = match original.root.take() {
            Node::Internal(internal) => {
                let (left, right) = internal.children();
                let left = match left {
                    Node::Internal(internal) => {
                        let (left, right) = internal.children();
                        let left = match left {
                            Node::Internal(internal) => {
                                let (left, right) = internal.children();
                                let left = match left {
                                    Node::Internal(internal) => {
                                        let (left, right) = internal.children();
                                        Node::internal(right, left)
                                    }
                                    _ => unreachable!(),
                                };
                                Node::internal(left, right)
                            }
                            _ => unreachable!(),
                        };
                        Node::internal(left, right)
                    }
                    _ => unreachable!(),
                };
                Node::internal(left, right)
            }
            _ => unreachable!(),
        };

        original.root.restore(root);

        let serialized = bincode::serialize(&original).unwrap();
        assert!(bincode::deserialize::<Map<u32, u32>>(&serialized).is_err());
    }

    #[test]
    fn serialize_flawed_mislabled_big() {
        let mut original: Map<u32, u32> = Map::new();

        for (key, value) in (0..1024).map(|i| (i, i)) {
            original.insert(key, value).unwrap();
        }

        let root = match original.root.take() {
            Node::Internal(internal) => {
                let (left, right) = internal.children();
                let left = match left {
                    Node::Internal(internal) => {
                        let (left, right) = internal.children();
                        let left = match left {
                            Node::Internal(internal) => {
                                let (left, right) = internal.children();
                                let left = match left {
                                    Node::Internal(internal) => {
                                        let hash = internal.hash();
                                        let (left, right) = internal.children();
                                        Node::Internal(Internal::raw(hash, right, left))
                                    }
                                    _ => unreachable!(),
                                };
                                Node::internal(left, right)
                            }
                            _ => unreachable!(),
                        };
                        Node::internal(left, right)
                    }
                    _ => unreachable!(),
                };
                Node::internal(left, right)
            }
            _ => unreachable!(),
        };

        original.root.restore(root);

        let serialized = bincode::serialize(&original).unwrap();
        assert!(bincode::deserialize::<Map<u32, u32>>(&serialized).is_err());
    }
}
