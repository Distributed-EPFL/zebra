use crate::{
    common::{store::Field, tree::Path, Commitment},
    database::{
        interact::{apply, drop, export, Batch},
        store::{Cell, Label},
    },
    map::store::Node as MapNode,
};

use oh_snap::Snap;

pub(crate) struct Handle<Key: Field, Value: Field> {
    pub cell: Cell<Key, Value>,
    pub root: Label,
}

impl<Key, Value> Handle<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub fn empty(cell: Cell<Key, Value>) -> Self {
        Handle {
            cell,
            root: Label::Empty,
        }
    }

    pub fn new(cell: Cell<Key, Value>, root: Label) -> Self {
        Handle { cell, root }
    }

    pub fn commit(&self) -> Commitment {
        self.root.hash().into()
    }

    pub async fn apply(
        &mut self,
        batch: Batch<Key, Value>,
    ) -> Batch<Key, Value> {
        let store = self.cell.take();

        let (store, root, batch) = apply::apply(store, self.root, batch).await;
        self.root = root;

        self.cell.restore(store);
        batch
    }

    pub async fn export(&mut self, paths: Snap<Path>) -> MapNode<Key, Value>
    where
        Key: Clone,
        Value: Clone,
    {
        let store = self.cell.take();
        let (store, root) = export::export(store, self.root, paths).await;
        self.cell.restore(store);

        root
    }
}

impl<Key, Value> Clone for Handle<Key, Value>
where
    Key: Field,
    Value: Field,
{
    fn clone(&self) -> Self {
        let mut store = self.cell.take();
        store.incref(self.root);
        self.cell.restore(store);

        Handle {
            cell: self.cell.clone(),
            root: self.root,
        }
    }
}

impl<Key, Value> Drop for Handle<Key, Value>
where
    Key: Field,
    Value: Field,
{
    fn drop(&mut self) {
        let mut store = self.cell.take();
        drop::drop(&mut store, self.root);
        self.cell.restore(store);
    }
}
