use crate::{
    common::store::Field,
    database::store::{Label, Node, Store},
};

pub(crate) fn drop<Key, Value>(store: &mut Store<Key, Value>, label: Label)
where
    Key: Field,
    Value: Field,
{
    match store.decref(label, false) {
        Some(Node::Internal(left, right)) => {
            drop(store, left);
            drop(store, right);
        }
        _ => (),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::database::interact::{apply, Batch};

    use rand::{seq::IteratorRandom, Rng};

    #[test]
    fn single() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (mut store, root, _) = apply::apply(store, Label::Empty, batch);
        store.check_leaks([root]);

        drop(&mut store, root);
        store.check_leaks([]);
    }

    #[test]
    fn double_independent() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (mut store, first_root, _) = apply::apply(store, Label::Empty, batch);
        store.check_leaks([first_root]);

        let batch = Batch::new((128..256).map(|i| set!(i, i)).collect());
        let (mut store, second_root, _) = apply::apply(store, Label::Empty, batch);
        store.check_leaks([first_root, second_root]);

        drop(&mut store, first_root);
        store.check_leaks([second_root]);

        drop(&mut store, second_root);
        store.check_leaks([]);
    }

    #[test]
    fn double_same() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (mut store, first_root, _) = apply::apply(store, Label::Empty, batch);
        store.check_leaks([first_root]);

        let batch = Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (mut store, second_root, _) = apply::apply(store, Label::Empty, batch);
        store.check_leaks([first_root, second_root]);

        drop(&mut store, first_root);
        store.check_leaks([second_root]);

        drop(&mut store, second_root);
        store.check_leaks([]);
    }

    #[test]
    fn double_overlap() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| set!(i, i)).collect());
        let (mut store, first_root, _) = apply::apply(store, Label::Empty, batch);
        store.check_leaks([first_root]);

        let batch = Batch::new((64..192).map(|i| set!(i, i)).collect());
        let (mut store, second_root, _) = apply::apply(store, Label::Empty, batch);
        store.check_leaks([first_root, second_root]);

        drop(&mut store, first_root);
        store.check_leaks([second_root]);

        drop(&mut store, second_root);
        store.check_leaks([]);
    }

    #[test]
    fn stress() {
        let mut rng = rand::thread_rng();
        let mut roots: Vec<Label> = Vec::new();

        let mut store = Store::<u32, u32>::new();

        for _ in 0..32 {
            if rng.gen::<bool>() {
                let keys = (0..1024).choose_multiple(&mut rng, 128);
                let batch = Batch::new(keys.iter().map(|&i| set!(i, i)).collect());

                let result = apply::apply(store, Label::Empty, batch);
                store = result.0;
                roots.push(result.1);
            } else {
                if let Some(index) = (0..roots.len()).choose(&mut rng) {
                    drop(&mut store, roots[index]);
                    roots.remove(index);
                }
            }

            store.check_leaks(roots.clone());
        }
    }
}
