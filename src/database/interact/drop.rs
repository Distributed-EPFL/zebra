use crate::database::store::{Field, Label, Node, Store};

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

    use crate::database::interact::{apply, Batch, Operation};

    use rand::seq::IteratorRandom;
    use rand::Rng;

    use std::collections::HashSet;

    fn op_set(key: u32, value: u32) -> Operation<u32, u32> {
        Operation::set(key, value).unwrap()
    }

    fn read_labels(
        store: &mut Store<u32, u32>,
        label: Label,
        collector: &mut HashSet<Label>,
    ) {
        if !label.is_empty() {
            collector.insert(label);
        }

        match label {
            Label::Internal(..) => {
                let (left, right) = store.fetch_internal(label);
                read_labels(store, left, collector);
                read_labels(store, right, collector);
            }
            _ => {}
        }
    }

    fn check_size(store: &mut Store<u32, u32>, roots: Vec<Label>) {
        let mut labels = HashSet::new();

        for root in roots {
            read_labels(store, root, &mut labels);
        }

        assert_eq!(store.size(), labels.len());
    }

    #[tokio::test]
    async fn single() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| op_set(i, i)).collect());
        let (mut store, root, _) =
            apply::apply(store, Label::Empty, batch).await;
        check_size(&mut store, vec![root]);

        drop(&mut store, root);
        check_size(&mut store, vec![]);
    }

    #[tokio::test]
    async fn double_independent() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| op_set(i, i)).collect());
        let (mut store, first_root, _) =
            apply::apply(store, Label::Empty, batch).await;
        check_size(&mut store, vec![first_root]);

        let batch = Batch::new((128..256).map(|i| op_set(i, i)).collect());
        let (mut store, second_root, _) =
            apply::apply(store, Label::Empty, batch).await;
        check_size(&mut store, vec![first_root, second_root]);

        drop(&mut store, first_root);
        check_size(&mut store, vec![second_root]);

        drop(&mut store, second_root);
        check_size(&mut store, vec![]);
    }

    #[tokio::test]
    async fn double_same() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| op_set(i, i)).collect());
        let (mut store, first_root, _) =
            apply::apply(store, Label::Empty, batch).await;
        check_size(&mut store, vec![first_root]);

        let batch = Batch::new((0..128).map(|i| op_set(i, i)).collect());
        let (mut store, second_root, _) =
            apply::apply(store, Label::Empty, batch).await;
        check_size(&mut store, vec![first_root, second_root]);

        drop(&mut store, first_root);
        check_size(&mut store, vec![second_root]);

        drop(&mut store, second_root);
        check_size(&mut store, vec![]);
    }

    #[tokio::test]
    async fn double_overlap() {
        let store = Store::<u32, u32>::new();

        let batch = Batch::new((0..128).map(|i| op_set(i, i)).collect());
        let (mut store, first_root, _) =
            apply::apply(store, Label::Empty, batch).await;
        check_size(&mut store, vec![first_root]);

        let batch = Batch::new((64..192).map(|i| op_set(i, i)).collect());
        let (mut store, second_root, _) =
            apply::apply(store, Label::Empty, batch).await;
        check_size(&mut store, vec![first_root, second_root]);

        drop(&mut store, first_root);
        check_size(&mut store, vec![second_root]);

        drop(&mut store, second_root);
        check_size(&mut store, vec![]);
    }

    #[tokio::test]
    async fn stress() {
        let mut rng = rand::thread_rng();
        let mut roots: Vec<Label> = Vec::new();

        let mut store = Store::<u32, u32>::new();

        for _ in 0..32 {
            if rng.gen::<bool>() {
                let keys = (0..1024).choose_multiple(&mut rng, 128);
                let batch =
                    Batch::new(keys.iter().map(|&i| op_set(i, i)).collect());

                let result = apply::apply(store, Label::Empty, batch).await;
                store = result.0;
                roots.push(result.1);
            } else {
                if let Some(index) = (0..roots.len()).choose(&mut rng) {
                    drop(&mut store, roots[index]);
                    roots.remove(index);
                }
            }

            check_size(&mut store, roots.clone());
        }
    }
}
