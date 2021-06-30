use crate::database::store::{Field, Store};

use std::sync::{Arc, Condvar, Mutex};

struct Inner<Key: Field, Value: Field> {
    store: Mutex<Option<Store<Key, Value>>>,
    condvar: Condvar,
}

pub(crate) struct Cell<Key: Field, Value: Field>(Arc<Inner<Key, Value>>);

impl<Key, Value> Inner<Key, Value>
where
    Key: Field,
    Value: Field,
{
    fn new(store: Store<Key, Value>) -> Self {
        Inner {
            store: Mutex::new(Some(store)),
            condvar: Condvar::new(),
        }
    }

    fn take(self: &Arc<Self>) -> Store<Key, Value> {
        let guard = self.store.lock().unwrap();
        let mut guard = (*self)
            .condvar
            .wait_while(guard, |store| store.is_none())
            .unwrap();
        guard.take().unwrap()
    }

    fn restore(self: &Arc<Self>, store: Store<Key, Value>) {
        *self.store.lock().unwrap() = Some(store);
        self.condvar.notify_one();
    }
}

impl<Key, Value> Cell<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub fn new(store: Store<Key, Value>) -> Self {
        Cell(Arc::new(Inner::new(store)))
    }

    pub fn take(&self) -> Store<Key, Value> {
        self.0.take()
    }

    pub fn restore(&self, store: Store<Key, Value>) {
        self.0.restore(store);
    }
}

impl<Key, Value> Clone for Cell<Key, Value>
where
    Key: Field,
    Value: Field,
{
    fn clone(&self) -> Self {
        Cell(self.0.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::thread;
    use std::thread::JoinHandle;
    use std::time::Duration;

    #[test]
    fn stress() {
        let cell: Cell<u32, u32> = Cell::new(Store::new());

        let threads: Vec<JoinHandle<()>> = (0..32)
            .map(|_| {
                let cell = cell.clone();
                thread::spawn(move || {
                    for _ in 0..10 {
                        let store = cell.take();
                        thread::sleep(Duration::from_millis(1));
                        cell.restore(store);
                    }
                })
            })
            .collect();

        for thread in threads {
            thread.join().unwrap();
        }
    }
}
