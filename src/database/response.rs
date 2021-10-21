use crate::{
    common::store::Field,
    database::{
        interact::{Action, Batch},
        DatabaseQuery, Tid,
    },
};

pub struct Response<Key: Field, Value: Field> {
    tid: Tid,
    batch: Batch<Key, Value>,
}

impl<Key, Value> Response<Key, Value>
where
    Key: Field,
    Value: Field,
{
    pub(crate) fn new(tid: Tid, batch: Batch<Key, Value>) -> Self {
        Response { tid, batch }
    }

    pub fn get(&self, query: &DatabaseQuery) -> Option<&Value> {
        assert_eq!(
            query.tid, self.tid,
            "called `Response::get` with a foreign `Query`"
        );

        let index = self
            .batch
            .operations()
            .binary_search_by_key(&query.path, |operation| operation.path)
            .unwrap();
        match &self.batch.operations()[index].action {
            Action::Get(Some(holder)) => Some(holder),
            Action::Get(None) => None,
            _ => unreachable!(),
        }
    }
}
