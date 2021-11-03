use crate::{
    common::store::Field,
    database::{Question, Table, TableReceiver},
};

pub enum TableStatus<Key: Field, Value: Field> {
    Complete(Table<Key, Value>),
    Incomplete(TableReceiver<Key, Value>, Question),
}
