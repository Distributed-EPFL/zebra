use crate::{common::store::Field, database::store::Node};

use serde::{Deserialize, Serialize};

use std::vec::Vec;

// Documentation links
#[allow(unused_imports)]
use crate::database::{Question, TableReceiver, TableSender};

/// A [`TableSender`]'s reply to a [`Question`] from a [`TableReceiver`]
///
/// See the [`TableSender`] and [`TableReceiver`] documentation for more details.
///
/// [`TableSender`]: crate::database::TableSender
/// [`TableReceiver`]: crate::database::TableReceiver
/// [`Question`]: crate::database::Question

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TableAnswer<Key: Field, Value: Field>(pub(crate) Vec<Node<Key, Value>>);
