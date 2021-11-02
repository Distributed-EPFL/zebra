use crate::database::store::Label;

use serde::{Deserialize, Serialize};

use std::vec::Vec;

// Documentation links
#[allow(unused_imports)]
use crate::database::{TableAnswer, TableReceiver, TableSender};

/// A [`TableReceiver`]'s query for a [`TableSender`], to be replied to with an [`Answer`].
///
/// See the [`TableSender`] and [`TableReceiver`] documentation for more details.
///
/// [`TableSender`]: crate::database::TableSender
/// [`TableReceiver`]: crate::database::TableReceiver
/// [`Answer`]: crate::database::Question

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Question(pub(crate) Vec<Label>);
