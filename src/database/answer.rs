use crate::{common::store::Field, database::store::Node};

use std::vec::Vec;

// Documentation links
#[allow(unused_imports)]
use crate::database::{Question, Receiver, Sender};

/// A [`Sender`]'s reply to a [`Question`] from a [`Receiver`]
///
/// See the [`Sender`] and [`Receiver`] documentation for more details.
///
/// [`Sender`]: crate::database::Sender
/// [`Receiver`]: crate::database::Receiver
/// [`Question`]: crate::database::Question

#[derive(Debug, Eq, PartialEq)]
pub struct Answer<Key: Field, Value: Field>(pub(crate) Vec<Node<Key, Value>>);
