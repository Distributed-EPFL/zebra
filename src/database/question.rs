use crate::database::store::Label;

use std::vec::Vec;

// Documentation links
#[allow(unused_imports)]
use crate::database::{Answer, Receiver, Sender};

/// A [`Receiver`]'s query for a [`Sender`], to be replied to with an [`Answer`].
///
/// See the [`Sender`] and [`Receiver`] documentation for more details.
///
/// [`Sender`]: crate::database::Sender
/// [`Receiver`]: crate::database::Receiver
/// [`Answer`]: crate::database::Question

#[derive(Debug, Eq, PartialEq)]
pub struct Question(pub(crate) Vec<Label>);
