use crate::database::store::Label;

use std::vec::Vec;

#[derive(Debug, Eq, PartialEq)]
pub struct Question(pub(crate) Vec<Label>);
