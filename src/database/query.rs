use crate::common::tree::Path;

/// Used with a [`TableResponse`] to obtain the result of a particular
/// operation in a [`Transaction`].
///
/// A `Query` is only usable with the `Response` obtained from executing
/// the `Transaction` it was associated with.
///
/// [`TableResponse`]: crate::database::TableResponse
/// [`Transaction`]: crate::database::TableTransaction

pub struct Query {
    pub(crate) tid: usize,
    pub(crate) path: Path,
}
