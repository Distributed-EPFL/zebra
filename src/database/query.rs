use crate::database::tree::Path;

/// Used with a [`Response`] to obtain the result of a particular
/// operation in a [`Transaction`].
///
/// A `Query` is only usable with the `Response` obtained from executing
/// the `Transaction` it was associated with.
///
/// [`Response`]: crate::database::Response
/// [`Transaction`]: crate::database::Transaction

pub struct Query {
    pub(crate) tid: usize,
    pub(crate) path: Path,
}
