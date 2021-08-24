use drop::crypto::hash::HashError as DropHashError;

use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum MapError {
    #[snafu(display("failed to hash field: {}", source))]
    HashError { source: DropHashError },
    #[snafu(display("attempt to operate on an unknown branch"))]
    BranchUnknown,
}
