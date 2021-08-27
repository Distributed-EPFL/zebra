use drop::crypto::hash::HashError as DropHashError;

use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum MapError {
    #[snafu(display("failed to hash field: {}", source))]
    HashError { source: DropHashError },
    #[snafu(display("attempted to operate on an unknown branch"))]
    BranchUnknown,
    #[snafu(display("attempted to import incompatible map"))]
    MapIncompatible,
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum TopologyError {
    #[snafu(display("children violate compactness"))]
    CompactnessViolation,
    #[snafu(display("leaf outside of its key path"))]
    PathViolation,
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum DeserializeError {
    #[snafu(display("flawed topology: {}", source))]
    FlawedTopology { source: TopologyError },
}
