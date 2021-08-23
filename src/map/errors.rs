use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum MapError {
    #[snafu(display("attempt to operate on an unknown branch"))]
    BranchUnknown,
}
