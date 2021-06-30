use crate::database::tree::Path;

pub struct Query {
    pub(crate) tid: usize,
    pub(crate) path: Path,
}
