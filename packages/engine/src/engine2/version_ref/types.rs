/// Current changelog head for a version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VersionHead {
    pub(crate) version_id: String,
    pub(crate) commit_id: String,
}
