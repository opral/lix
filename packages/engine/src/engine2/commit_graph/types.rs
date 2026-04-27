use crate::engine2::changelog::CanonicalChange;

/// Parsed `lix_commit` entity from the changelog.
///
/// Commits are stored as ordinary canonical changes. The graph reader parses
/// their snapshot so traversal code can work with explicit parent/member ids.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitGraphCommit {
    pub(crate) change: CanonicalChange,
    pub(crate) commit_id: String,
    pub(crate) change_ids: Vec<String>,
    pub(crate) parent_commit_ids: Vec<String>,
}

/// Canonical entity selected by resolving the commit graph at a commit head.
///
/// The changelog fact remains unchanged. The graph reader adds the commit that
/// made the fact visible at this head plus its distance from the requested
/// head, so tracked_state can materialize serving rows without knowing graph
/// traversal rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitGraphEntity {
    pub(crate) change: CanonicalChange,
    pub(crate) source_commit_id: String,
    pub(crate) depth: u32,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}
