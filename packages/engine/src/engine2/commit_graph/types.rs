use crate::engine2::changelog::CanonicalChange;
use crate::LixError;

/// Parsed `lix_commit` entity from the changelog.
///
/// Commits are stored as ordinary canonical changes. The graph reader parses
/// their snapshot so traversal code can work with explicit parent/member ids.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitGraphCommit {
    pub(crate) change: CanonicalChange,
    pub(crate) commit_id: String,
    pub(crate) change_set_id: String,
    pub(crate) change_ids: Vec<String>,
    pub(crate) parent_commit_ids: Vec<String>,
}

/// Commit reachable from a requested graph head.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReachableCommitGraphCommit {
    pub(crate) commit: CommitGraphCommit,
    pub(crate) depth: u32,
}

/// Derived parent/child edge between two commit entities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitGraphEdge {
    pub(crate) parent_commit_id: String,
    pub(crate) child_commit_id: String,
}

/// Derived change-set row for a commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitGraphChangeSet {
    pub(crate) id: String,
    pub(crate) commit_id: String,
}

/// Derived membership row for a commit's change set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitGraphChangeSetElement {
    pub(crate) change_set_id: String,
    pub(crate) change: CanonicalChange,
}

/// Execution-scoped reader for commit graph facts.
///
/// SQL surfaces consume this trait so they depend on graph semantics, not on
/// changelog storage or traversal details.
#[async_trait::async_trait]
pub(crate) trait CommitGraphReader: Send + Sync {
    async fn load_commit(&mut self, commit_id: &str)
        -> Result<Option<CommitGraphCommit>, LixError>;

    async fn all_commits(&mut self) -> Result<Vec<CommitGraphCommit>, LixError>;

    async fn reachable_commits(
        &mut self,
        head_commit_id: &str,
    ) -> Result<Vec<ReachableCommitGraphCommit>, LixError>;

    fn commit_edges(&self, commits: &[CommitGraphCommit]) -> Vec<CommitGraphEdge>;

    fn change_sets(&self, commits: &[CommitGraphCommit]) -> Vec<CommitGraphChangeSet>;

    async fn change_set_elements(
        &mut self,
        commits: &[CommitGraphCommit],
    ) -> Result<Vec<CommitGraphChangeSetElement>, LixError>;
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
