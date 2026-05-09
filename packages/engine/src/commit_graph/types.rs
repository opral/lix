use crate::commit_store::Change;
use crate::entity_identity::EntityIdentity;
use crate::LixError;

/// Parsed `lix_commit` entity from the changelog.
///
/// Commits are stored as ordinary canonical changes. The graph reader parses
/// their snapshot so traversal code can work with explicit parent ids and the
/// ordered canonical changes introduced relative to the first parent. A merge
/// commit may reference existing changes from another parent instead of owning
/// newly minted copies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitGraphCommit {
    pub(crate) canonical_change: Change,
    pub(crate) change: Change,
    pub(crate) commit_id: String,
    pub(crate) change_ids: Vec<String>,
    pub(crate) author_account_ids: Vec<String>,
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
    pub(crate) parent_order: u32,
}

/// Filter for canonical change history from a chosen traversal start commit.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CommitGraphChangeHistoryRequest {
    pub(crate) entity_ids: Vec<EntityIdentity>,
    pub(crate) schema_keys: Vec<String>,
    pub(crate) file_ids: Vec<String>,
    pub(crate) min_depth: Option<u32>,
    pub(crate) max_depth: Option<u32>,
    pub(crate) include_tombstones: bool,
}

/// Canonical change observed while walking commit history from a start commit.
///
/// `start_commit_id` is the traversal anchor requested by the caller. It is not
/// necessarily a graph root or a version head.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitGraphChangeHistoryEntry {
    pub(crate) change: Change,
    pub(crate) observed_commit_id: String,
    pub(crate) start_commit_id: String,
    pub(crate) depth: u32,
}

/// Execution-scoped reader for commit graph facts.
///
/// SQL surfaces consume this trait so they depend on graph semantics, not on
/// changelog storage or traversal details.
#[allow(dead_code)]
#[async_trait::async_trait]
pub(crate) trait CommitGraphReader: Send + Sync {
    #[allow(dead_code)]
    async fn load_commit(&mut self, commit_id: &str)
        -> Result<Option<CommitGraphCommit>, LixError>;

    async fn all_commits(&mut self) -> Result<Vec<CommitGraphCommit>, LixError>;

    async fn reachable_commits(
        &mut self,
        head_commit_id: &str,
    ) -> Result<Vec<ReachableCommitGraphCommit>, LixError>;

    /// Returns the best common ancestors shared by two commit heads.
    ///
    /// This is intentionally not called "lowest common ancestor": commit
    /// history is a DAG, not a tree, and some histories have multiple equally
    /// good common ancestors. Merge policy can require exactly one base later.
    #[allow(dead_code)]
    async fn best_common_ancestors(
        &mut self,
        left_commit_id: &str,
        right_commit_id: &str,
    ) -> Result<Vec<CommitGraphCommit>, LixError>;

    /// Resolves the single commit base to use for a three-way merge.
    ///
    /// This is merge policy, not raw graph math: no common history and multiple
    /// best common ancestors are both errors until merge has explicit support
    /// for those cases.
    #[allow(dead_code)]
    async fn merge_base(
        &mut self,
        left_commit_id: &str,
        right_commit_id: &str,
    ) -> Result<CommitGraphCommit, LixError>;

    fn commit_edges(&self, commits: &[CommitGraphCommit]) -> Vec<CommitGraphEdge>;

    async fn change_history_from_commit(
        &mut self,
        start_commit_id: &str,
        request: &CommitGraphChangeHistoryRequest,
    ) -> Result<Vec<CommitGraphChangeHistoryEntry>, LixError>;
}

/// Canonical entity selected by resolving the commit graph at a commit head.
///
/// The changelog fact remains unchanged. The graph reader adds the commit that
/// made the fact visible at this head plus its distance from the requested
/// head, so tracked_state can materialize serving rows without knowing graph
/// traversal rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitGraphEntity {
    pub(crate) change: Change,
    pub(crate) source_commit_id: String,
    pub(crate) depth: u32,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}
