use crate::LixError;
use crate::changelog::{ChangeId, CommitId};
use crate::common::LixTimestamp;
use crate::entity_pk::EntityPk;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitGraphChange {
    pub(crate) id: ChangeId,
    pub(crate) entity_pk: EntityPk,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot: crate::json_store::JsonSlot,
    pub(crate) metadata: crate::json_store::JsonSlot,
    pub(crate) created_at: LixTimestamp,
}

/// Parsed `lix_commit` entity from the changelog.
///
/// The graph reader projects direct changelog commit records into explicit
/// parent ids plus the commit's referenced canonical changes. A merge commit
/// points at selected existing change ids; it does not mint row/entity changes
/// for the merge itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitGraphCommit {
    pub(crate) canonical_change: CommitGraphChange,
    pub(crate) change: CommitGraphChange,
    pub(crate) commit_id: CommitId,
    pub(crate) change_ids: Vec<ChangeId>,
    pub(crate) author_account_ids: Vec<String>,
    pub(crate) parent_commit_ids: Vec<CommitId>,
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
    pub(crate) parent_commit_id: CommitId,
    pub(crate) child_commit_id: CommitId,
    pub(crate) parent_order: u32,
}

/// Filter for canonical change history from a chosen traversal start commit.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CommitGraphChangeHistoryRequest {
    pub(crate) entity_pks: Vec<EntityPk>,
    pub(crate) schema_keys: Vec<String>,
    pub(crate) file_ids: Vec<String>,
    pub(crate) min_depth: Option<u32>,
    pub(crate) max_depth: Option<u32>,
    pub(crate) include_tombstones: bool,
}

/// Canonical change observed while walking commit history from a start commit.
///
/// `start_commit_id` is the traversal anchor requested by the caller. It is not
/// necessarily a graph root or a branch head.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitGraphChangeHistoryEntry {
    pub(crate) change: CommitGraphChange,
    pub(crate) observed_commit_id: CommitId,
    pub(crate) start_commit_id: CommitId,
    pub(crate) depth: u32,
}

/// Execution-scoped reader for commit graph facts.
///
/// SQL surfaces consume this trait so they depend on graph semantics, not on
/// changelog storage or traversal details.
#[async_trait::async_trait]
pub(crate) trait CommitGraphReader: Send + Sync {
    async fn load_commit(
        &mut self,
        commit_id: &CommitId,
    ) -> Result<Option<CommitGraphCommit>, LixError>;

    async fn reachable_commits(
        &mut self,
        head_commit_id: &CommitId,
    ) -> Result<Vec<ReachableCommitGraphCommit>, LixError>;

    async fn change_history_from_commit(
        &mut self,
        start_commit_id: &CommitId,
        request: &CommitGraphChangeHistoryRequest,
    ) -> Result<Vec<CommitGraphChangeHistoryEntry>, LixError>;
}
