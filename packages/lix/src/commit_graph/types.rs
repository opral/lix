use std::sync::Arc;

use crate::LixError;
use crate::changelog::{ChangeId, CommitId, CommitRecord};
use crate::common::{ExactValue, LixTimestamp};
use crate::row_pk::RowPk;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitGraphChange {
    pub(crate) id: ChangeId,
    pub(crate) account_id: String,
    pub(crate) row_pk: RowPk,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot: crate::json_store::JsonSlot,
    pub(crate) metadata: crate::json_store::JsonSlot,
    pub(crate) created_at: LixTimestamp,
    pub(crate) origin_key: Option<String>,
}

/// One topology-first commit fact.
///
/// Nodes contain exactly the manifest fields needed by graph algorithms and
/// derived commit surfaces. Row/change payloads are loaded separately only
/// by history APIs that explicitly request them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitGraphNode {
    pub(crate) commit_id: CommitId,
    pub(crate) generation: u64,
    pub(crate) parent_commit_ids: Vec<CommitId>,
}

impl ExactValue<CommitId> for CommitGraphNode {
    fn matches_exact_key(&self, key: &CommitId) -> bool {
        self.commit_id == *key
    }
}

/// Commit reachable from a requested graph head.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReachableCommitGraphNode {
    pub(crate) commit: CommitGraphNode,
    pub(crate) depth: u32,
}

/// Filter for canonical change history from a chosen traversal start commit.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CommitGraphChangeHistoryRequest {
    pub(crate) row_pks: Vec<RowPk>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitGraphHistory {
    pub(crate) entries: Vec<CommitGraphChangeHistoryEntry>,
    pub(crate) reachable_nodes: Arc<[ReachableCommitGraphNode]>,
}

/// Execution-scoped reader for commit graph facts.
///
/// SQL surfaces consume this trait so they depend on graph semantics, not on
/// changelog storage or traversal details.
#[async_trait::async_trait]
pub(crate) trait CommitGraphReader: Send + Sync {
    async fn load_node(
        &mut self,
        commit_id: &CommitId,
    ) -> Result<Option<CommitGraphNode>, LixError>;

    async fn reachable_nodes(
        &mut self,
        head_commit_id: &CommitId,
    ) -> Result<Arc<[ReachableCommitGraphNode]>, LixError>;

    /// Returns commit identities pinned by authenticated ForkTree snapshot
    /// selectors. Recovery, checkpoint, undo, redo, and tombstone selectors
    /// are physical roots; no legacy checkpoint JSON row is consulted.
    async fn snapshot_roots(&mut self) -> Result<Vec<(String, CommitId)>, LixError>;

    /// Loads semantic commit metadata through this reader's retained
    /// authenticated view. Topology consumers must not hydrate this payload
    /// into `CommitGraphNode`.
    async fn load_commit_records(
        &mut self,
        commit_ids: &[CommitId],
    ) -> Result<Vec<Option<CommitRecord>>, LixError>;

    async fn change_history_from_commit(
        &mut self,
        start_commit_id: &CommitId,
        request: &CommitGraphChangeHistoryRequest,
    ) -> Result<CommitGraphHistory, LixError>;
}
