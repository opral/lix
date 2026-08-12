use std::sync::Arc;

use crate::LixError;
use crate::changelog::{ChangeId, CommitId};
use crate::common::{ExactValue, LixTimestamp};
use crate::entity_pk::EntityPk;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitGraphChange {
    pub(crate) id: ChangeId,
    pub(crate) account_id: String,
    pub(crate) entity_pk: EntityPk,
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
/// derived commit surfaces. Entity/change payloads are loaded separately only
/// by history APIs that explicitly request them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitGraphNode {
    pub(crate) commit_id: CommitId,
    pub(crate) change_id: ChangeId,
    pub(crate) account_id: String,
    pub(crate) generation: u64,
    pub(crate) parent_commit_ids: Vec<CommitId>,
    pub(crate) first_parent_jump_commit_id: CommitId,
    pub(crate) first_parent_jump_span: u64,
    pub(crate) created_at: LixTimestamp,
    /// Collection scopes this commit's delta has members in.
    ///
    /// This node is already loaded (and cached) once per reached commit by the
    /// traversal itself, so carrying the digest here makes the per-commit
    /// history membership test free of any additional point read — the same
    /// bargain `first_parent_jump_commit_id` already makes for level-ancestor
    /// queries.
    pub(crate) touched_scope_digest: crate::changelog::CommitTouchedScopeDigest,
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

/// Derived parent/child edge between two commit entities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitGraphEdge {
    pub(crate) parent_commit_id: CommitId,
    pub(crate) child_commit_id: CommitId,
    pub(crate) parent_order: u32,
}

pub(crate) fn commit_edges(commits: &[CommitGraphNode]) -> Vec<CommitGraphEdge> {
    commits
        .iter()
        .flat_map(|commit| {
            commit
                .parent_commit_ids
                .iter()
                .enumerate()
                .map(|(parent_order, parent_commit_id)| CommitGraphEdge {
                    parent_commit_id: *parent_commit_id,
                    child_commit_id: commit.commit_id,
                    parent_order: parent_order as u32,
                })
        })
        .collect()
}

/// Filter for canonical change history from a chosen traversal start commit.
///
/// `max_depth` and `limit` are traversal bounds, not post-filters. The reader
/// stops walking the commit graph as soon as neither can produce another row,
/// so a narrow history query never pays for the unreachable remainder of the
/// graph.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CommitGraphChangeHistoryRequest {
    pub(crate) entity_pks: Vec<EntityPk>,
    pub(crate) schema_keys: Vec<String>,
    pub(crate) file_ids: Vec<String>,
    pub(crate) min_depth: Option<u32>,
    pub(crate) max_depth: Option<u32>,
    pub(crate) include_tombstones: bool,
    /// Maximum number of history rows the caller will consume.
    ///
    /// Only set this when the caller returns the produced entries 1:1 as its
    /// output rows. Row-shaping surfaces must leave it unset because they can
    /// collapse or drop entries after the graph read.
    pub(crate) limit: Option<usize>,
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

    async fn change_history_from_commit(
        &mut self,
        start_commit_id: &CommitId,
        request: &CommitGraphChangeHistoryRequest,
    ) -> Result<CommitGraphHistory, LixError>;
}
