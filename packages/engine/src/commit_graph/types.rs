use std::collections::BTreeSet;

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
    pub(crate) origin_key: Option<String>,
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

/// Lightweight commit metadata for graph walks that do not inspect members.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitGraphCommitRecord {
    pub(crate) commit_id: CommitId,
    pub(crate) parent_commit_ids: Vec<CommitId>,
    pub(crate) created_at: LixTimestamp,
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

    async fn load_commit_record(
        &mut self,
        commit_id: &CommitId,
    ) -> Result<Option<CommitGraphCommitRecord>, LixError> {
        Ok(self
            .load_commit(commit_id)
            .await?
            .map(|commit| CommitGraphCommitRecord {
                commit_id: commit.commit_id,
                parent_commit_ids: commit.parent_commit_ids,
                created_at: commit.canonical_change.created_at,
            }))
    }

    async fn reachable_commits(
        &mut self,
        head_commit_id: &CommitId,
    ) -> Result<Vec<ReachableCommitGraphCommit>, LixError>;

    /// Resolves the single merge base used by Lix's three-way merge policy.
    ///
    /// This lives on the object-safe reader trait so SQL providers can apply
    /// the same graph policy as session-owned merge operations. A criss-cross
    /// history with multiple best bases remains deliberately unsupported.
    async fn merge_base(
        &mut self,
        left_commit_id: &CommitId,
        right_commit_id: &CommitId,
    ) -> Result<CommitGraphCommit, LixError> {
        let left_reachable = self.reachable_commits(left_commit_id).await?;
        let right_reachable = self.reachable_commits(right_commit_id).await?;
        let right_ids = right_reachable
            .iter()
            .map(|reachable| reachable.commit.commit_id)
            .collect::<BTreeSet<_>>();
        let common_ids = left_reachable
            .iter()
            .filter(|reachable| right_ids.contains(&reachable.commit.commit_id))
            .map(|reachable| reachable.commit.commit_id)
            .collect::<BTreeSet<_>>();

        // A common ancestor is superseded if a common child points at it.
        // Direct-parent inspection is sufficient because the shared ancestor
        // set is ancestor-closed.
        let mut superseded = BTreeSet::new();
        for reachable in &left_reachable {
            if !common_ids.contains(&reachable.commit.commit_id) {
                continue;
            }
            for parent_commit_id in &reachable.commit.parent_commit_ids {
                if common_ids.contains(parent_commit_id) {
                    superseded.insert(*parent_commit_id);
                }
            }
        }
        let mut ancestors = left_reachable
            .into_iter()
            .filter(|reachable| {
                common_ids.contains(&reachable.commit.commit_id)
                    && !superseded.contains(&reachable.commit.commit_id)
            })
            .map(|reachable| reachable.commit)
            .collect::<Vec<_>>();
        ancestors.sort_by_key(|commit| commit.commit_id);
        match ancestors.as_slice() {
            [] => Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "commit_graph found no common history between '{left_commit_id}' and '{right_commit_id}'"
                ),
            )),
            [base] => Ok(base.clone()),
            _ => Err(LixError::ambiguous_merge_base(
                left_commit_id,
                right_commit_id,
                ancestors
                    .iter()
                    .map(|ancestor| ancestor.commit_id.to_string())
                    .collect(),
            )),
        }
    }

    async fn change_history_from_commit(
        &mut self,
        start_commit_id: &CommitId,
        request: &CommitGraphChangeHistoryRequest,
    ) -> Result<Vec<CommitGraphChangeHistoryEntry>, LixError>;
}
