use std::collections::BTreeSet;

use serde_json::json;

use crate::LixError;
use crate::changelog::CommitId;
use crate::commit_graph::{CommitGraphChangeHistoryRequest, CommitGraphReader};
use crate::entity_pk::EntityPk;
use crate::transaction::types::{TransactionJson, TransactionWriteRow};

pub(crate) const CHECKPOINT_MARKER_SCHEMA_KEY: &str = "lix_checkpoint_marker";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckpointHistoryEntry {
    pub(crate) commit_id: CommitId,
    pub(crate) created_at: String,
    pub(crate) depth: u32,
}

pub(crate) fn checkpoint_marker_stage_row(branch_id: &str) -> TransactionWriteRow {
    TransactionWriteRow {
        entity_pk: Some(EntityPk::single(branch_id)),
        schema_key: CHECKPOINT_MARKER_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot: Some(TransactionJson::from_value_unchecked(json!({
            "branch_id": branch_id,
        }))),
        metadata: None,
        origin: None,
        created_at: None,
        updated_at: None,
        global: false,
        change_id: None,
        commit_id: None,
        untracked: false,
        branch_id: branch_id.to_string(),
    }
}

/// Returns checkpoints on the first-parent history of `head`, newest first.
///
/// The graph root is also an implicit checkpoint. That gives repositories
/// created before checkpoint markers existed the same useful baseline as new
/// repositories, whose initial commit carries an explicit marker.
pub(crate) async fn checkpoint_history_from_head(
    reader: &mut dyn CommitGraphReader,
    head: &CommitId,
) -> Result<Vec<CheckpointHistoryEntry>, LixError> {
    let marker_commits = reader
        .change_history_from_commit(
            head,
            &CommitGraphChangeHistoryRequest {
                schema_keys: vec![CHECKPOINT_MARKER_SCHEMA_KEY.to_string()],
                include_tombstones: true,
                ..CommitGraphChangeHistoryRequest::default()
            },
        )
        .await?
        .into_iter()
        .map(|entry| entry.observed_commit_id)
        .collect::<BTreeSet<_>>();

    let mut checkpoints = Vec::new();
    let mut current = Some(*head);
    let mut depth = 0_u32;
    let mut visited = BTreeSet::new();
    while let Some(commit_id) = current {
        if !visited.insert(commit_id) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "cycle encountered while walking checkpoint first-parent history",
            ));
        }
        let commit = reader.load_commit(&commit_id).await?.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("checkpoint history references missing commit '{commit_id}'"),
            )
        })?;
        let is_root = commit.parent_commit_ids.is_empty();
        if is_root || marker_commits.contains(&commit_id) {
            checkpoints.push(CheckpointHistoryEntry {
                commit_id,
                created_at: commit.canonical_change.created_at.to_string(),
                depth,
            });
        }
        current = commit.parent_commit_ids.first().copied();
        depth = depth.checked_add(1).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "checkpoint history depth overflow",
            )
        })?;
    }
    Ok(checkpoints)
}
