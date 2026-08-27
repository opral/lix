use serde_json::json;

#[cfg(feature = "storage-benches")]
use std::collections::HashMap;

use crate::branch::BranchHeadControlContext;
use crate::changelog::CommitId;
#[cfg(feature = "storage-benches")]
use crate::changelog::{ChangelogContext, ChangelogReader, CommitScanRequest};
use crate::commit_graph::CommitGraphContext;
#[cfg(feature = "storage-benches")]
use crate::commit_graph::CommitGraphNode;
use crate::hot_state::{HotStateExactBatchRequest, HotStateExactRowRequest, HotStateReader};
use crate::row_pk::RowPk;
use crate::storage_adapter::StorageAdapterRead;
use crate::transaction_types::{TransactionJson, TransactionWriteRow};
use crate::{GLOBAL_BRANCH_ID, LixError};

pub(crate) const CHECKPOINT_SCHEMA_KEY: &str = "lix_checkpoint";

#[cfg(feature = "storage-benches")]
const CHECKPOINT_RECORD_SCAN_PAGE_SIZE: usize = 1_024;

#[cfg(feature = "storage-benches")]
pub(crate) type CheckpointCommitRecords = HashMap<CommitId, CommitGraphNode>;

pub(crate) fn checkpoint_snapshot(commit_id: &CommitId) -> serde_json::Value {
    let commit_id = commit_id.to_string();
    json!({
        "id": commit_id.clone(),
        "commit_id": commit_id,
    })
}

pub(crate) fn checkpoint_stage_row(commit_id: &CommitId, change_id: String) -> TransactionWriteRow {
    let commit_id = commit_id.to_string();
    TransactionWriteRow {
        row_pk: Some(
            RowPk::uuid_from_canonical(&commit_id)
                .expect("checkpoint commit ID is a canonical UUID"),
        ),
        schema_key: CHECKPOINT_SCHEMA_KEY.into(),
        file_id: None,
        snapshot: Some(TransactionJson::from_value_unchecked(json!({
            "id": commit_id.clone(),
            "commit_id": commit_id,
        }))),
        metadata: None,
        origin: None,
        created_at: None,
        updated_at: None,
        global: true,
        change_id: Some(change_id),
        commit_id: None,
        untracked: false,
        branch_id: GLOBAL_BRANCH_ID.into(),
    }
}

/// Loads the private compaction cursor bound to an exact branch head.
///
/// Checkpoints are logical global rows. Branch-relative working-diff
/// baselines are control-plane state and must never be reconstructed by
/// searching checkpoint row history.
pub(crate) async fn checkpoint_commit_id_at_head<S>(
    store: S,
    branch_id: &str,
    head_commit_id: CommitId,
) -> Result<CommitId, LixError>
where
    S: StorageAdapterRead,
{
    let control = BranchHeadControlContext::new()
        .reader(store)
        .load(branch_id)
        .await?
        .ok_or_else(|| LixError::branch_not_found(branch_id, "load checkpoint cursor", "branch"))?;
    if control.head_commit_id != head_commit_id {
        return Err(LixError::new(
            LixError::CODE_TRANSACTION_CONFLICT,
            format!("branch '{branch_id}' head changed while loading its checkpoint cursor"),
        ));
    }
    control.working_diff_checkpoint_commit_id.ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("branch '{branch_id}' has no checkpoint cursor"),
        )
    })
}

/// Resolves the latest real checkpoint on the active branch's mainline.
///
/// The private working-diff cursor normally names a checkpoint, but branch
/// creation and restore can initialize it to an arbitrary source commit. A
/// cursor is therefore only a search anchor until its global checkpoint row
/// has been verified. Following first parents keeps merged checkpoints from
/// other branches out of this branch-scoped accessor.
pub(crate) async fn latest_checkpoint_commit_id_at_head<S>(
    store: S,
    hot_state: &dyn HotStateReader,
    branch_id: &str,
    head_commit_id: CommitId,
) -> Result<Option<CommitId>, LixError>
where
    S: StorageAdapterRead + Clone,
{
    let control = BranchHeadControlContext::new()
        .reader(store.clone())
        .load(branch_id)
        .await?
        .ok_or_else(|| {
            LixError::branch_not_found(branch_id, "resolve latest checkpoint", "branch")
        })?;
    if control.head_commit_id != head_commit_id {
        return Err(LixError::new(
            LixError::CODE_TRANSACTION_CONFLICT,
            format!("branch '{branch_id}' head changed while resolving its latest checkpoint"),
        ));
    }
    let Some(mut candidate) = control.working_diff_checkpoint_commit_id else {
        return Ok(None);
    };
    let mut commit_graph = CommitGraphContext::new().reader(store);

    loop {
        let row_pk = RowPk::uuid_from_canonical(&candidate.to_string()).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("checkpoint candidate '{candidate}' has an invalid row identity: {error}"),
            )
        })?;
        let markers = hot_state
            .load_exact_batch(&HotStateExactBatchRequest {
                rows: vec![HotStateExactRowRequest {
                    schema_key: CHECKPOINT_SCHEMA_KEY.to_string(),
                    branch_id: GLOBAL_BRANCH_ID.to_string(),
                    row_pk,
                    file_id: None,
                }],
                untracked: Some(false),
                ..Default::default()
            })
            .await?;
        if markers.row(0).is_some() {
            return Ok(Some(candidate));
        }

        let node = commit_graph.load_node(&candidate).await?.ok_or_else(|| {
            crate::commit_graph::missing_commit_graph_error(&candidate)
        })?;
        let Some(first_parent) = node.parent_commit_ids.first().copied() else {
            return Ok(None);
        };
        candidate = first_parent;
    }
}

#[cfg(feature = "storage-benches")]
pub(crate) async fn scan_checkpoint_commit_records<S>(
    store: S,
) -> Result<CheckpointCommitRecords, LixError>
where
    S: StorageAdapterRead,
{
    let mut reader = ChangelogContext::new().reader(store);
    let mut records = CheckpointCommitRecords::new();
    let mut start_after = None::<String>;

    loop {
        let batch = reader
            .scan_commits(CommitScanRequest {
                start_after: start_after.as_deref(),
                limit: Some(CHECKPOINT_RECORD_SCAN_PAGE_SIZE),
            })
            .await?;
        records.reserve(batch.entries.len());
        for record in batch.entries {
            records.insert(
                record.commit_id,
                CommitGraphNode {
                    commit_id: record.commit_id,
                    change_id: record.change_id(),
                    account_id: record.account_id,
                    generation: record.generation,
                    parent_commit_ids: record.parent_commit_ids,
                    base_commit_id: record.base_commit_id,
                    first_parent_jump_commit_id: record.first_parent_jump_commit_id,
                    first_parent_jump_span: record.first_parent_jump_span,
                    created_at: record.created_at,
                    touched_scope_digest: record.touched_scope_digest,
                },
            );
        }
        let Some(next) = batch.next_start_after else {
            break;
        };
        start_after = Some(next.to_string());
    }

    Ok(records)
}
