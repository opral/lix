use crate::LixError;
use crate::changelog::CommitId;
use crate::forktree::{CoherentView, ForkTreeReadFacade};
use crate::storage_adapter::StorageAdapterRead;
use crate::tracked_state::{
    TrackedStateDiff, TrackedStateDiffEntry, TrackedStateDiffIdentity, TrackedStateDiffKind,
    TrackedStateDiffRow, TrackedStateMergePlan, TrackedStatePayloadBatch, plan_merge,
};
use std::collections::BTreeSet;

use super::conflicts::MergeConflictBatch;
use super::stats::{MergeStats, stats_from_diff, stats_from_plan};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergeOutcome {
    AlreadyUpToDate,
    FastForward,
    MergeCommitted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MergeCommits {
    pub(crate) base_commit_id: CommitId,
    pub(crate) target_commit_id: CommitId,
    pub(crate) source_commit_id: CommitId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MergeAnalysis {
    pub(crate) outcome: MergeOutcome,
    pub(crate) commits: MergeCommits,
    pub(crate) source_diff: TrackedStateDiff,
    pub(crate) target_diff: TrackedStateDiff,
    pub(crate) stats: MergeStats,
    pub(crate) merge_plan: Option<TrackedStateMergePlan>,
}

impl MergeAnalysis {
    pub(crate) fn merge_plan(&self) -> Option<&TrackedStateMergePlan> {
        self.merge_plan.as_ref()
    }

    pub(crate) fn conflict_batch(&self) -> Option<MergeConflictBatch<'_>> {
        self.merge_plan.as_ref().map(MergeConflictBatch::from_plan)
    }
}

pub(crate) async fn analyze<S>(
    facade: &ForkTreeReadFacade<S>,
    branch_id: &str,
    commits: MergeCommits,
) -> Result<MergeAnalysis, LixError>
where
    S: StorageAdapterRead,
{
    let view = facade.branch(branch_id).await?;
    let mut source_diff =
        load_forktree_diff(&view, commits.base_commit_id, commits.source_commit_id).await?;
    let mut target_diff = if commits.base_commit_id == commits.source_commit_id
        || commits.base_commit_id == commits.target_commit_id
    {
        TrackedStateDiff::default()
    } else {
        load_forktree_diff(&view, commits.base_commit_id, commits.target_commit_id).await?
    };
    exclude_internal_checkpoint_markers(&mut source_diff);
    exclude_internal_checkpoint_markers(&mut target_diff);

    let outcome = if commits.base_commit_id == commits.source_commit_id {
        MergeOutcome::AlreadyUpToDate
    } else if commits.base_commit_id == commits.target_commit_id {
        MergeOutcome::FastForward
    } else {
        MergeOutcome::MergeCommitted
    };

    let merge_plan = if outcome == MergeOutcome::MergeCommitted {
        let payloads = TrackedStatePayloadBatch::default();
        Some(plan_merge(&target_diff, &source_diff, &payloads)?)
    } else {
        None
    };

    let stats = match outcome {
        MergeOutcome::AlreadyUpToDate => MergeStats::default(),
        MergeOutcome::FastForward => stats_from_diff(&source_diff),
        MergeOutcome::MergeCommitted => merge_plan
            .as_ref()
            .map(|plan| stats_from_plan(plan, &source_diff))
            .transpose()?
            .unwrap_or_default(),
    };

    Ok(MergeAnalysis {
        outcome,
        commits,
        source_diff,
        target_diff,
        stats,
        merge_plan,
    })
}

async fn load_forktree_diff<S>(
    view: &CoherentView<&S>,
    base_commit_id: CommitId,
    side_commit_id: CommitId,
) -> Result<TrackedStateDiff, LixError>
where
    S: StorageAdapterRead,
{
    let entries = view
        .diff_state_rows_between_commits(base_commit_id, side_commit_id)
        .await?;
    let mut change_ids = BTreeSet::new();
    for entry in &entries {
        if let Some(row) = entry.before.as_ref() {
            change_ids.insert(row.change_id);
        }
        if let Some(row) = entry.after.as_ref() {
            change_ids.insert(row.change_id);
        }
    }
    let change_ids = change_ids.into_iter().collect::<Vec<_>>();
    let records = view.load_change_records(&change_ids).await?;
    let mut authenticated = std::collections::HashMap::with_capacity(records.len());
    for (change_id, record) in change_ids.iter().copied().zip(records) {
        let record = record.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("merge state row references missing change '{change_id}'"),
            )
        })?;
        authenticated.insert(change_id, record);
    }
    for entry in &entries {
        for row in [entry.before.as_ref(), entry.after.as_ref()]
            .into_iter()
            .flatten()
        {
            let record = authenticated.get(&row.change_id).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("merge state row lost change payload '{}", row.change_id),
                )
            })?;
            let snapshot = row.snapshot_content.as_deref().map_or(
                crate::json_store::JsonSlot::None,
                crate::json_store::JsonSlot::from_json,
            );
            let metadata = row.metadata.as_deref().map_or(
                crate::json_store::JsonSlot::None,
                crate::json_store::JsonSlot::from_json,
            );
            if record.schema_key != row.key.schema_key
                || record.file_id != row.key.file_id
                || record.entity_pk != row.key.entity_pk
                || record.created_at != row.created_at
                || record.snapshot != snapshot
                || record.metadata != metadata
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "merge change '{}' does not authenticate its state row",
                        row.change_id
                    ),
                ));
            }
        }
    }
    let payloads = authenticated
        .into_iter()
        .map(|(change_id, record)| (change_id, record.snapshot, record.metadata));
    let payloads = TrackedStatePayloadBatch::from_payloads(payloads)?;
    let mut keys = Vec::with_capacity(entries.len());
    for entry in &entries {
        let key = entry
            .before
            .as_ref()
            .map(|row| row.key.clone())
            .or_else(|| entry.after.as_ref().map(|row| row.key.clone()))
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "ForkTree diff entry has neither a before nor after row",
                )
            })?;
        if entry
            .before
            .as_ref()
            .zip(entry.after.as_ref())
            .is_some_and(|(before, after)| before.key != after.key)
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "ForkTree diff entry has mismatched before/after identities",
            ));
        }
        keys.push(crate::tracked_state::TrackedStateKey {
            schema_key: key.schema_key,
            file_id: key.file_id,
            entity_pk: key.entity_pk,
        });
    }
    let identities = TrackedStateDiffIdentity::from_key_batch(keys)?;
    let mut converted = Vec::with_capacity(entries.len());
    for (entry, identity) in entries.into_iter().zip(identities) {
        let before = entry.before.map(|row| TrackedStateDiffRow {
            identity: identity.clone(),
            deleted: row.deleted,
            created_at: row.created_at,
            updated_at: row.updated_at,
            change_id: row.change_id,
            commit_id: row.commit_id,
        });
        let after = entry.after.map(|row| TrackedStateDiffRow {
            identity: identity.clone(),
            deleted: row.deleted,
            created_at: row.created_at,
            updated_at: row.updated_at,
            change_id: row.change_id,
            commit_id: row.commit_id,
        });
        let before_live = before.as_ref().is_some_and(|row| !row.deleted);
        let after_live = after.as_ref().is_some_and(|row| !row.deleted);
        let kind = match (before_live, after_live) {
            (false, true) => TrackedStateDiffKind::Added,
            (true, false) => TrackedStateDiffKind::Removed,
            (true, true) => TrackedStateDiffKind::Modified,
            (false, false) => continue,
        };
        converted.push(TrackedStateDiffEntry {
            identity,
            kind,
            before,
            after,
        });
    }
    Ok(TrackedStateDiff::from_entries_with_payloads(
        converted, payloads,
    ))
}

fn exclude_internal_checkpoint_markers(diff: &mut TrackedStateDiff) {
    diff.entries.retain(|entry| {
        entry.identity.schema_key() != crate::checkpoint::CHECKPOINT_MARKER_SCHEMA_KEY
            && entry.identity.schema_key() != crate::undo_redo::UNDO_REDO_MARKER_SCHEMA_KEY
    });
}
