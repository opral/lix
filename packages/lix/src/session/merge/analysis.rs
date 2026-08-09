use super::conflicts::MergeConflictBatch;
use super::stats::{MergeStats, stats_from_diff, stats_from_plan};
use crate::LixError;
use crate::branch::{BRANCH_DESCRIPTOR_SCHEMA_KEY, BRANCH_REF_SCHEMA_KEY};
use crate::changelog::CommitId;
use crate::forktree::{CoherentView, ForkTreeReadFacade};
use crate::storage_adapter::StorageAdapterRead;
use crate::tracked_state::{
    TrackedStateDiff, TrackedStateDiffEntry, TrackedStateDiffIdentity, TrackedStateDiffKind,
    TrackedStateDiffRow, TrackedStateMergePlan, TrackedStatePayloadBatch, plan_merge,
};

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

    let untracked_rows = view.scan_untracked_overlay_rows().await?;
    if let Some(entry) = source_diff.entries.iter().find(|entry| {
        untracked_rows.iter().any(|(_, key, _)| {
            key.schema_key == entry.identity.schema_key()
                && key.file_id.as_deref() == entry.identity.file_id()
                && key.entity_pk == *entry.identity.entity_pk()
        })
    }) {
        return Err(LixError::new(
            LixError::CODE_MERGE_CONFLICT,
            format!(
                "merge source identity conflicts with an untracked current row for schema '{}'",
                entry.identity.schema_key()
            ),
        )
        .with_details(serde_json::json!({
            "kind": "trackedUntrackedIdentityCollision",
            "schemaKey": entry.identity.schema_key(),
            "entityPk": entry.identity.entity_pk().as_json_array_value()?,
            "fileId": entry.identity.file_id(),
        })));
    }

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
    let authenticated_diff = view
        .diff_state_rows_between_commits_with_records(base_commit_id, side_commit_id)
        .await?;
    let entries = authenticated_diff.entries;
    let authenticated = authenticated_diff.changes;
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
        !matches!(
            entry.identity.schema_key(),
            crate::checkpoint::CHECKPOINT_MARKER_SCHEMA_KEY
                | crate::undo_redo::UNDO_REDO_MARKER_SCHEMA_KEY
                | BRANCH_DESCRIPTOR_SCHEMA_KEY
                | BRANCH_REF_SCHEMA_KEY
        )
    });
}
