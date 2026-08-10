use crate::LixError;
use crate::branch::{BRANCH_DESCRIPTOR_SCHEMA_KEY, BRANCH_REF_SCHEMA_KEY};
use crate::changelog::CommitId;
use crate::forktree::{ForkTreeReadFacade, load_commit_summary};
use crate::state::{ForkTreeStateView, StateRoots};
use crate::storage_adapter::StorageAdapterRead;

use super::conflicts::MergeConflictBatch;
use super::native::{MergeDiff, MergeKeyExt, MergePayloadBatch, MergePlan, plan_merge};
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
    pub(crate) source_diff: MergeDiff,
    pub(crate) target_diff: MergeDiff,
    pub(crate) stats: MergeStats,
    pub(crate) merge_plan: Option<MergePlan>,
}

impl MergeAnalysis {
    pub(crate) fn merge_plan(&self) -> Option<&MergePlan> {
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
    let base = load_commit_summary(&view, commits.base_commit_id)
        .await?
        .ok_or_else(|| {
            LixError::commit_not_found(commits.base_commit_id.to_string(), "merge", "base")
        })?;
    let source = load_commit_summary(&view, commits.source_commit_id)
        .await?
        .ok_or_else(|| {
            LixError::commit_not_found(commits.source_commit_id.to_string(), "merge", "source")
        })?;
    let target = if commits.base_commit_id == commits.source_commit_id
        || commits.base_commit_id == commits.target_commit_id
    {
        None
    } else {
        Some(
            load_commit_summary(&view, commits.target_commit_id)
                .await?
                .ok_or_else(|| {
                    LixError::commit_not_found(
                        commits.target_commit_id.to_string(),
                        "merge",
                        "target",
                    )
                })?,
        )
    };
    let state_view = ForkTreeStateView::new(view);
    let mut source_diff = load_forktree_diff(&state_view, &base, &source).await?;
    let mut target_diff = if commits.base_commit_id == commits.source_commit_id
        || commits.base_commit_id == commits.target_commit_id
    {
        MergeDiff::default()
    } else {
        load_forktree_diff(&state_view, &base, target.as_ref().expect("target loaded")).await?
    };
    exclude_internal_checkpoint_markers(&mut source_diff);
    exclude_internal_checkpoint_markers(&mut target_diff);

    let untracked_rows = state_view.untracked_overlay_rows().await?;
    if let Some(entry) = source_diff.entries.iter().find(|entry| {
        untracked_rows.iter().any(|row| {
            row.key.schema_key == entry.identity.schema_key()
                && row.key.file_id.as_deref() == entry.identity.file_id()
                && row.key.entity_pk == entry.identity.entity_pk().clone()
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
        let payloads = MergePayloadBatch::default();
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
    view: &ForkTreeStateView<&S>,
    base: &crate::forktree::CommitObjectV1,
    side: &crate::forktree::CommitObjectV1,
) -> Result<MergeDiff, LixError>
where
    S: StorageAdapterRead,
{
    let entries = view
        .diff_roots(
            StateRoots {
                global: base.global_state_root,
                local: Some(base.local_state_root),
            },
            StateRoots {
                global: side.global_state_root,
                local: Some(side.local_state_root),
            },
        )
        .await?;
    let mut change_ids = std::collections::BTreeSet::new();
    for entry in &entries {
        if let Some(value) = entry.before.as_ref() {
            change_ids.insert(value.value.change_id);
        }
        if let Some(value) = entry.after.as_ref() {
            change_ids.insert(value.value.change_id);
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
        for value in [entry.before.as_ref(), entry.after.as_ref()]
            .into_iter()
            .flatten()
        {
            let state = &value.value;
            let record = authenticated.get(&state.change_id).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("merge state row lost change payload '{}'", state.change_id),
                )
            })?;
            let snapshot = match &state.cell {
                crate::forktree::StateCell::Tombstone => crate::json_store::JsonSlot::None,
                crate::forktree::StateCell::Null => crate::json_store::JsonSlot::from_json("null"),
                crate::forktree::StateCell::Value(value) => {
                    crate::json_store::JsonSlot::from_json(value)
                }
            };
            let metadata = state.metadata.as_deref().map_or(
                crate::json_store::JsonSlot::None,
                crate::json_store::JsonSlot::from_json,
            );
            if record.schema_key != entry.key.schema_key
                || record.file_id != entry.key.file_id
                || record.entity_pk != entry.key.entity_pk
                || record.created_at != state.created_at
                || record.snapshot != snapshot
                || record.metadata != metadata
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "merge change '{}' does not authenticate its state row",
                        state.change_id
                    ),
                ));
            }
        }
    }
    let payloads = authenticated
        .into_iter()
        .map(|(change_id, record)| (change_id, record.snapshot, record.metadata));
    let payloads = MergePayloadBatch::from_payloads(payloads)?;
    Ok(MergeDiff::from_native(entries, payloads))
}

fn exclude_internal_checkpoint_markers(diff: &mut MergeDiff) {
    diff.entries.retain(|entry| {
        !matches!(
            entry.identity.schema_key.as_str(),
            crate::checkpoint::CHECKPOINT_MARKER_SCHEMA_KEY
                | crate::undo_redo::UNDO_REDO_MARKER_SCHEMA_KEY
                | BRANCH_DESCRIPTOR_SCHEMA_KEY
                | BRANCH_REF_SCHEMA_KEY
        )
    });
}
