use crate::LixError;
use crate::branch::{BRANCH_DESCRIPTOR_SCHEMA_KEY, BRANCH_REF_SCHEMA_KEY};
use crate::changelog::CommitId;
use crate::forktree::{ForkTreeReadFacade, load_commit_summary};
use crate::state::{ForkTreeStateView, StateRoots};
use crate::storage_adapter::StorageAdapterRead;

use super::conflicts::MergeConflictBatch;
use super::native::{MergeDiff, MergePayloadBatch, MergePlan, plan_merge};
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
    catalog: &crate::catalog::CatalogSnapshot,
    commits: MergeCommits,
) -> Result<MergeAnalysis, LixError>
where
    S: StorageAdapterRead,
{
    let view = facade.branch(branch_id).await?;
    let base = load_commit_summary(&view, native_commit_id(commits.base_commit_id))
        .await?
        .ok_or_else(|| {
            LixError::commit_not_found(commits.base_commit_id.to_string(), "merge", "base")
        })?;
    let source = load_commit_summary(&view, native_commit_id(commits.source_commit_id))
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
            load_commit_summary(&view, native_commit_id(commits.target_commit_id))
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
    let mut source_diff = load_forktree_diff(&state_view, catalog, &base, &source).await?;
    let mut target_diff = if commits.base_commit_id == commits.source_commit_id
        || commits.base_commit_id == commits.target_commit_id
    {
        MergeDiff::default()
    } else {
        load_forktree_diff(
            &state_view,
            catalog,
            &base,
            target.as_ref().expect("target loaded"),
        )
        .await?
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
        Some(plan_merge(&target_diff, &source_diff)?)
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

fn native_commit_id(value: CommitId) -> crate::forktree::CommitId {
    crate::forktree::CommitId::from_bytes(*value.as_uuid().as_bytes())
}

async fn load_forktree_diff<S>(
    view: &ForkTreeStateView<&S>,
    catalog: &crate::catalog::CatalogSnapshot,
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
            // ForkTree change payloads own their canonical JSON inline or in
            // an authenticated ForkTree object. Authenticate the canonical
            // bytes and lifecycle directly instead of comparing storage planes.
            let snapshot_matches = match (&record.snapshot, &state.cell) {
                (crate::json_store::JsonSlot::None, crate::forktree::StateCell::Tombstone) => true,
                (
                    crate::json_store::JsonSlot::Inline(expected),
                    crate::forktree::StateCell::NativeRow(native),
                ) => {
                    let global = value.source == crate::forktree::StateSource::Global;
                    let expected_digest = crate::native_row::semantic_digest_text(expected).ok();
                    catalog
                        .plan_for_key(&entry.key.schema_key)
                        .and_then(|(_, plan)| {
                            crate::native_row::logical_value(
                                &plan.relational_schema,
                                &entry.key.entity_pk,
                                global,
                                entry.key.file_id.as_deref(),
                                native,
                            )
                            .ok()
                        })
                        .zip(expected_digest)
                        .is_some_and(|(value, expected_digest)| {
                            crate::native_row::semantic_digest(&value) == expected_digest
                        })
                }
                _ => false,
            };
            let metadata_matches = match (&record.metadata, state.metadata.as_deref()) {
                (crate::json_store::JsonSlot::None, None) => true,
                (crate::json_store::JsonSlot::Inline(expected), Some(actual)) => {
                    expected.as_ref() == actual
                }
                _ => false,
            };
            if record.schema_key != entry.key.schema_key
                || record.file_id != entry.key.file_id
                || record.entity_pk != entry.key.entity_pk
                || record.created_at != state.created_at
                || !snapshot_matches
                || !metadata_matches
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
