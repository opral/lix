use std::collections::HashMap;

use crate::LixError;
use crate::changelog::CommitId;
use crate::storage_adapter::StorageAdapterRead;
use crate::tracked_state::{
    TrackedStateDiff, TrackedStateDiffRequest, TrackedStateMergePlan, TrackedStatePayloadBatch,
    TrackedStateStoreReader, plan_merge,
};

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
    reader: &mut TrackedStateStoreReader<S>,
    commits: MergeCommits,
) -> Result<MergeAnalysis, LixError>
where
    S: StorageAdapterRead,
{
    analyze_with_publication(reader, commits, false).await
}
/// Explicit migration authors a native two-parent commit even for a fast-forward
/// so selected historical rows receive candidate constraint validation and an
/// exact atomic outcome receipt. Ordinary branch merge retains its fast path.
pub(crate) async fn analyze_native_migration<S>(
    reader: &mut TrackedStateStoreReader<S>,
    commits: MergeCommits,
) -> Result<MergeAnalysis, LixError>
where
    S: StorageAdapterRead,
{
    analyze_with_publication(reader, commits, true).await
}
async fn analyze_with_publication<S>(
    reader: &mut TrackedStateStoreReader<S>,
    commits: MergeCommits,
    force_merge: bool,
) -> Result<MergeAnalysis, LixError>
where
    S: StorageAdapterRead,
{
    // Commit-graph analysis has already authenticated both heads and selected
    // this base. When the source is the base, the merge cannot contribute any
    // tracked-state changes, so avoid opening the immutable state authorities
    // solely to prove two empty diffs.
    if commits.base_commit_id == commits.source_commit_id {
        return Ok(MergeAnalysis {
            outcome: MergeOutcome::AlreadyUpToDate,
            commits,
            source_diff: TrackedStateDiff::default(),
            target_diff: TrackedStateDiff::default(),
            stats: MergeStats::default(),
            merge_plan: None,
        });
    }

    let request = TrackedStateDiffRequest::default();
    let base_commit_id = commits.base_commit_id.to_string();
    let source_commit_id = commits.source_commit_id.to_string();
    let target_commit_id = commits.target_commit_id.to_string();
    let mut source_diff = reader
        .diff_commit_members(&base_commit_id, &source_commit_id, &request)
        .await?;
    let mut target_diff = if commits.base_commit_id == commits.source_commit_id
        || commits.base_commit_id == commits.target_commit_id
    {
        TrackedStateDiff::default()
    } else {
        reader
            .diff_commit_members(&base_commit_id, &target_commit_id, &request)
            .await?
    };
    reject_independent_undo_state_overlap(&target_diff, &source_diff)?;
    exclude_checkpoint_rows(&mut source_diff);
    exclude_checkpoint_rows(&mut target_diff);

    let outcome = if !force_merge && commits.base_commit_id == commits.target_commit_id {
        MergeOutcome::FastForward
    } else {
        MergeOutcome::MergeCommitted
    };

    let merge_plan = if outcome == MergeOutcome::MergeCommitted {
        let fallback_ids =
            crate::tracked_state::merge_payload_fallback_ids(&target_diff, &source_diff)?;
        let payloads = if fallback_ids.is_empty() {
            TrackedStatePayloadBatch::default()
        } else {
            reader.load_change_payloads(&fallback_ids).await?
        };
        Some(plan_merge(&target_diff, &source_diff, &payloads)?)
    } else {
        None
    };

    let stats = match outcome {
        MergeOutcome::AlreadyUpToDate => unreachable!("already-up-to-date merges return early"),
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

fn exclude_checkpoint_rows(diff: &mut TrackedStateDiff) {
    diff.entries.retain(|entry| {
        entry.identity.schema_key() != crate::checkpoint::CHECKPOINT_SCHEMA_KEY
            && entry.identity.schema_key() != crate::undo_redo::UNDO_REDO_MARKER_SCHEMA_KEY
    });
}

/// Undo/redo state is a semantic receipt ledger. Two branches may carry the
/// same resulting JSON after independently consuming a receipt, but those
/// writes are still different events and cannot be reconciled by the generic
/// equal-final-state merge rule. Preserve a shared event identity when both
/// sides selected the exact same immutable change; reject every other overlap
/// before any merge plan or write is staged.
fn reject_independent_undo_state_overlap(
    target: &TrackedStateDiff,
    source: &TrackedStateDiff,
) -> Result<(), LixError> {
    // `analyze` receives globally sorted diffs, while partial incoming-row
    // analysis assembles independently filtered groups. Search only the
    // sparse internal ledger rows so this guard does not depend on either
    // caller's concatenation order.
    let source_undo_state = source
        .entries
        .iter()
        .filter(|entry| entry.identity.schema_key() == crate::undo_redo::UNDO_STATE_SCHEMA_KEY)
        .map(|entry| (entry.identity.clone(), entry))
        .collect::<HashMap<_, _>>();

    for target_entry in target
        .entries
        .iter()
        .filter(|entry| entry.identity.schema_key() == crate::undo_redo::UNDO_STATE_SCHEMA_KEY)
    {
        let Some(source_entry) = source_undo_state.get(&target_entry.identity) else {
            continue;
        };
        if target_entry.after.as_ref().map(|row| row.change_id)
            != source_entry.after.as_ref().map(|row| row.change_id)
        {
            return Err(LixError::new(
                LixError::CODE_MERGE_CONFLICT,
                format!(
                    "independent undo state changes conflict for row '{}'",
                    target_entry.identity.row_pk().as_json_array_text()?
                ),
            ));
        }
    }
    Ok(())
}

/// Analyze only caller-proven incoming identities. Remote-only rows remain in
/// the target root, so reconciliation never needs a repository-wide diff.
pub(crate) async fn analyze_incoming_rows<S: StorageAdapterRead>(
    reader: &mut TrackedStateStoreReader<S>,
    base: CommitId,
    target: CommitId,
    source: CommitId,
    mut source_diff: TrackedStateDiff,
    mut target_diff: TrackedStateDiff,
) -> Result<MergeAnalysis, LixError> {
    reject_independent_undo_state_overlap(&target_diff, &source_diff)?;
    exclude_checkpoint_rows(&mut source_diff);
    exclude_checkpoint_rows(&mut target_diff);
    let fallback = crate::tracked_state::merge_payload_fallback_ids(&target_diff, &source_diff)?;
    let payloads = reader.load_change_payloads(&fallback).await?;
    let plan = plan_merge(&target_diff, &source_diff, &payloads)?;
    let stats = stats_from_plan(&plan, &source_diff)?;
    Ok(MergeAnalysis {
        outcome: MergeOutcome::MergeCommitted,
        commits: MergeCommits {
            base_commit_id: base,
            target_commit_id: target,
            source_commit_id: source,
        },
        source_diff,
        target_diff,
        stats,
        merge_plan: Some(plan),
    })
}
