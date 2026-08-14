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
        .diff_commits(&base_commit_id, &source_commit_id, &request)
        .await?;
    let mut target_diff = if commits.base_commit_id == commits.source_commit_id
        || commits.base_commit_id == commits.target_commit_id
    {
        TrackedStateDiff::default()
    } else {
        reader
            .diff_commits(&base_commit_id, &target_commit_id, &request)
            .await?
    };
    exclude_checkpoint_rows(&mut source_diff);
    exclude_checkpoint_rows(&mut target_diff);

    let outcome = if commits.base_commit_id == commits.target_commit_id {
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
