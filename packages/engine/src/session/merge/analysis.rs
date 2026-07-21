use crate::LixError;
use crate::changelog::CommitId;
use crate::storage_adapter::StorageAdapterRead;
use crate::tracked_state::{
    TrackedStateDiff, TrackedStateDiffRequest, TrackedStateMergePlan, TrackedStateStoreReader,
    plan_merge,
};

use super::conflicts::{MergeConflict, conflicts_from_plan};
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
    pub(crate) conflicts: Vec<MergeConflict>,
    pub(crate) merge_plan: Option<TrackedStateMergePlan>,
}

impl MergeAnalysis {
    pub(crate) fn merge_plan(&self) -> Option<&TrackedStateMergePlan> {
        self.merge_plan.as_ref()
    }
}

pub(crate) async fn analyze<S>(
    reader: &mut TrackedStateStoreReader<S>,
    commits: MergeCommits,
) -> Result<MergeAnalysis, LixError>
where
    S: StorageAdapterRead,
{
    let request = TrackedStateDiffRequest::default();
    let base_commit_id = commits.base_commit_id.to_string();
    let source_commit_id = commits.source_commit_id.to_string();
    let target_commit_id = commits.target_commit_id.to_string();
    let source_diff = reader
        .diff_commits(&base_commit_id, &source_commit_id, &request)
        .await?;
    let target_diff = if commits.base_commit_id == commits.source_commit_id
        || commits.base_commit_id == commits.target_commit_id
    {
        TrackedStateDiff::default()
    } else {
        reader
            .diff_commits(&base_commit_id, &target_commit_id, &request)
            .await?
    };

    let outcome = if commits.base_commit_id == commits.source_commit_id {
        MergeOutcome::AlreadyUpToDate
    } else if commits.base_commit_id == commits.target_commit_id {
        MergeOutcome::FastForward
    } else {
        MergeOutcome::MergeCommitted
    };

    let merge_plan = if outcome == MergeOutcome::MergeCommitted {
        let fallback_ids =
            crate::tracked_state::merge_payload_fallback_ids(&target_diff, &source_diff);
        let payloads = reader.load_change_payloads(&fallback_ids).await?;
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

    let conflicts = merge_plan
        .as_ref()
        .map(conflicts_from_plan)
        .transpose()?
        .unwrap_or_default();

    Ok(MergeAnalysis {
        outcome,
        commits,
        source_diff,
        target_diff,
        stats,
        conflicts,
        merge_plan,
    })
}
