use crate::LixError;
use crate::branch::{BRANCH_DESCRIPTOR_SCHEMA_KEY, BRANCH_REF_SCHEMA_KEY};
use crate::changelog::CommitId;
use crate::forktree::ForkTreeReadFacade;
use crate::state::ForkTreeStateView;
use crate::storage_adapter::StorageAdapterRead;

use super::conflicts::MergeConflictBatch;
use super::native::{MergeDiff, MergePlan, plan_merge};
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
    let state_view = ForkTreeStateView::new(view);
    let mut source_diff = load_forktree_diff(
        &state_view,
        commits.base_commit_id,
        commits.source_commit_id,
    )
    .await?;
    let mut target_diff = if commits.base_commit_id == commits.source_commit_id
        || commits.base_commit_id == commits.target_commit_id
    {
        MergeDiff::default()
    } else {
        load_forktree_diff(
            &state_view,
            commits.base_commit_id,
            commits.target_commit_id,
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

async fn load_forktree_diff<S>(
    view: &ForkTreeStateView<&S>,
    base: crate::changelog::CommitId,
    side: crate::changelog::CommitId,
) -> Result<MergeDiff, LixError>
where
    S: StorageAdapterRead,
{
    MergeDiff::from_historical(view.diff_commits(base, side).await?)
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
