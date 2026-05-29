use serde_json::{Value as JsonValue, json};

use crate::LixError;
use crate::branch::{BranchLifecycle, BranchOperation, BranchReferenceRole};
use crate::storage::StorageBackend;

use super::analysis::{MergeCommits, MergeOutcome, analyze};
use super::conflicts::{
    MergeConflict as AnalysisMergeConflict,
    MergeConflictChangeKind as AnalysisMergeConflictChangeKind,
    MergeConflictKind as AnalysisMergeConflictKind, MergeConflictSide as AnalysisMergeConflictSide,
};
use super::stats::MergeStats;
use crate::session::context::SessionContext;
use crate::tracked_state::TrackedStateMergePick;
use crate::transaction::types::StagedCommitChangeRef;

/// Options for merging another branch into this session's active branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeBranchOptions {
    /// Branch whose changes should be merged into the active session branch.
    pub source_branch_id: String,
}

/// Options for previewing a merge from another branch into this session's
/// active branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeBranchPreviewOptions {
    /// Branch whose changes would be merged into the active session branch.
    pub source_branch_id: String,
}

/// Receipt returned after merging a branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeBranchReceipt {
    pub outcome: MergeBranchOutcome,
    pub target_branch_id: String,
    pub source_branch_id: String,
    pub base_commit_id: String,
    pub target_head_before_commit_id: String,
    pub source_head_before_commit_id: String,
    pub target_head_after_commit_id: String,
    pub created_merge_commit_id: Option<String>,
    pub change_stats: MergeChangeStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MergeChangeStats {
    pub total: usize,
    pub added: usize,
    pub modified: usize,
    pub removed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeBranchPreview {
    pub outcome: MergeBranchOutcome,
    pub target_branch_id: String,
    pub source_branch_id: String,
    pub base_commit_id: String,
    pub target_head_commit_id: String,
    pub source_head_commit_id: String,
    pub change_stats: MergeChangeStats,
    pub conflicts: Vec<MergeConflict>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeConflict {
    pub kind: MergeConflictKind,
    pub schema_key: String,
    pub entity_pk: JsonValue,
    pub file_id: Option<String>,
    pub target: MergeConflictSide,
    pub source: MergeConflictSide,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeConflictKind {
    SameEntityChanged,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeConflictSide {
    pub kind: MergeConflictChangeKind,
    pub before_change_id: Option<String>,
    pub after_change_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeConflictChangeKind {
    Added,
    Modified,
    Removed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeBranchOutcome {
    AlreadyUpToDate,
    FastForward,
    MergeCommitted,
}

impl<B> SessionContext<B>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    /// Previews merging `source_branch_id` into this session's active branch
    /// without advancing refs, staging changes, or creating commits.
    pub async fn merge_branch_preview(
        &self,
        options: MergeBranchPreviewOptions,
    ) -> Result<MergeBranchPreview, LixError> {
        let source_branch_id = options.source_branch_id;

        self.with_write_transaction(|transaction| {
            Box::pin(async move {
                let active_branch_id = transaction.active_branch_id().to_string();
                if source_branch_id == active_branch_id {
                    return Err(LixError::invalid_self_merge(active_branch_id));
                }

                let (target_head, source_head) = {
                    let reader = transaction.branch_ref_reader();
                    let lifecycle = BranchLifecycle::new(&reader);
                    let target_head = lifecycle
                        .require_existing_commit_id(
                            &active_branch_id,
                            BranchOperation::MergeBranchPreview,
                            BranchReferenceRole::Target,
                        )
                        .await?;
                    let source_head = lifecycle
                        .require_existing_commit_id(
                            &source_branch_id,
                            BranchOperation::MergeBranchPreview,
                            BranchReferenceRole::Source,
                        )
                        .await?;
                    (target_head, source_head)
                };

                let merge_base = {
                    let mut reader = transaction.commit_graph_reader();
                    reader.merge_base(&target_head, &source_head).await?
                };

                let analysis = {
                    let mut reader = transaction.tracked_state_reader();
                    analyze(
                        &mut reader,
                        MergeCommits {
                            base_commit_id: merge_base.commit_id,
                            target_commit_id: target_head,
                            source_commit_id: source_head,
                        },
                    )
                    .await?
                };

                Ok(preview_from_analysis(
                    &active_branch_id,
                    &source_branch_id,
                    &analysis,
                ))
            })
        })
        .await
    }

    /// Merges `source_branch_id` into this session's active branch.
    ///
    /// The generated target commit keeps the previous target head as its first
    /// parent and records the source head as an additional parent, so the
    /// commit graph preserves branch ancestry while tracked-state storage
    /// selects the planned source changes into the new target root.
    pub async fn merge_branch(
        &self,
        options: MergeBranchOptions,
    ) -> Result<MergeBranchReceipt, LixError> {
        let source_branch_id = options.source_branch_id;

        self.with_write_transaction(|transaction| {
            Box::pin(async move {
                let active_branch_id = transaction.active_branch_id().to_string();
                if source_branch_id == active_branch_id {
                    return Err(LixError::invalid_self_merge(active_branch_id));
                }

                let (target_head, source_head) = {
                    let reader = transaction.branch_ref_reader();
                    let lifecycle = BranchLifecycle::new(&reader);
                    let target_head = lifecycle
                        .require_existing_commit_id(
                            &active_branch_id,
                            BranchOperation::MergeBranch,
                            BranchReferenceRole::Target,
                        )
                        .await?;
                    let source_head = lifecycle
                        .require_existing_commit_id(
                            &source_branch_id,
                            BranchOperation::MergeBranch,
                            BranchReferenceRole::Source,
                        )
                        .await?;
                    (target_head, source_head)
                };

                let merge_base = {
                    let mut reader = transaction.commit_graph_reader();
                    reader.merge_base(&target_head, &source_head).await?
                };
                let base_commit_id = merge_base.commit_id;

                let analysis = {
                    let mut reader = transaction.tracked_state_reader();
                    analyze(
                        &mut reader,
                        MergeCommits {
                            base_commit_id,
                            target_commit_id: target_head,
                            source_commit_id: source_head,
                        },
                    )
                    .await?
                };

                if analysis.outcome == MergeOutcome::AlreadyUpToDate {
                    return Ok(MergeBranchReceipt {
                        outcome: MergeBranchOutcome::AlreadyUpToDate,
                        target_branch_id: active_branch_id,
                        source_branch_id,
                        base_commit_id: analysis.commits.base_commit_id.to_string(),
                        target_head_after_commit_id: analysis.commits.target_commit_id.to_string(),
                        target_head_before_commit_id: analysis.commits.target_commit_id.to_string(),
                        source_head_before_commit_id: analysis.commits.source_commit_id.to_string(),
                        created_merge_commit_id: None,
                        change_stats: merge_change_stats_from_analysis(&analysis.stats),
                    });
                }

                if analysis.outcome == MergeOutcome::FastForward {
                    transaction
                        .advance_branch_ref(&active_branch_id, analysis.commits.source_commit_id)
                        .await?;

                    return Ok(MergeBranchReceipt {
                        outcome: MergeBranchOutcome::FastForward,
                        target_branch_id: active_branch_id,
                        source_branch_id,
                        base_commit_id: analysis.commits.base_commit_id.to_string(),
                        target_head_before_commit_id: analysis.commits.target_commit_id.to_string(),
                        source_head_before_commit_id: analysis.commits.source_commit_id.to_string(),
                        target_head_after_commit_id: analysis.commits.source_commit_id.to_string(),
                        created_merge_commit_id: None,
                        change_stats: merge_change_stats_from_analysis(&analysis.stats),
                    });
                }

                let merge_plan = analysis
                    .merge_plan()
                    .expect("merge analysis should include a plan for mergeCommitted");

                if !analysis.conflicts.is_empty() {
                    return Err(merge_conflict_error(
                        &analysis
                            .conflicts
                            .iter()
                            .map(merge_conflict_from_analysis)
                            .collect::<Vec<_>>(),
                    )?);
                }

                let selected_changes = merge_plan
                    .picks
                    .iter()
                    .map(selected_change_from_merge_pick)
                    .collect::<Vec<_>>();
                let created_merge_commit_id = transaction.stage_merge_commit(
                    active_branch_id.clone(),
                    analysis.commits.source_commit_id,
                    selected_changes,
                )?;
                Ok(MergeBranchReceipt {
                    outcome: MergeBranchOutcome::MergeCommitted,
                    target_branch_id: active_branch_id,
                    source_branch_id,
                    base_commit_id: analysis.commits.base_commit_id.to_string(),
                    target_head_after_commit_id: created_merge_commit_id.clone(),
                    target_head_before_commit_id: analysis.commits.target_commit_id.to_string(),
                    source_head_before_commit_id: analysis.commits.source_commit_id.to_string(),
                    created_merge_commit_id: Some(created_merge_commit_id),
                    change_stats: merge_change_stats_from_analysis(&analysis.stats),
                })
            })
        })
        .await
    }
}

fn selected_change_from_merge_pick(pick: &TrackedStateMergePick) -> StagedCommitChangeRef {
    StagedCommitChangeRef {
        schema_key: pick.selected_row.schema_key.clone(),
        file_id: pick.selected_row.file_id.clone(),
        entity_pk: pick.selected_row.entity_pk.clone(),
        change_id: pick.change_id,
        snapshot_ref: pick.selected_row.snapshot_ref,
        metadata_ref: pick.selected_row.metadata_ref,
        deleted: pick.selected_row.deleted,
        created_at: pick.selected_row.created_at,
        updated_at: pick.selected_row.updated_at,
    }
}

fn preview_from_analysis(
    target_branch_id: &str,
    source_branch_id: &str,
    analysis: &super::analysis::MergeAnalysis,
) -> MergeBranchPreview {
    MergeBranchPreview {
        outcome: merge_branch_outcome_from_analysis(analysis.outcome),
        target_branch_id: target_branch_id.to_string(),
        source_branch_id: source_branch_id.to_string(),
        base_commit_id: analysis.commits.base_commit_id.to_string(),
        target_head_commit_id: analysis.commits.target_commit_id.to_string(),
        source_head_commit_id: analysis.commits.source_commit_id.to_string(),
        change_stats: merge_change_stats_from_analysis(&analysis.stats),
        conflicts: analysis
            .conflicts
            .iter()
            .map(merge_conflict_from_analysis)
            .collect(),
    }
}

fn merge_branch_outcome_from_analysis(outcome: MergeOutcome) -> MergeBranchOutcome {
    match outcome {
        MergeOutcome::AlreadyUpToDate => MergeBranchOutcome::AlreadyUpToDate,
        MergeOutcome::FastForward => MergeBranchOutcome::FastForward,
        MergeOutcome::MergeCommitted => MergeBranchOutcome::MergeCommitted,
    }
}

fn merge_change_stats_from_analysis(stats: &MergeStats) -> MergeChangeStats {
    MergeChangeStats {
        total: stats.total,
        added: stats.added,
        modified: stats.modified,
        removed: stats.removed,
    }
}

fn merge_conflict_from_analysis(conflict: &AnalysisMergeConflict) -> MergeConflict {
    MergeConflict {
        kind: match conflict.kind {
            AnalysisMergeConflictKind::SameEntityChanged => MergeConflictKind::SameEntityChanged,
        },
        schema_key: conflict.schema_key.clone(),
        entity_pk: conflict.entity_pk.clone(),
        file_id: conflict.file_id.clone(),
        target: merge_conflict_side_from_analysis(&conflict.target),
        source: merge_conflict_side_from_analysis(&conflict.source),
    }
}

fn merge_conflict_side_from_analysis(side: &AnalysisMergeConflictSide) -> MergeConflictSide {
    MergeConflictSide {
        kind: match side.kind {
            AnalysisMergeConflictChangeKind::Added => MergeConflictChangeKind::Added,
            AnalysisMergeConflictChangeKind::Modified => MergeConflictChangeKind::Modified,
            AnalysisMergeConflictChangeKind::Removed => MergeConflictChangeKind::Removed,
        },
        before_change_id: side.before_change_id.clone(),
        after_change_id: side.after_change_id.clone(),
    }
}

#[expect(clippy::unnecessary_wraps)]
fn merge_conflict_error(conflicts: &[MergeConflict]) -> Result<LixError, LixError> {
    let conflict_count = conflicts.len();
    Ok(LixError::new(
        LixError::CODE_MERGE_CONFLICT,
        format!("merge_branch found {conflict_count} tracked-state conflict(s)"),
    )
    .with_hint("Resolve the conflicting entities in the target branch, then retry the merge.")
    .with_details(json!({
        "conflicts": conflicts.iter()
            .map(merge_conflict_details)
            .collect::<Vec<_>>(),
    })))
}

fn merge_conflict_details(conflict: &MergeConflict) -> serde_json::Value {
    json!({
        "kind": match conflict.kind {
            MergeConflictKind::SameEntityChanged => "sameEntityChanged",
        },
        "schemaKey": conflict.schema_key,
        "entityPk": conflict.entity_pk,
        "fileId": conflict.file_id,
        "target": merge_conflict_side_details(&conflict.target),
        "source": merge_conflict_side_details(&conflict.source),
    })
}

fn merge_conflict_side_details(side: &MergeConflictSide) -> serde_json::Value {
    json!({
        "kind": match side.kind {
            MergeConflictChangeKind::Added => "added",
            MergeConflictChangeKind::Modified => "modified",
            MergeConflictChangeKind::Removed => "removed",
        },
        "beforeChangeId": side.before_change_id,
        "afterChangeId": side.after_change_id,
    })
}
