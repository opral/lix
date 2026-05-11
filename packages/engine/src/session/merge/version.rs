use serde_json::{json, Value as JsonValue};

use crate::transaction::types::TransactionWrite;
use crate::version::{VersionLifecycle, VersionOperation, VersionReferenceRole};
use crate::LixError;

use super::analysis::{analyze, MergeCommits, MergeOutcome};
use super::apply::adopted_changes_from_merge_plan;
use super::conflicts::{
    MergeConflict as AnalysisMergeConflict,
    MergeConflictChangeKind as AnalysisMergeConflictChangeKind,
    MergeConflictKind as AnalysisMergeConflictKind, MergeConflictSide as AnalysisMergeConflictSide,
};
use super::stats::MergeStats;
use crate::session::context::SessionContext;

/// Options for merging another version into this session's active version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeVersionOptions {
    /// Version whose changes should be merged into the active session version.
    pub source_version_id: String,
}

/// Options for previewing a merge from another version into this session's
/// active version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeVersionPreviewOptions {
    /// Version whose changes would be merged into the active session version.
    pub source_version_id: String,
}

/// Receipt returned after merging a version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeVersionReceipt {
    pub outcome: MergeVersionOutcome,
    pub target_version_id: String,
    pub source_version_id: String,
    pub base_commit_id: String,
    pub target_head_before_commit_id: String,
    pub source_head_before_commit_id: String,
    pub target_head_after_commit_id: String,
    pub created_merge_commit_id: Option<String>,
    pub change_stats: MergeChangeStats,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MergeChangeStats {
    pub total: usize,
    pub added: usize,
    pub modified: usize,
    pub removed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeVersionPreview {
    pub outcome: MergeVersionOutcome,
    pub target_version_id: String,
    pub source_version_id: String,
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
    pub entity_id: JsonValue,
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
pub enum MergeVersionOutcome {
    AlreadyUpToDate,
    FastForward,
    MergeCommitted,
}

impl SessionContext {
    /// Previews merging `source_version_id` into this session's active version
    /// without advancing refs, staging changes, or creating commits.
    pub async fn merge_version_preview(
        &self,
        options: MergeVersionPreviewOptions,
    ) -> Result<MergeVersionPreview, LixError> {
        let source_version_id = options.source_version_id;

        self.with_write_transaction(|transaction| {
            Box::pin(async move {
                let active_version_id = transaction.active_version_id().to_string();
                if source_version_id == active_version_id {
                    return Err(LixError::invalid_self_merge(active_version_id));
                }

                let (target_head, source_head) = {
                    let reader = transaction.version_ref_reader();
                    let lifecycle = VersionLifecycle::new(&reader);
                    let target_head = lifecycle
                        .require_existing_commit_id(
                            &active_version_id,
                            VersionOperation::MergeVersionPreview,
                            VersionReferenceRole::Target,
                        )
                        .await?;
                    let source_head = lifecycle
                        .require_existing_commit_id(
                            &source_version_id,
                            VersionOperation::MergeVersionPreview,
                            VersionReferenceRole::Source,
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
                    &active_version_id,
                    &source_version_id,
                    &analysis,
                ))
            })
        })
        .await
    }

    /// Merges `source_version_id` into this session's active version.
    ///
    /// The generated target commit keeps the previous target head as its first
    /// parent and records the source head as an additional parent, so the
    /// commit graph preserves branch ancestry while tracked-state storage can
    /// build the new root by applying source effects onto the target root.
    pub async fn merge_version(
        &self,
        options: MergeVersionOptions,
    ) -> Result<MergeVersionReceipt, LixError> {
        let source_version_id = options.source_version_id;

        self.with_write_transaction(|transaction| {
            Box::pin(async move {
                let active_version_id = transaction.active_version_id().to_string();
                if source_version_id == active_version_id {
                    return Err(LixError::invalid_self_merge(active_version_id));
                }

                let (target_head, source_head) = {
                    let reader = transaction.version_ref_reader();
                    let lifecycle = VersionLifecycle::new(&reader);
                    let target_head = lifecycle
                        .require_existing_commit_id(
                            &active_version_id,
                            VersionOperation::MergeVersion,
                            VersionReferenceRole::Target,
                        )
                        .await?;
                    let source_head = lifecycle
                        .require_existing_commit_id(
                            &source_version_id,
                            VersionOperation::MergeVersion,
                            VersionReferenceRole::Source,
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
                    return Ok(MergeVersionReceipt {
                        outcome: MergeVersionOutcome::AlreadyUpToDate,
                        target_version_id: active_version_id,
                        source_version_id,
                        base_commit_id: analysis.commits.base_commit_id,
                        target_head_after_commit_id: analysis.commits.target_commit_id.clone(),
                        target_head_before_commit_id: analysis.commits.target_commit_id,
                        source_head_before_commit_id: analysis.commits.source_commit_id,
                        created_merge_commit_id: None,
                        change_stats: merge_change_stats_from_analysis(&analysis.stats),
                    });
                }

                if analysis.outcome == MergeOutcome::FastForward {
                    transaction
                        .advance_version_ref(&active_version_id, &analysis.commits.source_commit_id)
                        .await?;

                    return Ok(MergeVersionReceipt {
                        outcome: MergeVersionOutcome::FastForward,
                        target_version_id: active_version_id,
                        source_version_id,
                        base_commit_id: analysis.commits.base_commit_id,
                        target_head_before_commit_id: analysis.commits.target_commit_id,
                        source_head_before_commit_id: analysis.commits.source_commit_id.clone(),
                        target_head_after_commit_id: analysis.commits.source_commit_id,
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

                let adopted_changes =
                    adopted_changes_from_merge_plan(merge_plan, &active_version_id);
                if adopted_changes.is_empty() {
                    let created_merge_commit_id =
                        transaction.stage_empty_commit(active_version_id.clone())?;
                    transaction.add_commit_parent(
                        active_version_id.clone(),
                        analysis.commits.source_commit_id.clone(),
                    )?;
                    return Ok(MergeVersionReceipt {
                        outcome: MergeVersionOutcome::MergeCommitted,
                        target_version_id: active_version_id,
                        source_version_id,
                        base_commit_id: analysis.commits.base_commit_id,
                        target_head_after_commit_id: created_merge_commit_id.clone(),
                        target_head_before_commit_id: analysis.commits.target_commit_id,
                        source_head_before_commit_id: analysis.commits.source_commit_id,
                        created_merge_commit_id: Some(created_merge_commit_id),
                        change_stats: merge_change_stats_from_analysis(&analysis.stats),
                    });
                }

                transaction
                    .stage_write(TransactionWrite::AdoptedChanges {
                        changes: adopted_changes,
                    })
                    .await?;
                let created_merge_commit_id = transaction
                    .staged_commit_id(&active_version_id)?
                    .ok_or_else(|| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            "merge_version staged tracked rows without a commit id",
                        )
                    })?;
                transaction.add_commit_parent(
                    active_version_id.clone(),
                    analysis.commits.source_commit_id.clone(),
                )?;

                Ok(MergeVersionReceipt {
                    outcome: MergeVersionOutcome::MergeCommitted,
                    target_version_id: active_version_id,
                    source_version_id,
                    base_commit_id: analysis.commits.base_commit_id,
                    target_head_before_commit_id: analysis.commits.target_commit_id,
                    source_head_before_commit_id: analysis.commits.source_commit_id,
                    created_merge_commit_id: Some(created_merge_commit_id.clone()),
                    target_head_after_commit_id: created_merge_commit_id,
                    change_stats: merge_change_stats_from_analysis(&analysis.stats),
                })
            })
        })
        .await
    }
}

fn preview_from_analysis(
    target_version_id: &str,
    source_version_id: &str,
    analysis: &super::analysis::MergeAnalysis,
) -> MergeVersionPreview {
    MergeVersionPreview {
        outcome: merge_version_outcome_from_analysis(analysis.outcome),
        target_version_id: target_version_id.to_string(),
        source_version_id: source_version_id.to_string(),
        base_commit_id: analysis.commits.base_commit_id.clone(),
        target_head_commit_id: analysis.commits.target_commit_id.clone(),
        source_head_commit_id: analysis.commits.source_commit_id.clone(),
        change_stats: merge_change_stats_from_analysis(&analysis.stats),
        conflicts: analysis
            .conflicts
            .iter()
            .map(merge_conflict_from_analysis)
            .collect(),
    }
}

fn merge_version_outcome_from_analysis(outcome: MergeOutcome) -> MergeVersionOutcome {
    match outcome {
        MergeOutcome::AlreadyUpToDate => MergeVersionOutcome::AlreadyUpToDate,
        MergeOutcome::FastForward => MergeVersionOutcome::FastForward,
        MergeOutcome::MergeCommitted => MergeVersionOutcome::MergeCommitted,
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
        entity_id: conflict.entity_id.clone(),
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

fn merge_conflict_error(conflicts: &[MergeConflict]) -> Result<LixError, LixError> {
    let conflict_count = conflicts.len();
    Ok(LixError::new(
        LixError::CODE_MERGE_CONFLICT,
        format!("merge_version found {conflict_count} tracked-state conflict(s)"),
    )
    .with_hint("Resolve the conflicting entities in the target version, then retry the merge.")
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
        "entityId": conflict.entity_id,
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
