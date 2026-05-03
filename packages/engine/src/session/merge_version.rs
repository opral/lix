use crate::tracked_state::{TrackedStateDiffRequest, TrackedStateMergePlan};
use crate::transaction::types::{StageAdoptedChange, StageWrite};
use crate::LixError;

use super::context::SessionContext;

/// Options for merging another version into this session's active version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeVersionOptions {
    /// Version whose changes should be merged into the active session version.
    pub source_version_id: String,
}

/// Receipt returned after merging a version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeVersionReceipt {
    pub outcome: MergeVersionOutcome,
    pub target_version_id: String,
    pub source_version_id: String,
    pub merge_base_commit_id: Option<String>,
    pub target_head_before_commit_id: String,
    pub source_head_before_commit_id: String,
    pub target_head_after_commit_id: String,
    pub created_merge_commit_id: Option<String>,
    /// Number of source-side changes applied into the target version.
    pub applied_change_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeVersionOutcome {
    AlreadyUpToDate,
    FastForward,
    MergeCommitted,
}

impl SessionContext {
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

                let (target_head, source_head) = {
                    let reader = transaction.version_ref_reader();
                    let target_head = reader
                        .load_head_commit_id(&active_version_id)
                        .await?
                        .ok_or_else(|| {
                            LixError::new(
                                "LIX_ERROR_UNKNOWN",
                                format!(
                                    "cannot merge into missing active version ref '{}'",
                                    active_version_id
                                ),
                            )
                        })?;
                    let source_head = reader
                        .load_head_commit_id(&source_version_id)
                        .await?
                        .ok_or_else(|| {
                            LixError::new(
                                "LIX_ERROR_UNKNOWN",
                                format!(
                                    "cannot merge from missing source version ref '{}'",
                                    source_version_id
                                ),
                            )
                        })?;
                    (target_head, source_head)
                };

                let merge_base = {
                    let mut reader = transaction.commit_graph_reader();
                    reader.merge_base(&target_head, &source_head).await?
                };

                if merge_base.commit_id == source_head {
                    return Ok(MergeVersionReceipt {
                        outcome: MergeVersionOutcome::AlreadyUpToDate,
                        target_version_id: active_version_id,
                        source_version_id,
                        merge_base_commit_id: Some(merge_base.commit_id),
                        target_head_after_commit_id: target_head.clone(),
                        target_head_before_commit_id: target_head,
                        source_head_before_commit_id: source_head,
                        created_merge_commit_id: None,
                        applied_change_count: 0,
                    });
                }

                if merge_base.commit_id == target_head {
                    transaction
                        .advance_version_ref(&active_version_id, &source_head)
                        .await?;

                    return Ok(MergeVersionReceipt {
                        outcome: MergeVersionOutcome::FastForward,
                        target_version_id: active_version_id,
                        source_version_id,
                        merge_base_commit_id: Some(merge_base.commit_id),
                        target_head_before_commit_id: target_head,
                        source_head_before_commit_id: source_head.clone(),
                        target_head_after_commit_id: source_head,
                        created_merge_commit_id: None,
                        applied_change_count: 0,
                    });
                }

                let merge_plan = {
                    let mut reader = transaction.tracked_state_reader();
                    reader
                        .plan_merge(
                            &merge_base.commit_id,
                            &target_head,
                            &source_head,
                            &TrackedStateDiffRequest::default(),
                        )
                        .await?
                };

                if !merge_plan.conflicts.is_empty() {
                    let conflict_count = merge_plan.conflicts.len();
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "engine2 merge_version found {conflict_count} tracked-state conflict(s)"
                        ),
                    ));
                }

                let adopted_changes =
                    adopted_changes_from_merge_plan(&merge_plan, &active_version_id);
                if adopted_changes.is_empty() {
                    return Ok(MergeVersionReceipt {
                        outcome: MergeVersionOutcome::AlreadyUpToDate,
                        target_version_id: active_version_id,
                        source_version_id,
                        merge_base_commit_id: Some(merge_base.commit_id),
                        target_head_after_commit_id: target_head.clone(),
                        target_head_before_commit_id: target_head,
                        source_head_before_commit_id: source_head,
                        created_merge_commit_id: None,
                        applied_change_count: 0,
                    });
                }

                let applied_change_count = adopted_changes.len();
                transaction.stage_write(StageWrite::AdoptedChanges {
                    changes: adopted_changes,
                })?;
                let created_merge_commit_id = transaction
                    .staged_commit_id(&active_version_id)?
                    .ok_or_else(|| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            "merge_version staged tracked rows without a commit id",
                        )
                    })?;
                transaction.add_commit_parent(active_version_id.clone(), source_head.clone())?;

                Ok(MergeVersionReceipt {
                    outcome: MergeVersionOutcome::MergeCommitted,
                    target_version_id: active_version_id,
                    source_version_id,
                    merge_base_commit_id: Some(merge_base.commit_id),
                    target_head_before_commit_id: target_head,
                    source_head_before_commit_id: source_head,
                    created_merge_commit_id: Some(created_merge_commit_id.clone()),
                    target_head_after_commit_id: created_merge_commit_id,
                    applied_change_count,
                })
            })
        })
        .await
    }
}

fn adopted_changes_from_merge_plan(
    plan: &TrackedStateMergePlan,
    target_version_id: &str,
) -> Vec<StageAdoptedChange> {
    plan.patches
        .iter()
        .map(|patch| stage_adopted_change_from_patch(patch, target_version_id))
        .collect()
}

fn stage_adopted_change_from_patch(
    patch: &crate::tracked_state::TrackedStateMergePatch,
    target_version_id: &str,
) -> StageAdoptedChange {
    StageAdoptedChange {
        version_id: target_version_id.to_string(),
        change_id: patch.change_id().to_string(),
        projected_row: patch.projected_row().clone(),
    }
}
