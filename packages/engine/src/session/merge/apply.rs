use crate::tracked_state::TrackedStateMergePlan;
use crate::transaction::types::TransactionAdoptedChange;

pub(crate) fn adopted_changes_from_merge_plan(
    plan: &TrackedStateMergePlan,
    target_version_id: &str,
    source_parent_commit_id: &str,
) -> Vec<TransactionAdoptedChange> {
    plan.patches
        .iter()
        .map(|patch| {
            stage_adopted_change_from_patch(patch, target_version_id, source_parent_commit_id)
        })
        .collect()
}

fn stage_adopted_change_from_patch(
    patch: &crate::tracked_state::TrackedStateMergePatch,
    target_version_id: &str,
    source_parent_commit_id: &str,
) -> TransactionAdoptedChange {
    TransactionAdoptedChange {
        version_id: target_version_id.to_string(),
        change_id: patch.change_id().to_string(),
        source_parent_commit_id: source_parent_commit_id.to_string(),
        projected_row: patch.projected_row().clone(),
    }
}
