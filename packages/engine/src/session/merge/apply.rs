use crate::tracked_state::TrackedStateMergePlan;
use crate::transaction::types::StageAdoptedChange;

pub(crate) fn adopted_changes_from_merge_plan(
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
