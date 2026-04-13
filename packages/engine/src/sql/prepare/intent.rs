use std::collections::BTreeSet;

use crate::sql::logical_plan::public_ir::PlannedFilesystemState;
use crate::sql::prepare::contracts::requirements::PlanRequirements;
use crate::LixError;

#[derive(Debug, Clone, Copy)]
pub(crate) struct EffectCollectionPolicy {
    pub(crate) skip_side_effect_collection: bool,
}

pub(crate) struct FilesystemIntent {
    pub(crate) filesystem_state: PlannedFilesystemState,
}

pub(crate) async fn collect_filesystem_intent(
    requirements: &PlanRequirements,
    policy: EffectCollectionPolicy,
) -> Result<FilesystemIntent, LixError> {
    let filesystem_state = if policy.skip_side_effect_collection || requirements.read_only_query {
        PlannedFilesystemState::default()
    } else {
        // Raw SQL filesystem-effect collection no longer stages filesystem ops through a
        // separate event stream. Public and transaction-local filesystem writes are carried
        // by the typed filesystem state built during write planning.
        PlannedFilesystemState::default()
    };

    Ok(FilesystemIntent { filesystem_state })
}

pub(crate) fn authoritative_binary_blob_write_targets_from_planned_state(
    state: &PlannedFilesystemState,
) -> BTreeSet<(String, String)> {
    state
        .files
        .values()
        .filter(|file| file.data.is_some())
        .map(|file| (file.file_id.clone(), file.version_id.clone()))
        .collect()
}

pub(crate) fn delete_targets_from_planned_filesystem_state(
    state: &PlannedFilesystemState,
) -> BTreeSet<(String, String)> {
    state
        .files
        .values()
        .filter(|file| file.deleted)
        .map(|file| (file.file_id.clone(), file.version_id.clone()))
        .collect()
}
