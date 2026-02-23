use crate::sql::PreprocessOutput;
use crate::sql::{active_version_from_mutations, active_version_from_update_validations};
use crate::state_commit_stream::state_commit_stream_changes_from_mutations;

use super::super::contracts::effects::PlanEffects;
use super::super::contracts::planner_error::PlannerError;

pub(crate) fn derive_plan_effects(
    output: &PreprocessOutput,
    writer_key: Option<&str>,
) -> Result<PlanEffects, PlannerError> {
    let state_commit_stream_changes =
        state_commit_stream_changes_from_mutations(&output.mutations, writer_key);
    let next_active_version_id = active_version_from_mutations(&output.mutations)
        .map_err(PlannerError::preprocess)?
        .or(
            active_version_from_update_validations(&output.update_validations)
                .map_err(PlannerError::preprocess)?,
        );

    Ok(PlanEffects {
        state_commit_stream_changes,
        next_active_version_id,
    })
}
