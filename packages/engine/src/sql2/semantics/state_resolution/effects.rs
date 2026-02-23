use crate::sql::{active_version_from_mutations, active_version_from_update_validations};
use crate::state_commit_stream::state_commit_stream_changes_from_mutations;

use super::super::super::contracts::effects::PlanEffects;
use super::super::super::contracts::planned_statement::PlannedStatementSet;
use super::super::super::contracts::planner_error::PlannerError;
use super::super::super::type_bridge::{to_sql_mutations, to_sql_update_validations};

pub(crate) fn derive_effects_from_state_resolution(
    preprocess: &PlannedStatementSet,
    writer_key: Option<&str>,
) -> Result<PlanEffects, PlannerError> {
    let sql_mutations = to_sql_mutations(&preprocess.mutations);
    let sql_update_validations = to_sql_update_validations(&preprocess.update_validations);
    let state_commit_stream_changes =
        state_commit_stream_changes_from_mutations(&sql_mutations, writer_key);
    let next_active_version_id = active_version_from_mutations(&sql_mutations)
        .map_err(PlannerError::preprocess)?
        .or(
            active_version_from_update_validations(&sql_update_validations)
                .map_err(PlannerError::preprocess)?,
        );

    Ok(PlanEffects {
        state_commit_stream_changes,
        next_active_version_id,
    })
}
