use crate::sql::{active_version_from_mutations, active_version_from_update_validations};
use crate::state_commit_stream::state_commit_stream_changes_from_mutations;

use super::super::contracts::effects::PlanEffects;
use super::super::contracts::planned_statement::PlannedStatementSet;
use super::super::contracts::planner_error::PlannerError;
use super::super::type_bridge::{to_sql_mutations, to_sql_update_validations};

pub(crate) fn derive_plan_effects(
    output: &PlannedStatementSet,
    writer_key: Option<&str>,
) -> Result<PlanEffects, PlannerError> {
    let sql_mutations = to_sql_mutations(&output.mutations);
    let sql_update_validations = to_sql_update_validations(&output.update_validations);
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
