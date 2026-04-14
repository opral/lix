use crate::sql::analysis::state_resolution::effects::derive_effects_from_state_resolution;
use crate::sql::prepare::contracts::effects::PlanEffects;
use crate::sql::prepare::contracts::planner_error::PlannerError;
use crate::sql::PlannedStatementSet;

pub(crate) fn derive_plan_effects(
    output: &PlannedStatementSet,
    writer_key: Option<&str>,
) -> Result<PlanEffects, PlannerError> {
    derive_effects_from_state_resolution(output, writer_key)
}
