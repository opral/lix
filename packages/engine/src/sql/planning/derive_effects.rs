use super::super::contracts::effects::PlanEffects;
use super::super::contracts::planned_statement::PlannedStatementSet;
use super::super::contracts::planner_error::PlannerError;
use super::super::semantics::state_resolution::effects::derive_effects_from_state_resolution;

pub(crate) fn derive_plan_effects(
    output: &PlannedStatementSet,
    writer_key: Option<&str>,
) -> Result<PlanEffects, PlannerError> {
    derive_effects_from_state_resolution(output, writer_key)
}
