use std::collections::BTreeSet;

use super::super::contracts::effects::PlanEffects;
use super::super::contracts::planned_statement::PlannedStatementSet;
use super::super::contracts::planner_error::PlannerError;
use super::super::semantics::state_resolution::effects::derive_effects_from_state_resolution;

pub(crate) fn derive_plan_effects(
    output: &PlannedStatementSet,
    writer_key: Option<&str>,
    pending_file_delete_targets: &BTreeSet<(String, String)>,
    authoritative_pending_file_write_targets: &BTreeSet<(String, String)>,
) -> Result<PlanEffects, PlannerError> {
    derive_effects_from_state_resolution(
        output,
        writer_key,
        pending_file_delete_targets,
        authoritative_pending_file_write_targets,
    )
}
