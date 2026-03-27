use crate::sql::execution::contracts::requirements::PlanRequirements;
use crate::sql::execution::runtime_state::derive_execution_runtime_effects;
use sqlparser::ast::Statement;

use super::canonical::canonicalize_state_resolution;
use super::optimize::optimize_state_resolution;

pub(crate) fn derive_requirements_from_state_resolution(
    statements: &[Statement],
) -> PlanRequirements {
    let canonical = canonicalize_state_resolution(statements);
    let optimized = optimize_state_resolution(statements, canonical);

    PlanRequirements {
        read_only_query: optimized.read_only_query,
        should_refresh_file_cache: optimized.should_refresh_file_cache,
        runtime_effects: derive_execution_runtime_effects(statements),
    }
}
