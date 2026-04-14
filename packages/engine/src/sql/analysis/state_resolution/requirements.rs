use crate::sql::optimizer::optimize_state_resolution;
use crate::sql::prepare::contracts::requirements::PlanRequirements;
use crate::sql::prepare::derive_statement_effects;
use sqlparser::ast::Statement;

use super::canonical::canonicalize_state_resolution;

pub(crate) fn derive_requirements_from_state_resolution(
    statements: &[Statement],
) -> PlanRequirements {
    let canonical = canonicalize_state_resolution(statements);
    let optimized = optimize_state_resolution(statements, canonical).optimized;

    PlanRequirements {
        read_only_query: optimized.read_only_query,
        should_refresh_file_cache: optimized.should_refresh_file_cache,
        statement_effects: derive_statement_effects(statements),
    }
}
