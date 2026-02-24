use super::super::super::ast::nodes::Statement;
use super::super::super::contracts::requirements::PlanRequirements;

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
        should_invalidate_installed_plugins_cache: optimized
            .should_invalidate_installed_plugins_cache,
    }
}
