use super::super::super::ast::nodes::Statement;

use super::canonical::{statement_targets_table_name, CanonicalStateResolution};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct OptimizedStateResolution {
    pub(crate) read_only_query: bool,
    pub(crate) should_refresh_file_cache: bool,
    pub(crate) should_invalidate_installed_plugins_cache: bool,
}

pub(crate) fn optimize_state_resolution(
    statements: &[Statement],
    canonical: CanonicalStateResolution,
) -> OptimizedStateResolution {
    OptimizedStateResolution {
        read_only_query: canonical.read_only_query,
        should_refresh_file_cache: !canonical.read_only_query
            && should_refresh_file_cache_for_statements(statements),
        should_invalidate_installed_plugins_cache: canonical
            .should_invalidate_installed_plugins_cache,
    }
}

pub(crate) fn should_refresh_file_cache_for_statements(statements: &[Statement]) -> bool {
    statements
        .iter()
        .any(statement_targets_file_cache_refresh_table)
}

fn statement_targets_file_cache_refresh_table(statement: &Statement) -> bool {
    statement_targets_table_name(statement, "lix_state")
        || statement_targets_table_name(statement, "lix_state_by_version")
}
