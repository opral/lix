use crate::sql::should_refresh_file_cache_for_statements;
use super::super::super::ast::nodes::Statement;

use super::canonical::CanonicalStateResolution;

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
