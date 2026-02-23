use crate::sql::{
    is_query_only_statements, should_invalidate_installed_plugins_cache_for_statements,
};
use super::super::super::ast::nodes::Statement;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct CanonicalStateResolution {
    pub(crate) read_only_query: bool,
    pub(crate) should_invalidate_installed_plugins_cache: bool,
}

pub(crate) fn canonicalize_state_resolution(statements: &[Statement]) -> CanonicalStateResolution {
    CanonicalStateResolution {
        read_only_query: is_query_only_statements(statements),
        should_invalidate_installed_plugins_cache:
            should_invalidate_installed_plugins_cache_for_statements(statements),
    }
}
