use sqlparser::ast::Statement;

use crate::sql::{
    is_query_only_statements, should_invalidate_installed_plugins_cache_for_statements,
    should_refresh_file_cache_for_statements,
};

use super::super::contracts::requirements::PlanRequirements;

pub(crate) fn derive_plan_requirements(statements: &[Statement]) -> PlanRequirements {
    let read_only_query = is_query_only_statements(statements);
    PlanRequirements {
        read_only_query,
        should_refresh_file_cache: !read_only_query
            && should_refresh_file_cache_for_statements(statements),
        should_invalidate_installed_plugins_cache:
            should_invalidate_installed_plugins_cache_for_statements(statements),
    }
}
