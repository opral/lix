use super::predicates::statement_targets_file_history;
use crate::filesystem::select_rewrite;
use crate::LixError;
use crate::Value;
use sqlparser::ast::Query;

pub(crate) fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    select_rewrite::rewrite_query(query)
}

pub(crate) fn rewrite_query_with_params(
    query: Query,
    params: &[Value],
) -> Result<Option<Query>, LixError> {
    select_rewrite::rewrite_query_with_params(query, params)
}

pub(crate) fn statement_targets_history_view(statement: &sqlparser::ast::Statement) -> bool {
    statement_targets_file_history(statement)
}
