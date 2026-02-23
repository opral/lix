use super::predicates::statement_targets_state_history;
use crate::{LixBackend, LixError};
use sqlparser::ast::Query;

pub(crate) fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    Ok(Some(query))
}

pub(crate) async fn rewrite_query_with_backend(
    _backend: &dyn LixBackend,
    query: Query,
) -> Result<Option<Query>, LixError> {
    Ok(Some(query))
}

pub(crate) fn statement_targets_history_view(statement: &sqlparser::ast::Statement) -> bool {
    statement_targets_state_history(statement)
}
