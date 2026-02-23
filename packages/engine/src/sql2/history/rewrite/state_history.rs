use super::predicates::statement_targets_state_history;
use crate::sql::rewrite_read_query_with_backend;
use crate::{LixBackend, LixError};
use sqlparser::ast::Query;

pub(crate) fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    Ok(Some(query))
}

pub(crate) async fn rewrite_query_with_backend(
    backend: &dyn LixBackend,
    query: Query,
) -> Result<Option<Query>, LixError> {
    rewrite_read_query_with_backend(backend, query)
        .await
        .map(Some)
}

pub(crate) fn statement_targets_history_view(statement: &sqlparser::ast::Statement) -> bool {
    statement_targets_state_history(statement)
}
