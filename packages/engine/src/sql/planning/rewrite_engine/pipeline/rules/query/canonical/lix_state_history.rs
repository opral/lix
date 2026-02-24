use sqlparser::ast::Query;

use crate::engine::sql::planning::rewrite_engine::steps::lix_state_history_view_read;
use crate::{LixBackend, LixError, Value};

pub(crate) fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    lix_state_history_view_read::rewrite_query(query)
}

pub(crate) async fn rewrite_query_with_backend(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
) -> Result<Option<Query>, LixError> {
    lix_state_history_view_read::rewrite_query_with_backend(backend, query, params).await
}
