use sqlparser::ast::Query;

use crate::engine::sql::planning::rewrite_engine::steps::vtable_read;
use crate::{LixBackend, LixError, Value};

pub(crate) fn rewrite_query(query: Query, params: &[Value]) -> Result<Option<Query>, LixError> {
    vtable_read::rewrite_query(query, params)
}

pub(crate) async fn rewrite_query_with_backend(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
) -> Result<Option<Query>, LixError> {
    vtable_read::rewrite_query_with_backend(backend, query, params).await
}
