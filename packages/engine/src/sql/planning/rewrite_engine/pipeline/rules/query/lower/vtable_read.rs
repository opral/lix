use sqlparser::ast::Query;

use crate::engine::sql::planning::rewrite_engine::steps::vtable_read;
use crate::{LixBackend, LixError};

pub(crate) fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    vtable_read::rewrite_query(query)
}

pub(crate) async fn rewrite_query_with_backend(
    backend: &dyn LixBackend,
    query: Query,
) -> Result<Option<Query>, LixError> {
    vtable_read::rewrite_query_with_backend(backend, query).await
}
