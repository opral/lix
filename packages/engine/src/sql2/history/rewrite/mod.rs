pub(crate) mod directory_history;
pub(crate) mod file_history;
pub(crate) mod predicates;
pub(crate) mod state_history;

use crate::sql as legacy_sql;
use crate::{LixBackend, LixError, Value};
use sqlparser::ast::Query;

pub(crate) type ReadRewriteSession = legacy_sql::ReadRewriteSession;

pub(crate) async fn rewrite_read_query_with_backend_and_params_in_session(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
    session: &mut ReadRewriteSession,
) -> Result<Query, LixError> {
    legacy_sql::rewrite_read_query_with_backend_and_params_in_session(backend, query, params, session)
        .await
}
