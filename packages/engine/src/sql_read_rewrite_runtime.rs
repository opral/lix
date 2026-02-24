use sqlparser::ast::Query;

use crate::sql::{
    rewrite_read_query_with_backend_and_params_in_session as legacy_rewrite_read_query_with_backend_and_params_in_session,
    ReadRewriteSession as LegacyReadRewriteSession,
};
use crate::{LixBackend, LixError, Value};

#[derive(Debug, Default, Clone)]
pub(crate) struct ReadRewriteSession {
    inner: LegacyReadRewriteSession,
}

impl ReadRewriteSession {
    pub(crate) fn cached_version_chain(&self, version_id: &str) -> Option<&[String]> {
        self.inner.cached_version_chain(version_id)
    }

    pub(crate) fn cache_version_chain(&mut self, version_id: String, chain: Vec<String>) {
        self.inner.cache_version_chain(version_id, chain);
    }
}

pub(crate) async fn rewrite_read_query_with_backend_and_params_in_session(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
    session: &mut ReadRewriteSession,
) -> Result<Query, LixError> {
    legacy_rewrite_read_query_with_backend_and_params_in_session(
        backend,
        query,
        params,
        &mut session.inner,
    )
    .await
}
