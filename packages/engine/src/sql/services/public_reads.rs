use crate::contracts::surface::SurfaceRegistry;
use crate::contracts::traits::{PendingPublicReadBackend, PendingView};
use crate::sql::executor::{
    execute_prepared_public_read, try_prepare_public_read_with_registry_and_internal_access,
};
use crate::{LixBackend, LixError, QueryResult, Value};
use sqlparser::ast::{Query, Statement};

pub(crate) async fn execute_public_query_with_optional_pending_transaction_view(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
    pending_transaction_view: Option<&dyn PendingView>,
) -> Result<QueryResult, LixError> {
    let registry = match pending_transaction_view {
        Some(pending_transaction_view) => {
            backend
                .bootstrap_public_surface_registry_with_pending_view(Some(pending_transaction_view))
                .await?
        }
        None => SurfaceRegistry::bootstrap_with_backend(backend).await?,
    };
    let statement = Statement::Query(Box::new(query));
    let prepared = try_prepare_public_read_with_registry_and_internal_access(
        backend,
        &registry,
        &[statement],
        params,
        active_version_id,
        writer_key,
        false,
        None,
    )
    .await?;
    let Some(public_read) = prepared else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "public write selector resolver expected a public read plan",
        ));
    };
    match pending_transaction_view {
        Some(pending_transaction_view) => {
            backend
                .execute_prepared_public_read_with_pending_view(
                    Some(pending_transaction_view),
                    &public_read,
                )
                .await
        }
        None => execute_prepared_public_read(backend, &public_read).await,
    }
}
