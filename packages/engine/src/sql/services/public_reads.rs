use crate::sql::catalog::SurfaceRegistry;
use crate::sql::executor::public_runtime::{
    execute_prepared_public_read, try_prepare_public_read_with_registry_and_internal_access,
};
use crate::sql::services::pending_reads::{
    bootstrap_public_surface_registry_with_pending_transaction_view,
    execute_prepared_public_read_with_pending_transaction_view,
};
use crate::transaction::PendingTransactionView;
use crate::{LixBackend, LixError, QueryResult, Value};
use sqlparser::ast::{Query, Statement};

pub(crate) async fn execute_public_query_with_optional_pending_transaction_view(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
    pending_transaction_view: Option<&PendingTransactionView>,
) -> Result<QueryResult, LixError> {
    let registry = match pending_transaction_view {
        Some(pending_transaction_view) => {
            bootstrap_public_surface_registry_with_pending_transaction_view(
                backend,
                Some(pending_transaction_view),
            )
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
            execute_prepared_public_read_with_pending_transaction_view(
                backend,
                Some(pending_transaction_view),
                &public_read,
            )
            .await
        }
        None => execute_prepared_public_read(backend, &public_read).await,
    }
}
