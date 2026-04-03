use crate::contracts::surface::SurfaceRegistry;
use crate::contracts::traits::{PendingPublicReadBackend, PendingView};
use crate::read_runtime::execute_prepared_public_read_artifact_with_backend;
use crate::sql::prepare::{
    prepare_public_read_artifact, try_prepare_public_read_with_registry_and_internal_access,
};
use crate::version::context::load_target_version_history_root_commit_id_with_backend;
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
    let active_history_root_commit_id = load_target_version_history_root_commit_id_with_backend(
        backend,
        Some(active_version_id),
        "active_version_id",
    )
    .await?;
    let prepared = try_prepare_public_read_with_registry_and_internal_access(
        backend,
        &registry,
        &[statement],
        params,
        active_version_id,
        active_history_root_commit_id.as_deref(),
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
        None => {
            let artifact = prepare_public_read_artifact(&public_read, backend.dialect())?;
            execute_prepared_public_read_artifact_with_backend(backend, &artifact).await
        }
    }
}
