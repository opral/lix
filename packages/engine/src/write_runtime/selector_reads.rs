use crate::contracts::projection::ProjectionRegistry;
use crate::contracts::traits::{PendingPublicReadBackend, PendingView};
use crate::read_runtime::execute_prepared_public_read_artifact_with_backend;
use crate::sql::prepare::{
    load_sql_compiler_metadata, prepare_public_read_artifact,
    try_prepare_public_read_with_registry_and_internal_access,
};
use crate::version::context::load_target_version_history_root_commit_id_with_backend;
use crate::{LixBackend, LixError, QueryResult, Value};
use sqlparser::ast::{Query, Statement};

pub(crate) async fn execute_public_query_with_optional_pending_transaction_view(
    backend: &dyn LixBackend,
    projection_registry: &ProjectionRegistry,
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
        None => crate::schema::load_public_surface_registry_with_backend(backend).await?,
    };
    let statement = Statement::Query(Box::new(query));
    let active_history_root_commit_id = load_target_version_history_root_commit_id_with_backend(
        backend,
        Some(active_version_id),
        "active_version_id",
    )
    .await?;
    let compiler_metadata = load_sql_compiler_metadata(backend, &registry).await?;
    let prepared = try_prepare_public_read_with_registry_and_internal_access(
        backend.dialect(),
        &registry,
        &compiler_metadata,
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
    let artifact = prepare_public_read_artifact(&public_read, backend.dialect())?;
    match pending_transaction_view {
        Some(pending_transaction_view) => {
            backend
                .execute_prepared_public_read_with_pending_view(
                    Some(pending_transaction_view),
                    projection_registry,
                    &artifact,
                )
                .await
        }
        None => {
            execute_prepared_public_read_artifact_with_backend(
                backend,
                projection_registry,
                &artifact,
            )
            .await
        }
    }
}
