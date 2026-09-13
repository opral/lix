use super::*;
fn merge_error(error: LixError) -> ApiError {
    let status = match error.code.as_str() {
        "LIX_NATIVE_UPLOAD_ATTEMPT_EXPIRED" => StatusCode::GONE,
        "LIX_MIGRATION_GLOBAL_ATTEMPT_RESTARTED"
        | "LIX_PARTIAL_ATTEMPT_RESTARTED"
        | "LIX_PARTIAL_MERGE_CONFLICT"
        | "LIX_PARTIAL_MERGE_AUTHORITY_CHANGED"
        | "LIX_PARTIAL_MERGE_ATTEMPT_REUSED" => StatusCode::CONFLICT,
        "LIX_SYNC_ACCOUNT_MISMATCH" => return ApiError::account_mismatch(),
        "LIX_MIGRATION_GLOBAL_RESTART_INVALID"
        | "LIX_MIGRATION_GLOBAL_CLEANUP_UNRESOLVED"
        | "LIX_MIGRATION_GLOBAL_SCOPE_UNSUPPORTED"
        | "LIX_MIGRATION_GLOBAL_BODY_INVALID"
        | "LIX_MIGRATION_GLOBAL_RETENTION_INVALID"
        | "LIX_PARTIAL_ATTEMPT_RESTART_INVALID"
        | "LIX_PARTIAL_MERGE_PROTOCOL_INVALID"
        | "LIX_PARTIAL_UPLOAD_ATTEMPT_INVALID"
        | "LIX_NATIVE_UPLOAD_ATTEMPT_INVALID"
        | "LIX_PARTIAL_MERGE_SCOPE_UNSUPPORTED"
        | "LIX_PARTIAL_MERGE_BUDGET_EXCEEDED" => StatusCode::BAD_REQUEST,
        _ => return ApiError::from(error),
    };
    ApiError {
        status,
        body: ErrorEnvelope::from_lix_error(&error),
    }
}
pub(super) fn retained_bodies<S>(
    lease: SessionLease<S>,
    Json(request): Json<crate::sync::RetainedBodyWaveRequest>,
) -> SqlHandlerFuture<Response>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    Box::pin(async move {
        request.validate().map_err(merge_error)?;
        ensure_sync_push_event_fits(&request.bodies, MAX_SYNC_PULL_RESPONSE_BYTES)?;
        let account = lease.record.principal.account_id().to_owned();
        let response = lease
            .run_durable(move |lix| async move {
                lix.push_retained_body_wave_for_account(&request, &account)
                    .await
            })
            .await
            .map_err(merge_error)?;
        bounded_sync_json_response(response, "retained body acknowledgment", 2048)
    })
}
pub(super) fn merge<S>(
    lease: SessionLease<S>,
    Json(request): Json<crate::sync::PartialMergeRequest>,
) -> SqlHandlerFuture<Response>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    Box::pin(async move {
        request.validate().map_err(merge_error)?;
        let account = lease.record.principal.account_id().to_owned();
        let response = lease
            .run_durable(move |lix| async move {
                lix.merge_partial_replica_for_account(&request, &account)
                    .await
            })
            .await
            .map_err(merge_error)?;
        bounded_sync_json_response(response, "authority merge receipt", 4096)
    })
}

pub(super) fn restart<S>(
    lease: SessionLease<S>,
    Json(request): Json<crate::sync::PartialAttemptRestartRequest>,
) -> SqlHandlerFuture<Response>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    Box::pin(async move {
        request.validate().map_err(merge_error)?;
        let account = lease.record.principal.account_id().to_owned();
        let response = lease
            .run_durable(move |lix| async move {
                lix.restart_partial_attempt_for_account(&request, &account)
                    .await
            })
            .await
            .map_err(merge_error)?;
        bounded_sync_json_response(response, "partial merge restart outcome", 4096)
    })
}

pub(super) fn native_migration_merge<S>(
    lease: SessionLease<S>,
    Json(request): Json<crate::sync::NativeMigrationMergeRequest>,
) -> SqlHandlerFuture<Response>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    Box::pin(async move {
        request.validate().map_err(merge_error)?;
        let account = lease.record.principal.account_id().to_owned();
        let response = lease
            .run_durable(move |lix| async move {
                Box::pin(lix.merge_native_migration_for_account(
                    &request.request,
                    &account,
                    &request.source_branch_id,
                ))
                .await
            })
            .await
            .map_err(merge_error)?;
        bounded_sync_json_response(response, "authority merge receipt", 4096)
    })
}

pub(super) fn native_migration_cleanup<S>(
    lease: SessionLease<S>,
    Json(request): Json<crate::sync::NativeMigrationCleanupRequest>,
) -> SqlHandlerFuture<Response>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    Box::pin(async move {
        request.validate().map_err(merge_error)?;
        let account = lease.record.principal.account_id().to_owned();
        let response = lease
            .run_durable(move |lix| async move {
                Box::pin(lix.cleanup_native_migration_for_account(&request, &account)).await
            })
            .await
            .map_err(merge_error)?;
        bounded_sync_json_response(response, "authority merge receipt", 4096)
    })
}

#[cfg(not(target_arch = "wasm32"))]
type GlobalMigrationResponseFuture =
    futures_util::future::BoxFuture<'static, Result<Response, ApiError>>;
#[cfg(target_arch = "wasm32")]
type GlobalMigrationResponseFuture =
    futures_util::future::LocalBoxFuture<'static, Result<Response, ApiError>>;

// Box behind an ordinary function boundary. Boxing an async function's result
// in the router can still reserve its full construction temporary on that
// router's poll stack, including while an unrelated route is executing.
pub(super) fn native_global_migration_merge<S>(
    lease: SessionLease<S>,
    Json(request): Json<crate::sync::NativeGlobalMigrationRequest>,
) -> GlobalMigrationResponseFuture
where
    S: Storage + Clone + Send + Sync + 'static,
{
    Box::pin(async move {
        request.validate().map_err(merge_error)?;
        let account = lease.record.principal.account_id().to_owned();
        let response = lease
            .run_durable(move |lix| async move {
                Box::pin(lix.merge_native_global_migration_for_account(&request, &account)).await
            })
            .await
            .map_err(merge_error)?;
        bounded_sync_json_response(response, "global migration native outcome", 256 * 1024)
    })
}
pub(super) fn native_global_migration_bodies<S>(
    lease: SessionLease<S>,
    Json(request): Json<crate::sync::NativeGlobalBodyWaveRequest>,
) -> GlobalMigrationResponseFuture
where
    S: Storage + Clone + Send + Sync + 'static,
{
    Box::pin(async move {
        request.validate().map_err(merge_error)?;
        ensure_sync_push_event_fits(&request.bodies, MAX_SYNC_PULL_RESPONSE_BYTES)?;
        let account = lease.record.principal.account_id().to_owned();
        let response = lease
            .run_durable(move |lix| async move {
                Box::pin(lix.push_global_migration_body_wave_for_account(&request, &account)).await
            })
            .await
            .map_err(merge_error)?;
        bounded_sync_json_response(response, "global migration body acknowledgement", 4096)
    })
}
pub(super) fn native_global_migration_restart<S>(
    lease: SessionLease<S>,
    Json(request): Json<crate::sync::NativeGlobalRestartRequest>,
) -> GlobalMigrationResponseFuture
where
    S: Storage + Clone + Send + Sync + 'static,
{
    Box::pin(async move {
        request.validate().map_err(merge_error)?;
        let account = lease.record.principal.account_id().to_owned();
        let response = lease
            .run_durable(move |lix| async move {
                Box::pin(lix.restart_native_global_migration_for_account(&request, &account)).await
            })
            .await
            .map_err(merge_error)?;
        bounded_sync_json_response(response, "global migration restart outcome", 256 * 1024)
    })
}
pub(super) fn native_global_migration_cleanup<S>(
    lease: SessionLease<S>,
    Json(request): Json<crate::sync::NativeGlobalMigrationRequest>,
) -> GlobalMigrationResponseFuture
where
    S: Storage + Clone + Send + Sync + 'static,
{
    Box::pin(async move {
        request.validate().map_err(merge_error)?;
        let account = lease.record.principal.account_id().to_owned();
        let response = lease
            .run_durable(move |lix| async move {
                Box::pin(lix.cleanup_native_global_migration_for_account(&request, &account)).await
            })
            .await
            .map_err(merge_error)?;
        bounded_sync_json_response(response, "global migration cleanup acknowledgement", 4096)
    })
}
