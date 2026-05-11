use crate::functions::FunctionContext;
use crate::session::context::{SessionContext, SessionSqlExecutionContext};
use crate::sql2::{self, SqlLogicalPlan};
use crate::storage::StorageReadScope;
use crate::transaction::open_transaction;
use crate::{LixError, SqlQueryResult, Value};

/// Opaque read plan used by the Optimization 9 SQL2 diagnostic benchmark.
///
/// This module is gated behind `storage-benches` and exists only to split SQL2
/// planning cost from SQL2 execution cost without widening the normal session
/// API.
pub struct PreparedReadPlan {
    plan: SqlLogicalPlan,
    read_scope:
        StorageReadScope<Box<dyn crate::storage::StorageReadTransaction + Send + Sync + 'static>>,
    runtime_functions: FunctionContext,
}

pub async fn plan_read_only(session: &SessionContext, sql: &str) -> Result<(), LixError> {
    let prepared = prepare_read_plan(session, sql).await?;
    drop(prepared.plan);
    drop(prepared.runtime_functions);
    prepared.read_scope.rollback().await
}

pub async fn plan_write_only(session: &SessionContext, sql: &str) -> Result<(), LixError> {
    session.ensure_open()?;
    let opened = open_transaction(
        &session.mode,
        session.storage.clone(),
        std::sync::Arc::clone(&session.live_state),
        std::sync::Arc::clone(&session.tracked_state),
        std::sync::Arc::clone(&session.binary_cas),
        std::sync::Arc::clone(&session.commit_store),
        std::sync::Arc::clone(&session.version_ctx),
        std::sync::Arc::clone(&session.catalog_context),
    )
    .await?;
    let mut transaction = opened.transaction;
    let runtime_functions = opened.runtime_functions;
    let plan = sql2::create_write_logical_plan(&mut transaction, sql).await?;
    drop(plan);
    drop(runtime_functions);
    transaction.rollback().await
}

pub async fn prepare_read_plan(
    session: &SessionContext,
    sql: &str,
) -> Result<PreparedReadPlan, LixError> {
    session.ensure_open()?;
    let read_scope = StorageReadScope::new(session.storage.begin_read_transaction().await?);
    let mut read_store = read_scope.store();
    let live_state: std::sync::Arc<dyn crate::live_state::LiveStateReader> =
        std::sync::Arc::new(session.live_state.reader(read_store.clone()));
    let runtime_functions = FunctionContext::prepare(live_state.as_ref()).await?;
    let functions = runtime_functions.provider();
    let active_version_id = session
        .active_version_id_from_reader(&mut read_store)
        .await?;
    let visible_schemas = session
        .catalog_context
        .schema_jsons_for_sql_read_planning(live_state.as_ref(), &active_version_id)
        .await?;
    let ctx = SessionSqlExecutionContext {
        active_version_id: &active_version_id,
        read_store,
        live_state: std::sync::Arc::clone(&session.live_state),
        binary_cas: std::sync::Arc::clone(&session.binary_cas),
        commit_store: std::sync::Arc::clone(&session.commit_store),
        version_ctx: std::sync::Arc::clone(&session.version_ctx),
        visible_schemas,
        functions: functions.clone(),
    };
    let plan = sql2::create_logical_plan(&ctx, sql).await?;
    drop(ctx);
    drop(live_state);

    Ok(PreparedReadPlan {
        plan,
        read_scope,
        runtime_functions,
    })
}

pub async fn execute_read_plan(
    prepared: PreparedReadPlan,
    params: &[Value],
) -> Result<SqlQueryResult, LixError> {
    let PreparedReadPlan {
        plan,
        read_scope,
        runtime_functions,
    } = prepared;
    let result = sql2::execute_logical_plan(plan, params).await;
    read_scope.rollback().await?;
    drop(runtime_functions);
    result
}
