use std::sync::Arc;

use datafusion::prelude::{SessionConfig, SessionContext};

use crate::LixError;

use super::change_provider::register_lix_change_provider;
use super::directory_history_provider::register_lix_directory_history_provider;
use super::directory_provider::{
    register_lix_directory_providers, register_lix_directory_write_providers,
};
use super::entity_provider::{register_entity_providers, register_entity_write_providers};
use super::file_history_provider::register_lix_file_history_provider;
use super::file_provider::{register_lix_file_providers, register_lix_file_write_providers};
use super::history_provider::register_history_providers;
use super::lix_state_provider::{register_lix_state_providers, register_lix_state_write_providers};
use super::udfs::register_sql2_functions;
use super::version_provider::{register_lix_version_provider, register_lix_version_write_provider};
use super::{SqlExecutionContext, SqlWriteContext, SqlWriteExecutionContext};

pub(crate) async fn build_read_session<C>(ctx: &C) -> Result<SessionContext, LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    let session = new_sql_session_context();
    let version_ref = ctx.version_ref();
    let active_version_commit_id = version_ref
        .load_head(ctx.active_version_id())
        .await?
        .map(|head| head.commit_id);
    register_sql2_functions(&session, ctx.functions(), active_version_commit_id);
    register_lix_state_providers(
        &session,
        ctx.active_version_id(),
        ctx.live_state(),
        Arc::clone(&version_ref),
    )
    .await?;
    register_lix_version_provider(&session, ctx.live_state(), Arc::clone(&version_ref)).await?;
    let commit_store_query_source = ctx.commit_store_query_source();
    register_lix_change_provider(&session, commit_store_query_source.clone()).await?;
    let state_history_commit_graph = ctx.commit_graph();
    register_history_providers(
        &session,
        state_history_commit_graph,
        commit_store_query_source.clone(),
    )
    .await?;
    let file_history_commit_graph = ctx.commit_graph();
    register_lix_file_history_provider(
        &session,
        file_history_commit_graph,
        commit_store_query_source.clone(),
        ctx.blob_reader(),
    )
    .await?;
    let directory_history_commit_graph = ctx.commit_graph();
    register_lix_directory_history_provider(
        &session,
        directory_history_commit_graph,
        commit_store_query_source.clone(),
    )
    .await?;
    let entity_commit_graph = Arc::new(tokio::sync::Mutex::new(ctx.commit_graph()));
    register_lix_directory_providers(
        &session,
        ctx.active_version_id(),
        ctx.live_state(),
        Arc::clone(&version_ref),
        ctx.functions(),
    )
    .await?;
    register_lix_file_providers(
        &session,
        ctx.active_version_id(),
        ctx.live_state(),
        Arc::clone(&version_ref),
        ctx.blob_reader(),
        ctx.functions(),
    )
    .await?;
    register_entity_providers(
        &session,
        ctx.active_version_id(),
        ctx.live_state(),
        Arc::clone(&version_ref),
        entity_commit_graph,
        commit_store_query_source,
        &ctx.list_visible_schemas()?,
    )
    .await?;

    Ok(session)
}

pub(crate) async fn build_write_session(
    ctx: &mut dyn SqlWriteExecutionContext,
) -> Result<SessionContext, LixError> {
    let session = new_sql_session_context();
    let write_ctx = SqlWriteContext::new(ctx);
    let active_version_commit_id = write_ctx
        .load_version_head(&write_ctx.active_version_id())
        .await?;
    register_sql2_functions(&session, write_ctx.functions(), active_version_commit_id);

    register_lix_state_write_providers(&session, write_ctx.clone()).await?;
    register_lix_version_write_provider(&session, write_ctx.clone()).await?;

    register_lix_directory_write_providers(&session, write_ctx.clone()).await?;
    register_lix_file_write_providers(&session, write_ctx.clone()).await?;
    register_entity_write_providers(
        &session,
        write_ctx.clone(),
        &write_ctx.list_visible_schemas()?,
    )
    .await?;

    Ok(session)
}

pub(crate) fn new_sql_session_context() -> SessionContext {
    SessionContext::new_with_config(
        SessionConfig::new()
            .with_information_schema(true)
            .with_target_partitions(1)
            .set_bool("datafusion.optimizer.repartition_aggregations", false)
            .set_bool("datafusion.optimizer.repartition_joins", false)
            .set_bool("datafusion.optimizer.repartition_sorts", false)
            .set_bool("datafusion.optimizer.repartition_windows", false)
            .set_bool("datafusion.optimizer.repartition_file_scans", false)
            .set_bool("datafusion.optimizer.enable_round_robin_repartition", false),
    )
}
