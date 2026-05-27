use datafusion::prelude::{SessionConfig, SessionContext};
use std::collections::BTreeSet;

use crate::LixError;

use super::providers;
use super::udfs::register_sql2_functions;
use super::{SqlExecutionContext, SqlWriteContext, SqlWriteExecutionContext};

pub(crate) async fn build_read_session<C>(ctx: &C) -> Result<SessionContext, LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    let session = new_sql_session_context();
    let branch_ref = ctx.branch_ref();
    let active_branch_commit_id = branch_ref
        .load_head(ctx.active_branch_id())
        .await?
        .map(|head| head.commit_id);
    register_sql2_functions(&session, ctx.functions(), active_branch_commit_id);
    providers::register_read(&session, ctx).await?;

    Ok(session)
}

pub(crate) async fn build_transaction_read_session<C>(
    read_ctx: &C,
    write_ctx: &mut dyn SqlWriteExecutionContext,
) -> Result<SessionContext, LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    let session = build_read_session(read_ctx).await?;
    let write_ctx = SqlWriteContext::new(write_ctx);
    providers::register_write(&session, write_ctx, SqlWriteSessionOptions::default()).await?;
    Ok(session)
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SqlWriteSessionOptions {
    pub(crate) omitted_insert_columns: BTreeSet<String>,
}

pub(crate) async fn build_write_session_with_options(
    ctx: &mut dyn SqlWriteExecutionContext,
    options: SqlWriteSessionOptions,
) -> Result<SessionContext, LixError> {
    let session = new_sql_session_context();
    let write_ctx = SqlWriteContext::new(ctx);
    let active_branch_id = write_ctx.active_branch_id();
    let active_branch_commit_id = write_ctx
        .load_branch_head(&active_branch_id)
        .await?
        .ok_or_else(|| {
            LixError::branch_not_found(
                active_branch_id.clone(),
                "build SQL write session",
                "active branch",
            )
        })?;
    register_sql2_functions(
        &session,
        write_ctx.functions(),
        Some(active_branch_commit_id),
    );
    providers::register_write(&session, write_ctx, options).await?;

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
