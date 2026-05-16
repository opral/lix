use datafusion::prelude::{SessionConfig, SessionContext};

use crate::LixError;

use super::providers;
use super::udfs::register_sql2_functions;
use super::{SqlExecutionContext, SqlWriteContext, SqlWriteExecutionContext};

pub(crate) async fn build_read_session(
    ctx: &dyn SqlExecutionContext,
) -> Result<SessionContext, LixError> {
    let session = new_sql_session_context();
    let version_ref = ctx.version_ref();
    let active_version_commit_id = version_ref
        .load_head(ctx.active_version_id())
        .await?
        .map(|head| head.commit_id);
    register_sql2_functions(&session, ctx.functions(), active_version_commit_id);
    providers::register_read(&session, ctx).await?;

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
    providers::register_write(&session, write_ctx).await?;

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
