use datafusion::prelude::{SessionConfig, SessionContext};
use std::collections::BTreeSet;
use std::sync::Arc;

use crate::LixError;
use crate::branch::{BranchHead, BranchRefReader};

use super::branch_ref::CachingBranchRefReader;
use super::providers;
use super::udfs::register_sql2_functions;
use super::{SqlExecutionContext, SqlWriteContext, SqlWriteExecutionContext};

pub(crate) async fn build_read_session<C>(ctx: &C) -> Result<SessionContext, LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    build_read_session_with_active_head(ctx, None).await
}

pub(crate) async fn build_read_session_at_head<C>(
    ctx: &C,
    active_head: BranchHead,
) -> Result<SessionContext, LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    build_read_session_with_active_head(ctx, Some(active_head)).await
}

async fn build_read_session_with_active_head<C>(
    ctx: &C,
    active_head: Option<BranchHead>,
) -> Result<SessionContext, LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    let session = new_sql_session_context();
    let branch_ref: Arc<dyn BranchRefReader> = match active_head.as_ref() {
        Some(head) => {
            if head.branch_id != ctx.active_branch_id() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "prepared SQL read head does not match the active branch",
                ));
            }
            Arc::new(CachingBranchRefReader::with_head(
                ctx.branch_ref(),
                head.clone(),
            ))
        }
        None => Arc::new(CachingBranchRefReader::new(ctx.branch_ref())),
    };
    let active_branch_commit_id = match active_head {
        Some(head) => Some(head.commit_id.to_string()),
        None => branch_ref
            .load_head(ctx.active_branch_id())
            .await?
            .map(|head| head.commit_id.to_string()),
    };
    register_sql2_functions(&session, ctx.functions(), active_branch_commit_id);
    providers::register_read(&session, ctx, branch_ref).await?;

    Ok(session)
}

pub(crate) async fn build_transaction_read_session<C>(
    read_ctx: &C,
    write_ctx: &mut dyn SqlWriteExecutionContext,
) -> Result<SessionContext, LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    let session = new_sql_session_context();
    let read_branch_ref: Arc<dyn BranchRefReader> =
        Arc::new(CachingBranchRefReader::new(read_ctx.branch_ref()));
    let active_branch_commit_id = read_branch_ref
        .load_head(read_ctx.active_branch_id())
        .await?
        .map(|head| head.commit_id.to_string());
    register_sql2_functions(&session, read_ctx.functions(), active_branch_commit_id);
    let write_ctx = SqlWriteContext::new(write_ctx);
    let write_branch_ref: Arc<dyn BranchRefReader> = Arc::new(CachingBranchRefReader::new(
        Arc::new(super::WriteContextBranchRefReader::new(write_ctx.clone())),
    ));
    providers::register_transaction(
        &session,
        read_ctx,
        read_branch_ref,
        write_ctx,
        write_branch_ref,
        SqlWriteSessionOptions::default(),
    )
    .await?;
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
    let branch_ref: Arc<dyn BranchRefReader> = Arc::new(CachingBranchRefReader::new(Arc::new(
        super::WriteContextBranchRefReader::new(write_ctx.clone()),
    )));
    let active_branch_commit_id =
        branch_ref
            .load_head(&active_branch_id)
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
        Some(active_branch_commit_id.commit_id.to_string()),
    );
    providers::register_write(&session, write_ctx, branch_ref, options).await?;

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
