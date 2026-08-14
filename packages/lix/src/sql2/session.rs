use datafusion::catalog::CatalogProviderList;
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::parser::Statement as DataFusionStatement;
use std::collections::BTreeSet;
use std::ops::Deref;
use std::sync::Arc;

use crate::LixError;
use crate::branch::{BranchHead, BranchRefReader};
use crate::storage_adapter::StorageAdapterRead;

use super::branch_ref::PreparedBranchRefReader;
use super::providers;
use super::udfs::{
    ExecutionSlots, bind_execution_sql2_functions, register_execution_sql2_functions,
    register_static_sql2_functions,
};
use super::{SqlExecutionContext, SqlWriteContext, SqlWriteExecutionContext};

pub(crate) async fn build_read_session<C>(
    ctx: &C,
    statements: &[DataFusionStatement],
) -> Result<SessionContext, LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    build_read_session_with_active_head(ctx, None, statements).await
}

pub(crate) async fn build_read_session_at_head<C>(
    ctx: &C,
    active_head: BranchHead,
    statements: &[DataFusionStatement],
) -> Result<SessionContext, LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    build_read_session_with_active_head(ctx, Some(active_head), statements).await
}

async fn build_read_session_with_active_head<C>(
    ctx: &C,
    active_head: Option<BranchHead>,
    statements: &[DataFusionStatement],
) -> Result<SessionContext, LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    let session = ctx.datafusion_read_session();
    let branch_ref: Arc<dyn BranchRefReader> = match active_head.as_ref() {
        Some(head) => {
            if head.branch_id != ctx.active_branch_id() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "prepared SQL read head does not match the active branch",
                ));
            }
            let branch_ref = Arc::new(PreparedBranchRefReader::new(ctx.branch_ref(), head.clone()));
            match branch_ref.load_head(&head.branch_id).await? {
                Some(_) => branch_ref,
                None => {
                    return Err(LixError::branch_not_found(
                        head.branch_id.clone(),
                        "build SQL read session at head",
                        "prepared branch selector",
                    ));
                }
            }
        }
        None => ctx.branch_ref(),
    };
    let active_branch_commit_id = match active_head {
        Some(head) => Some(head.commit_id.to_string()),
        None => branch_ref
            .load_head(ctx.active_branch_id())
            .await?
            .map(|head| head.commit_id.to_string()),
    };
    bind_execution_sql2_functions(
        &session,
        ctx.functions(),
        ctx.active_account_id(),
        Some(ctx.active_branch_id()),
        active_branch_commit_id.as_deref(),
    );
    let commit_graph = ctx.commit_graph();
    let commit_graph = Arc::new(tokio::sync::Mutex::new(commit_graph));
    let changelog_query_source = ctx.changelog_query_source();
    providers::register_diff_function(&session, changelog_query_source.clone());
    let provider_selection = providers::read_provider_selection(&session, statements);
    providers::register_read(
        &session,
        ctx,
        branch_ref,
        active_branch_commit_id,
        commit_graph,
        changelog_query_source,
        &provider_selection,
    )
    .await?;

    Ok(session)
}

pub(crate) async fn build_transaction_read_session<C, R>(
    read_ctx: &C,
    write_ctx: &mut dyn SqlWriteExecutionContext<ReadStore = R>,
    statement: &DataFusionStatement,
) -> Result<SessionContext, LixError>
where
    C: SqlExecutionContext + ?Sized,
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    let session = read_ctx.datafusion_read_session();
    let read_branch_ref: Arc<dyn BranchRefReader> = read_ctx.branch_ref();
    let active_branch_commit_id = read_branch_ref
        .load_head(read_ctx.active_branch_id())
        .await?
        .map(|head| head.commit_id.to_string());
    bind_execution_sql2_functions(
        &session,
        read_ctx.functions(),
        read_ctx.active_account_id(),
        Some(read_ctx.active_branch_id()),
        active_branch_commit_id.as_deref(),
    );
    let commit_graph = read_ctx.commit_graph();
    let commit_graph = Arc::new(tokio::sync::Mutex::new(commit_graph));
    let changelog_query_source = read_ctx.changelog_query_source();
    providers::register_diff_function(&session, changelog_query_source.clone());
    let write_ctx = SqlWriteContext::<R>::new(write_ctx);
    let write_branch_ref: Arc<dyn BranchRefReader> = Arc::new(write_ctx.clone());
    let provider_selection =
        providers::read_provider_selection(&session, std::slice::from_ref(statement));
    providers::register_transaction(
        &session,
        read_ctx,
        read_branch_ref,
        active_branch_commit_id,
        commit_graph,
        changelog_query_source,
        write_ctx,
        write_branch_ref,
        SqlWriteSessionOptions::default(),
        &provider_selection,
    )
    .await?;
    Ok(session)
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SqlWriteSessionOptions {
    pub(crate) omitted_insert_columns: BTreeSet<String>,
    pub(crate) explicit_insert_columns: Option<BTreeSet<String>>,
}

pub(crate) struct SqlWriteSession<R>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    datafusion: SessionContext,
    write_targets: Arc<providers::WriteTargetRegistry<R>>,
}

impl<R> SqlWriteSession<R>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    pub(crate) fn write_target(
        &self,
        table_name: &str,
    ) -> Result<Arc<providers::SpecWriteTarget<R>>, LixError> {
        self.write_targets.target(table_name)
    }
}

impl<R> Deref for SqlWriteSession<R>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    type Target = SessionContext;

    fn deref(&self) -> &Self::Target {
        &self.datafusion
    }
}

pub(crate) async fn build_write_session_with_options<R>(
    ctx: &mut dyn SqlWriteExecutionContext<ReadStore = R>,
    options: SqlWriteSessionOptions,
    provider_selection: &providers::ProviderSelection,
) -> Result<SqlWriteSession<R>, LixError>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    let session = ctx.datafusion_session();
    let write_ctx = SqlWriteContext::<R>::new(ctx)
        .with_explicit_insert_columns(options.explicit_insert_columns.clone());
    let write_targets = write_ctx.write_targets()?;
    let active_branch_id = write_ctx.active_branch_id();
    let branch_ref: Arc<dyn BranchRefReader> = Arc::new(write_ctx.clone());
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
    bind_execution_sql2_functions(
        &session,
        write_ctx.functions(),
        &write_ctx.active_account_id(),
        Some(active_branch_id.as_str()),
        Some(active_branch_commit_id.commit_id.to_string().as_str()),
    );
    providers::register_write(&session, write_ctx, branch_ref, options, provider_selection).await?;

    Ok(SqlWriteSession {
        datafusion: session,
        write_targets,
    })
}

pub(crate) fn new_sql_session_context() -> SessionContext {
    let config = SessionConfig::new()
        .set_str(
            "datafusion.sql_parser.dialect",
            super::dialect::DATAFUSION_SQL_DIALECT,
        )
        .with_information_schema(false)
        .with_target_partitions(1)
        .set_bool("datafusion.optimizer.repartition_aggregations", false)
        .set_bool("datafusion.optimizer.repartition_joins", false)
        .set_bool("datafusion.optimizer.repartition_sorts", false)
        .set_bool("datafusion.optimizer.repartition_windows", false)
        .set_bool("datafusion.optimizer.repartition_file_scans", false)
        .set_bool("datafusion.optimizer.enable_round_robin_repartition", false);
    let base_state = SessionStateBuilder::new_with_default_features()
        .with_config(config)
        .build();
    let mut physical_optimizers = base_state.physical_optimizers().to_vec();
    let aggregate_statistics_index = physical_optimizers
        .iter()
        .position(|rule| rule.name() == "aggregate_statistics")
        .expect("DataFusion default features include aggregate_statistics");
    physical_optimizers.insert(
        aggregate_statistics_index + 1,
        Arc::new(super::aggregate_statistics::ExactAggregateStatistics),
    );
    let state = SessionStateBuilder::new_from_existing(base_state)
        .with_physical_optimizer_rules(physical_optimizers)
        .build();
    let session = SessionContext::new_with_state(state);
    register_static_sql2_functions(&session);
    sql_session_from_template(session.state(), None)
}

pub(crate) fn sql_session_from_template(
    template: SessionState,
    catalog_list: Option<Arc<dyn CatalogProviderList>>,
) -> SessionContext {
    let slots = Arc::new(ExecutionSlots::default());
    let config = template.config().clone().with_extension(Arc::clone(&slots));
    let mut builder = SessionStateBuilder::new_from_existing(template).with_config(config);
    if let Some(catalog_list) = catalog_list {
        builder = builder.with_catalog_list(catalog_list);
    }
    let session = SessionContext::new_with_state(builder.build());
    register_execution_sql2_functions(&session, slots);
    session
}
