use datafusion::catalog::CatalogProviderList;
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::parser::Statement as DataFusionStatement;
use std::collections::BTreeSet;
use std::ops::Deref;
use std::sync::Arc;

use crate::LixError;
use crate::branch::{BranchHead, BranchRefReader};
use crate::checkpoint::latest_checkpoint_commit_id_at_head;

use super::branch_ref::CachingBranchRefReader;
use super::planning_cache::PooledReadSession;
use super::providers;
use super::udfs::{
    ExecutionSlots, bind_execution_sql2_functions, register_execution_sql2_functions,
    register_static_sql2_functions,
};
use super::{SqlExecutionContext, SqlWriteContext, SqlWriteExecutionContext};

pub(crate) async fn build_read_session<C>(
    ctx: &C,
    statements: &[DataFusionStatement],
) -> Result<PooledReadSession, LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    build_read_session_with_active_head(ctx, None, statements).await
}

pub(crate) async fn build_read_session_at_head<C>(
    ctx: &C,
    active_head: BranchHead,
    statements: &[DataFusionStatement],
) -> Result<PooledReadSession, LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    build_read_session_with_active_head(ctx, Some(active_head), statements).await
}

async fn build_read_session_with_active_head<C>(
    ctx: &C,
    active_head: Option<BranchHead>,
    statements: &[DataFusionStatement],
) -> Result<PooledReadSession, LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    let pooled = ctx.datafusion_read_session();
    let session = pooled.context();
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
    let root_commit_id = if statements
        .iter()
        .any(|statement| statement_uses_execution_function(statement, "lix_root_commit_id"))
    {
        resolve_root_commit_id(ctx, active_branch_commit_id.as_deref()).await?
    } else {
        None
    };
    let latest_checkpoint_commit_id = if statements.iter().any(|statement| {
        statement_uses_execution_function(statement, "lix_latest_checkpoint_commit_id")
    }) {
        resolve_latest_checkpoint_commit_id(
            ctx,
            active_branch_commit_id.as_deref(),
            root_commit_id.as_deref(),
        )
        .await?
    } else {
        None
    };
    bind_execution_sql2_functions(
        session,
        ctx.functions(),
        ctx.active_account_id(),
        Some(ctx.active_branch_id()),
        active_branch_commit_id.as_deref(),
        latest_checkpoint_commit_id.as_deref(),
        root_commit_id.as_deref(),
    );
    let provider_selection = providers::read_provider_selection(pooled.state(), statements);
    providers::register_read(
        session,
        ctx,
        branch_ref,
        active_branch_commit_id,
        &provider_selection,
    )
    .await?;

    Ok(pooled)
}

pub(crate) async fn build_transaction_read_session<C>(
    read_ctx: &C,
    write_ctx: &mut dyn SqlWriteExecutionContext,
    statement: &DataFusionStatement,
) -> Result<PooledReadSession, LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    let pooled = read_ctx.datafusion_read_session();
    let session = pooled.context();
    let read_branch_ref: Arc<dyn BranchRefReader> =
        Arc::new(CachingBranchRefReader::new(read_ctx.branch_ref()));
    let active_branch_commit_id = read_branch_ref
        .load_head(read_ctx.active_branch_id())
        .await?
        .map(|head| head.commit_id.to_string());
    let root_commit_id = if statement_uses_execution_function(statement, "lix_root_commit_id") {
        resolve_root_commit_id(read_ctx, active_branch_commit_id.as_deref()).await?
    } else {
        None
    };
    let latest_checkpoint_commit_id =
        if statement_uses_execution_function(statement, "lix_latest_checkpoint_commit_id") {
            resolve_latest_checkpoint_commit_id(
                read_ctx,
                active_branch_commit_id.as_deref(),
                root_commit_id.as_deref(),
            )
            .await?
        } else {
            None
        };
    bind_execution_sql2_functions(
        session,
        read_ctx.functions(),
        read_ctx.active_account_id(),
        Some(read_ctx.active_branch_id()),
        active_branch_commit_id.as_deref(),
        latest_checkpoint_commit_id.as_deref(),
        root_commit_id.as_deref(),
    );
    let write_ctx = SqlWriteContext::new(write_ctx);
    let write_branch_ref: Arc<dyn BranchRefReader> = Arc::new(CachingBranchRefReader::new(
        Arc::new(super::WriteContextBranchRefReader::new(write_ctx.clone())),
    ));
    let provider_selection =
        providers::read_provider_selection(pooled.state(), std::slice::from_ref(statement));
    providers::register_transaction(
        session,
        read_ctx,
        read_branch_ref,
        active_branch_commit_id,
        write_ctx,
        write_branch_ref,
        SqlWriteSessionOptions::default(),
        &provider_selection,
    )
    .await?;
    Ok(pooled)
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SqlWriteSessionOptions {
    pub(crate) omitted_insert_columns: BTreeSet<String>,
    pub(crate) explicit_insert_columns: Option<BTreeSet<String>>,
}

pub(crate) struct SqlWriteSession {
    datafusion: SessionContext,
    write_targets: Arc<providers::WriteTargetRegistry>,
}

impl SqlWriteSession {
    pub(crate) fn write_target(
        &self,
        table_name: &str,
    ) -> Result<Arc<providers::SpecWriteTarget>, LixError> {
        self.write_targets.target(table_name)
    }
}

impl Deref for SqlWriteSession {
    type Target = SessionContext;

    fn deref(&self) -> &Self::Target {
        &self.datafusion
    }
}

pub(crate) async fn build_write_session_with_options(
    ctx: &mut dyn SqlWriteExecutionContext,
    options: SqlWriteSessionOptions,
    provider_selection: &providers::ProviderSelection,
) -> Result<SqlWriteSession, LixError> {
    let session = ctx.datafusion_session();
    let write_ctx = SqlWriteContext::new(ctx)
        .with_explicit_insert_columns(options.explicit_insert_columns.clone());
    let write_targets = write_ctx.write_targets()?;
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
    bind_execution_sql2_functions(
        &session,
        write_ctx.functions(),
        &write_ctx.active_account_id(),
        Some(&active_branch_id),
        Some(&active_branch_commit_id.commit_id.to_string()),
        None,
        None,
    );
    providers::register_write(&session, write_ctx, branch_ref, options, provider_selection).await?;

    Ok(SqlWriteSession {
        datafusion: session,
        write_targets,
    })
}

fn statement_uses_execution_function(statement: &DataFusionStatement, function_name: &str) -> bool {
    use datafusion::sql::sqlparser::ast::{Expr, TableFactor, Visit, Visitor};
    use std::ops::ControlFlow;

    struct ExecutionFunctionVisitor<'a> {
        function_name: &'a str,
    }

    impl Visitor for ExecutionFunctionVisitor<'_> {
        type Break = ();

        fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<Self::Break> {
            if let Expr::Function(function) = expression
                && crate::sql2::parse::object_name_is_public_function(
                    &function.name,
                    self.function_name,
                )
            {
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }

        fn pre_visit_table_factor(&mut self, table: &TableFactor) -> ControlFlow<Self::Break> {
            if self.function_name == "lix_root_commit_id"
                && let TableFactor::Table { name, args: Some(_), .. } = table
                && crate::sql2::parse::object_name_is_public_function(name, "lix_state_at")
            {
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    }

    match statement {
        DataFusionStatement::Statement(statement) => statement
            .visit(&mut ExecutionFunctionVisitor { function_name })
            .is_break(),
        DataFusionStatement::Explain(explain) => {
            statement_uses_execution_function(explain.statement.as_ref(), function_name)
        }
        _ => false,
    }
}

async fn resolve_latest_checkpoint_commit_id<C>(
    context: &C,
    active_branch_commit_id: Option<&str>,
    root_commit_id: Option<&str>,
) -> Result<Option<String>, LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    let Some(active_branch_commit_id) = active_branch_commit_id else {
        return Ok(None);
    };
    let head_commit_id = active_branch_commit_id
        .parse::<crate::changelog::CommitId>()
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "active branch commit ID is invalid while resolving the latest checkpoint: {error}"
                ),
            )
        })?;
    let hot_state = context.hot_state();
    if let Some(checkpoint_commit_id) = latest_checkpoint_commit_id_at_head(
        context.changelog_query_source().store,
        hot_state.as_ref(),
        context.active_branch_id(),
        head_commit_id,
    )
    .await?
    {
        return Ok(Some(checkpoint_commit_id.to_string()));
    }

    if let Some(root_commit_id) = root_commit_id {
        return Ok(Some(root_commit_id.to_string()));
    }
    resolve_root_commit_id(context, Some(active_branch_commit_id)).await
}

async fn resolve_root_commit_id<C>(
    context: &C,
    active_branch_commit_id: Option<&str>,
) -> Result<Option<String>, LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    let Some(active_branch_commit_id) = active_branch_commit_id else {
        return Ok(None);
    };
    let mut current = active_branch_commit_id
        .parse::<crate::changelog::CommitId>()
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("active branch commit ID is invalid while resolving the root: {error}"),
            )
        })?;
    let mut commit_graph = context.commit_graph();
    loop {
        let node = commit_graph.load_node(&current).await?.ok_or_else(|| {
            crate::commit_graph::missing_commit_graph_error(&current)
        })?;
        let Some(first_parent) = node.parent_commit_ids.first().copied() else {
            return Ok(Some(node.commit_id.to_string()));
        };
        current = if node.first_parent_jump_span > 0 {
            node.first_parent_jump_commit_id
        } else {
            first_parent
        };
    }
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

#[cfg(test)]
mod tests {
    use datafusion::common::config::Dialect;

    use super::new_sql_session_context;

    #[test]
    fn datafusion_session_uses_postgresql_dialect() {
        let session = new_sql_session_context();
        assert_eq!(
            session.copied_config().options().sql_parser.dialect,
            Dialect::PostgreSQL
        );
    }
}

/// Builds a Lix SQL session from a template state, gives it its own
/// execution-function slots, and registers the five execution UDFs against them.
///
/// Every Lix SQL session goes through here exactly once. Doing it at session
/// construction rather than per statement is what lets a pooled session keep a
/// stable function registry: nothing mutates `SessionState`'s scalar-function
/// map after this point.
///
/// The slots are installed into the config *before* the state is built, so this
/// costs one `SessionState` construction rather than one per configuration step.
/// Write sessions are not pooled — they are built per statement — so an extra
/// registry deep copy here would land on every write.
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
