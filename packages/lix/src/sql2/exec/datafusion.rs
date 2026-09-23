#![allow(
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::elidable_lifetime_names,
    clippy::match_same_arms,
    clippy::option_if_let_else,
    clippy::redundant_clone,
    clippy::unnecessary_wraps
)]

use crate::branch::BranchHead;
use crate::functions::FunctionContext;
use crate::sql2::bind::expr::{
    BoundBinaryOperator, BoundCastType, BoundColumnRef, BoundExpr, BoundLiteral, ReturningImage,
};
use crate::sql2::bind::write::{
    BoundInsertValues, BoundReturning, BoundReturningItem, FileWriteSurface, RowWriteSurface,
};
use crate::sql2::bind::write::{
    BoundWriteInput, BoundWriteOp, BoundWriteTarget, DirectoryWriteSurface,
};
use crate::sql2::plan::LogicalWritePlan;
use crate::sql2::plan::branch_scope::BranchScope;
use crate::sql2::plan::predicate::BoundPredicate;
use crate::{GLOBAL_BRANCH_ID, LixError, LixNotice, SqlQueryResult, Value};
use datafusion::arrow::array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, PrimitiveArray, StringArray,
    StringViewArray, TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::metadata::{FieldMetadata, ScalarAndMetadata};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, ParamValues, ScalarValue};
use datafusion::datasource::{MemTable, empty::EmptyTable, provider_as_source};
use datafusion::logical_expr::expr::{BinaryExpr, Case, Cast, InList, Like, ScalarFunction};
use datafusion::logical_expr::registry::FunctionRegistry;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Operator};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DataFusionStatement;
use datafusion::sql::sqlparser::ast::{
    Expr as SqlExpr, FunctionArg, FunctionArgExpr, Ident, ObjectName, ObjectNamePart, TableFactor,
    Statement as SqlStatement, Value as SqlValue, Visit, VisitMut, Visitor, VisitorMut,
};
#[cfg(any(feature = "storage-benches", test))]
use futures_util::TryStreamExt;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::marker::PhantomData;
use std::ops::ControlFlow;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
#[cfg(feature = "storage-benches")]
use std::time::Instant;

use crate::catalog::CatalogFingerprint;
use crate::sql2::logical_value_compatibility::{
    validate_lix_expr_compatibility, validate_lix_value_compatibility,
};
use crate::sql2::logical_value_metadata::{expr_lix_value_kind, propagate_lix_value_metadata};
use crate::sql2::providers::ProviderSelection;
use crate::sql2::result_metadata::{
    LIX_VALUE_TYPE_JSONB, LIX_VALUE_TYPE_METADATA_KEY, LIX_VALUE_TYPE_ROW_REF, field_is_json,
    field_is_row_ref,
};
use crate::sql2::session::{
    SqlWriteSessionOptions, build_read_session, build_read_session_at_head,
    build_transaction_read_session, build_write_session_with_options,
};
use crate::sql2::write_normalization::lix_file_content_type_lix_error;
use crate::sql2::{
    CachedReadPlan, PhysicalReadPlanCacheKey, SqlExecutionContext, SqlPlanningCache,
    SqlWriteExecutionContext,
};

use super::{SqlDataFusionLogicalPlan, SqlLogicalPlan, SqlWriteResult};
use crate::sql2::PooledReadSession;
use datafusion::execution::SessionState;

pub(crate) const LIX_INSERT_COLUMN_OMITTED_METADATA_KEY: &str = "lix_insert_column_omitted";

pub(crate) struct DataFusionLogicalPlan {
    pub(super) state: Arc<SessionState>,
    pub(super) plan: crate::sql2::runtime::RuntimeReadPlan,
    pub(super) notices: Vec<LixNotice>,
    pub(super) expected_parameter_count: usize,
    pub(super) physical_planning_cache: Option<(
        Arc<SqlPlanningCache<CatalogFingerprint>>,
        PhysicalReadPlanCacheKey<CatalogFingerprint>,
    )>,
}

pub(crate) struct SessionReadSqlResult {
    pub(crate) runtime_functions: Option<FunctionContext>,
    pub(crate) query: SessionReadResult,
}

/// A live DataFusion result returned before any RecordBatch is collected or
/// converted to public scalar rows.
#[cfg(any(feature = "storage-benches", test))]
pub(crate) struct SessionReadBatchStreamResult<'session> {
    pub(crate) fields: Vec<Field>,
    pub(crate) stream: SendableRecordBatchStream,
    pub(crate) notices: Vec<LixNotice>,
    _session: PhantomData<&'session ()>,
}

/// Benchmark control that always stops at collected Arrow batches and never
/// applies the public result path's row-retention heuristic.
#[cfg(feature = "storage-benches")]
pub(crate) struct SessionReadCollectedBatchResult {
    pub(crate) fields: Vec<Field>,
    pub(crate) batches: Arc<[RecordBatch]>,
    pub(crate) notices: Vec<LixNotice>,
}

#[cfg(any(feature = "storage-benches", test))]
enum BatchRowSource<'a> {
    Collected {
        batches: &'a [RecordBatch],
        next_batch: usize,
    },
    Live(&'a mut SendableRecordBatchStream),
}

/// Internal row cursor over Arrow batches. It borrows its batch source, so a
/// live cursor cannot escape the read session and storage snapshot that own
/// the DataFusion stream.
#[cfg(any(feature = "storage-benches", test))]
pub(crate) struct BatchRowCursor<'a> {
    fields: &'a [Field],
    source: BatchRowSource<'a>,
    current_batch: Option<RecordBatch>,
    next_row: usize,
}

#[cfg(any(feature = "storage-benches", test))]
impl<'a> BatchRowCursor<'a> {
    pub(crate) fn collected(fields: &'a [Field], batches: &'a [RecordBatch]) -> Self {
        Self {
            fields,
            source: BatchRowSource::Collected {
                batches,
                next_batch: 0,
            },
            current_batch: None,
            next_row: 0,
        }
    }

    pub(crate) fn live(result: &'a mut SessionReadBatchStreamResult<'_>) -> Self {
        Self {
            fields: &result.fields,
            source: BatchRowSource::Live(&mut result.stream),
            current_batch: None,
            next_row: 0,
        }
    }

    pub(crate) async fn next_values(&mut self) -> Result<Option<Vec<Value>>, LixError> {
        loop {
            if let Some(batch) = &self.current_batch
                && self.next_row < batch.num_rows()
            {
                #[cfg(feature = "storage-benches")]
                let started = crate::sql_profile::is_active().then(Instant::now);
                let values = row_values_from_batch(self.fields, batch, self.next_row)?;
                #[cfg(feature = "storage-benches")]
                if let Some(started) = started {
                    crate::sql_profile::record_phase(
                        crate::sql_profile::Phase::PublicResultMaterialization,
                        started.elapsed(),
                    );
                }
                self.next_row += 1;
                return Ok(Some(values));
            }

            self.current_batch = self.next_batch().await?;
            self.next_row = 0;
            if self.current_batch.is_none() {
                return Ok(None);
            }
        }
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, LixError> {
        match &mut self.source {
            BatchRowSource::Collected {
                batches,
                next_batch,
            } => {
                let batch = batches.get(*next_batch).cloned();
                *next_batch += usize::from(batch.is_some());
                Ok(batch)
            }
            BatchRowSource::Live(stream) => {
                #[cfg(feature = "storage-benches")]
                let started = crate::sql_profile::is_active().then(Instant::now);
                let batch = stream
                    .try_next()
                    .await
                    .map_err(datafusion_error_to_lix_error);
                #[cfg(feature = "storage-benches")]
                if let Some(started) = started {
                    crate::sql_profile::record_phase(
                        crate::sql_profile::Phase::ArrowExecution,
                        started.elapsed(),
                    );
                }
                batch
            }
        }
    }
}

/// One read-result authority. DataFusion results remain owned by their
/// RecordBatches until a caller actually requests row views; small/native
/// routes continue to carry their already-materialized rows.
pub(crate) enum SessionReadResult {
    Rows(SqlQueryResult),
    Columnar {
        fields: Vec<Field>,
        batches: Arc<[RecordBatch]>,
        notices: Vec<LixNotice>,
    },
}

impl SessionReadResult {
    pub(crate) fn into_sql_query_result(self) -> Result<SqlQueryResult, LixError> {
        match self {
            Self::Rows(result) => Ok(result),
            Self::Columnar {
                fields,
                batches,
                notices,
            } => {
                let mut result = query_result_from_batches(&fields, &batches)?;
                result.notices = notices;
                Ok(result)
            }
        }
    }
}

/// DataFusion catalog and providers scoped to one immutable storage read.
pub(crate) struct ReadSqlSession<'ctx> {
    session: Option<PooledReadSession>,
    planning_environment: Option<(
        Arc<SqlPlanningCache<CatalogFingerprint>>,
        CatalogFingerprint,
    )>,
    _context: PhantomData<&'ctx ()>,
}

impl ReadSqlSession<'_> {
    fn pooled(&self) -> &PooledReadSession {
        self.session
            .as_ref()
            .expect("read session is only taken back when the statement ends")
    }

    fn context(&self) -> &SessionContext {
        self.pooled().context()
    }

    fn state(&self) -> &Arc<SessionState> {
        self.pooled().state()
    }
}

impl Drop for ReadSqlSession<'_> {
    fn drop(&mut self) {
        if let (Some((cache, _)), Some(session)) = (&self.planning_environment, self.session.take())
        {
            cache.recycle_datafusion_read_session(session);
        }
    }
}

#[cfg(test)]
async fn execute_sql<C>(ctx: &C, sql: &str, params: &[Value]) -> Result<SqlQueryResult, LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    let statement = crate::sql2::parse::parse_statement(sql)?;
    execute_read_statement_from_parsed(ctx, sql, statement, params).await
}

#[cfg(test)]
async fn execute_read_statement_from_parsed<C>(
    ctx: &C,
    sql: &str,
    statement: DataFusionStatement,
    params: &[Value],
) -> Result<SqlQueryResult, LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    let session = prepare_read_session(ctx, std::slice::from_ref(&statement)).await?;
    execute_read_statement_in_session_from_parsed(&session, sql, statement, params).await
}

pub(crate) async fn prepare_read_session<'ctx, C>(
    ctx: &'ctx C,
    statements: &[DataFusionStatement],
) -> Result<ReadSqlSession<'ctx>, LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    let planning_environment = ctx.sql_planning_environment().await?;
    Ok(ReadSqlSession {
        session: Some(build_read_session(ctx, statements).await?),
        planning_environment,
        _context: PhantomData,
    })
}

pub(crate) async fn prepare_read_session_at_head<'ctx, C>(
    ctx: &'ctx C,
    active_head: BranchHead,
    statements: &[DataFusionStatement],
) -> Result<ReadSqlSession<'ctx>, LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    let planning_environment = ctx.sql_planning_environment().await?;
    Ok(ReadSqlSession {
        session: Some(build_read_session_at_head(ctx, active_head, statements).await?),
        planning_environment,
        _context: PhantomData,
    })
}

pub(crate) async fn execute_read_statement_in_session_from_parsed(
    session: &ReadSqlSession<'_>,
    sql: &str,
    statement: DataFusionStatement,
    params: &[Value],
) -> Result<SqlQueryResult, LixError> {
    #[cfg(feature = "storage-benches")]
    let started = crate::sql_profile::is_active().then(Instant::now);
    let plan = create_logical_plan_in_session_from_parsed(session, sql, statement, params).await?;
    #[cfg(feature = "storage-benches")]
    if let Some(started) = started {
        crate::sql_profile::record_phase(
            crate::sql_profile::Phase::LogicalPlanning,
            started.elapsed(),
        );
    }
    execute_logical_plan(plan, params)
        .await?
        .into_sql_query_result()
}

pub(crate) async fn execute_read_statement_in_session_with_result(
    session: &ReadSqlSession<'_>,
    sql: &str,
    statement: DataFusionStatement,
    params: &[Value],
) -> Result<SessionReadSqlResult, LixError> {
    #[cfg(feature = "storage-benches")]
    let started = crate::sql_profile::is_active().then(Instant::now);
    let plan = create_logical_plan_in_session_from_parsed(session, sql, statement, params).await?;
    #[cfg(feature = "storage-benches")]
    if let Some(started) = started {
        crate::sql_profile::record_phase(
            crate::sql_profile::Phase::LogicalPlanning,
            started.elapsed(),
        );
    }
    Ok(SessionReadSqlResult {
        runtime_functions: None,
        query: execute_logical_plan(plan, params).await?,
    })
}

#[cfg(feature = "storage-benches")]
pub(crate) async fn execute_read_statement_in_session_with_batch_stream<'session>(
    session: &'session ReadSqlSession<'_>,
    sql: &str,
    statement: DataFusionStatement,
    params: &[Value],
) -> Result<SessionReadBatchStreamResult<'session>, LixError> {
    #[cfg(feature = "storage-benches")]
    let started = crate::sql_profile::is_active().then(Instant::now);
    let plan = create_logical_plan_in_session_from_parsed(session, sql, statement, params).await?;
    #[cfg(feature = "storage-benches")]
    if let Some(started) = started {
        crate::sql_profile::record_phase(
            crate::sql_profile::Phase::LogicalPlanning,
            started.elapsed(),
        );
    }
    execute_logical_plan_stream(plan, params, session).await
}

#[cfg(feature = "storage-benches")]
pub(crate) async fn execute_read_statement_in_session_with_collected_batches(
    session: &ReadSqlSession<'_>,
    sql: &str,
    statement: DataFusionStatement,
    params: &[Value],
) -> Result<SessionReadCollectedBatchResult, LixError> {
    let started = crate::sql_profile::is_active().then(Instant::now);
    let plan = create_logical_plan_in_session_from_parsed(session, sql, statement, params).await?;
    if let Some(started) = started {
        crate::sql_profile::record_phase(
            crate::sql_profile::Phase::LogicalPlanning,
            started.elapsed(),
        );
    }
    execute_logical_plan_collected_batches(plan, params).await
}

async fn create_logical_plan_in_session_from_parsed(
    session: &ReadSqlSession<'_>,
    sql: &str,
    mut statement: DataFusionStatement,
    params: &[Value],
) -> Result<SqlLogicalPlan, LixError> {
    crate::sql2::bind_read_statement(sql, &statement)?;
    let parameter_names = statement_parameter_names(&statement)?;
    let expected_parameter_count = expected_positional_parameter_count(&parameter_names)?;
    validate_parameter_count_values(expected_parameter_count, &parameter_names, params.len())?;
    let cacheable_statement = !statement_has_table_function(&statement);
    if cacheable_statement
        && let Some((cache, catalog)) = &session.planning_environment
        && let Some(cached) = cache.read_plan(sql, params, catalog)
    {
        let physical_planning_cache = PhysicalReadPlanCacheKey::new(sql, params, catalog.clone())
            .map(|key| (Arc::clone(cache), key));
        // With a physical-cache key the runtime rebinds scan providers lazily:
        // a warm template execution never touches the logical plan, so eagerly
        // resolving providers into it here would be pure per-statement waste.
        let plan = if physical_planning_cache.is_some() {
            crate::sql2::runtime::RuntimeReadPlan::Detached(cached.plan.clone())
        } else {
            crate::sql2::runtime::RuntimeReadPlan::Bound(
                rebind_cached_read_plan(session.context(), cached.plan.clone()).await?,
            )
        };
        return Ok(SqlLogicalPlan::DataFusion(SqlDataFusionLogicalPlan {
            state: Arc::clone(session.state()),
            plan,
            notices: Vec::new(),
            expected_parameter_count: cached.expected_parameter_count,
            physical_planning_cache,
        }));
    }
    bind_table_function_parameters(&mut statement, params)?;
    let plan = create_logical_plan_from_statement(session.context(), statement, params).await?;
    validate_supported_logical_plan(&plan)?;

    let physical_plan_cacheable = cacheable_statement
        && !logical_plan_has_scalar_function(&plan)
        && !logical_plan_has_subquery_expression(&plan);
    if physical_plan_cacheable && let Some((cache, catalog)) = &session.planning_environment {
        cache.remember_read_plan(
            sql,
            params,
            catalog.clone(),
            CachedReadPlan {
                plan: detach_cached_read_plan(plan.clone())?,
                expected_parameter_count,
            },
        );
    }

    let physical_planning_cache = if physical_plan_cacheable {
        session
            .planning_environment
            .as_ref()
            .and_then(|(cache, catalog)| {
                PhysicalReadPlanCacheKey::new(sql, params, catalog.clone())
                    .map(|key| (Arc::clone(cache), key))
            })
    } else {
        None
    };

    Ok(SqlLogicalPlan::DataFusion(SqlDataFusionLogicalPlan {
        state: Arc::clone(session.state()),
        plan: crate::sql2::runtime::RuntimeReadPlan::Bound(plan),
        notices: Vec::new(),
        expected_parameter_count,
        physical_planning_cache,
    }))
}

fn detach_cached_read_plan(plan: LogicalPlan) -> Result<LogicalPlan, LixError> {
    plan.transform_up(|node| {
        let LogicalPlan::TableScan(mut scan) = node else {
            return Ok(Transformed::no(node));
        };
        scan.source =
            provider_as_source(Arc::new(EmptyTable::new(scan.source.schema())));
        Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
    })
    .map(|transformed| transformed.data)
    .map_err(datafusion_error_to_lix_error)
}

async fn rebind_cached_read_plan(
    session: &SessionContext,
    plan: LogicalPlan,
) -> Result<LogicalPlan, LixError> {
    let mut tables = BTreeSet::new();
    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            tables.insert(scan.table_name.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .map_err(datafusion_error_to_lix_error)?;
    let mut providers = BTreeMap::new();
    for table in tables {
        let provider = session
            .table_provider(table.clone())
            .await
            .map_err(datafusion_error_to_lix_error)?;
        providers.insert(table, provider_as_source(provider));
    }
    plan.transform_up(|node| {
        let LogicalPlan::TableScan(mut scan) = node else {
            return Ok(Transformed::no(node));
        };
        scan.source = providers.get(&scan.table_name).cloned().ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(format!(
                "cached SQL plan provider '{}' is unavailable",
                scan.table_name
            ))
        })?;
        Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
    })
    .map(|transformed| transformed.data)
    .map_err(datafusion_error_to_lix_error)
}

/// Reports whether any expression in `plan` carries a nested subquery plan.
///
/// `LogicalPlan`'s tree traversal walks plan inputs only; the plans hidden
/// inside `Expr::ScalarSubquery`, `Expr::InSubquery` and `Expr::Exists` are
/// invisible to it. `detach_cached_read_plan` therefore cannot swap their
/// `TableScan` sources for placeholders, so caching such a plan would park
/// live snapshot-bound providers in an engine-lifetime LRU: the read scope
/// then fails `finish()` with leaked handles, and a later cache hit would
/// execute against a storage read that has already been released.
///
/// Planning-cache participation is an optimization, so the safe answer is to
/// keep these statements out of the cache entirely rather than grow a second
/// subquery-aware rewrite path that has to stay in sync with `detach`/`rebind`.
fn logical_plan_has_subquery_expression(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| {
        for expression in node.expressions() {
            let _ = expression.apply(|expression| {
                if matches!(
                    expression,
                    Expr::ScalarSubquery(_) | Expr::InSubquery(_) | Expr::Exists(_)
                ) {
                    found = true;
                    Ok(TreeNodeRecursion::Stop)
                } else {
                    Ok(TreeNodeRecursion::Continue)
                }
            });
            if found {
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

fn logical_plan_has_scalar_function(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| {
        for expression in node.expressions() {
            let _ = expression.apply(|expression| {
                if matches!(expression, Expr::ScalarFunction(_)) {
                    found = true;
                    Ok(TreeNodeRecursion::Stop)
                } else {
                    Ok(TreeNodeRecursion::Continue)
                }
            });
            if found {
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

pub(crate) fn statement_has_table_function(statement: &DataFusionStatement) -> bool {
    struct TableFunctionVisitor(bool);

    impl Visitor for TableFunctionVisitor {
        type Break = ();

        fn pre_visit_table_factor(
            &mut self,
            table_factor: &TableFactor,
        ) -> ControlFlow<Self::Break> {
            if matches!(table_factor, TableFactor::Table { args: Some(_), .. }) {
                self.0 = true;
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }

    fn visit(statement: &DataFusionStatement, visitor: &mut TableFunctionVisitor) {
        match statement {
            DataFusionStatement::Statement(statement) => {
                let _ = statement.visit(visitor);
            }
            DataFusionStatement::Explain(explain) => visit(explain.statement.as_ref(), visitor),
            _ => {}
        }
    }

    let mut visitor = TableFunctionVisitor(false);
    visit(statement, &mut visitor);
    visitor.0
}

pub(crate) async fn execute_transaction_read_statement_from_parsed(
    read_ctx: &impl SqlExecutionContext,
    write_ctx: &mut dyn SqlWriteExecutionContext,
    sql: &str,
    statement: DataFusionStatement,
    params: &[Value],
) -> Result<(SqlQueryResult, DataFusionStatement), LixError> {
    write_ctx.ensure_statement_allowed_after_restore()?;
    // Same fence as session reads, with the transaction overlay available
    // during planning/execution but not returned to the caller.
    let planning_environment = read_ctx.sql_planning_environment().await?;
    let (plan, session, resolved_statement) = create_transaction_read_logical_plan_from_parsed(
        read_ctx, write_ctx, sql, statement, params,
    )
    .await?;
    let result = execute_logical_plan(plan, params)
        .await
        .and_then(SessionReadResult::into_sql_query_result);
    if let Some((cache, _)) = planning_environment {
        cache.recycle_datafusion_read_session(session);
    }
    result.map(|result| (result, resolved_statement))
}

async fn create_transaction_read_logical_plan_from_parsed(
    read_ctx: &impl SqlExecutionContext,
    write_ctx: &mut dyn SqlWriteExecutionContext,
    sql: &str,
    mut statement: DataFusionStatement,
    params: &[Value],
) -> Result<(SqlLogicalPlan, PooledReadSession, DataFusionStatement), LixError> {
    crate::sql2::bind_read_statement(sql, &statement)?;
    let parameter_names = statement_parameter_names(&statement)?;
    let expected_parameter_count = expected_positional_parameter_count(&parameter_names)?;
    validate_parameter_count_values(expected_parameter_count, &parameter_names, params.len())?;
    bind_table_function_parameters(&mut statement, params)?;
    let session = build_transaction_read_session(read_ctx, write_ctx, &statement).await?;
    Box::pin(resolve_temporal_subquery_arguments(
        session.context(),
        &mut statement,
        params,
    ))
    .await?;
    let plan =
        create_logical_plan_from_statement(session.context(), statement.clone(), params).await?;
    validate_supported_logical_plan(&plan)?;

    Ok((
        SqlLogicalPlan::DataFusion(SqlDataFusionLogicalPlan {
            state: Arc::clone(session.state()),
            plan: crate::sql2::runtime::RuntimeReadPlan::Bound(plan),
            notices: Vec::new(),
            expected_parameter_count,
            physical_planning_cache: None,
        }),
        session,
        statement,
    ))
}

async fn create_logical_plan_from_statement(
    session: &SessionContext,
    mut statement: DataFusionStatement,
    params: &[Value],
) -> Result<LogicalPlan, LixError> {
    // Endpoint evaluation includes a complete read execution. Keep that future
    // off the enclosing read/write futures' stacks, including ordinary queries
    // that do not contain temporal arguments.
    Box::pin(resolve_temporal_subquery_arguments(
        session,
        &mut statement,
        params,
    ))
    .await?;
    let plan = session
        .state()
        .statement_to_plan(statement)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    let plan = propagate_lix_value_metadata(plan).map_err(datafusion_error_to_lix_error)?;
    validate_lix_value_compatibility(&plan)?;
    Ok(plan)
}

/// Table providers need concrete endpoints while planning their schemas. Resolve
/// scalar-subquery arguments against the statement's existing read session before
/// invoking DataFusion's synchronous table-function registry. Never cache these
/// resolved values: table-function statements already bypass the plan cache.
async fn resolve_temporal_subquery_arguments(
    session: &SessionContext,
    statement: &mut DataFusionStatement,
    params: &[Value],
) -> Result<(), LixError> {
    use datafusion::sql::sqlparser::ast::{Query, With};

    struct SubqueryFinder;
    impl Visitor for SubqueryFinder {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &SqlExpr) -> ControlFlow<()> {
            if matches!(expr, SqlExpr::Subquery(_)) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }

    // Each scope remembers CTE query addresses to distinguish a definition from
    // the query body: a nonrecursive CTE can see earlier siblings, not itself or
    // later siblings. Nested WITH scopes retain the normal SQL planner behavior.
    struct Scope {
        with: Option<With>,
        cte_queries: Vec<usize>,
        visible: Vec<With>,
    }
    struct ArgumentVisitor {
        scopes: Vec<Scope>,
        replacement: Option<SqlExpr>,
    }
    impl VisitorMut for ArgumentVisitor {
        type Break = Box<(SqlExpr, Vec<With>)>;
        fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
            let mut visible = self
                .scopes
                .last()
                .map(|s| s.visible.clone())
                .unwrap_or_default();
            if let Some(parent) = self.scopes.last()
                && let Some(with) = &parent.with
                && let Some(index) = parent
                    .cte_queries
                    .iter()
                    .position(|address| *address == std::ptr::from_ref(&*query).addr())
            {
                visible.pop();
                let mut preceding = with.clone();
                preceding.cte_tables.truncate(index);
                if !preceding.cte_tables.is_empty() {
                    visible.push(preceding);
                }
            }
            if let Some(with) = &query.with {
                visible.push(with.clone());
            }
            self.scopes.push(Scope {
                with: query.with.clone(),
                cte_queries: query
                    .with
                    .as_ref()
                    .map(|with| {
                        with.cte_tables
                            .iter()
                            .map(|cte| std::ptr::from_ref(cte.query.as_ref()).addr())
                            .collect()
                    })
                    .unwrap_or_default(),
                visible,
            });
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, _: &mut Query) -> ControlFlow<Self::Break> {
            self.scopes.pop();
            ControlFlow::Continue(())
        }
        fn post_visit_table_factor(
            &mut self,
            factor: &mut TableFactor,
        ) -> ControlFlow<Self::Break> {
            let TableFactor::Table {
                name,
                args: Some(args),
                ..
            } = factor
            else {
                return ControlFlow::Continue(());
            };
            let endpoint_count =
                if crate::sql2::parse::object_name_is_public_function(name, "lix_as_of") {
                    1
                } else if crate::sql2::parse::object_name_is_public_function(name, "lix_diff") {
                    2
                } else {
                    return ControlFlow::Continue(());
                };
            for argument in args.args.iter_mut().skip(1).take(endpoint_count) {
                let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = argument else {
                    continue;
                };
                if Visit::visit(&*expr, &mut SubqueryFinder).is_break() {
                    let found = expr.clone();
                    if let Some(replacement) = self.replacement.take() {
                        *expr = replacement;
                    }
                    return ControlFlow::Break(Box::new((
                        found,
                        self.scopes
                            .last()
                            .map(|s| s.visible.clone())
                            .unwrap_or_default(),
                    )));
                }
            }
            ControlFlow::Continue(())
        }
    }
    fn visit(
        statement: &mut DataFusionStatement,
        visitor: &mut ArgumentVisitor,
    ) -> ControlFlow<Box<(SqlExpr, Vec<With>)>> {
        match statement {
            DataFusionStatement::Statement(statement) => statement.visit(visitor),
            DataFusionStatement::Explain(explain) => visit(&mut explain.statement, visitor),
            _ => ControlFlow::Continue(()),
        }
    }

    loop {
        let ControlFlow::Break(argument) = visit(
            statement,
            &mut ArgumentVisitor {
                scopes: Vec::new(),
                replacement: None,
            },
        ) else {
            return Ok(());
        };
        let (expression, scopes) = *argument;
        let mut sql = format!("SELECT {expression}");
        for scope in scopes.into_iter().rev() {
            sql = format!("{scope} SELECT ({sql})");
        }
        let mut argument_statement = crate::sql2::parse::parse_statement(&sql)?;
        // Preserve Lix's read-only and expression restrictions even though the
        // scalar query is executed during endpoint resolution.
        crate::sql2::bind_read_statement(&sql, &argument_statement)?;
        bind_table_function_parameters(&mut argument_statement, params)?;
        let plan = Box::pin(create_logical_plan_from_statement(
            session,
            argument_statement,
            params,
        ))
        .await?;
        validate_supported_logical_plan(&plan)?;
        let plan = bind_plan_param_values(plan, params)?;
        let batches = crate::sql2::runtime::collect_plan(
            &session.state(),
            crate::sql2::runtime::RuntimeReadPlan::Bound(plan),
            None,
        )
        .await
        .map_err(datafusion_error_to_lix_error)?;
        if batches.iter().map(RecordBatch::num_rows).sum::<usize>() != 1
            || batches.iter().any(|batch| batch.num_columns() != 1)
        {
            return Err(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                "temporal commit argument must return exactly one column and one row",
            ));
        }
        let batch = batches
            .iter()
            .find(|batch| batch.num_rows() != 0)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_TYPE_MISMATCH,
                    "temporal commit argument must be non-null text",
                )
            })?;
        let value = ScalarValue::try_from_array(batch.column(0), 0)
            .map_err(datafusion_error_to_lix_error)?;
        let text = value.try_as_str().flatten().ok_or_else(|| {
            LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                "temporal commit argument must be non-null text",
            )
        })?;
        let _ = visit(
            statement,
            &mut ArgumentVisitor {
                scopes: Vec::new(),
                replacement: Some(SqlExpr::value(SqlValue::SingleQuotedString(
                    text.to_string(),
                ))),
            },
        );
    }
}

fn bind_runtime_plan_param_values(
    plan: crate::sql2::runtime::RuntimeReadPlan,
    params: &[Value],
) -> Result<crate::sql2::runtime::RuntimeReadPlan, LixError> {
    use crate::sql2::runtime::RuntimeReadPlan;
    Ok(match plan {
        RuntimeReadPlan::Bound(plan) => {
            RuntimeReadPlan::Bound(bind_plan_param_values(plan, params)?)
        }
        RuntimeReadPlan::Detached(plan) => {
            RuntimeReadPlan::Detached(bind_plan_param_values(plan, params)?)
        }
    })
}

fn bind_plan_param_values(plan: LogicalPlan, params: &[Value]) -> Result<LogicalPlan, LixError> {
    if params.is_empty() {
        return Ok(plan);
    }
    let plan = plan
        .with_param_values(ParamValues::List(
            params
                .iter()
                .map(scalar_value_from_lix_value)
                .collect::<Result<Vec<_>, _>>()?,
        ))
        .map_err(datafusion_error_to_lix_error)?;
    let plan = propagate_lix_value_metadata(plan).map_err(datafusion_error_to_lix_error)?;
    validate_lix_value_compatibility(&plan)?;
    Ok(plan)
}

async fn execute_logical_plan(
    plan: SqlLogicalPlan,
    params: &[Value],
) -> Result<SessionReadResult, LixError> {
    let SqlLogicalPlan::DataFusion(plan) = plan else {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "sql2 bound write execution is not wired yet",
        ));
    };
    let SqlDataFusionLogicalPlan {
        state,
        plan,
        notices,
        expected_parameter_count,
        physical_planning_cache,
    } = plan;
    debug_assert_eq!(expected_parameter_count, params.len());

    // `SessionContext::execute_logical_plan` only branches for DDL and utility
    // statements, both of which `validate_supported_logical_plan` already
    // rejects, and otherwise wraps the plan in a `DataFrame` whose only purpose
    // here is to carry a freshly deep-copied `SessionState`. Bind the
    // parameters on the plan directly against the statement's pooled state.
    let plan = bind_runtime_plan_param_values(plan, params)?;

    let logical_fields = plan
        .inner()
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let (schema, batches) =
        crate::sql2::runtime::collect_plan_with_schema(&state, plan, physical_planning_cache)
            .await
            .map_err(datafusion_error_to_lix_error)?;
    let result_fields = resolved_result_fields(&logical_fields, &schema);
    // This is a benchmark-only causal ceiling probe. It keeps DataFusion's
    // RecordBatch owners alive through execution, counts the rows/batches,
    // and deliberately omits public scalar/row conversion. No production
    // build can enter this branch because the symbol is feature-gated.
    #[cfg(feature = "storage-benches")]
    if std::env::var("LIX_TRACKED_STATE_CRUD_PROFILE_RESULT_MODE").as_deref() == Ok("count_only") {
        let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        crate::sql_profile::record_result_count_only(rows, batches.len());
        return Ok(SessionReadResult::Columnar {
            fields: result_fields,
            batches: Arc::from(batches),
            notices,
        });
    }
    if retain_columnar_result(&result_fields, &batches) {
        return Ok(SessionReadResult::Columnar {
            fields: result_fields,
            batches: Arc::from(batches),
            notices,
        });
    }
    #[cfg(feature = "storage-benches")]
    let started = crate::sql_profile::is_active().then(Instant::now);
    let mut result = query_result_from_batches(&result_fields, &batches)?;
    #[cfg(feature = "storage-benches")]
    if let Some(started) = started {
        crate::sql_profile::record_phase(
            crate::sql_profile::Phase::PublicResultMaterialization,
            started.elapsed(),
        );
    }
    result.notices = notices;
    Ok(SessionReadResult::Rows(result))
}

#[cfg(feature = "storage-benches")]
async fn execute_logical_plan_stream<'session>(
    plan: SqlLogicalPlan,
    params: &[Value],
    _read_session: &'session ReadSqlSession<'_>,
) -> Result<SessionReadBatchStreamResult<'session>, LixError> {
    let SqlLogicalPlan::DataFusion(plan) = plan else {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "sql2 bound write execution is not wired yet",
        ));
    };
    let SqlDataFusionLogicalPlan {
        state,
        plan,
        notices,
        expected_parameter_count,
        physical_planning_cache,
    } = plan;
    debug_assert_eq!(expected_parameter_count, params.len());

    // `SessionContext::execute_logical_plan` only branches for DDL and utility
    // statements, both of which `validate_supported_logical_plan` already
    // rejects, and otherwise wraps the plan in a `DataFrame` whose only purpose
    // here is to carry a freshly deep-copied `SessionState`. Bind the
    // parameters on the plan directly against the statement's pooled state.
    let plan = bind_runtime_plan_param_values(plan, params)?;
    let fields = plan
        .inner()
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let stream = crate::sql2::runtime::stream_plan(&state, plan, physical_planning_cache)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    let fields = resolved_result_fields(&fields, &stream.schema());
    Ok(SessionReadBatchStreamResult {
        fields,
        stream,
        notices,
        _session: PhantomData,
    })
}

#[cfg(feature = "storage-benches")]
async fn execute_logical_plan_collected_batches(
    plan: SqlLogicalPlan,
    params: &[Value],
) -> Result<SessionReadCollectedBatchResult, LixError> {
    let SqlLogicalPlan::DataFusion(plan) = plan else {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "sql2 bound write execution is not wired yet",
        ));
    };
    let SqlDataFusionLogicalPlan {
        state,
        plan,
        notices,
        expected_parameter_count,
        physical_planning_cache,
    } = plan;
    debug_assert_eq!(expected_parameter_count, params.len());

    // `SessionContext::execute_logical_plan` only branches for DDL and utility
    // statements, both of which `validate_supported_logical_plan` already
    // rejects, and otherwise wraps the plan in a `DataFrame` whose only purpose
    // here is to carry a freshly deep-copied `SessionState`. Bind the
    // parameters on the plan directly against the statement's pooled state.
    let plan = bind_runtime_plan_param_values(plan, params)?;
    let fields = plan
        .inner()
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let batches = crate::sql2::runtime::collect_plan(&state, plan, physical_planning_cache)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    Ok(SessionReadCollectedBatchResult {
        fields,
        batches: Arc::from(batches),
        notices,
    })
}

/// Keep large, ordinary Arrow result sets columnar until a caller requests a
/// row view. The threshold is based only on output cells, not SQL shape or
/// table identity; unsupported/JSON fields retain the fallible eager route so
/// public error semantics remain unchanged.
fn retain_columnar_result(fields: &[Field], batches: &[RecordBatch]) -> bool {
    const COLUMNAR_CELL_THRESHOLD: usize = 4_096;
    if fields.is_empty()
        || fields.iter().any(|field| {
            !matches!(
                field.data_type(),
                DataType::Null
                    | DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Utf8
                    | DataType::Utf8View
                    | DataType::LargeUtf8
                    | DataType::Binary
                    | DataType::LargeBinary
            )
        })
    {
        return false;
    }
    if batches.iter().enumerate().any(|(_, batch)| {
        fields.iter().enumerate().any(|(column_index, field)| {
            let array = batch.column(column_index);
            match field.data_type() {
                DataType::UInt64 => {
                    array
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .is_some_and(|values| {
                            values.iter().flatten().any(|value| value > i64::MAX as u64)
                        })
                }
                DataType::Float32 => {
                    array
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .is_some_and(|values| {
                            (0..values.len()).any(|index| {
                                values.is_valid(index) && !values.value(index).is_finite()
                            })
                        })
                }
                DataType::Float64 => {
                    array
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .is_some_and(|values| {
                            (0..values.len()).any(|index| {
                                values.is_valid(index) && !values.value(index).is_finite()
                            })
                        })
                }
                _ => false,
            }
        })
    }) {
        return false;
    }
    batches
        .iter()
        .map(|batch| batch.num_rows().saturating_mul(batch.num_columns()))
        .sum::<usize>()
        >= COLUMNAR_CELL_THRESHOLD
}

struct BoundReturningImageCapture {
    old_positions: Vec<Option<usize>>,
    new_positions: Vec<Option<usize>>,
    width: usize,
    images: DeferredReturningImageColumns,
}

/// Registered row tables deliberately reject raw DataFusion INSERT writes.
/// Keep their bound mutation path as a narrow image-capture adapter, then run
/// the complete RETURNING list through DataFusion.
async fn try_execute_deferred_bound_row_returning(
    ctx: &mut dyn SqlWriteExecutionContext,
    session: &SessionContext,
    plan: &LogicalWritePlan,
    params: &[Value],
    table_schema: &Schema,
    target_name: &str,
    returning: &BoundReturning,
) -> Result<Option<SqlWriteResult>, LixError> {
    if !matches!(
        plan.bound.target,
        BoundWriteTarget::Row(RowWriteSurface::Base { .. })
    ) {
        return Ok(None);
    }
    let Some((capture_plan, capture)) =
        bound_returning_image_capture_plan(session, plan, table_schema, target_name, returning)
            .await?
    else {
        return Ok(None);
    };
    if !super::bound_public_write::supports_bound_public_write(&capture_plan) {
        return Ok(None);
    }
    let execution = super::bound_public_write::try_execute_bound_public_write(
        ctx,
        &capture_plan,
        params,
        &crate::common::ExecuteStatementMetadata::default(),
    )
    .await?;
    let super::bound_public_write::BoundPublicWriteExecution::Executed(mut result) = execution
    else {
        return Ok(None);
    };
    let captured = result.returning.take().ok_or_else(|| {
        LixError::unknown("bound RETURNING capture omitted its requested image columns")
    })?;
    if captured.rows.iter().any(|row| row.len() != capture.width) {
        return Err(LixError::unknown(
            "bound RETURNING capture returned an unexpected image width",
        ));
    }

    let old_batch = bridge_returning_image_batch(&captured, table_schema, &capture.old_positions)?;
    let new_batch = if plan.bound.op == BoundWriteOp::Delete {
        None
    } else {
        Some(bridge_returning_image_batch(
            &captured,
            table_schema,
            &capture.new_positions,
        )?)
    };
    let (fields, batches) = datafusion_returning_projection(
        session,
        table_schema,
        target_name,
        plan.bound.op == BoundWriteOp::Delete,
        returning,
        &capture.images,
        Some(&old_batch),
        new_batch.as_ref(),
        params,
    )
    .await?;
    let mut query = query_result_from_batches(&fields, &batches)?;
    query.notices = captured.notices;
    let mut projected = SqlWriteResult::returning(result.rows_affected, query);
    projected.checkpoint_telemetry = result.checkpoint_telemetry;
    Ok(Some(projected))
}

async fn bound_returning_image_capture_plan(
    session: &SessionContext,
    plan: &LogicalWritePlan,
    table_schema: &Schema,
    target_name: &str,
    returning: &BoundReturning,
) -> Result<Option<(LogicalWritePlan, BoundReturningImageCapture)>, LixError> {
    if !matches!(
        plan.bound.target,
        BoundWriteTarget::Row(RowWriteSurface::Base { .. })
    ) {
        return Ok(None);
    }
    let delete = plan.bound.op == BoundWriteOp::Delete;
    let images =
        deferred_returning_image_columns(session, returning, table_schema, target_name, delete)
            .await?;
    let mut items = Vec::new();
    let mut old_positions = vec![None; table_schema.fields().len()];
    let mut new_positions = vec![None; table_schema.fields().len()];

    for (field_index, field) in table_schema.fields().iter().enumerate() {
        if images.old.contains(field.name()) {
            push_bound_returning_image_column(
                &mut items,
                &mut old_positions,
                field_index,
                field.name(),
                target_name,
                ReturningImage::Old,
            );
        }
        if images.new.contains(field.name()) {
            push_bound_returning_image_column(
                &mut items,
                &mut new_positions,
                field_index,
                field.name(),
                target_name,
                ReturningImage::New,
            );
        }
    }

    // Preserve affected rows for a constant-only RETURNING projection.
    if items.is_empty() {
        let Some(field) = table_schema.fields().first() else {
            return Ok(None);
        };
        let (image, positions) = if delete {
            (ReturningImage::Old, &mut old_positions)
        } else {
            (ReturningImage::New, &mut new_positions)
        };
        push_bound_returning_image_column(
            &mut items,
            positions,
            0,
            field.name(),
            target_name,
            image,
        );
    }

    let width = items.len();
    let mut capture_plan = plan.clone();
    capture_plan.bound.returning = Some(BoundReturning { items });
    Ok(Some((
        capture_plan,
        BoundReturningImageCapture {
            old_positions,
            new_positions,
            width,
            images,
        },
    )))
}

fn push_bound_returning_image_column(
    items: &mut Vec<BoundReturningItem>,
    positions: &mut [Option<usize>],
    field_index: usize,
    field_name: &str,
    target_name: &str,
    image: ReturningImage,
) {
    let position = items.len();
    positions[field_index] = Some(position);
    items.push(BoundReturningItem {
        expr: Some(BoundExpr::Column(BoundColumnRef {
            image: Some(image),
            table: target_name.to_string(),
            column_id: field_index,
            name: field_name.to_string(),
        })),
        sql_expr: None,
        output_name: format!("__lix_returning_image_{}_{}", image.qualifier(), field_name),
        output_alias: None,
    });
}

fn bridge_returning_image_batch(
    result: &SqlQueryResult,
    table_schema: &Schema,
    positions: &[Option<usize>],
) -> Result<RecordBatch, LixError> {
    let row_count = result.rows.len();
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    for (field_index, field) in table_schema.fields().iter().enumerate() {
        let Some(position) = positions[field_index] else {
            continue;
        };
        fields.push(field.as_ref().clone().with_nullable(true));
        if row_count == 0 {
            columns.push(datafusion::arrow::array::new_empty_array(field.data_type()));
            continue;
        }
        let values = result
            .rows
            .iter()
            .map(|row| {
                let value = row.get(position).ok_or_else(|| {
                    LixError::unknown("bound RETURNING image is missing a captured field")
                })?;
                if matches!(value, Value::Null) {
                    ScalarValue::try_from(field.data_type()).map_err(datafusion_error_to_lix_error)
                } else {
                    scalar_value_from_lix_value(value)?
                        .value
                        .cast_to(field.data_type())
                        .map_err(datafusion_error_to_lix_error)
                }
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        columns.push(
            ScalarValue::iter_to_array(values.into_iter())
                .map_err(datafusion_error_to_lix_error)?,
        );
    }
    RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        columns,
        &datafusion::arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(row_count)),
    )
    .map_err(|error| LixError::unknown(format!("failed to build RETURNING image: {error}")))
}

pub(crate) async fn execute_datafusion_write_logical_plan(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    params: &[Value],
) -> Result<SqlWriteResult, LixError> {
    validate_bound_write_input(plan, params)?;
    let table_name = write_target_table_name(plan)?;
    let session = build_write_session_with_options(ctx, write_session_options(plan), plan).await?;
    let table = session
        .table_provider(&table_name)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    let write_target = session.write_target(&table_name)?;
    let table_schema = table.schema();
    let state = session.state();
    let original_returning = plan.bound.returning.as_ref();

    let returning = datafusion_dml_returning(
        &session,
        table_schema.as_ref(),
        &table_name,
        original_returning,
        params,
        matches!(plan.bound.op, BoundWriteOp::Delete),
    )
    .await?;

    if returning
        .as_ref()
        .is_some_and(crate::sql2::providers::DmlReturning::is_deferred_projection)
    {
        if let Some(bound_returning) = original_returning {
            if let Some(result) = try_execute_deferred_bound_row_returning(
                ctx,
                &session,
                plan,
                params,
                table_schema.as_ref(),
                &table_name,
                bound_returning,
            )
            .await?
            {
                return Ok(result);
            }
        }
    }

    let exec = match plan.bound.op {
        BoundWriteOp::Insert => {
            let input =
                insert_input_plan(&session, Arc::clone(&table_schema), plan, params)
                    .await?;
            if plan.bound.branch_scope == BranchScope::Empty {
                return sql_write_empty_returning_result(
                    &session,
                    table_schema.as_ref(),
                    &table_name,
                    matches!(plan.bound.op, BoundWriteOp::Delete),
                    returning.as_ref(),
                    original_returning,
                    params,
                )
                .await;
            }
            if let Some(conflict) = &plan.bound.conflict {
                let target_columns: Vec<String> = conflict
                    .target_columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect();
                write_target
                    .validate_upsert_target(&input, &target_columns)
                    .await
                    .map_err(datafusion_error_to_lix_error)?;
                let proposed_batches = crate::sql2::runtime::stream_input_plan(
                    Arc::clone(&input),
                    session.task_ctx(),
                )
                .map_err(datafusion_error_to_lix_error)?;
                let action = match &conflict.action {
                    crate::sql2::bind::write::BoundConflictAction::DoNothing => {
                        crate::sql2::providers::UpsertAction::DoNothing
                    }
                    crate::sql2::bind::write::BoundConflictAction::DoUpdate { assignments } => {
                        crate::sql2::providers::UpsertAction::DoUpdate {
                            assignments: datafusion_conflict_assignments(
                                &session,
                                table_schema.as_ref(),
                                assignments,
                                params,
                            )?,
                        }
                    }
                };
                let rows_affected = match &returning {
                    Some(returning) => write_target
                        .execute_upsert_with_returning(
                            &input,
                            proposed_batches,
                            &target_columns,
                            &action,
                            returning.clone(),
                        )
                        .await
                        .map_err(datafusion_error_to_lix_error)?,
                    None => write_target
                        .execute_upsert(&input, proposed_batches, &target_columns, &action)
                        .await
                        .map_err(datafusion_error_to_lix_error)?,
                };
                return match returning.as_ref() {
                    Some(returning) => {
                        sql_write_captured_returning_result(
                            rows_affected,
                            returning,
                            &session,
                            table_schema.as_ref(),
                            &table_name,
                            false,
                            original_returning.expect("RETURNING plan exists"),
                            params,
                        )
                        .await
                    }
                    None => Ok(SqlWriteResult::affected(rows_affected)),
                };
            }
            match &returning {
                Some(returning) => write_target
                    .insert_with_returning(&state, input, returning.clone())
                    .await
                    .map_err(datafusion_error_to_lix_error),
                None => write_target
                    .insert(input)
                    .await
                    .map_err(datafusion_error_to_lix_error),
            }
        }
        BoundWriteOp::Update => {
            let assignments =
                datafusion_assignments(&session, table_schema.as_ref(), plan, params)?;
            let filters = datafusion_write_filters(&session, table_schema.as_ref(), plan, params)?;
            if plan.bound.branch_scope == BranchScope::Empty {
                return sql_write_empty_returning_result(
                    &session,
                    table_schema.as_ref(),
                    &table_name,
                    false,
                    returning.as_ref(),
                    original_returning,
                    params,
                )
                .await;
            }
            match &returning {
                Some(returning) => write_target
                    .update_with_returning(&state, assignments, filters, returning.clone())
                    .await
                    .map_err(datafusion_error_to_lix_error),
                None => write_target
                    .update(&state, assignments, filters)
                    .await
                    .map_err(datafusion_error_to_lix_error),
            }
        }
        BoundWriteOp::Delete => {
            let filters = datafusion_write_filters(&session, table_schema.as_ref(), plan, params)?;
            if plan.bound.branch_scope == BranchScope::Empty {
                return sql_write_empty_returning_result(
                    &session,
                    table_schema.as_ref(),
                    &table_name,
                    true,
                    returning.as_ref(),
                    original_returning,
                    params,
                )
                .await;
            }
            match &returning {
                Some(returning) => write_target
                    .delete_with_returning(&state, filters, returning.clone())
                    .await
                    .map_err(datafusion_error_to_lix_error),
                None => write_target
                    .delete(&state, filters)
                    .await
                    .map_err(datafusion_error_to_lix_error),
            }
        }
    }?;

    let batches = crate::sql2::runtime::collect_input_plan(exec, session.task_ctx())
        .await
        .map_err(datafusion_error_to_lix_error)?;
    let result =
        query_result_from_batches(&[Field::new("count", DataType::UInt64, false)], &batches)?;
    let rows_affected = affected_rows_from_query_result(result)?;
    match returning {
        Some(returning) => {
            sql_write_captured_returning_result(
                rows_affected,
                &returning,
                &session,
                table_schema.as_ref(),
                &table_name,
                matches!(plan.bound.op, BoundWriteOp::Delete),
                original_returning.expect("RETURNING plan exists"),
                params,
            )
            .await
        }
        None => Ok(SqlWriteResult::affected(rows_affected)),
    }
}

pub(super) async fn row_insert_query_stream(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    params: &[Value],
) -> Result<SendableRecordBatchStream, LixError> {
    let BoundWriteTarget::Row(RowWriteSurface::Base { schema_key }) = &plan.bound.target else {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "expected registered INSERT target",
        ));
    };
    let table_name = schema_key.clone();
    let session = build_write_session_with_options(ctx, write_session_options(plan), plan).await?;
    let table = session
        .table_provider(&table_name)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    let BoundWriteInput::Query { query, columns } = &plan.bound.input else {
        unreachable!()
    };
    let input =
        insert_query_input_plan(&session, table.schema(), query, columns, params, false).await?;
    crate::sql2::runtime::stream_input_plan(input, session.task_ctx())
        .map_err(datafusion_error_to_lix_error)
}

#[derive(Clone, Default)]
struct DeferredReturningImageColumns {
    old: BTreeSet<String>,
    new: BTreeSet<String>,
}

impl DeferredReturningImageColumns {
    fn insert(&mut self, image: ReturningImage, column: String) {
        match image {
            ReturningImage::Old => &mut self.old,
            ReturningImage::New => &mut self.new,
        }
        .insert(column);
    }

    fn for_image(&self, image: ReturningImage) -> &BTreeSet<String> {
        match image {
            ReturningImage::Old => &self.old,
            ReturningImage::New => &self.new,
        }
    }
}

async fn datafusion_returning_projection(
    session: &SessionContext,
    table_schema: &Schema,
    target_name: &str,
    delete: bool,
    returning: &BoundReturning,
    images: &DeferredReturningImageColumns,
    old_batch: Option<&RecordBatch>,
    new_batch: Option<&RecordBatch>,
    params: &[Value],
) -> Result<(Vec<Field>, Vec<RecordBatch>), LixError> {
    let count = old_batch.or(new_batch).map_or(0, RecordBatch::num_rows);
    if old_batch.is_some_and(|batch| batch.num_rows() != count) {
        return Err(LixError::unknown(
            "RETURNING old image has a different cardinality",
        ));
    }
    if new_batch.is_some_and(|batch| batch.num_rows() != count) {
        return Err(LixError::unknown(
            "RETURNING row images have different cardinalities",
        ));
    }
    let current_image = if delete {
        ReturningImage::Old
    } else {
        ReturningImage::New
    };
    let current_batch = if delete {
        old_batch.ok_or_else(|| LixError::unknown("RETURNING old image missing after DELETE"))?
    } else {
        new_batch.ok_or_else(|| {
            LixError::unknown("RETURNING current image missing after a non-DELETE write")
        })?
    };

    let mut source_fields = Vec::new();
    let mut source_arrays = Vec::new();
    let mut null_arrays = HashMap::new();
    let mut old_aliases = BTreeMap::new();
    let mut new_aliases = BTreeMap::new();
    let mut available_source_columns = BTreeSet::new();
    for (index, field) in table_schema.fields().iter().enumerate() {
        let name = field.name();
        if images.for_image(current_image).contains(name) {
            available_source_columns.insert(name.clone());
        }
        // Keep the complete OLD/NEW input schema available to DataFusion so
        // the original SQL expressions remain resolvable after dependency
        // optimization prunes a constant-dead branch. Values omitted from the
        // optimized row-image dependencies are typed NULL placeholders; the
        // optimized plan below verifies that it does not read them.
        push_returning_source_column(
            &mut source_fields,
            &mut source_arrays,
            &mut null_arrays,
            field,
            name,
            Some(current_batch),
            name,
            count,
        );
        let old_alias = returning_pseudo_column_name("old", index, table_schema);
        if images.old.contains(name) {
            available_source_columns.insert(old_alias.clone());
        }
        push_returning_source_column(
            &mut source_fields,
            &mut source_arrays,
            &mut null_arrays,
            field,
            &old_alias,
            old_batch,
            name,
            count,
        );
        old_aliases.insert(name.clone(), old_alias);

        let new_alias = returning_pseudo_column_name("new", index, table_schema);
        if images.new.contains(name) {
            available_source_columns.insert(new_alias.clone());
        }
        push_returning_source_column(
            &mut source_fields,
            &mut source_arrays,
            &mut null_arrays,
            field,
            &new_alias,
            new_batch,
            name,
            count,
        );
        new_aliases.insert(name.clone(), new_alias);
    }

    // A constant-only RETURNING list still needs one source column to preserve
    // the number of affected rows through the projection.
    if source_fields.is_empty() {
        const ROW_MARKER: &str = "__lix_returning_row_marker";
        source_fields.push(Field::new(ROW_MARKER, DataType::UInt64, false));
        source_arrays.push(Arc::new(UInt64Array::from(vec![0_u64; count])));
    }

    let input_batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::new(source_fields)),
        source_arrays,
        &datafusion::arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(count)),
    )
    .map_err(|error| LixError::unknown(error.to_string()))?;
    const INPUT_TABLE_NAME: &str = "__lix_returning_images";
    let input_provider = Arc::new(
        MemTable::try_new(input_batch.schema(), vec![vec![input_batch]])
            .map_err(datafusion_error_to_lix_error)?,
    );
    session
        .register_table(INPUT_TABLE_NAME, input_provider)
        .map_err(datafusion_error_to_lix_error)?;

    let old_aliases = old_aliases
        .iter()
        .map(|(column, alias)| (column.to_ascii_lowercase(), alias.clone()))
        .collect::<BTreeMap<_, _>>();
    let new_aliases = new_aliases
        .iter()
        .map(|(column, alias)| (column.to_ascii_lowercase(), alias.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut expressions = Vec::with_capacity(returning.items.len());
    for item in &returning.items {
        let mut sql_expr = item.sql_expr.clone().ok_or_else(|| {
            LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "RETURNING expression cannot be planned by DataFusion",
            )
        })?;
        rewrite_returning_image_qualifiers(&mut sql_expr, &old_aliases, &new_aliases);
        let forced_name = item.output_alias.as_ref().or_else(|| {
            item.expr
                .as_ref()
                .is_some_and(|expr| matches!(expr, BoundExpr::Column(_)))
                .then_some(&item.output_name)
        });
        expressions.push(match forced_name {
            Some(name) => format!("{sql_expr} AS {}", Ident::with_quote('"', name.clone())),
            None => sql_expr.to_string(),
        });
    }
    let sql = format!(
        "SELECT {} FROM {} AS {}",
        expressions.join(", "),
        Ident::with_quote('"', INPUT_TABLE_NAME),
        Ident::with_quote('"', target_name),
    );
    let dataframe = session
        .sql(&sql)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    let logical_plan = dataframe.into_unoptimized_plan();
    let logical_plan =
        propagate_lix_value_metadata(logical_plan).map_err(datafusion_error_to_lix_error)?;
    validate_lix_value_compatibility(&logical_plan)?;
    let logical_plan = bind_plan_param_values(logical_plan, params)?;
    let mut fields = logical_plan
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    // The private OLD/NEW input columns are an execution adapter. For an
    // implicit expression label, ask DataFusion to derive the name after
    // replacing only those private column nodes with their SQL-facing
    // qualifiers; keep the executable plan unchanged.
    if let LogicalPlan::Projection(projection) = &logical_plan {
        for (index, (item, expression)) in returning.items.iter().zip(&projection.expr).enumerate()
        {
            if item.output_alias.is_some()
                || item
                    .expr
                    .as_ref()
                    .is_some_and(|expr| matches!(expr, BoundExpr::Column(_)))
            {
                continue;
            }
            let display_expr = expression
                .clone()
                .transform_up(|expression| {
                    let Expr::Column(column) = expression else {
                        return Ok(Transformed::no(expression));
                    };
                    let replacement = old_aliases
                        .iter()
                        .find(|(_, alias)| alias.as_str() == column.name.as_str())
                        .map(|(name, _)| ("old", name))
                        .or_else(|| {
                            new_aliases
                                .iter()
                                .find(|(_, alias)| alias.as_str() == column.name.as_str())
                                .map(|(name, _)| ("new", name))
                        });
                    Ok(match replacement {
                        Some((image, name)) => Transformed::yes(Expr::Column(Column::new(
                            Some(datafusion::common::TableReference::bare(image)),
                            name.clone(),
                        ))),
                        None => Transformed::no(Expr::Column(column)),
                    })
                })
                .map_err(datafusion_error_to_lix_error)?
                .data;
            fields[index] = fields[index]
                .clone()
                .with_name(display_expr.schema_name().to_string());
        }
    }
    let state = session.state();
    let logical_plan = state
        .optimize(&logical_plan)
        .map_err(datafusion_error_to_lix_error)?;
    let read_source_columns =
        optimized_returning_source_columns(&logical_plan, INPUT_TABLE_NAME)?;
    if let Some(unavailable) = read_source_columns
        .difference(&available_source_columns)
        .next()
    {
        return Err(LixError::unknown(format!(
            "optimized RETURNING projection reads uncaptured image column {unavailable}"
        )));
    }
    let physical_plan = state
        .query_planner()
        .create_physical_plan(&logical_plan, &state)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    let batches = crate::sql2::runtime::collect_input_plan(physical_plan, session.task_ctx())
        .await
        .map_err(datafusion_error_to_lix_error)?;
    Ok((fields, batches))
}

fn returning_pseudo_column_name(image: &str, index: usize, schema: &Schema) -> String {
    let mut name = format!("__lix_returning_{image}_{index}");
    while schema.fields().iter().any(|field| field.name() == &name) {
        name.push('_');
    }
    name
}

/// Returns the source columns that the optimized RETURNING plan reads or uses
/// in a pushed-down scan filter. A pruned column still belongs in the source
/// schema used to plan the original expression, but its value need not be
/// fetched.
fn optimized_returning_source_columns(
    plan: &LogicalPlan,
    source_table_name: &str,
) -> Result<BTreeSet<String>, LixError> {
    let mut columns = BTreeSet::new();
    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && scan.table_name.to_string() == source_table_name
        {
            let schema = scan.source.schema();
            let mut indexes = scan
                .projection
                .clone()
                .unwrap_or_else(|| (0..schema.fields().len()).collect::<Vec<_>>())
                .into_iter()
                .collect::<BTreeSet<_>>();
            for filter in &scan.filters {
                filter.apply(|expr| {
                    if let Expr::Column(column) = expr
                        && let Ok(index) = schema.index_of(&column.name)
                    {
                        indexes.insert(index);
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
            }
            for index in indexes {
                if let Some(field) = schema.fields().get(index) {
                    columns.insert(field.name().clone());
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .map_err(datafusion_error_to_lix_error)?;
    Ok(columns)
}

fn rewrite_returning_image_qualifiers(
    expr: &mut SqlExpr,
    old_aliases: &BTreeMap<String, String>,
    new_aliases: &BTreeMap<String, String>,
) {
    struct Rewriter<'a> {
        old_aliases: &'a BTreeMap<String, String>,
        new_aliases: &'a BTreeMap<String, String>,
        local_range_variables: Vec<HashSet<String>>,
    }

    impl VisitorMut for Rewriter<'_> {
        type Break = ();

        fn pre_visit_select(
            &mut self,
            select: &mut datafusion::sql::sqlparser::ast::Select,
        ) -> ControlFlow<Self::Break> {
            self.local_range_variables
                .push(returning_select_range_variables(select));
            ControlFlow::Continue(())
        }

        fn post_visit_select(
            &mut self,
            _select: &mut datafusion::sql::sqlparser::ast::Select,
        ) -> ControlFlow<Self::Break> {
            self.local_range_variables.pop();
            ControlFlow::Continue(())
        }

        fn post_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
            let SqlExpr::CompoundIdentifier(identifiers) = expr else {
                return ControlFlow::Continue(());
            };
            if identifiers.len() != 2 {
                return ControlFlow::Continue(());
            }
            let qualifier = normalized_returning_identifier(&identifiers[0]);
            if self
                .local_range_variables
                .iter()
                .rev()
                .any(|scope| scope.contains(&qualifier))
            {
                return ControlFlow::Continue(());
            }
            let column = normalized_returning_identifier(&identifiers[1]);
            let alias = match qualifier.as_str() {
                "old" => self.old_aliases.get(&column),
                "new" => self.new_aliases.get(&column),
                _ => None,
            };
            if let Some(alias) = alias {
                *expr = SqlExpr::Identifier(Ident::new(alias));
            }
            ControlFlow::Continue(())
        }
    }

    let _ = expr.visit(&mut Rewriter {
        old_aliases,
        new_aliases,
        local_range_variables: Vec::new(),
    });
}

fn returning_select_range_variables(
    select: &datafusion::sql::sqlparser::ast::Select,
) -> HashSet<String> {
    let mut range_variables = HashSet::new();
    for table in &select.from {
        returning_table_factor_range_variables(&table.relation, &mut range_variables);
        for join in &table.joins {
            returning_table_factor_range_variables(&join.relation, &mut range_variables);
        }
    }
    range_variables
}

fn normalized_returning_identifier(identifier: &Ident) -> String {
    if identifier.quote_style.is_some() {
        identifier.value.clone()
    } else {
        identifier.value.to_ascii_lowercase()
    }
}

fn returning_table_factor_range_variables(
    factor: &TableFactor,
    range_variables: &mut HashSet<String>,
) {
    use TableFactor::*;

    let alias = match factor {
        Table { name, alias, .. } => {
            if let Some(alias) = alias {
                Some(&alias.name)
            } else {
                name.0.last().and_then(ObjectNamePart::as_ident)
            }
        }
        Derived { alias, .. }
        | TableFunction { alias, .. }
        | Function { alias, .. }
        | UNNEST { alias, .. }
        | JsonTable { alias, .. }
        | OpenJsonTable { alias, .. }
        | Pivot { alias, .. }
        | Unpivot { alias, .. }
        | MatchRecognize { alias, .. }
        | XmlTable { alias, .. }
        | SemanticView { alias, .. } => alias.as_ref().map(|alias| &alias.name),
        NestedJoin {
            table_with_joins,
            alias,
        } => {
            if let Some(alias) = alias {
                Some(&alias.name)
            } else {
                returning_table_factor_range_variables(&table_with_joins.relation, range_variables);
                for join in &table_with_joins.joins {
                    returning_table_factor_range_variables(&join.relation, range_variables);
                }
                None
            }
        }
    };
    if let Some(alias) = alias {
        range_variables.insert(normalized_returning_identifier(alias));
    }
}

fn push_returning_source_column(
    fields: &mut Vec<Field>,
    arrays: &mut Vec<Arc<dyn Array>>,
    null_arrays: &mut HashMap<DataType, Arc<dyn Array>>,
    field: &datafusion::arrow::datatypes::FieldRef,
    source_name: &str,
    batch: Option<&RecordBatch>,
    name: &str,
    count: usize,
) {
    fields.push(
        field
            .as_ref()
            .clone()
            .with_name(source_name)
            .with_nullable(true),
    );
    let actual = batch.and_then(|batch| batch.column_by_name(name).cloned());
    let array = actual.unwrap_or_else(|| {
        null_arrays
            .entry(field.data_type().clone())
            .or_insert_with(|| datafusion::arrow::array::new_null_array(field.data_type(), count))
            .clone()
    });
    arrays.push(array);
}

async fn insert_input_plan(
    session: &SessionContext,
    schema: SchemaRef,
    plan: &LogicalWritePlan,
    params: &[Value],
) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>, LixError> {
    match &plan.bound.input {
        BoundWriteInput::Values(values) => {
            insert_values_input_plan(session, schema, plan, params, values).await
        }
        BoundWriteInput::Query { query, columns } => {
            insert_query_input_plan(session, schema, query, columns, params, true).await
        }
        BoundWriteInput::None => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "INSERT source is required",
        )),
    }
}

async fn insert_values_input_plan(
    session: &SessionContext,
    schema: SchemaRef,
    plan: &LogicalWritePlan,
    params: &[Value],
    values: &BoundInsertValues,
) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>, LixError> {
    if values.rows.is_empty() {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "sql2 DataFusion reference writer cannot execute empty INSERT",
        ));
    }
    let field_source_indexes = schema
        .fields()
        .iter()
        .map(|field| values.column_index(field.name()))
        .collect::<Vec<_>>();
    // Keep omission intent on the VALUES input fields as well as the output
    // aliases below. DataFusion can eliminate the identity projection while
    // optimizing an INSERT, but it preserves the VALUES schema at the table
    // provider boundary.
    let nullable_schema = Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .zip(field_source_indexes.iter())
            .map(|(field, source_index)| {
                let field = Field::new(field.name(), field.data_type().clone(), true);
                if source_index.is_none() {
                    field.with_metadata(
                        [(
                            LIX_INSERT_COLUMN_OMITTED_METADATA_KEY.to_string(),
                            "true".to_string(),
                        )]
                        .into_iter()
                        .collect(),
                    )
                } else {
                    field
                }
            })
            .collect::<Vec<_>>(),
    ));
    let df_schema = Arc::new(
        DFSchema::try_from(nullable_schema).map_err(datafusion_error_to_lix_error)?,
    );
    let rows = values
        .rows
        .iter()
        .map(|row| {
            schema
                .fields()
                .iter()
                .zip(field_source_indexes.iter())
                .map(|(field, source_index)| {
                    insert_field_expr(
                        session,
                        row,
                        *source_index,
                        field.name(),
                        field.data_type(),
                        plan,
                        params,
                    )
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    for expr in rows.iter().flatten() {
        validate_lix_expr_compatibility(expr, &[df_schema.as_ref()])?;
    }
    let projection = schema
        .fields()
        .iter()
        .zip(field_source_indexes.iter())
        .enumerate()
        .map(|(index, (field, source_index))| {
            let metadata = if source_index.is_none() {
                Some(FieldMetadata::new(BTreeMap::from([(
                    LIX_INSERT_COLUMN_OMITTED_METADATA_KEY.to_string(),
                    "true".to_string(),
                )])))
            } else {
                None
            };
            Expr::Column(Column::from_name(format!("column{}", index + 1)))
                .alias_with_metadata(field.name(), metadata)
        })
        .collect::<Vec<_>>();
    let logical_plan = LogicalPlanBuilder::values_with_schema(rows, &df_schema)
        .map_err(datafusion_error_to_lix_error)?
        .project(projection)
        .map_err(datafusion_error_to_lix_error)?
        .build()
        .map_err(datafusion_error_to_lix_error)?;
    session
        .state()
        .create_physical_plan(&logical_plan)
        .await
        .map_err(datafusion_error_to_lix_error)
}

async fn insert_query_input_plan(
    session: &SessionContext,
    schema: SchemaRef,
    query: &crate::sql2::bind::read::BoundRead,
    columns: &[BoundColumnRef],
    params: &[Value],
    coerce: bool,
) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>, LixError> {
    let mut statement = DataFusionStatement::Statement(Box::new(
        datafusion::sql::sqlparser::ast::Statement::Query(query.query.clone()),
    ));
    let parameter_names = statement_parameter_names(&statement)?;
    let expected = expected_positional_parameter_count(&parameter_names)?;
    validate_parameter_count_values(expected, &parameter_names, params.len())?;
    bind_table_function_parameters(&mut statement, params)?;
    let input = create_logical_plan_from_statement(session, statement, params).await?;
    validate_supported_logical_plan(&input)?;
    let input = bind_plan_param_values(input, params)?;
    if input.schema().fields().len() != columns.len() {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            format!(
                "INSERT has {} target columns but query returns {} columns",
                columns.len(),
                input.schema().fields().len()
            ),
        ));
    }

    let input_schema = input.schema().clone();
    let projection = schema
        .fields()
        .iter()
        .map(|field| {
            let expr = columns
                .iter()
                .position(|column| column.name == *field.name())
                .map(|index| {
                    let (qualifier, source_field) = input_schema.qualified_field(index);
                    Expr::Column(Column::new(qualifier.cloned(), source_field.name().clone()))
                })
                .unwrap_or_else(|| {
                    Expr::Literal(ScalarValue::try_new_null(field.data_type()).unwrap(), None)
                });
            if coerce {
                Ok(coerce_assignment_expr(expr, field, input_schema.as_ref())?.alias(field.name()))
            } else {
                // The native writer applies schema assignment rules. Preserve
                // source logical types, especially JSONB versus SQL text.
                Ok(expr.alias(field.name()))
            }
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let dataframe = session
        .execute_logical_plan(input)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    let logical_plan = LogicalPlanBuilder::from(
        dataframe
            .into_optimized_plan()
            .map_err(datafusion_error_to_lix_error)?,
    )
    .project(projection)
    .map_err(datafusion_error_to_lix_error)?
    .build()
    .map_err(datafusion_error_to_lix_error)?;
    session
        .state()
        .create_physical_plan(&logical_plan)
        .await
        .map_err(datafusion_error_to_lix_error)
}

fn insert_column_is_omitted(values: &BoundInsertValues, field_name: &str) -> bool {
    values.column_index(field_name).is_none()
}

fn validate_bound_write_input(plan: &LogicalWritePlan, params: &[Value]) -> Result<(), LixError> {
    if plan.bound.op == BoundWriteOp::Insert
        && matches!(
            plan.bound.target,
            BoundWriteTarget::File(_) | BoundWriteTarget::Directory(_)
        )
        && let BoundWriteInput::Values(values) = &plan.bound.input
        && let Some(id_index) = values.column_index("id")
    {
        for row in &values.rows {
            let explicit_null = match &row[id_index] {
                BoundExpr::Literal(BoundLiteral::Null) => true,
                BoundExpr::Param(param) => params
                    .get(param.index.saturating_sub(1))
                    .is_some_and(|value| matches!(value, Value::Null)),
                _ => false,
            };
            if explicit_null {
                return Err(LixError::new(
                    LixError::CODE_TYPE_MISMATCH,
                    "defaulted filesystem id may be omitted, but explicit NULL is not allowed",
                ));
            }
        }
    }

    if !matches!(
        plan.bound.target,
        BoundWriteTarget::File(FileWriteSurface::Base)
    ) {
        return Ok(());
    }

    if plan.bound.op == BoundWriteOp::Insert {
        match &plan.bound.input {
            BoundWriteInput::Values(values) => {
                if let Some(column_index) = values.column_index("content") {
                    for row in &values.rows {
                        validate_lix_file_content_write_expr(&row[column_index], params, false)?;
                    }
                }
            }
            BoundWriteInput::Query { columns, .. } => {
                if columns.iter().any(|column| column.name == "content") {
                    return Err(lix_file_content_type_lix_error());
                }
            }
            BoundWriteInput::None => {}
        }
    }

    for assignment in &plan.bound.assignments {
        if assignment.column.name == "content" {
            validate_lix_file_content_write_expr(&assignment.value, params, false)?;
        }
    }
    if let Some(conflict) = &plan.bound.conflict {
        for assignment in conflict.action.assignments() {
            if assignment.column.name == "content" {
                validate_lix_file_content_write_expr(&assignment.value, params, true)?;
            }
        }
    }

    Ok(())
}

fn validate_lix_file_content_write_expr(
    expr: &BoundExpr,
    params: &[Value],
    allow_excluded_column: bool,
) -> Result<(), LixError> {
    match expr {
        BoundExpr::Param(param) => match params.get(param.index.saturating_sub(1)) {
            Some(Value::Blob(_)) => Ok(()),
            _ => Err(lix_file_content_type_lix_error()),
        },
        BoundExpr::Cast {
            data_type: BoundCastType::Binary,
            ..
        } => Ok(()),
        BoundExpr::ExcludedColumn(_) if allow_excluded_column => Ok(()),
        BoundExpr::ExcludedColumn(_) => Err(lix_file_content_type_lix_error()),
        _ => Err(lix_file_content_type_lix_error()),
    }
}

fn write_session_options(plan: &LogicalWritePlan) -> SqlWriteSessionOptions {
    let mut omitted_insert_columns = BTreeSet::new();
    if let BoundWriteInput::Values(values) = &plan.bound.input {
        if insert_column_is_omitted(values, "content") {
            omitted_insert_columns.insert("content".to_string());
        }
    }
    let explicit_insert_columns = match &plan.bound.input {
        BoundWriteInput::Values(values) => {
            let columns = values
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<BTreeSet<_>>();
            Some(columns)
        }
        BoundWriteInput::Query { columns, .. } => {
            Some(columns.iter().map(|column| column.name.clone()).collect())
        }
        BoundWriteInput::None => None,
    };
    SqlWriteSessionOptions {
        omitted_insert_columns,
        explicit_insert_columns,
    }
}

pub(crate) fn write_provider_selection(
    state: &SessionState,
    plan: &LogicalWritePlan,
    target_table_name: &str,
) -> ProviderSelection {
    let source = match (&plan.bound.op, &plan.bound.input) {
        (BoundWriteOp::Insert, BoundWriteInput::Query { query, .. }) => {
            let statement = DataFusionStatement::Statement(Box::new(SqlStatement::Query(
                query.query.clone(),
            )));
            crate::sql2::providers::read_provider_selection(state, &[statement])
        }
        _ => ProviderSelection::Only {
            names: BTreeSet::new(),
            history_relations: BTreeSet::new(),
        },
    };

    match source {
        ProviderSelection::Only {
            mut names,
            history_relations,
        } => {
            names.insert(target_table_name.to_string());
            ProviderSelection::Only {
                names,
                history_relations,
            }
        }
        ProviderSelection::OnlyWithVisibleSchemas {
            mut names,
            history_relations,
        } => {
            names.insert(target_table_name.to_string());
            ProviderSelection::OnlyWithVisibleSchemas {
                names,
                history_relations,
            }
        }
        ProviderSelection::All => ProviderSelection::All,
        ProviderSelection::AllWithHistory(history_relations) => {
            ProviderSelection::AllWithHistory(history_relations)
        }
    }
}

pub(crate) fn write_source_uses_read_table_functions(plan: &LogicalWritePlan) -> bool {
    let BoundWriteInput::Query { query, .. } = &plan.bound.input else {
        return false;
    };

    struct ReadTableFunctionVisitor;

    impl Visitor for ReadTableFunctionVisitor {
        type Break = ();

        fn pre_visit_table_factor(&mut self, table_factor: &TableFactor) -> ControlFlow<Self::Break> {
            let TableFactor::Table {
                name,
                args: Some(_),
                ..
            } = table_factor
            else {
                return ControlFlow::Continue(());
            };
            if crate::sql2::providers::READ_TABLE_FUNCTION_NAMES
                .iter()
                .any(|function| {
                    crate::sql2::parse::object_name_is_public_function(name, function)
                })
            {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }

    matches!(
        query.query.visit(&mut ReadTableFunctionVisitor),
        ControlFlow::Break(())
    )
}

/// Bound mutations bypass the SQL planner, so apply the same coercion and
/// function rewrites as SessionState::create_physical_expr before providers
/// inspect or compile these expressions (including their scan filters).
fn prepare_write_expr(
    session: &SessionContext,
    schema: &DFSchema,
    expr: Expr,
) -> Result<Expr, LixError> {
    use datafusion::logical_expr::simplify::SimplifyContext;
    use datafusion::optimizer::simplify_expressions::ExprSimplifier;

    let state = session.state();
    let config = state.config_options();
    let context = SimplifyContext::builder()
        .with_schema(Arc::new(schema.clone()))
        .with_config_options(Arc::clone(config))
        .with_query_execution_start_time(state.execution_props().query_execution_start_time)
        .build();
    let simplifier = ExprSimplifier::new(context);
    let mut expr = simplifier
        .coerce(expr, schema)
        .map_err(datafusion_error_to_lix_error)?;
    for rewrite in state.analyzer().function_rewrites() {
        expr = expr
            .transform_up(|expr| rewrite.rewrite(expr, schema, config))
            .map_err(datafusion_error_to_lix_error)?
            .data;
    }
    // Some scalar functions (for example COALESCE) lower to executable
    // expressions during simplification rather than physical planning.
    simplifier
        .simplify(expr)
        .map_err(datafusion_error_to_lix_error)
}

async fn datafusion_dml_returning(
    session: &SessionContext,
    table_schema: &Schema,
    target_name: &str,
    returning: Option<&BoundReturning>,
    params: &[Value],
    delete: bool,
) -> Result<Option<crate::sql2::providers::DmlReturning>, LixError> {
    let Some(returning) = returning else {
        return Ok(None);
    };
    if returning.items.iter().any(|item| {
        item.expr
            .as_ref()
            .is_none_or(|expr| !matches!(expr, BoundExpr::Column(_)))
    }) {
        let images =
            deferred_returning_image_columns(session, returning, table_schema, target_name, delete)
                .await?;
        let required_columns = images
            .old
            .union(&images.new)
            .cloned()
            .collect::<BTreeSet<_>>();
        return Ok(Some(crate::sql2::providers::DmlReturning::new_deferred(
            Arc::new(table_schema.clone()),
            required_columns,
            delete,
            images.old,
            images.new,
        )));
    }
    let mut input_fields = Vec::new();
    for qualifier in ["current", "old", "new"] {
        for field in table_schema.fields() {
            input_fields.push((
                Some(datafusion::common::TableReference::bare(qualifier)),
                Arc::new(field.as_ref().clone().with_nullable(true)),
            ));
        }
    }
    let df_schema = DFSchema::new_with_metadata(input_fields, Default::default())
        .map_err(datafusion_error_to_lix_error)?;
    let props = session.state_ref().read().execution_props().clone();
    let mut fields = Vec::with_capacity(returning.items.len());
    let mut expressions = Vec::with_capacity(returning.items.len());
    let mut required_columns = BTreeSet::new();

    for item in &returning.items {
        let bound_expr = item.expr.as_ref().ok_or_else(|| {
            LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "RETURNING expression requires DataFusion projection",
            )
        })?;
        let expr =
            datafusion_expr_from_bound_expr_with_schema(session, bound_expr, params, table_schema)?
                .transform_up(|expr| {
                    Ok(match expr {
                        Expr::Column(mut column) if column.relation.is_none() => {
                            column.relation =
                                Some(datafusion::common::TableReference::bare("current"));
                            Transformed::yes(Expr::Column(column))
                        }
                        expr => Transformed::no(expr),
                    })
                })
                .map_err(datafusion_error_to_lix_error)?
                .data;
        validate_lix_expr_compatibility(&expr, &[&df_schema])?;
        let kind = expr_lix_value_kind(&expr, &df_schema);
        let expr = prepare_write_expr(session, &df_schema, expr)?;
        let (_, inferred_field) = expr
            .to_field(&df_schema)
            .map_err(datafusion_error_to_lix_error)?;
        fields.push(crate::sql2::logical_value_metadata::field_with_kind(
            &inferred_field.as_ref().clone().with_name(&item.output_name),
            kind,
        ));
        expressions.push(
            datafusion::physical_expr::create_physical_expr(
                &expr,
                &df_schema,
                &props,
                &datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext::default(),
            )
                .map_err(datafusion_error_to_lix_error)?,
        );
        bound_expr_column_names(bound_expr, &mut required_columns);
    }

    Ok(Some(crate::sql2::providers::DmlReturning::new(
        Arc::new(Schema::new(fields)),
        expressions,
        required_columns,
        Arc::new(table_schema.clone()),
        delete,
        returning_image_columns(returning, delete, ReturningImage::Old),
        returning_image_columns(returning, delete, ReturningImage::New),
    )))
}

fn returning_image_columns(
    returning: &BoundReturning,
    delete: bool,
    image: ReturningImage,
) -> BTreeSet<String> {
    fn visit(
        expr: &BoundExpr,
        default: ReturningImage,
        image: ReturningImage,
        columns: &mut BTreeSet<String>,
    ) {
        match expr {
            BoundExpr::Column(column) if column.image.unwrap_or(default) == image => {
                columns.insert(column.name.clone());
            }
            BoundExpr::Cast { expr, .. } => visit(expr, default, image, columns),
            BoundExpr::Function { args, .. } => {
                for expr in args {
                    visit(expr, default, image, columns);
                }
            }
            BoundExpr::Binary { left, right, .. } => {
                visit(left, default, image, columns);
                visit(right, default, image, columns);
            }
            BoundExpr::Not(expr) => visit(expr, default, image, columns),
            BoundExpr::Predicate(predicate) => visit_predicate(predicate, default, image, columns),
            BoundExpr::Case {
                operand,
                conditions,
                else_result,
            } => {
                if let Some(operand) = operand {
                    visit(operand, default, image, columns);
                }
                for (condition, result) in conditions {
                    visit(condition, default, image, columns);
                    visit(result, default, image, columns);
                }
                if let Some(else_result) = else_result {
                    visit(else_result, default, image, columns);
                }
            }
            _ => {}
        }
    }
    fn visit_predicate(
        predicate: &BoundPredicate,
        default: ReturningImage,
        image: ReturningImage,
        columns: &mut BTreeSet<String>,
    ) {
        match predicate {
            BoundPredicate::Eq(left, right) => {
                visit(left, default, image, columns);
                visit(right, default, image, columns);
            }
            BoundPredicate::Like { expr, pattern, .. } => {
                visit(expr, default, image, columns);
                visit(pattern, default, image, columns);
            }
            BoundPredicate::IsNull(expr) | BoundPredicate::IsNotNull(expr) => {
                visit(expr, default, image, columns);
            }
            BoundPredicate::In { expr, values } => {
                visit(expr, default, image, columns);
                for value in values {
                    visit(value, default, image, columns);
                }
            }
            BoundPredicate::And(predicates) | BoundPredicate::Or(predicates) => {
                for predicate in predicates {
                    visit_predicate(predicate, default, image, columns);
                }
            }
            BoundPredicate::True | BoundPredicate::False => {}
        }
    }
    let mut columns = BTreeSet::new();
    for item in &returning.items {
        if let Some(expr) = &item.expr {
            visit(
                expr,
                if delete {
                    ReturningImage::Old
                } else {
                    ReturningImage::New
                },
                image,
                &mut columns,
            );
        }
    }
    columns
}

async fn deferred_returning_image_columns(
    session: &SessionContext,
    returning: &BoundReturning,
    table_schema: &Schema,
    target_name: &str,
    delete: bool,
) -> Result<DeferredReturningImageColumns, LixError> {
    // Plan the complete projection against a synthetic row containing every
    // possible image column. DataFusion resolves local scopes and correlations;
    // the resulting TableScan projection is the sole dependency source.
    let mut fields = table_schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let mut old_aliases = BTreeMap::new();
    let mut new_aliases = BTreeMap::new();
    for (index, field) in table_schema.fields().iter().enumerate() {
        let old_alias = returning_pseudo_column_name("old", index, table_schema);
        let new_alias = returning_pseudo_column_name("new", index, table_schema);
        fields.push(field.as_ref().clone().with_name(old_alias.clone()));
        fields.push(field.as_ref().clone().with_name(new_alias.clone()));
        old_aliases.insert(field.name().to_ascii_lowercase(), old_alias);
        new_aliases.insert(field.name().to_ascii_lowercase(), new_alias);
    }
    let input_schema = Arc::new(Schema::new(fields));
    let empty = RecordBatch::new_empty(Arc::clone(&input_schema));
    static DEPENDENCY_PLAN_ID: AtomicUsize = AtomicUsize::new(0);
    let input_table_name = format!(
        "__lix_returning_dependency_plan_{}",
        DEPENDENCY_PLAN_ID.fetch_add(1, Ordering::Relaxed)
    );
    let provider = Arc::new(
        MemTable::try_new(Arc::clone(&input_schema), vec![vec![empty]])
            .map_err(datafusion_error_to_lix_error)?,
    );
    session
        .register_table(&input_table_name, provider)
        .map_err(datafusion_error_to_lix_error)?;

    let mut expressions = Vec::with_capacity(returning.items.len());
    for (index, item) in returning.items.iter().enumerate() {
        let mut expr = item.sql_expr.clone().ok_or_else(|| {
            LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "RETURNING expression cannot be planned by DataFusion",
            )
        })?;
        rewrite_returning_image_qualifiers(&mut expr, &old_aliases, &new_aliases);
        // This projection exists only to resolve image dependencies. Use
        // unique private labels so distinct expressions with the same native
        // DataFusion display name (for example two scalar subqueries) remain
        // plannable; the user-facing projection below owns result labels.
        expressions.push(format!(
            "{expr} AS {}",
            Ident::with_quote('"', format!("__lix_returning_dependency_{index}"))
        ));
    }
    let sql = format!(
        "SELECT {} FROM {} AS {}",
        expressions.join(", "),
        Ident::with_quote('"', input_table_name.clone()),
        Ident::with_quote('"', target_name),
    );
    let dataframe = session
        .sql(&sql)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    let plan = dataframe.into_unoptimized_plan();
    let state = session.state();
    let plan = state
        .optimize(&plan)
        .map_err(datafusion_error_to_lix_error)?;
    let default_image = if delete {
        ReturningImage::Old
    } else {
        ReturningImage::New
    };
    let mut images = DeferredReturningImageColumns::default();
    for name in optimized_returning_source_columns(&plan, &input_table_name)? {
        if table_schema.field_with_name(&name).is_ok() {
            images.insert(default_image, name);
        } else if let Some((column, _)) = old_aliases
            .iter()
            .find(|(_, alias)| alias.as_str() == name)
        {
            images.insert(ReturningImage::Old, column.clone());
        } else if let Some((column, _)) = new_aliases
            .iter()
            .find(|(_, alias)| alias.as_str() == name)
        {
            images.insert(ReturningImage::New, column.clone());
        }
    }
    Ok(images)
}

fn bound_expr_column_names(expr: &BoundExpr, columns: &mut BTreeSet<String>) {
    match expr {
        BoundExpr::Column(column) => {
            columns.insert(column.name.clone());
        }
        BoundExpr::ExcludedColumn(_) | BoundExpr::Param(_) | BoundExpr::Literal(_) => {}
        BoundExpr::Cast { expr, .. } => bound_expr_column_names(expr, columns),
        BoundExpr::Not(expr) => bound_expr_column_names(expr, columns),
        BoundExpr::Function { args, .. } => {
            for arg in args {
                bound_expr_column_names(arg, columns);
            }
        }
        BoundExpr::Binary { left, right, .. } => {
            bound_expr_column_names(left, columns);
            bound_expr_column_names(right, columns);
        }
        BoundExpr::Predicate(predicate) => bound_predicate_column_names(predicate, columns),
        BoundExpr::Case {
            operand,
            conditions,
            else_result,
        } => {
            if let Some(operand) = operand {
                bound_expr_column_names(operand, columns);
            }
            for (condition, result) in conditions {
                bound_expr_column_names(condition, columns);
                bound_expr_column_names(result, columns);
            }
            if let Some(else_result) = else_result {
                bound_expr_column_names(else_result, columns);
            }
        }
    }
}

fn bound_predicate_column_names(predicate: &BoundPredicate, columns: &mut BTreeSet<String>) {
    match predicate {
        BoundPredicate::Eq(left, right) => {
            bound_expr_column_names(left, columns);
            bound_expr_column_names(right, columns);
        }
        BoundPredicate::Like { expr, pattern, .. } => {
            bound_expr_column_names(expr, columns);
            bound_expr_column_names(pattern, columns);
        }
        BoundPredicate::IsNull(expr) | BoundPredicate::IsNotNull(expr) => {
            bound_expr_column_names(expr, columns);
        }
        BoundPredicate::In { expr, values } => {
            bound_expr_column_names(expr, columns);
            for value in values {
                bound_expr_column_names(value, columns);
            }
        }
        BoundPredicate::And(predicates) | BoundPredicate::Or(predicates) => {
            for predicate in predicates {
                bound_predicate_column_names(predicate, columns);
            }
        }
        BoundPredicate::True | BoundPredicate::False => {}
    }
}

async fn sql_write_empty_returning_result(
    session: &SessionContext,
    table_schema: &Schema,
    target_name: &str,
    delete: bool,
    returning: Option<&crate::sql2::providers::DmlReturning>,
    bound_returning: Option<&BoundReturning>,
    params: &[Value],
) -> Result<SqlWriteResult, LixError> {
    let Some(returning) = returning else {
        return Ok(SqlWriteResult::affected(0));
    };
    if returning.is_deferred_projection() {
        let bound_returning = bound_returning.ok_or_else(|| {
            LixError::unknown("deferred RETURNING plan lost its bound SQL expressions")
        })?;
        let empty = RecordBatch::new_empty(Arc::new(table_schema.clone()));
        let (fields, _) = datafusion_returning_projection(
            session,
            table_schema,
            target_name,
            delete,
            bound_returning,
            &DeferredReturningImageColumns {
                old: returning.old_columns().clone(),
                new: returning.new_columns().clone(),
            },
            Some(&empty),
            Some(&empty),
            params,
        )
        .await?;
        return Ok(SqlWriteResult::returning(
            0,
            query_result_from_batches(&fields, &[])?,
        ));
    }
    let fields = returning
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    Ok(SqlWriteResult::returning(
        0,
        query_result_from_batches(&fields, &[])?,
    ))
}

async fn sql_write_captured_returning_result(
    rows_affected: u64,
    returning: &crate::sql2::providers::DmlReturning,
    session: &SessionContext,
    table_schema: &Schema,
    target_name: &str,
    delete: bool,
    bound_returning: &BoundReturning,
    params: &[Value],
) -> Result<SqlWriteResult, LixError> {
    if returning.is_deferred_projection() {
        let images = returning
            .take_captured_images()
            .map_err(datafusion_error_to_lix_error)?;
        let (fields, batches) = datafusion_returning_projection(
            session,
            table_schema,
            target_name,
            delete,
            bound_returning,
            &DeferredReturningImageColumns {
                old: returning.old_columns().clone(),
                new: returning.new_columns().clone(),
            },
            images.old.as_ref(),
            images.new.as_ref(),
            params,
        )
        .await?;
        return Ok(SqlWriteResult::returning(
            rows_affected,
            query_result_from_batches(&fields, &batches)?,
        ));
    }
    let batch = returning
        .take_captured()
        .map_err(datafusion_error_to_lix_error)?;
    let fields = returning
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let result = query_result_from_batches(&fields, &[batch])?;
    Ok(SqlWriteResult::returning(rows_affected, result))
}

fn insert_field_expr(
    session: &SessionContext,
    row: &[BoundExpr],
    source_index: Option<usize>,
    _field_name: &str,
    data_type: &DataType,
    _plan: &LogicalWritePlan,
    params: &[Value],
) -> Result<Expr, LixError> {
    source_index
        .map(|column_index| datafusion_expr_from_bound_expr(session, &row[column_index], params))
        .unwrap_or_else(|| {
            ScalarValue::try_new_null(data_type)
                .map(|value| Expr::Literal(value, None))
                .map_err(datafusion_error_to_lix_error)
        })
}

fn assignment_source_expr(
    session: &SessionContext,
    expr: &BoundExpr,
    params: &[Value],
    schema: &Schema,
) -> Result<Expr, LixError> {
    datafusion_expr_from_bound_expr_with_schema(session, expr, params, schema)
}

fn coerce_assignment_expr(expr: Expr, field: &Field, schema: &DFSchema) -> Result<Expr, LixError> {
    let target = field.data_type();
    let (_, source_field) = expr
        .to_field(schema)
        .map_err(datafusion_error_to_lix_error)?;
    if field_is_json(&source_field) && !field_is_json(field) {
        return Err(LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            "JSONB assignment to a scalar column requires an explicit CAST",
        ));
    }
    let source = expr
        .get_type(schema)
        .map_err(datafusion_error_to_lix_error)?;
    crate::sql2::value_contract::validate_assignment_types(&source, target)?;
    expr.cast_to(target, schema)
        .map_err(datafusion_error_to_lix_error)
}

fn datafusion_assignments(
    session: &SessionContext,
    schema: &Schema,
    plan: &LogicalWritePlan,
    params: &[Value],
) -> Result<Vec<(String, Expr)>, LixError> {
    let df_schema = DFSchema::try_from(schema.clone()).map_err(datafusion_error_to_lix_error)?;
    plan.bound
        .assignments
        .iter()
        .map(|assignment| {
            let field = schema
                .field_with_name(&assignment.column.name)
                .map_err(|error| LixError::unknown(format!("unknown update column: {error}")))?;
            let expr = assignment_source_expr(session, &assignment.value, params, schema)?;
            validate_lix_expr_compatibility(&expr, &[&df_schema])?;
            let expr = prepare_write_expr(session, &df_schema, expr)?;
            validate_lix_expr_compatibility(&expr, &[&df_schema])?;
            let expr = coerce_assignment_expr(expr, field, &df_schema)?;
            Ok((assignment.column.name.clone(), expr))
        })
        .collect()
}

/// Compile `DO UPDATE` conflict assignments to physical expressions over the
/// augmented schema `[table cols..., excluded.<col>...]`, so `excluded.*`
/// references resolve against the proposed-row columns the upsert driver
/// appends.
fn datafusion_conflict_assignments(
    session: &SessionContext,
    schema: &Schema,
    assignments: &[crate::sql2::bind::write::BoundAssignment],
    params: &[Value],
) -> Result<
    Vec<(
        String,
        Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    )>,
    LixError,
> {
    let mut fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    for field in schema.fields() {
        fields.push(Field::new(
            crate::sql2::providers::excluded_field_name(field.name()),
            field.data_type().clone(),
            field.is_nullable(),
        ));
    }
    let augmented = Schema::new(fields);
    let df_schema = DFSchema::try_from(augmented.clone()).map_err(datafusion_error_to_lix_error)?;
    let props = session.state_ref().read().execution_props().clone();

    assignments
        .iter()
        .map(|assignment| {
            let field = schema
                .field_with_name(&assignment.column.name)
                .map_err(|error| LixError::unknown(format!("unknown conflict column: {error}")))?;
            let expr = assignment_source_expr(session, &assignment.value, params, &augmented)?;
            validate_lix_expr_compatibility(&expr, &[&df_schema])?;
            let expr = prepare_write_expr(session, &df_schema, expr)?;
            validate_lix_expr_compatibility(&expr, &[&df_schema])?;
            let expr = coerce_assignment_expr(expr, field, &df_schema)?;
            let physical =
                datafusion::physical_expr::create_physical_expr(
                    &expr,
                    &df_schema,
                    &props,
                    &datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext::default(),
                )
                    .map_err(datafusion_error_to_lix_error)?;
            Ok((assignment.column.name.clone(), physical))
        })
        .collect()
}

fn datafusion_write_filters(
    session: &SessionContext,
    schema: &Schema,
    plan: &LogicalWritePlan,
    params: &[Value],
) -> Result<Vec<Expr>, LixError> {
    let df_schema = DFSchema::try_from(schema.clone()).map_err(datafusion_error_to_lix_error)?;
    let mut filters =
        datafusion_filters_from_predicate(session, schema, &plan.bound.predicate, params)?
            .into_iter()
            .map(|expr| {
                validate_lix_expr_compatibility(&expr, &[&df_schema])?;
                prepare_write_expr(session, &df_schema, expr)
            })
            .collect::<Result<Vec<_>, _>>()?;
    for filter in &filters {
        validate_lix_expr_compatibility(filter, &[&df_schema])?;
    }
    if plan.bound.branch_scope == BranchScope::Global {
        let branch_column = schema
            .field_with_name("branch_id")
            .is_ok()
            .then_some("branch_id");
        let Some(branch_column) = branch_column else {
            return Ok(filters);
        };
        filters.push(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name(branch_column))),
            Operator::Eq,
            Box::new(Expr::Literal(
                ScalarValue::Utf8(Some(GLOBAL_BRANCH_ID.to_string())),
                None,
            )),
        )));
    }
    Ok(filters)
}

fn datafusion_filters_from_predicate(
    session: &SessionContext,
    schema: &Schema,
    predicate: &BoundPredicate,
    params: &[Value],
) -> Result<Vec<Expr>, LixError> {
    match predicate {
        BoundPredicate::True => Ok(Vec::new().into()),
        BoundPredicate::False => Ok(vec![Expr::Literal(ScalarValue::Boolean(Some(false)), None)]),
        BoundPredicate::And(predicates) => {
            let mut filters = Vec::new();
            for predicate in predicates {
                filters.extend(datafusion_filters_from_predicate(
                    session, schema, predicate, params,
                )?);
            }
            Ok(filters)
        }
        BoundPredicate::Or(predicates) => {
            let mut iter = predicates.iter();
            let Some(first) = iter.next() else {
                return Ok(Vec::new().into());
            };
            let mut expr = datafusion_single_filter_from_predicate(session, schema, first, params)?;
            for predicate in iter {
                expr = Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(expr),
                    Operator::Or,
                    Box::new(datafusion_single_filter_from_predicate(
                        session, schema, predicate, params,
                    )?),
                ));
            }
            Ok(vec![expr])
        }
        BoundPredicate::Eq(left, right) => {
            let left_expr = datafusion_expr_from_bound_expr(session, left, params)?;
            let right_expr = datafusion_expr_from_bound_expr(session, right, params)?;
            Ok(vec![Expr::BinaryExpr(BinaryExpr::new(
                Box::new(left_expr),
                Operator::Eq,
                Box::new(right_expr),
            ))])
        }
        BoundPredicate::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
            escape_char,
        } => Ok(vec![Expr::Like(Like::new(
            *negated,
            Box::new(datafusion_expr_from_bound_expr(session, expr, params)?),
            Box::new(datafusion_expr_from_bound_expr(session, pattern, params)?),
            *escape_char,
            *case_insensitive,
        ))]),
        BoundPredicate::IsNull(expr) => Ok(vec![Expr::IsNull(Box::new(
            datafusion_expr_from_bound_expr(session, expr, params)?,
        ))]),
        BoundPredicate::IsNotNull(expr) => Ok(vec![Expr::IsNotNull(Box::new(
            datafusion_expr_from_bound_expr(session, expr, params)?,
        ))]),
        BoundPredicate::In { expr, values } => {
            let input_expr = datafusion_expr_from_bound_expr(session, expr, params)?;
            Ok(vec![Expr::InList(InList::new(
                Box::new(input_expr),
                values
                    .iter()
                    .map(|value| datafusion_expr_from_bound_expr(session, value, params))
                    .collect::<Result<Vec<_>, _>>()?,
                false,
            ))])
        }
    }
}

fn datafusion_single_filter_from_predicate(
    session: &SessionContext,
    schema: &Schema,
    predicate: &BoundPredicate,
    params: &[Value],
) -> Result<Expr, LixError> {
    let filters = datafusion_filters_from_predicate(session, schema, predicate, params)?;
    let mut iter = filters.into_iter();
    let mut expr = iter
        .next()
        .unwrap_or_else(|| Expr::Literal(ScalarValue::Boolean(Some(true)), None));
    for filter in iter {
        expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(expr),
            Operator::And,
            Box::new(filter),
        ));
    }
    Ok(expr)
}

fn datafusion_expr_from_bound_expr(
    session: &SessionContext,
    expr: &BoundExpr,
    params: &[Value],
) -> Result<Expr, LixError> {
    let schema = Schema::empty();
    datafusion_expr_from_bound_expr_inner(session, expr, params, &schema)
}

fn datafusion_expr_from_bound_expr_with_schema(
    session: &SessionContext,
    expr: &BoundExpr,
    params: &[Value],
    schema: &Schema,
) -> Result<Expr, LixError> {
    datafusion_expr_from_bound_expr_inner(session, expr, params, schema)
}

fn datafusion_expr_from_bound_expr_inner(
    session: &SessionContext,
    expr: &BoundExpr,
    params: &[Value],
    schema: &Schema,
) -> Result<Expr, LixError> {
    match expr {
        BoundExpr::Column(column) => Ok(Expr::Column(match column.image {
            Some(image) => Column::new(Some(image.qualifier()), column.name.clone()),
            None => Column::from_name(column.name.clone()),
        })),
        // `excluded.<col>` resolves to the proposed row's value, carried in the
        // augmented conflict batch as an `excluded.<col>` column.
        BoundExpr::ExcludedColumn(column) => Ok(Expr::Column(Column::from_name(
            crate::sql2::providers::excluded_field_name(&column.name),
        ))),
        BoundExpr::Literal(literal) => Ok(Expr::Literal(
            scalar_from_bound_literal(literal)?,
            bound_literal_metadata(literal),
        )),
        BoundExpr::Param(param) => {
            let Some(value) = params.get(param.index - 1) else {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("missing SQL parameter ${}", param.index),
                ));
            };
            let ScalarAndMetadata { value, metadata } = scalar_value_from_lix_value(value)?;
            Ok(Expr::Literal(value, metadata))
        }
        BoundExpr::Cast { expr, data_type } => {
            let expr = datafusion_expr_from_bound_expr_inner(session, expr, params, schema)?;
            if *data_type == BoundCastType::Text {
                let udf = session
                    .udf("__lix_text_cast")
                    .map_err(datafusion_error_to_lix_error)?;
                return Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
                    udf,
                    vec![expr],
                )));
            }
            if *data_type == BoundCastType::Uuid {
                let udf = session
                    .udf("__lix_uuid_cast")
                    .map_err(datafusion_error_to_lix_error)?;
                return Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
                    udf,
                    vec![expr],
                )));
            }
            if *data_type == BoundCastType::Jsonb {
                let udf = session
                    .udf("__lix_jsonb")
                    .map_err(datafusion_error_to_lix_error)?;
                return Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
                    udf,
                    vec![expr],
                )));
            }
            let bound_data_type = match data_type {
                BoundCastType::Text => unreachable!("TEXT casts are handled by __lix_text_cast"),
                BoundCastType::Uuid => unreachable!("UUID casts are handled by __lix_uuid_cast"),
                BoundCastType::Binary => DataType::Binary,
                BoundCastType::BigInt => DataType::Int64,
                BoundCastType::Double => DataType::Float64,
                BoundCastType::Boolean => DataType::Boolean,
                BoundCastType::Jsonb => unreachable!("JSONB casts are handled by __lix_jsonb"),
            };
            let cast = Expr::Cast(Cast::new(Box::new(expr), bound_data_type));
            Ok(cast)
        }
        BoundExpr::Function { name, args } => {
            let udf = session.udf(name).map_err(datafusion_error_to_lix_error)?;
            let args = args
                .iter()
                .map(|arg| datafusion_expr_from_bound_expr_inner(session, arg, params, schema))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Expr::ScalarFunction(ScalarFunction::new_udf(udf, args)))
        }
        BoundExpr::Binary { left, op, right } => {
            let left_expr = datafusion_expr_from_bound_expr_inner(session, left, params, schema)?;
            let right_expr = datafusion_expr_from_bound_expr_inner(session, right, params, schema)?;
            Ok(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(left_expr),
                match op {
                    BoundBinaryOperator::Add => Operator::Plus,
                    BoundBinaryOperator::Subtract => Operator::Minus,
                    BoundBinaryOperator::Multiply => Operator::Multiply,
                    BoundBinaryOperator::Divide => Operator::Divide,
                    BoundBinaryOperator::Modulo => Operator::Modulo,
                    BoundBinaryOperator::StringConcat => Operator::StringConcat,
                    BoundBinaryOperator::Eq => Operator::Eq,
                    BoundBinaryOperator::NotEq => Operator::NotEq,
                    BoundBinaryOperator::Lt => Operator::Lt,
                    BoundBinaryOperator::LtEq => Operator::LtEq,
                    BoundBinaryOperator::Gt => Operator::Gt,
                    BoundBinaryOperator::GtEq => Operator::GtEq,
                    BoundBinaryOperator::And => Operator::And,
                    BoundBinaryOperator::Or => Operator::Or,
                },
                Box::new(right_expr),
            )))
        }
        BoundExpr::Not(expr) => Ok(Expr::Not(Box::new(datafusion_expr_from_bound_expr_inner(
            session, expr, params, schema,
        )?))),
        BoundExpr::Predicate(predicate) => {
            datafusion_single_filter_from_predicate(session, schema, predicate, params)
        }
        BoundExpr::Case {
            operand,
            conditions,
            else_result,
        } => {
            let operand = operand
                .as_deref()
                .map(|expr| datafusion_expr_from_bound_expr_inner(session, expr, params, schema))
                .transpose()?
                .map(Box::new);
            let when_then_expr = conditions
                .iter()
                .map(|(condition, result)| {
                    Ok((
                        Box::new(datafusion_expr_from_bound_expr_inner(
                            session, condition, params, schema,
                        )?),
                        Box::new(datafusion_expr_from_bound_expr_inner(
                            session, result, params, schema,
                        )?),
                    ))
                })
                .collect::<Result<Vec<_>, LixError>>()?;
            let else_expr = else_result
                .as_deref()
                .map(|expr| datafusion_expr_from_bound_expr_inner(session, expr, params, schema))
                .transpose()?
                .map(Box::new);
            Ok(Expr::Case(Case::new(operand, when_then_expr, else_expr)))
        }
    }
}

fn scalar_from_bound_literal(literal: &BoundLiteral) -> Result<ScalarValue, LixError> {
    Ok(match literal {
        BoundLiteral::Null => ScalarValue::Null,
        BoundLiteral::Bool(value) => ScalarValue::Boolean(Some(*value)),
        BoundLiteral::Integer(value) => ScalarValue::Int64(Some(*value)),
        BoundLiteral::Number { value, .. } => value.as_u64().map_or_else(
            || ScalarValue::Float64(value.as_f64()),
            |value| ScalarValue::UInt64(Some(value)),
        ),
        BoundLiteral::Text(value) => ScalarValue::Utf8(Some(value.clone())),
        BoundLiteral::Json(value) => ScalarValue::Utf8(Some(value.to_string())),
    })
}

fn bound_literal_metadata(literal: &BoundLiteral) -> Option<FieldMetadata> {
    match literal {
        BoundLiteral::Json(_) => Some(json_field_metadata()),
        _ => None,
    }
}

pub(crate) fn write_target_table_name(plan: &LogicalWritePlan) -> Result<String, LixError> {
    match &plan.bound.target {
        BoundWriteTarget::Row(RowWriteSurface::Base { schema_key })
            if bound_predicate_contains_like(&plan.bound.predicate)
                || bound_write_requires_datafusion(plan) =>
        {
            Ok(schema_key.clone())
        }
        BoundWriteTarget::File(FileWriteSurface::Base) => Ok("lix_file".to_string()),
        BoundWriteTarget::Directory(DirectoryWriteSurface::Base) => Ok("lix_directory".to_string()),
        BoundWriteTarget::Branch => Ok("lix_branch".to_string()),
        BoundWriteTarget::Row(_) => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "sql2 DataFusion reference writer does not support this row write",
        )),
    }
}

fn bound_write_requires_datafusion(plan: &LogicalWritePlan) -> bool {
    (matches!(plan.bound.op, BoundWriteOp::Insert)
        && matches!(plan.bound.input, BoundWriteInput::Query { .. }))
        || (matches!(plan.bound.op, BoundWriteOp::Update)
        && (plan
            .bound
            .assignments
            .iter()
            .any(|assignment| bound_expr_requires_datafusion(&assignment.value))
            || bound_predicate_requires_datafusion(&plan.bound.predicate)))
        || plan.bound.returning.as_ref().is_some_and(|returning| {
            returning.items.iter().any(|item| {
                item.expr
                    .as_ref()
                    .is_none_or(|expr| !matches!(expr, BoundExpr::Column(_)))
            })
        })
}

fn bound_expr_requires_datafusion(expr: &BoundExpr) -> bool {
    match expr {
        BoundExpr::Binary { .. }
        | BoundExpr::Not(_)
        | BoundExpr::Predicate(_)
        | BoundExpr::Case { .. } => true,
        BoundExpr::Cast { expr, .. } => bound_expr_requires_datafusion(expr),
        BoundExpr::Function { name, args } => {
            !matches!(
                name.as_str(),
                "uuidv7"
                    | "__lix_uuid_cast"
                    | "__lix_text_cast"
                    | "__lix_timestamptz_cast"
                    | "__lix_current_timestamp"
                    | "lix_active_branch_id"
                    | "lix_active_branch_commit_id"
                    | "__lix_json_get"
                    | "__lix_json_get_text"
                    | "__lix_json_path_get"
                    | "__lix_json_path_get_text"
                    | "__lix_json_contains"
                    | "__lix_json_exists"
                    | "__lix_jsonb"
                    | "lix_order_between"
                    | "lix_row_ref"
            ) || args.iter().any(bound_expr_requires_datafusion)
        }
        BoundExpr::Column(_)
        | BoundExpr::ExcludedColumn(_)
        | BoundExpr::Param(_)
        | BoundExpr::Literal(_) => false,
    }
}

fn bound_predicate_requires_datafusion(predicate: &BoundPredicate) -> bool {
    match predicate {
        BoundPredicate::Eq(left, right) => {
            bound_expr_requires_datafusion(left) || bound_expr_requires_datafusion(right)
        }
        BoundPredicate::Like { expr, pattern, .. } => {
            bound_expr_requires_datafusion(expr) || bound_expr_requires_datafusion(pattern)
        }
        BoundPredicate::IsNull(expr) | BoundPredicate::IsNotNull(expr) => {
            bound_expr_requires_datafusion(expr)
        }
        BoundPredicate::In { expr, values, .. } => {
            bound_expr_requires_datafusion(expr)
                || values.iter().any(bound_expr_requires_datafusion)
        }
        BoundPredicate::And(predicates) | BoundPredicate::Or(predicates) => {
            predicates.iter().any(bound_predicate_requires_datafusion)
        }
        BoundPredicate::True | BoundPredicate::False => false,
    }
}

fn bound_predicate_contains_like(predicate: &BoundPredicate) -> bool {
    match predicate {
        BoundPredicate::Like { .. } => true,
        BoundPredicate::And(predicates) | BoundPredicate::Or(predicates) => {
            predicates.iter().any(bound_predicate_contains_like)
        }
        BoundPredicate::True
        | BoundPredicate::False
        | BoundPredicate::Eq(_, _)
        | BoundPredicate::IsNull(_)
        | BoundPredicate::IsNotNull(_)
        | BoundPredicate::In { .. } => false,
    }
}

fn affected_rows_from_query_result(result: SqlQueryResult) -> Result<u64, LixError> {
    let Some(first_row) = result.rows.first() else {
        return Ok(0);
    };
    let Some(first_value) = first_row.first() else {
        return Ok(0);
    };
    match first_value {
        Value::Integer(value) if *value >= 0 => Ok(*value as u64),
        Value::Text(value) => value.parse::<u64>().map_err(|error| {
            LixError::new(
                LixError::CODE_UNKNOWN,
                format!("failed to parse affected row count from SQL result: {error}"),
            )
        }),
        other => Err(LixError::new(
            LixError::CODE_UNKNOWN,
            format!("expected affected row count, got {other:?}"),
        )),
    }
}

fn validate_parameter_count_values(
    expected_count: usize,
    parameter_names: &HashSet<String>,
    param_count: usize,
) -> Result<(), LixError> {
    if param_count == expected_count {
        return Ok(());
    }

    Err(LixError::new(
        LixError::CODE_INVALID_PARAM,
        format!(
            "SQL expected {expected_count} parameter(s), but {param_count} parameter(s) were provided"
        ),
    )
    .with_details(json!({
        "operation": "execute",
        "expected_param_count": expected_count,
        "provided_param_count": param_count,
        "placeholders": sorted_parameter_names(&parameter_names),
    })))
}

fn statement_parameter_names(statement: &DataFusionStatement) -> Result<HashSet<String>, LixError> {
    struct ParameterVisitor {
        names: HashSet<String>,
    }

    impl Visitor for ParameterVisitor {
        type Break = ();

        fn pre_visit_expr(&mut self, expression: &SqlExpr) -> ControlFlow<Self::Break> {
            if let SqlExpr::Value(value) = expression
                && let SqlValue::Placeholder(name) = &value.value
            {
                self.names.insert(name.clone());
            }
            ControlFlow::Continue(())
        }
    }

    fn visit(
        statement: &DataFusionStatement,
        visitor: &mut ParameterVisitor,
    ) -> Result<(), LixError> {
        match statement {
            DataFusionStatement::Statement(statement) => {
                let _ = statement.visit(visitor);
                Ok(())
            }
            DataFusionStatement::Explain(explain) => visit(explain.statement.as_ref(), visitor),
            _ => Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "SQL statement is not supported by Lix SQL",
            )),
        }
    }

    let mut visitor = ParameterVisitor {
        names: HashSet::new(),
    };
    visit(statement, &mut visitor)?;
    Ok(visitor.names)
}

fn bind_table_function_parameters(
    statement: &mut DataFusionStatement,
    params: &[Value],
) -> Result<(), LixError> {
    struct TableFunctionParameterBinder<'a> {
        params: &'a [Value],
    }

    impl VisitorMut for TableFunctionParameterBinder<'_> {
        type Break = Box<LixError>;

        fn pre_visit_table_factor(
            &mut self,
            table_factor: &mut TableFactor,
        ) -> ControlFlow<Self::Break> {
            let TableFactor::Table {
                name,
                args: Some(arguments),
                ..
            } = table_factor
            else {
                return ControlFlow::Continue(());
            };
            let public_function_name = [
                "lix_history",
                "lix_diff",
                "lix_as_of",
                "lix_commit_ancestry",
            ]
            .into_iter()
            .find(|candidate| crate::sql2::parse::object_name_is_public_function(name, candidate));
            if let Some(function_name) = public_function_name {
                // DataFusion's table-function registry is case-sensitive and
                // global rather than schema-scoped. Normalize the public SQL
                // spelling after preserving quoted-identifier semantics.
                *name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(function_name))]);
            }
            for argument in &mut arguments.args {
                let FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) = argument else {
                    continue;
                };
                let SqlExpr::Value(value) = expression else {
                    continue;
                };
                let SqlValue::Placeholder(name) = &value.value else {
                    continue;
                };
                let Some(index) = name
                    .strip_prefix('$')
                    .and_then(|raw| raw.parse::<usize>().ok())
                    .and_then(|index| index.checked_sub(1))
                else {
                    return ControlFlow::Break(Box::new(LixError::new(
                        LixError::CODE_PARSE_ERROR,
                        format!("unsupported SQL parameter placeholder '{name}'"),
                    )));
                };
                let Some(param) = self.params.get(index) else {
                    return ControlFlow::Break(Box::new(LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        format!("missing SQL parameter ${}", index + 1),
                    )));
                };
                let Value::Text(text) = param else {
                    return ControlFlow::Break(Box::new(LixError::new(
                        LixError::CODE_TYPE_MISMATCH,
                        "table function arguments must be text",
                    )));
                };
                *expression = SqlExpr::value(SqlValue::SingleQuotedString(text.clone()));
            }
            ControlFlow::Continue(())
        }
    }

    fn visit(
        statement: &mut DataFusionStatement,
        visitor: &mut TableFunctionParameterBinder<'_>,
    ) -> Result<(), LixError> {
        let result = match statement {
            DataFusionStatement::Statement(statement) => statement.visit(visitor),
            DataFusionStatement::Explain(explain) => {
                return visit(explain.statement.as_mut(), visitor);
            }
            _ => return Ok(()),
        };
        match result {
            ControlFlow::Continue(()) => Ok(()),
            ControlFlow::Break(error) => Err(*error),
        }
    }

    visit(statement, &mut TableFunctionParameterBinder { params })
}

fn expected_positional_parameter_count(
    parameter_names: &HashSet<String>,
) -> Result<usize, LixError> {
    let mut max_index = 0usize;
    for name in parameter_names {
        let Some(index) = name
            .strip_prefix('$')
            .and_then(|raw| raw.parse::<usize>().ok())
        else {
            return Err(LixError::new(
                LixError::CODE_PARSE_ERROR,
                format!("unsupported SQL parameter placeholder '{name}'"),
            )
            .with_hint("Use PostgreSQL-style numbered placeholders like $1, $2, ...")
            .with_details(json!({
                "operation": "execute",
                "placeholder": name,
            })));
        };
        if index == 0 {
            return Err(LixError::new(
                LixError::CODE_PARSE_ERROR,
                "SQL parameter placeholders are 1-indexed",
            )
            .with_hint("Use PostgreSQL-style numbered placeholders like $1, $2, ...")
            .with_details(json!({
                "operation": "execute",
                "placeholder": name,
            })));
        }
        max_index = max_index.max(index);
    }
    Ok(max_index)
}

fn sorted_parameter_names(parameter_names: &HashSet<String>) -> Vec<String> {
    let mut names = parameter_names.iter().cloned().collect::<Vec<_>>();
    names.sort();
    names
}

fn validate_supported_logical_plan(plan: &LogicalPlan) -> Result<(), LixError> {
    match plan {
        LogicalPlan::Ddl(_) => {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "DDL statements are not supported by Lix SQL",
            )
            .with_hint(
                "Use Lix SQL surfaces such as lix_registered_schema, lix_branch, lix_file, and lix_key_value instead of CREATE/DROP statements.",
            ));
        }
        LogicalPlan::Statement(_) => {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "SQL utility statements are not supported by Lix SQL",
            ));
        }
        LogicalPlan::Copy(_) => {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "COPY statements are not supported by Lix SQL",
            ));
        }
        _ => {}
    }

    for input in plan.inputs() {
        validate_supported_logical_plan(input)?;
    }

    Ok(())
}

fn scalar_value_from_lix_value(value: &Value) -> Result<ScalarAndMetadata, LixError> {
    let metadata = match value {
        Value::Jsonb(_) => Some(json_field_metadata()),
        Value::RowRef(_) => Some(row_ref_field_metadata()),
        _ => None,
    };
    Ok(ScalarAndMetadata::new(
        crate::sql2::value_contract::public_scalar(value)?,
        metadata,
    ))
}

fn json_field_metadata() -> FieldMetadata {
    FieldMetadata::new(BTreeMap::from([(
        LIX_VALUE_TYPE_METADATA_KEY.to_string(),
        LIX_VALUE_TYPE_JSONB.to_string(),
    )]))
}

fn row_ref_field_metadata() -> FieldMetadata {
    FieldMetadata::new(BTreeMap::from([(
        LIX_VALUE_TYPE_METADATA_KEY.to_string(),
        LIX_VALUE_TYPE_ROW_REF.to_string(),
    )]))
}

fn datafusion_error_to_lix_error(error: datafusion::error::DataFusionError) -> LixError {
    crate::sql2::error::datafusion_error_to_lix_error(error)
}

pub(crate) fn query_result_from_batches(
    result_fields: &[Field],
    batches: &[RecordBatch],
) -> Result<SqlQueryResult, LixError> {
    let result_columns = result_fields
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let column_types = result_fields
        .iter()
        .map(crate::sql2::result_column_type)
        .collect::<Result<Vec<_>, _>>()?;
    let mut rows =
        Vec::<Vec<Value>>::with_capacity(batches.iter().map(RecordBatch::num_rows).sum::<usize>());
    for batch in batches {
        append_batch_rows(result_fields, batch, &mut rows)?;
    }

    Ok(SqlQueryResult {
        rows,
        columns: result_columns,
        column_types,
        notices: Vec::new(),
    })
}

fn resolved_result_fields(logical: &[Field], physical: &Schema) -> Vec<Field> {
    logical
        .iter()
        .zip(physical.fields())
        .map(|(logical, physical)| {
            physical
                .as_ref()
                .clone()
                .with_name(logical.name())
                .with_metadata(logical.metadata().clone())
        })
        .collect()
}

/// Materializes Arrow batches into one row-major value arena.
///
/// The public result backing can retain this arena directly, avoiding one
/// allocation for every result row and the cache-unfriendly column-by-column
/// writes into thousands of independently allocated `Vec`s.
pub(crate) fn query_values_from_batches(
    result_fields: &[Field],
    batches: &[RecordBatch],
) -> Result<(Vec<Value>, usize), LixError> {
    let column_count = result_fields.len();
    let mut row_count = 0_usize;
    let mut cell_count = 0_usize;
    for batch in batches {
        if batch.num_columns() != column_count {
            return Err(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                format!(
                    "SQL result batch has {} columns but the result schema has {column_count}",
                    batch.num_columns()
                ),
            ));
        }
        row_count = row_count.checked_add(batch.num_rows()).ok_or_else(|| {
            LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                "SQL result row count exceeds addressable memory",
            )
        })?;
        let batch_cells = batch.num_rows().checked_mul(column_count).ok_or_else(|| {
            LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                "SQL result cell count exceeds addressable memory",
            )
        })?;
        cell_count = cell_count.checked_add(batch_cells).ok_or_else(|| {
            LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                "SQL result cell count exceeds addressable memory",
            )
        })?;
    }
    let mut values = Vec::with_capacity(cell_count);
    for batch in batches {
        let cursors = batch
            .columns()
            .iter()
            .enumerate()
            .map(|(column_index, array)| {
                column_cursor(result_fields.get(column_index), array.as_ref())
            })
            .collect::<Result<Vec<_>, _>>()?;
        for row_index in 0..batch.num_rows() {
            for cursor in &cursors {
                values.push(cursor.value(row_index)?);
            }
        }
    }
    if values.len() != cell_count {
        return Err(LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            "SQL result values do not match the checked result shape",
        ));
    }
    Ok((values, row_count))
}

/// Appends one batch to `rows`, filling it column by column.
///
/// Rows are grown to their final width first, then each column is downcast once
/// and written across every row. Reading a column top to bottom keeps both the
/// Arrow side and the type dispatch out of the inner loop: the array kind is
/// matched once per column instead of once per cell.
fn append_batch_rows(
    result_fields: &[Field],
    batch: &RecordBatch,
    rows: &mut Vec<Vec<Value>>,
) -> Result<(), LixError> {
    let row_base = rows.len();
    let column_count = batch.num_columns();
    rows.resize_with(row_base + batch.num_rows(), || {
        Vec::<Value>::with_capacity(column_count)
    });
    let batch_rows = &mut rows[row_base..];
    for (column_index, array) in batch.columns().iter().enumerate() {
        let cursor = column_cursor(result_fields.get(column_index), array.as_ref())?;
        cursor.append_column(batch_rows)?;
    }
    Ok(())
}

#[cfg(any(feature = "storage-benches", test))]
pub(crate) fn row_values_from_batch(
    result_fields: &[Field],
    batch: &RecordBatch,
    row_index: usize,
) -> Result<Vec<Value>, LixError> {
    // Slicing is an offset adjustment, not a copy, so one row reuses exactly
    // the same column fill as a whole batch.
    let row = batch.slice(row_index, 1);
    let mut rows = Vec::<Vec<Value>>::with_capacity(1);
    append_batch_rows(result_fields, &row, &mut rows)?;
    rows.pop().ok_or_else(|| {
        LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            "result row index out of range",
        )
    })
}

/// One result column of one `RecordBatch`, already downcast to its concrete
/// Arrow array.
///
/// Result materialization used to build a `ScalarValue` per cell, which meant
/// one dynamic downcast plus one owned allocation for every cell of every scan.
/// The batch is uniform by construction, so the downcast is hoisted here and
/// the row loop reads straight out of the typed array into `Value`.
pub(super) enum ColumnCursor<'a> {
    Null,
    Boolean(&'a BooleanArray),
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    UInt8(&'a UInt8Array),
    UInt16(&'a UInt16Array),
    UInt32(&'a UInt32Array),
    UInt64(&'a UInt64Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Utf8(&'a StringArray, TextKind),
    LargeUtf8(&'a LargeStringArray, TextKind),
    Utf8View(&'a StringViewArray, TextKind),
    Binary(&'a BinaryArray),
    LargeBinary(&'a LargeBinaryArray),
    TimestampMicrosecond(&'a TimestampMicrosecondArray),
}

/// Whether a string column carries JSON payloads, decided once per batch from
/// the result field metadata rather than re-tested per cell.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum TextKind {
    Text,
    Jsonb,
    RowRef,
}

pub(super) fn column_cursor<'a>(
    field: Option<&Field>,
    array: &'a dyn Array,
) -> Result<ColumnCursor<'a>, LixError> {
    let text_kind = if field.is_some_and(field_is_row_ref) {
        TextKind::RowRef
    } else if field.is_some_and(field_is_json) {
        TextKind::Jsonb
    } else {
        TextKind::Text
    };
    let cursor = match array.data_type() {
        DataType::Null => ColumnCursor::Null,
        DataType::Boolean => ColumnCursor::Boolean(downcast_column(array)?),
        DataType::Int8 => ColumnCursor::Int8(downcast_column(array)?),
        DataType::Int16 => ColumnCursor::Int16(downcast_column(array)?),
        DataType::Int32 => ColumnCursor::Int32(downcast_column(array)?),
        DataType::Int64 => ColumnCursor::Int64(downcast_column(array)?),
        DataType::UInt8 => ColumnCursor::UInt8(downcast_column(array)?),
        DataType::UInt16 => ColumnCursor::UInt16(downcast_column(array)?),
        DataType::UInt32 => ColumnCursor::UInt32(downcast_column(array)?),
        DataType::UInt64 => ColumnCursor::UInt64(downcast_column(array)?),
        DataType::Float32 => ColumnCursor::Float32(downcast_column(array)?),
        DataType::Float64 => ColumnCursor::Float64(downcast_column(array)?),
        DataType::Utf8 => ColumnCursor::Utf8(downcast_column(array)?, text_kind),
        DataType::LargeUtf8 => ColumnCursor::LargeUtf8(downcast_column(array)?, text_kind),
        DataType::Utf8View => ColumnCursor::Utf8View(downcast_column(array)?, text_kind),
        DataType::Binary => ColumnCursor::Binary(downcast_column(array)?),
        DataType::LargeBinary => ColumnCursor::LargeBinary(downcast_column(array)?),
        DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, _) => {
            ColumnCursor::TimestampMicrosecond(downcast_column(array)?)
        }
        other => {
            return Err(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                format!("SQL query produced an unsupported result column type {other}"),
            )
            .with_hint(
                "Cast the column to a supported Lix result type such as TEXT, BIGINT, DOUBLE, BOOLEAN, or BYTEA.",
            ));
        }
    };
    Ok(cursor)
}

fn downcast_column<'a, ArrayType: 'static>(
    array: &'a dyn Array,
) -> Result<&'a ArrayType, LixError> {
    array.as_any().downcast_ref::<ArrayType>().ok_or_else(|| {
        LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            format!(
                "SQL result column declares Arrow type {} but carries a different array layout",
                array.data_type()
            ),
        )
    })
}

impl ColumnCursor<'_> {
    pub(super) fn value(&self, row_index: usize) -> Result<Value, LixError> {
        let value = match self {
            Self::Null => Value::Null,
            Self::Boolean(values) => {
                if values.is_null(row_index) {
                    Value::Null
                } else {
                    Value::Boolean(values.value(row_index))
                }
            }
            Self::Int8(values) => integer_value(*values, row_index),
            Self::Int16(values) => integer_value(*values, row_index),
            Self::Int32(values) => integer_value(*values, row_index),
            Self::Int64(values) => integer_value(*values, row_index),
            Self::UInt8(values) => integer_value(*values, row_index),
            Self::UInt16(values) => integer_value(*values, row_index),
            Self::UInt32(values) => integer_value(*values, row_index),
            Self::UInt64(values) => {
                if values.is_null(row_index) {
                    Value::Null
                } else {
                    let value = values.value(row_index);
                    crate::sql2::value_contract::unsigned_integer_result(value)?
                }
            }
            Self::Float32(values) => real_value(*values, row_index)?,
            Self::Float64(values) => real_value(*values, row_index)?,
            Self::Utf8(values, kind) => {
                if values.is_null(row_index) {
                    Value::Null
                } else {
                    text_value(values.value(row_index), *kind)
                }
            }
            Self::LargeUtf8(values, kind) => {
                if values.is_null(row_index) {
                    Value::Null
                } else {
                    text_value(values.value(row_index), *kind)
                }
            }
            Self::Utf8View(values, kind) => {
                if values.is_null(row_index) {
                    Value::Null
                } else {
                    text_value(values.value(row_index), *kind)
                }
            }
            Self::Binary(values) => {
                if values.is_null(row_index) {
                    Value::Null
                } else {
                    Value::Blob(values.value(row_index).into())
                }
            }
            Self::LargeBinary(values) => {
                if values.is_null(row_index) {
                    Value::Null
                } else {
                    Value::Blob(values.value(row_index).into())
                }
            }
            Self::TimestampMicrosecond(values) => {
                if values.is_null(row_index) {
                    Value::Null
                } else {
                    Value::Timestamptz(values.value(row_index))
                }
            }
        };
        Ok(value)
    }

    /// Pushes this column's value onto every row of the batch.
    ///
    /// `rows` is exactly the batch's row window, so row `n` of the slice is row
    /// `n` of the array.
    fn append_column(&self, rows: &mut [Vec<Value>]) -> Result<(), LixError> {
        match self {
            Self::Null => {
                for row in rows.iter_mut() {
                    row.push(Value::Null);
                }
            }
            Self::Boolean(values) => {
                for (row_index, row) in rows.iter_mut().enumerate() {
                    row.push(if values.is_null(row_index) {
                        Value::Null
                    } else {
                        Value::Boolean(values.value(row_index))
                    });
                }
            }
            Self::Int8(values) => append_integers(values, rows),
            Self::Int16(values) => append_integers(values, rows),
            Self::Int32(values) => append_integers(values, rows),
            Self::Int64(values) => append_integers(values, rows),
            Self::UInt8(values) => append_integers(values, rows),
            Self::UInt16(values) => append_integers(values, rows),
            Self::UInt32(values) => append_integers(values, rows),
            Self::UInt64(values) => {
                for (row_index, row) in rows.iter_mut().enumerate() {
                    row.push(if values.is_null(row_index) {
                        Value::Null
                    } else {
                        let value = values.value(row_index);
                        crate::sql2::value_contract::unsigned_integer_result(value)?
                    });
                }
            }
            Self::Float32(values) => append_reals(values, rows)?,
            Self::Float64(values) => append_reals(values, rows)?,
            Self::Utf8(values, kind) => {
                for (row_index, row) in rows.iter_mut().enumerate() {
                    row.push(if values.is_null(row_index) {
                        Value::Null
                    } else {
                        text_value(values.value(row_index), *kind)
                    });
                }
            }
            Self::LargeUtf8(values, kind) => {
                for (row_index, row) in rows.iter_mut().enumerate() {
                    row.push(if values.is_null(row_index) {
                        Value::Null
                    } else {
                        text_value(values.value(row_index), *kind)
                    });
                }
            }
            Self::Utf8View(values, kind) => {
                for (row_index, row) in rows.iter_mut().enumerate() {
                    row.push(if values.is_null(row_index) {
                        Value::Null
                    } else {
                        text_value(values.value(row_index), *kind)
                    });
                }
            }
            Self::Binary(values) => {
                for (row_index, row) in rows.iter_mut().enumerate() {
                    row.push(if values.is_null(row_index) {
                        Value::Null
                    } else {
                        Value::Blob(values.value(row_index).into())
                    });
                }
            }
            Self::LargeBinary(values) => {
                for (row_index, row) in rows.iter_mut().enumerate() {
                    row.push(if values.is_null(row_index) {
                        Value::Null
                    } else {
                        Value::Blob(values.value(row_index).into())
                    });
                }
            }
            Self::TimestampMicrosecond(values) => {
                for (row_index, row) in rows.iter_mut().enumerate() {
                    row.push(if values.is_null(row_index) {
                        Value::Null
                    } else {
                        Value::Timestamptz(values.value(row_index))
                    });
                }
            }
        }
        Ok(())
    }
}

fn integer_value<NativeType>(values: &PrimitiveArray<NativeType>, row_index: usize) -> Value
where
    NativeType: ArrowPrimitiveType,
    NativeType::Native: Into<i64>,
{
    if values.is_null(row_index) {
        Value::Null
    } else {
        Value::Integer(values.value(row_index).into())
    }
}

fn real_value<NativeType>(
    values: &PrimitiveArray<NativeType>,
    row_index: usize,
) -> Result<Value, LixError>
where
    NativeType: ArrowPrimitiveType,
    NativeType::Native: Into<f64>,
{
    if values.is_null(row_index) {
        Ok(Value::Null)
    } else {
        finite_query_float(values.value(row_index).into())
    }
}

fn append_integers<NativeType>(values: &PrimitiveArray<NativeType>, rows: &mut [Vec<Value>])
where
    NativeType: ArrowPrimitiveType,
    NativeType::Native: Into<i64>,
{
    for (row_index, row) in rows.iter_mut().enumerate() {
        row.push(if values.is_null(row_index) {
            Value::Null
        } else {
            Value::Integer(values.value(row_index).into())
        });
    }
}

fn append_reals<NativeType>(
    values: &PrimitiveArray<NativeType>,
    rows: &mut [Vec<Value>],
) -> Result<(), LixError>
where
    NativeType: ArrowPrimitiveType,
    NativeType::Native: Into<f64>,
{
    for (row_index, row) in rows.iter_mut().enumerate() {
        row.push(if values.is_null(row_index) {
            Value::Null
        } else {
            finite_query_float(values.value(row_index).into())?
        });
    }
    Ok(())
}

fn text_value(value: &str, kind: TextKind) -> Value {
    match kind {
        // The write boundary canonicalizes every JSON payload before it reaches
        // storage, and the projection decoder copies those bytes into Arrow
        // verbatim. Re-parsing here only rebuilt a DOM that was immediately
        // re-serialized, so the bytes are retained directly instead.
        TextKind::Jsonb => Value::Jsonb(crate::Json::from_canonical_text(value)),
        TextKind::RowRef => Value::RowRef(crate::RowRef(value.to_owned())),
        TextKind::Text => Value::Text(value.to_owned()),
    }
}

fn finite_query_float(value: f64) -> Result<Value, LixError> {
    if !value.is_finite() {
        return Err(LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            "SQL query produced a non-finite number",
        ));
    }
    Ok(Value::Real(value))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::collections::VecDeque;
    use std::pin::Pin;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use std::task::{Context, Poll};

    use async_trait::async_trait;
    use futures_util::{FutureExt, Stream};
    use serde_json::Value as JsonValue;
    use serde_json::json;

    use super::{
        SqlExecutionContext, SqlWriteExecutionContext, build_write_session_with_options,
        deferred_returning_image_columns, execute_sql, query_result_from_batches,
        query_values_from_batches, retain_columnar_result, row_values_from_batch,
        write_provider_selection, write_session_options, write_target_table_name,
    };
    use crate::binary_cas::BlobDataReader;
    use crate::branch::BranchRefReader;
    use crate::changelog::{ChangeId, CommitId};
    use crate::commit_graph::{CommitGraphNode, CommitGraphReader, ReachableCommitGraphNode};
    use crate::common::LixTimestamp;
    use crate::functions::FunctionProviderHandle;
    use crate::hot_state::{HotStateReader, HotStateScanRequest, MaterializedHotStateRow};
    use crate::sql2::{ChangelogQuerySource, RowSnapshotReader, SqlChangelogQuerySource};
    use crate::sql2::{
        PublicCatalog, WriteExecutorMode, WriteExecutorPath, create_write_logical_plan,
        execute_write_logical_plan, execute_write_logical_plan_with_mode_and_trace,
    };
    use crate::storage_adapter::{
        Memory, MemoryRead, SharedStorageAdapterRead, StorageAdapter, StorageAdapterReadScope,
        StorageReadOptions,
    };
    use crate::transaction_types::{
        TransactionWrite, TransactionWriteOutcome, TransactionWriteRow,
    };
    use crate::{CreateBranchOptions, ExecuteResult, MergeBranchOptions, engine::Engine};
    use crate::{LixError, NullableKeyFilter, Value};
    use datafusion::arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, NullArray, StringArray,
        StringViewArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::error::DataFusionError;
    use datafusion::physical_plan::RecordBatchStream;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn returning_dependencies_ignore_nested_local_columns() {
        use datafusion::sql::sqlparser::{
            ast::{SelectItem, SetExpr, Statement},
            dialect::PostgreSqlDialect,
            parser::Parser,
        };

        let statements = Parser::parse_sql(
            &PostgreSqlDialect {},
            "SELECT (SELECT content FROM (SELECT 'local' AS content) nested), id",
        )
        .unwrap();
        let Statement::Query(query) = &statements[0] else {
            panic!("expected SELECT")
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected SELECT body")
        };
        let items = select
            .projection
            .iter()
            .enumerate()
            .map(|(index, item)| {
                let expr = match item {
                    SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                        expr.clone()
                    }
                    _ => panic!("expected expression"),
                };
                crate::sql2::bind::write::BoundReturningItem {
                    expr: None,
                    sql_expr: Some(expr),
                    output_name: format!("column_{index}"),
                    output_alias: None,
                }
            })
            .collect();
        let returning = crate::sql2::bind::write::BoundReturning { items };
        let table_schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("content", DataType::Binary, true),
        ]);
        let session = SessionContext::new();
        let dependencies = deferred_returning_image_columns(
            &session,
            &returning,
            &table_schema,
            "lix_file",
            false,
        )
        .await
        .unwrap();

        assert_eq!(dependencies.new, BTreeSet::from(["id".to_string()]));
        assert!(dependencies.old.is_empty());
    }

    #[test]
    fn direct_typed_public_rows_match_generic_scalar_conversion_for_every_supported_type() {
        const ROWS: usize = 242;
        let present = |index: usize| index % 7 != 0;
        let fields = vec![
            Field::new("null", DataType::Null, true),
            Field::new("bool", DataType::Boolean, true),
            Field::new("i8", DataType::Int8, true),
            Field::new("i16", DataType::Int16, true),
            Field::new("i32", DataType::Int32, true),
            Field::new("i64", DataType::Int64, true),
            Field::new("u8", DataType::UInt8, true),
            Field::new("u16", DataType::UInt16, true),
            Field::new("u32", DataType::UInt32, true),
            Field::new("u64", DataType::UInt64, true),
            Field::new("f32", DataType::Float32, true),
            Field::new("f64", DataType::Float64, true),
            Field::new("utf8", DataType::Utf8, true),
            Field::new("utf8_view", DataType::Utf8View, true),
            Field::new("large_utf8", DataType::LargeUtf8, true),
            Field::new("binary", DataType::Binary, true),
            Field::new("large_binary", DataType::LargeBinary, true),
        ];
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(NullArray::new(ROWS)),
            Arc::new(BooleanArray::from_iter(
                (0..ROWS).map(|index| present(index).then_some(index % 2 == 0)),
            )),
            Arc::new(Int8Array::from_iter(
                (0..ROWS).map(|index| present(index).then_some((index % 101) as i8 - 50)),
            )),
            Arc::new(Int16Array::from_iter(
                (0..ROWS).map(|index| present(index).then_some(index as i16 - 120)),
            )),
            Arc::new(Int32Array::from_iter(
                (0..ROWS).map(|index| present(index).then_some(index as i32 - 130)),
            )),
            Arc::new(Int64Array::from_iter(
                (0..ROWS).map(|index| present(index).then_some(index as i64 - 140)),
            )),
            Arc::new(UInt8Array::from_iter(
                (0..ROWS).map(|index| present(index).then_some(index as u8)),
            )),
            Arc::new(UInt16Array::from_iter(
                (0..ROWS).map(|index| present(index).then_some(index as u16 * 3)),
            )),
            Arc::new(UInt32Array::from_iter(
                (0..ROWS).map(|index| present(index).then_some(index as u32 * 5)),
            )),
            Arc::new(UInt64Array::from_iter((0..ROWS).map(|index| {
                present(index).then_some(if index == 1 {
                    i64::MAX as u64
                } else {
                    index as u64 * 7
                })
            }))),
            Arc::new(Float32Array::from_iter((0..ROWS).map(|index| {
                present(index).then_some(index as f32 * 0.25 - 10.0)
            }))),
            Arc::new(Float64Array::from_iter((0..ROWS).map(|index| {
                present(index).then_some(index as f64 * 0.5 - 20.0)
            }))),
            Arc::new(StringArray::from_iter(
                (0..ROWS).map(|index| present(index).then_some("utf8")),
            )),
            Arc::new(StringViewArray::from_iter(
                (0..ROWS).map(|index| present(index).then_some("utf8-view")),
            )),
            Arc::new(LargeStringArray::from_iter(
                (0..ROWS).map(|index| present(index).then_some("large-utf8")),
            )),
            Arc::new(BinaryArray::from_iter(
                (0..ROWS).map(|index| present(index).then_some(b"binary".as_slice())),
            )),
            Arc::new(LargeBinaryArray::from_iter((0..ROWS).map(|index| {
                present(index).then_some(b"large-binary".as_slice())
            }))),
        ];
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields.clone())), arrays)
            .expect("all ordinary result arrays share one schema");

        let generic_rows = (0..ROWS)
            .map(|row_index| row_values_from_batch(&fields, &batch, row_index))
            .collect::<Result<Vec<_>, _>>()
            .expect("generic scalar conversion");
        let (flat_values, flat_row_count) =
            query_values_from_batches(&fields, std::slice::from_ref(&batch))
                .expect("flat typed conversion");
        let flat_rows = flat_values
            .chunks_exact(fields.len())
            .map(<[Value]>::to_vec)
            .collect::<Vec<_>>();
        let direct = query_result_from_batches(&fields, &[batch]).expect("direct typed conversion");

        assert_eq!(
            direct.columns,
            fields.iter().map(Field::name).cloned().collect::<Vec<_>>()
        );
        assert_eq!(
            direct.column_types,
            vec![
                crate::ResultColumnType::Null,
                crate::ResultColumnType::Boolean,
                crate::ResultColumnType::Integer,
                crate::ResultColumnType::Integer,
                crate::ResultColumnType::Integer,
                crate::ResultColumnType::Integer,
                crate::ResultColumnType::Integer,
                crate::ResultColumnType::Integer,
                crate::ResultColumnType::Integer,
                crate::ResultColumnType::Integer,
                crate::ResultColumnType::Real,
                crate::ResultColumnType::Real,
                crate::ResultColumnType::Text,
                crate::ResultColumnType::Text,
                crate::ResultColumnType::Text,
                crate::ResultColumnType::Blob,
                crate::ResultColumnType::Blob,
            ],
        );
        assert_eq!(direct.rows, generic_rows);
        assert_eq!(flat_row_count, ROWS);
        assert_eq!(flat_rows, generic_rows);
    }

    #[test]
    fn flat_query_values_reject_batch_schema_width_mismatch() {
        let result_fields = vec![
            Field::new("first", DataType::Int64, false),
            Field::new("second", DataType::Int64, false),
        ];
        let batch_fields = vec![Field::new("first", DataType::Int64, false)];
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(batch_fields)),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .expect("test batch should match its own schema");

        let error = query_values_from_batches(&result_fields, &[batch])
            .expect_err("flat results require the batch and result schema widths to match");
        assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);
        assert!(error.message.contains("1 columns"));
        assert!(error.message.contains("2"));
    }

    struct DummyBlobReader;
    struct StaticBlobReader {
        bytes: Vec<u8>,
    }
    struct DummyHotStateReader;
    struct RowsHotStateReader {
        rows: Vec<MaterializedHotStateRow>,
    }
    struct CapturingRowsHotStateReader {
        rows: Vec<MaterializedHotStateRow>,
        requests: Arc<Mutex<Vec<HotStateScanRequest>>>,
    }
    struct CountingRowsHotStateReader {
        rows: Vec<MaterializedHotStateRow>,
        scans: Arc<AtomicUsize>,
    }
    struct CountingBatchStream {
        schema: SchemaRef,
        batches: VecDeque<RecordBatch>,
        polls: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
    }

    impl Stream for CountingBatchStream {
        type Item = Result<RecordBatch, DataFusionError>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(self.batches.pop_front().map(Ok))
        }
    }

    impl RecordBatchStream for CountingBatchStream {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    impl Drop for CountingBatchStream {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn dropping_live_batch_cursor_stops_before_polling_later_batches() {
        let field = Field::new("ordinal", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![field.clone()]));
        let batches = [1i64, 2i64]
            .into_iter()
            .map(|value| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from(vec![value]))],
                )
                .expect("test batch should match schema")
            })
            .collect::<VecDeque<_>>();
        let polls = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let stream = CountingBatchStream {
            schema,
            batches,
            polls: Arc::clone(&polls),
            dropped: Arc::clone(&dropped),
        };
        let mut result = super::SessionReadBatchStreamResult {
            fields: vec![field],
            stream: Box::pin(stream),
            notices: Vec::new(),
            _session: std::marker::PhantomData,
        };
        assert!(result.notices.is_empty());

        let mut cursor = super::BatchRowCursor::live(&mut result);
        assert_eq!(
            cursor.next_values().await.unwrap(),
            Some(vec![Value::Integer(1)])
        );
        assert_eq!(polls.load(Ordering::SeqCst), 1);

        drop(cursor);
        drop(result);
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn collected_batch_cursor_converts_rows_without_retaining_public_rows() {
        let field = Field::new("ordinal", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![field.clone()]));
        let batches = vec![
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![1i64, 2i64]))],
            )
            .expect("test batch should match schema"),
        ];
        let fields = vec![field];
        let mut cursor = super::BatchRowCursor::collected(&fields, &batches);

        assert_eq!(
            cursor.next_values().await.unwrap(),
            Some(vec![Value::Integer(1)])
        );
        assert_eq!(
            cursor.next_values().await.unwrap(),
            Some(vec![Value::Integer(2)])
        );
        assert_eq!(cursor.next_values().await.unwrap(), None);
    }
    struct RecordingRowSnapshotReader {
        snapshots: Vec<(crate::row_pk::RowPk, bytes::Bytes)>,
        requests: Arc<Mutex<Vec<HotStateScanRequest>>>,
    }
    struct DummyCommitGraphReader;
    struct DummyBranchRefReader;
    fn test_read_scope(storage: &StorageAdapter<Memory>) -> StorageAdapterReadScope<MemoryRead> {
        storage
            .begin_read(StorageReadOptions::default())
            .now_or_never()
            .expect("in-memory read should complete without yielding")
            .expect("read should open")
    }

    fn test_functions() -> FunctionProviderHandle {
        FunctionProviderHandle::system()
    }

    #[test]
    fn unsigned_overflow_never_changes_integer_results_into_text() {
        let fields = vec![Field::new("n", DataType::UInt64, true)];
        for rows in [1, 4096] {
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(fields.clone())),
                vec![Arc::new(UInt64Array::from(vec![u64::MAX; rows]))],
            )
            .unwrap();
            assert!(!retain_columnar_result(
                &fields,
                std::slice::from_ref(&batch)
            ));
            assert_eq!(
                query_result_from_batches(&fields, std::slice::from_ref(&batch))
                    .unwrap_err()
                    .code,
                LixError::CODE_TYPE_MISMATCH
            );
            assert_eq!(
                row_values_from_batch(&fields, &batch, 0).unwrap_err().code,
                LixError::CODE_TYPE_MISMATCH
            );
            assert!(query_values_from_batches(&fields, &[batch]).is_err());
        }
    }

    #[test]
    fn typed_row_conversion_covers_every_supported_result_column_type() {
        use datafusion::arrow::array::{
            BooleanArray, Float64Array, LargeBinaryArray, NullArray, StringArray, UInt64Array,
        };

        let fields = vec![
            Field::new("nothing", DataType::Null, true),
            Field::new("flag", DataType::Boolean, true),
            Field::new("ordinal", DataType::Int64, true),
            Field::new("big", DataType::UInt64, true),
            Field::new("ratio", DataType::Float64, true),
            Field::new("label", DataType::Utf8, true),
            crate::sql2::result_metadata::json_field("document", true),
            Field::new("payload", DataType::LargeBinary, true),
        ];
        let schema = Arc::new(Schema::new(fields.clone()));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(NullArray::new(2)),
                Arc::new(BooleanArray::from(vec![Some(true), None])),
                Arc::new(Int64Array::from(vec![Some(7i64), None])),
                Arc::new(UInt64Array::from(vec![Some(i64::MAX as u64), None])),
                Arc::new(Float64Array::from(vec![Some(1.5f64), None])),
                Arc::new(StringArray::from(vec![Some("hello"), None])),
                Arc::new(StringArray::from(vec![Some(r#"{"a":1}"#), None])),
                Arc::new(LargeBinaryArray::from(vec![
                    Some([0x41u8, 0x42, 0x43].as_slice()),
                    None,
                ])),
            ],
        )
        .expect("test batch should match schema");

        assert_eq!(
            row_values_from_batch(&fields, &batch, 0).expect("typed row conversion"),
            vec![
                Value::Null,
                Value::Boolean(true),
                Value::Integer(7),
                Value::Integer(i64::MAX),
                Value::Real(1.5),
                Value::Text("hello".to_owned()),
                Value::Jsonb(crate::Json::from_canonical_text(r#"{"a":1}"#)),
                Value::Blob(vec![0x41, 0x42, 0x43].into()),
            ]
        );
        assert_eq!(
            row_values_from_batch(&fields, &batch, 1).expect("typed row conversion"),
            vec![Value::Null; 8]
        );
    }

    #[test]
    fn unsupported_result_column_types_are_rejected_once_per_batch() {
        use datafusion::arrow::array::Date32Array;

        let fields = vec![Field::new("day", DataType::Date32, true)];
        let schema = Arc::new(Schema::new(fields.clone()));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Date32Array::from(vec![0i32]))])
            .expect("test batch should match schema");

        let error = row_values_from_batch(&fields, &batch, 0)
            .expect_err("unsupported column types must be rejected");
        assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);
    }

    #[derive(Default)]
    struct CapturingStagedWrites {
        deltas: Vec<CapturedStageWrite>,
    }

    #[derive(Clone)]
    struct CapturedStageWrite {
        rows: Vec<TransactionWriteRow>,
    }

    impl CapturedStageWrite {
        fn pending_write_overlay(&self) -> Result<CapturedStageOverlay, LixError> {
            Ok(CapturedStageOverlay {
                rows: self.rows.clone(),
            })
        }
    }

    struct CapturedStageOverlay {
        rows: Vec<TransactionWriteRow>,
    }

    impl CapturedStageOverlay {
        fn visible_semantic_rows(
            &self,
            include_tombstones: bool,
            schema_key: &str,
        ) -> Vec<CapturedStageRow> {
            self.visible_all_semantic_rows()
                .into_iter()
                .filter(|row| row.schema_key == schema_key)
                .filter(|row| include_tombstones || !row.tombstone)
                .collect()
        }

        fn visible_all_semantic_rows(&self) -> Vec<CapturedStageRow> {
            self.rows
                .iter()
                .cloned()
                .map(CapturedStageRow::from)
                .collect()
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct CapturedStageRow {
        row_pk: String,
        schema_key: String,
        branch_id: String,
        file_id: Option<String>,
        snapshot_content: Option<String>,
        metadata: Option<String>,
        global: bool,
        untracked: bool,
        tombstone: bool,
    }

    impl From<TransactionWriteRow> for CapturedStageRow {
        fn from(row: TransactionWriteRow) -> Self {
            Self {
                row_pk: row
                    .row_pk
                    .expect("captured staged row should carry row_pk")
                    .as_json_array_text()
                    .expect("captured staged row should project row_pk"),
                schema_key: row.schema_key.into(),
                branch_id: row.branch_id.into(),
                file_id: row.file_id.map(Into::into),
                global: row.global,
                untracked: row.untracked,
                tombstone: row.snapshot.is_none(),
                snapshot_content: row.snapshot.map(|snapshot| snapshot.to_string()),
                metadata: row.metadata.map(|metadata| metadata.to_string()),
            }
        }
    }

    struct DummySqlExecutionContext<'a> {
        active_branch_id: &'a str,
        blob_reader: Arc<dyn BlobDataReader>,
        hot_state: Arc<dyn HotStateReader>,
        row_snapshot_reader: Option<Arc<dyn RowSnapshotReader>>,
        schema_definitions: Vec<JsonValue>,
    }

    #[async_trait]
    impl<'a> SqlExecutionContext for DummySqlExecutionContext<'a> {
        type ReadStore = SharedStorageAdapterRead<MemoryRead>;

        fn active_branch_id(&self) -> &str {
            self.active_branch_id
        }

        fn hot_state(&self) -> Arc<dyn HotStateReader> {
            Arc::clone(&self.hot_state)
        }

        fn row_snapshot_reader(&self) -> Option<Arc<dyn RowSnapshotReader>> {
            self.row_snapshot_reader.clone()
        }

        fn filesystem_path_index(&self) -> Arc<dyn crate::filesystem::FilesystemPathIndexReader> {
            Arc::new(crate::filesystem::UncachedFilesystemPathIndexReader::new(
                Arc::clone(&self.hot_state),
            ))
        }

        fn functions(&self) -> FunctionProviderHandle {
            test_functions()
        }

        fn blob_reader(&self) -> Arc<dyn BlobDataReader> {
            Arc::clone(&self.blob_reader)
        }

        fn changelog_query_source(&self) -> SqlChangelogQuerySource<Self::ReadStore> {
            let storage = StorageAdapter::new(Memory::new());
            let read_scope = SharedStorageAdapterRead::new(test_read_scope(&storage));
            ChangelogQuerySource { store: read_scope }
        }

        fn commit_graph(&self) -> Box<dyn CommitGraphReader> {
            Box::new(DummyCommitGraphReader)
        }

        fn branch_ref(&self) -> Arc<dyn BranchRefReader> {
            Arc::new(DummyBranchRefReader)
        }

        async fn load_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
            Ok(self.schema_definitions.clone())
        }
    }

    struct DummySqlWriteExecutionContext<'a> {
        active_branch_id: &'a str,
        blob_reader: Arc<dyn BlobDataReader>,
        hot_state: Arc<dyn HotStateReader>,
        staged_writes: Arc<Mutex<CapturingStagedWrites>>,
        schema_definitions: Vec<JsonValue>,
    }

    struct CountingWriteSessionContext<'a> {
        inner: DummySqlWriteExecutionContext<'a>,
        branch_head_loads: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl SqlWriteExecutionContext for DummySqlWriteExecutionContext<'_> {
        fn active_branch_id(&self) -> &str {
            self.active_branch_id
        }

        fn functions(&self) -> FunctionProviderHandle {
            test_functions()
        }

        fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
            Ok(self.schema_definitions.clone())
        }

        fn public_catalog(&self) -> Result<Arc<PublicCatalog>, LixError> {
            Ok(Arc::new(PublicCatalog::from_visible_schemas(
                &self.schema_definitions,
            )?))
        }

        async fn load_bytes_many(
            &mut self,
            hashes: &[crate::binary_cas::BlobId],
        ) -> Result<crate::binary_cas::BlobBytesBatch, LixError> {
            self.blob_reader.load_bytes_many(hashes).await
        }

        async fn scan_hot_state_batch(
            &mut self,
            request: &HotStateScanRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateBatch, LixError> {
            self.hot_state.scan_batch(request).await
        }

        async fn load_exact_hot_state_batch(
            &mut self,
            request: &crate::hot_state::HotStateExactBatchRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateExactBatch, LixError> {
            self.hot_state.load_exact_batch(request).await
        }

        async fn load_branch_head(
            &mut self,
            branch_id: &str,
        ) -> Result<Option<CommitId>, LixError> {
            if branch_id == "missing-branch" {
                return Ok(None);
            }
            Ok(Some(CommitId::for_test_label(&format!(
                "commit-{branch_id}"
            ))))
        }

        async fn stage_write(
            &mut self,
            write: TransactionWrite,
        ) -> Result<TransactionWriteOutcome, LixError> {
            let count = match &write {
                TransactionWrite::Rows { rows, .. } => rows.len() as u64,
                TransactionWrite::RowsWithFileContent { count, .. } => *count,
            };
            let rows = match write {
                TransactionWrite::Rows { rows, .. } => rows.into_rows(),
                TransactionWrite::RowsWithFileContent { rows, .. } => rows.into_rows(),
            };
            self.staged_writes
                .lock()
                .expect("staged writes lock")
                .deltas
                .push(CapturedStageWrite { rows });
            Ok(TransactionWriteOutcome { count })
        }

        async fn stage_typed_mutation_journal_replace(
            &mut self,
            _rows: crate::transaction_types::TypedMutationJournalBatch,
        ) -> Result<TransactionWriteOutcome, LixError> {
            Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "DataFusion test context does not stage transaction journals",
            ))
        }

        async fn can_stage_typed_mutation_journal_replace(
            &mut self,
            _schema_key: &str,
            _live_count: u64,
            _ordered_identity_digest: [u8; 32],
        ) -> Result<bool, LixError> {
            Ok(false)
        }
    }

    #[async_trait]
    impl SqlWriteExecutionContext for CountingWriteSessionContext<'_> {
        fn active_branch_id(&self) -> &str {
            self.inner.active_branch_id()
        }

        fn functions(&self) -> FunctionProviderHandle {
            self.inner.functions()
        }

        fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
            self.inner.list_visible_schemas()
        }

        fn public_catalog(&self) -> Result<Arc<PublicCatalog>, LixError> {
            self.inner.public_catalog()
        }

        async fn load_bytes_many(
            &mut self,
            hashes: &[crate::binary_cas::BlobId],
        ) -> Result<crate::binary_cas::BlobBytesBatch, LixError> {
            self.inner.load_bytes_many(hashes).await
        }

        async fn scan_hot_state_batch(
            &mut self,
            request: &HotStateScanRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateBatch, LixError> {
            self.inner.scan_hot_state_batch(request).await
        }

        async fn load_exact_hot_state_batch(
            &mut self,
            request: &crate::hot_state::HotStateExactBatchRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateExactBatch, LixError> {
            self.inner.load_exact_hot_state_batch(request).await
        }

        async fn load_branch_head(
            &mut self,
            branch_id: &str,
        ) -> Result<Option<CommitId>, LixError> {
            self.branch_head_loads.fetch_add(1, Ordering::SeqCst);
            self.inner.load_branch_head(branch_id).await
        }

        async fn stage_write(
            &mut self,
            write: TransactionWrite,
        ) -> Result<TransactionWriteOutcome, LixError> {
            self.inner.stage_write(write).await
        }

        async fn stage_typed_mutation_journal_replace(
            &mut self,
            rows: crate::transaction_types::TypedMutationJournalBatch,
        ) -> Result<TransactionWriteOutcome, LixError> {
            self.inner.stage_typed_mutation_journal_replace(rows).await
        }

        async fn can_stage_typed_mutation_journal_replace(
            &mut self,
            schema_key: &str,
            live_count: u64,
            ordered_identity_digest: [u8; 32],
        ) -> Result<bool, LixError> {
            self.inner
                .can_stage_typed_mutation_journal_replace(
                    schema_key,
                    live_count,
                    ordered_identity_digest,
                )
                .await
        }
    }

    async fn execute_write_sql(
        ctx: &mut dyn SqlWriteExecutionContext,
        sql: &str,
        params: &[Value],
    ) -> Result<crate::SqlQueryResult, LixError> {
        let plan = create_write_logical_plan(ctx, sql).await?;
        let count = execute_write_logical_plan(ctx, plan, params).await?;
        Ok(crate::SqlQueryResult {
            columns: vec!["count".to_string()],
            column_types: vec![crate::ResultColumnType::Integer],
            rows: vec![vec![Value::Integer(count as i64)]],
            notices: Vec::new(),
        })
    }

    async fn execute_write_sql_trace(
        ctx: &mut dyn SqlWriteExecutionContext,
        sql: &str,
        params: &[Value],
        mode: WriteExecutorMode,
    ) -> Result<(crate::SqlQueryResult, WriteExecutorPath), LixError> {
        let plan = create_write_logical_plan(ctx, sql).await?;
        let (count, path) =
            execute_write_logical_plan_with_mode_and_trace(ctx, plan, params, mode).await?;
        Ok((
            crate::SqlQueryResult {
                columns: vec!["count".to_string()],
                column_types: vec![crate::ResultColumnType::Integer],
                rows: vec![vec![Value::Integer(count as i64)]],
                notices: Vec::new(),
            },
            path,
        ))
    }

    #[tokio::test]
    async fn target_only_write_shapes_construct_only_the_target_provider() {
        for sql in [
            "UPDATE lix_file SET content = CAST('A' AS BYTEA) WHERE id = '01920000-0000-7000-8000-0000000000d2'",
            "DELETE FROM lix_file WHERE id = '01920000-0000-7000-8000-0000000000d2' RETURNING id, path",
            "INSERT INTO lix_file (path, content) VALUES ('/readme.md', CAST('A' AS BYTEA)) \
             ON CONFLICT (path) DO UPDATE SET content = excluded.content",
        ] {
            let (mut ctx, _, _) = counting_write_context(Vec::new());
            let plan = create_write_logical_plan(&mut ctx, sql)
                .await
                .unwrap_or_else(|error| panic!("target-only write should plan: {sql}: {error}"));
            let crate::sql2::exec::SqlLogicalPlan::Write(plan) = plan else {
                panic!("target-only SQL should produce a write plan: {sql}");
            };
            let table_name = write_target_table_name(&plan.plan).expect("target should resolve");
            let planning_session = ctx.datafusion_session();
            let selection = write_provider_selection(
                &planning_session.state(),
                &plan.plan,
                &table_name,
            );

            assert_eq!(
                selection,
                crate::sql2::providers::ProviderSelection::Only {
                    names: BTreeSet::from(["lix_file".to_string()]),
                    history_relations: BTreeSet::new(),
                },
                "{sql}"
            );

            let session = build_write_session_with_options(
                &mut ctx,
                write_session_options(&plan.plan),
                &plan.plan,
            )
            .await
            .unwrap_or_else(|error| {
                panic!("target-only write session should build: {sql}: {error}")
            });
            let public = session
                .catalog("datafusion")
                .expect("default catalog should exist")
                .schema("public")
                .expect("public schema should exist");
            let mut table_names = public.table_names();
            table_names.sort();

            assert_eq!(table_names, vec!["lix_file"], "{sql}");
        }
    }

    #[tokio::test]
    async fn query_backed_insert_registers_only_resolved_source_and_target() {
        let (mut ctx, _, _) = counting_write_context(Vec::new());
        let insert_select = create_write_logical_plan(
            &mut ctx,
            "INSERT INTO lix_file (id, path) SELECT 'copied', '/copied.md'",
        )
        .await
        .expect("query-backed insert should plan");
        let crate::sql2::exec::SqlLogicalPlan::Write(insert_select) = insert_select else {
            panic!("query-backed insert should produce a write plan");
        };
        let table_name =
            write_target_table_name(&insert_select.plan).expect("target should resolve");
        let planning_session = ctx.datafusion_session();
        let selection = write_provider_selection(
            &planning_session.state(),
            &insert_select.plan,
            &table_name,
        );

        assert_eq!(
            selection,
            crate::sql2::providers::ProviderSelection::Only {
                names: BTreeSet::from(["lix_file".to_string()]),
                history_relations: BTreeSet::new(),
            },
        );

        let session = build_write_session_with_options(
            &mut ctx,
            write_session_options(&insert_select.plan),
            &insert_select.plan,
        )
        .await
        .expect("query-backed insert session should build");
        let public = session
            .catalog("datafusion")
            .expect("default catalog should exist")
            .schema("public")
            .expect("public schema should exist");
        let mut table_names = public.table_names();
        table_names.sort();

        assert_eq!(table_names, vec!["lix_file"]);
    }

    #[tokio::test]
    async fn insert_source_provider_selection_uses_datafusion_reference_resolution() {
        for (sql, expected) in [
            (
                "INSERT INTO lix_file(id, path) \
                 WITH source AS (SELECT id, path FROM lix_file) \
                 SELECT source.id, source.path FROM source AS source",
                crate::sql2::providers::ProviderSelection::Only {
                    names: BTreeSet::from(["lix_file".to_string()]),
                    history_relations: BTreeSet::new(),
                },
            ),
            (
                "INSERT INTO lix_file(id, path) \
                 WITH source AS (SELECT id, to_path FROM lix_diff('lix_file')) \
                 SELECT source.id, source.to_path FROM source AS source",
                crate::sql2::providers::ProviderSelection::Only {
                    names: BTreeSet::from(["lix_diff".to_string(), "lix_file".to_string()]),
                    history_relations: BTreeSet::new(),
                },
            ),
            (
                "INSERT INTO lix_file(id, path) \
                 SELECT id, to_path FROM lix_diff('lix_file')",
                crate::sql2::providers::ProviderSelection::Only {
                    names: BTreeSet::from([
                        "lix_diff".to_string(),
                        "lix_file".to_string(),
                    ]),
                    history_relations: BTreeSet::new(),
                },
            ),
            (
                "INSERT INTO lix_file(id, path) \
                 SELECT table_name, table_type FROM information_schema.tables",
                crate::sql2::providers::ProviderSelection::All,
            ),
        ] {
            let (mut ctx, _, _) = counting_write_context(Vec::new());
            let logical = create_write_logical_plan(&mut ctx, sql)
                .await
                .unwrap_or_else(|error| panic!("insert query should bind: {sql}: {error}"));
            let crate::sql2::exec::SqlLogicalPlan::Write(plan) = logical else {
                panic!("INSERT should produce a write plan: {sql}");
            };
            let table_name = write_target_table_name(&plan.plan).expect("target should resolve");
            let planning_session = ctx.datafusion_session();
            let actual = write_provider_selection(
                &planning_session.state(),
                &plan.plan,
                &table_name,
            );
            assert_eq!(actual, expected, "{sql}");
        }
    }

    #[tokio::test]
    async fn target_only_delete_returning_executes_with_selected_provider() {
        let (mut ctx, staged_writes, _) = counting_write_context(vec![live_file_row(
            "01920000-0000-7000-8000-0000000000d2",
            "01920000-0000-7000-8000-0000000000a1",
            None,
            "readme.md",
        )]);
        let plan = create_write_logical_plan(
            &mut ctx,
            "DELETE FROM lix_file WHERE id = '01920000-0000-7000-8000-0000000000d2' RETURNING id, path",
        )
        .await
        .expect("DELETE RETURNING should plan");
        let (result, path) = crate::sql2::execute_write_logical_plan_with_mode_and_trace_result(
            &mut ctx,
            plan,
            &[],
            WriteExecutorMode::ForceDataFusion,
        )
        .await
        .expect("target-only DELETE RETURNING should execute");

        assert_eq!(path, WriteExecutorPath::DataFusion);
        assert_eq!(result.rows_affected, 1);
        let returning = result.returning.expect("RETURNING rows should be present");
        assert_eq!(returning.columns, vec!["id", "path"]);
        assert_eq!(
            returning.rows,
            vec![vec![
                Value::Text("01920000-0000-7000-8000-0000000000d2".to_string()),
                Value::Text("/readme.md".to_string()),
            ]]
        );
        assert_eq!(
            staged_writes
                .lock()
                .expect("staged writes lock")
                .deltas
                .len(),
            1
        );
    }

    #[async_trait]
    impl BranchRefReader for DummyBranchRefReader {
        async fn load_head(
            &self,
            branch_id: &str,
        ) -> Result<Option<crate::branch::BranchHead>, LixError> {
            if branch_id == "missing-branch" {
                return Ok(None);
            }
            Ok(Some(crate::branch::BranchHead {
                working_base_commit_id: None,
                branch_id: branch_id.to_string(),
                commit_id: CommitId::for_test_label(&format!("commit-{branch_id}")),
            }))
        }

        async fn scan_heads(&self) -> Result<Vec<crate::branch::BranchHead>, LixError> {
            Ok([
                "01920000-0000-7000-8000-0000000000a1",
                "01920000-0000-7000-8000-0000000000b1",
            ]
            .into_iter()
            .map(|branch_id| crate::branch::BranchHead {
                working_base_commit_id: None,
                branch_id: branch_id.to_string(),
                commit_id: CommitId::for_test_label(&format!("commit-{branch_id}")),
            })
            .collect())
        }
    }

    #[async_trait]
    impl CommitGraphReader for DummyCommitGraphReader {
        async fn load_node(
            &mut self,
            _commit_id: &CommitId,
        ) -> Result<Option<CommitGraphNode>, LixError> {
            Ok(None)
        }

        async fn reachable_nodes(
            &mut self,
            _head_commit_id: &CommitId,
        ) -> Result<Arc<[ReachableCommitGraphNode]>, LixError> {
            Ok(Vec::new().into())
        }
    }

    #[async_trait]
    impl HotStateReader for DummyHotStateReader {
        async fn load_exact_batch(
            &self,
            request: &crate::hot_state::HotStateExactBatchRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateExactBatch, LixError> {
            crate::hot_state::load_exact_batch_via_scan_for_test(self, request).await
        }

        async fn scan_batch(
            &self,
            _request: &HotStateScanRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateBatch, LixError> {
            Ok(vec![].into())
        }
    }

    fn filter_hot_state_rows(
        rows: &[MaterializedHotStateRow],
        request: &HotStateScanRequest,
    ) -> Vec<MaterializedHotStateRow> {
        if matches!(
            request.filter.rows,
            crate::hot_state::HotStateRowFilter::None
        ) {
            return Vec::new();
        }
        let mut rows = rows
            .iter()
            .filter(|row| {
                (request.filter.schema_keys.is_empty()
                    || request.filter.schema_keys.contains(&row.schema_key))
                    && request.filter.matches_row_pk(&row.row_pk)
                    && (request.filter.branch_ids.is_empty()
                        || request
                            .filter
                            .branch_ids
                            .iter()
                            .any(|branch_id| branch_id == row.branch_id.as_ref()))
                    && request
                        .filter
                        .untracked
                        .is_none_or(|untracked| row.untracked == untracked)
                    && (request.filter.include_tombstones || !row.deleted)
                    && (request.filter.file_ids.is_empty()
                        || request.filter.file_ids.iter().any(|filter| match filter {
                            NullableKeyFilter::Any => true,
                            NullableKeyFilter::Null => row.file_id.is_none(),
                            NullableKeyFilter::Value(file_id) => {
                                row.file_id.as_ref() == Some(file_id)
                            }
                        }))
            })
            .cloned()
            .collect::<Vec<_>>();
        if let Some(limit) = request.limit {
            rows.truncate(limit);
        }
        rows
    }

    #[async_trait]
    impl HotStateReader for RowsHotStateReader {
        async fn load_exact_batch(
            &self,
            request: &crate::hot_state::HotStateExactBatchRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateExactBatch, LixError> {
            crate::hot_state::load_exact_batch_via_scan_for_test(self, request).await
        }

        async fn scan_batch(
            &self,
            request: &HotStateScanRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateBatch, LixError> {
            Ok(filter_hot_state_rows(&self.rows, request).into())
        }
    }

    #[async_trait]
    impl HotStateReader for CapturingRowsHotStateReader {
        async fn load_exact_batch(
            &self,
            request: &crate::hot_state::HotStateExactBatchRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateExactBatch, LixError> {
            crate::hot_state::load_exact_batch_via_scan_for_test(self, request).await
        }

        async fn scan_batch(
            &self,
            request: &HotStateScanRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateBatch, LixError> {
            self.requests
                .lock()
                .expect("captured live-state requests lock")
                .push(request.clone());
            Ok(filter_hot_state_rows(&self.rows, request).into())
        }
    }

    #[async_trait]
    impl HotStateReader for CountingRowsHotStateReader {
        async fn load_exact_batch(
            &self,
            request: &crate::hot_state::HotStateExactBatchRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateExactBatch, LixError> {
            crate::hot_state::load_exact_batch_via_scan_for_test(self, request).await
        }

        async fn scan_batch(
            &self,
            request: &HotStateScanRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateBatch, LixError> {
            self.scans.fetch_add(1, Ordering::SeqCst);
            Ok(filter_hot_state_rows(&self.rows, request).into())
        }
    }

    #[async_trait]
    impl RowSnapshotReader for RecordingRowSnapshotReader {
        async fn scan_row_snapshots(
            &self,
            request: HotStateScanRequest,
        ) -> Result<Option<crate::tracked_state::ExclusiveRowSnapshotBatch>, LixError> {
            self.requests
                .lock()
                .expect("captured snapshot requests lock")
                .push(request);
            Ok(Some(crate::tracked_state::ExclusiveRowSnapshotBatch::Raw(
                self.snapshots.clone(),
            )))
        }
    }

    #[async_trait]
    impl BlobDataReader for DummyBlobReader {
        async fn load_bytes_many(
            &self,
            hashes: &[crate::binary_cas::BlobId],
        ) -> Result<crate::binary_cas::BlobBytesBatch, LixError> {
            Ok(crate::binary_cas::BlobBytesBatch::new(vec![
                None;
                hashes.len()
            ]))
        }
    }

    #[async_trait]
    impl BlobDataReader for StaticBlobReader {
        async fn load_bytes_many(
            &self,
            hashes: &[crate::binary_cas::BlobId],
        ) -> Result<crate::binary_cas::BlobBytesBatch, LixError> {
            Ok(crate::binary_cas::BlobBytesBatch::new(vec![
                Some(
                    self.bytes.clone()
                );
                hashes.len()
            ]))
        }
    }

    fn live_row(row_pk: &str, branch_id: &str, value: &str) -> MaterializedHotStateRow {
        MaterializedHotStateRow {
            row_pk: crate::row_pk::RowPk::single(row_pk),
            schema_key: "test_state_schema".to_string(),
            file_id: None,
            snapshot_content: Some(format!("{{\"id\":\"{row_pk}\",\"value\":\"{value}\"}}").into()),
            metadata: Some(json!({ "source": row_pk }).to_string().into()),
            deleted: false,
            branch_id: branch_id.into(),
            change_id: Some(ChangeId::for_test_label(&format!("change-{row_pk}"))),
            commit_id: Some(CommitId::for_test_label(&format!("commit-{row_pk}"))),
            global: false,
            untracked: false,
            created_at: LixTimestamp::expect_parse("test created_at", "2026-04-23T00:00:00Z"),
            updated_at: LixTimestamp::expect_parse("test updated_at", "2026-04-23T01:00:00Z"),
        }
    }

    fn live_test_state_row(
        row_pk: &str,
        branch_id: &str,
        value: &str,
        untracked: bool,
    ) -> MaterializedHotStateRow {
        let mut row = live_row(row_pk, branch_id, value);
        row.snapshot_content = Some(json!({ "id": row_pk, "value": value }).to_string().into());
        row.untracked = untracked;
        row
    }

    fn live_directory_row(
        row_pk: &str,
        branch_id: &str,
        parent_id: Option<&str>,
        name: &str,
    ) -> MaterializedHotStateRow {
        MaterializedHotStateRow {
            row_pk: crate::row_pk::RowPk::uuid_from_canonical(row_pk)
                .expect("fixture directory ID should be a UUID"),
            schema_key: "lix_directory_descriptor".to_string(),
            file_id: None,
            snapshot_content: Some(
                json!({
                    "id": row_pk,
                    "parent_id": parent_id,
                    "name": name
                })
                .to_string()
                .into(),
            ),
            metadata: Some(json!({ "source": row_pk }).to_string().into()),
            deleted: false,
            branch_id: branch_id.into(),
            change_id: Some(ChangeId::for_test_label(&format!("change-{row_pk}"))),
            commit_id: Some(CommitId::for_test_label(&format!("commit-{row_pk}"))),
            global: false,
            untracked: false,
            created_at: LixTimestamp::expect_parse("test created_at", "2026-04-23T00:00:00Z"),
            updated_at: LixTimestamp::expect_parse("test updated_at", "2026-04-23T01:00:00Z"),
        }
    }

    fn live_file_row(
        row_pk: &str,
        branch_id: &str,
        directory_id: Option<&str>,
        name: &str,
    ) -> MaterializedHotStateRow {
        MaterializedHotStateRow {
            row_pk: crate::row_pk::RowPk::uuid_from_canonical(row_pk)
                .expect("fixture file ID should be a UUID"),
            schema_key: "lix_file_descriptor".to_string(),
            file_id: Some(row_pk.to_string()),
            snapshot_content: Some(
                json!({
                    "id": row_pk,
                    "directory_id": directory_id,
                    "name": name
                })
                .to_string()
                .into(),
            ),
            metadata: Some(json!({ "source": row_pk }).to_string().into()),
            deleted: false,
            branch_id: branch_id.into(),
            change_id: Some(ChangeId::for_test_label(&format!("change-{row_pk}"))),
            commit_id: Some(CommitId::for_test_label(&format!("commit-{row_pk}"))),
            global: false,
            untracked: false,
            created_at: LixTimestamp::expect_parse("test created_at", "2026-04-23T00:00:00Z"),
            updated_at: LixTimestamp::expect_parse("test updated_at", "2026-04-23T01:00:00Z"),
        }
    }

    fn live_blob_ref_row(row_pk: &str, branch_id: &str, bytes: &[u8]) -> MaterializedHotStateRow {
        MaterializedHotStateRow {
            row_pk: crate::row_pk::RowPk::uuid_from_canonical(row_pk)
                .expect("fixture blob-ref ID should be a UUID"),
            schema_key: "lix_binary_blob_ref".to_string(),
            file_id: Some(row_pk.to_string()),
            snapshot_content: Some(
                json!({
                    "id": row_pk,
                    "blob_hash": crate::binary_cas::BlobId::from_content(bytes).to_hex(),
                    "size_bytes": bytes.len()
                })
                .to_string()
                .into(),
            ),
            metadata: Some(json!({ "source": row_pk }).to_string().into()),
            deleted: false,
            branch_id: branch_id.into(),
            change_id: Some(ChangeId::for_test_label(&format!("change-{row_pk}-blob"))),
            commit_id: Some(CommitId::for_test_label(&format!("commit-{row_pk}-blob"))),
            global: false,
            untracked: false,
            created_at: LixTimestamp::expect_parse("test created_at", "2026-04-23T00:00:00Z"),
            updated_at: LixTimestamp::expect_parse("test updated_at", "2026-04-23T01:00:00Z"),
        }
    }

    /// The revocation guard refuses a retired write context instead of
    /// dereferencing a pointer into a destroyed `Transaction`.
    ///
    /// The first half is load-bearing: it proves the guard is *reached* on this
    /// path. Without it a green result could mean "never checked" rather than
    /// "checked and live", which is exactly how a null instrument result lies.
    #[tokio::test]
    async fn retired_write_context_is_refused_instead_of_dereferenced() {
        const BRANCH: &str = "01920000-0000-7000-8000-0000000000a1";
        const REFUSAL: &str = "refusing to dereference a retired context";

        let (mut ctx, _staged, _scans) = counting_write_context(vec![]);
        let write_ctx = crate::sql2::SqlWriteContext::new(&mut ctx);

        let live = write_ctx.load_branch_head(BRANCH).await;
        assert!(
            !format!("{live:?}").contains(REFUSAL),
            "guard must not fire while the borrowed context is live: {live:?}"
        );

        // Exactly what `Transaction::drop` does.
        write_ctx.liveness_for_test().retire();

        let error = write_ctx
            .load_branch_head(BRANCH)
            .await
            .expect_err("a retired write context must be refused");
        assert!(
            format!("{error:?}").contains(REFUSAL),
            "retired context produced the wrong failure: {error:?}"
        );
    }

    fn counting_write_context(
        rows: Vec<MaterializedHotStateRow>,
    ) -> (
        DummySqlWriteExecutionContext<'static>,
        Arc<Mutex<CapturingStagedWrites>>,
        Arc<AtomicUsize>,
    ) {
        counting_write_context_with_blob_reader(rows, Arc::new(DummyBlobReader))
    }

    fn counting_write_context_with_blob_reader(
        rows: Vec<MaterializedHotStateRow>,
        blob_reader: Arc<dyn BlobDataReader>,
    ) -> (
        DummySqlWriteExecutionContext<'static>,
        Arc<Mutex<CapturingStagedWrites>>,
        Arc<AtomicUsize>,
    ) {
        let scans = Arc::new(AtomicUsize::new(0));
        let hot_state: Arc<dyn HotStateReader> = Arc::new(CountingRowsHotStateReader {
            rows,
            scans: Arc::clone(&scans),
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        (
            DummySqlWriteExecutionContext {
                active_branch_id: "01920000-0000-7000-8000-0000000000a1",
                blob_reader,
                hot_state,
                staged_writes: Arc::clone(&staged_writes),
                schema_definitions: vec![],
            },
            staged_writes,
            scans,
        )
    }

    fn mark_untracked(mut row: MaterializedHotStateRow) -> MaterializedHotStateRow {
        row.untracked = true;
        row
    }

    fn descriptor_names(rows: &[CapturedStageRow]) -> Vec<String> {
        let mut names = rows
            .iter()
            .map(|row| {
                let snapshot: JsonValue =
                    serde_json::from_str(row.snapshot_content.as_deref().unwrap())
                        .expect("descriptor snapshot JSON");
                snapshot["name"].as_str().unwrap().to_string()
            })
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    #[tokio::test]
    #[expect(trivial_casts)]
    async fn sql_execution_context_exposes_hot_state_and_blob_reader() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(DummyHotStateReader);
        let ctx = DummySqlExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader: Arc::clone(&blob_reader),
            hot_state: Arc::clone(&hot_state) as Arc<dyn HotStateReader>,
            row_snapshot_reader: None,
            schema_definitions: vec![],
        };

        let actual = ctx.hot_state();
        let expected = hot_state as Arc<dyn HotStateReader>;
        assert_eq!(
            ctx.active_branch_id(),
            "01920000-0000-7000-8000-0000000000a1"
        );
        assert!(Arc::ptr_eq(&actual, &expected));
        assert!(Arc::ptr_eq(&ctx.blob_reader(), &blob_reader));
    }

    #[tokio::test]
    async fn execute_sql_uses_execution_context_boundary() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(DummyHotStateReader);
        let ctx = DummySqlExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            row_snapshot_reader: None,
            schema_definitions: vec![],
        };

        let result = execute_sql(&ctx, "SELECT 1", &[])
            .await
            .expect("sql2 execute should support literal-only queries");
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);
    }

    #[tokio::test]
    async fn integer_primary_key_read_pushes_exact_identity_to_hot_state() {
        let branch_id = "01920000-0000-7000-8000-0000000000a1";
        let component_types = [crate::row_pk::RowPkComponentType::Integer];
        let row_pk =
            crate::row_pk::RowPk::from_external_parts(vec!["42".to_string()], &component_types)
                .expect("integer fixture identity should encode");
        let mut row = live_row("42", branch_id, "answer");
        row.row_pk = row_pk.clone();
        row.schema_key = "integer_state_schema".to_string();
        row.snapshot_content = Some(json!({ "id": 42, "value": "answer" }).to_string().into());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let ctx = DummySqlExecutionContext {
            active_branch_id: branch_id,
            blob_reader: Arc::new(DummyBlobReader),
            hot_state: Arc::new(CapturingRowsHotStateReader {
                rows: vec![row],
                requests: Arc::clone(&requests),
            }),
            row_snapshot_reader: None,
            schema_definitions: vec![json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "integer_state_schema",
                "columns": [
                    { "name": "id", "type": "int8", "nullable": false },
                    { "name": "value", "type": "text", "nullable": false },
                ],
                "primary_key": ["id"],
            })],
        };

        let result = execute_sql(
            &ctx,
            "SELECT value FROM integer_state_schema WHERE id = $1",
            &[Value::Integer(42)],
        )
        .await
        .expect("integer point read should execute");

        assert_eq!(result.rows, vec![vec![Value::Text("answer".to_string())]]);
        let requests = requests.lock().expect("captured live-state requests lock");
        let [request] = requests.as_slice() else {
            panic!("integer point read should issue one live-state scan");
        };
        assert_eq!(request.filter.row_pks, vec![row_pk]);
    }

    #[tokio::test]
    async fn datafusion_row_primary_key_read_materializes_public_result() {
        let sql = "SELECT id, value FROM test_state_schema \
                   WHERE id IN ('row-b', 'row-a') ORDER BY id";
        let ctx = DummySqlExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader: Arc::new(DummyBlobReader),
            hot_state: Arc::new(RowsHotStateReader {
                rows: vec![
                    live_test_state_row(
                        "row-b",
                        "01920000-0000-7000-8000-0000000000a1",
                        "B",
                        false,
                    ),
                    live_test_state_row(
                        "row-a",
                        "01920000-0000-7000-8000-0000000000a1",
                        "A",
                        false,
                    ),
                ],
            }),
            row_snapshot_reader: None,
            schema_definitions: vec![json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "test_state_schema",
                "columns": [
                    { "name": "id", "type": "text", "nullable": false },
                    { "name": "value", "type": "text", "nullable": false },
                ],
                "primary_key": ["id"],
            })],
        };
        let result = execute_sql(&ctx, sql, &[])
            .await
            .expect("DataFusion primary-key read should execute");

        assert_eq!(result.columns, vec!["id", "value"]);
        assert_eq!(
            result.rows,
            vec![
                vec![
                    Value::Text("row-a".to_string()),
                    Value::Text("A".to_string())
                ],
                vec![
                    Value::Text("row-b".to_string()),
                    Value::Text("B".to_string())
                ],
            ]
        );
    }

    #[tokio::test]
    async fn datafusion_row_primary_key_read_uses_registered_provider() {
        let sql = "SELECT id, value FROM test_state_schema \
                   WHERE id IN ('row-b', 'row-a', 'row-b') ORDER BY id";
        let schema_definition = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "test_state_schema",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        let schema_spec =
            crate::sql2::catalog::derive_schema_surface_spec_from_schema(&schema_definition)
                .expect("test schema should compile");
        let native_row = |id: &str, value: &str| {
            let typed = crate::row_payload::TypedRow {
                schema_fingerprint: schema_spec.schema_fingerprint,
                row_pk: vec![lix_schema::Value::Text(id.to_owned())].into(),
                row: lix_schema::Row::from([
                    ("id".to_owned(), lix_schema::Value::Text(id.to_owned())),
                    (
                        "value".to_owned(),
                        lix_schema::Value::Text(value.to_owned()),
                    ),
                ]),
                native_payload: std::sync::OnceLock::new(),
                boundary_create_validation: std::sync::OnceLock::new(),
            };
            let payload = typed.durable_payload().expect("test row should encode");
            (
                crate::row_pk::RowPk::single(id),
                bytes::Bytes::copy_from_slice(&payload),
            )
        };
        let requests = Arc::new(Mutex::new(Vec::new()));
        let snapshot_reader = Arc::new(RecordingRowSnapshotReader {
            snapshots: vec![native_row("row-a", "A"), native_row("row-b", "B")],
            requests: Arc::clone(&requests),
        });
        let scans = Arc::new(AtomicUsize::new(0));
        let ctx = DummySqlExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader: Arc::new(DummyBlobReader),
            hot_state: Arc::new(CountingRowsHotStateReader {
                rows: vec![
                    live_test_state_row(
                        "row-b",
                        "01920000-0000-7000-8000-0000000000a1",
                        "B",
                        false,
                    ),
                    live_test_state_row(
                        "row-a",
                        "01920000-0000-7000-8000-0000000000a1",
                        "A",
                        false,
                    ),
                ],
                scans: Arc::clone(&scans),
            }),
            row_snapshot_reader: Some(snapshot_reader),
            schema_definitions: vec![schema_definition],
        };
        let result = execute_sql(&ctx, sql, &[])
            .await
            .expect("DataFusion primary-key read should execute");

        assert_eq!(
            result.rows,
            vec![
                vec![
                    Value::Text("row-a".to_string()),
                    Value::Text("A".to_string())
                ],
                vec![
                    Value::Text("row-b".to_string()),
                    Value::Text("B".to_string())
                ],
            ]
        );
        // A `WHERE` clause that resolves to a complete row identity set is
        // applied in full by the `row_pks` access path, so this read must
        // take the direct point-snapshot route rather than the generic
        // visibility scan.
        //
        // This assertion pair previously read `scans == 1` and
        // `requests.is_empty()` — the exact opposite — and its message called
        // the snapshot reader "the deleted native snapshot route". That wording
        // predates the current `RowSnapshotReader`, which is a live route
        // registered from `SqlExecutionContext::row_snapshot_reader` and
        // backed by the row point-snapshot cache. The old expectation froze
        // a mis-gate in `plan_scan_parts`: it re-derived a residual row filter
        // for a predicate the access path already applied, and a non-empty
        // `row_filters` disqualifies every direct route.
        assert_eq!(
            scans.load(Ordering::SeqCst),
            0,
            "an exact identity point read must not fall back to the generic visibility scan"
        );
        let requests = requests.lock().expect("captured snapshot requests lock");
        assert_eq!(
            requests.len(),
            1,
            "an exact identity point read must consult the row point-snapshot route exactly once"
        );
        assert_eq!(
            requests[0].filter.schema_keys,
            vec!["test_state_schema".to_string()],
            "the point-snapshot request must be scoped to the queried schema"
        );
        assert_eq!(
            requests[0].filter.row_pks,
            vec![
                crate::row_pk::RowPk::single("row-a"),
                crate::row_pk::RowPk::single("row-b"),
            ],
            "the repeated 'row-b' in the IN list must collapse into a deduplicated, \
             ordered identity set rather than being pushed down three times"
        );
    }

    #[tokio::test]
    async fn datafusion_row_left_join_preserves_matches_and_null_extension() {
        let sql = r#"SELECT "bundle"."id" AS "bundle_id",
                         "message"."id" AS "message_id",
                         "variant"."id" AS "variantId",
                         "variant"."pattern" AS "variantPattern"
                    FROM "bundle"
                    LEFT JOIN "message" ON "message"."bundle_id" = "bundle"."id"
                    LEFT JOIN "variant" ON "variant"."message_id" = "message"."id"
                   WHERE "bundle"."id" = $1"#;
        let row = |schema_key: &str, row_pk: &str, snapshot: &str| {
            let mut row = live_row(row_pk, "01920000-0000-7000-8000-0000000000a1", "");
            row.schema_key = schema_key.to_string();
            row.snapshot_content = Some(snapshot.to_string().into());
            row
        };
        let ctx = DummySqlExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader: Arc::new(DummyBlobReader),
            hot_state: Arc::new(RowsHotStateReader {
                rows: vec![
                    row("bundle", "b1", r#"{"id":"b1"}"#),
                    // Stored JSON can violate a registered string type. The
                    // provider projection retains the established coercion.
                    row("message", "true", r#"{"id":true,"bundle_id":"b1"}"#),
                    row("message", "m2", r#"{"id":"m2","bundle_id":"b1"}"#),
                    row(
                        "variant",
                        "v1",
                        r#"{"id":"v1","message_id":true,"pattern":"Hello"}"#,
                    ),
                ],
            }),
            row_snapshot_reader: None,
            schema_definitions: vec![
                json!({
                    "$schema": "https://lix.dev/schema-v1.json",
                    "key": "bundle",
                    "columns": [
                        { "name": "id", "type": "text", "nullable": false },
                    ],
                    "primary_key": ["id"],
                }),
                json!({
                    "$schema": "https://lix.dev/schema-v1.json",
                    "key": "message",
                    "columns": [
                        { "name": "id", "type": "text", "nullable": false },
                        { "name": "bundle_id", "type": "text", "nullable": false },
                    ],
                    "primary_key": ["id"],
                }),
                json!({
                    "$schema": "https://lix.dev/schema-v1.json",
                    "key": "variant",
                    "columns": [
                        { "name": "id", "type": "text", "nullable": false },
                        { "name": "message_id", "type": "text", "nullable": false },
                        { "name": "pattern", "type": "text", "nullable": false },
                    ],
                    "primary_key": ["id"],
                }),
            ],
        };
        let result = execute_sql(&ctx, sql, &[Value::Text("b1".to_string())])
            .await
            .expect("DataFusion row join should execute");

        assert_eq!(
            result.columns,
            ["bundle_id", "message_id", "variantId", "variantPattern"]
        );
        assert_eq!(
            result.rows,
            vec![
                vec![
                    Value::Text("b1".to_string()),
                    Value::Text("true".to_string()),
                    Value::Text("v1".to_string()),
                    Value::Text("Hello".to_string()),
                ],
                vec![
                    Value::Text("b1".to_string()),
                    Value::Text("m2".to_string()),
                    Value::Null,
                    Value::Null,
                ],
            ]
        );
    }

    #[tokio::test]
    async fn execute_sql_collects_union_all_partitions() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(DummyHotStateReader);
        let ctx = DummySqlExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            row_snapshot_reader: None,
            schema_definitions: vec![],
        };

        let result = execute_sql(&ctx, "SELECT 1 UNION ALL SELECT 2", &[])
            .await
            .expect("sql2 execute should collect UNION ALL partitions");
        assert_eq!(
            result.rows,
            vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]
        );
    }

    #[tokio::test]
    async fn filtered_sum_does_not_use_unfiltered_exact_statistics() {
        let storage = Memory::new();
        let init_receipt = Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(storage).await.expect("engine should open");
        let session = engine
            .open_session_at(init_receipt.main_branch_id)
            .await
            .expect("session should open");
        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"aggregate_filter_test\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"value\",\"type\":\"int8\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
                 false, false)",
                &[],
            )
            .await
            .expect("test schema should register");
        session
            .execute(
                "INSERT INTO aggregate_filter_test (id, value) VALUES ('a', 10), ('b', 20)",
                &[],
            )
            .await
            .expect("test rows should insert");

        let result = session
            .execute(
                "SELECT SUM(value) AS total, AVG(value) AS average \
                 FROM aggregate_filter_test WHERE value < 0",
                &[],
            )
            .await
            .expect("filtered aggregate should execute");

        assert_eq!(result.rows()[0].values(), &[Value::Null, Value::Null]);
    }

    #[tokio::test]
    async fn execute_sql_rejects_extra_parameters() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(DummyHotStateReader);
        let ctx = DummySqlExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            row_snapshot_reader: None,
            schema_definitions: vec![],
        };

        let error = execute_sql(
            &ctx,
            "SELECT $1 AS value",
            &[Value::Integer(1), Value::Integer(2)],
        )
        .await
        .expect_err("extra params should fail instead of being ignored");

        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert_eq!(
            error.message,
            "SQL expected 1 parameter(s), but 2 parameter(s) were provided"
        );
        assert_eq!(
            error.details(),
            Some(&json!({
                "operation": "execute",
                "expected_param_count": 1,
                "provided_param_count": 2,
                "placeholders": ["$1"],
            }))
        );
    }

    #[tokio::test]
    async fn execute_sql_exposes_datafusion_information_schema() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(DummyHotStateReader);
        let ctx = DummySqlExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            row_snapshot_reader: None,
            schema_definitions: vec![],
        };

        let information_schema_result = execute_sql(
            &ctx,
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'lix_file'",
            &[],
        )
        .await
        .expect("information_schema.tables should be enabled");
        assert_eq!(
            information_schema_result.rows,
            vec![vec![Value::Text("lix_file".to_string())]]
        );

        let tables_result = execute_sql(
            &ctx,
            "SELECT table_name FROM information_schema.tables",
            &[],
        )
        .await
        .expect("information_schema.tables should list registered tables");
        assert!(tables_result.rows.iter().any(|row| {
            row.iter()
                .any(|value| matches!(value, Value::Text(value) if value == "lix_file"))
        }));
    }

    #[tokio::test]
    async fn whole_row_collection_delete_uses_one_generation_fact_and_allows_recreation() {
        let storage = Memory::new();
        let init_receipt = Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(storage).await.expect("engine should open");
        let session = engine
            .open_session_at(init_receipt.main_branch_id)
            .await
            .expect("session should open");
        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"test_state_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"value\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("test schema should register");
        session
            .execute(
                "INSERT INTO test_state_schema (id, value) VALUES \
                 ('a', 'A'), ('b', 'B'), ('c', 'C')",
                &[],
            )
            .await
            .expect("test rows should insert");
        let initial_count = session
            .execute("SELECT COUNT(*) AS count FROM test_state_schema", &[])
            .await
            .expect("collection metadata count should query after inserts");
        assert_eq!(
            initial_count.rows()[0]
                .get::<i64>("count")
                .expect("count should be numeric"),
            3
        );
        let deleted = session
            .execute("DELETE FROM test_state_schema", &[])
            .await
            .expect("whole collection should delete");
        assert_eq!(deleted.rows_affected(), 3);
        let generation = session
            .execute(
                "SELECT schema_key, snapshot_content IS NOT NULL AS is_added \
                 FROM lix_change WHERE schema_key = 'lix_collection_generation'",
                &[],
            )
            .await
            .expect("collection generation fact should query");
        assert_eq!(
            rows_from_execute_result(generation).1,
            vec![vec![
                Value::Text("lix_collection_generation".to_string()),
                Value::Boolean(true),
            ]],
            "whole-collection deletion authors one generation fact instead of row tombstones",
        );
        let marker_changes = session
            .execute(
                "SELECT COUNT(*) AS changes FROM lix_change \
                 WHERE schema_key = 'lix_collection_generation'",
                &[],
            )
            .await
            .expect("collection marker changelog should query")
            .rows()[0]
            .get::<i64>("changes")
            .expect("marker count should be numeric");
        assert_eq!(marker_changes, 1);
        let expanded_tombstones = session
            .execute(
                "SELECT COUNT(*) AS changes FROM lix_change \
                 WHERE schema_key = 'test_state_schema' AND snapshot_content IS NULL",
                &[],
            )
            .await
            .expect("row tombstone changelog should query")
            .rows()[0]
            .get::<i64>("changes")
            .expect("tombstone count should be numeric");
        assert_eq!(expanded_tombstones, 0);
        let selected = session
            .execute("SELECT id FROM test_state_schema ORDER BY id", &[])
            .await
            .expect("deleted collection should remain queryable");
        assert!(rows_from_execute_result(selected).1.is_empty());
        let deleted_count = session
            .execute("SELECT COUNT(*) AS count FROM test_state_schema", &[])
            .await
            .expect("collection metadata count should query after delete");
        assert_eq!(
            deleted_count.rows()[0]
                .get::<i64>("count")
                .expect("count should be numeric"),
            0
        );
        session
            .create_checkpoint()
            .await
            .expect("checkpoint should preserve a collection generation delete");
        let selected = session
            .execute("SELECT id FROM test_state_schema ORDER BY id", &[])
            .await
            .expect("checkpointed deleted collection should query");
        assert!(rows_from_execute_result(selected).1.is_empty());
        let checkpointed_count = session
            .execute("SELECT COUNT(*) AS count FROM test_state_schema", &[])
            .await
            .expect("collection metadata count should query after checkpoint");
        assert_eq!(
            checkpointed_count.rows()[0]
                .get::<i64>("count")
                .expect("count should be numeric"),
            0
        );
        let checkout = session
            .create_branch(CreateBranchOptions {
                id: None,
                name: "after-generation-delete".to_string(),
                from_commit_id: None,
            })
            .await
            .expect("branching from the deleted collection should succeed");
        let checkout_session = engine
            .open_session_at(checkout.id)
            .await
            .expect("checkout branch session should open");
        let selected = checkout_session
            .execute("SELECT id FROM test_state_schema ORDER BY id", &[])
            .await
            .expect("branch created from the deleted generation should query");
        assert!(rows_from_execute_result(selected).1.is_empty());

        session
            .execute(
                "INSERT INTO test_state_schema (id, value) VALUES ('a', 'A2')",
                &[],
            )
            .await
            .expect("a retired-generation identity should be reusable");
        let selected = session
            .execute("SELECT id, value FROM test_state_schema", &[])
            .await
            .expect("recreated generation should be visible");
        assert_eq!(
            rows_from_execute_result(selected).1,
            vec![vec![
                Value::Text("a".to_string()),
                Value::Text("A2".to_string())
            ]]
        );
        let recreated_count = session
            .execute("SELECT COUNT(*) AS count FROM test_state_schema", &[])
            .await
            .expect("collection metadata count should query after recreation");
        assert_eq!(
            recreated_count.rows()[0]
                .get::<i64>("count")
                .expect("count should be numeric"),
            1
        );

        let deleted = session
            .execute("DELETE FROM test_state_schema", &[])
            .await
            .expect("recreated collection should delete");
        assert_eq!(deleted.rows_affected(), 1);
    }

    #[tokio::test]
    async fn whole_row_collection_delete_is_visible_inside_explicit_transaction() {
        let storage = Memory::new();
        let init_receipt = Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(storage).await.expect("engine should open");
        let session = engine
            .open_session_at(init_receipt.main_branch_id)
            .await
            .expect("session should open");
        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"test_state_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("test schema should register");
        session
            .execute(
                "INSERT INTO test_state_schema (id) VALUES ('a'), ('b')",
                &[],
            )
            .await
            .expect("test rows should insert");

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("explicit transaction should open");
        let deleted = transaction
            .execute("DELETE FROM test_state_schema", &[])
            .await
            .expect("collection delete should stage");
        assert_eq!(deleted.rows_affected(), 2);
        let selected = transaction
            .execute("SELECT id FROM test_state_schema", &[])
            .await
            .expect("staged collection delete should be visible");
        assert!(rows_from_execute_result(selected).1.is_empty());
        let deleted_again = transaction
            .execute("DELETE FROM test_state_schema", &[])
            .await
            .expect("repeated staged collection delete should be a no-op");
        assert_eq!(deleted_again.rows_affected(), 0);
        let recreate_error = transaction
            .execute(
                "INSERT INTO test_state_schema (id) VALUES ('next-generation')",
                &[],
            )
            .await
            .expect_err("recreation should require the deletion commit boundary");
        assert_eq!(recreate_error.code, LixError::CODE_CONSTRAINT_VIOLATION);
        assert_eq!(
            recreate_error.hint.as_deref(),
            Some("Commit the collection deletion before recreating rows in its next generation.")
        );
        transaction
            .commit()
            .await
            .expect("collection delete should commit");

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("explicit transaction on the empty collection should open");
        transaction
            .execute(
                "INSERT INTO test_state_schema (id) VALUES ('only-staged')",
                &[],
            )
            .await
            .expect("member should stage against an empty committed collection");
        let deleted = transaction
            .execute("DELETE FROM test_state_schema", &[])
            .await
            .expect("staged-only collection should delete");
        assert_eq!(deleted.rows_affected(), 1);
        let selected = transaction
            .execute("SELECT id FROM test_state_schema", &[])
            .await
            .expect("staged-only collection delete should be visible");
        assert!(rows_from_execute_result(selected).1.is_empty());
        transaction
            .commit()
            .await
            .expect("staged-only collection delete should commit");
        let selected = session
            .execute("SELECT id FROM test_state_schema", &[])
            .await
            .expect("committed staged-only collection delete should query");
        assert!(rows_from_execute_result(selected).1.is_empty());

        session
            .execute(
                "INSERT INTO test_state_schema (id) VALUES ('committed-a'), ('committed-b')",
                &[],
            )
            .await
            .expect("committed members should recreate the collection");
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("explicit transaction on the nonempty collection should open");
        transaction
            .execute(
                "INSERT INTO test_state_schema (id) VALUES ('staged-c')",
                &[],
            )
            .await
            .expect("additional member should stage");
        let deleted = transaction
            .execute("DELETE FROM test_state_schema", &[])
            .await
            .expect("committed and staged members should delete together");
        assert_eq!(deleted.rows_affected(), 3);
        let selected = transaction
            .execute("SELECT id FROM test_state_schema", &[])
            .await
            .expect("mixed committed and staged collection delete should be visible");
        assert!(rows_from_execute_result(selected).1.is_empty());
        transaction
            .commit()
            .await
            .expect("mixed committed and staged collection delete should commit");
    }

    #[tokio::test]
    async fn whole_row_collection_delete_falls_back_for_global_members() {
        let storage = Memory::new();
        let init_receipt = Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(storage).await.expect("engine should open");
        let session = engine
            .open_session_at(init_receipt.main_branch_id)
            .await
            .expect("session should open");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_global) VALUES \
                 ('local-only', CAST('1' AS JSONB), false), \
                 ('global-only', CAST('2' AS JSONB), true), \
                 ('shadowed', CAST('3' AS JSONB), true), \
                 ('shadowed', CAST('4' AS JSONB), false)",
                &[],
            )
            .await
            .expect("local, global, and shadowing rows should insert");
        let visible_before = session
            .execute("SELECT COUNT(*) AS count FROM lix_key_value", &[])
            .await
            .expect("visible collection count should query")
            .rows()[0]
            .get::<i64>("count")
            .expect("visible collection count should be numeric");

        let deleted = session
            .execute("DELETE FROM lix_key_value", &[])
            .await
            .expect("mixed global collection should delete through row fallback");
        assert_eq!(deleted.rows_affected(), visible_before as u64);
        let marker_count = session
            .execute(
                "SELECT COUNT(*) AS count FROM lix_change \
                 WHERE schema_key = 'lix_collection_generation'",
                &[],
            )
            .await
            .expect("marker count should query")
            .rows()[0]
            .get::<i64>("count")
            .expect("marker count should be numeric");
        assert_eq!(marker_count, 0);
    }

    #[tokio::test]
    async fn merge_applies_collection_generation_delete_without_expanding_members() {
        let storage = Memory::new();
        let init_receipt = Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(storage).await.expect("engine should open");
        let main = engine
            .open_session_at(init_receipt.main_branch_id)
            .await
            .expect("main session should open");
        main.execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"test_state_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
             false,\
             false\
             )",
            &[],
        )
        .await
        .expect("test schema should register");
        main.execute(
            "INSERT INTO test_state_schema (id) VALUES ('a'), ('b'), ('c')",
            &[],
        )
        .await
        .expect("test rows should insert");
        let source = main
            .create_branch(CreateBranchOptions {
                id: None,
                name: "delete-source".to_string(),
                from_commit_id: None,
            })
            .await
            .expect("source branch should create");
        let source_session = engine
            .open_session_at(source.id.clone())
            .await
            .expect("source session should open");
        let deleted = source_session
            .execute("DELETE FROM test_state_schema", &[])
            .await
            .expect("source collection should delete");
        assert_eq!(deleted.rows_affected(), 3);

        let before_merge = main
            .execute("SELECT id FROM test_state_schema", &[])
            .await
            .expect("target collection should query before merge");
        assert_eq!(rows_from_execute_result(before_merge).1.len(), 3);
        main.merge_branch(MergeBranchOptions {
            source_branch_id: source.id,
        })
        .await
        .expect("collection delete should merge");
        let after_merge = main
            .execute("SELECT id FROM test_state_schema", &[])
            .await
            .expect("target collection should query after merge");
        assert!(rows_from_execute_result(after_merge).1.is_empty());

        let expanded_tombstones = main
            .execute(
                "SELECT COUNT(*) AS changes FROM lix_change \
                 WHERE schema_key = 'test_state_schema' AND snapshot_content IS NULL",
                &[],
            )
            .await
            .expect("merged row tombstones should query")
            .rows()[0]
            .get::<i64>("changes")
            .expect("tombstone count should be numeric");
        assert_eq!(expanded_tombstones, 0);
    }

    #[tokio::test]
    async fn lix_file_path_predicates_preserve_literal_values_like_writes() {
        let storage = Memory::new();
        let init_receipt = Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(storage).await.expect("engine should open");
        let session = engine
            .open_session_at(init_receipt.main_branch_id)
            .await
            .expect("session should open");

        session
            .execute(
                "INSERT INTO lix_file (id, path, content) VALUES ('01920000-0000-7000-8000-000000000302', $1, CAST('A' AS BYTEA))",
                &[Value::Text("/Cafe\u{301}.txt".to_string())],
            )
            .await
            .expect("decomposed path insert should preserve literal text");

        let decomposed_result = session
            .execute(
                "SELECT id FROM lix_file WHERE path = $1",
                &[Value::Text("/Cafe\u{301}.txt".to_string())],
            )
            .await
            .expect("decomposed path predicate should match literal text");
        assert_eq!(
            rows_from_execute_result(decomposed_result).1,
            vec![vec![Value::Text(
                "01920000-0000-7000-8000-000000000302".to_string()
            )]]
        );

        let composed_alias_result = session
            .execute(
                "SELECT id FROM lix_file WHERE path = $1",
                &[Value::Text("/Café.txt".to_string())],
            )
            .await
            .expect("composed path predicate should execute");
        assert!(rows_from_execute_result(composed_alias_result).1.is_empty());

        let update_result = session
            .execute(
                "UPDATE lix_file SET content = CAST('B' AS BYTEA) WHERE path = $1",
                &[Value::Text("/Cafe\u{301}.txt".to_string())],
            )
            .await
            .expect("update predicate should match literal text");
        assert_eq!(update_result.rows_affected(), 1);

        let delete_result = session
            .execute(
                "DELETE FROM lix_file WHERE path = $1",
                &[Value::Text("/Cafe\u{301}.txt".to_string())],
            )
            .await
            .expect("delete predicate should match literal text");
        assert_eq!(delete_result.rows_affected(), 1);
    }

    #[tokio::test]
    async fn lix_directory_path_predicates_preserve_literal_values_like_writes() {
        let storage = Memory::new();
        let init_receipt = Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(storage).await.expect("engine should open");
        let session = engine
            .open_session_at(init_receipt.main_branch_id)
            .await
            .expect("session should open");

        session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES ('01920000-0000-7000-8000-000000000303', $1)",
                &[Value::Text("/Cafe\u{301}".to_string())],
            )
            .await
            .expect("decomposed directory path insert should preserve literal text");

        let result = session
            .execute(
                "SELECT id FROM lix_directory WHERE path IN ($1)",
                &[Value::Text("/Cafe\u{301}".to_string())],
            )
            .await
            .expect("directory path predicate should match literal text");
        assert_eq!(
            rows_from_execute_result(result).1,
            vec![vec![Value::Text(
                "01920000-0000-7000-8000-000000000303".to_string()
            )]]
        );

        let composed_alias_result = session
            .execute(
                "SELECT id FROM lix_directory WHERE path IN ($1)",
                &[Value::Text("/Café".to_string())],
            )
            .await
            .expect("composed directory path predicate should execute");
        assert!(rows_from_execute_result(composed_alias_result).1.is_empty());
    }

    fn rows_from_execute_result(result: ExecuteResult) -> (Vec<String>, Vec<Vec<Value>>) {
        let rows = result;
        (
            rows.columns().to_vec(),
            rows.rows()
                .iter()
                .map(|row| row.values().to_vec())
                .collect(),
        )
    }

    #[tokio::test]
    async fn execute_sql_rejects_writes_to_removed_history_suffixes_before_planning() {
        for sql in [
            "DELETE FROM test_state_schema_history",
            "DELETE FROM TEST_STATE_SCHEMA_HISTORY",
        ] {
            let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
            let hot_state = Arc::new(DummyHotStateReader);
            let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
            let mut ctx = DummySqlWriteExecutionContext {
                active_branch_id: "01920000-0000-7000-8000-0000000000a1",
                blob_reader,
                hot_state,
                staged_writes,
                schema_definitions: vec![json!({
                    "$schema": "https://lix.dev/schema-v1.json",
                    "key": "test_state_schema",
                    "columns": [
                        { "name": "id", "type": "text", "nullable": false },
                        { "name": "value", "type": "text", "nullable": false },
                    ],
                    "primary_key": ["id"],
                })],
            };

            let error = execute_write_sql(&mut ctx, sql, &[])
                .await
                .expect_err("history suffix surfaces should not exist");

            assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL, "{sql}");
            assert!(
                error.message.contains("unknown SQL table"),
                "{sql}: {error:?}"
            );
        }
    }

    #[tokio::test]
    async fn execute_sql_insert_into_lix_file_select_without_data_stages_descriptor() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(DummyHotStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_file (id, path) SELECT '01920000-0000-7000-8000-000000000312', '/docs/from-select.txt'",
            &[],
        )
        .await
        .expect("lix_file INSERT SELECT without content should execute");

        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);
        let staged_writes = staged_writes.lock().expect("staged writes lock");
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_file_descriptor");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_pk, "[\"01920000-0000-7000-8000-000000000312\"]");
        assert_eq!(rows[0].branch_id, "01920000-0000-7000-8000-0000000000a1");
    }

    #[tokio::test]
    async fn execute_sql_insert_into_active_row_defaults_active_branch() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(DummyHotStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "test_state_schema",
                "columns": [
                    { "name": "id", "type": "text", "nullable": false },
                    { "name": "value", "type": "text", "nullable": false },
                ],
                "primary_key": ["id"],
            })],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO test_state_schema (id, value) VALUES ('row-c', 'C')",
            &[],
        )
        .await
        .expect("INSERT INTO active schema surface should stage write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "test_state_schema");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_pk, "[\"row-c\"]");
        assert_eq!(rows[0].branch_id, "01920000-0000-7000-8000-0000000000a1");
        assert!(!rows[0].global);
        assert!(!rows[0].untracked);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"id\":\"row-c\",\"value\":\"C\"}")
        );
    }

    #[tokio::test]
    async fn execute_sql_insert_default_values_uses_the_native_row_writer() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(DummyHotStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "default_values_probe",
                "columns": [
                    { "name": "id", "type": "uuid", "nullable": false, "default_expression": "uuidv7()" },
                    { "name": "label", "type": "text", "nullable": false, "default_value": "untitled" },
                ],
                "primary_key": ["id"],
            })],
        };

        let (result, path) = execute_write_sql_trace(
            &mut ctx,
            "INSERT INTO default_values_probe DEFAULT VALUES",
            &[],
            WriteExecutorMode::ForceFast,
        )
        .await
        .expect("DEFAULT VALUES should not require the DataFusion writer");

        assert_eq!(path, WriteExecutorPath::Fast);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);
        let staged_writes = staged_writes.lock().expect("staged writes lock");
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "default_values_probe");
        assert_eq!(rows.len(), 1);
        let snapshot = serde_json::from_str::<JsonValue>(
            rows[0]
                .snapshot_content
                .as_deref()
                .expect("inserted row should have a snapshot"),
        )
        .expect("defaulted snapshot should be JSON");
        let id = snapshot["id"]
            .as_str()
            .expect("UUID default should materialize a string");
        assert_eq!(
            uuid::Uuid::parse_str(id)
                .expect("UUID default should be parseable")
                .get_version_num(),
            7
        );
        assert_eq!(snapshot["label"], "untitled");
    }

    #[tokio::test]
    async fn execute_sql_insert_into_active_row_does_not_probe_active_head_during_lowering() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(DummyHotStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "missing-branch",
            blob_reader,
            hot_state,
            staged_writes,
            schema_definitions: vec![json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "test_state_schema",
                "columns": [
                    { "name": "id", "type": "text", "nullable": false },
                    { "name": "value", "type": "text", "nullable": false },
                ],
                "primary_key": ["id"],
            })],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO test_state_schema (id, value) VALUES ('row-c', 'C')",
            &[],
        )
        .await
        .expect("lowering should not probe the active head before commit");

        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);
        assert_eq!(
            ctx.staged_writes
                .lock()
                .expect("staged writes lock")
                .deltas
                .len(),
            1,
            "the transaction commit boundary owns active-branch validation"
        );
    }

    #[tokio::test]
    async fn execute_sql_noop_active_row_write_does_not_probe_active_head() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(DummyHotStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "missing-branch",
            blob_reader,
            hot_state,
            staged_writes,
            schema_definitions: vec![json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "test_state_schema",
                "columns": [
                    { "name": "id", "type": "text", "nullable": false },
                    { "name": "value", "type": "text", "nullable": false },
                ],
                "primary_key": ["id"],
            })],
        };

        for sql in [
            "UPDATE test_state_schema SET value = 'D' WHERE false",
            "DELETE FROM test_state_schema WHERE false",
        ] {
            let result = execute_write_sql(&mut ctx, sql, &[])
                .await
                .expect("no-op lowering should not probe the active head");

            assert_eq!(result.rows, vec![vec![Value::Integer(0)]], "{sql}");
        }
        assert!(
            ctx.staged_writes
                .lock()
                .expect("staged writes lock")
                .deltas
                .is_empty(),
            "no-op writes must not create a staged commit"
        );
    }

    #[tokio::test]
    async fn execute_sql_row_upsert_conflict_scan_is_narrowed_to_inserted_identity() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(CapturingRowsHotStateReader {
            rows: vec![
                live_test_state_row(
                    "target",
                    "01920000-0000-7000-8000-0000000000b1",
                    "old",
                    true,
                ),
                live_test_state_row(
                    "other",
                    "01920000-0000-7000-8000-0000000000b1",
                    "skip",
                    true,
                ),
            ],
            requests: Arc::clone(&requests),
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000b1",
            blob_reader,
            hot_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "test_state_schema",
                "columns": [
                    { "name": "id", "type": "text", "nullable": false },
                    { "name": "value", "type": "text", "nullable": false },
                ],
                "primary_key": ["id"],
            })],
        };

        let (result, path) = execute_write_sql_trace(
            &mut ctx,
            "INSERT INTO test_state_schema \
             (id, value, lixcol_untracked) VALUES ('target', 'new', true) \
             ON CONFLICT(id) DO UPDATE SET value = excluded.value",
            &[],
            WriteExecutorMode::Auto,
        )
        .await
        .expect("row upsert should update the matching row");

        assert_eq!(path, WriteExecutorPath::Fast);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);
        let requests = requests.lock().expect("captured requests lock");
        assert_eq!(requests.len(), 1);
        let filter = &requests[0].filter;
        assert_eq!(filter.schema_keys, vec!["test_state_schema"]);
        assert_eq!(filter.row_pks, vec![crate::row_pk::RowPk::single("target")]);
        assert_eq!(
            filter.branch_ids,
            vec!["01920000-0000-7000-8000-0000000000b1"]
        );
        assert_eq!(filter.file_ids, vec![NullableKeyFilter::Null]);
        // V12 has one canonical identity across retention. The probe remains
        // narrowed by schema, PK, branch, and file ID, but must inspect both
        // tracked and untracked rows so an upsert preserves existing retention.
        assert_eq!(filter.untracked, None);
        assert!(!filter.include_tombstones);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "test_state_schema");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_pk, "[\"target\"]");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"id\":\"target\",\"value\":\"new\"}")
        );
    }

    #[tokio::test]
    async fn integer_primary_key_update_and_delete_narrow_candidate_scans() {
        let branch_id = "01920000-0000-7000-8000-0000000000a1";
        let component_types = [crate::row_pk::RowPkComponentType::Integer];
        let row_pk =
            crate::row_pk::RowPk::from_external_parts(vec!["42".to_string()], &component_types)
                .expect("integer fixture identity should encode");

        for (sql, params) in [
            (
                "UPDATE integer_state_schema SET value = $1 WHERE id = $2",
                vec![Value::Text("updated".to_string()), Value::Integer(42)],
            ),
            (
                "DELETE FROM integer_state_schema WHERE id = $1",
                vec![Value::Integer(42)],
            ),
        ] {
            let mut row = live_row("42", branch_id, "before");
            row.row_pk = row_pk.clone();
            row.schema_key = "integer_state_schema".to_string();
            row.snapshot_content = Some(json!({ "id": 42, "value": "before" }).to_string().into());
            let requests = Arc::new(Mutex::new(Vec::new()));
            let mut ctx = DummySqlWriteExecutionContext {
                active_branch_id: branch_id,
                blob_reader: Arc::new(DummyBlobReader),
                hot_state: Arc::new(CapturingRowsHotStateReader {
                    rows: vec![row],
                    requests: Arc::clone(&requests),
                }),
                staged_writes: Arc::new(Mutex::new(CapturingStagedWrites::default())),
                schema_definitions: vec![json!({
                    "$schema": "https://lix.dev/schema-v1.json",
                    "key": "integer_state_schema",
                    "columns": [
                        { "name": "id", "type": "int8", "nullable": false },
                        { "name": "value", "type": "text", "nullable": false },
                    ],
                    "primary_key": ["id"],
                })],
            };

            let (result, path) =
                execute_write_sql_trace(&mut ctx, sql, &params, WriteExecutorMode::Auto)
                    .await
                    .expect("integer point write should execute");

            assert_eq!(path, WriteExecutorPath::Fast, "{sql}");
            assert_eq!(result.rows, vec![vec![Value::Integer(1)]], "{sql}");
            let requests = requests.lock().expect("captured requests lock");
            let [request] = requests.as_slice() else {
                panic!("integer point write should issue one candidate scan: {sql}");
            };
            assert_eq!(request.filter.row_pks, vec![row_pk.clone()], "{sql}");
        }
    }

    #[tokio::test]
    async fn execute_sql_file_path_upsert_uses_indexed_conflict_candidates() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(StaticBlobReader {
            bytes: b"old".to_vec(),
        });
        let hot_state = Arc::new(CapturingRowsHotStateReader {
            rows: vec![
                live_directory_row(
                    "01920000-0000-7000-8000-0000000000d3",
                    "01920000-0000-7000-8000-0000000000a1",
                    None,
                    "docs",
                ),
                live_file_row(
                    "01920000-0000-7000-8000-000000000562",
                    "01920000-0000-7000-8000-0000000000a1",
                    Some("01920000-0000-7000-8000-0000000000d3"),
                    "target.md",
                ),
                live_file_row(
                    "01920000-0000-7000-8000-000000000572",
                    "01920000-0000-7000-8000-0000000000a1",
                    None,
                    "other.md",
                ),
                live_blob_ref_row(
                    "01920000-0000-7000-8000-000000000562",
                    "01920000-0000-7000-8000-0000000000a1",
                    b"old",
                ),
                live_blob_ref_row(
                    "01920000-0000-7000-8000-000000000572",
                    "01920000-0000-7000-8000-0000000000a1",
                    b"skip",
                ),
            ],
            requests: Arc::clone(&requests),
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let (result, path) = execute_write_sql_trace(
            &mut ctx,
            "INSERT INTO lix_file (path, content, lixcol_metadata) \
             VALUES ('/docs/target.md', CAST('new' AS BYTEA), '{\"size\":3}') \
             ON CONFLICT (path) DO UPDATE \
             SET content = excluded.content, lixcol_metadata = excluded.lixcol_metadata",
            &[],
            WriteExecutorMode::ForceDataFusion,
        )
        .await
        .expect("path upsert should update the matching file");

        assert_eq!(path, WriteExecutorPath::DataFusion);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let requests = requests.lock().expect("captured requests lock");
        let topology_scans = requests
            .iter()
            .filter(|request| {
                request.filter.schema_keys
                    == vec![
                        "lix_binary_blob_ref".to_string(),
                        "lix_directory_descriptor".to_string(),
                        "lix_file_descriptor".to_string(),
                    ]
            })
            .count();
        assert_eq!(
            topology_scans, 1,
            "the path conflict index needs one combined topology scan"
        );
        let directory_scans = requests
            .iter()
            .filter(|request| {
                request.filter.schema_keys == vec!["lix_directory_descriptor".to_string()]
                    && request.filter.row_pks.is_empty()
            })
            .count();
        assert_eq!(
            directory_scans, 0,
            "the augmented conflict batch already carries the selected path, so an attribute-only update needs no directory rescan"
        );
        let blob_requests = requests
            .iter()
            .filter(|request| request.filter.schema_keys == vec!["lix_binary_blob_ref".to_string()])
            .collect::<Vec<_>>();
        assert_eq!(
            blob_requests.len(),
            1,
            "the path index carries the conflict probe blob; only conflict apply point-loads it"
        );
        for request in blob_requests {
            assert_eq!(
                request.filter.row_pks,
                vec![
                    crate::row_pk::RowPk::uuid_from_canonical(
                        "01920000-0000-7000-8000-000000000562",
                    )
                    .expect("fixture file ID"),
                ]
            );
            assert_eq!(
                request.filter.file_ids,
                vec![NullableKeyFilter::Value(
                    "01920000-0000-7000-8000-000000000562".to_string(),
                )]
            );
        }
        drop(requests);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let descriptor_rows = overlay.visible_semantic_rows(false, "lix_file_descriptor");
        assert_eq!(descriptor_rows.len(), 1);
        assert_eq!(
            descriptor_rows[0].row_pk,
            "[\"01920000-0000-7000-8000-000000000562\"]"
        );
        let descriptor: JsonValue = serde_json::from_str(
            descriptor_rows[0]
                .snapshot_content
                .as_deref()
                .expect("descriptor should carry a snapshot"),
        )
        .expect("descriptor snapshot JSON");
        assert_eq!(descriptor["id"], "01920000-0000-7000-8000-000000000562");
        assert_eq!(
            descriptor["directory_id"],
            "01920000-0000-7000-8000-0000000000d3"
        );
        assert_eq!(descriptor["name"], "target.md");
        assert_eq!(descriptor_rows[0].metadata.as_deref(), Some("{\"size\":3}"));
        let blob_ref_rows = overlay.visible_semantic_rows(false, "lix_binary_blob_ref");
        assert_eq!(blob_ref_rows.len(), 1);
        assert_eq!(
            blob_ref_rows[0].row_pk,
            "[\"01920000-0000-7000-8000-000000000562\"]"
        );
        let blob_ref: JsonValue = serde_json::from_str(
            blob_ref_rows[0]
                .snapshot_content
                .as_deref()
                .expect("blob ref should carry a snapshot"),
        )
        .expect("blob ref snapshot JSON");
        assert_eq!(blob_ref["size_bytes"], 3);
        assert_eq!(
            blob_ref["blob_hash"],
            crate::binary_cas::BlobId::from_content(b"new").to_hex()
        );
    }

    #[tokio::test]
    async fn execute_sql_insert_into_active_directory_defaults_active_branch() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(DummyHotStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_directory (id, parent_id, name) \
             VALUES ('01920000-0000-7000-8000-0000000000d3', NULL, 'docs')",
            &[],
        )
        .await
        .expect("INSERT INTO lix_directory should stage write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_directory_descriptor");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_pk, "[\"01920000-0000-7000-8000-0000000000d3\"]");
        assert_eq!(rows[0].branch_id, "01920000-0000-7000-8000-0000000000a1");
        assert!(!rows[0].global);
        assert!(!rows[0].untracked);
    }

    #[tokio::test]
    async fn execute_sql_update_directory_stages_rewritten_descriptor() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(RowsHotStateReader {
            rows: vec![
                live_directory_row(
                    "01920000-0000-7000-8000-0000000000d3",
                    "01920000-0000-7000-8000-0000000000a1",
                    None,
                    "docs",
                ),
                live_directory_row(
                    "01920000-0000-7000-8000-000000000313",
                    "01920000-0000-7000-8000-0000000000a1",
                    Some("01920000-0000-7000-8000-0000000000d3"),
                    "guides",
                ),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "UPDATE lix_directory \
             SET name = 'docs-updated', lixcol_metadata = '{\"source\":\"directory-update\"}' \
             WHERE id = '01920000-0000-7000-8000-0000000000d3'",
            &[],
        )
        .await
        .expect("UPDATE lix_directory should stage rewritten descriptor");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_directory_descriptor");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_pk, "[\"01920000-0000-7000-8000-0000000000d3\"]");
        assert_eq!(rows[0].branch_id, "01920000-0000-7000-8000-0000000000a1");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some(
                "{\"id\":\"01920000-0000-7000-8000-0000000000d3\",\"name\":\"docs-updated\",\"parent_id\":null}"
            )
        );
        assert_eq!(
            rows[0].metadata.as_deref(),
            Some("{\"source\":\"directory-update\"}")
        );
    }

    #[tokio::test]
    async fn execute_sql_update_directory_stages_path_assignment() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(RowsHotStateReader {
            rows: vec![live_directory_row(
                "01920000-0000-7000-8000-0000000000d3",
                "01920000-0000-7000-8000-0000000000a1",
                None,
                "docs",
            )],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "UPDATE lix_directory SET path = '/renamed' WHERE id = '01920000-0000-7000-8000-0000000000d3'",
            &[],
        )
        .await
        .expect("path update should stage descriptor rewrite");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_directory_descriptor");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_pk, "[\"01920000-0000-7000-8000-0000000000d3\"]");
        assert_eq!(rows[0].branch_id, "01920000-0000-7000-8000-0000000000a1");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some(
                "{\"id\":\"01920000-0000-7000-8000-0000000000d3\",\"name\":\"renamed\",\"parent_id\":null}"
            )
        );
    }

    #[tokio::test]
    async fn execute_sql_insert_into_active_file_defaults_active_branch() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(DummyHotStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_file (id, directory_id, name) \
             VALUES ('01920000-0000-7000-8000-0000000000d2', '01920000-0000-7000-8000-0000000000d3', 'readme.md')",
            &[],
        )
        .await
        .expect("INSERT INTO lix_file should stage descriptor write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_file_descriptor");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_pk, "[\"01920000-0000-7000-8000-0000000000d2\"]");
        assert_eq!(rows[0].branch_id, "01920000-0000-7000-8000-0000000000a1");
        assert!(!rows[0].global);
        assert!(!rows[0].untracked);
    }

    #[tokio::test]
    async fn execute_sql_insert_into_file_with_data_stages_blob_ref() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(RowsHotStateReader {
            rows: vec![live_directory_row(
                "01920000-0000-7000-8000-0000000000d3",
                "01920000-0000-7000-8000-0000000000a1",
                None,
                "docs",
            )],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_file (\
             id, directory_id, name, content\
             ) VALUES ('01920000-0000-7000-8000-0000000000d2', '01920000-0000-7000-8000-0000000000d3', 'readme.md', CAST('AB' AS BYTEA))",
            &[],
        )
        .await
        .expect("INSERT INTO lix_file should stage descriptor and content writes");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let descriptor_rows = overlay.visible_semantic_rows(false, "lix_file_descriptor");
        assert_eq!(descriptor_rows.len(), 1);
        assert_eq!(
            descriptor_rows[0].row_pk,
            "[\"01920000-0000-7000-8000-0000000000d2\"]"
        );
        let blob_ref_rows = overlay.visible_semantic_rows(false, "lix_binary_blob_ref");
        assert_eq!(blob_ref_rows.len(), 1);
        assert_eq!(
            blob_ref_rows[0].row_pk,
            "[\"01920000-0000-7000-8000-0000000000d2\"]"
        );
        assert_eq!(
            blob_ref_rows[0].file_id.as_deref(),
            Some("01920000-0000-7000-8000-0000000000d2")
        );
        assert_eq!(
            blob_ref_rows[0].branch_id,
            "01920000-0000-7000-8000-0000000000a1"
        );
        let snapshot: JsonValue =
            serde_json::from_str(blob_ref_rows[0].snapshot_content.as_deref().unwrap())
                .expect("blob ref snapshot JSON");
        assert_eq!(snapshot["id"], "01920000-0000-7000-8000-0000000000d2");
        assert_eq!(snapshot["size_bytes"], 2);
        assert!(
            snapshot["blob_hash"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
        );
    }

    #[tokio::test]
    async fn execute_sql_multi_row_lix_file_path_data_uses_one_fast_stage() {
        let (mut ctx, staged_writes, scans) = counting_write_context(vec![]);

        let (result, path) = execute_write_sql_trace(
            &mut ctx,
            "INSERT INTO lix_file (path, content) \
             VALUES ('/multi/a.md', CAST('a' AS BYTEA)), ('/multi/b.md', CAST('b' AS BYTEA))",
            &[],
            WriteExecutorMode::ForceFast,
        )
        .await
        .expect("multi-row path/data insert should use the fast writer");

        assert_eq!(path, WriteExecutorPath::Fast);
        assert_eq!(result.rows, vec![vec![Value::Integer(2)]]);
        assert_eq!(scans.load(Ordering::SeqCst), 1);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let descriptor_rows = overlay.visible_semantic_rows(false, "lix_file_descriptor");
        assert_eq!(descriptor_names(&descriptor_rows), vec!["a.md", "b.md"]);
        let blob_ref_rows = overlay.visible_semantic_rows(false, "lix_binary_blob_ref");
        assert_eq!(blob_ref_rows.len(), 2);
    }

    #[tokio::test]
    async fn execute_sql_multi_row_lix_file_path_data_params_use_fast_stage() {
        let (mut ctx, staged_writes, scans) = counting_write_context(vec![]);

        let (result, path) = execute_write_sql_trace(
            &mut ctx,
            "INSERT INTO lix_file (path, content) VALUES ($1, $2), ($3, $4)",
            &[
                Value::Text("/multi/param-a.md".to_string()),
                Value::Blob(b"param-a".to_vec().into()),
                Value::Text("/multi/param-b.md".to_string()),
                Value::Blob(b"param-b".to_vec().into()),
            ],
            WriteExecutorMode::ForceFast,
        )
        .await
        .expect("parameterized multi-row path/data insert should use the fast writer");

        assert_eq!(path, WriteExecutorPath::Fast);
        assert_eq!(result.rows, vec![vec![Value::Integer(2)]]);
        assert_eq!(scans.load(Ordering::SeqCst), 1);
        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
    }

    #[tokio::test]
    async fn execute_sql_multi_row_lix_file_path_data_metadata_params_use_fast_stage() {
        let (mut ctx, staged_writes, scans) = counting_write_context(vec![]);

        let (result, path) = execute_write_sql_trace(
            &mut ctx,
            "INSERT INTO lix_file (path, content, lixcol_metadata) \
             VALUES ($1, $2, $3), ($4, $5, $6)",
            &[
                Value::Text("/multi/param-a.md".to_string()),
                Value::Blob(b"param-a".to_vec().into()),
                Value::Jsonb(json!({"source": "json-param"}).into()),
                Value::Text("/multi/param-b.md".to_string()),
                Value::Blob(b"param-b".to_vec().into()),
                Value::Text(r#"{"source":"text-param"}"#.to_string()),
            ],
            WriteExecutorMode::ForceFast,
        )
        .await
        .expect("parameterized path/data/metadata insert should use the fast writer");

        assert_eq!(path, WriteExecutorPath::Fast);
        assert_eq!(result.rows, vec![vec![Value::Integer(2)]]);
        assert_eq!(scans.load(Ordering::SeqCst), 1);
        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let mut descriptor_metadata = overlay
            .visible_semantic_rows(false, "lix_file_descriptor")
            .into_iter()
            .filter_map(|row| row.metadata)
            .collect::<Vec<_>>();
        descriptor_metadata.sort();
        assert_eq!(
            descriptor_metadata,
            vec![
                r#"{"source":"json-param"}"#.to_string(),
                r#"{"source":"text-param"}"#.to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn execute_sql_lix_file_metadata_upsert_fast_path_matches_datafusion() {
        let mut existing = live_file_row(
            "01920000-0000-7000-8000-000000000322",
            "01920000-0000-7000-8000-0000000000a1",
            Some("01920000-0000-7000-8000-0000000000d3"),
            "existing.md",
        );
        existing.metadata = Some(r#"{"source":"old"}"#.into());
        let rows = vec![
            live_directory_row(
                "01920000-0000-7000-8000-0000000000d3",
                "01920000-0000-7000-8000-0000000000a1",
                None,
                "docs",
            ),
            existing,
            live_blob_ref_row(
                "01920000-0000-7000-8000-000000000322",
                "01920000-0000-7000-8000-0000000000a1",
                b"old",
            ),
        ];
        let fast_blob_reader: Arc<dyn BlobDataReader> = Arc::new(StaticBlobReader {
            bytes: b"old".to_vec(),
        });
        let datafusion_blob_reader: Arc<dyn BlobDataReader> = Arc::new(StaticBlobReader {
            bytes: b"old".to_vec(),
        });
        let (mut fast_ctx, fast_staged, fast_scans) =
            counting_write_context_with_blob_reader(rows.clone(), fast_blob_reader);
        let (mut datafusion_ctx, datafusion_staged, datafusion_scans) =
            counting_write_context_with_blob_reader(rows, datafusion_blob_reader);
        let sql = "INSERT INTO lix_file (path, content, lixcol_metadata) VALUES ($1, $2, $3) \
                   ON CONFLICT (path) DO UPDATE SET content = excluded.content, \
                   lixcol_metadata = excluded.lixcol_metadata";
        let params = [
            Value::Text("/docs/existing.md".to_string()),
            Value::Blob(b"updated".to_vec().into()),
            Value::Jsonb(json!({"source": "upload"}).into()),
        ];

        let (fast_result, fast_path) =
            execute_write_sql_trace(&mut fast_ctx, sql, &params, WriteExecutorMode::ForceFast)
                .await
                .expect("metadata upsert should use the bound fast path");
        let (datafusion_result, datafusion_path) = execute_write_sql_trace(
            &mut datafusion_ctx,
            sql,
            &params,
            WriteExecutorMode::ForceDataFusion,
        )
        .await
        .expect("reference metadata upsert should succeed");

        assert_eq!(fast_path, WriteExecutorPath::Fast);
        assert_eq!(datafusion_path, WriteExecutorPath::DataFusion);
        assert_eq!(fast_result.rows, datafusion_result.rows);
        // Both routes now receive the correlated blob ref from the path index;
        // neither repeats the exact live-state load.
        assert_eq!(fast_scans.load(Ordering::SeqCst), 2);
        assert_eq!(datafusion_scans.load(Ordering::SeqCst), 2);

        let fast_rows = fast_staged.lock().expect("fast writes lock").deltas[0]
            .pending_write_overlay()
            .expect("fast staged delta should project")
            .visible_all_semantic_rows();
        let datafusion_rows = datafusion_staged
            .lock()
            .expect("DataFusion writes lock")
            .deltas[0]
            .pending_write_overlay()
            .expect("DataFusion staged delta should project")
            .visible_all_semantic_rows();
        assert_eq!(fast_rows, datafusion_rows);
        let descriptor = fast_rows
            .iter()
            .find(|row| row.schema_key == "lix_file_descriptor")
            .expect("metadata upsert should rewrite the descriptor");
        assert_eq!(
            descriptor.metadata.as_deref(),
            Some(r#"{"source":"upload"}"#)
        );
        assert_eq!(
            descriptor.file_id.as_deref(),
            Some("01920000-0000-7000-8000-000000000322")
        );
        let snapshot: JsonValue = serde_json::from_str(
            descriptor
                .snapshot_content
                .as_deref()
                .expect("descriptor snapshot"),
        )
        .expect("descriptor snapshot JSON");
        assert_eq!(
            snapshot["directory_id"],
            "01920000-0000-7000-8000-0000000000d3"
        );
        assert_eq!(snapshot["name"], "existing.md");
    }

    #[tokio::test]
    async fn execute_sql_lix_file_metadata_fast_path_validates_before_staging() {
        let (mut ctx, staged_writes, scans) = counting_write_context(vec![]);

        let error = execute_write_sql_trace(
            &mut ctx,
            "INSERT INTO lix_file (path, content, lixcol_metadata) VALUES ($1, $2, $3)",
            &[
                Value::Text("/invalid.md".to_string()),
                Value::Blob(b"content".to_vec().into()),
                Value::Jsonb(json!(["not", "an", "object"]).into()),
            ],
            WriteExecutorMode::ForceFast,
        )
        .await
        .expect_err("non-object metadata should fail before the fast writer scans or stages");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert_eq!(scans.load(Ordering::SeqCst), 0);
        assert!(
            staged_writes
                .lock()
                .expect("staged writes lock")
                .deltas
                .is_empty()
        );
    }

    #[tokio::test]
    async fn bound_lix_file_metadata_do_nothing_stays_on_datafusion() {
        let (mut ctx, _, _) = counting_write_context(Vec::new());
        let sql = "INSERT INTO lix_file (path, content, lixcol_metadata) \
                   VALUES ($1, $2, $3) ON CONFLICT (path) DO NOTHING";
        let plan = create_write_logical_plan(&mut ctx, sql)
            .await
            .expect("metadata DO NOTHING should plan");
        let crate::sql2::exec::SqlLogicalPlan::Write(plan) = plan else {
            panic!("metadata DO NOTHING should produce a write plan");
        };

        assert!(
            !crate::sql2::exec::bound_public_write::supports_bound_public_write(&plan.plan),
            "metadata DO NOTHING must preserve DataFusion's skipped-row validation semantics"
        );
    }

    #[tokio::test]
    async fn execute_sql_multi_row_lix_file_do_nothing_validates_and_skips_existing() {
        let (mut ctx, staged_writes, scans) = counting_write_context(vec![
            live_file_row(
                "01920000-0000-7000-8000-000000000322",
                "01920000-0000-7000-8000-0000000000a1",
                None,
                "existing.md",
            ),
            live_blob_ref_row(
                "01920000-0000-7000-8000-000000000322",
                "01920000-0000-7000-8000-0000000000a1",
                b"old",
            ),
        ]);

        let (result, path) = execute_write_sql_trace(
            &mut ctx,
            "INSERT INTO lix_file (path, content) \
             VALUES ('/existing.md', CAST('new' AS BYTEA)), ('/fresh.md', CAST('fresh' AS BYTEA)) \
             ON CONFLICT (path) DO NOTHING",
            &[],
            WriteExecutorMode::ForceFast,
        )
        .await
        .expect("multi-row DO NOTHING should use the fast writer");

        assert_eq!(path, WriteExecutorPath::Fast);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);
        assert_eq!(scans.load(Ordering::SeqCst), 1);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let descriptor_rows = overlay.visible_semantic_rows(false, "lix_file_descriptor");
        assert_eq!(descriptor_names(&descriptor_rows), vec!["fresh.md"]);
        let blob_ref_rows = overlay.visible_semantic_rows(false, "lix_binary_blob_ref");
        assert_eq!(blob_ref_rows.len(), 1);
    }

    #[tokio::test]
    async fn execute_sql_multi_row_lix_file_update_existing_and_insert_fresh() {
        let (mut ctx, staged_writes, scans) = counting_write_context(vec![
            live_file_row(
                "01920000-0000-7000-8000-000000000322",
                "01920000-0000-7000-8000-0000000000a1",
                None,
                "existing.md",
            ),
            live_blob_ref_row(
                "01920000-0000-7000-8000-000000000322",
                "01920000-0000-7000-8000-0000000000a1",
                b"old",
            ),
        ]);

        let (result, path) = execute_write_sql_trace(
            &mut ctx,
            "INSERT INTO lix_file (path, content) \
             VALUES ('/existing.md', CAST('new' AS BYTEA)), ('/fresh.md', CAST('fresh' AS BYTEA)) \
             ON CONFLICT (path) DO UPDATE SET content = excluded.content",
            &[],
            WriteExecutorMode::ForceFast,
        )
        .await
        .expect("multi-row DO UPDATE should use the fast writer");

        assert_eq!(path, WriteExecutorPath::Fast);
        assert_eq!(result.rows, vec![vec![Value::Integer(2)]]);
        // The indexed route probes the path index before declining to the
        // generic mixed existing/new batch scan.
        assert_eq!(scans.load(Ordering::SeqCst), 2);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let descriptor_rows = overlay.visible_semantic_rows(false, "lix_file_descriptor");
        assert_eq!(descriptor_names(&descriptor_rows), vec!["fresh.md"]);
        let blob_ref_rows = overlay.visible_semantic_rows(false, "lix_binary_blob_ref");
        assert_eq!(blob_ref_rows.len(), 2);
        assert!(
            blob_ref_rows
                .iter()
                .any(|row| row.row_pk == "[\"01920000-0000-7000-8000-000000000322\"]")
        );
    }

    #[tokio::test]
    async fn execute_sql_multi_row_lix_file_id_upsert_uses_fast_writer() {
        let rows = vec![
            live_file_row(
                "01920000-0000-7000-8000-000000000322",
                "01920000-0000-7000-8000-0000000000a1",
                None,
                "existing.md",
            ),
            live_blob_ref_row(
                "01920000-0000-7000-8000-000000000322",
                "01920000-0000-7000-8000-0000000000a1",
                b"old",
            ),
        ];
        let (mut fast_ctx, fast_staged, _) = counting_write_context(rows);
        let sql = "INSERT INTO lix_file (id, path, content, lixcol_metadata) VALUES \
            ('01920000-0000-7000-8000-000000000322', '/ignored.md', CAST('new' AS BYTEA), '{\"source\":\"update\"}'), \
            ('01920000-0000-7000-8000-000000000323', '/fresh.md', CAST('fresh' AS BYTEA), '{\"source\":\"insert\"}') \
            ON CONFLICT (id) DO UPDATE SET content = excluded.content, lixcol_metadata = excluded.lixcol_metadata";

        let (fast_result, fast_path) =
            execute_write_sql_trace(&mut fast_ctx, sql, &[], WriteExecutorMode::ForceFast)
                .await
                .expect("ID upsert should use the fast writer");
        assert_eq!(fast_path, WriteExecutorPath::Fast);
        assert_eq!(fast_result.rows, vec![vec![Value::Integer(2)]]);
        let fast_rows = fast_staged.lock().expect("fast writes lock").deltas[0]
            .pending_write_overlay()
            .expect("fast staged delta should project")
            .visible_all_semantic_rows();
        let descriptor_rows = fast_rows
            .iter()
            .filter(|row| row.schema_key == "lix_file_descriptor")
            .collect::<Vec<_>>();
        assert!(descriptor_rows.iter().any(|row| {
            row.row_pk == "[\"01920000-0000-7000-8000-000000000322\"]"
                && row
                    .snapshot_content
                    .as_deref()
                    .is_some_and(|snapshot| snapshot.contains("existing.md"))
        }));
    }

    #[tokio::test]
    async fn execute_sql_lix_file_id_upsert_rejects_path_collision_on_id_miss() {
        let rows = vec![live_file_row(
            "01920000-0000-7000-8000-000000000322",
            "01920000-0000-7000-8000-0000000000a1",
            None,
            "existing.md",
        )];
        let (mut ctx, staged_writes, _) = counting_write_context(rows);
        let error = execute_write_sql_trace(
            &mut ctx,
            "INSERT INTO lix_file (id, path, content) VALUES \
             ('01920000-0000-7000-8000-000000000323', '/existing.md', CAST('new' AS BYTEA)) \
             ON CONFLICT (id) DO UPDATE SET content = excluded.content",
            &[],
            WriteExecutorMode::ForceFast,
        )
        .await
        .expect_err("an ID miss must still enforce path uniqueness");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert!(
            staged_writes
                .lock()
                .expect("staged writes lock")
                .deltas
                .is_empty()
        );
    }

    #[tokio::test]
    async fn query_upsert_duplicate_paths_validate_every_source_batch() {
        for table in ["lix_file", "lix_directory"] {
            for returning in ["", " RETURNING id"] {
                for (existing, opposite_lane, expected) in [
                    (false, false, LixError::CODE_UNIQUE),
                    (false, true, LixError::CODE_UNIQUE),
                    (true, true, LixError::CODE_CONSTRAINT_VIOLATION),
                ] {
                    let rows = if existing {
                        vec![if table == "lix_file" {
                            live_file_row(
                                "01920000-0000-7000-8000-000000000322",
                                "01920000-0000-7000-8000-0000000000a1",
                                None,
                                "dupe",
                            )
                        } else {
                            live_directory_row(
                                "01920000-0000-7000-8000-000000000322",
                                "01920000-0000-7000-8000-0000000000a1",
                                None,
                                "dupe",
                            )
                        }]
                    } else {
                        vec![]
                    };
                    let (mut ctx, staged, _) = counting_write_context(rows);
                    let sql = format!(
                        "INSERT INTO {table} (path,lixcol_untracked) (SELECT '/fresh',false UNION ALL SELECT '/dupe',false UNION ALL SELECT '/dupe',{opposite_lane}) ON CONFLICT (path) DO NOTHING{returning}"
                    );
                    crate::sql2::providers::take_upsert_source_batches();
                    let error = execute_write_sql_trace(
                        &mut ctx,
                        &sql,
                        &[],
                        WriteExecutorMode::ForceDataFusion,
                    )
                    .await
                    .expect_err("duplicate missing paths and lane collisions must fail");
                    assert_eq!(error.code, expected, "{sql}: {error}");
                    assert!(
                        crate::sql2::providers::take_upsert_source_batches() >= 3,
                        "test must consume separate source batches: {sql}"
                    );
                    assert!(
                        staged.lock().unwrap().deltas.is_empty(),
                        "late failure must not stage the fresh prefix: {sql}"
                    );
                }
                let rows = vec![if table == "lix_file" {
                    live_file_row(
                        "01920000-0000-7000-8000-000000000322",
                        "01920000-0000-7000-8000-0000000000a1",
                        None,
                        "dupe",
                    )
                } else {
                    live_directory_row(
                        "01920000-0000-7000-8000-000000000322",
                        "01920000-0000-7000-8000-0000000000a1",
                        None,
                        "dupe",
                    )
                }];
                let (mut ctx, staged, _) = counting_write_context(rows);
                let sql = format!(
                    "INSERT INTO {table} (path) (SELECT '/dupe' UNION ALL SELECT '/dupe') ON CONFLICT (path) DO NOTHING{returning}"
                );
                crate::sql2::providers::take_upsert_source_batches();
                execute_write_sql_trace(&mut ctx, &sql, &[], WriteExecutorMode::ForceDataFusion)
                    .await
                    .expect("same-lane existing duplicates remain valid DO NOTHING");
                assert!(
                    crate::sql2::providers::take_upsert_source_batches() >= 2,
                    "expected separate source batches"
                );
                assert!(staged.lock().unwrap().deltas.is_empty());
            }
        }
    }

    #[tokio::test]
    async fn execute_sql_multi_row_lix_file_duplicate_insert_paths_reject_before_staging() {
        for sql in [
            "INSERT INTO lix_file (path, content) \
             VALUES ('/dupe.md', CAST('a' AS BYTEA)), ('/dupe.md', CAST('b' AS BYTEA))",
            "INSERT INTO lix_file (path, content) \
             VALUES ('/dupe.md', CAST('a' AS BYTEA)), ('/dupe.md', CAST('b' AS BYTEA)) \
             ON CONFLICT (path) DO NOTHING",
            "INSERT INTO lix_file (path, content) \
             VALUES ('/dupe.md', CAST('a' AS BYTEA)), ('/dupe.md', CAST('b' AS BYTEA)) \
             ON CONFLICT (path) DO UPDATE SET content = excluded.content",
        ] {
            let (mut ctx, staged_writes, scans) = counting_write_context(vec![]);

            let error = execute_write_sql_trace(&mut ctx, sql, &[], WriteExecutorMode::ForceFast)
                .await
                .expect_err("duplicate VALUES paths should fail");

            assert_eq!(error.code, LixError::CODE_UNIQUE, "{sql}");
            // Existing and missing path conflicts are both resolved from the
            // indexed route without a second generic scan.
            assert_eq!(scans.load(Ordering::SeqCst), 1, "{sql}");
            assert!(
                staged_writes
                    .lock()
                    .expect("staged writes lock")
                    .deltas
                    .is_empty(),
                "{sql}"
            );
        }
    }

    #[tokio::test]
    async fn execute_sql_multi_row_lix_file_duplicate_existing_do_nothing_skips_all() {
        let (mut ctx, staged_writes, scans) = counting_write_context(vec![
            live_file_row(
                "01920000-0000-7000-8000-000000000322",
                "01920000-0000-7000-8000-0000000000a1",
                None,
                "existing.md",
            ),
            live_blob_ref_row(
                "01920000-0000-7000-8000-000000000322",
                "01920000-0000-7000-8000-0000000000a1",
                b"old",
            ),
        ]);

        let (result, path) = execute_write_sql_trace(
            &mut ctx,
            "INSERT INTO lix_file (path, content) \
             VALUES ('/existing.md', CAST('a' AS BYTEA)), ('/existing.md', CAST('b' AS BYTEA)) \
             ON CONFLICT (path) DO NOTHING",
            &[],
            WriteExecutorMode::ForceFast,
        )
        .await
        .expect("duplicate existing paths should follow DO NOTHING");

        assert_eq!(path, WriteExecutorPath::Fast);
        assert_eq!(result.rows, vec![vec![Value::Integer(0)]]);
        assert_eq!(scans.load(Ordering::SeqCst), 1);
        assert!(
            staged_writes
                .lock()
                .expect("staged writes lock")
                .deltas
                .is_empty()
        );
    }

    #[tokio::test]
    async fn execute_sql_multi_row_lix_file_namespace_conflict_leaves_no_stage() {
        let (mut ctx, staged_writes, scans) = counting_write_context(vec![]);

        let error = execute_write_sql_trace(
            &mut ctx,
            "INSERT INTO lix_file (path, content) \
             VALUES ('/folder', CAST('a' AS BYTEA)), ('/folder/file.md', CAST('b' AS BYTEA))",
            &[],
            WriteExecutorMode::ForceFast,
        )
        .await
        .expect_err("batch should reject file/directory namespace conflicts");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert_eq!(scans.load(Ordering::SeqCst), 1);
        assert!(
            staged_writes
                .lock()
                .expect("staged writes lock")
                .deltas
                .is_empty()
        );
    }

    #[tokio::test]
    async fn execute_sql_multi_row_lix_file_invalid_later_row_leaves_no_stage() {
        let (mut ctx, staged_writes, scans) = counting_write_context(vec![]);

        execute_write_sql_trace(
            &mut ctx,
            "INSERT INTO lix_file (path, content) \
             VALUES ('/ok.md', CAST('ok' AS BYTEA)), ('relative.md', CAST('bad' AS BYTEA))",
            &[],
            WriteExecutorMode::ForceFast,
        )
        .await
        .expect_err("invalid later path should fail before staging");

        assert_eq!(scans.load(Ordering::SeqCst), 0);
        assert!(
            staged_writes
                .lock()
                .expect("staged writes lock")
                .deltas
                .is_empty()
        );
    }

    #[tokio::test]
    async fn execute_sql_multi_row_lix_file_bad_data_param_leaves_no_stage() {
        let (mut ctx, staged_writes, scans) = counting_write_context(vec![]);

        let error = execute_write_sql_trace(
            &mut ctx,
            "INSERT INTO lix_file (path, content) VALUES ($1, $2), ($3, $4)",
            &[
                Value::Text("/ok.md".to_string()),
                Value::Blob(b"ok".to_vec().into()),
                Value::Text("/bad.md".to_string()),
                Value::Text("not a blob".to_string()),
            ],
            WriteExecutorMode::ForceFast,
        )
        .await
        .expect_err("wrong data param type should fail before staging");

        assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);
        assert_eq!(scans.load(Ordering::SeqCst), 0);
        assert!(
            staged_writes
                .lock()
                .expect("staged writes lock")
                .deltas
                .is_empty()
        );
    }

    #[tokio::test]
    async fn execute_sql_multi_row_lix_file_do_nothing_rejects_untracked_collision() {
        let (mut ctx, staged_writes, scans) =
            counting_write_context(vec![mark_untracked(live_file_row(
                "01920000-0000-7000-8000-000000000132",
                "01920000-0000-7000-8000-0000000000a1",
                None,
                "untracked.md",
            ))]);

        let error = execute_write_sql_trace(
            &mut ctx,
            "INSERT INTO lix_file (path, content) \
             VALUES ('/untracked.md', CAST('new' AS BYTEA)), ('/fresh.md', CAST('fresh' AS BYTEA)) \
             ON CONFLICT (path) DO NOTHING",
            &[],
            WriteExecutorMode::ForceFast,
        )
        .await
        .expect_err("DO NOTHING should still reject tracked/untracked conflicts");

        assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
        assert_eq!(scans.load(Ordering::SeqCst), 1);
        assert!(
            staged_writes
                .lock()
                .expect("staged writes lock")
                .deltas
                .is_empty()
        );
    }

    #[tokio::test]
    async fn execute_sql_multi_row_lix_file_id_path_data_uses_fast_shape() {
        let (mut ctx, staged_writes, scans) = counting_write_context(vec![]);

        let (result, path) = execute_write_sql_trace(
            &mut ctx,
            "INSERT INTO lix_file (id, path, content) \
             VALUES ('01920000-0000-7000-8000-0000000000a2', '/a.md', CAST('a' AS BYTEA)), ('01920000-0000-7000-8000-0000000000b2', '/b.md', CAST('b' AS BYTEA))",
            &[],
            WriteExecutorMode::ForceFast,
        )
        .await
        .expect("id/path/data should use the capability-based file fast path");

        assert_eq!(path, WriteExecutorPath::Fast);
        assert_eq!(result.rows, vec![vec![Value::Integer(2)]]);
        assert_eq!(scans.load(Ordering::SeqCst), 1);
        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let mut ids = overlay
            .visible_semantic_rows(false, "lix_file_descriptor")
            .into_iter()
            .map(|row| row.row_pk)
            .collect::<Vec<_>>();
        ids.sort();
        assert_eq!(
            ids,
            vec![
                "[\"01920000-0000-7000-8000-0000000000a2\"]",
                "[\"01920000-0000-7000-8000-0000000000b2\"]"
            ]
        );
    }

    #[tokio::test]
    async fn execute_sql_lix_file_id_path_content_metadata_uses_fast_shape() {
        let (mut ctx, staged_writes, scans) = counting_write_context(vec![]);

        let (result, path) = execute_write_sql_trace(
            &mut ctx,
            "INSERT INTO lix_file (id, path, content, lixcol_metadata) \
             VALUES ('01920000-0000-7000-8000-0000000000a2', '/a.md', CAST('a' AS BYTEA), '{\"source\":\"test\"}')",
            &[],
            WriteExecutorMode::ForceFast,
        )
        .await
        .expect("id/path/data/metadata should use the capability-based file fast path");

        assert_eq!(path, WriteExecutorPath::Fast);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);
        assert_eq!(scans.load(Ordering::SeqCst), 1);
        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let descriptor_rows = overlay.visible_semantic_rows(false, "lix_file_descriptor");
        assert_eq!(descriptor_rows.len(), 1);
        assert_eq!(
            descriptor_rows[0].row_pk,
            "[\"01920000-0000-7000-8000-0000000000a2\"]"
        );
        assert_eq!(
            descriptor_rows[0].metadata.as_deref(),
            Some(r#"{"source":"test"}"#)
        );
    }

    #[tokio::test]
    async fn execute_sql_update_file_stages_rewritten_descriptor() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(RowsHotStateReader {
            rows: vec![
                live_directory_row(
                    "01920000-0000-7000-8000-0000000000d3",
                    "01920000-0000-7000-8000-0000000000a1",
                    None,
                    "docs",
                ),
                live_file_row(
                    "01920000-0000-7000-8000-0000000000d2",
                    "01920000-0000-7000-8000-0000000000a1",
                    Some("01920000-0000-7000-8000-0000000000d3"),
                    "readme.md",
                ),
                live_file_row(
                    "01920000-0000-7000-8000-000000000332",
                    "01920000-0000-7000-8000-0000000000a1",
                    Some("01920000-0000-7000-8000-0000000000d3"),
                    "guide.md",
                ),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "UPDATE lix_file \
             SET name = 'readme-updated.txt', lixcol_metadata = '{\"source\":\"file-update\"}' \
             WHERE id = '01920000-0000-7000-8000-0000000000d2'",
            &[],
        )
        .await
        .expect("UPDATE lix_file should stage rewritten descriptor");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_file_descriptor");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_pk, "[\"01920000-0000-7000-8000-0000000000d2\"]");
        assert_eq!(rows[0].branch_id, "01920000-0000-7000-8000-0000000000a1");
        let snapshot: JsonValue =
            serde_json::from_str(rows[0].snapshot_content.as_deref().unwrap())
                .expect("descriptor snapshot JSON");
        assert_eq!(snapshot["id"], "01920000-0000-7000-8000-0000000000d2");
        assert_eq!(
            snapshot["directory_id"],
            "01920000-0000-7000-8000-0000000000d3"
        );
        assert_eq!(snapshot["name"], "readme-updated.txt");
        assert_eq!(
            rows[0].metadata.as_deref(),
            Some("{\"source\":\"file-update\"}")
        );
    }

    #[tokio::test]
    async fn execute_sql_file_content_update_by_id_fast_path_matches_datafusion() {
        let rows = vec![
            live_directory_row(
                "01920000-0000-7000-8000-0000000000d3",
                "01920000-0000-7000-8000-0000000000a1",
                None,
                "docs",
            ),
            live_file_row(
                "01920000-0000-7000-8000-0000000000d2",
                "01920000-0000-7000-8000-0000000000a1",
                Some("01920000-0000-7000-8000-0000000000d3"),
                "readme.md",
            ),
            live_blob_ref_row(
                "01920000-0000-7000-8000-0000000000d2",
                "01920000-0000-7000-8000-0000000000a1",
                b"old",
            ),
        ];
        let (mut fast_ctx, fast_staged, fast_scans) = counting_write_context(rows.clone());
        let (mut datafusion_ctx, datafusion_staged, datafusion_scans) =
            counting_write_context(rows);
        let sql = "UPDATE lix_file SET content = CAST('AB' AS BYTEA) WHERE id = '01920000-0000-7000-8000-0000000000d2'";

        let (fast_result, fast_path) =
            execute_write_sql_trace(&mut fast_ctx, sql, &[], WriteExecutorMode::ForceFast)
                .await
                .expect("file data update should use the bound fast path");
        let (datafusion_result, datafusion_path) = execute_write_sql_trace(
            &mut datafusion_ctx,
            sql,
            &[],
            WriteExecutorMode::ForceDataFusion,
        )
        .await
        .expect("reference file data update should succeed");

        assert_eq!(fast_path, WriteExecutorPath::Fast);
        assert_eq!(datafusion_path, WriteExecutorPath::DataFusion);
        assert_eq!(fast_result.rows, datafusion_result.rows);
        assert_eq!(fast_scans.load(Ordering::SeqCst), 2);
        assert_eq!(datafusion_scans.load(Ordering::SeqCst), 2);

        let fast_rows = fast_staged.lock().expect("fast writes lock").deltas[0]
            .pending_write_overlay()
            .expect("fast staged delta should project")
            .visible_all_semantic_rows();
        let datafusion_rows = datafusion_staged
            .lock()
            .expect("DataFusion writes lock")
            .deltas[0]
            .pending_write_overlay()
            .expect("DataFusion staged delta should project")
            .visible_all_semantic_rows();
        assert_eq!(fast_rows, datafusion_rows);
    }

    #[tokio::test]
    async fn execute_sql_guarded_file_content_fallback_builds_one_write_session() {
        let rows = vec![
            live_directory_row(
                "01920000-0000-7000-8000-0000000000d3",
                "01920000-0000-7000-8000-0000000000a1",
                None,
                "docs",
            ),
            live_file_row(
                "01920000-0000-7000-8000-0000000000d2",
                "01920000-0000-7000-8000-0000000000a1",
                Some("01920000-0000-7000-8000-0000000000d3"),
                "readme.md",
            ),
            live_blob_ref_row(
                "01920000-0000-7000-8000-0000000000d2",
                "01920000-0000-7000-8000-0000000000a1",
                b"old",
            ),
        ];
        let (inner, staged_writes, scans) = counting_write_context_with_blob_reader(
            rows,
            Arc::new(StaticBlobReader {
                bytes: b"old".to_vec(),
            }),
        );
        let branch_head_loads = Arc::new(AtomicUsize::new(0));
        let mut ctx = CountingWriteSessionContext {
            inner,
            branch_head_loads: Arc::clone(&branch_head_loads),
        };

        let (result, path) = execute_write_sql_trace(
            &mut ctx,
            "UPDATE lix_file SET content = $1 WHERE id = $2 AND content = $3",
            &[
                Value::Blob(b"new".to_vec().into()),
                Value::Text("01920000-0000-7000-8000-0000000000d2".to_string()),
                Value::Blob(b"old".to_vec().into()),
            ],
            WriteExecutorMode::Auto,
        )
        .await
        .expect("guarded file update should use the DataFusion fallback");

        assert_eq!(path, WriteExecutorPath::DataFusion);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);
        assert_eq!(
            branch_head_loads.load(Ordering::SeqCst),
            1,
            "the fallback should build and initialize one DataFusion write session"
        );
        assert_eq!(
            scans.load(Ordering::SeqCst),
            2,
            "the descriptor and blob-ref reads should run once, during execution"
        );

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let blob_refs = overlay.visible_semantic_rows(false, "lix_binary_blob_ref");
        assert_eq!(blob_refs.len(), 1);
        let snapshot: JsonValue = serde_json::from_str(
            blob_refs[0]
                .snapshot_content
                .as_deref()
                .expect("blob ref snapshot"),
        )
        .expect("blob ref snapshot JSON");
        assert_eq!(snapshot["id"], "01920000-0000-7000-8000-0000000000d2");
        assert_eq!(snapshot["size_bytes"], 3);
        assert_eq!(
            snapshot["blob_hash"],
            crate::binary_cas::BlobId::from_content(b"new").to_hex()
        );
    }

    #[tokio::test]
    async fn execute_sql_file_content_update_by_id_updates_same_path_in_every_matching_durability_lane()
     {
        let root = live_file_row(
            "01920000-0000-7000-8000-0000000000d2",
            "01920000-0000-7000-8000-0000000000a1",
            None,
            "shared.md",
        );
        let mut scoped = live_file_row(
            "01920000-0000-7000-8000-0000000000d2",
            "01920000-0000-7000-8000-0000000000a1",
            None,
            "shared.md",
        );
        scoped.untracked = true;
        scoped.change_id = None;
        scoped.commit_id = None;
        let rows = vec![root, scoped];
        let (mut fast_ctx, fast_staged, _) = counting_write_context(rows.clone());
        let (mut datafusion_ctx, datafusion_staged, _) = counting_write_context(rows);
        let sql = "UPDATE lix_file SET content = CAST('AB' AS BYTEA) WHERE id = '01920000-0000-7000-8000-0000000000d2'";

        let (fast_result, fast_path) =
            execute_write_sql_trace(&mut fast_ctx, sql, &[], WriteExecutorMode::ForceFast)
                .await
                .expect("scoped file data update should use the fast path");
        let (datafusion_result, datafusion_path) = execute_write_sql_trace(
            &mut datafusion_ctx,
            sql,
            &[],
            WriteExecutorMode::ForceDataFusion,
        )
        .await
        .expect("reference scoped file data update should succeed");

        assert_eq!(fast_path, WriteExecutorPath::Fast);
        assert_eq!(datafusion_path, WriteExecutorPath::DataFusion);
        assert_eq!(fast_result.rows, vec![vec![Value::Integer(2)]]);
        assert_eq!(fast_result.rows, datafusion_result.rows);
        let fast_rows = fast_staged.lock().expect("fast writes lock").deltas[0]
            .pending_write_overlay()
            .expect("fast staged delta should project")
            .visible_all_semantic_rows();
        let datafusion_rows = datafusion_staged
            .lock()
            .expect("DataFusion writes lock")
            .deltas[0]
            .pending_write_overlay()
            .expect("DataFusion staged delta should project")
            .visible_all_semantic_rows();
        assert_eq!(fast_rows, datafusion_rows);
    }

    #[tokio::test]
    async fn execute_sql_file_content_update_by_id_validates_active_branch() {
        let make_context = || {
            let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
            DummySqlWriteExecutionContext {
                active_branch_id: "missing-branch",
                blob_reader: Arc::new(DummyBlobReader),
                hot_state: Arc::new(RowsHotStateReader { rows: Vec::new() }),
                staged_writes,
                schema_definitions: vec![],
            }
        };
        let sql = "UPDATE lix_file SET content = CAST('A' AS BYTEA) WHERE id = '01920000-0000-7000-8000-0000000000d2'";
        let mut fast_ctx = make_context();
        let mut datafusion_ctx = make_context();

        let fast_error =
            execute_write_sql_trace(&mut fast_ctx, sql, &[], WriteExecutorMode::ForceFast)
                .await
                .expect_err("fast update must reject a missing active branch");
        let datafusion_error = execute_write_sql_trace(
            &mut datafusion_ctx,
            sql,
            &[],
            WriteExecutorMode::ForceDataFusion,
        )
        .await
        .expect_err("DataFusion update must reject a missing active branch");

        assert_eq!(fast_error.code, datafusion_error.code);
        assert_eq!(fast_error.code, LixError::CODE_BRANCH_NOT_FOUND);
    }

    #[tokio::test]
    async fn execute_sql_file_content_update_by_id_validates_orphan_blob_refs() {
        let mut malformed = live_blob_ref_row(
            "01920000-0000-7000-8000-0000000000d2",
            "01920000-0000-7000-8000-0000000000a1",
            b"old",
        );
        malformed.snapshot_content = Some("not-json".into());
        let (mut fast_ctx, _, _) = counting_write_context(vec![malformed.clone()]);
        let (mut datafusion_ctx, _, _) = counting_write_context(vec![malformed]);
        let sql = "UPDATE lix_file SET content = CAST('A' AS BYTEA) WHERE id = '01920000-0000-7000-8000-0000000000d2'";

        let fast_error =
            execute_write_sql_trace(&mut fast_ctx, sql, &[], WriteExecutorMode::ForceFast)
                .await
                .expect_err("fast update must validate targeted orphan blob refs");
        let datafusion_error = execute_write_sql_trace(
            &mut datafusion_ctx,
            sql,
            &[],
            WriteExecutorMode::ForceDataFusion,
        )
        .await
        .expect_err("DataFusion update must validate targeted orphan blob refs");

        assert_eq!(fast_error.code, datafusion_error.code);
    }

    #[tokio::test]
    async fn execute_sql_file_content_update_by_id_supports_params() {
        let (mut ctx, staged_writes, scans) = counting_write_context(vec![live_file_row(
            "01920000-0000-7000-8000-0000000000d2",
            "01920000-0000-7000-8000-0000000000a1",
            None,
            "readme.md",
        )]);

        let (result, path) = execute_write_sql_trace(
            &mut ctx,
            "UPDATE lix_file SET content = $1 WHERE id = $2",
            &[
                Value::Blob(b"parameterized".to_vec().into()),
                Value::Text("01920000-0000-7000-8000-0000000000d2".to_string()),
            ],
            WriteExecutorMode::ForceFast,
        )
        .await
        .expect("parameterized file data update should use the fast path");

        assert_eq!(path, WriteExecutorPath::Fast);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);
        assert_eq!(
            scans.load(Ordering::SeqCst),
            2,
            "a blob-less file needs no second materialization probe"
        );
        let staged_writes = staged_writes.lock().expect("staged writes lock");
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let blob_refs = overlay.visible_semantic_rows(false, "lix_binary_blob_ref");
        assert_eq!(blob_refs.len(), 1);
        let snapshot: JsonValue = serde_json::from_str(
            blob_refs[0]
                .snapshot_content
                .as_deref()
                .expect("blob ref snapshot"),
        )
        .expect("blob ref snapshot JSON");
        assert_eq!(snapshot["size_bytes"], 13);
    }

    #[tokio::test]
    async fn execute_sql_file_content_and_metadata_update_by_id_uses_fast_path() {
        let rows = vec![
            live_file_row(
                "01920000-0000-7000-8000-0000000000d2",
                "01920000-0000-7000-8000-0000000000a1",
                None,
                "readme.md",
            ),
            live_blob_ref_row(
                "01920000-0000-7000-8000-0000000000d2",
                "01920000-0000-7000-8000-0000000000a1",
                b"old",
            ),
        ];
        let (mut fast_ctx, fast_staged, _) = counting_write_context(rows.clone());
        let (mut datafusion_ctx, datafusion_staged, _) = counting_write_context(rows);
        let sql = "UPDATE lix_file SET content = $1, lixcol_metadata = $2 WHERE id = $3";
        let params = [
            Value::Blob(b"parameterized".to_vec().into()),
            Value::Jsonb(serde_json::json!({"source": "git"}).into()),
            Value::Text("01920000-0000-7000-8000-0000000000d2".to_string()),
        ];

        let (fast_result, fast_path) =
            execute_write_sql_trace(&mut fast_ctx, sql, &params, WriteExecutorMode::ForceFast)
                .await
                .expect("data and metadata update should use the fast path");
        let (datafusion_result, datafusion_path) = execute_write_sql_trace(
            &mut datafusion_ctx,
            sql,
            &params,
            WriteExecutorMode::ForceDataFusion,
        )
        .await
        .expect("reference data and metadata update should succeed");

        assert_eq!(fast_path, WriteExecutorPath::Fast);
        assert_eq!(datafusion_path, WriteExecutorPath::DataFusion);
        assert_eq!(fast_result.rows, vec![vec![Value::Integer(1)]]);
        assert_eq!(fast_result.rows, datafusion_result.rows);
        let fast_rows = fast_staged.lock().expect("fast writes lock").deltas[0]
            .pending_write_overlay()
            .expect("fast staged delta should project")
            .visible_all_semantic_rows();
        let datafusion_rows = datafusion_staged
            .lock()
            .expect("DataFusion writes lock")
            .deltas[0]
            .pending_write_overlay()
            .expect("DataFusion staged delta should project")
            .visible_all_semantic_rows();
        assert_eq!(fast_rows, datafusion_rows);
    }

    #[tokio::test]
    async fn execute_sql_file_content_update_by_id_treats_null_id_as_no_match() {
        let rows = vec![live_file_row(
            "01920000-0000-7000-8000-0000000000d2",
            "01920000-0000-7000-8000-0000000000a1",
            None,
            "readme.md",
        )];
        let (mut fast_ctx, fast_staged, fast_scans) = counting_write_context(rows);
        let sql = "UPDATE lix_file SET content = $1 WHERE id = $2";
        let params = [Value::Blob(b"parameterized".to_vec().into()), Value::Null];

        let (fast_result, fast_path) =
            execute_write_sql_trace(&mut fast_ctx, sql, &params, WriteExecutorMode::ForceFast)
                .await
                .expect("NULL file id should be a fast no-op");
        assert_eq!(fast_path, WriteExecutorPath::Fast);
        assert_eq!(fast_result.rows, vec![vec![Value::Integer(0)]]);
        assert_eq!(fast_scans.load(Ordering::SeqCst), 0);
        assert!(
            fast_staged
                .lock()
                .expect("fast writes lock")
                .deltas
                .is_empty()
        );
    }

    #[tokio::test]
    async fn execute_sql_file_content_update_by_id_tombstones_blob_ref_for_empty_data() {
        let (mut ctx, staged_writes, scans) = counting_write_context(vec![
            live_file_row(
                "01920000-0000-7000-8000-0000000000d2",
                "01920000-0000-7000-8000-0000000000a1",
                None,
                "readme.md",
            ),
            live_blob_ref_row(
                "01920000-0000-7000-8000-0000000000d2",
                "01920000-0000-7000-8000-0000000000a1",
                b"old",
            ),
        ]);

        let (result, path) = execute_write_sql_trace(
            &mut ctx,
            "UPDATE lix_file SET content = CAST('' AS BYTEA) WHERE id = '01920000-0000-7000-8000-0000000000d2'",
            &[],
            WriteExecutorMode::ForceFast,
        )
        .await
        .expect("empty file data update should use the fast path");

        assert_eq!(path, WriteExecutorPath::Fast);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);
        assert_eq!(scans.load(Ordering::SeqCst), 2);
        let staged_writes = staged_writes.lock().expect("staged writes lock");
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let blob_refs = overlay.visible_semantic_rows(true, "lix_binary_blob_ref");
        assert_eq!(blob_refs.len(), 1);
        assert!(blob_refs[0].tombstone);
    }

    #[tokio::test]
    async fn execute_sql_file_content_update_by_id_returns_zero_for_missing_file() {
        let (mut ctx, staged_writes, scans) = counting_write_context(Vec::new());

        let (result, path) = execute_write_sql_trace(
            &mut ctx,
            "UPDATE lix_file SET content = CAST('A' AS BYTEA) WHERE id = '01920000-0000-7000-8000-000000000582'",
            &[],
            WriteExecutorMode::ForceFast,
        )
        .await
        .expect("missing file update should still use the fast path");

        assert_eq!(path, WriteExecutorPath::Fast);
        assert_eq!(result.rows, vec![vec![Value::Integer(0)]]);
        assert_eq!(scans.load(Ordering::SeqCst), 2);
        assert!(
            staged_writes
                .lock()
                .expect("staged writes lock")
                .deltas
                .is_empty()
        );
    }

    #[tokio::test]
    async fn execute_sql_file_content_update_by_id_preserves_plugin_path_restrictions() {
        let (mut ctx, staged_writes, scans) = counting_write_context(vec![
            live_directory_row(
                "01920000-0000-7000-8000-000000000323",
                "01920000-0000-7000-8000-0000000000a1",
                None,
                ".lix",
            ),
            live_directory_row(
                "01920000-0000-7000-8000-000000000333",
                "01920000-0000-7000-8000-0000000000a1",
                Some("01920000-0000-7000-8000-000000000323"),
                "plugins",
            ),
            live_directory_row(
                "01920000-0000-7000-8000-000000000343",
                "01920000-0000-7000-8000-0000000000a1",
                Some("01920000-0000-7000-8000-000000000333"),
                "nested",
            ),
            live_file_row(
                "01920000-0000-7000-8000-000000000352",
                "01920000-0000-7000-8000-0000000000a1",
                Some("01920000-0000-7000-8000-000000000343"),
                "plugin_sentinel.lixplugin",
            ),
        ]);

        let error = execute_write_sql_trace(
            &mut ctx,
            "UPDATE lix_file SET content = CAST('A' AS BYTEA) WHERE id = '01920000-0000-7000-8000-000000000352'",
            &[],
            WriteExecutorMode::ForceFast,
        )
        .await
        .expect_err("nested plugin archive path should remain invalid");

        assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
        assert_eq!(
            scans.load(Ordering::SeqCst),
            2,
            "a blob-less file needs no second materialization probe"
        );
        assert!(
            staged_writes
                .lock()
                .expect("staged writes lock")
                .deltas
                .is_empty()
        );
    }

    #[tokio::test]
    async fn bound_file_content_update_fast_path_rejects_broader_shapes() {
        let (mut ctx, _, _) = counting_write_context(Vec::new());
        for sql in [
            "UPDATE lix_file SET content = CAST('A' AS BYTEA) WHERE path = '/readme.md'",
            "UPDATE lix_file SET content = CAST('A' AS BYTEA), name = 'renamed.md' WHERE id = '01920000-0000-7000-8000-0000000000d2'",
            "UPDATE lix_file SET content = content WHERE id = '01920000-0000-7000-8000-0000000000d2'",
        ] {
            let plan = create_write_logical_plan(&mut ctx, sql)
                .await
                .unwrap_or_else(|error| panic!("{sql} should plan: {error}"));
            let crate::sql2::exec::SqlLogicalPlan::Write(plan) = plan else {
                panic!("{sql} should produce a write plan");
            };
            assert!(
                !crate::sql2::exec::bound_public_write::supports_bound_public_write(&plan.plan),
                "broader shape should fall back: {sql}"
            );
        }
    }

    #[tokio::test]
    async fn execute_sql_update_file_stages_data_blob_ref() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(RowsHotStateReader {
            rows: vec![
                live_directory_row(
                    "01920000-0000-7000-8000-0000000000d3",
                    "01920000-0000-7000-8000-0000000000a1",
                    None,
                    "docs",
                ),
                live_file_row(
                    "01920000-0000-7000-8000-0000000000d2",
                    "01920000-0000-7000-8000-0000000000a1",
                    Some("01920000-0000-7000-8000-0000000000d3"),
                    "readme.md",
                ),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "UPDATE lix_file SET content = CAST('AB' AS BYTEA) WHERE id = '01920000-0000-7000-8000-0000000000d2'",
            &[],
        )
        .await
        .expect("UPDATE lix_file should stage content write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        assert!(
            overlay
                .visible_semantic_rows(false, "lix_file_descriptor")
                .is_empty()
        );
        let blob_ref_rows = overlay.visible_semantic_rows(false, "lix_binary_blob_ref");
        assert_eq!(blob_ref_rows.len(), 1);
        assert_eq!(
            blob_ref_rows[0].row_pk,
            "[\"01920000-0000-7000-8000-0000000000d2\"]"
        );
        let snapshot: JsonValue =
            serde_json::from_str(blob_ref_rows[0].snapshot_content.as_deref().unwrap())
                .expect("blob ref snapshot JSON");
        assert_eq!(snapshot["id"], "01920000-0000-7000-8000-0000000000d2");
        assert_eq!(snapshot["size_bytes"], 2);
    }

    #[tokio::test]
    async fn execute_sql_update_file_stages_path_assignment() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(RowsHotStateReader {
            rows: vec![
                live_directory_row(
                    "01920000-0000-7000-8000-0000000000d3",
                    "01920000-0000-7000-8000-0000000000a1",
                    None,
                    "docs",
                ),
                live_file_row(
                    "01920000-0000-7000-8000-0000000000d2",
                    "01920000-0000-7000-8000-0000000000a1",
                    Some("01920000-0000-7000-8000-0000000000d3"),
                    "readme.md",
                ),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "UPDATE lix_file SET path = '/docs/renamed.md' WHERE id = '01920000-0000-7000-8000-0000000000d2'",
            &[],
        )
        .await
        .expect("path update should stage descriptor rewrite");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_file_descriptor");
        assert_eq!(rows.len(), 1);
        let snapshot: JsonValue =
            serde_json::from_str(rows[0].snapshot_content.as_deref().unwrap())
                .expect("descriptor snapshot JSON");
        assert_eq!(
            snapshot["directory_id"],
            "01920000-0000-7000-8000-0000000000d3"
        );
        assert_eq!(snapshot["name"], "renamed.md");
    }

    #[tokio::test]
    async fn execute_sql_update_schema_surface_stages_rewritten_snapshot() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(RowsHotStateReader {
            rows: vec![
                live_row("row-a", "01920000-0000-7000-8000-0000000000a1", "A"),
                live_row("row-b", "01920000-0000-7000-8000-0000000000a1", "B"),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "test_state_schema",
                "columns": [
                    { "name": "id", "type": "text", "nullable": false },
                    { "name": "value", "type": "text", "nullable": false },
                ],
                "primary_key": ["id"],
            })],
        };

        let result = execute_write_sql(
            &mut ctx,
            "UPDATE test_state_schema \
             SET value = 'updated', lixcol_metadata = '{\"source\":\"row-update\"}' \
             WHERE value = 'A'",
            &[],
        )
        .await
        .expect("UPDATE schema surface should stage rewritten row");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "test_state_schema");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_pk, "[\"row-a\"]");
        assert_eq!(rows[0].branch_id, "01920000-0000-7000-8000-0000000000a1");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"id\":\"row-a\",\"value\":\"updated\"}")
        );
        assert_eq!(
            rows[0].metadata.as_deref(),
            Some("{\"source\":\"row-update\"}")
        );
    }

    #[tokio::test]
    async fn bound_public_write_supports_only_supported_row_shapes() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(RowsHotStateReader { rows: vec![] });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            staged_writes,
            schema_definitions: vec![json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "test_state_schema",
                "columns": [
                    { "name": "id", "type": "text", "nullable": false },
                    { "name": "value", "type": "text", "nullable": false },
                ],
                "primary_key": ["id"],
            })],
        };

        let supported_plan = create_write_logical_plan(
            &mut ctx,
            "UPDATE test_state_schema SET value = 'updated' WHERE value = 'A'",
        )
        .await
        .expect("supported row update should plan");
        let crate::sql2::exec::SqlLogicalPlan::Write(supported_plan) = supported_plan else {
            panic!("expected write plan");
        };
        assert!(
            crate::sql2::exec::bound_public_write::supports_bound_public_write(
                &supported_plan.plan
            )
        );

        let mut unsupported_plan = supported_plan.plan.clone();
        unsupported_plan.bound.op = crate::sql2::bind::write::BoundWriteOp::Insert;
        assert!(
            !crate::sql2::exec::bound_public_write::supports_bound_public_write(&unsupported_plan)
        );
    }

    #[tokio::test]
    async fn execute_sql_delete_unsupported_target_contradiction_still_falls_back_and_errors() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(RowsHotStateReader { rows: vec![] });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            staged_writes,
            schema_definitions: vec![json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "test_state_schema",
                "columns": [
                    { "name": "id", "type": "text", "nullable": false },
                    { "name": "value", "type": "text", "nullable": false },
                ],
                "primary_key": ["id"],
            })],
        };

        let plan = create_write_logical_plan(
            &mut ctx,
            "DELETE FROM test_state_schema WHERE value = 'A' AND value = 'B'",
        )
        .await
        .expect("registered row write should bind before reference writer selection");
        let error = crate::sql2::execute_write_logical_plan_with_mode(
            &mut ctx,
            plan,
            &[],
            WriteExecutorMode::ForceDataFusion,
        )
        .await
        .expect_err("unsupported reference writer target should not become a fast no-op");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error.message.contains("does not support this row write"));
    }

    #[tokio::test]
    async fn execute_sql_delete_unsupported_target_false_predicate_still_errors() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(RowsHotStateReader { rows: vec![] });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            staged_writes,
            schema_definitions: vec![json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "test_state_schema",
                "columns": [
                    { "name": "id", "type": "text", "nullable": false },
                    { "name": "value", "type": "text", "nullable": false },
                ],
                "primary_key": ["id"],
            })],
        };

        let plan = create_write_logical_plan(&mut ctx, "DELETE FROM test_state_schema WHERE false")
            .await
            .expect("registered row write should bind before reference writer selection");
        let error = crate::sql2::execute_write_logical_plan_with_mode(
            &mut ctx,
            plan,
            &[],
            WriteExecutorMode::ForceDataFusion,
        )
        .await
        .expect_err("unsupported target with empty scope should not become a no-op");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error.message.contains("does not support this row write"));
    }

    async fn setup_sql2_state_fixture() -> Result<DummySqlExecutionContext<'static>, LixError> {
        let schema_definition = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "test_state_schema",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        Ok(DummySqlExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader: Arc::new(StaticBlobReader {
                bytes: vec![0x41, 0x42],
            }),
            hot_state: Arc::new(RowsHotStateReader {
                rows: vec![
                    live_row("row-a", "01920000-0000-7000-8000-0000000000a1", "A"),
                    live_row("row-b", "01920000-0000-7000-8000-0000000000b1", "B"),
                    live_directory_row(
                        "01920000-0000-7000-8000-0000000000d3",
                        "01920000-0000-7000-8000-0000000000a1",
                        None,
                        "docs",
                    ),
                    live_file_row(
                        "01920000-0000-7000-8000-0000000000a2",
                        "01920000-0000-7000-8000-0000000000a1",
                        Some("01920000-0000-7000-8000-0000000000d3"),
                        "readme.md",
                    ),
                    live_blob_ref_row(
                        "01920000-0000-7000-8000-0000000000a2",
                        "01920000-0000-7000-8000-0000000000a1",
                        &[0x41, 0x42],
                    ),
                ],
            }),
            row_snapshot_reader: None,
            schema_definitions: vec![schema_definition],
        })
    }

    #[tokio::test]
    async fn execute_sql_reads_row_view_from_active_branch() {
        let ctx = setup_sql2_state_fixture()
            .await
            .expect("fixture should initialize");

        let result = execute_sql(
            &ctx,
            "SELECT value, id \
                     FROM test_state_schema",
            &[],
        )
        .await
        .expect("sql2 execute should read row view");

        assert_eq!(result.columns, vec!["value", "id"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Text("A".to_string()));
        assert_eq!(result.rows[0][1], Value::Text("row-a".to_string()));
    }

    #[tokio::test]
    async fn execute_sql_reads_lix_directory_from_active_branch() {
        let ctx = setup_sql2_state_fixture()
            .await
            .expect("fixture should initialize");

        let result = execute_sql(
            &ctx,
            "SELECT path, name \
                     FROM lix_directory \
                     WHERE id = '01920000-0000-7000-8000-0000000000d3'",
            &[],
        )
        .await
        .expect("sql2 execute should read lix_directory");

        assert_eq!(result.columns, vec!["path", "name"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Text("/docs".to_string()));
        assert_eq!(result.rows[0][1], Value::Text("docs".to_string()));
    }

    #[tokio::test]
    async fn execute_sql_reads_lix_file_from_active_branch() {
        let ctx = setup_sql2_state_fixture()
            .await
            .expect("fixture should initialize");

        let result = execute_sql(
            &ctx,
            "SELECT path, name, content \
                     FROM lix_file \
                     WHERE id = '01920000-0000-7000-8000-0000000000a2'",
            &[],
        )
        .await
        .expect("sql2 execute should read lix_file");

        assert_eq!(result.columns, vec!["path", "name", "content"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0][0],
            Value::Text("/docs/readme.md".to_string())
        );
        assert_eq!(result.rows[0][1], Value::Text("readme.md".to_string()));
        assert_eq!(result.rows[0][2], Value::Blob(vec![0x41, 0x42].into()));
    }
}
