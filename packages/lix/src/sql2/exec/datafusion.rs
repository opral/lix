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
use crate::sql2::bind::expr::{BoundBinaryOperator, BoundCastType, BoundExpr, BoundLiteral};
use crate::sql2::bind::write::{BoundInsertValues, BoundReturning, FileWriteSurface};
use crate::sql2::bind::write::{
    BoundWriteInput, BoundWriteOp, BoundWriteTarget, DirectoryWriteSurface,
};
use crate::sql2::history_route::invalid_history_anchor_error;
use crate::sql2::plan::LogicalWritePlan;
use crate::sql2::plan::branch_scope::BranchScope;
use crate::sql2::plan::predicate::BoundPredicate;
use crate::{GLOBAL_BRANCH_ID, LixError, LixNotice, SqlQueryResult, Value};
use datafusion::arrow::array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, PrimitiveArray, StringArray,
    StringViewArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::metadata::{FieldMetadata, ScalarAndMetadata};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, DFSchemaRef, JoinType, ParamValues, ScalarValue};
use datafusion::datasource::{empty::EmptyTable, provider_as_source};
use datafusion::logical_expr::expr::{BinaryExpr, Cast, InList, Like, ScalarFunction};
use datafusion::logical_expr::registry::FunctionRegistry;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Operator};
#[cfg(any(feature = "storage-benches", test))]
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DataFusionStatement;
use datafusion::sql::sqlparser::ast::{
    Expr as SqlExpr, FunctionArg, FunctionArgExpr, TableFactor, Value as SqlValue, Visit, VisitMut,
    Visitor, VisitorMut,
};
#[cfg(any(feature = "storage-benches", test))]
use futures_util::TryStreamExt;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::marker::PhantomData;
use std::ops::ControlFlow;
#[cfg(feature = "storage-benches")]
use std::time::Instant;

use crate::catalog::CatalogFingerprint;
use crate::sql2::predicate_typecheck::{
    json_predicate_placeholder_indexes_with_dfschema, validate_json_predicate_expr_with_dfschema,
};
use crate::sql2::providers::ProviderSelection;
use crate::sql2::result_metadata::{
    LIX_VALUE_TYPE_JSON, LIX_VALUE_TYPE_METADATA_KEY, field_is_json,
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
    pub(super) state: std::sync::Arc<SessionState>,
    pub(super) plan: crate::sql2::runtime::RuntimeReadPlan,
    pub(super) notices: Vec<LixNotice>,
    pub(super) json_predicate_params: BTreeSet<usize>,
    pub(super) expected_parameter_count: usize,
    pub(super) physical_planning_cache: Option<(
        std::sync::Arc<SqlPlanningCache<CatalogFingerprint>>,
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
    pub(crate) batches: std::sync::Arc<[RecordBatch]>,
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
        batches: std::sync::Arc<[RecordBatch]>,
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
        std::sync::Arc<SqlPlanningCache<CatalogFingerprint>>,
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

    fn state(&self) -> &std::sync::Arc<SessionState> {
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
            .map(|key| (std::sync::Arc::clone(cache), key));
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
            state: std::sync::Arc::clone(session.state()),
            plan,
            notices: Vec::new(),
            json_predicate_params: cached.json_predicate_params.clone(),
            expected_parameter_count: cached.expected_parameter_count,
            physical_planning_cache,
        }));
    }
    bind_table_function_parameters(&mut statement, params)?;
    let plan = create_logical_plan_from_statement(session.context(), statement).await?;
    validate_supported_logical_plan(&plan)?;
    validate_json_predicates_in_logical_plan(&plan)?;
    validate_history_anchor_predicates_in_logical_plan(&plan)?;
    let json_predicate_params = json_predicate_params_in_logical_plan(&plan);

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
                json_predicate_params: json_predicate_params.clone(),
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
                    .map(|key| (std::sync::Arc::clone(cache), key))
            })
    } else {
        None
    };

    Ok(SqlLogicalPlan::DataFusion(SqlDataFusionLogicalPlan {
        state: std::sync::Arc::clone(session.state()),
        plan: crate::sql2::runtime::RuntimeReadPlan::Bound(plan),
        notices: Vec::new(),
        json_predicate_params,
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
            provider_as_source(std::sync::Arc::new(EmptyTable::new(scan.source.schema())));
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

    let mut visitor = TableFunctionVisitor(false);
    if let DataFusionStatement::Statement(statement) = statement {
        let _ = statement.visit(&mut visitor);
    }
    visitor.0
}

pub(crate) async fn execute_transaction_read_statement_from_parsed(
    read_ctx: &impl SqlExecutionContext,
    write_ctx: &mut dyn SqlWriteExecutionContext,
    sql: &str,
    statement: DataFusionStatement,
    params: &[Value],
) -> Result<SqlQueryResult, LixError> {
    // Same fence as session reads, with the transaction overlay available
    // during planning/execution but not returned to the caller.
    let planning_environment = read_ctx.sql_planning_environment().await?;
    let (plan, session) = create_transaction_read_logical_plan_from_parsed(
        read_ctx, write_ctx, sql, statement, params,
    )
    .await?;
    let result = execute_logical_plan(plan, params)
        .await
        .and_then(SessionReadResult::into_sql_query_result);
    if let Some((cache, _)) = planning_environment {
        cache.recycle_datafusion_read_session(session);
    }
    result
}

async fn create_transaction_read_logical_plan_from_parsed(
    read_ctx: &impl SqlExecutionContext,
    write_ctx: &mut dyn SqlWriteExecutionContext,
    sql: &str,
    mut statement: DataFusionStatement,
    params: &[Value],
) -> Result<(SqlLogicalPlan, PooledReadSession), LixError> {
    crate::sql2::bind_read_statement(sql, &statement)?;
    let parameter_names = statement_parameter_names(&statement)?;
    let expected_parameter_count = expected_positional_parameter_count(&parameter_names)?;
    validate_parameter_count_values(expected_parameter_count, &parameter_names, params.len())?;
    bind_table_function_parameters(&mut statement, params)?;
    let session = build_transaction_read_session(read_ctx, write_ctx, &statement).await?;
    let plan = create_logical_plan_from_statement(session.context(), statement).await?;
    validate_supported_logical_plan(&plan)?;
    validate_json_predicates_in_logical_plan(&plan)?;
    validate_history_anchor_predicates_in_logical_plan(&plan)?;
    let json_predicate_params = json_predicate_params_in_logical_plan(&plan);

    Ok((
        SqlLogicalPlan::DataFusion(SqlDataFusionLogicalPlan {
            state: std::sync::Arc::clone(session.state()),
            plan: crate::sql2::runtime::RuntimeReadPlan::Bound(plan),
            notices: Vec::new(),
            json_predicate_params,
            expected_parameter_count,
            physical_planning_cache: None,
        }),
        session,
    ))
}

async fn create_logical_plan_from_statement(
    session: &SessionContext,
    statement: DataFusionStatement,
) -> Result<LogicalPlan, LixError> {
    session
        .state()
        .statement_to_plan(statement)
        .await
        .map_err(datafusion_error_to_lix_error)
}

fn validate_json_predicates_in_logical_plan(plan: &LogicalPlan) -> Result<(), LixError> {
    for expr in plan.expressions() {
        validate_json_predicate_expr_with_dfschema(plan.schema(), &expr)?;
    }
    match plan {
        LogicalPlan::Filter(filter) => {
            validate_json_predicate_expr_with_dfschema(filter.input.schema(), &filter.predicate)?;
        }
        LogicalPlan::TableScan(scan) => {
            for filter in &scan.filters {
                validate_json_predicate_expr_with_dfschema(scan.projected_schema.as_ref(), filter)?;
            }
        }
        _ => {}
    }

    for input in plan.inputs() {
        validate_json_predicates_in_logical_plan(input)?;
    }

    Ok(())
}

fn json_predicate_params_in_logical_plan(plan: &LogicalPlan) -> BTreeSet<usize> {
    let mut params = BTreeSet::new();
    for expr in plan.expressions() {
        params.extend(json_predicate_placeholder_indexes_with_dfschema(
            plan.schema(),
            &expr,
        ));
    }
    match plan {
        LogicalPlan::Filter(filter) => {
            params.extend(json_predicate_placeholder_indexes_with_dfschema(
                filter.input.schema(),
                &filter.predicate,
            ));
        }
        LogicalPlan::TableScan(scan) => {
            for filter in &scan.filters {
                params.extend(json_predicate_placeholder_indexes_with_dfschema(
                    scan.projected_schema.as_ref(),
                    filter,
                ));
            }
        }
        _ => {}
    }

    for input in plan.inputs() {
        params.extend(json_predicate_params_in_logical_plan(input));
    }
    params
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HistoryAnchorLineage {
    Direct,
    Derived,
}

#[derive(Clone, Debug)]
struct HistoryAnchorScope {
    schema: DFSchemaRef,
    columns: Vec<Option<HistoryAnchorLineage>>,
}

impl HistoryAnchorScope {
    fn empty(schema: DFSchemaRef) -> Self {
        Self {
            columns: vec![None; schema.fields().len()],
            schema,
        }
    }

    fn resolve(&self, column: &Column) -> Option<(usize, HistoryAnchorLineage)> {
        let index = self.schema.maybe_index_of_column(column)?;
        self.columns
            .get(index)
            .copied()
            .flatten()
            .map(|lineage| (index, lineage))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HistoryAnchorLocation {
    Local,
    Outer,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ResolvedHistoryAnchor {
    location: HistoryAnchorLocation,
    scope_index: usize,
    column_index: usize,
    lineage: HistoryAnchorLineage,
    column_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct HistoryAnchorKey {
    scope_index: usize,
    column_index: usize,
}

struct HistoryAnchorResolver<'a> {
    local: &'a [HistoryAnchorScope],
    outer: &'a [HistoryAnchorScope],
}

impl HistoryAnchorResolver<'_> {
    fn resolve_local(&self, column: &Column) -> Option<ResolvedHistoryAnchor> {
        Self::resolve(column, self.local, HistoryAnchorLocation::Local)
    }

    fn resolve_outer(&self, column: &Column) -> Option<ResolvedHistoryAnchor> {
        Self::resolve(column, self.outer, HistoryAnchorLocation::Outer)
    }

    fn resolve(
        column: &Column,
        scopes: &[HistoryAnchorScope],
        location: HistoryAnchorLocation,
    ) -> Option<ResolvedHistoryAnchor> {
        let mut schema_matches = scopes
            .iter()
            .enumerate()
            .filter_map(|(scope_index, scope)| {
                scope
                    .schema
                    .maybe_index_of_column(column)
                    .map(|column_index| (scope_index, column_index, scope))
            });
        let first = schema_matches.next()?;
        // An unqualified same-named column across multiple inputs is not enough
        // to identify the history relation. DataFusion normally qualifies these
        // references; keeping ambiguity non-history avoids relation-blind false
        // positives if an extension plan does not.
        if schema_matches.next().is_some() {
            return None;
        }
        let lineage = first.2.columns.get(first.1).copied().flatten()?;
        Some(ResolvedHistoryAnchor {
            location,
            scope_index: first.0,
            column_index: first.1,
            lineage,
            column_name: column.name.clone(),
        })
    }
}

fn history_anchor_references(
    expr: &Expr,
    resolver: &HistoryAnchorResolver<'_>,
) -> Vec<ResolvedHistoryAnchor> {
    let mut references = expr
        .column_refs()
        .into_iter()
        .filter_map(|column| resolver.resolve_local(column))
        .collect::<Vec<_>>();
    expr.apply(|nested| {
        if let Expr::OuterReferenceColumn(_, column) = nested {
            if let Some(reference) = resolver.resolve_outer(column) {
                references.push(reference);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("history anchor expression traversal is infallible");
    references
}

fn direct_history_anchor_reference(
    expr: &Expr,
    resolver: &HistoryAnchorResolver<'_>,
) -> Option<ResolvedHistoryAnchor> {
    match expr {
        Expr::Alias(alias) => direct_history_anchor_reference(&alias.expr, resolver),
        Expr::Column(column) => resolver.resolve_local(column),
        Expr::OuterReferenceColumn(_, column) => resolver.resolve_outer(column),
        _ => None,
    }
}

fn routable_history_anchor_value_shape(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Literal(ScalarValue::Utf8(Some(_)), _) | Expr::Placeholder(_)
    ) || matches!(
        expr,
        Expr::ScalarFunction(function)
            if function.name() == "lix_active_branch_commit_id" && function.args.is_empty()
    )
}

fn exact_history_anchor_equality_key(
    binary: &BinaryExpr,
    resolver: &HistoryAnchorResolver<'_>,
) -> Option<HistoryAnchorKey> {
    for (anchor, value) in [
        (binary.left.as_ref(), binary.right.as_ref()),
        (binary.right.as_ref(), binary.left.as_ref()),
    ] {
        let Some(reference) = direct_history_anchor_reference(anchor, resolver) else {
            continue;
        };
        if reference.location == HistoryAnchorLocation::Local
            && reference.lineage == HistoryAnchorLineage::Direct
            && history_anchor_references(value, resolver).is_empty()
            && routable_history_anchor_value_shape(value)
        {
            return Some(HistoryAnchorKey {
                scope_index: reference.scope_index,
                column_index: reference.column_index,
            });
        }
    }
    None
}

fn exact_history_anchor_term_key(
    expr: &Expr,
    resolver: &HistoryAnchorResolver<'_>,
) -> Option<HistoryAnchorKey> {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            exact_history_anchor_equality_key(binary, resolver)
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
            let left = exact_history_anchor_term_key(&binary.left, resolver)?;
            let right = exact_history_anchor_term_key(&binary.right, resolver)?;
            (left == right).then_some(left)
        }
        Expr::InList(in_list) if !in_list.negated && !in_list.list.is_empty() => {
            let reference = direct_history_anchor_reference(&in_list.expr, resolver)?;
            (reference.location == HistoryAnchorLocation::Local
                && reference.lineage == HistoryAnchorLineage::Direct
                && in_list.list.iter().all(routable_history_anchor_value_shape))
            .then_some(HistoryAnchorKey {
                scope_index: reference.scope_index,
                column_index: reference.column_index,
            })
        }
        _ => None,
    }
}

fn history_anchor_predicate_shape_is_exact(
    expr: &Expr,
    resolver: &HistoryAnchorResolver<'_>,
) -> bool {
    if history_anchor_references(expr, resolver).is_empty() {
        return true;
    }
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            history_anchor_predicate_shape_is_exact(&binary.left, resolver)
                && history_anchor_predicate_shape_is_exact(&binary.right, resolver)
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
            exact_history_anchor_term_key(expr, resolver).is_some()
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            exact_history_anchor_equality_key(binary, resolver).is_some()
        }
        Expr::InList(_) => exact_history_anchor_term_key(expr, resolver).is_some(),
        _ => false,
    }
}

fn validate_history_anchor_predicate(
    expr: &Expr,
    resolver: &HistoryAnchorResolver<'_>,
) -> Result<(), LixError> {
    let references = history_anchor_references(expr, resolver);
    if references.is_empty() || history_anchor_predicate_shape_is_exact(expr, resolver) {
        return Ok(());
    }
    Err(invalid_history_anchor_error(
        &references[0].column_name,
        None,
    ))
}

fn join_on_history_anchor_side_is_pushable(join_type: JoinType, scope_index: usize) -> bool {
    let (left, right) = match join_type {
        JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi => (true, true),
        JoinType::Left | JoinType::LeftAnti | JoinType::LeftMark => (false, true),
        JoinType::Right | JoinType::RightAnti | JoinType::RightMark => (true, false),
        JoinType::Full => (false, false),
    };
    match scope_index {
        0 => left,
        1 => right,
        _ => false,
    }
}

fn validate_history_anchor_join_predicate(
    expr: &Expr,
    resolver: &HistoryAnchorResolver<'_>,
    join_type: JoinType,
) -> Result<(), LixError> {
    validate_history_anchor_predicate(expr, resolver)?;
    let unpushable = history_anchor_references(expr, resolver)
        .into_iter()
        .find(|reference| {
            reference.location == HistoryAnchorLocation::Local
                && !join_on_history_anchor_side_is_pushable(join_type, reference.scope_index)
        });
    let Some(reference) = unpushable else {
        return Ok(());
    };
    Err(invalid_history_anchor_error(&reference.column_name, None))
}

fn validate_embedded_history_anchor_predicates(
    expr: &Expr,
    resolver: &HistoryAnchorResolver<'_>,
) -> Result<(), LixError> {
    let mut result = Ok(());
    expr.apply(|nested| {
        let predicate = match nested {
            Expr::AggregateFunction(function) => function.params.filter.as_deref(),
            Expr::WindowFunction(function) => function.params.filter.as_deref(),
            _ => None,
        };
        if let Some(predicate) = predicate {
            if let Some(reference) = history_anchor_references(predicate, resolver).first() {
                result = Err(invalid_history_anchor_error(&reference.column_name, None));
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("embedded history anchor predicate traversal is infallible");
    result
}

fn projected_history_anchor_scope(
    schema: DFSchemaRef,
    expressions: &[Expr],
    inputs: &[HistoryAnchorScope],
    outer: &[HistoryAnchorScope],
) -> HistoryAnchorScope {
    let resolver = HistoryAnchorResolver {
        local: inputs,
        outer,
    };
    let mut output = HistoryAnchorScope::empty(schema);
    for (index, expression) in expressions.iter().enumerate() {
        let has_local_reference = history_anchor_references(expression, &resolver)
            .into_iter()
            .any(|reference| reference.location == HistoryAnchorLocation::Local);
        if !has_local_reference {
            continue;
        }
        let lineage = direct_history_anchor_reference(expression, &resolver)
            .filter(|reference| reference.location == HistoryAnchorLocation::Local)
            .map_or(HistoryAnchorLineage::Derived, |reference| reference.lineage);
        if let Some(column) = output.columns.get_mut(index) {
            *column = Some(lineage);
        }
    }
    output
}

fn positional_history_anchor_scope(
    schema: DFSchemaRef,
    input: &HistoryAnchorScope,
) -> HistoryAnchorScope {
    let mut output = HistoryAnchorScope::empty(schema);
    for (target, source) in output.columns.iter_mut().zip(&input.columns) {
        *target = *source;
    }
    output
}

fn unroutable_history_anchor_scope(
    schema: DFSchemaRef,
    input: &HistoryAnchorScope,
) -> HistoryAnchorScope {
    let mut output = positional_history_anchor_scope(schema, input);
    for lineage in output.columns.iter_mut().flatten() {
        *lineage = HistoryAnchorLineage::Derived;
    }
    output
}

fn merged_history_anchor_scope(
    schema: DFSchemaRef,
    inputs: &[HistoryAnchorScope],
) -> HistoryAnchorScope {
    let mut output = HistoryAnchorScope::empty(schema);
    for index in 0..output.columns.len() {
        let column = Column::from(output.schema.qualified_field(index));
        let mut matches = inputs.iter().filter_map(|input| input.resolve(&column));
        let first = matches.next();
        if matches.next().is_none() {
            output.columns[index] = first.map(|(_, lineage)| lineage);
        }
    }
    output
}

fn validate_history_anchor_subqueries(
    expr: &Expr,
    local: &[HistoryAnchorScope],
    outer: &[HistoryAnchorScope],
) -> Result<(), LixError> {
    let nested_outer = local.iter().chain(outer).cloned().collect::<Vec<_>>();
    let mut result = Ok(());
    expr.apply(|nested| {
        let subquery = match nested {
            Expr::Exists(exists) => Some(exists.subquery.subquery.as_ref()),
            Expr::InSubquery(in_subquery) => Some(in_subquery.subquery.subquery.as_ref()),
            Expr::SetComparison(comparison) => Some(comparison.subquery.subquery.as_ref()),
            Expr::ScalarSubquery(subquery) => Some(subquery.subquery.as_ref()),
            _ => None,
        };
        if let Some(subquery) = subquery {
            result = visit_history_anchor_plan(subquery, &nested_outer).map(|_| ());
            if result.is_err() {
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("history anchor subquery traversal is infallible");
    result
}

fn visit_history_anchor_plan(
    plan: &LogicalPlan,
    outer: &[HistoryAnchorScope],
) -> Result<HistoryAnchorScope, LixError> {
    match plan {
        LogicalPlan::TableScan(scan) => {
            let Some(anchor_column) =
                crate::sql2::providers::history_anchor_column(scan.source.as_ref())
            else {
                return Ok(HistoryAnchorScope::empty(scan.projected_schema.clone()));
            };
            let source_schema = std::sync::Arc::new(
                DFSchema::try_from_qualified_schema(
                    scan.table_name.clone(),
                    scan.source.schema().as_ref(),
                )
                .map_err(datafusion_error_to_lix_error)?,
            );
            let mut source_scope = HistoryAnchorScope::empty(source_schema);
            if let Some(index) = source_scope
                .schema
                .index_of_column_by_name(Some(&scan.table_name), anchor_column)
            {
                source_scope.columns[index] = Some(HistoryAnchorLineage::Direct);
            }
            let local = std::slice::from_ref(&source_scope);
            let resolver = HistoryAnchorResolver { local, outer };
            for filter in &scan.filters {
                validate_history_anchor_subqueries(filter, local, outer)?;
                validate_history_anchor_predicate(filter, &resolver)?;
            }

            let mut output = HistoryAnchorScope::empty(scan.projected_schema.clone());
            if let Some(index) = output
                .schema
                .index_of_column_by_name(Some(&scan.table_name), anchor_column)
            {
                output.columns[index] = Some(HistoryAnchorLineage::Direct);
            }
            Ok(output)
        }
        LogicalPlan::Filter(filter) => {
            let input = visit_history_anchor_plan(&filter.input, outer)?;
            let local = std::slice::from_ref(&input);
            validate_history_anchor_subqueries(&filter.predicate, local, outer)?;
            validate_history_anchor_predicate(
                &filter.predicate,
                &HistoryAnchorResolver { local, outer },
            )?;
            Ok(positional_history_anchor_scope(
                plan.schema().clone(),
                &input,
            ))
        }
        LogicalPlan::Projection(projection) => {
            let input = visit_history_anchor_plan(&projection.input, outer)?;
            let local = std::slice::from_ref(&input);
            for expression in &projection.expr {
                validate_history_anchor_subqueries(expression, local, outer)?;
                validate_embedded_history_anchor_predicates(
                    expression,
                    &HistoryAnchorResolver { local, outer },
                )?;
            }
            Ok(projected_history_anchor_scope(
                projection.schema.clone(),
                &projection.expr,
                local,
                outer,
            ))
        }
        LogicalPlan::Aggregate(aggregate) => {
            let input = visit_history_anchor_plan(&aggregate.input, outer)?;
            let local = std::slice::from_ref(&input);
            let expressions = aggregate
                .group_expr
                .iter()
                .chain(&aggregate.aggr_expr)
                .cloned()
                .collect::<Vec<_>>();
            for expression in &expressions {
                validate_history_anchor_subqueries(expression, local, outer)?;
                validate_embedded_history_anchor_predicates(
                    expression,
                    &HistoryAnchorResolver { local, outer },
                )?;
            }
            let projected = projected_history_anchor_scope(
                aggregate.schema.clone(),
                &expressions,
                local,
                outer,
            );
            Ok(unroutable_history_anchor_scope(
                aggregate.schema.clone(),
                &projected,
            ))
        }
        LogicalPlan::Window(window) => {
            let input = visit_history_anchor_plan(&window.input, outer)?;
            let local = std::slice::from_ref(&input);
            for expression in &window.window_expr {
                validate_history_anchor_subqueries(expression, local, outer)?;
                validate_embedded_history_anchor_predicates(
                    expression,
                    &HistoryAnchorResolver { local, outer },
                )?;
            }
            let mut output = positional_history_anchor_scope(window.schema.clone(), &input);
            let input_width = input.schema.fields().len();
            for (index, expression) in window.window_expr.iter().enumerate() {
                if history_anchor_references(expression, &HistoryAnchorResolver { local, outer })
                    .into_iter()
                    .any(|reference| reference.location == HistoryAnchorLocation::Local)
                {
                    if let Some(column) = output.columns.get_mut(input_width + index) {
                        *column = Some(HistoryAnchorLineage::Derived);
                    }
                }
            }
            Ok(unroutable_history_anchor_scope(
                window.schema.clone(),
                &output,
            ))
        }
        LogicalPlan::Join(join) => {
            let left = visit_history_anchor_plan(&join.left, outer)?;
            let right = visit_history_anchor_plan(&join.right, outer)?;
            let local = [left, right];
            let resolver = HistoryAnchorResolver {
                local: &local,
                outer,
            };
            for (left, right) in &join.on {
                let predicate = Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(left.clone()),
                    Operator::Eq,
                    Box::new(right.clone()),
                ));
                validate_history_anchor_subqueries(&predicate, &local, outer)?;
                validate_history_anchor_join_predicate(&predicate, &resolver, join.join_type)?;
            }
            if let Some(filter) = &join.filter {
                validate_history_anchor_subqueries(filter, &local, outer)?;
                validate_history_anchor_join_predicate(filter, &resolver, join.join_type)?;
            }
            Ok(merged_history_anchor_scope(join.schema.clone(), &local))
        }
        LogicalPlan::SubqueryAlias(alias) => {
            let input = visit_history_anchor_plan(&alias.input, outer)?;
            Ok(positional_history_anchor_scope(
                alias.schema.clone(),
                &input,
            ))
        }
        LogicalPlan::Limit(limit) => {
            let input = visit_history_anchor_plan(&limit.input, outer)?;
            for expression in plan.expressions() {
                validate_history_anchor_subqueries(
                    &expression,
                    std::slice::from_ref(&input),
                    outer,
                )?;
            }
            Ok(unroutable_history_anchor_scope(
                plan.schema().clone(),
                &input,
            ))
        }
        LogicalPlan::Union(union) => {
            let inputs = union
                .inputs
                .iter()
                .map(|input| visit_history_anchor_plan(input, outer))
                .collect::<Result<Vec<_>, _>>()?;
            let mut output = HistoryAnchorScope::empty(union.schema.clone());
            for index in 0..output.columns.len() {
                let lineages = inputs
                    .iter()
                    .filter_map(|input| input.columns.get(index).copied().flatten())
                    .collect::<Vec<_>>();
                if !lineages.is_empty() {
                    output.columns[index] = Some(
                        if lineages
                            .iter()
                            .all(|lineage| *lineage == HistoryAnchorLineage::Direct)
                        {
                            HistoryAnchorLineage::Direct
                        } else {
                            HistoryAnchorLineage::Derived
                        },
                    );
                }
            }
            Ok(output)
        }
        other => {
            let inputs = other
                .inputs()
                .into_iter()
                .map(|input| visit_history_anchor_plan(input, outer))
                .collect::<Result<Vec<_>, _>>()?;
            for expression in other.expressions() {
                validate_history_anchor_subqueries(&expression, &inputs, outer)?;
                validate_embedded_history_anchor_predicates(
                    &expression,
                    &HistoryAnchorResolver {
                        local: &inputs,
                        outer,
                    },
                )?;
            }
            if inputs.len() == 1 {
                Ok(positional_history_anchor_scope(
                    other.schema().clone(),
                    &inputs[0],
                ))
            } else {
                Ok(merged_history_anchor_scope(other.schema().clone(), &inputs))
            }
        }
    }
}

fn validate_history_anchor_predicates_in_logical_plan(plan: &LogicalPlan) -> Result<(), LixError> {
    visit_history_anchor_plan(plan, &[]).map(|_| ())
}

/// Substitutes positional parameters into a bound read plan.
///
/// Mirrors `DataFrame::with_param_values`, which is exactly
/// `LogicalPlan::with_param_values` plus a `SessionState` it does not need.
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
    plan.with_param_values(ParamValues::List(
        params.iter().map(scalar_value_from_lix_value).collect(),
    ))
    .map_err(datafusion_error_to_lix_error)
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
        json_predicate_params,
        expected_parameter_count,
        physical_planning_cache,
    } = plan;
    debug_assert_eq!(expected_parameter_count, params.len());
    validate_json_predicate_params(&json_predicate_params, params)?;

    // `SessionContext::execute_logical_plan` only branches for DDL and utility
    // statements, both of which `validate_supported_logical_plan` already
    // rejects, and otherwise wraps the plan in a `DataFrame` whose only purpose
    // here is to carry a freshly deep-copied `SessionState`. Bind the
    // parameters on the plan directly against the statement's pooled state.
    let plan = bind_runtime_plan_param_values(plan, params)?;

    let result_fields = plan
        .inner()
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let batches = crate::sql2::runtime::collect_plan(&state, plan, physical_planning_cache)
        .await
        .map_err(datafusion_error_to_lix_error)?;
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
            batches: std::sync::Arc::from(batches),
            notices,
        });
    }
    if retain_columnar_result(&result_fields, &batches) {
        return Ok(SessionReadResult::Columnar {
            fields: result_fields,
            batches: std::sync::Arc::from(batches),
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
        json_predicate_params,
        expected_parameter_count,
        physical_planning_cache,
    } = plan;
    debug_assert_eq!(expected_parameter_count, params.len());
    validate_json_predicate_params(&json_predicate_params, params)?;

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
        json_predicate_params,
        expected_parameter_count,
        physical_planning_cache,
    } = plan;
    debug_assert_eq!(expected_parameter_count, params.len());
    validate_json_predicate_params(&json_predicate_params, params)?;

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
        batches: std::sync::Arc::from(batches),
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
        || fields.iter().any(|field| field_is_json(field))
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

pub(crate) async fn execute_datafusion_write_logical_plan(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    params: &[Value],
) -> Result<SqlWriteResult, LixError> {
    validate_bound_write_input(plan, params)?;
    let table_name = write_target_table_name(plan)?;
    let provider_selection = write_provider_selection(plan, &table_name);
    let session =
        build_write_session_with_options(ctx, write_session_options(plan), &provider_selection)
            .await?;
    let table = session
        .table_provider(&table_name)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    let write_target = session.write_target(&table_name)?;
    let table_schema = table.schema();
    let state = session.state();
    // Diff command sinks own their dedicated RETURNING contract. Every
    // registered table surface takes the normal DML path below, where a
    // provider must explicitly capture the relevant image (pre-delete or
    // post-insert/update) rather than silently returning only an affected
    // count.
    let returning = if matches!(plan.bound.target, BoundWriteTarget::DiffCommand(_)) {
        None
    } else {
        datafusion_dml_returning(
            &session,
            table_schema.as_ref(),
            plan.bound.returning.as_ref(),
            params,
        )?
    };

    let exec = match plan.bound.op {
        BoundWriteOp::Insert => {
            let input =
                insert_input_plan(&session, std::sync::Arc::clone(&table_schema), plan, params)
                    .await?;
            if plan.bound.branch_scope == BranchScope::Empty {
                return sql_write_empty_returning_result(returning.as_ref());
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
                let proposed_batches = crate::sql2::runtime::collect_input_plan(
                    std::sync::Arc::clone(&input),
                    session.task_ctx(),
                )
                .await
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
                        sql_write_captured_returning_result(rows_affected, returning)
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
                return sql_write_empty_returning_result(returning.as_ref());
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
                return sql_write_empty_returning_result(returning.as_ref());
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
    if matches!(plan.bound.target, BoundWriteTarget::DiffCommand(_)) {
        let outcome = crate::sql2::DiffCommandOutcome {
            rows_affected,
            commit_id: if rows_affected == 0 {
                None
            } else {
                ctx.staged_commit_id(ctx.active_branch_id())?
            },
        };
        return SqlWriteResult::diff_command(outcome, plan.bound.returning.as_ref());
    }
    match returning {
        Some(returning) => sql_write_captured_returning_result(rows_affected, &returning),
        None => Ok(SqlWriteResult::affected(rows_affected)),
    }
}

async fn insert_input_plan(
    session: &SessionContext,
    schema: SchemaRef,
    plan: &LogicalWritePlan,
    params: &[Value],
) -> Result<std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>, LixError> {
    match &plan.bound.input {
        BoundWriteInput::Values(values) => {
            insert_values_input_plan(session, schema, plan, params, values).await
        }
        BoundWriteInput::Query { query, columns } => {
            insert_query_input_plan(session, schema, query, columns, params).await
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
) -> Result<std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>, LixError> {
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
    let nullable_schema = std::sync::Arc::new(Schema::new(
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
    let df_schema = std::sync::Arc::new(
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
    columns: &[crate::sql2::bind::expr::BoundColumnRef],
    params: &[Value],
) -> Result<std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>, LixError> {
    let input = session
        .state()
        .statement_to_plan(DataFusionStatement::Statement(Box::new(
            datafusion::sql::sqlparser::ast::Statement::Query(query.query.clone()),
        )))
        .await
        .map_err(datafusion_error_to_lix_error)?;
    validate_supported_logical_plan(&input)?;
    validate_json_predicates_in_logical_plan(&input)?;
    let json_predicate_params = json_predicate_params_in_logical_plan(&input);
    validate_parameter_count(&input, params.len())?;
    validate_json_predicate_params(&json_predicate_params, params)?;
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
            Ok(expr
                .cast_to(field.data_type(), input_schema.as_ref())
                .map_err(datafusion_error_to_lix_error)?
                .alias(field.name()))
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let mut dataframe = session
        .execute_logical_plan(input)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    if !params.is_empty() {
        dataframe = dataframe
            .with_param_values(ParamValues::List(
                params.iter().map(scalar_value_from_lix_value).collect(),
            ))
            .map_err(datafusion_error_to_lix_error)?;
    }
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
        BoundWriteTarget::File(FileWriteSurface::Base | FileWriteSurface::ByBranch)
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

fn write_provider_selection(plan: &LogicalWritePlan, target_table_name: &str) -> ProviderSelection {
    // Bound VALUES, UPDATE, and DELETE expressions can reference only the
    // target surface. Query-backed inserts may read any visible surface, so
    // keep their existing catalog-wide registration until source selection is
    // derived from the bound query itself.
    match (&plan.bound.op, &plan.bound.input) {
        (BoundWriteOp::Insert, BoundWriteInput::Values(_))
        | (BoundWriteOp::Update | BoundWriteOp::Delete, BoundWriteInput::None) => {
            ProviderSelection::Only(BTreeSet::from([target_table_name.to_string()]))
        }
        _ => ProviderSelection::All,
    }
}

fn datafusion_dml_returning(
    session: &SessionContext,
    table_schema: &Schema,
    returning: Option<&BoundReturning>,
    params: &[Value],
) -> Result<Option<crate::sql2::providers::DmlReturning>, LixError> {
    let Some(returning) = returning else {
        return Ok(None);
    };
    let df_schema =
        DFSchema::try_from(table_schema.clone()).map_err(datafusion_error_to_lix_error)?;
    let props = session.state_ref().read().execution_props().clone();
    let mut fields = Vec::with_capacity(returning.items.len());
    let mut expressions = Vec::with_capacity(returning.items.len());
    let mut required_columns = BTreeSet::new();

    for item in &returning.items {
        let expr = datafusion_expr_from_bound_expr(session, &item.expr, params)?;
        let (_, inferred_field) = expr
            .to_field(&df_schema)
            .map_err(datafusion_error_to_lix_error)?;
        fields.push(
            Field::new(
                &item.output_name,
                inferred_field.data_type().clone(),
                inferred_field.is_nullable(),
            )
            .with_metadata(inferred_field.metadata().clone()),
        );
        expressions.push(
            datafusion::physical_expr::create_physical_expr(&expr, &df_schema, &props)
                .map_err(datafusion_error_to_lix_error)?,
        );
        bound_expr_column_names(&item.expr, &mut required_columns);
    }

    Ok(Some(crate::sql2::providers::DmlReturning::new(
        std::sync::Arc::new(Schema::new(fields)),
        expressions,
        required_columns,
    )))
}

fn bound_expr_column_names(expr: &BoundExpr, columns: &mut BTreeSet<String>) {
    match expr {
        BoundExpr::Column(column) => {
            columns.insert(column.name.clone());
        }
        BoundExpr::ExcludedColumn(_) | BoundExpr::Param(_) | BoundExpr::Literal(_) => {}
        BoundExpr::Cast { expr, .. } => bound_expr_column_names(expr, columns),
        BoundExpr::Function { args, .. } => {
            for arg in args {
                bound_expr_column_names(arg, columns);
            }
        }
        BoundExpr::Binary { left, right, .. } => {
            bound_expr_column_names(left, columns);
            bound_expr_column_names(right, columns);
        }
    }
}

fn sql_write_empty_returning_result(
    returning: Option<&crate::sql2::providers::DmlReturning>,
) -> Result<SqlWriteResult, LixError> {
    let Some(returning) = returning else {
        return Ok(SqlWriteResult::affected(0));
    };
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

fn sql_write_captured_returning_result(
    rows_affected: u64,
    returning: &crate::sql2::providers::DmlReturning,
) -> Result<SqlWriteResult, LixError> {
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
            let expr = datafusion_expr_from_bound_expr(session, &assignment.value, params)?
                .cast_to(field.data_type(), &df_schema)
                .map_err(datafusion_error_to_lix_error)?;
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
        std::sync::Arc<dyn datafusion::physical_expr::PhysicalExpr>,
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
    let df_schema = DFSchema::try_from(augmented).map_err(datafusion_error_to_lix_error)?;
    let props = session.state_ref().read().execution_props().clone();

    assignments
        .iter()
        .map(|assignment| {
            let field = schema
                .field_with_name(&assignment.column.name)
                .map_err(|error| LixError::unknown(format!("unknown conflict column: {error}")))?;
            let expr = datafusion_expr_from_bound_expr(session, &assignment.value, params)?
                .cast_to(field.data_type(), &df_schema)
                .map_err(datafusion_error_to_lix_error)?;
            let physical =
                datafusion::physical_expr::create_physical_expr(&expr, &df_schema, &props)
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
    let mut filters =
        datafusion_filters_from_predicate(session, schema, &plan.bound.predicate, params)?;
    if plan.bound.branch_scope == BranchScope::Global {
        let branch_column = if schema.field_with_name("branch_id").is_ok() {
            Some("branch_id")
        } else if schema.field_with_name("lixcol_branch_id").is_ok() {
            Some("lixcol_branch_id")
        } else {
            None
        };
        let Some(branch_column) = branch_column else {
            let df_schema =
                DFSchema::try_from(schema.clone()).map_err(datafusion_error_to_lix_error)?;
            for filter in &filters {
                validate_json_predicate_expr_with_dfschema(&df_schema, filter)?;
            }
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
    let df_schema = DFSchema::try_from(schema.clone()).map_err(datafusion_error_to_lix_error)?;
    for filter in &filters {
        validate_json_predicate_expr_with_dfschema(&df_schema, filter)?;
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
            let left_is_json = bound_expr_is_json(left, schema);
            let right_is_json = bound_expr_is_json(right, schema);
            Ok(vec![Expr::BinaryExpr(BinaryExpr::new(
                Box::new(datafusion_filter_expr_from_bound_expr(
                    session,
                    left,
                    params,
                    right_is_json,
                    is_identity_json_bound_expr(right),
                )?),
                Operator::Eq,
                Box::new(datafusion_filter_expr_from_bound_expr(
                    session,
                    right,
                    params,
                    left_is_json,
                    is_identity_json_bound_expr(left),
                )?),
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
            Box::new(datafusion_filter_expr_from_bound_expr(
                session, expr, params, false, false,
            )?),
            Box::new(datafusion_filter_expr_from_bound_expr(
                session, pattern, params, false, false,
            )?),
            *escape_char,
            *case_insensitive,
        ))]),
        BoundPredicate::IsNull(expr) => Ok(vec![Expr::IsNull(Box::new(
            datafusion_filter_expr_from_bound_expr(session, expr, params, false, false)?,
        ))]),
        BoundPredicate::IsNotNull(expr) => Ok(vec![Expr::IsNotNull(Box::new(
            datafusion_filter_expr_from_bound_expr(session, expr, params, false, false)?,
        ))]),
        BoundPredicate::In { expr, values } => {
            let expr_is_json = bound_expr_is_json(expr, schema);
            let values_include_json = values.iter().any(|value| bound_expr_is_json(value, schema));
            let expr_is_identity_json = is_identity_json_bound_expr(expr);
            let values_include_identity_json = values.iter().any(is_identity_json_bound_expr);
            Ok(vec![Expr::InList(InList::new(
                Box::new(datafusion_filter_expr_from_bound_expr(
                    session,
                    expr,
                    params,
                    values_include_json,
                    values_include_identity_json,
                )?),
                values
                    .iter()
                    .map(|value| {
                        datafusion_filter_expr_from_bound_expr(
                            session,
                            value,
                            params,
                            expr_is_json,
                            expr_is_identity_json,
                        )
                    })
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

fn datafusion_filter_expr_from_bound_expr(
    session: &SessionContext,
    expr: &BoundExpr,
    params: &[Value],
    json_comparison_context: bool,
    identity_json_comparison_context: bool,
) -> Result<Expr, LixError> {
    match expr {
        BoundExpr::Param(param) if json_comparison_context => {
            let Some(value) = params.get(param.index - 1) else {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("missing SQL parameter ${}", param.index),
                ));
            };
            let ScalarAndMetadata { value, metadata } = scalar_value_from_lix_value(value);
            if identity_json_comparison_context {
                if let ScalarValue::Utf8(Some(raw)) = &value {
                    return Ok(Expr::Literal(
                        ScalarValue::Utf8(Some(canonical_json_text(raw)?)),
                        Some(json_field_metadata()),
                    ));
                }
            }
            let metadata = metadata.or_else(|| match &value {
                ScalarValue::Utf8(Some(_)) => Some(json_field_metadata()),
                _ => None,
            });
            Ok(Expr::Literal(value, metadata))
        }
        BoundExpr::Literal(BoundLiteral::Text(value))
            if json_comparison_context && identity_json_comparison_context =>
        {
            Ok(Expr::Literal(
                ScalarValue::Utf8(Some(canonical_json_text(value)?)),
                Some(json_field_metadata()),
            ))
        }
        _ => datafusion_expr_from_bound_expr(session, expr, params),
    }
}

fn datafusion_expr_from_bound_expr(
    session: &SessionContext,
    expr: &BoundExpr,
    params: &[Value],
) -> Result<Expr, LixError> {
    match expr {
        BoundExpr::Column(column) => Ok(Expr::Column(Column::from_name(column.name.clone()))),
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
            let ScalarAndMetadata { value, metadata } = scalar_value_from_lix_value(value);
            Ok(Expr::Literal(value, metadata))
        }
        BoundExpr::Cast { expr, data_type } => {
            let data_type = match data_type {
                BoundCastType::Text => DataType::Utf8,
                BoundCastType::Binary => DataType::Binary,
                BoundCastType::BigInt => DataType::Int64,
                BoundCastType::Double => DataType::Float64,
                BoundCastType::Boolean => DataType::Boolean,
            };
            Ok(Expr::Cast(Cast::new(
                Box::new(datafusion_expr_from_bound_expr(session, expr, params)?),
                data_type,
            )))
        }
        BoundExpr::Function { name, args } => {
            let udf = session.udf(name).map_err(datafusion_error_to_lix_error)?;
            let args = args
                .iter()
                .map(|arg| datafusion_expr_from_bound_expr(session, arg, params))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Expr::ScalarFunction(ScalarFunction::new_udf(udf, args)))
        }
        BoundExpr::Binary { left, op, right } => Ok(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(datafusion_expr_from_bound_expr(session, left, params)?),
            match op {
                BoundBinaryOperator::Add => Operator::Plus,
                BoundBinaryOperator::Subtract => Operator::Minus,
                BoundBinaryOperator::Multiply => Operator::Multiply,
                BoundBinaryOperator::Divide => Operator::Divide,
                BoundBinaryOperator::Modulo => Operator::Modulo,
            },
            Box::new(datafusion_expr_from_bound_expr(session, right, params)?),
        ))),
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

fn bound_expr_is_json(expr: &BoundExpr, schema: &Schema) -> bool {
    match expr {
        BoundExpr::Column(column) | BoundExpr::ExcludedColumn(column) => schema
            .fields()
            .iter()
            .find(|field| field.name() == &column.name)
            .is_some_and(|field| field_is_json(field.as_ref())),
        BoundExpr::Literal(BoundLiteral::Json(_)) => true,
        BoundExpr::Function { name, .. } => matches!(name.as_str(), "lix_json" | "lix_json_get"),
        _ => false,
    }
}

fn is_identity_json_bound_expr(expr: &BoundExpr) -> bool {
    matches!(
        expr,
        BoundExpr::Column(column) | BoundExpr::ExcludedColumn(column)
            if matches!(column.name.as_str(), "entity_pk" | "lixcol_entity_pk")
    )
}

fn canonical_json_text(raw: &str) -> Result<String, LixError> {
    serde_json::from_str::<serde_json::Value>(raw)
        .map(|value| value.to_string())
        .map_err(|error| {
            LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                format!("JSON comparison value is not valid JSON: {error}"),
            )
        })
}

fn write_target_table_name(plan: &LogicalWritePlan) -> Result<String, LixError> {
    match &plan.bound.target {
        BoundWriteTarget::Entity(crate::sql2::bind::write::EntityWriteSurface::Base {
            schema_key,
        }) if bound_predicate_contains_like(&plan.bound.predicate)
            || bound_update_contains_binary(plan) =>
        {
            Ok(schema_key.clone())
        }
        BoundWriteTarget::Entity(crate::sql2::bind::write::EntityWriteSurface::ByBranch {
            schema_key,
        }) if bound_predicate_contains_like(&plan.bound.predicate)
            || bound_update_contains_binary(plan) =>
        {
            Ok(format!("{schema_key}_by_branch"))
        }
        BoundWriteTarget::File(FileWriteSurface::Base) => Ok("lix_file".to_string()),
        BoundWriteTarget::File(FileWriteSurface::ByBranch) => Ok("lix_file_by_branch".to_string()),
        BoundWriteTarget::Directory(DirectoryWriteSurface::Base) => Ok("lix_directory".to_string()),
        BoundWriteTarget::Directory(DirectoryWriteSurface::ByBranch) => {
            Ok("lix_directory_by_branch".to_string())
        }
        BoundWriteTarget::Branch => Ok("lix_branch".to_string()),
        BoundWriteTarget::DiffCommand(crate::sql2::DiffCommand::Revert) => {
            Ok("lix_revert".to_string())
        }
        BoundWriteTarget::DiffCommand(crate::sql2::DiffCommand::Apply) => {
            Ok("lix_apply".to_string())
        }
        BoundWriteTarget::DiffCommand(crate::sql2::DiffCommand::CreateCheckpoint) => {
            Ok("lix_create_checkpoint".to_string())
        }
        BoundWriteTarget::Entity(_) => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "sql2 DataFusion reference writer does not support this entity write",
        )),
    }
}

fn bound_update_contains_binary(plan: &LogicalWritePlan) -> bool {
    matches!(plan.bound.op, BoundWriteOp::Update)
        && (plan
            .bound
            .assignments
            .iter()
            .any(|assignment| bound_expr_contains_binary(&assignment.value))
            || bound_predicate_contains_binary(&plan.bound.predicate)
            || plan.bound.returning.as_ref().is_some_and(|returning| {
                returning
                    .items
                    .iter()
                    .any(|item| bound_expr_contains_binary(&item.expr))
            }))
}

fn bound_expr_contains_binary(expr: &BoundExpr) -> bool {
    match expr {
        BoundExpr::Binary { .. } => true,
        BoundExpr::Cast { expr, .. } => bound_expr_contains_binary(expr),
        BoundExpr::Function { args, .. } => args.iter().any(bound_expr_contains_binary),
        BoundExpr::Column(_)
        | BoundExpr::ExcludedColumn(_)
        | BoundExpr::Param(_)
        | BoundExpr::Literal(_) => false,
    }
}

fn bound_predicate_contains_binary(predicate: &BoundPredicate) -> bool {
    match predicate {
        BoundPredicate::Eq(left, right) => {
            bound_expr_contains_binary(left) || bound_expr_contains_binary(right)
        }
        BoundPredicate::Like { expr, pattern, .. } => {
            bound_expr_contains_binary(expr) || bound_expr_contains_binary(pattern)
        }
        BoundPredicate::IsNull(expr) | BoundPredicate::IsNotNull(expr) => {
            bound_expr_contains_binary(expr)
        }
        BoundPredicate::In { expr, values, .. } => {
            bound_expr_contains_binary(expr) || values.iter().any(bound_expr_contains_binary)
        }
        BoundPredicate::And(predicates) | BoundPredicate::Or(predicates) => {
            predicates.iter().any(bound_predicate_contains_binary)
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

fn validate_json_predicate_params(
    json_predicate_params: &BTreeSet<usize>,
    params: &[Value],
) -> Result<(), LixError> {
    for index in json_predicate_params {
        let Some(value) = params.get(index - 1) else {
            continue;
        };
        if !matches!(value, Value::Json(_) | Value::Null) {
            return Err(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                "JSON columns can only be compared with JSON expressions",
            )
            .with_hint("Use lix_json(...) or pass a JSON parameter value instead of bare text."));
        }
    }
    Ok(())
}

fn validate_parameter_count(plan: &LogicalPlan, param_count: usize) -> Result<(), LixError> {
    let parameter_names = plan
        .get_parameter_names()
        .map_err(datafusion_error_to_lix_error)?;
    let expected_count = expected_positional_parameter_count(&parameter_names)?;
    validate_parameter_count_values(expected_count, &parameter_names, param_count)
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
                args: Some(arguments),
                ..
            } = table_factor
            else {
                return ControlFlow::Continue(());
            };
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
                "Use Lix entity surfaces such as lix_registered_schema, lix_branch, lix_file, and lix_key_value instead of CREATE/DROP statements.",
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
        LogicalPlan::RecursiveQuery(_) => {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "recursive CTEs are not supported by Lix SQL",
            )
            .with_hint(
                "Use explicit commit graph surfaces such as lix_commit and lix_commit_edge, or a typed <schema>_history surface, instead of WITH RECURSIVE.",
            ));
        }
        _ => {}
    }

    for input in plan.inputs() {
        validate_supported_logical_plan(input)?;
    }

    Ok(())
}

fn scalar_value_from_lix_value(value: &Value) -> ScalarAndMetadata {
    match value {
        Value::Null => ScalarValue::Null.into(),
        Value::Boolean(value) => ScalarValue::Boolean(Some(*value)).into(),
        Value::Integer(value) => ScalarValue::Int64(Some(*value)).into(),
        Value::Real(value) => ScalarValue::Float64(Some(*value)).into(),
        Value::Text(value) => ScalarValue::Utf8(Some(value.clone())).into(),
        Value::Json(value) => ScalarAndMetadata::new(
            ScalarValue::Utf8(Some(value.to_string())),
            Some(json_field_metadata()),
        ),
        Value::Blob(value) => ScalarValue::LargeBinary(Some(value.to_vec())).into(),
    }
}

fn json_field_metadata() -> FieldMetadata {
    FieldMetadata::new(BTreeMap::from([(
        LIX_VALUE_TYPE_METADATA_KEY.to_string(),
        LIX_VALUE_TYPE_JSON.to_string(),
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
    let mut rows =
        Vec::<Vec<Value>>::with_capacity(batches.iter().map(RecordBatch::num_rows).sum::<usize>());
    for batch in batches {
        append_batch_rows(result_fields, batch, &mut rows)?;
    }

    Ok(SqlQueryResult {
        rows,
        columns: result_columns,
        notices: Vec::new(),
    })
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
enum ColumnCursor<'a> {
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
}

/// Whether a string column carries JSON payloads, decided once per batch from
/// the result field metadata rather than re-tested per cell.
#[derive(Clone, Copy, PartialEq, Eq)]
enum TextKind {
    Text,
    Json,
}

fn column_cursor<'a>(
    field: Option<&Field>,
    array: &'a dyn Array,
) -> Result<ColumnCursor<'a>, LixError> {
    let text_kind = if field.is_some_and(field_is_json) {
        TextKind::Json
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
                        match i64::try_from(value) {
                            Ok(value) => Value::Integer(value),
                            // Unsigned values past the signed range have no
                            // integer representation in a Lix row, so they are
                            // preserved exactly as decimal text.
                            Err(_) => Value::Text(value.to_string()),
                        }
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
        }
        Ok(())
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
        TextKind::Json => Value::Json(crate::Json::from_canonical_text(value)),
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
        execute_sql, query_result_from_batches, row_values_from_batch, write_provider_selection,
        write_session_options, write_target_table_name,
    };
    use crate::binary_cas::BlobDataReader;
    use crate::branch::BranchRefReader;
    use crate::changelog::{ChangeId, CommitId};
    use crate::commit_graph::{
        CommitGraphChangeHistoryRequest, CommitGraphNode, CommitGraphReader,
        ReachableCommitGraphNode,
    };
    use crate::common::LixTimestamp;
    use crate::functions::FunctionProviderHandle;
    use crate::json_store::JsonStoreContext;
    use crate::hot_state::{HotStateReader, HotStateScanRequest, MaterializedHotStateRow};
    use crate::sql2::{
        ChangelogQuerySource, EntitySnapshotReader, HistoryQuerySource, SqlChangelogQuerySource,
        SqlHistoryQuerySource,
    };
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
    use crate::{
        CreateBranchOptions, ExecuteResult, MergeBranchOptions, engine::Engine,
        session::SessionContext,
    };
    use crate::{LixError, NullableKeyFilter, Value};
    use bytes::Bytes;
    use datafusion::arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, NullArray, StringArray,
        StringViewArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::error::DataFusionError;
    use datafusion::physical_plan::RecordBatchStream;

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
                    u64::MAX
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
        let direct = query_result_from_batches(&fields, &[batch]).expect("direct typed conversion");

        assert_eq!(
            direct.columns,
            fields.iter().map(Field::name).cloned().collect::<Vec<_>>()
        );
        assert_eq!(direct.rows, generic_rows);
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
    struct RecordingEntitySnapshotReader {
        snapshots: Vec<Option<Bytes>>,
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
                Arc::new(UInt64Array::from(vec![Some(u64::MAX), None])),
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
                Value::Text(u64::MAX.to_string()),
                Value::Real(1.5),
                Value::Text("hello".to_owned()),
                Value::Json(crate::Json::from_canonical_text(r#"{"a":1}"#)),
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
        entity_pk: String,
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
                entity_pk: row
                    .entity_pk
                    .expect("captured staged row should carry entity_pk")
                    .as_json_array_text()
                    .expect("captured staged row should project entity_pk"),
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
        entity_snapshot_reader: Option<Arc<dyn EntitySnapshotReader>>,
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

        fn entity_snapshot_reader(&self) -> Option<Arc<dyn EntitySnapshotReader>> {
            self.entity_snapshot_reader.clone()
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

        fn history_query_source(
            &self,
            default_as_of_commit_id: String,
        ) -> SqlHistoryQuerySource<Self::ReadStore> {
            let storage = StorageAdapter::new(Memory::new());
            let read_scope = SharedStorageAdapterRead::new(test_read_scope(&storage));
            HistoryQuerySource {
                store: read_scope.clone(),
                json_reader: JsonStoreContext::new().reader(read_scope),
                certified_history_reader: None,
                default_as_of_commit_id,
            }
        }

        fn changelog_query_source(&self) -> SqlChangelogQuerySource<Self::ReadStore> {
            let storage = StorageAdapter::new(Memory::new());
            let read_scope = SharedStorageAdapterRead::new(test_read_scope(&storage));
            ChangelogQuerySource {
                store: read_scope.clone(),
                json_reader: JsonStoreContext::new().reader(read_scope),
            }
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

        async fn execute_diff_command(
            &mut self,
            _command: crate::sql2::DiffCommand,
            diff_ids: Vec<String>,
        ) -> Result<crate::sql2::DiffCommandOutcome, LixError> {
            Ok(crate::sql2::DiffCommandOutcome {
                rows_affected: diff_ids.len() as u64,
                commit_id: (!diff_ids.is_empty()).then(|| "commit-diff-command".to_string()),
            })
        }

        fn staged_commit_id(&self, _branch_id: &str) -> Result<Option<String>, LixError> {
            Ok(Some("commit-diff-command".to_string()))
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
            let selection = write_provider_selection(&plan.plan, &table_name);

            assert_eq!(
                selection,
                crate::sql2::providers::ProviderSelection::Only(BTreeSet::from([
                    "lix_file".to_string()
                ])),
                "{sql}"
            );

            let session = build_write_session_with_options(
                &mut ctx,
                write_session_options(&plan.plan),
                &selection,
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
    async fn query_backed_insert_keeps_catalog_wide_provider_registration() {
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
        let selection = write_provider_selection(&insert_select.plan, &table_name);

        assert_eq!(selection, crate::sql2::providers::ProviderSelection::All,);

        let session = build_write_session_with_options(
            &mut ctx,
            write_session_options(&insert_select.plan),
            &selection,
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

        assert_eq!(
            table_names,
            vec![
                "lix_apply",
                "lix_branch",
                "lix_create_checkpoint",
                "lix_directory",
                "lix_directory_by_branch",
                "lix_file",
                "lix_file_by_branch",
                "lix_revert",
            ]
        );
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

    #[tokio::test]
    async fn diff_command_returning_executes_through_datafusion() {
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader: Arc::new(StaticBlobReader { bytes: Vec::new() }),
            hot_state: Arc::new(CapturingRowsHotStateReader {
                rows: Vec::new(),
                requests: Arc::new(Mutex::new(Vec::new())),
            }),
            staged_writes: Arc::new(Mutex::new(CapturingStagedWrites::default())),
            schema_definitions: Vec::new(),
        };
        let plan = create_write_logical_plan(
            &mut ctx,
            "INSERT INTO lix_revert (diff_id) \
             SELECT diff_id FROM (VALUES ('d1.test')) AS selected(diff_id) \
             RETURNING commit_id",
        )
        .await
        .expect("diff command RETURNING should plan");
        let (result, path) = crate::sql2::execute_write_logical_plan_with_mode_and_trace_result(
            &mut ctx,
            plan,
            &[],
            WriteExecutorMode::ForceDataFusion,
        )
        .await
        .expect("diff command RETURNING should execute through DataFusion");

        assert_eq!(path, WriteExecutorPath::DataFusion);
        assert_eq!(result.rows_affected, 1);
        assert_eq!(
            result.returning,
            Some(crate::SqlQueryResult {
                columns: vec!["commit_id".to_string()],
                rows: vec![vec![Value::Text("commit-diff-command".to_string())]],
                notices: Vec::new(),
            })
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

        async fn change_history_from_commit(
            &mut self,
            _start_commit_id: &CommitId,
            _request: &CommitGraphChangeHistoryRequest,
        ) -> Result<crate::commit_graph::CommitGraphHistory, LixError> {
            Ok(crate::commit_graph::CommitGraphHistory {
                entries: Vec::new(),
                reachable_nodes: Arc::from([]),
            })
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
                    && (request.filter.entity_pks.is_empty()
                        || request.filter.entity_pks.contains(&row.entity_pk))
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
    impl EntitySnapshotReader for RecordingEntitySnapshotReader {
        async fn scan_entity_snapshots(
            &self,
            request: HotStateScanRequest,
        ) -> Result<Option<Vec<Option<Bytes>>>, LixError> {
            self.requests
                .lock()
                .expect("captured snapshot requests lock")
                .push(request);
            Ok(Some(self.snapshots.clone()))
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

    fn live_entity_row(entity_pk: &str, branch_id: &str, value: &str) -> MaterializedHotStateRow {
        MaterializedHotStateRow {
            entity_pk: crate::entity_pk::EntityPk::single(entity_pk),
            schema_key: "test_state_schema".to_string(),
            file_id: None,
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}").into()),
            metadata: Some(json!({ "source": entity_pk }).to_string().into()),
            deleted: false,
            branch_id: branch_id.into(),
            change_id: Some(ChangeId::for_test_label(&format!("change-{entity_pk}"))),
            commit_id: Some(CommitId::for_test_label(&format!("commit-{entity_pk}"))),
            global: false,
            untracked: false,
            created_at: LixTimestamp::expect_parse("test created_at", "2026-04-23T00:00:00Z"),
            updated_at: LixTimestamp::expect_parse("test updated_at", "2026-04-23T01:00:00Z"),
        }
    }

    fn live_test_state_row(
        entity_pk: &str,
        branch_id: &str,
        value: &str,
        untracked: bool,
    ) -> MaterializedHotStateRow {
        let mut row = live_entity_row(entity_pk, branch_id, value);
        row.snapshot_content = Some(
            json!({ "id": entity_pk, "value": value })
                .to_string()
                .into(),
        );
        row.untracked = untracked;
        row
    }

    fn live_directory_row(
        entity_pk: &str,
        branch_id: &str,
        parent_id: Option<&str>,
        name: &str,
    ) -> MaterializedHotStateRow {
        MaterializedHotStateRow {
            entity_pk: crate::entity_pk::EntityPk::uuid_from_canonical(entity_pk)
                .expect("fixture directory ID should be a UUID"),
            schema_key: "lix_directory_descriptor".to_string(),
            file_id: None,
            snapshot_content: Some(
                json!({
                    "id": entity_pk,
                    "parent_id": parent_id,
                    "name": name
                })
                .to_string()
                .into(),
            ),
            metadata: Some(json!({ "source": entity_pk }).to_string().into()),
            deleted: false,
            branch_id: branch_id.into(),
            change_id: Some(ChangeId::for_test_label(&format!("change-{entity_pk}"))),
            commit_id: Some(CommitId::for_test_label(&format!("commit-{entity_pk}"))),
            global: false,
            untracked: false,
            created_at: LixTimestamp::expect_parse("test created_at", "2026-04-23T00:00:00Z"),
            updated_at: LixTimestamp::expect_parse("test updated_at", "2026-04-23T01:00:00Z"),
        }
    }

    fn live_file_row(
        entity_pk: &str,
        branch_id: &str,
        directory_id: Option<&str>,
        name: &str,
    ) -> MaterializedHotStateRow {
        MaterializedHotStateRow {
            entity_pk: crate::entity_pk::EntityPk::uuid_from_canonical(entity_pk)
                .expect("fixture file ID should be a UUID"),
            schema_key: "lix_file_descriptor".to_string(),
            file_id: Some(entity_pk.to_string()),
            snapshot_content: Some(
                json!({
                    "id": entity_pk,
                    "directory_id": directory_id,
                    "name": name
                })
                .to_string()
                .into(),
            ),
            metadata: Some(json!({ "source": entity_pk }).to_string().into()),
            deleted: false,
            branch_id: branch_id.into(),
            change_id: Some(ChangeId::for_test_label(&format!("change-{entity_pk}"))),
            commit_id: Some(CommitId::for_test_label(&format!("commit-{entity_pk}"))),
            global: false,
            untracked: false,
            created_at: LixTimestamp::expect_parse("test created_at", "2026-04-23T00:00:00Z"),
            updated_at: LixTimestamp::expect_parse("test updated_at", "2026-04-23T01:00:00Z"),
        }
    }

    fn live_blob_ref_row(
        entity_pk: &str,
        branch_id: &str,
        bytes: &[u8],
    ) -> MaterializedHotStateRow {
        MaterializedHotStateRow {
            entity_pk: crate::entity_pk::EntityPk::uuid_from_canonical(entity_pk)
                .expect("fixture blob-ref ID should be a UUID"),
            schema_key: "lix_binary_blob_ref".to_string(),
            file_id: Some(entity_pk.to_string()),
            snapshot_content: Some(
                json!({
                    "id": entity_pk,
                    "blob_hash": crate::binary_cas::BlobId::from_content(bytes).to_hex(),
                    "size_bytes": bytes.len()
                })
                .to_string()
                .into(),
            ),
            metadata: Some(json!({ "source": entity_pk }).to_string().into()),
            deleted: false,
            branch_id: branch_id.into(),
            change_id: Some(ChangeId::for_test_label(&format!(
                "change-{entity_pk}-blob"
            ))),
            commit_id: Some(CommitId::for_test_label(&format!(
                "commit-{entity_pk}-blob"
            ))),
            global: false,
            untracked: false,
            created_at: LixTimestamp::expect_parse("test created_at", "2026-04-23T00:00:00Z"),
            updated_at: LixTimestamp::expect_parse("test updated_at", "2026-04-23T01:00:00Z"),
        }
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
            entity_snapshot_reader: None,
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
            entity_snapshot_reader: None,
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
        let component_types = [crate::entity_pk::EntityPkComponentType::Integer];
        let entity_pk = crate::entity_pk::EntityPk::from_external_parts(
            vec!["42".to_string()],
            &component_types,
        )
        .expect("integer fixture identity should encode");
        let mut row = live_entity_row("42", branch_id, "answer");
        row.entity_pk = entity_pk.clone();
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
            entity_snapshot_reader: None,
            schema_definitions: vec![json!({
                "x-lix-key": "integer_state_schema",
                "x-lix-primary-key": ["/id"],
                "type": "object",
                "properties": {
                    "id": { "type": "integer" },
                    "value": { "type": "string" }
                },
                "required": ["id", "value"],
                "additionalProperties": false
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
        assert_eq!(request.filter.entity_pks, vec![entity_pk]);
    }

    #[tokio::test]
    async fn datafusion_entity_primary_key_read_materializes_public_result() {
        let sql = "SELECT id, value FROM test_state_schema \
                   WHERE id IN ('entity-b', 'entity-a') ORDER BY id";
        let ctx = DummySqlExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader: Arc::new(DummyBlobReader),
            hot_state: Arc::new(RowsHotStateReader {
                rows: vec![
                    live_test_state_row(
                        "entity-b",
                        "01920000-0000-7000-8000-0000000000a1",
                        "B",
                        false,
                    ),
                    live_test_state_row(
                        "entity-a",
                        "01920000-0000-7000-8000-0000000000a1",
                        "A",
                        false,
                    ),
                ],
            }),
            entity_snapshot_reader: None,
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "x-lix-primary-key": ["/id"],
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "value": { "type": "string" }
                },
                "required": ["id", "value"],
                "additionalProperties": false
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
                    Value::Text("entity-a".to_string()),
                    Value::Text("A".to_string())
                ],
                vec![
                    Value::Text("entity-b".to_string()),
                    Value::Text("B".to_string())
                ],
            ]
        );
    }

    #[tokio::test]
    async fn datafusion_entity_primary_key_read_uses_registered_provider() {
        let sql = "SELECT id, value FROM test_state_schema \
                   WHERE id IN ('entity-b', 'entity-a', 'entity-b') ORDER BY id";
        let requests = Arc::new(Mutex::new(Vec::new()));
        let snapshot_reader = Arc::new(RecordingEntitySnapshotReader {
            snapshots: vec![
                Some(Bytes::from_static(br#"{"id":"entity-a","value":"A"}"#)),
                Some(Bytes::from_static(br#"{"id":"entity-b","value":"B"}"#)),
            ],
            requests: Arc::clone(&requests),
        });
        let scans = Arc::new(AtomicUsize::new(0));
        let ctx = DummySqlExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader: Arc::new(DummyBlobReader),
            hot_state: Arc::new(CountingRowsHotStateReader {
                rows: vec![
                    live_test_state_row(
                        "entity-b",
                        "01920000-0000-7000-8000-0000000000a1",
                        "B",
                        false,
                    ),
                    live_test_state_row(
                        "entity-a",
                        "01920000-0000-7000-8000-0000000000a1",
                        "A",
                        false,
                    ),
                ],
                scans: Arc::clone(&scans),
            }),
            entity_snapshot_reader: Some(snapshot_reader),
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "x-lix-primary-key": ["/id"],
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "value": { "type": "string" }
                },
                "required": ["id", "value"],
                "additionalProperties": false
            })],
        };
        let result = execute_sql(&ctx, sql, &[])
            .await
            .expect("DataFusion primary-key read should execute");

        assert_eq!(
            result.rows,
            vec![
                vec![
                    Value::Text("entity-a".to_string()),
                    Value::Text("A".to_string())
                ],
                vec![
                    Value::Text("entity-b".to_string()),
                    Value::Text("B".to_string())
                ],
            ]
        );
        // A `WHERE` clause that resolves to a complete entity identity set is
        // applied in full by the `entity_pks` access path, so this read must
        // take the direct point-snapshot route rather than the generic
        // visibility scan.
        //
        // This assertion pair previously read `scans == 1` and
        // `requests.is_empty()` — the exact opposite — and its message called
        // the snapshot reader "the deleted native snapshot route". That wording
        // predates the current `EntitySnapshotReader`, which is a live route
        // registered from `SqlExecutionContext::entity_snapshot_reader` and
        // backed by the entity point-snapshot cache. The old expectation froze
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
            "an exact identity point read must consult the entity point-snapshot route exactly once"
        );
        assert_eq!(
            requests[0].filter.schema_keys,
            vec!["test_state_schema".to_string()],
            "the point-snapshot request must be scoped to the queried schema"
        );
        assert_eq!(
            requests[0].filter.entity_pks,
            vec![
                crate::entity_pk::EntityPk::single("entity-a"),
                crate::entity_pk::EntityPk::single("entity-b"),
            ],
            "the repeated 'entity-b' in the IN list must collapse into a deduplicated, \
             ordered identity set rather than being pushed down three times"
        );
    }

    #[tokio::test]
    async fn datafusion_entity_left_join_preserves_matches_and_null_extension() {
        let sql = r#"SELECT "bundle"."id" AS "bundleId",
                         "message"."id" AS "messageId",
                         "variant"."id" AS "variantId",
                         "variant"."pattern" AS "variantPattern"
                    FROM "bundle"
                    LEFT JOIN "message" ON "message"."bundleId" = "bundle"."id"
                    LEFT JOIN "variant" ON "variant"."messageId" = "message"."id"
                   WHERE "bundle"."id" = $1"#;
        let row = |schema_key: &str, entity_pk: &str, snapshot: &str| {
            let mut row = live_entity_row(entity_pk, "01920000-0000-7000-8000-0000000000a1", "");
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
                    row("message", "true", r#"{"id":true,"bundleId":"b1"}"#),
                    row("message", "m2", r#"{"id":"m2","bundleId":"b1"}"#),
                    row(
                        "variant",
                        "v1",
                        r#"{"id":"v1","messageId":true,"pattern":"Hello"}"#,
                    ),
                ],
            }),
            entity_snapshot_reader: None,
            schema_definitions: vec![
                json!({
                    "x-lix-key": "bundle",
                    "x-lix-primary-key": ["/id"],
                    "type": "object",
                    "properties": { "id": { "type": "string" } },
                    "required": ["id"],
                    "additionalProperties": false
                }),
                json!({
                    "x-lix-key": "message",
                    "x-lix-primary-key": ["/id"],
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" },
                        "bundleId": { "type": "string" }
                    },
                    "required": ["id", "bundleId"],
                    "additionalProperties": false
                }),
                json!({
                    "x-lix-key": "variant",
                    "x-lix-primary-key": ["/id"],
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" },
                        "messageId": { "type": "string" },
                        "pattern": { "type": "string" }
                    },
                    "required": ["id", "messageId", "pattern"],
                    "additionalProperties": false
                }),
            ],
        };
        let result = execute_sql(&ctx, sql, &[Value::Text("b1".to_string())])
            .await
            .expect("DataFusion entity join should execute");

        assert_eq!(
            result.columns,
            ["bundleId", "messageId", "variantId", "variantPattern"]
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
            entity_snapshot_reader: None,
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
                 lix_json('{\"x-lix-key\":\"aggregate_filter_test\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"value\":{\"type\":\"integer\"}},\"required\":[\"id\",\"value\"],\"additionalProperties\":false}'),\
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
            entity_snapshot_reader: None,
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
            error.details,
            Some(json!({
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
            entity_snapshot_reader: None,
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

    async fn setup_engine_history_fixture() -> Result<(SessionContext, String), LixError> {
        let storage = Memory::new();
        let init_receipt = Engine::initialize(storage.clone()).await?;
        let engine = Engine::new(storage).await?;
        let session = engine.open_session_at(init_receipt.main_branch_id).await?;

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"test_state_schema\",\"type\":\"object\",\"properties\":{\"value\":{\"type\":\"string\"},\"count\":{\"type\":\"integer\"}},\"required\":[\"value\",\"count\"],\"additionalProperties\":false}'),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await?;
        session
            .execute(
                "INSERT INTO test_state_schema \
	             (lixcol_entity_pk, value, count, lixcol_metadata, lixcol_untracked) \
	             VALUES (lix_json('[\"entity-history\"]'), 'A', 7, '{\"source\":\"history\"}', false)",
                &[],
            )
            .await?;
        session
            .execute(
                "INSERT INTO lix_directory (id, path) \
                 VALUES ('01920000-0000-7000-8000-0000000000d3', '/docs')",
                &[],
            )
            .await?;
        session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('01920000-0000-7000-8000-0000000000a2', '/docs/readme.md', CAST('hello' AS BYTEA))",
                &[],
            )
            .await?;

        let active_branch_id = session.active_branch_id().await?;
        let head_commit_id = engine
            .load_branch_head_commit_id(&active_branch_id)
            .await?
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "history fixture expected the session branch to have a head commit",
                )
            })?;
        Ok((session, head_commit_id))
    }

    #[tokio::test]
    async fn whole_entity_collection_delete_uses_one_generation_fact_and_allows_recreation() {
        let storage = Memory::new();
        let init_receipt = Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(storage).await.expect("engine should open");
        let branch_id = init_receipt.main_branch_id.clone();
        let session = engine
            .open_session_at(init_receipt.main_branch_id)
            .await
            .expect("session should open");
        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"test_state_schema\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"value\":{\"type\":\"string\"}},\"required\":[\"id\",\"value\"],\"additionalProperties\":false,\"x-lix-primary-key\":[\"/id\"]}'),\
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
        let before_delete = engine
            .load_branch_head_commit_id(&branch_id)
            .await
            .expect("head before delete should load")
            .expect("head before delete should exist");

        let deleted = session
            .execute("DELETE FROM test_state_schema", &[])
            .await
            .expect("whole collection should delete");
        assert_eq!(deleted.rows_affected(), 3);
        let after_delete = engine
            .load_branch_head_commit_id(&branch_id)
            .await
            .expect("head after delete should load")
            .expect("head after delete should exist");
        let diff = session
            .execute(
                &format!(
                    "SELECT schema_key, diff_type FROM lix_diff('{before_delete}', '{after_delete}')"
                ),
                &[],
            )
            .await
            .expect("collection delete diff should query");
        assert_eq!(
            rows_from_execute_result(diff).1,
            vec![vec![
                Value::Text("lix_collection_generation".to_string()),
                Value::Text("added".to_string())
            ]]
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
            .expect("entity tombstone changelog should query")
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
    async fn whole_entity_collection_delete_is_visible_inside_explicit_transaction() {
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
                 lix_json('{\"x-lix-key\":\"test_state_schema\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false,\"x-lix-primary-key\":[\"/id\"]}'),\
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
    async fn whole_entity_collection_delete_falls_back_for_global_members() {
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
                 ('local-only', lix_json('1'), false), \
                 ('global-only', lix_json('2'), true), \
                 ('shadowed', lix_json('3'), true), \
                 ('shadowed', lix_json('4'), false)",
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
             lix_json('{\"x-lix-key\":\"test_state_schema\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false,\"x-lix-primary-key\":[\"/id\"]}'),\
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
            .expect("merged entity tombstones should query")
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
    async fn execute_sql_reads_entity_history_view_from_history_context() {
        let (session, head_commit_id) = setup_engine_history_fixture()
            .await
            .expect("history fixture should initialize");
        let result = session
            .execute(
                &format!(
                    "SELECT value, count, lixcol_entity_pk, lixcol_depth \
	             FROM test_state_schema_history('{head_commit_id}') \
	             WHERE lixcol_entity_pk = lix_json('[\"entity-history\"]')"
                ),
                &[],
            )
            .await
            .expect("sql2 execute should read entity history through real engine context");
        let (columns, rows) = rows_from_execute_result(result);

        assert_eq!(
            columns,
            vec!["value", "count", "lixcol_entity_pk", "lixcol_depth",]
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Text("A".to_string()));
        assert_eq!(rows[0][1], Value::Integer(7));
        assert_eq!(rows[0][2], Value::Json(json!(["entity-history"]).into()));
        assert!(matches!(rows[0][3], Value::Integer(_)));
    }

    #[tokio::test]
    async fn execute_sql_reads_directory_history_view_from_history_context() {
        let (session, head_commit_id) = setup_engine_history_fixture()
            .await
            .expect("history fixture should initialize");
        let result = session
            .execute(
                &format!(
                    "SELECT id, parent_id, name, path, lixcol_depth \
             FROM lix_directory_history('{head_commit_id}') \
             WHERE id = '01920000-0000-7000-8000-0000000000d3'"
                ),
                &[],
            )
            .await
            .expect("sql2 execute should read directory history through real engine context");
        assert!(
            result.notices().is_empty(),
            "identity-filtered directory history should not emit soft notices"
        );
        let (columns, rows) = rows_from_execute_result(result);

        assert_eq!(
            columns,
            vec!["id", "parent_id", "name", "path", "lixcol_depth",]
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0][0],
            Value::Text("01920000-0000-7000-8000-0000000000d3".to_string())
        );
        assert_eq!(rows[0][1], Value::Null);
        assert_eq!(rows[0][2], Value::Text("docs".to_string()));
        assert_eq!(rows[0][3], Value::Text("/docs".to_string()));
        assert!(matches!(rows[0][4], Value::Integer(_)));

        let name_filtered_result = session
            .execute(
                &format!(
                    "SELECT id \
             FROM lix_directory_history('{head_commit_id}') \
             WHERE name = 'docs'"
                ),
                &[],
            )
            .await
            .expect("name-filtered directory history should execute");
        assert!(
            name_filtered_result.notices().is_empty(),
            "ordinary SQL predicates should not emit identity heuristics"
        );
    }

    #[tokio::test]
    async fn execute_sql_reads_file_history_view_from_history_context() {
        let (session, head_commit_id) = setup_engine_history_fixture()
            .await
            .expect("history fixture should initialize");
        let result = session
            .execute(
                &format!(
                    "SELECT id, path, content, lixcol_depth \
             FROM lix_file_history('{head_commit_id}') \
             WHERE id = '01920000-0000-7000-8000-0000000000a2' \
               AND content IS NOT NULL \
             ORDER BY lixcol_depth",
                ),
                &[],
            )
            .await
            .expect("sql2 execute should read file history through real engine context");
        assert!(
            result.notices().is_empty(),
            "identity-filtered file history should not emit soft notices"
        );
        let (columns, rows) = rows_from_execute_result(result);

        assert_eq!(columns, vec!["id", "path", "content", "lixcol_depth",]);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0][0],
            Value::Text("01920000-0000-7000-8000-0000000000a2".to_string())
        );
        assert_eq!(rows[0][1], Value::Text("/docs/readme.md".to_string()));
        assert_eq!(rows[0][2], Value::Blob(b"hello".to_vec().into()));
        assert!(matches!(rows[0][3], Value::Integer(_)));

        let path_filtered_result = session
            .execute(
                &format!(
                    "SELECT id \
             FROM lix_file_history('{head_commit_id}') \
             WHERE path = '/docs/readme.md'"
                ),
                &[],
            )
            .await
            .expect("path-filtered file history should execute");
        assert!(
            path_filtered_result.notices().is_empty(),
            "ordinary SQL predicates should not emit identity heuristics"
        );
    }

    /// Connectivity check for the per-commit touched-scope digest.
    ///
    /// This asserts the *route*, not the timing: that the digest is consulted
    /// once per reached commit, that it actually prunes commits the query
    /// cannot need, and that it never falls back to the pre-digest path on a
    /// repository this build wrote. Four optimizations in earlier rounds
    /// measured flat because the changed code never ran; a timing sweep cannot
    /// tell that apart from "the optimization does not help".
    #[tokio::test]
    async fn file_history_consults_the_touched_scope_digest_per_commit() {
        let (session, _) = setup_engine_history_fixture()
            .await
            .expect("history fixture should initialize");

        // Commits that touch a different file. A history read for
        // `/docs/readme.md` still reaches every one of them, but none can
        // contribute a row.
        session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('01920000-0000-7000-8000-0000000000b7', '/docs/noise.md', CAST('n0' AS BYTEA))",
                &[],
            )
            .await
            .expect("noise file should insert");
        for revision in 0..8 {
            session
                .execute(
                    &format!(
                        "UPDATE lix_file SET content = CAST('n{revision}' AS BYTEA) \
                         WHERE id = '01920000-0000-7000-8000-0000000000b7'"
                    ),
                    &[],
                )
                .await
                .expect("noise commit should apply");
        }

        let head_commit_id = session
            .execute("SELECT lix_active_branch_commit_id()", &[])
            .await
            .expect("active commit should resolve")
            .rows()[0]
            .values()[0]
            .clone();
        let Value::Text(head_commit_id) = head_commit_id else {
            panic!("active branch commit id should be text");
        };

        let before = crate::commit_graph::scope_digest_census();
        let _ = crate::commit_graph::scope_digest_census::by_projection::take();
        let result = session
            .execute(
                &format!(
                    "SELECT id, path, lixcol_depth FROM lix_file_history('{head_commit_id}') \
                     WHERE path = '/docs/readme.md' ORDER BY lixcol_depth"
                ),
                &[],
            )
            .await
            .expect("by-path history should execute");
        let census = crate::commit_graph::scope_digest_census().since(&before);
        let by_projection = crate::commit_graph::scope_digest_census::by_projection::take();
        eprintln!("scope_digest_census {census:?}");
        for (projection, buckets) in &by_projection {
            eprintln!("scope_digest_projection {projection} {buckets:?}");
        }

        assert!(
            !result.rows().is_empty(),
            "the queried path must still have history rows"
        );
        assert!(
            census.probed() > 0,
            "the digest must be consulted at least once per history read: {census:?}"
        );
        assert!(
            census.pruned > 0,
            "the digest must skip commits that cannot contribute rows: {census:?}"
        );
        assert!(
            census.loaded_present > 0 || census.loaded_opaque > 0,
            "commits that can contribute must still be loaded: {census:?}"
        );
        assert_eq!(
            census.loaded_absent, 0,
            "a repository written by this build must carry a digest on every commit: {census:?}"
        );

        // Requirement: the digest must serve every projection of history-by-path,
        // not just the one a benchmark happens to exercise. Each of these is a
        // separate commit-graph traversal with its own schema-key set.
        for projection in [
            "lix_binary_blob_ref+lix_file_descriptor",
            "lix_directory_descriptor",
            "lix_key_value",
        ] {
            let buckets = by_projection.get(projection).unwrap_or_else(|| {
                panic!("by-path history should traverse {projection}: {by_projection:?}")
            });
            assert!(
                buckets.get("pruned").copied().unwrap_or(0) > 0,
                "{projection} must be able to prune commits: {by_projection:?}"
            );
            assert_eq!(
                buckets.get("loaded_absent").copied().unwrap_or(0),
                0,
                "{projection} must never hit the pre-digest fallback: {by_projection:?}"
            );
        }
    }

    #[tokio::test]
    async fn execute_sql_rejects_writes_to_history_views_before_planning() {
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
                    "x-lix-key": "test_state_schema",
                    "x-lix-primary-key": ["/id"],
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" },
                        "value": { "type": "string" }
                    },
                    "required": ["id", "value"],
                    "additionalProperties": false
                })],
            };

            let error = execute_write_sql(&mut ctx, sql, &[])
                .await
                .expect_err("history views are read-only");

            assert_eq!(error.code, LixError::CODE_READ_ONLY, "{sql}");
            assert_eq!(
                error.message, "DML cannot write read-only SQL table 'test_state_schema_history'",
                "{sql}"
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
        assert_eq!(
            rows[0].entity_pk,
            "[\"01920000-0000-7000-8000-000000000312\"]"
        );
        assert_eq!(rows[0].branch_id, "01920000-0000-7000-8000-0000000000a1");
    }

    #[tokio::test]
    async fn execute_sql_insert_into_entity_by_branch_stages_write() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(DummyHotStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO test_state_schema_by_branch (\
	     lixcol_entity_pk, lixcol_branch_id, value\
	     ) VALUES (lix_json('[\"entity-c\"]'), '01920000-0000-7000-8000-0000000000b1', 'C')",
            &[],
        )
        .await
        .expect("INSERT INTO entity by-branch surface should stage write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "test_state_schema");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"entity-c\"]");
        assert_eq!(rows[0].branch_id, "01920000-0000-7000-8000-0000000000b1");
        assert!(!rows[0].global);
        assert!(!rows[0].untracked);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"C\"}")
        );
    }

    #[tokio::test]
    async fn execute_sql_insert_into_entity_by_branch_accepts_parameterized_branch_id() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(DummyHotStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO test_state_schema_by_branch (\
             lixcol_entity_pk, lixcol_branch_id, value\
             ) VALUES (lix_json('[\"entity-c\"]'), $1, 'C')",
            &[Value::Text(
                "01920000-0000-7000-8000-0000000000b1".to_string(),
            )],
        )
        .await
        .expect("parameterized by-branch entity insert should stage write");

        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "test_state_schema");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"entity-c\"]");
        assert_eq!(rows[0].branch_id, "01920000-0000-7000-8000-0000000000b1");
    }

    #[tokio::test]
    async fn execute_sql_insert_into_active_entity_defaults_active_branch() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(DummyHotStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO test_state_schema (lixcol_entity_pk, value) \
	     VALUES (lix_json('[\"entity-c\"]'), 'C')",
            &[],
        )
        .await
        .expect("INSERT INTO active entity surface should stage write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "test_state_schema");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"entity-c\"]");
        assert_eq!(rows[0].branch_id, "01920000-0000-7000-8000-0000000000a1");
        assert!(!rows[0].global);
        assert!(!rows[0].untracked);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"C\"}")
        );
    }

    #[tokio::test]
    async fn execute_sql_insert_default_values_uses_the_native_entity_writer() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(DummyHotStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![json!({
                "x-lix-key": "default_values_probe",
                "x-lix-primary-key": ["/id"],
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string",
                        "x-lix-default": "lix_uuid_v7()"
                    },
                    "label": { "type": "string", "default": "untitled" }
                },
                "required": ["id", "label"],
                "additionalProperties": false
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
                .expect("inserted entity should have a snapshot"),
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
    async fn execute_sql_insert_into_active_entity_does_not_probe_active_head_during_lowering() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(DummyHotStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "missing-branch",
            blob_reader,
            hot_state,
            staged_writes,
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO test_state_schema (lixcol_entity_pk, value) \
             VALUES (lix_json('[\"entity-c\"]'), 'C')",
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
    async fn execute_sql_noop_active_entity_write_does_not_probe_active_head() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(DummyHotStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "missing-branch",
            blob_reader,
            hot_state,
            staged_writes,
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
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
    async fn execute_sql_entity_upsert_conflict_scan_is_narrowed_to_inserted_identity() {
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
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "x-lix-primary-key": ["/id"],
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "value": { "type": "string" }
                },
                "required": ["id", "value"],
                "additionalProperties": false
            })],
        };

        let (result, path) = execute_write_sql_trace(
            &mut ctx,
            "INSERT INTO test_state_schema_by_branch \
             (id, value, lixcol_branch_id, lixcol_untracked) \
             VALUES ('target', 'new', '01920000-0000-7000-8000-0000000000b1', true) \
             ON CONFLICT(id, lixcol_branch_id) DO UPDATE SET value = excluded.value",
            &[],
            WriteExecutorMode::Auto,
        )
        .await
        .expect("entity upsert should update the matching row");

        assert_eq!(path, WriteExecutorPath::Fast);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);
        let requests = requests.lock().expect("captured requests lock");
        assert_eq!(requests.len(), 1);
        let filter = &requests[0].filter;
        assert_eq!(filter.schema_keys, vec!["test_state_schema"]);
        assert_eq!(
            filter.entity_pks,
            vec![crate::entity_pk::EntityPk::single("target")]
        );
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
        assert_eq!(rows[0].entity_pk, "[\"target\"]");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"id\":\"target\",\"value\":\"new\"}")
        );
    }

    #[tokio::test]
    async fn integer_primary_key_update_and_delete_narrow_candidate_scans() {
        let branch_id = "01920000-0000-7000-8000-0000000000a1";
        let component_types = [crate::entity_pk::EntityPkComponentType::Integer];
        let entity_pk = crate::entity_pk::EntityPk::from_external_parts(
            vec!["42".to_string()],
            &component_types,
        )
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
            let mut row = live_entity_row("42", branch_id, "before");
            row.entity_pk = entity_pk.clone();
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
                    "x-lix-key": "integer_state_schema",
                    "x-lix-primary-key": ["/id"],
                    "type": "object",
                    "properties": {
                        "id": { "type": "integer" },
                        "value": { "type": "string" }
                    },
                    "required": ["id", "value"],
                    "additionalProperties": false
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
            assert_eq!(request.filter.entity_pks, vec![entity_pk.clone()], "{sql}");
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
                    && request.filter.entity_pks.is_empty()
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
                request.filter.entity_pks,
                vec![
                    crate::entity_pk::EntityPk::uuid_from_canonical(
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
            descriptor_rows[0].entity_pk,
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
            blob_ref_rows[0].entity_pk,
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
    async fn execute_sql_insert_into_directory_by_branch_stages_write() {
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
            "INSERT INTO lix_directory_by_branch (\
             id, parent_id, name, lixcol_branch_id\
             ) VALUES ('01920000-0000-7000-8000-0000000000d3', NULL, 'docs', '01920000-0000-7000-8000-0000000000b1')",
            &[],
        )
        .await
        .expect("INSERT INTO lix_directory_by_branch should stage write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_directory_descriptor");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].entity_pk,
            "[\"01920000-0000-7000-8000-0000000000d3\"]"
        );
        assert_eq!(rows[0].branch_id, "01920000-0000-7000-8000-0000000000b1");
        assert!(!rows[0].global);
        assert!(!rows[0].untracked);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some(
                "{\"id\":\"01920000-0000-7000-8000-0000000000d3\",\"name\":\"docs\",\"parent_id\":null}"
            )
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
        assert_eq!(
            rows[0].entity_pk,
            "[\"01920000-0000-7000-8000-0000000000d3\"]"
        );
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
        assert_eq!(
            rows[0].entity_pk,
            "[\"01920000-0000-7000-8000-0000000000d3\"]"
        );
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
        assert_eq!(
            rows[0].entity_pk,
            "[\"01920000-0000-7000-8000-0000000000d3\"]"
        );
        assert_eq!(rows[0].branch_id, "01920000-0000-7000-8000-0000000000a1");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some(
                "{\"id\":\"01920000-0000-7000-8000-0000000000d3\",\"name\":\"renamed\",\"parent_id\":null}"
            )
        );
    }

    #[tokio::test]
    async fn execute_sql_delete_directory_by_branch_stages_tombstone() {
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
                    "01920000-0000-7000-8000-0000000000b1",
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
            "DELETE FROM lix_directory_by_branch \
             WHERE id = '01920000-0000-7000-8000-000000000313' AND lixcol_branch_id = '01920000-0000-7000-8000-0000000000b1'",
            &[],
        )
        .await
        .expect("DELETE lix_directory_by_branch should stage tombstone");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_all_semantic_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].entity_pk,
            "[\"01920000-0000-7000-8000-000000000313\"]"
        );
        assert_eq!(rows[0].branch_id, "01920000-0000-7000-8000-0000000000b1");
        assert!(rows[0].tombstone);
        assert_eq!(rows[0].snapshot_content, None);
    }

    #[tokio::test]
    async fn execute_sql_insert_into_file_by_branch_stages_descriptor_write() {
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
            "INSERT INTO lix_file_by_branch (\
             id, directory_id, name, lixcol_branch_id\
             ) VALUES ('01920000-0000-7000-8000-0000000000d2', '01920000-0000-7000-8000-0000000000d3', 'readme.md', '01920000-0000-7000-8000-0000000000b1')",
            &[],
        )
        .await
        .expect("INSERT INTO lix_file_by_branch should stage descriptor write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_file_descriptor");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].entity_pk,
            "[\"01920000-0000-7000-8000-0000000000d2\"]"
        );
        assert_eq!(rows[0].branch_id, "01920000-0000-7000-8000-0000000000b1");
        assert!(!rows[0].global);
        assert!(!rows[0].untracked);
        let snapshot: JsonValue =
            serde_json::from_str(rows[0].snapshot_content.as_deref().unwrap())
                .expect("descriptor snapshot JSON");
        assert_eq!(snapshot["id"], "01920000-0000-7000-8000-0000000000d2");
        assert_eq!(
            snapshot["directory_id"],
            "01920000-0000-7000-8000-0000000000d3"
        );
        assert_eq!(snapshot["name"], "readme.md");
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
        assert_eq!(
            rows[0].entity_pk,
            "[\"01920000-0000-7000-8000-0000000000d2\"]"
        );
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
                "01920000-0000-7000-8000-0000000000b1",
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
            "INSERT INTO lix_file_by_branch (\
             id, directory_id, name, content, lixcol_branch_id\
             ) VALUES ('01920000-0000-7000-8000-0000000000d2', '01920000-0000-7000-8000-0000000000d3', 'readme.md', CAST('AB' AS BYTEA), '01920000-0000-7000-8000-0000000000b1')",
            &[],
        )
        .await
        .expect("INSERT INTO lix_file_by_branch should stage descriptor and content writes");

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
            descriptor_rows[0].entity_pk,
            "[\"01920000-0000-7000-8000-0000000000d2\"]"
        );
        let blob_ref_rows = overlay.visible_semantic_rows(false, "lix_binary_blob_ref");
        assert_eq!(blob_ref_rows.len(), 1);
        assert_eq!(
            blob_ref_rows[0].entity_pk,
            "[\"01920000-0000-7000-8000-0000000000d2\"]"
        );
        assert_eq!(
            blob_ref_rows[0].file_id.as_deref(),
            Some("01920000-0000-7000-8000-0000000000d2")
        );
        assert_eq!(
            blob_ref_rows[0].branch_id,
            "01920000-0000-7000-8000-0000000000b1"
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
                Value::Json(json!({"source": "json-param"}).into()),
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
            Value::Json(json!({"source": "upload"}).into()),
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
                Value::Json(json!(["not", "an", "object"]).into()),
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
                .any(|row| row.entity_pk == "[\"01920000-0000-7000-8000-000000000322\"]")
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
            row.entity_pk == "[\"01920000-0000-7000-8000-000000000322\"]"
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
            .map(|row| row.entity_pk)
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
            descriptor_rows[0].entity_pk,
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
        assert_eq!(
            rows[0].entity_pk,
            "[\"01920000-0000-7000-8000-0000000000d2\"]"
        );
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
            Value::Json(serde_json::json!({"source": "git"}).into()),
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
            "UPDATE lix_file_by_branch SET content = CAST('A' AS BYTEA) WHERE id = '01920000-0000-7000-8000-0000000000d2' AND lixcol_branch_id = '01920000-0000-7000-8000-0000000000a1'",
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
            blob_ref_rows[0].entity_pk,
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
    async fn execute_sql_delete_file_by_branch_stages_descriptor_tombstone() {
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
                    "01920000-0000-7000-8000-0000000000d3",
                    "01920000-0000-7000-8000-0000000000b1",
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
                    "01920000-0000-7000-8000-0000000000b1",
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
            "DELETE FROM lix_file_by_branch \
             WHERE id = '01920000-0000-7000-8000-000000000332' AND lixcol_branch_id = '01920000-0000-7000-8000-0000000000b1'",
            &[],
        )
        .await
        .expect("DELETE lix_file_by_branch should stage descriptor tombstone");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_all_semantic_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].entity_pk,
            "[\"01920000-0000-7000-8000-000000000332\"]"
        );
        assert_eq!(rows[0].branch_id, "01920000-0000-7000-8000-0000000000b1");
        assert!(rows[0].tombstone);
        assert_eq!(rows[0].snapshot_content, None);
    }

    #[tokio::test]
    async fn execute_sql_update_entity_surface_stages_rewritten_snapshot() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(RowsHotStateReader {
            rows: vec![
                live_entity_row("entity-a", "01920000-0000-7000-8000-0000000000a1", "A"),
                live_entity_row("entity-b", "01920000-0000-7000-8000-0000000000a1", "B"),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        let result = execute_write_sql(
            &mut ctx,
            "UPDATE test_state_schema \
             SET value = 'updated', lixcol_metadata = '{\"source\":\"entity-update\"}' \
             WHERE value = 'A'",
            &[],
        )
        .await
        .expect("UPDATE entity surface should stage rewritten row");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "test_state_schema");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"entity-a\"]");
        assert_eq!(rows[0].branch_id, "01920000-0000-7000-8000-0000000000a1");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"updated\"}")
        );
        assert_eq!(
            rows[0].metadata.as_deref(),
            Some("{\"source\":\"entity-update\"}")
        );
    }

    #[tokio::test]
    async fn execute_sql_delete_entity_by_branch_stages_tombstone() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(RowsHotStateReader {
            rows: vec![
                live_entity_row("entity-a", "01920000-0000-7000-8000-0000000000a1", "A"),
                live_entity_row("entity-b", "01920000-0000-7000-8000-0000000000b1", "B"),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        let result = execute_write_sql(
            &mut ctx,
            "DELETE FROM test_state_schema_by_branch \
             WHERE lixcol_branch_id = $1",
            &[Value::Text(
                "01920000-0000-7000-8000-0000000000b1".to_string(),
            )],
        )
        .await
        .expect("parameterized DELETE entity by-branch surface should stage tombstone");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_all_semantic_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"entity-b\"]");
        assert_eq!(rows[0].branch_id, "01920000-0000-7000-8000-0000000000b1");
        assert!(rows[0].tombstone);
        assert_eq!(rows[0].snapshot_content, None);
    }

    #[tokio::test]
    async fn execute_sql_delete_entity_by_branch_like_uses_datafusion_and_stages_tombstone() {
        let (mut ctx, staged_writes, scans) = counting_write_context(vec![
            live_entity_row("entity-a", "01920000-0000-7000-8000-0000000000a1", "A"),
            live_entity_row("entity-b", "01920000-0000-7000-8000-0000000000b1", "Before"),
            live_entity_row("entity-c", "01920000-0000-7000-8000-0000000000b1", "After"),
        ]);
        ctx.schema_definitions = vec![json!({
            "x-lix-key": "test_state_schema",
            "type": "object",
            "properties": {
                "value": { "type": "string" }
            }
        })];

        let (result, path) = execute_write_sql_trace(
            &mut ctx,
            "DELETE FROM test_state_schema_by_branch \
             WHERE lixcol_branch_id = $1 AND value LIKE $2",
            &[
                Value::Text("01920000-0000-7000-8000-0000000000b1".to_string()),
                Value::Text("Before%".to_string()),
            ],
            WriteExecutorMode::Auto,
        )
        .await
        .expect("DELETE LIKE on an entity surface should stage a tombstone");

        assert_eq!(path, WriteExecutorPath::DataFusion);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);
        assert_eq!(scans.load(Ordering::SeqCst), 1);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_all_semantic_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"entity-b\"]");
        assert_eq!(rows[0].branch_id, "01920000-0000-7000-8000-0000000000b1");
        assert!(rows[0].tombstone);
    }

    #[tokio::test]
    async fn bound_public_write_supports_only_supported_entity_shapes() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let hot_state = Arc::new(RowsHotStateReader { rows: vec![] });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader,
            hot_state,
            staged_writes,
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        let supported_plan = create_write_logical_plan(
            &mut ctx,
            "UPDATE test_state_schema SET value = 'updated' WHERE value = 'A'",
        )
        .await
        .expect("supported entity update should plan");
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
                "x-lix-key": "test_state_schema",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        let plan = create_write_logical_plan(
            &mut ctx,
            "DELETE FROM test_state_schema WHERE value = 'A' AND value = 'B'",
        )
        .await
        .expect("registered entity write should bind before reference writer selection");
        let error = crate::sql2::execute_write_logical_plan_with_mode(
            &mut ctx,
            plan,
            &[],
            WriteExecutorMode::ForceDataFusion,
        )
        .await
        .expect_err("unsupported reference writer target should not become a fast no-op");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error.message.contains("does not support this entity write"));
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
                "x-lix-key": "test_state_schema",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        let plan = create_write_logical_plan(&mut ctx, "DELETE FROM test_state_schema WHERE false")
            .await
            .expect("registered entity write should bind before reference writer selection");
        let error = crate::sql2::execute_write_logical_plan_with_mode(
            &mut ctx,
            plan,
            &[],
            WriteExecutorMode::ForceDataFusion,
        )
        .await
        .expect_err("unsupported target with empty scope should not become a no-op");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error.message.contains("does not support this entity write"));
    }

    async fn setup_sql2_state_fixture() -> Result<DummySqlExecutionContext<'static>, LixError> {
        let schema_definition = json!({
            "x-lix-key": "test_state_schema",
            "type": "object",
            "properties": {
                "value": { "type": "string" }
            },
            "required": ["value"],
            "additionalProperties": false
        });
        Ok(DummySqlExecutionContext {
            active_branch_id: "01920000-0000-7000-8000-0000000000a1",
            blob_reader: Arc::new(StaticBlobReader {
                bytes: vec![0x41, 0x42],
            }),
            hot_state: Arc::new(RowsHotStateReader {
                rows: vec![
                    live_entity_row("entity-a", "01920000-0000-7000-8000-0000000000a1", "A"),
                    live_entity_row("entity-b", "01920000-0000-7000-8000-0000000000b1", "B"),
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
            entity_snapshot_reader: None,
            schema_definitions: vec![schema_definition],
        })
    }

    fn run_async_test_with_large_stack(
        test: impl FnOnce() -> futures_util::future::LocalBoxFuture<'static, ()> + Send + 'static,
    ) {
        std::thread::Builder::new()
            .name("sql2-execute-test".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime should build")
                    .block_on(test());
            })
            .expect("test thread should spawn")
            .join()
            .expect("test thread should join");
    }

    #[test]
    fn execute_sql_reads_entity_view_from_active_branch() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let ctx = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");

                let result = execute_sql(
                    &ctx,
                    "SELECT value, lixcol_entity_pk \
                     FROM test_state_schema",
                    &[],
                )
                .await
                .expect("sql2 execute should read entity view");

                assert_eq!(result.columns, vec!["value", "lixcol_entity_pk"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0][0], Value::Text("A".to_string()));
                assert_eq!(result.rows[0][1], Value::Json(json!(["entity-a"]).into()));
            })
        });
    }

    #[test]
    fn execute_sql_reads_entity_by_branch_view() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let ctx = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");

                let result = execute_sql(
                    &ctx,
                    "SELECT value, lixcol_branch_id \
                     FROM test_state_schema_by_branch \
                     WHERE lixcol_branch_id = '01920000-0000-7000-8000-0000000000b1'",
                    &[],
                )
                .await
                .expect("sql2 execute should read entity by-branch view");

                assert_eq!(result.columns, vec!["value", "lixcol_branch_id"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0][0], Value::Text("B".to_string()));
                assert_eq!(
                    result.rows[0][1],
                    Value::Text("01920000-0000-7000-8000-0000000000b1".to_string())
                );
            })
        });
    }

    #[test]
    fn execute_sql_reads_lix_directory_by_branch_view() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let ctx = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");

                let result = execute_sql(
                    &ctx,
                    "SELECT path, name, lixcol_branch_id \
                     FROM lix_directory_by_branch \
                     WHERE id = '01920000-0000-7000-8000-0000000000d3' AND lixcol_branch_id = '01920000-0000-7000-8000-0000000000a1'",
                    &[],
                )
                .await
                .expect("sql2 execute should read lix_directory_by_branch");

                assert_eq!(result.columns, vec!["path", "name", "lixcol_branch_id"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0][0], Value::Text("/docs".to_string()));
                assert_eq!(result.rows[0][1], Value::Text("docs".to_string()));
                assert_eq!(
                    result.rows[0][2],
                    Value::Text("01920000-0000-7000-8000-0000000000a1".to_string())
                );
            })
        });
    }

    #[test]
    fn execute_sql_reads_lix_directory_from_active_branch() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
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
            })
        });
    }

    #[test]
    fn execute_sql_reads_lix_file_by_branch_view() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let ctx = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");

                let result = execute_sql(
                    &ctx,
                    "SELECT path, name, content, lixcol_branch_id \
                     FROM lix_file_by_branch \
                     WHERE id = '01920000-0000-7000-8000-0000000000a2' AND lixcol_branch_id = '01920000-0000-7000-8000-0000000000a1'",
                    &[],
                )
                .await
                .expect("sql2 execute should read lix_file_by_branch");

                assert_eq!(
                    result.columns,
                    vec!["path", "name", "content", "lixcol_branch_id"]
                );
                assert_eq!(result.rows.len(), 1);
                assert_eq!(
                    result.rows[0][0],
                    Value::Text("/docs/readme.md".to_string())
                );
                assert_eq!(result.rows[0][1], Value::Text("readme.md".to_string()));
                assert_eq!(result.rows[0][2], Value::Blob(vec![0x41, 0x42].into()));
                assert_eq!(
                    result.rows[0][3],
                    Value::Text("01920000-0000-7000-8000-0000000000a1".to_string())
                );
            })
        });
    }

    #[test]
    fn execute_sql_reads_lix_file_from_active_branch() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
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
            })
        });
    }
}
