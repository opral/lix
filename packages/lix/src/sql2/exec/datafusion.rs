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
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
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
use crate::storage_adapter::StorageAdapterRead;

use super::{SqlDataFusionLogicalPlan, SqlLogicalPlan, SqlWriteResult};

pub(crate) const LIX_INSERT_COLUMN_OMITTED_METADATA_KEY: &str = "lix_insert_column_omitted";

pub(crate) struct DataFusionLogicalPlan {
    pub(super) session: SessionContext,
    pub(super) plan: LogicalPlan,
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
    session: SessionContext,
    planning_environment: Option<(
        std::sync::Arc<SqlPlanningCache<CatalogFingerprint>>,
        CatalogFingerprint,
    )>,
    _context: PhantomData<&'ctx ()>,
}

impl Drop for ReadSqlSession<'_> {
    fn drop(&mut self) {
        if let Some((cache, _)) = &self.planning_environment {
            cache.recycle_datafusion_read_session(self.session.clone());
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
        session: build_read_session(ctx, statements).await?,
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
        session: build_read_session_at_head(ctx, active_head, statements).await?,
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
        let plan = rebind_cached_read_plan(&session.session, cached.plan.clone()).await?;
        return Ok(SqlLogicalPlan::DataFusion(SqlDataFusionLogicalPlan {
            session: session.session.clone(),
            plan,
            notices: Vec::new(),
            json_predicate_params: cached.json_predicate_params.clone(),
            expected_parameter_count: cached.expected_parameter_count,
            physical_planning_cache: PhysicalReadPlanCacheKey::new(sql, params, catalog.clone())
                .map(|key| (std::sync::Arc::clone(cache), key)),
        }));
    }
    bind_table_function_parameters(&mut statement, params)?;
    let plan = create_logical_plan_from_statement(&session.session, statement).await?;
    validate_supported_logical_plan(&plan)?;
    validate_json_predicates_in_logical_plan(&plan)?;
    validate_history_anchor_predicates_in_logical_plan(&plan)?;
    let json_predicate_params = json_predicate_params_in_logical_plan(&plan);

    let physical_plan_cacheable = cacheable_statement && !logical_plan_has_scalar_function(&plan);
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
        session: session.session.clone(),
        plan,
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

pub(crate) async fn execute_transaction_read_statement_from_parsed<R>(
    read_ctx: &impl SqlExecutionContext,
    write_ctx: &mut dyn SqlWriteExecutionContext<ReadStore = R>,
    sql: &str,
    statement: DataFusionStatement,
    params: &[Value],
) -> Result<SqlQueryResult, LixError>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    // Same fence as session reads, with the transaction overlay available
    // during planning/execution but not returned to the caller.
    let planning_environment = read_ctx.sql_planning_environment().await?;
    let plan = create_transaction_read_logical_plan_from_parsed(
        read_ctx, write_ctx, sql, statement, params,
    )
    .await?;
    let session = match &plan {
        SqlLogicalPlan::DataFusion(plan) => plan.session.clone(),
        _ => unreachable!("transaction reads are planned by DataFusion"),
    };
    let result = execute_logical_plan(plan, params)
        .await
        .and_then(SessionReadResult::into_sql_query_result);
    if let Some((cache, _)) = planning_environment {
        cache.recycle_datafusion_read_session(session);
    }
    result
}

async fn create_transaction_read_logical_plan_from_parsed<R>(
    read_ctx: &impl SqlExecutionContext,
    write_ctx: &mut dyn SqlWriteExecutionContext<ReadStore = R>,
    sql: &str,
    mut statement: DataFusionStatement,
    params: &[Value],
) -> Result<SqlLogicalPlan, LixError>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    crate::sql2::bind_read_statement(sql, &statement)?;
    let parameter_names = statement_parameter_names(&statement)?;
    let expected_parameter_count = expected_positional_parameter_count(&parameter_names)?;
    validate_parameter_count_values(expected_parameter_count, &parameter_names, params.len())?;
    bind_table_function_parameters(&mut statement, params)?;
    let session = build_transaction_read_session(read_ctx, write_ctx, &statement).await?;
    let plan = create_logical_plan_from_statement(&session, statement).await?;
    validate_supported_logical_plan(&plan)?;
    validate_json_predicates_in_logical_plan(&plan)?;
    validate_history_anchor_predicates_in_logical_plan(&plan)?;
    let json_predicate_params = json_predicate_params_in_logical_plan(&plan);

    Ok(SqlLogicalPlan::DataFusion(SqlDataFusionLogicalPlan {
        session,
        plan,
        notices: Vec::new(),
        json_predicate_params,
        expected_parameter_count,
        physical_planning_cache: None,
    }))
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
        session,
        plan,
        notices,
        json_predicate_params,
        expected_parameter_count,
        physical_planning_cache,
    } = plan;
    debug_assert_eq!(expected_parameter_count, params.len());
    validate_json_predicate_params(&json_predicate_params, params)?;

    let mut dataframe = session
        .execute_logical_plan(plan)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    if !params.is_empty() {
        dataframe = dataframe
            .with_param_values(ParamValues::List(
                params.iter().map(scalar_value_from_lix_value).collect(),
            ))
            .map_err(datafusion_error_to_lix_error)?;
    }

    let result_fields = dataframe
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let batches = crate::sql2::runtime::collect_dataframe(dataframe, physical_planning_cache)
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
        session,
        plan,
        notices,
        json_predicate_params,
        expected_parameter_count,
        physical_planning_cache,
    } = plan;
    debug_assert_eq!(expected_parameter_count, params.len());
    validate_json_predicate_params(&json_predicate_params, params)?;

    let mut dataframe = session
        .execute_logical_plan(plan)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    if !params.is_empty() {
        dataframe = dataframe
            .with_param_values(ParamValues::List(
                params.iter().map(scalar_value_from_lix_value).collect(),
            ))
            .map_err(datafusion_error_to_lix_error)?;
    }
    let fields = dataframe
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let stream = crate::sql2::runtime::stream_dataframe(dataframe, physical_planning_cache)
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
        session,
        plan,
        notices,
        json_predicate_params,
        expected_parameter_count,
        physical_planning_cache,
    } = plan;
    debug_assert_eq!(expected_parameter_count, params.len());
    validate_json_predicate_params(&json_predicate_params, params)?;

    let mut dataframe = session
        .execute_logical_plan(plan)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    if !params.is_empty() {
        dataframe = dataframe
            .with_param_values(ParamValues::List(
                params.iter().map(scalar_value_from_lix_value).collect(),
            ))
            .map_err(datafusion_error_to_lix_error)?;
    }
    let fields = dataframe
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let batches = crate::sql2::runtime::collect_dataframe(dataframe, physical_planning_cache)
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
                DataType::Float32 => array
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Float32Array>()
                    .is_some_and(|values| {
                        (0..values.len())
                            .any(|index| values.is_valid(index) && !values.value(index).is_finite())
                    }),
                DataType::Float64 => array
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Float64Array>()
                    .is_some_and(|values| {
                        (0..values.len())
                            .any(|index| values.is_valid(index) && !values.value(index).is_finite())
                    }),
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

pub(crate) async fn execute_datafusion_write_logical_plan<R>(
    ctx: &mut dyn SqlWriteExecutionContext<ReadStore = R>,
    plan: &LogicalWritePlan,
    params: &[Value],
) -> Result<SqlWriteResult, LixError>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
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
    let props = session.state().execution_props().clone();
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
    let props = session.state().execution_props().clone();

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
                BoundCastType::Jsonb => DataType::Utf8,
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
            if matches!(column.name.as_str(), "row_pk" | "lixcol_row_pk")
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
        BoundWriteTarget::Row(crate::sql2::bind::write::RowWriteSurface::Base {
            schema_key,
        }) if bound_predicate_contains_like(&plan.bound.predicate)
            || bound_update_contains_binary(plan) =>
        {
            Ok(schema_key.clone())
        }
        BoundWriteTarget::Row(crate::sql2::bind::write::RowWriteSurface::ByBranch {
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
        BoundWriteTarget::Row(_) => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "sql2 DataFusion reference writer does not support this row write",
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
            .with_hint("Use placeholders like ?, ? or numbered placeholders like $1, $2, ...")
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
            .with_hint("Use placeholders like ?, ? or numbered placeholders like $1, $2, ...")
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
                "Use Lix row surfaces such as lix_registered_schema, lix_branch, lix_file, and lix_key_value instead of CREATE/DROP statements.",
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
            ScalarValue::Utf8(Some(value.as_str().to_owned())),
            Some(json_field_metadata()),
        ),
        Value::Timestamp(value) => ScalarValue::TimestampMicrosecond(
            Some(*value),
            Some("UTC".into()),
        )
        .into(),
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
    let mut rows = Vec::<Vec<Value>>::new();
    for batch in batches {
        for row_index in 0..batch.num_rows() {
            rows.push(row_values_from_batch(result_fields, batch, row_index)?);
        }
    }

    Ok(SqlQueryResult {
        rows,
        columns: result_columns.clone(),
        notices: Vec::new(),
    })
}

pub(crate) fn row_values_from_batch(
    result_fields: &[Field],
    batch: &RecordBatch,
    row_index: usize,
) -> Result<Vec<Value>, LixError> {
    let mut row = Vec::<Value>::with_capacity(batch.num_columns());
    for (column_index, array) in batch.columns().iter().enumerate() {
        let scalar = ScalarValue::try_from_array(array.as_ref(), row_index)
            .map_err(datafusion_error_to_lix_error)?;
        let field = result_fields.get(column_index);
        row.push(scalar_value_to_lix_value(scalar, field)?);
    }
    Ok(row)
}

fn scalar_value_to_lix_value(value: ScalarValue, field: Option<&Field>) -> Result<Value, LixError> {
    match value {
        ScalarValue::Null => Ok(Value::Null),
        ScalarValue::Boolean(Some(value)) => Ok(Value::Boolean(value)),
        ScalarValue::Boolean(None) => Ok(Value::Null),
        ScalarValue::Int8(Some(value)) => Ok(Value::Integer(i64::from(value))),
        ScalarValue::Int8(None) => Ok(Value::Null),
        ScalarValue::Int16(Some(value)) => Ok(Value::Integer(i64::from(value))),
        ScalarValue::Int16(None) => Ok(Value::Null),
        ScalarValue::Int32(Some(value)) => Ok(Value::Integer(i64::from(value))),
        ScalarValue::Int32(None) => Ok(Value::Null),
        ScalarValue::Int64(Some(value)) => Ok(Value::Integer(value)),
        ScalarValue::Int64(None) => Ok(Value::Null),
        ScalarValue::UInt8(Some(value)) => Ok(Value::Integer(i64::from(value))),
        ScalarValue::UInt8(None) => Ok(Value::Null),
        ScalarValue::UInt16(Some(value)) => Ok(Value::Integer(i64::from(value))),
        ScalarValue::UInt16(None) => Ok(Value::Null),
        ScalarValue::UInt32(Some(value)) => Ok(Value::Integer(i64::from(value))),
        ScalarValue::UInt32(None) => Ok(Value::Null),
        ScalarValue::UInt64(Some(value)) => match i64::try_from(value) {
            Ok(value) => Ok(Value::Integer(value)),
            Err(_) => Ok(Value::Text(value.to_string())),
        },
        ScalarValue::UInt64(None) => Ok(Value::Null),
        ScalarValue::Float32(Some(value)) => finite_query_float(f64::from(value)),
        ScalarValue::Float32(None) => Ok(Value::Null),
        ScalarValue::Float64(Some(value)) => finite_query_float(value),
        ScalarValue::Float64(None) => Ok(Value::Null),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => string_scalar_to_lix_value(value, field),
        ScalarValue::Utf8(None) | ScalarValue::Utf8View(None) | ScalarValue::LargeUtf8(None) => {
            Ok(Value::Null)
        }
        ScalarValue::Binary(Some(value)) | ScalarValue::LargeBinary(Some(value)) => {
            Ok(Value::Blob(value.into()))
        }
        ScalarValue::Binary(None) | ScalarValue::LargeBinary(None) => Ok(Value::Null),
        ScalarValue::TimestampMicrosecond(Some(value), _) => Ok(Value::Timestamp(value)),
        ScalarValue::TimestampMicrosecond(None, _) => Ok(Value::Null),
        other => Ok(Value::Text(other.to_string())),
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

fn string_scalar_to_lix_value(value: String, field: Option<&Field>) -> Result<Value, LixError> {
    if field.is_some_and(field_is_json) {
        return serde_json::from_str::<serde_json::Value>(&value)
            .map(|value| Value::Json(value.into()))
            .map_err(|error| {
                LixError::new(
                    "LIX_ERROR_INVALID_JSON",
                    format!(
                        "column '{}' is marked as JSON but contains invalid JSON: {error}",
                        field
                            .map(|field| field.name().as_str())
                            .unwrap_or("<unknown>")
                    ),
                )
            });
    }
    Ok(Value::Text(value))
}
