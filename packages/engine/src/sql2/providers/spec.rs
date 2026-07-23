//! Generic DataFusion plumbing shared by every lix virtual table.
//!
//! A table contributes a [`TableSpec`]: its schema, how to load rows, and how
//! to turn filter-matched rows into staged transaction writes. Everything
//! DataFusion requires beyond that — `TableProvider`, `ExecutionPlan`,
//! `InsertSink`, plan properties, the single-partition stream scaffolding,
//! and the COUNT result batch for DML — is implemented once here.
//!
//! Dispatch through the spec happens per statement (plan + one execute), never
//! per row, so the indirection has no effect on scan or write throughput.

use std::any::Any;
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BooleanArray, UInt64Array};
use datafusion::arrow::compute::{SortOptions, and, filter_record_batch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DFSchema, DataFusionError, Result, SchemaExt, not_impl_err};
use datafusion::datasource::TableType;
use datafusion::execution::TaskContext;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    EquivalenceProperties, PhysicalExpr, PhysicalSortExpr, create_physical_expr,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, PlanProperties};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
use futures_util::future::BoxFuture;
use futures_util::{TryStreamExt, stream};

use crate::LixError;
use crate::sql2::dml::{InsertExec, InsertSink};
use crate::sql2::{SqlWriteContext, WriteAccess};

use super::upsert;

/// Exec-time row loader. Captures whatever plan-time state the spec computed
/// (scan requests, readers, projections) and produces the source batch.
/// Re-invocable: DataFusion may execute a scan node more than once.
pub(super) type RowSource = Arc<dyn Fn() -> BoxFuture<'static, Result<RecordBatch>> + Send + Sync>;

/// Build a [`RowSource`] from owned plan-time state and an async body taking
/// that state by value. Owns the once-per-invocation clone that
/// re-invocability requires, so specs write the load body with no capture
/// ceremony. The clone is cheap (`Arc`s and small values) and happens once
/// per statement execution, never per row.
pub(super) fn row_source<S, Fut>(
    state: S,
    f: impl Fn(S) -> Fut + Send + Sync + 'static,
) -> RowSource
where
    S: Clone + Send + Sync + 'static,
    Fut: Future<Output = Result<RecordBatch>> + Send + 'static,
{
    Arc::new(move || Box::pin(f(state.clone())))
}

/// Exec-time DML handler: receives the filter-matched batch, stages the
/// resulting transaction writes, and returns the affected-row count.
pub(super) type DmlApply =
    Arc<dyn Fn(RecordBatch) -> BoxFuture<'static, Result<u64>> + Send + Sync>;

/// Optional pre-delete projection captured from the exact batch that will be
/// handed to a DML apply handler.  The capture is deliberately separate from
/// the physical DML count output: callers continue to receive an accurate
/// affected-row count even when a delete stages additional cascade rows.
#[derive(Clone)]
pub(crate) struct DmlReturning {
    schema: SchemaRef,
    expressions: Vec<Arc<dyn PhysicalExpr>>,
    required_columns: BTreeSet<String>,
    captured: Arc<Mutex<Option<RecordBatch>>>,
}

impl DmlReturning {
    pub(crate) fn new(
        schema: SchemaRef,
        expressions: Vec<Arc<dyn PhysicalExpr>>,
        required_columns: BTreeSet<String>,
    ) -> Self {
        Self {
            schema,
            expressions,
            required_columns,
            captured: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    pub(crate) fn required_columns(&self) -> &BTreeSet<String> {
        &self.required_columns
    }

    fn project(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let columns = self
            .expressions
            .iter()
            .map(|expression| {
                expression
                    .evaluate(batch)
                    .and_then(|value| value.into_array(batch.num_rows()))
            })
            .collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(DataFusionError::from)
    }

    fn capture(&self, batch: RecordBatch) {
        *self
            .captured
            .lock()
            .expect("DELETE RETURNING capture mutex poisoned") = Some(batch);
    }

    pub(crate) fn take_captured(&self) -> Result<RecordBatch> {
        self.captured
            .lock()
            .expect("DELETE RETURNING capture mutex poisoned")
            .take()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "DELETE RETURNING execution completed without a captured result".to_string(),
                )
            })
    }
}

/// Extra planning inputs needed by a DML spec without making `RETURNING`
/// behavior part of every table implementation.  Most specs ignore it; the
/// file surface uses it to avoid loading binary blobs unless a return
/// expression actually references `data`.
#[derive(Clone, Debug, Default)]
pub(super) struct DmlPlanOptions {
    pub(super) returning_columns: BTreeSet<String>,
}

impl DmlPlanOptions {
    fn from_returning(returning: Option<&DmlReturning>) -> Self {
        Self {
            returning_columns: returning
                .map(|returning| returning.required_columns().clone())
                .unwrap_or_default(),
        }
    }
}

/// Exec-time INSERT handler: receives the collected input batches, stages
/// the resulting transaction writes, and returns the inserted-row count.
pub(super) type InsertApply =
    Arc<dyn Fn(Vec<RecordBatch>) -> BoxFuture<'static, Result<u64>> + Send + Sync>;

/// A planned read: the (projected) output schema plus the loader that
/// materializes it during execution.
pub(super) struct PlannedScan {
    pub(super) schema: SchemaRef,
    pub(super) load: RowSource,
    pub(super) ordering: Option<String>,
}

/// A planned UPDATE/DELETE: the candidate-row source the filters run against,
/// and the handler that stages writes for the rows that matched.
///
/// Contract: per execution, `SpecDmlExec` invokes `source` exactly once and
/// then `apply` exactly once with the filter-matched batch. Specs may pass
/// state computed during `source` to `apply` out of band (lix_file stashes
/// blob-ref keys and its plugin render context this way), so a plan must not
/// be executed concurrently — the engine executes each DML root once.
pub(super) struct PlannedDml {
    pub(super) source: RowSource,
    pub(super) apply: DmlApply,
}

/// Everything that makes one lix virtual table different from the others.
///
/// Read-only tables implement `table_name`/`schema`/`plan_scan` (plus
/// `table_type`/`filter_pushdown` where they deviate) and inherit the
/// rejecting defaults for the write hooks; the provider additionally gates
/// writes on [`WriteAccess`], so the defaults are only a backstop.
///
/// Writable tables additionally implement `stage_insert`, `plan_delete`, and
/// `plan_update`, with `validate_update_assignments`/`prepare_write_filters`
/// for plan-time validation. Implement `plan_insert` instead of
/// `stage_insert` only when the spec must inspect or reject the physical
/// INSERT input plan before execution (lix_file, entity).
#[async_trait]
pub(super) trait TableSpec: Send + Sync + 'static {
    /// Name used in error messages and plan display. The builtin tables
    /// deliberately return their base name (e.g. "lix_state") for both the
    /// active and `_by_branch` surfaces — that is what the pre-framework
    /// providers hardcoded into their messages — while entity surfaces
    /// return the full catalog surface name.
    fn table_name(&self) -> &str;

    fn schema(&self) -> SchemaRef;

    /// Public column that routes a history scan to an explicit commit.
    ///
    /// This is provider identity, not a name heuristic: ordinary entity
    /// schemas may legitimately expose a property with the same name.
    fn history_anchor_column(&self) -> Option<&'static str> {
        None
    }

    /// How the surface introspects in `information_schema.tables`.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn filter_pushdown(&self, _filter: &Expr) -> TableProviderFilterPushDown {
        TableProviderFilterPushDown::Unsupported
    }

    /// Rejects filters that would be unsafe to leave as residual expressions.
    ///
    /// Most providers accept every well-typed filter and keep the default.
    /// History providers use this hook to prevent an unrouteable time-travel
    /// anchor from being mistaken for an anchor-free active-head query.
    fn validate_filter_pushdown(&self, _filter: &Expr) -> Result<()> {
        Ok(())
    }

    /// `props` are the session's execution properties, for specs that compile
    /// pushed-down filters to physical expressions at plan time.
    async fn plan_scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        props: &ExecutionProps,
    ) -> Result<PlannedScan>;

    /// Convert INSERT input batches into staged writes; returns the row count.
    async fn stage_insert(
        &self,
        _write_ctx: &SqlWriteContext,
        _batches: Vec<RecordBatch>,
    ) -> Result<u64> {
        Err(DataFusionError::Execution(format!(
            "INSERT into {} is not supported",
            self.table_name()
        )))
    }

    /// Plan-time INSERT hook for specs that must inspect or validate the
    /// physical input plan (e.g. lix_file's insert-column intent detection
    /// and binary-cast rejection). Returning `Some` bypasses `stage_insert`
    /// and routes the collected input batches to the returned handler.
    async fn plan_insert(
        &self,
        _write_ctx: SqlWriteContext,
        _input: &Arc<dyn ExecutionPlan>,
    ) -> Result<Option<InsertApply>> {
        Ok(None)
    }

    /// Plan-time validation of UPDATE assignment targets.
    fn validate_update_assignments(&self, _assignments: &[(String, Expr)]) -> Result<()> {
        Ok(())
    }

    /// Rewrite/validate UPDATE/DELETE filters before physical conversion.
    fn prepare_write_filters(&self, filters: Vec<Expr>) -> Result<Vec<Expr>> {
        Ok(filters)
    }

    async fn plan_delete(
        &self,
        _write_ctx: SqlWriteContext,
        _filters: &[Expr],
    ) -> Result<PlannedDml> {
        Err(DataFusionError::Execution(format!(
            "DELETE FROM {} is not supported",
            self.table_name()
        )))
    }

    /// Variant of [`TableSpec::plan_delete`] that exposes only the pieces of
    /// a `RETURNING` projection a source loader may need.  Specs that do not
    /// have lazily loaded columns retain their existing plan unchanged.
    async fn plan_delete_with_options(
        &self,
        write_ctx: SqlWriteContext,
        filters: &[Expr],
        _options: DmlPlanOptions,
    ) -> Result<PlannedDml> {
        self.plan_delete(write_ctx, filters).await
    }

    async fn plan_update(
        &self,
        _write_ctx: SqlWriteContext,
        _assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
        _filters: &[Expr],
    ) -> Result<PlannedDml> {
        Err(DataFusionError::Execution(format!(
            "UPDATE {} is not supported",
            self.table_name()
        )))
    }

    /// The spec's `INSERT ... ON CONFLICT` capability, if it supports upsert.
    fn upsert_support(&self) -> Option<&dyn upsert::UpsertSupport> {
        None
    }
}

/// Register `spec` as a DataFusion table under its surface name.
pub(super) fn register_spec_table(
    session: &datafusion::prelude::SessionContext,
    surface_name: &str,
    spec: Arc<dyn TableSpec>,
    write_access: WriteAccess,
) -> Result<(), LixError> {
    session
        .register_table(
            surface_name,
            Arc::new(SpecTableProvider::new(spec, write_access)),
        )
        .map_err(crate::sql2::error::datafusion_error_to_lix_error)?;
    Ok(())
}

pub(super) struct SpecTableProvider {
    spec: Arc<dyn TableSpec>,
    schema: SchemaRef,
    write_access: WriteAccess,
}

impl SpecTableProvider {
    pub(super) fn new(spec: Arc<dyn TableSpec>, write_access: WriteAccess) -> Self {
        Self {
            schema: spec.schema(),
            spec,
            write_access,
        }
    }

    pub(super) fn history_anchor_column(&self) -> Option<&'static str> {
        self.spec.history_anchor_column()
    }

    #[cfg(test)]
    pub(super) fn is_write(&self) -> bool {
        self.write_access.is_write()
    }

    /// Execute an `INSERT ... ON CONFLICT` against this table. The conflict
    /// target columns are resolved by the spec, then the generic upsert driver
    /// composes the spec's insert/scan/update builders.
    pub(crate) async fn execute_upsert(
        &self,
        input: &Arc<dyn ExecutionPlan>,
        proposed_batches: Vec<RecordBatch>,
        target_columns: &[String],
        action: &upsert::UpsertAction,
    ) -> Result<u64> {
        let (write_ctx, support, target) = self.validate_upsert(input, target_columns).await?;
        upsert::execute_upsert(support, &write_ctx, proposed_batches, &target, action).await
    }

    pub(crate) async fn validate_upsert(
        &self,
        input: &Arc<dyn ExecutionPlan>,
        target_columns: &[String],
    ) -> Result<(
        SqlWriteContext,
        &dyn upsert::UpsertSupport,
        upsert::UpsertConflictTarget,
    )> {
        let table = self.spec.table_name();
        let write_ctx = self
            .write_access
            .require_write(&format!("INSERT into {table}"))?;
        self.schema
            .logically_equivalent_names_and_types(&input.schema())?;
        let support = self.spec.upsert_support().ok_or_else(|| {
            DataFusionError::Execution(format!("INSERT ON CONFLICT is not supported on {table}"))
        })?;
        let target = support.resolve_conflict_target(table, target_columns)?;
        self.spec.plan_insert(write_ctx.clone(), input).await?;
        Ok((write_ctx, support, target))
    }

    pub(crate) async fn delete_with_returning(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
        returning: DmlReturning,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.delete_impl(state, filters, Some(returning)).await
    }

    async fn delete_impl(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
        returning: Option<DmlReturning>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let table = self.spec.table_name();
        let write_ctx = self
            .write_access
            .require_write(&format!("DELETE FROM {table}"))?;
        let filters = self.spec.prepare_write_filters(filters)?;
        let physical_filters = physical_filters(&self.schema, &filters, state)?;
        let planned = self
            .spec
            .plan_delete_with_options(
                write_ctx,
                &filters,
                DmlPlanOptions::from_returning(returning.as_ref()),
            )
            .await?;
        Ok(Arc::new(SpecDmlExec::new(
            table.into(),
            "DELETE",
            planned,
            physical_filters,
            returning,
        )))
    }
}

impl std::fmt::Debug for SpecTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpecTableProvider")
            .field("table", &self.spec.table_name())
            .field("write", &self.write_access.is_write())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for SpecTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        self.spec.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        filters
            .iter()
            .map(|filter| {
                self.spec.validate_filter_pushdown(filter)?;
                Ok(self.spec.filter_pushdown(filter))
            })
            .collect()
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planned = self
            .spec
            .plan_scan(projection, filters, limit, _state.execution_props())
            .await?;
        Ok(Arc::new(SpecScanExec::new(
            self.spec.table_name().into(),
            planned,
        )))
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let table = self.spec.table_name();
        if insert_op != InsertOp::Append {
            return not_impl_err!("{insert_op} not implemented for {table} yet");
        }
        let write_ctx = self
            .write_access
            .require_write(&format!("INSERT into {table}"))?;
        self.schema
            .logically_equivalent_names_and_types(&input.schema())?;
        let sink: Arc<dyn InsertSink> =
            match self.spec.plan_insert(write_ctx.clone(), &input).await? {
                Some(apply) => Arc::new(PlannedInsertSink {
                    table: table.into(),
                    apply,
                }),
                None => Arc::new(SpecInsertSink {
                    spec: Arc::clone(&self.spec),
                    write_ctx,
                }),
            };
        Ok(Arc::new(InsertExec::new(input, sink)))
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.delete_impl(state, filters, None).await
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let table = self.spec.table_name();
        let write_ctx = self
            .write_access
            .require_write(&format!("UPDATE {table}"))?;
        self.spec.validate_update_assignments(&assignments)?;
        let filters = self.spec.prepare_write_filters(filters)?;
        let df_schema = DFSchema::try_from(Arc::clone(&self.schema))?;
        let physical_assignments = assignments
            .iter()
            .map(|(column_name, expr)| {
                Ok((
                    column_name.clone(),
                    create_physical_expr(expr, &df_schema, state.execution_props())?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let physical_filters = filters
            .iter()
            .map(|expr| create_physical_expr(expr, &df_schema, state.execution_props()))
            .collect::<Result<Vec<_>>>()?;
        let planned = self
            .spec
            .plan_update(write_ctx, physical_assignments, &filters)
            .await?;
        Ok(Arc::new(SpecDmlExec::new(
            table.into(),
            "UPDATE",
            planned,
            physical_filters,
            None,
        )))
    }
}

fn physical_filters(
    schema: &SchemaRef,
    filters: &[Expr],
    state: &dyn Session,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    let df_schema = DFSchema::try_from(Arc::clone(schema))?;
    filters
        .iter()
        .map(|expr| create_physical_expr(expr, &df_schema, state.execution_props()))
        .collect()
}

struct SpecInsertSink {
    spec: Arc<dyn TableSpec>,
    write_ctx: SqlWriteContext,
}

impl std::fmt::Debug for SpecInsertSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpecInsertSink")
            .field("table", &self.spec.table_name())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for SpecInsertSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SpecInsertSink({})", self.spec.table_name())
    }
}

#[async_trait]
impl InsertSink for SpecInsertSink {
    async fn write_batches(
        &self,
        batches: Vec<RecordBatch>,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        self.spec.stage_insert(&self.write_ctx, batches).await
    }
}

/// Insert sink for specs that planned their own handler via `plan_insert`.
struct PlannedInsertSink {
    table: Arc<str>,
    apply: InsertApply,
}

impl std::fmt::Debug for PlannedInsertSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlannedInsertSink")
            .field("table", &self.table)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for PlannedInsertSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PlannedInsertSink({})", self.table)
    }
}

#[async_trait]
impl InsertSink for PlannedInsertSink {
    async fn write_batches(
        &self,
        batches: Vec<RecordBatch>,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        (self.apply)(batches).await
    }
}

struct SpecScanExec {
    table: Arc<str>,
    schema: SchemaRef,
    load: RowSource,
    properties: Arc<PlanProperties>,
}

impl SpecScanExec {
    fn new(table: Arc<str>, planned: PlannedScan) -> Self {
        let equivalence_properties = planned
            .ordering
            .as_deref()
            .and_then(|column_name| {
                planned
                    .schema
                    .index_of(column_name)
                    .ok()
                    .map(|column_index| {
                        EquivalenceProperties::new_with_orderings(
                            Arc::clone(&planned.schema),
                            [vec![PhysicalSortExpr {
                                expr: Arc::new(Column::new(column_name, column_index)),
                                options: SortOptions {
                                    descending: false,
                                    nulls_first: false,
                                },
                            }]],
                        )
                    })
            })
            .unwrap_or_else(|| EquivalenceProperties::new(Arc::clone(&planned.schema)));
        let properties = PlanProperties::new(
            equivalence_properties,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            table,
            schema: planned.schema,
            load: planned.load,
            properties: Arc::new(properties),
        }
    }
}

impl std::fmt::Debug for SpecScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpecScanExec")
            .field("table", &self.table)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for SpecScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SpecScanExec({})", self.table)
    }
}

impl ExecutionPlan for SpecScanExec {
    fn name(&self) -> &'static str {
        "SpecScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "SpecScanExec({}) does not accept children",
                self.table
            )));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "SpecScanExec({}) only exposes one partition, got {partition}",
                self.table
            )));
        }
        let load = Arc::clone(&self.load);
        let schema = Arc::clone(&self.schema);
        let stream = stream::once(async move { load().await });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

pub(super) struct SpecDmlExec {
    table: Arc<str>,
    operation: &'static str,
    source: RowSource,
    apply: DmlApply,
    filters: Vec<Arc<dyn PhysicalExpr>>,
    returning: Option<DmlReturning>,
    result_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl SpecDmlExec {
    fn new(
        table: Arc<str>,
        operation: &'static str,
        planned: PlannedDml,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        returning: Option<DmlReturning>,
    ) -> Self {
        let result_schema = dml_count_schema();
        let properties = dml_plan_properties(Arc::clone(&result_schema));
        Self {
            table,
            operation,
            source: planned.source,
            apply: planned.apply,
            filters,
            returning,
            result_schema,
            properties: Arc::new(properties),
        }
    }
}

impl std::fmt::Debug for SpecDmlExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpecDmlExec")
            .field("table", &self.table)
            .field("operation", &self.operation)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for SpecDmlExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SpecDmlExec({} {}, filters={})",
            self.operation,
            self.table,
            self.filters.len()
        )
    }
}

impl ExecutionPlan for SpecDmlExec {
    fn name(&self) -> &'static str {
        "SpecDmlExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "SpecDmlExec({}) does not accept children",
                self.table
            )));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "SpecDmlExec({}) only exposes one partition, got {partition}",
                self.table
            )));
        }
        let source = Arc::clone(&self.source);
        let apply = Arc::clone(&self.apply);
        let filters = self.filters.clone();
        let returning = self.returning.clone();
        let table = Arc::clone(&self.table);
        let result_schema = Arc::clone(&self.result_schema);
        let stream_schema = Arc::clone(&result_schema);

        let stream = stream::once(async move {
            let source_batch = source().await?;
            let matched_batch = filter_batch(source_batch, &filters, &table)?;
            let returned_batch = returning
                .as_ref()
                .map(|returning| returning.project(&matched_batch))
                .transpose()?;
            let count = apply(matched_batch).await?;
            if let (Some(returning), Some(returned_batch)) = (returning, returned_batch) {
                returning.capture(returned_batch);
            }
            Ok::<_, DataFusionError>(stream::iter(vec![Ok::<RecordBatch, DataFusionError>(
                dml_count_batch(stream_schema, count)?,
            )]))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            result_schema,
            stream,
        )))
    }
}

/// Shared scan tail for specs that build their source batch against the full
/// table schema: apply the pushed-down filters, project to the scan's output
/// columns, and slice to the limit.
pub(super) fn finish_scan_batch(
    batch: RecordBatch,
    filters: &[Arc<dyn PhysicalExpr>],
    projection: Option<&[usize]>,
    limit: Option<usize>,
    table_name: &str,
) -> Result<RecordBatch> {
    let filtered = filter_batch(batch, filters, table_name)?;
    let projected = match projection {
        Some(indices) => filtered.project(indices)?,
        None => filtered,
    };
    Ok(match limit {
        Some(limit) => projected.slice(0, limit.min(projected.num_rows())),
        None => projected,
    })
}

/// Apply conjunctive physical filters to a batch, keeping rows where every
/// filter evaluates to true (nulls count as false).
pub(super) fn filter_batch(
    batch: RecordBatch,
    filters: &[Arc<dyn PhysicalExpr>],
    table_name: &str,
) -> Result<RecordBatch> {
    let Some(mask) = evaluate_filters(&batch, filters, table_name)? else {
        return Ok(batch);
    };
    Ok(filter_record_batch(&batch, &mask)?)
}

fn evaluate_filters(
    batch: &RecordBatch,
    filters: &[Arc<dyn PhysicalExpr>],
    table_name: &str,
) -> Result<Option<BooleanArray>> {
    if filters.is_empty() {
        return Ok(None);
    }

    let mut combined_mask: Option<BooleanArray> = None;
    for filter in filters {
        let result = filter.evaluate(batch)?;
        let array = result.into_array(batch.num_rows())?;
        let bool_array = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(format!("{table_name} filter was not boolean"))
            })?;
        let normalized = bool_array
            .iter()
            .map(|value| Some(value == Some(true)))
            .collect::<BooleanArray>();
        combined_mask = Some(match combined_mask {
            Some(existing) => and(&existing, &normalized)?,
            None => normalized,
        });
    }
    Ok(combined_mask)
}

pub(super) fn dml_count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

fn dml_plan_properties(schema: SchemaRef) -> PlanProperties {
    PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Final,
        Boundedness::Bounded,
    )
}

#[expect(trivial_casts)]
fn dml_count_batch(schema: SchemaRef, count: u64) -> Result<RecordBatch> {
    RecordBatch::try_new(
        schema,
        vec![Arc::new(UInt64Array::from(vec![count])) as ArrayRef],
    )
    .map_err(DataFusionError::from)
}

/// Project `schema` by the optional column-index projection.
pub(super) fn projected_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> SchemaRef {
    projection.map_or_else(
        || Arc::clone(schema),
        |projection| Arc::new(schema.project(projection).expect("projection is valid")),
    )
}
