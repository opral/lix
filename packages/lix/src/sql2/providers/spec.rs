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
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BooleanArray, UInt32Array, UInt64Array};
use datafusion::arrow::compute::{SortOptions, and, filter_record_batch, take};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DFSchema, DataFusionError, Result, SchemaExt};
use datafusion::datasource::TableType;
use datafusion::execution::TaskContext;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    AcrossPartitions, ConstExpr, EquivalenceProperties, PhysicalExpr, PhysicalSortExpr,
    create_physical_expr,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, PlanProperties};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    Statistics,
};
#[cfg(feature = "storage-benches")]
use futures_util::Stream;
use futures_util::future::BoxFuture;
use futures_util::{TryStreamExt, stream};

use crate::LixError;
use crate::sql2::dml::{InsertExec, InsertSink};
use crate::sql2::write_normalization::{InsertColumnIntents, mark_omitted_insert_columns};
use crate::sql2::{SqlWriteContext, WriteAccess};

use super::upsert;

/// Exec-time row loader. Captures whatever plan-time state the spec computed
/// (scan requests, readers, projections) and produces the source batch.
/// Re-invocable: DataFusion may execute a scan node more than once.
pub(super) type RowSource = Arc<dyn Fn() -> BoxFuture<'static, Result<RecordBatch>> + Send + Sync>;

/// Re-invocable factory for scans that can produce Arrow batches incrementally.
///
/// Unlike [`RowSource`], this preserves storage page and row-group boundaries
/// all the way into DataFusion. The factory itself is synchronous because scan
/// setup belongs in the returned stream; reads and decoding remain async and
/// backpressured by the consumer.
pub(super) type BatchStreamSource =
    Arc<dyn Fn(usize, Arc<TaskContext>) -> Result<SendableRecordBatchStream> + Send + Sync>;

#[derive(Clone)]
pub(super) struct ScanSource {
    partition_count: usize,
    statistics: Arc<Vec<Statistics>>,
    source_statistics: Option<Statistics>,
    open: BatchStreamSource,
}

impl ScanSource {
    fn new(
        schema: &SchemaRef,
        statistics: Vec<Statistics>,
        source_statistics: Option<Statistics>,
        open: impl Fn(usize, Arc<TaskContext>) -> Result<SendableRecordBatchStream>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        let partition_count = statistics.len();
        assert!(partition_count > 0, "scan source must expose a partition");
        assert!(statistics.iter().all(|statistics| {
            statistics.column_statistics.is_empty()
                || statistics.column_statistics.len() == schema.fields().len()
        }));
        assert!(source_statistics.as_ref().is_none_or(|statistics| {
            statistics.column_statistics.is_empty()
                || statistics.column_statistics.len() == schema.fields().len()
        }));
        Self {
            partition_count,
            statistics: Arc::new(statistics),
            source_statistics,
            open: Arc::new(open),
        }
    }

    fn open(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.partition_count {
            return Err(DataFusionError::Execution(format!(
                "scan source exposes {} partitions, got {partition}",
                self.partition_count
            )));
        }
        (self.open)(partition, context)
    }
}

#[cfg(test)]
impl ScanSource {
    pub(super) async fn load_single_batch(&self) -> Result<RecordBatch> {
        let batches = self
            .open(0, Arc::new(TaskContext::default()))?
            .try_collect::<Vec<_>>()
            .await?;
        let [batch] = batches.as_slice() else {
            return Err(DataFusionError::Execution(format!(
                "test expected one scan batch, got {}",
                batches.len()
            )));
        };
        Ok(batch.clone())
    }
}

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

/// Adapt an existing materializing loader into a planned scan source.
pub(super) fn scan_row_source<S, Fut>(
    schema: SchemaRef,
    state: S,
    f: impl Fn(S) -> Fut + Send + Sync + 'static,
) -> ScanSource
where
    S: Clone + Send + Sync + 'static,
    Fut: Future<Output = Result<RecordBatch>> + Send + 'static,
{
    let load = row_source(state, f);
    batch_stream_source(Arc::clone(&schema), 1, move |_partition, _context| {
        let load = Arc::clone(&load);
        let stream = stream::once(async move { load().await });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            stream,
        )))
    })
}

/// Build a storage-originating streaming scan source.
pub(super) fn batch_stream_source(
    schema: SchemaRef,
    partition_count: usize,
    factory: impl Fn(usize, Arc<TaskContext>) -> Result<SendableRecordBatchStream>
    + Send
    + Sync
    + 'static,
) -> ScanSource {
    let statistics = (0..partition_count)
        .map(|_| Statistics::new_unknown(schema.as_ref()))
        .collect();
    batch_stream_source_with_statistics(schema, statistics, factory)
}

/// Build a streaming scan whose immutable source can expose exact per-partition
/// row and column statistics to DataFusion's generic physical optimizers.
pub(super) fn batch_stream_source_with_statistics(
    schema: SchemaRef,
    statistics: Vec<Statistics>,
    factory: impl Fn(usize, Arc<TaskContext>) -> Result<SendableRecordBatchStream>
    + Send
    + Sync
    + 'static,
) -> ScanSource {
    batch_stream_source_with_statistics_and_source(schema, statistics, None, factory)
}

/// Build a streaming scan with both per-partition statistics and an optional
/// independently proven whole-source summary.
///
/// A source-wide summary is useful when overlays make the exact distribution
/// across physical partitions unknown even though collection-level metadata
/// still proves the statistics for their logical union. It never replaces a
/// request for one specific partition.
pub(super) fn batch_stream_source_with_statistics_and_source(
    schema: SchemaRef,
    statistics: Vec<Statistics>,
    source_statistics: Option<Statistics>,
    factory: impl Fn(usize, Arc<TaskContext>) -> Result<SendableRecordBatchStream>
    + Send
    + Sync
    + 'static,
) -> ScanSource {
    ScanSource::new(&schema, statistics, source_statistics, factory)
}

/// Exec-time DML handler: receives the filter-matched batch, stages the
/// resulting transaction writes, and returns the affected-row count.
pub(super) type DmlApply =
    Arc<dyn Fn(RecordBatch) -> BoxFuture<'static, Result<u64>> + Send + Sync>;

/// Optional DML projection captured by a write handler. DELETE captures its
/// pre-image in [`SpecDmlExec`], while INSERT and UPDATE providers capture
/// their post-image only after the staged write has succeeded. The capture is
/// deliberately separate from the physical DML count output: callers still
/// receive an accurate affected-row count even when a write stages auxiliary
/// rows (for example, filesystem descriptors or cascades).
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

    pub(super) fn project(&self, batch: &RecordBatch) -> Result<RecordBatch> {
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

    pub(super) fn capture(&self, batch: RecordBatch) {
        *self
            .captured
            .lock()
            .expect("DML RETURNING capture mutex poisoned") = Some(batch);
    }

    pub(crate) fn take_captured(&self) -> Result<RecordBatch> {
        self.captured
            .lock()
            .expect("DML RETURNING capture mutex poisoned")
            .take()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "DML RETURNING execution completed without a captured result".to_string(),
                )
            })
    }
}

/// Extra planning inputs needed by a DML spec without making `RETURNING`
/// behavior part of every table implementation.  Most specs ignore it; the
/// file surface uses it to avoid loading binary blobs unless a return
/// expression actually references `content`.
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
    pub(super) source: ScanSource,
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
    /// Name used in error messages and plan display.
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
    ///
    /// Within one statement, repeated scans of the same provider instance and
    /// projection with no pushed filters or limit must expose the same bounded
    /// source. The runtime may execute that source once and replay its batches.
    /// Different table-function arguments must use distinct provider instances.
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

    /// Plan an INSERT that must produce the exact inserted post-image.  This
    /// intentionally has no default fallback to `stage_insert`: a provider
    /// that does not explicitly construct and capture its post-image must not
    /// turn `INSERT ... RETURNING` into a count-only mutation.
    async fn plan_insert_with_returning(
        &self,
        _write_ctx: SqlWriteContext,
        _input: &Arc<dyn ExecutionPlan>,
        _returning: DmlReturning,
    ) -> Result<InsertApply> {
        Err(DataFusionError::Execution(format!(
            "INSERT RETURNING is not supported on {}",
            self.table_name()
        )))
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

    /// Plan an UPDATE that must produce the exact updated post-image.  Like
    /// [`TableSpec::plan_insert_with_returning`], this deliberately rejects by
    /// default so a newly writable provider cannot silently report only the
    /// affected count for `UPDATE ... RETURNING`.
    async fn plan_update_with_returning(
        &self,
        _write_ctx: SqlWriteContext,
        _assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
        _filters: &[Expr],
        _returning: DmlReturning,
    ) -> Result<PlannedDml> {
        Err(DataFusionError::Execution(format!(
            "UPDATE RETURNING is not supported on {}",
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
    if let Some(write_ctx) = write_access.into_write_context() {
        write_ctx.write_targets()?.register(
            surface_name,
            Arc::new(SpecWriteTarget::new(
                Arc::clone(&spec),
                write_ctx.into_physical_target(),
            )),
        )?;
    }
    let provider = Arc::new(SpecTableProvider::new(spec));
    if let Some(anchor_column) = provider.history_anchor_column() {
        return super::history_table_function::register_history_table_function(
            session,
            surface_name,
            provider,
            anchor_column,
        );
    }
    session
        .register_table(surface_name, provider)
        .map_err(crate::sql2::error::datafusion_error_to_lix_error)?;
    Ok(())
}

pub(super) struct SpecTableProvider {
    provider_id: u64,
    spec: Arc<dyn TableSpec>,
    schema: SchemaRef,
}

impl SpecTableProvider {
    pub(super) fn new(spec: Arc<dyn TableSpec>) -> Self {
        static NEXT_PROVIDER_ID: AtomicU64 = AtomicU64::new(0);
        Self {
            provider_id: NEXT_PROVIDER_ID.fetch_add(1, AtomicOrdering::Relaxed),
            schema: spec.schema(),
            spec,
        }
    }

    pub(super) fn history_anchor_column(&self) -> Option<&'static str> {
        self.spec.history_anchor_column()
    }
}

/// Transaction-scoped physical targets selected by Lix's bound write plan.
///
/// DataFusion table providers never receive this registry and therefore cannot
/// acquire mutation authority through the public `TableProvider` boundary.
#[derive(Default)]
pub(crate) struct WriteTargetRegistry {
    targets: Mutex<BTreeMap<String, Arc<SpecWriteTarget>>>,
}

impl WriteTargetRegistry {
    fn register(&self, name: &str, target: Arc<SpecWriteTarget>) -> Result<(), LixError> {
        let mut targets = self.targets.lock().map_err(|_| {
            LixError::unknown("SQL physical write-target registry lock was poisoned")
        })?;
        if targets.insert(name.to_string(), target).is_some() {
            return Err(LixError::unknown(format!(
                "SQL physical write target '{name}' was registered more than once"
            )));
        }
        Ok(())
    }

    pub(crate) fn target(&self, name: &str) -> Result<Arc<SpecWriteTarget>, LixError> {
        self.targets
            .lock()
            .map_err(|_| LixError::unknown("SQL physical write-target registry lock was poisoned"))?
            .get(name)
            .cloned()
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_UNSUPPORTED_SQL,
                    format!("SQL table '{name}' is not a writable Lix surface"),
                )
            })
    }
}

/// The physical mutation capability behind one bound Lix SQL surface.
///
/// RETURNING and ON CONFLICT semantics remain in Lix's bound executor; this
/// target only plans and stages the selected surface's physical operation.
pub(crate) struct SpecWriteTarget {
    spec: Arc<dyn TableSpec>,
    schema: SchemaRef,
    write_ctx: SqlWriteContext,
}

impl SpecWriteTarget {
    fn new(spec: Arc<dyn TableSpec>, write_ctx: SqlWriteContext) -> Self {
        Self {
            schema: spec.schema(),
            spec,
            write_ctx,
        }
    }

    pub(crate) async fn insert(
        &self,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let table = self.spec.table_name();
        self.schema
            .logically_equivalent_names_and_types(&input.schema())?;
        let omitted_insert_columns = self.write_ctx.explicit_insert_columns().map_or_else(
            || InsertColumnIntents::from_input(&input).omitted_columns(self.schema.as_ref()),
            |explicit_columns| {
                self.schema
                    .fields()
                    .iter()
                    .filter(|field| !explicit_columns.contains(field.name().as_str()))
                    .map(|field| field.name().clone())
                    .collect()
            },
        );
        let sink: Arc<dyn InsertSink> = match self
            .spec
            .plan_insert(self.write_ctx.clone(), &input)
            .await?
        {
            Some(apply) => Arc::new(PlannedInsertSink {
                table: table.into(),
                apply,
                omitted_insert_columns,
            }),
            None => Arc::new(SpecInsertSink {
                spec: Arc::clone(&self.spec),
                write_ctx: self.write_ctx.clone(),
                omitted_insert_columns,
            }),
        };
        Ok(Arc::new(InsertExec::new(input, sink)))
    }

    pub(crate) async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let table = self.spec.table_name();
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
            .plan_update(self.write_ctx.clone(), physical_assignments, &filters)
            .await?;
        Ok(Arc::new(SpecDmlExec::new(
            table.into(),
            "UPDATE",
            planned,
            physical_filters,
            None,
        )))
    }

    pub(crate) async fn delete(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.delete_impl(state, filters, None).await
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
        let (support, target) = self.validate_upsert(input, target_columns).await?;
        upsert::execute_upsert(support, &self.write_ctx, proposed_batches, &target, action).await
    }

    /// Execute an `INSERT ... ON CONFLICT ... RETURNING` through the shared
    /// upsert driver. The driver requires provider-owned post-image capture,
    /// so this has no count-only fallback.
    pub(crate) async fn execute_upsert_with_returning(
        &self,
        input: &Arc<dyn ExecutionPlan>,
        proposed_batches: Vec<RecordBatch>,
        target_columns: &[String],
        action: &upsert::UpsertAction,
        returning: DmlReturning,
    ) -> Result<u64> {
        let (support, target) = self.validate_upsert(input, target_columns).await?;
        upsert::execute_upsert_with_returning(
            support,
            &self.write_ctx,
            proposed_batches,
            &target,
            action,
            returning,
        )
        .await
    }

    async fn validate_upsert(
        &self,
        input: &Arc<dyn ExecutionPlan>,
        target_columns: &[String],
    ) -> Result<(&dyn upsert::UpsertSupport, upsert::UpsertConflictTarget)> {
        let table = self.spec.table_name();
        self.schema
            .logically_equivalent_names_and_types(&input.schema())?;
        let support = self.spec.upsert_support().ok_or_else(|| {
            DataFusionError::Execution(format!("INSERT ON CONFLICT is not supported on {table}"))
        })?;
        let target = support.resolve_conflict_target(table, target_columns)?;
        self.spec.plan_insert(self.write_ctx.clone(), input).await?;
        Ok((support, target))
    }

    pub(crate) async fn validate_upsert_target(
        &self,
        input: &Arc<dyn ExecutionPlan>,
        target_columns: &[String],
    ) -> Result<()> {
        self.validate_upsert(input, target_columns).await.map(drop)
    }

    pub(crate) async fn delete_with_returning(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
        returning: DmlReturning,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.delete_impl(state, filters, Some(returning)).await
    }

    /// Plan an INSERT whose provider captures a post-write `RETURNING`
    /// projection. This path is separate from `TableProvider::insert_into` so
    /// only providers that explicitly implement post-image capture can expose
    /// the SQL surface.
    pub(crate) async fn insert_with_returning(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        returning: DmlReturning,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let table = self.spec.table_name();
        self.schema
            .logically_equivalent_names_and_types(&input.schema())?;
        let omitted_insert_columns = self.write_ctx.explicit_insert_columns().map_or_else(
            || InsertColumnIntents::from_input(&input).omitted_columns(self.schema.as_ref()),
            |explicit_columns| {
                self.schema
                    .fields()
                    .iter()
                    .filter(|field| !explicit_columns.contains(field.name().as_str()))
                    .map(|field| field.name().clone())
                    .collect()
            },
        );
        let apply = self
            .spec
            .plan_insert_with_returning(self.write_ctx.clone(), &input, returning)
            .await?;
        let sink: Arc<dyn InsertSink> = Arc::new(PlannedInsertSink {
            table: table.into(),
            apply,
            omitted_insert_columns,
        });
        Ok(Arc::new(InsertExec::new(input, sink)))
    }

    /// Plan an UPDATE whose provider captures a post-write `RETURNING`
    /// projection. `SpecDmlExec` receives no returning projection here because
    /// its built-in capture is intentionally the DELETE pre-image path.
    pub(crate) async fn update_with_returning(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
        returning: DmlReturning,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let table = self.spec.table_name();
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
            .plan_update_with_returning(
                self.write_ctx.clone(),
                physical_assignments,
                &filters,
                returning,
            )
            .await?;
        Ok(Arc::new(SpecDmlExec::new(
            table.into(),
            "UPDATE",
            planned,
            physical_filters,
            None,
        )))
    }

    async fn delete_impl(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
        returning: Option<DmlReturning>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let table = self.spec.table_name();
        let filters = self.spec.prepare_write_filters(filters)?;
        let physical_filters = physical_filters(&self.schema, &filters, state)?;
        let planned = self
            .spec
            .plan_delete_with_options(
                self.write_ctx.clone(),
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
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let physical_cache_key = PhysicalScanKey {
            table: self.spec.table_name().into(),
            projection: projection.cloned(),
            filters: filters.iter().map(ToString::to_string).collect(),
            limit,
        };
        let constant_columns = exact_filter_constant_columns(self.spec.as_ref(), filters);
        let planned = self
            .spec
            .plan_scan(projection, filters, limit, state.execution_props())
            .await?;
        // Runtime sharing is deliberately limited to an unmodified source read.
        // Pushed filters and limits can make two scans with the same projection
        // observe different source behavior that is not represented in this key.
        let statement_cache_key =
            (filters.is_empty() && limit.is_none()).then(|| StatementScanKey {
                provider_id: self.provider_id,
                table: self.spec.table_name().into(),
                projection: projection.cloned(),
            });
        Ok(Arc::new(SpecScanExec::new(
            self.spec.table_name().into(),
            planned,
            state.config().target_partitions(),
            statement_cache_key,
            physical_cache_key,
            &constant_columns,
        )?))
    }
}

/// Output columns pinned to one literal value by a fully-applied (`Exact`)
/// pushed-down filter.
///
/// `Exact` pushdown means the provider applies the predicate in full, so every
/// row leaving the scan satisfies it. For predicates shaped `col = literal` or
/// `col IN (single literal)` the column is therefore constant across the scan's
/// output — the same fact `FilterExec` would have advertised through its
/// equivalence properties had the filter stayed above the scan. Restoring it
/// here lets DataFusion's own `EnforceSorting` rule elide sort operators whose
/// sort keys are pinned; the canonical beneficiary is the point read
/// `WHERE pk = … ORDER BY pk`, which otherwise streams its at-most-one row
/// through a full `SortExec`. Conjunctions recurse — an exactly-applied
/// `a = 'x' AND b = 'y'` pins both columns — while `OR`, negated `IN`, and
/// multi-value `IN` pin nothing. Filters the spec reports as `Inexact` or
/// `Unsupported` are skipped: their rows are only narrowed above the scan, so
/// the scan itself proves nothing.
fn exact_filter_constant_columns(spec: &dyn TableSpec, filters: &[Expr]) -> Vec<String> {
    fn collect(filter: &Expr, columns: &mut Vec<String>) {
        match filter {
            Expr::BinaryExpr(binary) if binary.op == Operator::And => {
                collect(&binary.left, columns);
                collect(&binary.right, columns);
            }
            Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
                match (binary.left.as_ref(), binary.right.as_ref()) {
                    (Expr::Column(column), Expr::Literal(..))
                    | (Expr::Literal(..), Expr::Column(column)) => {
                        columns.push(column.name.clone());
                    }
                    _ => {}
                }
            }
            Expr::InList(in_list) if !in_list.negated && in_list.list.len() == 1 => {
                if let (Expr::Column(column), Expr::Literal(..)) =
                    (in_list.expr.as_ref(), &in_list.list[0])
                {
                    columns.push(column.name.clone());
                }
            }
            _ => {}
        }
    }

    let mut columns = Vec::new();
    for filter in filters {
        if spec.filter_pushdown(filter) != TableProviderFilterPushDown::Exact {
            continue;
        }
        collect(filter, &mut columns);
    }
    columns
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
    omitted_insert_columns: BTreeSet<String>,
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
        let batches = batches
            .into_iter()
            .map(|batch| mark_omitted_insert_columns(batch, &self.omitted_insert_columns))
            .collect::<Result<Vec<_>>>()?;
        self.spec.stage_insert(&self.write_ctx, batches).await
    }
}

/// Insert sink for specs that planned their own handler via `plan_insert`.
struct PlannedInsertSink {
    table: Arc<str>,
    apply: InsertApply,
    omitted_insert_columns: BTreeSet<String>,
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
        let batches = batches
            .into_iter()
            .map(|batch| mark_omitted_insert_columns(batch, &self.omitted_insert_columns))
            .collect::<Result<Vec<_>>>()?;
        (self.apply)(batches).await
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct StatementScanKey {
    provider_id: u64,
    table: Arc<str>,
    projection: Option<Vec<usize>>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct PhysicalScanKey {
    table: Arc<str>,
    projection: Option<Vec<usize>>,
    filters: Vec<String>,
    limit: Option<usize>,
}

pub(crate) struct SpecScanExec {
    table: Arc<str>,
    schema: SchemaRef,
    source: ScanSource,
    fragment_ranges: Arc<Vec<Range<usize>>>,
    properties: Arc<PlanProperties>,
    statement_cache_key: Option<StatementScanKey>,
    physical_cache_key: PhysicalScanKey,
}

impl SpecScanExec {
    fn new(
        table: Arc<str>,
        planned: PlannedScan,
        target_partitions: usize,
        statement_cache_key: Option<StatementScanKey>,
        physical_cache_key: PhysicalScanKey,
        constant_columns: &[String],
    ) -> Result<Self> {
        // A declared ordering only proves that each source fragment is sorted,
        // not that adjacent fragments have non-overlapping value ranges.
        // Keep ordered fragments separate unless the source can eventually
        // provide that stronger cross-fragment guarantee.
        let preserve_fragment_boundaries = planned.ordering.is_some();
        let mut equivalence_properties = planned
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
        // Exact-pushdown equalities pin these output columns to one literal
        // value, uniformly across every partition. Columns pruned from the
        // projected schema carry no ordering obligations and are skipped.
        let constants = constant_columns
            .iter()
            .filter_map(|column_name| {
                planned.schema.index_of(column_name).ok().map(|index| {
                    ConstExpr::new(
                        Arc::new(Column::new(column_name, index)),
                        AcrossPartitions::Uniform(None),
                    )
                })
            })
            .collect::<Vec<_>>();
        if !constants.is_empty() {
            equivalence_properties.add_constants(constants)?;
        }
        let grouped_target = if preserve_fragment_boundaries {
            planned.source.partition_count
        } else {
            target_partitions.max(1)
        };
        let fragment_ranges =
            grouped_fragment_ranges(planned.source.partition_count, grouped_target);
        let properties = PlanProperties::new(
            equivalence_properties,
            Partitioning::UnknownPartitioning(fragment_ranges.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(Self {
            table,
            schema: planned.schema,
            source: planned.source,
            fragment_ranges: Arc::new(fragment_ranges),
            properties: Arc::new(properties),
            statement_cache_key,
            physical_cache_key,
        })
    }

    #[cfg(test)]
    fn new_for_test(
        table: Arc<str>,
        planned: PlannedScan,
        target_partitions: usize,
        statement_cache_key: Option<StatementScanKey>,
    ) -> Self {
        let physical_cache_key = PhysicalScanKey {
            table: Arc::clone(&table),
            projection: None,
            filters: Vec::new(),
            limit: None,
        };
        Self::new(
            table,
            planned,
            target_partitions,
            statement_cache_key,
            physical_cache_key,
            &[],
        )
        .expect("test scan properties build")
    }

    pub(crate) fn statement_cache_key(&self) -> Option<&StatementScanKey> {
        self.statement_cache_key.as_ref()
    }

    pub(crate) fn physical_cache_key(&self) -> &PhysicalScanKey {
        &self.physical_cache_key
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
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let fragment_range = self
            .fragment_ranges
            .get(partition)
            .cloned()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "SpecScanExec({}) exposes {} partitions, got {partition}",
                    self.table,
                    self.fragment_ranges.len()
                ))
            })?;
        let source = self.source.clone();
        let schema = Arc::clone(&self.schema);
        let table = Arc::clone(&self.table);
        let fragments = fragment_range
            .map(move |fragment| {
                let fragment_stream = source.open(fragment, Arc::clone(&context))?;
                if fragment_stream.schema() != schema {
                    return Err(DataFusionError::Execution(format!(
                        "SpecScanExec({table}) stream schema does not match its planned schema"
                    )));
                }
                Ok(fragment_stream)
            })
            .collect::<Result<Vec<_>>>()?;
        let fragments =
            stream::iter(fragments.into_iter().map(Ok::<_, DataFusionError>)).try_flatten();
        let stream = RecordBatchStreamAdapter::new(Arc::clone(&self.schema), fragments);
        #[cfg(feature = "storage-benches")]
        let stream = ProfiledScanStream::new(stream);
        Ok(Box::pin(stream))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        match partition {
            Some(partition) => {
                let fragment_range = self.fragment_ranges.get(partition).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "SpecScanExec({}) exposes {} partitions, got statistics request for {partition}",
                        self.table,
                        self.fragment_ranges.len()
                    ))
                })?;
                Statistics::try_merge_iter(
                    self.source.statistics[fragment_range.clone()].iter(),
                    self.schema.as_ref(),
                )
            }
            None => match &self.source.source_statistics {
                Some(statistics) => Ok(statistics.clone()),
                None => {
                    Statistics::try_merge_iter(self.source.statistics.iter(), self.schema.as_ref())
                }
            },
        }
    }
}

#[cfg(feature = "storage-benches")]
struct ProfiledScanStream<S> {
    inner: S,
}

#[cfg(feature = "storage-benches")]
impl<S> ProfiledScanStream<S> {
    fn new(inner: S) -> Self {
        Self { inner }
    }
}

#[cfg(feature = "storage-benches")]
impl<S> Stream for ProfiledScanStream<S>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let started = crate::sql_profile::is_active().then(std::time::Instant::now);
        let polled = std::pin::Pin::new(&mut self.inner).poll_next(cx);
        if let Some(started) = started {
            let elapsed = started.elapsed();
            match &polled {
                std::task::Poll::Ready(Some(Ok(batch))) => crate::sql_profile::record_scan(
                    batch.num_rows(),
                    1,
                    batch.get_array_memory_size(),
                    elapsed,
                ),
                _ => crate::sql_profile::record_scan(0, 0, 0, elapsed),
            }
        }
        polled
    }
}

#[cfg(feature = "storage-benches")]
impl<S> datafusion::physical_plan::RecordBatchStream for ProfiledScanStream<S>
where
    S: datafusion::physical_plan::RecordBatchStream + Unpin,
{
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

fn grouped_fragment_ranges(fragment_count: usize, target_partitions: usize) -> Vec<Range<usize>> {
    debug_assert!(fragment_count > 0);
    let partition_count = fragment_count.min(target_partitions.max(1));
    (0..partition_count)
        .map(|partition| {
            partition * fragment_count / partition_count
                ..(partition + 1) * fragment_count / partition_count
        })
        .collect()
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

/// Select rows from a fully materialized provider batch in a caller-defined
/// order. `RETURNING` reloads use this after reading the transaction-visible
/// post-image by stable provider identity, so the result still corresponds to
/// the input write rows rather than incidental storage scan ordering.
pub(super) fn take_record_batch_rows(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch> {
    let indices = UInt32Array::from(indices.to_vec());
    let columns = batch
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    RecordBatch::try_new(batch.schema(), columns).map_err(DataFusionError::from)
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

#[cfg(test)]
mod scan_source_tests {
    use std::collections::HashMap;
    use std::future::pending;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use datafusion::arrow::array::Int64Array;
    use datafusion::common::stats::Precision;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::union::UnionExec;
    use datafusion::prelude::SessionConfig;
    use futures_util::{StreamExt, TryStreamExt};

    use super::*;

    fn int_schema(name: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]))
    }

    fn int_batch(schema: SchemaRef, values: &[i64]) -> RecordBatch {
        let values: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
        RecordBatch::try_new(schema, vec![values]).expect("test batch should match schema")
    }

    struct CountingSpec {
        schema: SchemaRef,
        opens: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl TableSpec for CountingSpec {
        fn table_name(&self) -> &str {
            "counted"
        }

        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        async fn plan_scan(
            &self,
            projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
            _props: &ExecutionProps,
        ) -> Result<PlannedScan> {
            let schema = projected_schema(&self.schema, projection);
            let source_schema = Arc::clone(&schema);
            let opens = Arc::clone(&self.opens);
            let source = batch_stream_source(Arc::clone(&schema), 1, move |_, _| {
                opens.fetch_add(1, Ordering::SeqCst);
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    Arc::clone(&source_schema),
                    stream::iter([Ok(int_batch(Arc::clone(&source_schema), &[1, 2, 3]))]),
                )))
            });
            Ok(PlannedScan {
                schema,
                source,
                ordering: None,
            })
        }
    }

    #[tokio::test]
    async fn referenced_twice_cte_opens_identical_scan_once_per_statement() {
        let opens = Arc::new(AtomicUsize::new(0));
        let spec = Arc::new(CountingSpec {
            schema: int_schema("value"),
            opens: Arc::clone(&opens),
        });
        let session = crate::sql2::session::new_sql_session_context();
        session
            .register_table("counted", Arc::new(SpecTableProvider::new(spec)))
            .expect("counted test table should register");
        let plan = session
            .state()
            .create_logical_plan(
                "WITH reused AS (SELECT value FROM counted) \
                 SELECT value FROM reused UNION ALL SELECT value FROM reused",
            )
            .await
            .expect("CTE should plan");
        let batches = crate::sql2::runtime::collect_plan(&session.state(), plan, None)
            .await
            .expect("CTE should execute");
        let values = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("CTE value should be Int64")
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();

        assert_eq!(values, [1, 2, 3, 1, 2, 3]);
        assert_eq!(opens.load(Ordering::SeqCst), 1);
    }

    fn cacheable_scan(
        schema: SchemaRef,
        source: ScanSource,
        provider_id: u64,
    ) -> Arc<dyn ExecutionPlan> {
        let key = StatementScanKey {
            provider_id,
            table: Arc::from("cache_test"),
            projection: None,
        };
        Arc::new(SpecScanExec::new_for_test(
            Arc::from("cache_test"),
            PlannedScan {
                schema,
                source,
                ordering: None,
            },
            1,
            Some(key),
        ))
    }

    fn repeated_cacheable_scan_plan(
        schema: SchemaRef,
        source: ScanSource,
    ) -> Arc<dyn ExecutionPlan> {
        let scans = (0..2)
            .map(|_| cacheable_scan(Arc::clone(&schema), source.clone(), 0))
            .collect();
        UnionExec::try_new(scans).expect("cache test union schemas should match")
    }

    #[tokio::test]
    async fn distinct_provider_instances_with_same_table_name_do_not_share() {
        let schema = int_schema("value");
        let first_opens = Arc::new(AtomicUsize::new(0));
        let second_opens = Arc::new(AtomicUsize::new(0));
        let source = |value, opens: Arc<AtomicUsize>| {
            let source_schema = Arc::clone(&schema);
            batch_stream_source(Arc::clone(&schema), 1, move |_, _| {
                opens.fetch_add(1, Ordering::SeqCst);
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    Arc::clone(&source_schema),
                    stream::iter([Ok(int_batch(Arc::clone(&source_schema), &[value]))]),
                )))
            })
        };
        let scans = vec![
            cacheable_scan(Arc::clone(&schema), source(1, Arc::clone(&first_opens)), 1),
            cacheable_scan(Arc::clone(&schema), source(2, Arc::clone(&second_opens)), 2),
        ];
        let plan = crate::sql2::runtime::adapt_runtime_plan(
            UnionExec::try_new(scans).expect("provider identity union should plan"),
        )
        .expect("provider identity union should adapt");
        let context = Arc::new(TaskContext::default());
        let mut values = Vec::new();
        for partition in 0..2 {
            let batches = plan
                .execute(partition, Arc::clone(&context))
                .expect("provider identity scan should open")
                .try_collect::<Vec<_>>()
                .await
                .expect("provider identity scan should complete");
            values.push(
                batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("provider identity value should be Int64")
                    .value(0),
            );
        }

        assert_eq!(values, [1, 2]);
        assert_eq!(first_opens.load(Ordering::SeqCst), 1);
        assert_eq!(second_opens.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn concurrent_scan_cache_consumers_share_one_source_open() {
        let schema = int_schema("value");
        let opens = Arc::new(AtomicUsize::new(0));
        let source_schema = Arc::clone(&schema);
        let source_opens = Arc::clone(&opens);
        let source = batch_stream_source(Arc::clone(&schema), 1, move |_, _| {
            source_opens.fetch_add(1, Ordering::SeqCst);
            let schema = Arc::clone(&source_schema);
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&schema),
                stream::once(async move {
                    tokio::task::yield_now().await;
                    Ok(int_batch(schema, &[1, 2, 3]))
                }),
            )))
        });
        let plan = crate::sql2::runtime::adapt_runtime_plan(repeated_cacheable_scan_plan(
            Arc::clone(&schema),
            source,
        ))
        .expect("cacheable union should adapt");
        let context = Arc::new(TaskContext::default());
        let first = plan
            .execute(0, Arc::clone(&context))
            .expect("first cache consumer should open")
            .try_collect::<Vec<_>>();
        let second = plan
            .execute(1, context)
            .expect("second cache consumer should open")
            .try_collect::<Vec<_>>();
        let (first, second) = tokio::join!(first, second);

        assert_eq!(
            first.expect("first consumer should complete")[0].num_rows(),
            3
        );
        assert_eq!(
            second.expect("second consumer should complete")[0].num_rows(),
            3
        );
        assert_eq!(opens.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn scan_cache_shares_source_failure_without_retrying() {
        const ERROR_CODE: &str = "LIX_ERROR_STATEMENT_SCAN_CACHE_TEST";
        let schema = int_schema("value");
        let opens = Arc::new(AtomicUsize::new(0));
        let source_schema = Arc::clone(&schema);
        let source_opens = Arc::clone(&opens);
        let source = batch_stream_source(Arc::clone(&schema), 1, move |_, _| {
            source_opens.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&source_schema),
                stream::iter([Err(DataFusionError::External(Box::new(LixError::new(
                    ERROR_CODE,
                    "source failed",
                ))))]),
            )))
        });
        let plan =
            crate::sql2::runtime::adapt_runtime_plan(repeated_cacheable_scan_plan(schema, source))
                .expect("cacheable union should adapt");
        let context = Arc::new(TaskContext::default());
        for partition in 0..2 {
            let error = plan
                .execute(partition, Arc::clone(&context))
                .expect("cache consumer should open")
                .try_collect::<Vec<_>>()
                .await
                .expect_err("cached source failure should propagate");
            let error = crate::sql2::error::datafusion_error_to_lix_error(error);
            assert_eq!(error.code, ERROR_CODE);
            assert!(error.message.contains("source failed"));
        }
        assert_eq!(opens.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn scan_cache_reserves_incrementally_and_releases_on_memory_error() {
        let schema = int_schema("value");
        let batch = int_batch(Arc::clone(&schema), &[1, 2, 3]);
        let one_batch_bytes = batch.get_array_memory_size();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(one_batch_bytes));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool))
            .build_arc()
            .expect("limited runtime should build");
        let context = Arc::new(TaskContext::new(
            None,
            "statement-scan-cache-memory-test".into(),
            SessionConfig::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            runtime,
        ));
        let opens = Arc::new(AtomicUsize::new(0));
        let source_schema = Arc::clone(&schema);
        let source_opens = Arc::clone(&opens);
        let source = batch_stream_source(Arc::clone(&schema), 1, move |_, _| {
            source_opens.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&source_schema),
                stream::iter([Ok(batch.clone()), Ok(batch.clone())]),
            )))
        });
        let plan =
            crate::sql2::runtime::adapt_runtime_plan(repeated_cacheable_scan_plan(schema, source))
                .expect("cacheable union should adapt");
        let error = plan
            .execute(0, context)
            .expect("cache consumer should open")
            .try_collect::<Vec<_>>()
            .await
            .expect_err("second retained batch should exceed the memory pool");

        assert!(error.to_string().contains("Resources exhausted"));
        assert_eq!(opens.load(Ordering::SeqCst), 1);
        assert_eq!(pool.reserved(), 0);
    }

    #[tokio::test]
    async fn cancelled_cache_initializer_can_be_retried_by_another_consumer() {
        let schema = int_schema("value");
        let opens = Arc::new(AtomicUsize::new(0));
        let source_schema = Arc::clone(&schema);
        let source_opens = Arc::clone(&opens);
        let source = batch_stream_source(Arc::clone(&schema), 1, move |_, _| {
            let open = source_opens.fetch_add(1, Ordering::SeqCst);
            let schema = Arc::clone(&source_schema);
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&schema),
                stream::once(async move {
                    if open == 0 {
                        pending::<()>().await;
                    }
                    Ok(int_batch(schema, &[1, 2, 3]))
                }),
            )))
        });
        let plan =
            crate::sql2::runtime::adapt_runtime_plan(repeated_cacheable_scan_plan(schema, source))
                .expect("cacheable union should adapt");
        let context = Arc::new(TaskContext::default());
        let mut first = plan
            .execute(0, Arc::clone(&context))
            .expect("first cache consumer should open");
        assert!(
            tokio::time::timeout(Duration::from_millis(10), first.next())
                .await
                .is_err()
        );
        drop(first);

        let second = plan
            .execute(1, context)
            .expect("second cache consumer should open")
            .try_collect::<Vec<_>>()
            .await
            .expect("second consumer should retry cancelled initialization");
        assert_eq!(second[0].num_rows(), 3);
        assert_eq!(opens.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn streaming_scan_is_incremental_reusable_and_partition_checked() {
        let schema = int_schema("value");
        let source_schema = Arc::clone(&schema);
        let opens = Arc::new(AtomicUsize::new(0));
        let source_opens = Arc::clone(&opens);
        let source = batch_stream_source(Arc::clone(&schema), 1, move |_partition, _context| {
            source_opens.fetch_add(1, Ordering::SeqCst);
            let batches = vec![
                Ok(int_batch(Arc::clone(&source_schema), &[1, 2])),
                Ok(int_batch(Arc::clone(&source_schema), &[3])),
            ];
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&source_schema),
                stream::iter(batches),
            )))
        });
        let exec = SpecScanExec::new_for_test(
            Arc::from("stream_test"),
            PlannedScan {
                schema: Arc::clone(&schema),
                source,
                ordering: None,
            },
            1,
            None,
        );

        for _ in 0..2 {
            let batches = exec
                .execute(0, Arc::new(TaskContext::default()))
                .expect("partition zero should open")
                .try_collect::<Vec<_>>()
                .await
                .expect("stream should complete");
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
            assert_eq!(batches.len(), 2);
        }
        assert_eq!(opens.load(Ordering::SeqCst), 2);
        assert!(exec.execute(1, Arc::new(TaskContext::default())).is_err());
    }

    #[test]
    fn streaming_scan_rejects_schema_drift_before_polling() {
        let planned_schema = int_schema("expected");
        let stream_schema = int_schema("unexpected");
        let source = batch_stream_source(
            Arc::clone(&planned_schema),
            1,
            move |_partition, _context| {
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    Arc::clone(&stream_schema),
                    stream::empty(),
                )))
            },
        );
        let exec = SpecScanExec::new_for_test(
            Arc::from("schema_drift_test"),
            PlannedScan {
                schema: planned_schema,
                source,
                ordering: None,
            },
            1,
            None,
        );

        let error = exec
            .execute(0, Arc::new(TaskContext::default()))
            .err()
            .expect("schema drift must fail");
        assert!(error.to_string().contains("stream schema"));
    }

    #[test]
    fn streaming_scan_exposes_and_merges_exact_partition_statistics() {
        let schema = int_schema("value");
        let statistics = [2, 3]
            .into_iter()
            .map(|rows| {
                Statistics::new_unknown(schema.as_ref()).with_num_rows(Precision::Exact(rows))
            })
            .collect();
        let source_schema = Arc::clone(&schema);
        let source = batch_stream_source_with_statistics(
            Arc::clone(&schema),
            statistics,
            move |_partition, _context| {
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    Arc::clone(&source_schema),
                    stream::empty(),
                )))
            },
        );
        let exec = SpecScanExec::new_for_test(
            Arc::from("statistics_test"),
            PlannedScan {
                schema,
                source,
                ordering: None,
            },
            2,
            None,
        );

        assert_eq!(
            exec.partition_statistics(Some(0))
                .expect("partition statistics")
                .num_rows,
            Precision::Exact(2)
        );
        assert_eq!(
            exec.partition_statistics(None)
                .expect("merged statistics")
                .num_rows,
            Precision::Exact(5)
        );
    }

    #[test]
    fn streaming_scan_source_statistics_override_only_the_union() {
        let schema = int_schema("value");
        let partition_statistics = [Precision::Absent, Precision::Absent]
            .into_iter()
            .map(|rows| Statistics::new_unknown(schema.as_ref()).with_num_rows(rows))
            .collect();
        let source_statistics =
            Statistics::new_unknown(schema.as_ref()).with_num_rows(Precision::Exact(7));
        let source_schema = Arc::clone(&schema);
        let source = batch_stream_source_with_statistics_and_source(
            Arc::clone(&schema),
            partition_statistics,
            Some(source_statistics),
            move |_partition, _context| {
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    Arc::clone(&source_schema),
                    stream::empty(),
                )))
            },
        );
        let exec = SpecScanExec::new_for_test(
            Arc::from("source_statistics_test"),
            PlannedScan {
                schema,
                source,
                ordering: None,
            },
            2,
            None,
        );

        assert_eq!(
            exec.partition_statistics(Some(0))
                .expect("partition statistics")
                .num_rows,
            Precision::Absent
        );
        assert_eq!(
            exec.partition_statistics(None)
                .expect("source statistics")
                .num_rows,
            Precision::Exact(7)
        );
    }

    #[test]
    fn fragment_ranges_are_contiguous_balanced_and_bounded_by_target() {
        assert_eq!(grouped_fragment_ranges(5, 2), [0..2, 2..5]);
        assert_eq!(grouped_fragment_ranges(2, 8), [0..1, 1..2]);
        let one_partition = grouped_fragment_ranges(3, 0);
        assert_eq!(one_partition.len(), 1);
        assert_eq!(one_partition[0], 0..3);
    }

    #[tokio::test]
    async fn grouped_scan_preserves_fragment_order_and_merges_partition_statistics() {
        let schema = int_schema("value");
        let statistics = (0..5)
            .map(|_| Statistics::new_unknown(schema.as_ref()).with_num_rows(Precision::Exact(1)))
            .collect();
        let source_schema = Arc::clone(&schema);
        let source = batch_stream_source_with_statistics(
            Arc::clone(&schema),
            statistics,
            move |fragment, _context| {
                let batch = int_batch(Arc::clone(&source_schema), &[fragment as i64]);
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    Arc::clone(&source_schema),
                    stream::iter([Ok(batch)]),
                )))
            },
        );
        let exec = SpecScanExec::new_for_test(
            Arc::from("grouped_test"),
            PlannedScan {
                schema,
                source,
                ordering: None,
            },
            2,
            None,
        );

        assert_eq!(exec.properties().output_partitioning().partition_count(), 2);
        for (partition, expected) in [(0, vec![0, 1]), (1, vec![2, 3, 4])] {
            let batches = exec
                .execute(partition, Arc::new(TaskContext::default()))
                .expect("grouped partition should open")
                .try_collect::<Vec<_>>()
                .await
                .expect("grouped stream should complete");
            let actual = batches
                .iter()
                .map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("test column should be Int64")
                        .value(0)
                })
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        }
        assert_eq!(
            exec.partition_statistics(Some(0))
                .expect("first grouped statistics")
                .num_rows,
            Precision::Exact(2)
        );
        assert_eq!(
            exec.partition_statistics(Some(1))
                .expect("second grouped statistics")
                .num_rows,
            Precision::Exact(3)
        );
        assert!(exec.partition_statistics(Some(2)).is_err());
    }

    #[test]
    fn grouped_scan_keeps_ordered_source_fragments_separate() {
        let schema = int_schema("value");
        let source = batch_stream_source(Arc::clone(&schema), 5, {
            let schema = Arc::clone(&schema);
            move |_fragment, _context| {
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    Arc::clone(&schema),
                    stream::empty(),
                )))
            }
        });
        let exec = SpecScanExec::new_for_test(
            Arc::from("ordered_test"),
            PlannedScan {
                schema,
                source,
                ordering: Some("value".into()),
            },
            1,
            None,
        );

        assert_eq!(exec.properties().output_partitioning().partition_count(), 5);
    }
}
