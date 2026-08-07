use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::sync::Arc;
#[cfg(feature = "storage-benches")]
use std::time::Instant;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::ScanArgs;
use datafusion::common::{DataFusionError, internal_err};
use datafusion::dataframe::DataFrame;
use datafusion::datasource::source_as_provider;
use datafusion::error::Result;
use datafusion::execution::SessionState;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::expr_rewriter::unnormalize_cols;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{CardinalityEffect, EmissionType};
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::limit::LimitStream;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use futures_util::{StreamExt, TryStreamExt, stream};
use tokio::sync::OnceCell;

use crate::catalog::CatalogFingerprint;
use crate::sql2::{PhysicalReadPlanCacheKey, SqlPlanningCache};

use super::providers::{PhysicalScanKey, SpecScanExec, StatementScanKey};

type PhysicalPlanningCache = (
    Arc<SqlPlanningCache<CatalogFingerprint>>,
    PhysicalReadPlanCacheKey<CatalogFingerprint>,
);

pub(crate) async fn collect_dataframe(
    dataframe: DataFrame,
    physical_planning_cache: Option<PhysicalPlanningCache>,
) -> Result<Vec<RecordBatch>> {
    let task_ctx = Arc::new(dataframe.task_ctx());
    #[cfg(feature = "storage-benches")]
    let started = crate::sql_profile::is_active().then(Instant::now);
    let plan = create_or_rebind_physical_plan(dataframe, physical_planning_cache).await?;
    let plan = adapt_runtime_plan(plan)?;
    #[cfg(feature = "storage-benches")]
    if let Some(started) = started {
        crate::sql_profile::record_phase(
            crate::sql_profile::Phase::PhysicalPlanning,
            started.elapsed(),
        );
    }
    #[cfg(feature = "storage-benches")]
    let started = crate::sql_profile::is_active().then(Instant::now);
    let result = collect_adapted_input_plan(plan, task_ctx).await;
    #[cfg(feature = "storage-benches")]
    if let Some(started) = started {
        crate::sql_profile::record_phase(
            crate::sql_profile::Phase::ArrowExecution,
            started.elapsed(),
        );
    }
    result
}

/// Create a pull-based stream from a DataFusion physical plan without
/// collecting its output batches first. The caller owns the stream and may
/// stop polling it early; dropping the stream then drops the underlying scan
/// futures as well.
#[cfg(feature = "storage-benches")]
pub(crate) async fn stream_dataframe(
    dataframe: DataFrame,
    physical_planning_cache: Option<PhysicalPlanningCache>,
) -> Result<SendableRecordBatchStream> {
    let task_ctx = Arc::new(dataframe.task_ctx());
    #[cfg(feature = "storage-benches")]
    let started = crate::sql_profile::is_active().then(Instant::now);
    let plan = create_or_rebind_physical_plan(dataframe, physical_planning_cache).await?;
    let plan = adapt_runtime_plan(plan)?;
    #[cfg(feature = "storage-benches")]
    if let Some(started) = started {
        crate::sql_profile::record_phase(
            crate::sql_profile::Phase::PhysicalPlanning,
            started.elapsed(),
        );
    }
    stream_adapted_input_plan(plan, task_ctx)
}

async fn create_or_rebind_physical_plan(
    dataframe: DataFrame,
    physical_planning_cache: Option<PhysicalPlanningCache>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let Some((cache, key)) = physical_planning_cache else {
        return dataframe.create_physical_plan().await;
    };
    let Some(template) = cache.physical_read_plan(&key) else {
        let plan = dataframe.create_physical_plan().await?;
        if let Some(template) = detach_physical_plan_template(Arc::clone(&plan)) {
            cache.remember_physical_read_plan(key, template);
        }
        return Ok(plan);
    };

    let (state, logical_plan) = dataframe.into_parts();
    let optimized = state.optimize(&logical_plan)?;
    if let Some(replacements) = plan_current_spec_scans(&optimized, &state).await?
        && let Some(plan) = rebind_physical_plan_template(template, replacements)
    {
        return Ok(plan);
    }

    // Any provider, partitioning, ordering, schema, or operator-shape drift
    // invalidates the detached template before execution. Fall back to the
    // ordinary DataFusion planner for this statement and evict the stale entry.
    cache.forget_physical_read_plan(&key);
    state
        .query_planner()
        .create_physical_plan(&optimized, &state)
        .await
}

fn collect_logical_table_scans(
    plan: &LogicalPlan,
    scans: &mut Vec<datafusion::logical_expr::TableScan>,
) {
    if let LogicalPlan::TableScan(scan) = plan {
        scans.push(scan.clone());
    }
    for input in plan.inputs() {
        collect_logical_table_scans(input, scans);
    }
}

async fn plan_current_spec_scans(
    logical_plan: &LogicalPlan,
    state: &SessionState,
) -> Result<Option<HashMap<PhysicalScanKey, VecDeque<Arc<dyn ExecutionPlan>>>>> {
    let mut logical_scans = Vec::new();
    collect_logical_table_scans(logical_plan, &mut logical_scans);
    let mut replacements: HashMap<PhysicalScanKey, VecDeque<Arc<dyn ExecutionPlan>>> =
        HashMap::new();
    for scan in logical_scans {
        let Ok(provider) = source_as_provider(&scan.source) else {
            return Ok(None);
        };
        let filters = unnormalize_cols(scan.filters);
        let args = ScanArgs::default()
            .with_projection(scan.projection.as_deref())
            .with_filters(Some(&filters))
            .with_limit(scan.fetch);
        let result = provider.scan_with_args(state, args).await?;
        let plan = Arc::clone(result.plan());
        let Some(spec_scan) = plan.as_any().downcast_ref::<SpecScanExec>() else {
            return Ok(None);
        };
        replacements
            .entry(spec_scan.physical_cache_key().clone())
            .or_default()
            .push_back(plan);
    }
    Ok(Some(replacements))
}

fn detach_physical_plan_template(plan: Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(scan) = plan.as_any().downcast_ref::<SpecScanExec>() {
        return Some(Arc::new(DetachedSpecScanExec::new(scan)));
    }
    if !matches!(
        plan.name(),
        "ProjectionExec" | "HashJoinExec" | "FilterExec" | "CooperativeExec"
    ) {
        return None;
    }
    let children = plan
        .children()
        .into_iter()
        .map(|child| detach_physical_plan_template(Arc::clone(child)))
        .collect::<Option<Vec<_>>>()?;
    rebuild_template_node(plan, children)
}

fn rebind_physical_plan_template(
    plan: Arc<dyn ExecutionPlan>,
    mut replacements: HashMap<PhysicalScanKey, VecDeque<Arc<dyn ExecutionPlan>>>,
) -> Option<Arc<dyn ExecutionPlan>> {
    let plan = rebind_physical_plan_template_inner(plan, &mut replacements)?;
    replacements
        .values()
        .all(VecDeque::is_empty)
        .then_some(plan)
}

fn rebind_physical_plan_template_inner(
    plan: Arc<dyn ExecutionPlan>,
    replacements: &mut HashMap<PhysicalScanKey, VecDeque<Arc<dyn ExecutionPlan>>>,
) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(detached) = plan.as_any().downcast_ref::<DetachedSpecScanExec>() {
        let replacement = replacements.get_mut(&detached.key)?.pop_front()?;
        return (physical_plan_fingerprint(replacement.as_ref()) == detached.fingerprint)
            .then_some(replacement);
    }
    let children = plan
        .children()
        .into_iter()
        .map(|child| rebind_physical_plan_template_inner(Arc::clone(child), replacements))
        .collect::<Option<Vec<_>>>()?;
    rebuild_template_node(plan, children)
}

fn rebuild_template_node(
    plan: Arc<dyn ExecutionPlan>,
    children: Vec<Arc<dyn ExecutionPlan>>,
) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() {
        // `HashJoinExec::with_new_children` intentionally preserves its
        // `OnceAsync` build table and dynamic-filter state. A reusable operator
        // template must explicitly reset both or a later snapshot can observe
        // rows from the first execution.
        return join
            .builder()
            .with_new_children(children)
            .ok()?
            .reset_state()
            .build_exec()
            .ok();
    }
    plan.with_new_children(children).ok()
}

fn physical_plan_fingerprint(plan: &dyn ExecutionPlan) -> Arc<str> {
    Arc::from(format!(
        "schema={:?};properties={:?}",
        plan.schema(),
        plan.properties()
    ))
}

struct DetachedSpecScanExec {
    key: PhysicalScanKey,
    fingerprint: Arc<str>,
    properties: Arc<PlanProperties>,
}

impl DetachedSpecScanExec {
    fn new(scan: &SpecScanExec) -> Self {
        Self {
            key: scan.physical_cache_key().clone(),
            fingerprint: physical_plan_fingerprint(scan),
            properties: Arc::clone(scan.properties()),
        }
    }
}

impl Debug for DetachedSpecScanExec {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DetachedSpecScanExec")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for DetachedSpecScanExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        formatter: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(formatter, "DetachedSpecScanExec")
    }
}

impl ExecutionPlan for DetachedSpecScanExec {
    fn name(&self) -> &'static str {
        "DetachedSpecScanExec"
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
            return internal_err!("DetachedSpecScanExec does not accept children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        internal_err!("detached physical scan template reached execution")
    }
}

pub(crate) async fn collect_input_plan(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
) -> Result<Vec<RecordBatch>> {
    let plan = adapt_runtime_plan(plan)?;
    collect_adapted_input_plan(plan, task_ctx).await
}

async fn collect_adapted_input_plan(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
) -> Result<Vec<RecordBatch>> {
    let partition_count = plan.output_partitioning().partition_count();
    let mut batches = Vec::new();
    for partition in 0..partition_count {
        let partition_batches = plan
            .execute(partition, Arc::clone(&task_ctx))?
            .try_collect::<Vec<_>>()
            .await?;
        batches.extend(partition_batches);
    }
    Ok(batches)
}

#[cfg(feature = "storage-benches")]
fn stream_adapted_input_plan(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    let partition_count = plan.output_partitioning().partition_count();
    if partition_count == 0 {
        return internal_err!("execution plan exposes no output partitions");
    }
    if partition_count == 1 {
        return plan.execute(0, task_ctx);
    }

    // Preserve Lix's serial partition semantics and defer even construction of
    // each child stream until the flattened stream needs that partition.
    // Dropping early therefore leaves later partitions unexecuted, not merely
    // unpolled.
    let schema = plan.schema();
    let streams = stream::iter(0..partition_count).then(move |partition| {
        let plan = Arc::clone(&plan);
        let task_ctx = Arc::clone(&task_ctx);
        async move { plan.execute(partition, task_ctx) }
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        schema,
        streams.try_flatten(),
    )))
}

pub(crate) fn adapt_runtime_plan(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    // DataFusion expands each CTE reference into an independent physical tree.
    // Share only provider-certified identical scan leaves: residual expressions
    // still run independently, so volatile SQL expressions retain their normal
    // evaluation semantics while storage and Arrow decoding happen once.
    let mut scan_counts = HashMap::new();
    collect_statement_scan_counts(&plan, &mut scan_counts);
    let mut caches = HashMap::new();
    adapt_runtime_plan_inner(plan, &scan_counts, &mut caches)
}

fn collect_statement_scan_counts(
    plan: &Arc<dyn ExecutionPlan>,
    counts: &mut HashMap<StatementScanKey, usize>,
) {
    if let Some(key) = plan
        .as_any()
        .downcast_ref::<SpecScanExec>()
        .and_then(SpecScanExec::statement_cache_key)
    {
        *counts.entry(key.clone()).or_default() += 1;
    }
    for child in plan.children() {
        collect_statement_scan_counts(child, counts);
    }
}

fn adapt_runtime_plan_inner(
    plan: Arc<dyn ExecutionPlan>,
    scan_counts: &HashMap<StatementScanKey, usize>,
    caches: &mut HashMap<StatementScanKey, Arc<StatementScanCacheState>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut children_changed = false;
    let mut children = Vec::new();
    for child in plan.children() {
        let original = Arc::clone(child);
        let adapted = adapt_runtime_plan_inner(Arc::clone(child), scan_counts, caches)?;
        children_changed |= !Arc::ptr_eq(&original, &adapted);
        children.push(adapted);
    }
    let plan = if children_changed {
        plan.with_new_children(children)?
    } else {
        plan
    };

    if let Some(key) = plan
        .as_any()
        .downcast_ref::<SpecScanExec>()
        .and_then(SpecScanExec::statement_cache_key)
        .filter(|key| scan_counts.get(*key).copied().unwrap_or_default() > 1)
        .cloned()
    {
        let state = caches
            .entry(key)
            .or_insert_with(|| Arc::new(StatementScanCacheState::new(Arc::clone(&plan))))
            .clone();
        return Ok(Arc::new(StatementScanCacheExec::new(state)));
    }

    let Some(coalesce) = plan.as_any().downcast_ref::<CoalescePartitionsExec>() else {
        return Ok(plan);
    };
    Ok(Arc::new(SerialCoalescePartitionsExec::new(
        Arc::clone(coalesce.input()),
        coalesce.fetch(),
    )))
}

#[derive(Debug)]
enum CachedScanPartition {
    Ready {
        batches: Vec<RecordBatch>,
        _reservation: MemoryReservation,
    },
    Failed {
        error: Arc<DataFusionError>,
    },
}

#[derive(Debug)]
struct StatementScanCacheState {
    // Owned only by the adapted physical plan, so cached batches cannot cross
    // a statement boundary or outlive that statement's pinned Lix snapshot.
    input: Arc<dyn ExecutionPlan>,
    partitions: Vec<Arc<OnceCell<CachedScanPartition>>>,
}

impl StatementScanCacheState {
    fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let partition_count = input.output_partitioning().partition_count();
        Self {
            input,
            partitions: (0..partition_count)
                .map(|_| Arc::new(OnceCell::new()))
                .collect(),
        }
    }
}

#[derive(Debug)]
struct StatementScanCacheExec {
    state: Arc<StatementScanCacheState>,
    properties: Arc<PlanProperties>,
}

impl StatementScanCacheExec {
    fn new(state: Arc<StatementScanCacheState>) -> Self {
        Self {
            properties: Arc::new(
                state
                    .input
                    .properties()
                    .as_ref()
                    .clone()
                    .with_emission_type(EmissionType::Final),
            ),
            state,
        }
    }
}

impl DisplayAs for StatementScanCacheExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StatementScanCacheExec")
    }
}

impl ExecutionPlan for StatementScanCacheExec {
    fn name(&self) -> &'static str {
        "StatementScanCacheExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.state.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "StatementScanCacheExec expects one child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self::new(Arc::new(StatementScanCacheState::new(
            children.swap_remove(0),
        )))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let cell = self
            .state
            .partitions
            .get(partition)
            .cloned()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "StatementScanCacheExec exposes {} partitions, got {partition}",
                    self.state.partitions.len()
                ))
            })?;
        let input = Arc::clone(&self.state.input);
        let schema = self.schema();
        let cached = stream::once(async move {
            let partition = cell
                .get_or_init(|| async {
                    let loaded = async {
                        let reservation = MemoryConsumer::new("StatementScanCacheExec")
                            .register(context.memory_pool());
                        let mut input = input.execute(partition, Arc::clone(&context))?;
                        let mut batches = Vec::new();
                        while let Some(batch) = input.try_next().await? {
                            reservation.try_grow(batch.get_array_memory_size())?;
                            batches.push(batch);
                        }
                        Ok::<_, DataFusionError>((batches, reservation))
                    }
                    .await;
                    match loaded {
                        Ok((batches, reservation)) => CachedScanPartition::Ready {
                            batches,
                            _reservation: reservation,
                        },
                        Err(error) => CachedScanPartition::Failed {
                            error: Arc::new(error),
                        },
                    }
                })
                .await;
            match partition {
                CachedScanPartition::Ready { batches, .. } => {
                    Ok(stream::iter(batches.clone().into_iter().map(Ok)))
                }
                CachedScanPartition::Failed { error } => {
                    Err(DataFusionError::Shared(Arc::clone(error)))
                }
            }
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, cached)))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.state.input.partition_statistics(partition)
    }
}

/// Runtime-neutral partition coalescing.
///
/// DataFusion's coalescer spawns one task per input partition. Lix deliberately
/// uses a single-partition SQL session and can merge the structural partitions
/// produced by operators such as `UNION ALL` serially instead. Keeping this
/// adapter target-independent gives native and WebAssembly the same SQL plan
/// semantics without relying on runtime-specific task spawning.
#[derive(Debug)]
struct SerialCoalescePartitionsExec {
    input: Arc<dyn ExecutionPlan>,
    fetch: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    properties: Arc<PlanProperties>,
}

impl SerialCoalescePartitionsExec {
    fn new(input: Arc<dyn ExecutionPlan>, fetch: Option<usize>) -> Self {
        let datafusion_plan = CoalescePartitionsExec::new(Arc::clone(&input)).with_fetch(fetch);
        Self {
            input,
            fetch,
            metrics: ExecutionPlanMetricsSet::new(),
            properties: Arc::clone(datafusion_plan.properties()),
        }
    }
}

impl DisplayAs for SerialCoalescePartitionsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SerialCoalescePartitionsExec")?;
        if let Some(fetch) = self.fetch {
            write!(f, ": fetch={fetch}")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for SerialCoalescePartitionsExec {
    fn name(&self) -> &'static str {
        "SerialCoalescePartitionsExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "SerialCoalescePartitionsExec expects one child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self::new(children.swap_remove(0), self.fetch)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!(
                "SerialCoalescePartitionsExec only exposes partition 0, got {partition}"
            );
        }
        let partition_count = self.input.output_partitioning().partition_count();
        if partition_count == 0 {
            return internal_err!(
                "SerialCoalescePartitionsExec requires at least one input partition"
            );
        }

        let streams = (0..partition_count)
            .map(|input_partition| self.input.execute(input_partition, Arc::clone(&context)))
            .collect::<Result<Vec<_>>>()?;
        let schema = self.schema();
        let serial_stream = stream::iter(streams).flatten();
        let adapted: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            serial_stream,
        ));

        if self.fetch.is_none() {
            return Ok(adapted);
        }
        Ok(Box::pin(LimitStream::new(
            adapted,
            0,
            self.fetch,
            BaselineMetrics::new(&self.metrics, partition),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Statistics> {
        self.input
            .partition_statistics(None)?
            .with_fetch(self.fetch, 0, 1)
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }
}
