use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::sync::Arc;
#[cfg(feature = "storage-benches")]
use std::time::Instant;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::ScanArgs;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, NullEquality, ScalarValue, internal_err};
use datafusion::datasource::provider_as_source;
use datafusion::error::Result;
use datafusion::execution::SessionState;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::logical_expr::expr_rewriter::unnormalize_cols;
use datafusion::logical_expr::{JoinType, LogicalPlan};
use datafusion::physical_expr::expressions::Column as PhysicalColumn;
use datafusion::physical_expr::{LexOrdering, Partitioning, PhysicalExpr};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{Boundedness, CardinalityEffect, EmissionType};
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::limit::LimitStream;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream, Statistics,
    StatisticsArgs, StatisticsContext,
};
use futures_util::{StreamExt, TryStreamExt, stream};
use tokio::sync::OnceCell;

use crate::catalog::CatalogFingerprint;
use crate::sql2::{
    CachedPhysicalRead, CachedScanRequest, PhysicalReadPlanCacheKey, SqlPlanningCache,
};

use super::providers::{PhysicalScanKey, SpecScanExec, StatementScanKey};

type PhysicalPlanningCache = (
    Arc<SqlPlanningCache<CatalogFingerprint>>,
    PhysicalReadPlanCacheKey<CatalogFingerprint>,
);

/// One read statement's logical plan as handed to the execution runtime.
///
/// A warm physical-template execution never reads the logical plan at all —
/// the cached leaf-scan requests replan directly against the current session —
/// so a statement that owns a physical-cache key hands over the planning
/// cache's *detached* template (table scans holding `EmptyTable` placeholders)
/// instead of eagerly rebinding every scan to live providers first. The
/// rebinding then happens here, and only on the paths that actually plan the
/// logical tree.
pub(crate) enum RuntimeReadPlan {
    /// Table scans carry live snapshot-bound providers; plannable as-is.
    Bound(LogicalPlan),
    /// Table scans carry detached `EmptyTable` placeholders and must be
    /// rebound against the session state before DataFusion may plan them.
    Detached(LogicalPlan),
}

impl RuntimeReadPlan {
    pub(crate) fn inner(&self) -> &LogicalPlan {
        match self {
            Self::Bound(plan) | Self::Detached(plan) => plan,
        }
    }

    async fn into_bound(self, state: &SessionState) -> Result<LogicalPlan> {
        match self {
            Self::Bound(plan) => Ok(plan),
            Self::Detached(plan) => rebind_detached_read_plan(state, plan).await,
        }
    }
}

/// Replaces detached `EmptyTable` scan sources with the current session's
/// providers. Mirrors the eager rebinding previously done during logical
/// planning; provider resolution goes through the same shared catalog list.
async fn rebind_detached_read_plan(state: &SessionState, plan: LogicalPlan) -> Result<LogicalPlan> {
    let mut tables = BTreeSet::new();
    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            tables.insert(scan.table_name.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    let mut providers = BTreeMap::new();
    for table in tables {
        let provider = state
            .schema_for_ref(table.clone())?
            .table(table.table())
            .await?
            .ok_or_else(|| {
                DataFusionError::Plan(format!("cached SQL plan provider '{table}' is unavailable"))
            })?;
        providers.insert(table, provider_as_source(provider));
    }
    plan.transform_up(|node| {
        let LogicalPlan::TableScan(mut scan) = node else {
            return Ok(Transformed::no(node));
        };
        scan.source = providers.get(&scan.table_name).cloned().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "cached SQL plan provider '{}' is unavailable",
                scan.table_name
            ))
        })?;
        Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
    })
    .map(|transformed| transformed.data)
}

pub(crate) async fn collect_plan(
    state: &SessionState,
    logical_plan: RuntimeReadPlan,
    physical_planning_cache: Option<PhysicalPlanningCache>,
) -> Result<Vec<RecordBatch>> {
    collect_plan_with_schema(state, logical_plan, physical_planning_cache)
        .await
        .map(|(_, batches)| batches)
}

/// Return the physical schema even for an empty result. Logical plans before
/// coercion can still advertise NULL for the first input of a UNION.
pub(crate) async fn collect_plan_with_schema(
    state: &SessionState,
    logical_plan: RuntimeReadPlan,
    physical_planning_cache: Option<PhysicalPlanningCache>,
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let task_ctx = execution_task_context(state);
    #[cfg(feature = "storage-benches")]
    let started = crate::sql_profile::is_active().then(Instant::now);
    let plan = create_or_rebind_physical_plan(state, logical_plan, physical_planning_cache).await?;
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
    let schema = plan.schema();
    let result = collect_bounded_read_output(plan, task_ctx).await;
    #[cfg(feature = "storage-benches")]
    if let Some(started) = started {
        crate::sql_profile::record_phase(
            crate::sql_profile::Phase::ArrowExecution,
            started.elapsed(),
        );
    }
    result.map(|batches| (schema, batches))
}

/// Create a pull-based stream from a DataFusion physical plan without
/// collecting its output batches first. The caller owns the stream and may
/// stop polling it early; dropping the stream then drops the underlying scan
/// futures as well.
#[cfg(feature = "storage-benches")]
pub(crate) async fn stream_plan(
    state: &SessionState,
    logical_plan: RuntimeReadPlan,
    physical_planning_cache: Option<PhysicalPlanningCache>,
) -> Result<SendableRecordBatchStream> {
    let task_ctx = execution_task_context(state);
    #[cfg(feature = "storage-benches")]
    let started = crate::sql_profile::is_active().then(Instant::now);
    let plan = create_or_rebind_physical_plan(state, logical_plan, physical_planning_cache).await?;
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

/// Builds the execution context for one statement without copying the session
/// function registries.
///
/// `TaskContext`'s registries exist for plan deserialization. Physical
/// expressions already own an `Arc` to every function they call, and Lix never
/// resolves a function by name during execution, so cloning several hundred
/// `String` keys per query would be pure overhead.
fn execution_task_context(state: &SessionState) -> Arc<TaskContext> {
    Arc::new(TaskContext::new(
        None,
        state.session_id().to_string(),
        state.config().clone(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        Arc::clone(state.runtime_env()),
    ))
}

async fn create_or_rebind_physical_plan(
    state: &SessionState,
    logical_plan: RuntimeReadPlan,
    physical_planning_cache: Option<PhysicalPlanningCache>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let Some((cache, key)) = physical_planning_cache else {
        return state
            .create_physical_plan(&logical_plan.into_bound(state).await?)
            .await;
    };
    let Some(cached) = cache.physical_read_plan(&key) else {
        let logical_plan = logical_plan.into_bound(state).await?;
        let optimized = state.optimize(&logical_plan)?;
        let plan = state
            .query_planner()
            .create_physical_plan(&optimized, state)
            .await?;
        if let Some(template) = detach_physical_plan_template(Arc::clone(&plan)) {
            cache.remember_physical_read_plan(
                key,
                CachedPhysicalRead {
                    scans: cached_scan_requests(&optimized),
                    template,
                },
            );
        }
        return Ok(plan);
    };

    // Warm path. Neither DataFusion's analyzer, logical optimizer nor physical
    // planner runs again for this statement shape: each cached leaf scan is
    // replanned against the current snapshot's provider and grafted into the
    // template. The logical plan is never consulted, which is why a detached
    // one may arrive here without ever being rebound.
    if let Some(replacements) = plan_cached_spec_scans(&cached.scans, state).await?
        && let Some(plan) =
            rebind_physical_plan_template(Arc::clone(&cached.template), replacements)
    {
        return Ok(plan);
    }

    // Any provider, partitioning, ordering, schema, or operator-shape drift
    // invalidates the detached template before execution. Fall back to the
    // ordinary DataFusion planner for this statement and evict the stale entry.
    cache.forget_physical_read_plan(&key);
    let logical_plan = logical_plan.into_bound(state).await?;
    let optimized = state.optimize(&logical_plan)?;
    state
        .query_planner()
        .create_physical_plan(&optimized, state)
        .await
}

/// Reduces an optimized plan's leaf scans to provider-free scan requests.
///
/// A cache entry must never retain a table provider bound to the storage
/// snapshot that planned it. Keeping only the resolved table name, projection,
/// filters and fetch makes that structural rather than a discipline: there is
/// no provider in the entry to go stale, and no logical plan to clone on the
/// warm path.
fn cached_scan_requests(plan: &LogicalPlan) -> Vec<CachedScanRequest> {
    fn collect(plan: &LogicalPlan, scans: &mut Vec<CachedScanRequest>) {
        if let LogicalPlan::TableScan(scan) = plan {
            scans.push(CachedScanRequest {
                table: scan.table_name.clone(),
                projection: scan.projection.clone(),
                filters: unnormalize_cols(scan.filters.clone()),
                fetch: scan.fetch,
            });
        }
        for input in plan.inputs() {
            collect(input, scans);
        }
    }

    let mut scans = Vec::new();
    collect(plan, &mut scans);
    scans
}

/// Replans each cached leaf scan against the current snapshot's provider.
///
/// Declining (returning `None`) is always safe: the caller evicts the entry and
/// falls back to the ordinary DataFusion planner.
async fn plan_cached_spec_scans(
    scans: &[CachedScanRequest],
    state: &SessionState,
) -> Result<Option<HashMap<PhysicalScanKey, VecDeque<Arc<dyn ExecutionPlan>>>>> {
    let mut replacements: HashMap<PhysicalScanKey, VecDeque<Arc<dyn ExecutionPlan>>> =
        HashMap::new();
    for scan in scans {
        let Ok(schema) = state.schema_for_ref(scan.table.clone()) else {
            return Ok(None);
        };
        let Some(provider) = schema.table(scan.table.table()).await? else {
            return Ok(None);
        };
        let args = ScanArgs::default()
            .with_projection(scan.projection.as_deref())
            .with_filters(Some(&scan.filters))
            .with_limit(scan.fetch);
        let result = provider.scan_with_args(state, args).await?;
        let plan = Arc::clone(result.plan());
        let Some(spec_scan) = plan.downcast_ref::<SpecScanExec>() else {
            return Ok(None);
        };
        replacements
            .entry(spec_scan.physical_cache_key().clone())
            .or_default()
            .push_back(plan);
    }
    Ok(Some(replacements))
}

/// Operators a reusable template may contain.
///
/// Every entry must rebuild into a fresh operator that carries no state from a
/// prior execution. `SortExec` qualifies only without a fetch: a top-k sort
/// owns a shared dynamic filter that its ordinary clone deliberately keeps
/// alive, so it is rejected rather than rebuilt.
fn template_operator_is_reusable(plan: &dyn ExecutionPlan) -> bool {
    match plan.name() {
        "ProjectionExec"
        | "HashJoinExec"
        | "FilterExec"
        | "CooperativeExec"
        | "CoalesceBatchesExec"
        | "CoalescePartitionsExec"
        | "SortPreservingMergeExec" => true,
        "SortExec" => plan
            .downcast_ref::<SortExec>()
            .is_some_and(|sort| sort.fetch().is_none()),
        _ => false,
    }
}

fn detach_physical_plan_template(plan: Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(scan) = plan.downcast_ref::<SpecScanExec>() {
        return Some(Arc::new(DetachedSpecScanExec::new(scan)));
    }
    if !template_operator_is_reusable(plan.as_ref()) {
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
    if let Some(detached) = plan.downcast_ref::<DetachedSpecScanExec>() {
        let replacement = replacements.get_mut(&detached.key)?.pop_front()?;
        return detached
            .fingerprint
            .matches(replacement.as_ref())
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
    if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
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
    if let Some(sort) = plan.downcast_ref::<SortExec>() {
        // `SortExec::with_new_children` clones the operator, which shares its
        // metrics set and top-k dynamic filter with the template. Build a fresh
        // sort instead; `template_operator_is_reusable` already excluded the
        // fetch variants that own a dynamic filter.
        if sort.fetch().is_some() {
            return None;
        }
        let [child] = <[Arc<dyn ExecutionPlan>; 1]>::try_from(children).ok()?;
        return Some(Arc::new(
            SortExec::new(sort.expr().clone(), child)
                .with_preserve_partitioning(sort.preserve_partitioning()),
        ));
    }
    plan.replace_children(
        children,
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
    )
    .ok()
}

/// The structural identity a replacement scan must reproduce before a detached
/// template leaf accepts it.
///
/// This is compared once per leaf on every warm execution, so it is kept as
/// direct field equality. Formatting `Debug` for the schema and the full
/// `PlanProperties` — including `EquivalenceProperties` — allocated several
/// kilobytes of string per scan per query for the same decision.
///
/// `Partitioning` deliberately compares by discriminant and partition count:
/// its `PartialEq` returns `false` for two identical `UnknownPartitioning`
/// values, which is the variant every `SpecScanExec` reports.
struct ScanFingerprint {
    schema: SchemaRef,
    partitioning: std::mem::Discriminant<Partitioning>,
    partition_count: usize,
    output_ordering: Option<LexOrdering>,
    emission_type: EmissionType,
    boundedness: Boundedness,
}

impl ScanFingerprint {
    fn new(plan: &dyn ExecutionPlan) -> Self {
        let properties = plan.properties();
        Self {
            schema: plan.schema(),
            partitioning: std::mem::discriminant(&properties.partitioning),
            partition_count: properties.partitioning.partition_count(),
            output_ordering: properties.output_ordering().cloned(),
            emission_type: properties.emission_type,
            boundedness: properties.boundedness,
        }
    }

    fn matches(&self, plan: &dyn ExecutionPlan) -> bool {
        let properties = plan.properties();
        self.partition_count == properties.partitioning.partition_count()
            && self.partitioning == std::mem::discriminant(&properties.partitioning)
            && self.emission_type == properties.emission_type
            && self.boundedness == properties.boundedness
            && self.output_ordering.as_ref() == properties.output_ordering()
            && self.schema == plan.schema()
    }
}

struct DetachedSpecScanExec {
    key: PhysicalScanKey,
    fingerprint: ScanFingerprint,
    properties: Arc<PlanProperties>,
}

impl DetachedSpecScanExec {
    fn new(scan: &SpecScanExec) -> Self {
        Self {
            key: scan.physical_cache_key().clone(),
            fingerprint: ScanFingerprint::new(scan),
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


    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
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

/// Pulls one partition at a time. Sinks consume and release each source batch
/// before requesting the next; they publish writes only after the source ends.
pub(crate) fn stream_input_plan(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    let plan = adapt_runtime_plan(plan)?;
    let schema = plan.schema();
    let partitions = plan.output_partitioning().partition_count();
    let stream = stream::iter(0..partitions)
        .map(move |partition| plan.execute(partition, Arc::clone(&task_ctx)))
        .try_flatten();
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}

pub(crate) async fn collect_input_plan(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
) -> Result<Vec<RecordBatch>> {
    let plan = adapt_runtime_plan(plan)?;
    collect_adapted_input_plan(plan, task_ctx).await
}

async fn collect_bounded_read_output(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
) -> Result<Vec<RecordBatch>> {
    let mut budget = crate::common::ReadResultBudget::default();
    let mut batches = Vec::new();
    for partition in 0..plan.output_partitioning().partition_count() {
        collect_bounded_read_stream(
            plan.execute(partition, Arc::clone(&task_ctx))?,
            &mut budget,
            &mut batches,
        )
        .await?;
    }
    Ok(batches)
}

async fn collect_bounded_read_stream(
    mut stream: SendableRecordBatchStream,
    budget: &mut crate::common::ReadResultBudget,
    batches: &mut Vec<RecordBatch>,
) -> Result<()> {
    while let Some(batch) = stream.try_next().await? {
        budget
            .charge(batch.get_array_memory_size(), batch.num_rows())
            .map_err(super::error::lix_error_to_datafusion_error)?;
        batches.push(batch);
    }
    Ok(())
}

#[cfg(test)]
mod read_output_tests {
    use super::*;
    #[tokio::test]
    async fn result_collection_stops_polling_before_retaining_oversized_batch() {
        use datafusion::arrow::{
            array::NullArray,
            datatypes::{DataType, Field, Schema},
        };
        use std::sync::atomic::{AtomicUsize, Ordering};
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Null, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(NullArray::new(
                crate::common::MAX_READ_RESULT_ROWS + 1,
            ))],
        )
        .unwrap();
        let polls = Arc::new(AtomicUsize::new(0));
        let observed = polls.clone();
        let stream = RecordBatchStreamAdapter::new(
            schema,
            stream::iter(vec![Ok(batch.clone()), Ok(batch)]).map(move |batch| {
                observed.fetch_add(1, Ordering::SeqCst);
                batch
            }),
        );
        let mut result = Vec::new();
        let error =
            collect_bounded_read_stream(Box::pin(stream), &mut Default::default(), &mut result)
                .await
                .unwrap_err();
        assert_eq!(
            crate::sql2::error::datafusion_error_to_lix_error(error).code,
            "LIX_READ_RESOURCE_EXHAUSTED"
        );
        assert!(result.is_empty());
        assert_eq!(polls.load(Ordering::SeqCst), 1);
    }
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
        plan.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?
    } else {
        plan
    };

    if let Some(key) = plan
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

    if let Some(probe_join) = probe_key_join(&plan)? {
        return Ok(probe_join);
    }

    let Some(coalesce) = plan.downcast_ref::<CoalescePartitionsExec>() else {
        return Ok(plan);
    };
    Ok(Arc::new(SerialCoalescePartitionsExec::new(
        Arc::clone(coalesce.input()),
        coalesce.fetch(),
    )))
}

/// Wraps a hash join whose probe side is a scan that can seek on the join key.
///
/// Only the shapes where discarding a non-matching probe row is already the
/// join's own behaviour qualify:
///
/// - `CollectLeft`, so the build side is the left input and is fully consumed
///   before the probe side is read;
/// - `Inner` or `Left`, whose output contains a right row only when it matched
///   a left row — `Right` and `Full` null-extend unmatched probe rows and are
///   therefore excluded;
/// - `NullEqualsNothing`, so a null probe key cannot match and dropping null
///   rows is not observable;
/// - a single output partition, so the build side is consumed once.
///
/// With more than one equijoin key, restricting on any one of them is still
/// conservative: a row that matches must match on every key, so it survives the
/// restriction on each of them individually.
fn probe_key_join(plan: &Arc<dyn ExecutionPlan>) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let Some(join) = plan.downcast_ref::<HashJoinExec>() else {
        return Ok(None);
    };
    if join.mode != PartitionMode::CollectLeft
        || !matches!(join.join_type(), JoinType::Inner | JoinType::Left)
        || join.null_equality() != NullEquality::NullEqualsNothing
        || plan.output_partitioning().partition_count() != 1
        || join.left().output_partitioning().partition_count() != 1
    {
        return Ok(None);
    }
    let Some(scan) = probe_scan(join.right()) else {
        return Ok(None);
    };
    let left_schema = join.left().schema();
    let right_schema = join.right().schema();
    for (left_key, right_key) in join.on() {
        let Some(column) = right_key.downcast_ref::<PhysicalColumn>() else {
            continue;
        };
        if !scan.serves_probe_column(column.name()) {
            continue;
        }
        // The restriction is handed to the provider as literals of the probe
        // column's own type. A join key that had to be widened or cast is not
        // that type, so leave those to the scan.
        if left_key.data_type(&left_schema)? != right_key.data_type(&right_schema)? {
            continue;
        }
        return Ok(Some(Arc::new(ProbeKeyJoinExec::new(
            Arc::clone(plan),
            Arc::clone(left_key),
            column.name().to_string(),
        ))));
    }
    Ok(None)
}

/// The scan a probe side reads its rows from, if the operators above it in
/// that side neither rename its columns nor add rows to it.
///
/// DataFusion puts a cooperative-yielding wrapper over every leaf and may put
/// batch coalescing or a residual filter between the join and the scan. All
/// three pass their input's schema through unchanged, so a join key names the
/// same scan column through any chain of them, and all three only ever emit a
/// subset of what the scan produced — which is what makes restricting the scan
/// beneath them observationally identical to restricting the probe side.
fn probe_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<&SpecScanExec> {
    if let Some(scan) = plan.downcast_ref::<SpecScanExec>() {
        return Some(scan);
    }
    if !probe_passthrough(plan.as_ref()) {
        return None;
    }
    let children = plan.children();
    let [child] = children.as_slice() else {
        return None;
    };
    probe_scan(child)
}

fn probe_passthrough(plan: &dyn ExecutionPlan) -> bool {
    matches!(
        plan.name(),
        "CooperativeExec" | "CoalesceBatchesExec" | "FilterExec"
    )
}

/// Rebuilds a probe side around a replacement for the scan `probe_scan` found.
fn replace_probe_scan(
    plan: &Arc<dyn ExecutionPlan>,
    replacement: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if plan.downcast_ref::<SpecScanExec>().is_some() {
        return Ok(replacement);
    }
    let children = plan.children();
    let [child] = children.as_slice() else {
        return internal_err!("probe side lost its scan");
    };
    let child = replace_probe_scan(child, replacement)?;
    Arc::clone(plan).replace_children(
        vec![child],
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
    )
}

/// A hash join that reads its build side first and replans its probe scan
/// restricted to the build side's key values.
///
/// The rewrite is an access-path choice, never a semantic one. The restricted
/// scan is the same provider scan with one extra `IN` predicate on a key
/// column, and the join that runs over it is the original join with its
/// children swapped for a replay of the build side and that restricted scan.
/// Every reason to decline — too many distinct keys, a provider that cannot
/// seek the column, a build side whose keys are all null — falls back to the
/// unrestricted scan and produces the same rows more slowly.
#[derive(Debug)]
struct ProbeKeyJoinExec {
    join: Arc<dyn ExecutionPlan>,
    left_key: Arc<dyn PhysicalExpr>,
    probe_column: String,
    properties: Arc<PlanProperties>,
}

impl ProbeKeyJoinExec {
    fn new(
        join: Arc<dyn ExecutionPlan>,
        left_key: Arc<dyn PhysicalExpr>,
        probe_column: String,
    ) -> Self {
        Self {
            properties: Arc::clone(join.properties()),
            join,
            left_key,
            probe_column,
        }
    }
}

impl DisplayAs for ProbeKeyJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ProbeKeyJoinExec: probe_key={}", self.probe_column)
    }
}

impl ExecutionPlan for ProbeKeyJoinExec {
    fn name(&self) -> &'static str {
        "ProbeKeyJoinExec"
    }


    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.join.children()
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        datafusion::physical_plan::apply_expression_roots([&self.left_key], f)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let join = rebuild_hash_join(&self.join, children)?;
        // Re-derive rather than reattach: a replacement probe child that can no
        // longer seek this key must go back to the plain join.
        Ok(probe_key_join(&join)?.unwrap_or(join))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let join = Arc::clone(&self.join);
        let left_key = Arc::clone(&self.left_key);
        let probe_column = self.probe_column.clone();
        let schema = self.schema();
        let restricted = stream::once(async move {
            let children = join.children();
            let (Some(build), Some(probe)) = (children.first(), children.get(1)) else {
                return internal_err!("probe-key join lost a child");
            };
            // The build side is consumed once here to read the keys and replayed
            // from memory by the join. `CollectLeft` already materializes it, so
            // this is the same collection moved one operator earlier.
            let build: Arc<dyn ExecutionPlan> = Arc::new(StatementScanCacheExec::new(Arc::new(
                StatementScanCacheState::new(Arc::clone(build)),
            )));
            let batches =
                collect_adapted_input_plan(Arc::clone(&build), Arc::clone(&context)).await?;
            let restricted_scan = match probe_keys(&batches, &left_key)? {
                Some(values) => match probe_scan(probe) {
                    Some(scan) if scan.serves_probe_column(&probe_column) => {
                        scan.rebind_probe(&probe_column, values).await?
                    }
                    _ => None,
                },
                None => None,
            };
            let probe = match restricted_scan {
                Some(restricted_scan) => replace_probe_scan(probe, restricted_scan)?,
                None => Arc::clone(probe),
            };
            rebuild_hash_join(&join, vec![build, probe])?.execute(partition, context)
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, restricted)))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        StatisticsContext::new().compute(
            self.join.as_ref(),
            &StatisticsArgs::new().with_partition(partition),
        )
    }
}

/// Rebuilds a hash join over new children with no state carried over.
///
/// `HashJoinExec::with_new_children` deliberately preserves the build table and
/// dynamic-filter state of the operator it clones, which a rebuilt join must
/// not inherit.
fn rebuild_hash_join(
    join: &Arc<dyn ExecutionPlan>,
    children: Vec<Arc<dyn ExecutionPlan>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let Some(hash_join) = join.downcast_ref::<HashJoinExec>() else {
        return internal_err!("probe-key join wraps a non-hash join");
    };
    hash_join
        .builder()
        .with_new_children(children)?
        .reset_state()
        .build_exec()
}

/// The distinct non-null build-side key values, or `None` when the join must
/// keep its unrestricted probe scan.
///
/// Null keys are dropped because the caller already established
/// `NullEqualsNothing`, under which a null key matches nothing.
fn probe_keys(
    batches: &[RecordBatch],
    key: &Arc<dyn PhysicalExpr>,
) -> Result<Option<Vec<ScalarValue>>> {
    probe_keys_with_byte_budget(batches, key, PROBE_KEY_BYTE_BUDGET)
}

// Restricting the probe is optional. Account for both retained ScalarValue
// copies, container spare capacity, and the downstream IN literal expression
// nodes before retaining a distinct key. Large build relations keep the normal
// join rather than constructing an unbounded auxiliary filter.
const PROBE_KEY_BYTE_BUDGET: usize = 4 * 1024 * 1024;

fn probe_key_bytes(value: &ScalarValue) -> usize {
    value.size().saturating_mul(4).saturating_add(
        size_of::<datafusion::logical_expr::Expr>()
            .saturating_mul(2)
            .saturating_add(32),
    )
}

fn probe_keys_with_byte_budget(
    batches: &[RecordBatch],
    key: &Arc<dyn PhysicalExpr>,
    byte_budget: usize,
) -> Result<Option<Vec<ScalarValue>>> {
    let mut seen = HashSet::new();
    let mut values = Vec::new();
    let mut retained_bytes = 0usize;
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let array = key.evaluate(batch)?.into_array(batch.num_rows())?;
        for index in 0..array.len() {
            if array.is_null(index) {
                continue;
            }
            // Arrow strings borrow the build batch. Check their payload before
            // ScalarValue materialization would allocate a potentially huge copy.
            let string_bytes = array
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .map(|strings| strings.value(index).len())
                .or_else(|| {
                    array
                        .as_any()
                        .downcast_ref::<datafusion::arrow::array::LargeStringArray>()
                        .map(|strings| strings.value(index).len())
                })
                .or_else(|| {
                    array.as_any()
                        .downcast_ref::<datafusion::arrow::array::StringViewArray>()
                        .map(|strings| strings.value(index).len())
                });
            if string_bytes.is_some_and(|bytes| bytes > byte_budget / 4) {
                return Ok(None);
            }
            let value = ScalarValue::try_from_array(&array, index)?;
            if seen.contains(&value) {
                continue;
            }
            let Some(next_bytes) = retained_bytes.checked_add(probe_key_bytes(&value)) else {
                return Ok(None);
            };
            if next_bytes > byte_budget {
                return Ok(None);
            }
            retained_bytes = next_bytes;
            seen.insert(value.clone());
            values.push(value);
        }
    }
    Ok((!values.is_empty()).then_some(values))
}

#[cfg(test)]
mod probe_key_budget_tests {
    use super::*;
    use datafusion::arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };

    #[test]
    fn byte_budget_preserves_seventy_keys_and_deduplicates_before_charging() {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(
                (0..70).chain(0..70).collect::<Vec<i64>>(),
            ))],
        )
        .unwrap();
        let key: Arc<dyn PhysicalExpr> = Arc::new(PhysicalColumn::new("key", 0));
        let exact_budget = 70 * probe_key_bytes(&ScalarValue::Int64(Some(0)));
        assert_eq!(
            probe_keys_with_byte_budget(std::slice::from_ref(&batch), &key, exact_budget)
                .unwrap()
                .unwrap()
                .len(),
            70
        );
        assert!(
            probe_keys_with_byte_budget(std::slice::from_ref(&batch), &key, exact_budget - 1)
                .unwrap()
                .is_none()
        );
        assert_eq!(probe_keys(&[batch], &key).unwrap().unwrap().len(), 70);
    }

    #[test]
    fn byte_budget_limits_large_string_keys_not_just_key_count() {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)]));
        let text = "x".repeat(PROBE_KEY_BYTE_BUDGET / 2);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![text]))]).unwrap();
        let key: Arc<dyn PhysicalExpr> = Arc::new(PhysicalColumn::new("key", 0));
        assert!(probe_keys(&[batch], &key).unwrap().is_none());
    }
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


    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.state.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
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

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        StatisticsContext::new().compute(
            self.state.input.as_ref(),
            &StatisticsArgs::new().with_partition(partition),
        )
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


    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
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

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        StatisticsContext::new()
            .compute(self.input.as_ref(), &StatisticsArgs::new())?
            .as_ref()
            .clone()
            .with_fetch(self.fetch, 0, 1)
            .map(Arc::new)
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }
}
