use std::any::Any;
use std::collections::BTreeSet;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Int64Array, Int64Builder, StringArray,
    StringBuilder, UInt64Array,
};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::datasource::TableType;
use datafusion::execution::TaskContext;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, PlanProperties};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    displayable,
};
use futures_util::{TryStreamExt, stream};
use lix::storage::{Memory, Storage};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters};

use super::model::{ForkTree, Update};
use super::{
    Backend, CountingStorage, IoStats, Layout, Parameters, ProcIo, begin_allocation_profile,
    directory_bytes, end_allocation_profile, physical_delta, process_cpu_nanos,
    process_resident_bytes, take_stats,
};

use super::olap_common as common;
use common::{Cell, Query};

const NULLABLE_PREFIX: u8 = b'x';
const BATCH_ROWS_ENV: &str = "FORKTREE_OLAP_BATCH_ROWS";

fn batch_rows() -> Option<usize> {
    std::env::var(BATCH_ROWS_ENV).ok().map(|value| {
        let rows = value
            .parse::<usize>()
            .unwrap_or_else(|error| panic!("invalid {BATCH_ROWS_ENV}={value:?}: {error}"));
        assert!(rows > 0, "{BATCH_ROWS_ENV} must be positive");
        rows
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TableKind {
    Narrow,
    Wide,
    Dimension,
    Nullable,
}

impl TableKind {
    const fn prefix(self) -> u8 {
        match self {
            Self::Narrow => b'n',
            Self::Wide => b'w',
            Self::Dimension => b'd',
            Self::Nullable => NULLABLE_PREFIX,
        }
    }

    const fn primary_key(self) -> &'static str {
        match self {
            Self::Dimension => "lane",
            _ => "id",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Datum {
    Null,
    Integer(i64),
    Text(String),
    Boolean(bool),
}

impl Datum {
    fn sql_bool(&self) -> Result<Option<bool>> {
        match self {
            Self::Null => Ok(None),
            Self::Boolean(value) => Ok(Some(*value)),
            other => Err(DataFusionError::Execution(format!(
                "ForkTree pushed filter expected boolean, got {other:?}"
            ))),
        }
    }
}

#[derive(Clone)]
struct ForkTreeTableProvider<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    tree: ForkTree<CountingStorage<S>>,
    branch: Arc<str>,
    kind: TableKind,
    schema: SchemaRef,
    batch_rows: Option<usize>,
}

impl<S> Debug for ForkTreeTableProvider<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ForkTreeTableProvider")
            .field("branch", &self.branch)
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<S> TableProvider for ForkTreeTableProvider<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if filter_supported(filter) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
        Ok(Arc::new(ForkTreeScanExec::new(
            self.tree.clone(),
            Arc::clone(&self.branch),
            self.kind,
            Arc::clone(&self.schema),
            projection,
            filters.to_vec(),
            limit,
            self.batch_rows,
        )?))
    }
}

#[derive(Clone)]
struct ForkTreeScanExec<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    tree: ForkTree<CountingStorage<S>>,
    branch: Arc<str>,
    kind: TableKind,
    source_schema: SchemaRef,
    projection: Arc<[usize]>,
    filters: Arc<[Expr]>,
    limit: Option<usize>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    batch_rows: Option<usize>,
}

impl<S> ForkTreeScanExec<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fn new(
        tree: ForkTree<CountingStorage<S>>,
        branch: Arc<str>,
        kind: TableKind,
        source_schema: SchemaRef,
        projection: Vec<usize>,
        filters: Vec<Expr>,
        limit: Option<usize>,
        batch_rows: Option<usize>,
    ) -> Result<Self> {
        let schema = Arc::new(source_schema.project(&projection)?);
        let equivalence = schema.index_of(kind.primary_key()).ok().map_or_else(
            || EquivalenceProperties::new(Arc::clone(&schema)),
            |index| {
                EquivalenceProperties::new_with_orderings(
                    Arc::clone(&schema),
                    [vec![PhysicalSortExpr {
                        expr: Arc::new(Column::new(kind.primary_key(), index)),
                        options: SortOptions {
                            descending: false,
                            nulls_first: false,
                        },
                    }]],
                )
            },
        );
        let properties = Arc::new(PlanProperties::new(
            equivalence,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            tree,
            branch,
            kind,
            source_schema,
            projection: projection.into(),
            filters: filters.into(),
            limit,
            schema,
            properties,
            batch_rows,
        })
    }

    async fn load_batch(&self) -> Result<RecordBatch> {
        let mut needed = self.projection.iter().copied().collect::<BTreeSet<_>>();
        for filter in self.filters.iter() {
            collect_filter_columns(filter, &self.source_schema, &mut needed)?;
        }
        let (start, end) = range_bounds(self.kind, &self.filters)?;
        let kind = self.kind;
        let source_schema = Arc::clone(&self.source_schema);
        let needed_for_decode = needed.clone();
        let rows = self
            .tree
            .read_projected_range(&self.branch, &start, &end, move |encoded| {
                decode_row(kind, &source_schema, &needed_for_decode, encoded)
                    .map_err(|error| error.to_string())
            })
            .await
            .map_err(DataFusionError::Execution)?;
        let mut selected = Vec::new();
        for (key, mut row) in rows {
            set_primary_key(self.kind, &self.source_schema, &mut row, &key)?;
            let mut keep = Some(true);
            for filter in self.filters.iter() {
                keep = sql_and(
                    keep,
                    evaluate_filter(filter, &self.source_schema, &row)?.sql_bool()?,
                );
            }
            if keep == Some(true) {
                selected.push(row);
                if self.limit.is_some_and(|limit| selected.len() >= limit) {
                    break;
                }
            }
        }
        rows_to_batch(
            &selected,
            &self.source_schema,
            &self.projection,
            &self.schema,
        )
    }

    async fn load_batched(&self, batch_rows: usize) -> Result<Vec<RecordBatch>> {
        let mut needed = self.projection.iter().copied().collect::<BTreeSet<_>>();
        for filter in self.filters.iter() {
            collect_filter_columns(filter, &self.source_schema, &mut needed)?;
        }
        let (start, end) = range_bounds(self.kind, &self.filters)?;
        let raw_rows = self
            .tree
            .read_range(&self.branch, &start, &end)
            .await
            .map_err(DataFusionError::Execution)?;
        let mut batches = Vec::new();
        let mut remaining_limit = self.limit;
        for chunk in raw_rows.chunks(batch_rows) {
            let mut builders = BatchBuilders::new(&self.source_schema, &self.projection);
            for (key, encoded) in chunk {
                let mut row = decode_row(self.kind, &self.source_schema, &needed, encoded)?;
                set_primary_key(self.kind, &self.source_schema, &mut row, key)?;
                let mut keep = Some(true);
                for filter in self.filters.iter() {
                    keep = sql_and(
                        keep,
                        evaluate_filter(filter, &self.source_schema, &row)?.sql_bool()?,
                    );
                }
                if keep != Some(true) {
                    continue;
                }
                builders.append(&row, &self.projection)?;
                if let Some(limit) = remaining_limit.as_mut() {
                    *limit = limit.saturating_sub(1);
                    if *limit == 0 {
                        break;
                    }
                }
            }
            if builders.len() != 0 {
                batches.push(builders.finish(&self.schema)?);
            }
            if remaining_limit == Some(0) {
                break;
            }
        }
        if batches.is_empty() {
            batches.push(
                BatchBuilders::new(&self.source_schema, &self.projection).finish(&self.schema)?,
            );
        }
        Ok(batches)
    }
}

impl<S> Debug for ForkTreeScanExec<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ForkTreeScanExec")
            .field("kind", &self.kind)
            .field("projection", &self.projection)
            .field("filters", &self.filters)
            .field("limit", &self.limit)
            .field("batch_rows", &self.batch_rows)
            .finish_non_exhaustive()
    }
}

impl<S> DisplayAs for ForkTreeScanExec<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fn fmt_as(
        &self,
        _display: DisplayFormatType,
        formatter: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        write!(
            formatter,
            "ForkTreeScanExec kind={:?}, projection={:?}, filters={}, limit={:?}",
            self.kind,
            self.projection,
            self.filters.len(),
            self.limit
        )
    }
}

impl<S> ExecutionPlan for ForkTreeScanExec<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        "ForkTreeScanExec"
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
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Execution(
                "ForkTreeScanExec does not accept children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "ForkTreeScanExec exposes one partition, got {partition}"
            )));
        }
        let scan = self.clone();
        let schema = Arc::clone(&self.schema);
        let batches = stream::once(async move {
            let batches = match scan.batch_rows {
                Some(batch_rows) => scan.load_batched(batch_rows).await?,
                None => vec![scan.load_batch().await?],
            };
            Ok::<_, DataFusionError>(batches)
        })
        .map_ok(|batches| stream::iter(batches.into_iter().map(Ok::<_, DataFusionError>)))
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, batches)))
    }
}

pub(super) async fn run(parameters: Parameters) {
    assert_eq!(parameters.layout, Layout::ForkTree);
    println!(
        "forktree_datafusion_boundary,sql_wiring=true,provider=ForkTreeTableProvider,authority=one_storage_read_authenticated_iterator,request_big_o=O(height),projection_allocation=O(batch*selected_width),batch_rows={:?}",
        batch_rows()
    );
    if parameters.rows == 1_000 {
        run_memory_provider_oracle().await;
    }
    match parameters.backend {
        Backend::RocksDb => run_rocks(parameters).await,
        Backend::SlateDb => run_slate(parameters).await,
    }
}

async fn run_memory_provider_oracle() {
    let (storage, _) = CountingStorage::new(Memory::default());
    let (tree, context, _) = prepare(storage, 1_000).await;
    run_semantic_oracle(&context).await;

    tree.create_branch("snapshot", None)
        .await
        .expect("create ForkTree provider snapshot branch");
    let mut changed = common::narrow_row(123);
    changed.score += 1_000_000;
    tree.apply_sorted_updates(&[Update {
        key: common::key(b'n', &changed.id),
        value: common::encode_narrow(&changed),
    }])
    .await
    .expect("update ForkTree provider main branch");
    let sql = "SELECT score FROM forktree_olap_narrow WHERE id = '/~forktree-olap/000000123'";
    let snapshot = execute_sql(
        &register_context(tree.clone(), "snapshot").await,
        sql,
        false,
    )
    .await;
    let main = execute_sql(&register_context(tree.clone(), "main").await, sql, false).await;
    assert_ne!(
        snapshot, main,
        "branch snapshot must retain its visible root"
    );

    tree.verify_projected_range_corruption_fail_closed()
        .await
        .expect("ForkTree provider corruption fixture must fail closed");
    for fault in ["malformed", "truncated", "substituted"] {
        let context = register_context(tree.clone(), &format!("range-fault-{fault}")).await;
        assert!(
            context
                .sql("SELECT id FROM forktree_olap_narrow")
                .await
                .expect("plan corrupt ForkTree provider branch")
                .collect()
                .await
                .is_err(),
            "provider accepted {fault} authenticated block"
        );
    }
    println!(
        "forktree_datafusion_memory_gate,rows=1000,exact_batches=true,branch_snapshot_visible=true,malformed_fail_closed=true,truncated_fail_closed=true,substituted_fail_closed=true"
    );
}

async fn run_rocks(parameters: Parameters) {
    let directory = tempfile::tempdir().expect("create ForkTree DataFusion RocksDB directory");
    let database = RocksDB::open(directory.path()).expect("open ForkTree DataFusion RocksDB");
    let (storage, stats) = CountingStorage::new(database.clone());
    let (tree, context, expected) = prepare(storage, parameters.rows).await;
    database
        .flush()
        .expect("flush ForkTree DataFusion RocksDB setup");
    run_queries(
        &context,
        &expected,
        parameters,
        &stats,
        directory.path(),
        None,
    )
    .await;
    database
        .flush()
        .expect("flush ForkTree DataFusion RocksDB result");
    let disk_bytes = directory_bytes(directory.path());
    drop(context);
    drop(tree);
    drop(database);
    let reopened = RocksDB::open(directory.path()).expect("reopen ForkTree DataFusion RocksDB");
    let (reopened, _) = CountingStorage::new(reopened);
    let tree = ForkTree::new(reopened);
    let context = register_context(tree, "main").await;
    verify(&context, &expected).await;
    println!(
        "forktree_datafusion_reopen,backend=rocksdb,rows={},exact_results=true,disk_bytes={disk_bytes}",
        parameters.rows
    );
}

async fn run_slate(parameters: Parameters) {
    let directory = tempfile::tempdir().expect("create ForkTree DataFusion SlateDB directory");
    let counters = SlateDBIoCounters::default();
    let database = SlateDB::open_with_io_counters(directory.path(), counters.clone())
        .expect("open ForkTree DataFusion SlateDB");
    let (storage, stats) = CountingStorage::new(database.clone());
    let (tree, context, expected) = prepare(storage, parameters.rows).await;
    database
        .flush_memtable_for_diagnostics()
        .await
        .expect("flush ForkTree DataFusion SlateDB setup");
    run_queries(
        &context,
        &expected,
        parameters,
        &stats,
        directory.path(),
        Some(&counters),
    )
    .await;
    database
        .flush_memtable_for_diagnostics()
        .await
        .expect("flush ForkTree DataFusion SlateDB result");
    let disk_bytes = directory_bytes(directory.path());
    drop(context);
    drop(tree);
    drop(database);
    let reopened = SlateDB::open(directory.path()).expect("reopen ForkTree DataFusion SlateDB");
    let (reopened, _) = CountingStorage::new(reopened);
    let tree = ForkTree::new(reopened);
    let context = register_context(tree, "main").await;
    verify(&context, &expected).await;
    println!(
        "forktree_datafusion_reopen,backend=slatedb,rows={},exact_results=true,disk_bytes={disk_bytes}",
        parameters.rows
    );
}

struct Expected {
    queries: Vec<(Query, [u8; 32], usize)>,
}

async fn prepare<S>(
    storage: CountingStorage<S>,
    rows: usize,
) -> (ForkTree<CountingStorage<S>>, SessionContext, Expected)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let narrow = (0..rows).map(common::narrow_row).collect::<Vec<_>>();
    let wide = (0..rows).map(common::wide_row).collect::<Vec<_>>();
    let dimensions = common::dimension_rows();
    let mut encoded = Vec::with_capacity(rows * 2 + dimensions.len() + 4);
    for row in &narrow {
        encoded.push((common::key(b'n', &row.id), common::encode_narrow(row)));
    }
    for row in &wide {
        encoded.push((common::key(b'w', &row.base.id), common::encode_wide(row)));
    }
    for (lane, label) in &dimensions {
        encoded.push((
            common::key(b'd', &format!("{lane:02}")),
            label.as_bytes().to_vec(),
        ));
    }
    encoded.extend(nullable_rows());
    encoded.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    let expected = Expected {
        queries: Query::ALL
            .into_iter()
            .map(|query| {
                let result = common::evaluate(query, &narrow, &wide, &dimensions);
                (query, common::digest(&result), result.len())
            })
            .collect(),
    };
    let tree = ForkTree::new(storage);
    tree.initialize(&encoded)
        .await
        .expect("initialize ForkTree DataFusion rows");
    let context = register_context(tree.clone(), "main").await;
    (tree, context, expected)
}

async fn register_context<S>(tree: ForkTree<CountingStorage<S>>, branch: &str) -> SessionContext
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let context = SessionContext::new();
    for (name, kind, schema) in [
        ("forktree_olap_narrow", TableKind::Narrow, narrow_schema()),
        ("forktree_olap_wide", TableKind::Wide, wide_schema()),
        (
            "forktree_olap_dim",
            TableKind::Dimension,
            dimension_schema(),
        ),
        (
            "forktree_olap_nullable",
            TableKind::Nullable,
            nullable_schema(),
        ),
    ] {
        context
            .register_table(
                name,
                Arc::new(ForkTreeTableProvider {
                    tree: tree.clone(),
                    branch: Arc::from(branch),
                    kind,
                    schema,
                    batch_rows: batch_rows(),
                }),
            )
            .expect("register ForkTree DataFusion provider");
    }
    context
}

async fn run_queries(
    context: &SessionContext,
    expected: &Expected,
    parameters: Parameters,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
) {
    if parameters.rows == 1_000 {
        run_semantic_oracle(context).await;
    }
    for &(query, digest, rows) in &expected.queries {
        if parameters.rows == 10_000 {
            let _ = execute_sql(context, query.sql(), true).await;
        }
        for sample in 0..parameters.warmups + parameters.samples {
            let _ = take_stats(stats);
            let physical_before = counters.map(SlateDBIoCounters::snapshot);
            let proc_before = ProcIo::read();
            let rss_before = process_resident_bytes();
            let cpu_before = process_cpu_nanos();
            begin_allocation_profile();
            let started = Instant::now();
            let result = execute_sql(
                context,
                query.sql(),
                sample == 0 && parameters.rows == 1_000,
            )
            .await;
            let wall_us = started.elapsed().as_secs_f64() * 1_000_000.0;
            let cpu_us = process_cpu_nanos().saturating_sub(cpu_before) as f64 / 1_000.0;
            let (alloc_bytes, alloc_calls) = end_allocation_profile();
            let rss_after = process_resident_bytes();
            assert_eq!(result.len(), rows);
            assert_eq!(common::digest(&result), digest);
            let logical = take_stats(stats);
            let physical = physical_delta(counters, physical_before);
            let proc_io = ProcIo::read().saturating_sub(proc_before);
            assert_eq!(logical.begin_writes, 0, "SELECT opened a write transaction");
            assert_eq!(logical.write_batches, 0, "SELECT staged writes");
            assert_eq!(logical.commits, 0, "SELECT committed writes");
            assert_eq!(physical.write_objects, 0, "SELECT wrote physical objects");
            assert_eq!(physical.write_bytes, 0, "SELECT wrote physical bytes");
            if sample >= parameters.warmups {
                println!(
                    "forktree_datafusion_olap,sample={},backend={},rows={},query={},wall_us={wall_us:.3},cpu_us={cpu_us:.3},alloc_bytes={alloc_bytes},alloc_calls={alloc_calls},rss_before_bytes={rss_before},rss_after_bytes={rss_after},begin_reads={},begin_writes={},get_calls={},get_keys={},get_values={},get_value_bytes={},scan_calls={},scan_entries={},scan_value_bytes={},write_batches={},write_puts={},write_deletes={},write_ranges={},logical_write_bytes={},commits={},physical_read_objects={},physical_read_bytes={},physical_write_objects={},physical_write_bytes={},os_read_calls={},os_read_chars={},os_read_bytes={},os_write_calls={},os_write_bytes={},logical_result_rows={},result_digest={},disk_bytes={}",
                    sample - parameters.warmups + 1,
                    parameters.backend.label(),
                    parameters.rows,
                    query.label(),
                    logical.begin_reads,
                    logical.begin_writes,
                    logical.get_calls,
                    logical.get_keys,
                    logical.get_values,
                    logical.get_value_bytes,
                    logical.scan_calls,
                    logical.scan_entries,
                    logical.scan_value_bytes,
                    logical.write_batches,
                    logical.write_puts,
                    logical.write_deletes,
                    logical.write_ranges,
                    logical.write_bytes,
                    logical.commits,
                    physical.read_objects,
                    physical.read_bytes,
                    physical.write_objects,
                    physical.write_bytes,
                    proc_io.syscr,
                    proc_io.rchar,
                    proc_io.read_bytes,
                    proc_io.syscw,
                    proc_io.write_bytes,
                    result.len(),
                    hex_digest(digest),
                    directory_bytes(path),
                );
            }
            std::hint::black_box(result);
        }
    }
}

async fn verify(context: &SessionContext, expected: &Expected) {
    for &(query, digest, rows) in &expected.queries {
        let result = execute_sql(context, query.sql(), false).await;
        assert_eq!(result.len(), rows);
        assert_eq!(common::digest(&result), digest);
    }
}

async fn execute_sql(context: &SessionContext, sql: &str, print_plan: bool) -> Vec<Vec<Cell>> {
    let frame = context
        .sql(sql)
        .await
        .expect("plan ForkTree DataFusion SQL");
    if print_plan {
        let logical = frame.logical_plan().display_indent().to_string();
        let physical = frame
            .clone()
            .create_physical_plan()
            .await
            .expect("create ForkTree DataFusion physical plan");
        println!(
            "forktree_datafusion_plan,sql={},logical={:?},physical={:?}",
            sql,
            logical,
            displayable(physical.as_ref()).indent(true).to_string()
        );
    }
    batches_to_cells(
        frame
            .collect()
            .await
            .expect("execute ForkTree DataFusion SQL"),
    )
}

async fn run_semantic_oracle(context: &SessionContext) {
    let cases = [
        (
            "pk_point",
            "SELECT id, score FROM forktree_olap_narrow WHERE id = '/~forktree-olap/000000123'",
        ),
        (
            "pk_range",
            "SELECT id, ordinal FROM forktree_olap_narrow WHERE id >= '/~forktree-olap/000000120' AND id < '/~forktree-olap/000000130' ORDER BY id",
        ),
        (
            "pushdown",
            "SELECT id, score FROM forktree_olap_narrow WHERE active = TRUE AND lane = 7 ORDER BY ordinal LIMIT 17",
        ),
        (
            "null_projection",
            "SELECT id, note, score, note IS NULL FROM forktree_olap_nullable ORDER BY id",
        ),
        (
            "null_filter",
            "SELECT id FROM forktree_olap_nullable WHERE note IS NULL ORDER BY id",
        ),
        (
            "null_aggregate",
            "SELECT COUNT(*) AS rows, COUNT(note) AS notes, SUM(score) AS score_sum FROM forktree_olap_nullable",
        ),
        (
            "ordering",
            "SELECT id FROM forktree_olap_narrow ORDER BY id LIMIT 32",
        ),
        (
            "limit_pushdown",
            "SELECT id FROM forktree_olap_narrow LIMIT 7",
        ),
    ];
    for (label, sql) in cases {
        let rows = execute_sql(context, sql, true).await;
        println!(
            "forktree_datafusion_semantic,label={label},rows={},digest={}",
            rows.len(),
            hex_digest(common::digest(&rows))
        );
    }
}

fn batches_to_cells(batches: Vec<RecordBatch>) -> Vec<Vec<Cell>> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut values = Vec::with_capacity(batch.num_columns());
            for column in batch.columns() {
                if column.is_null(row) {
                    values.push(Cell::Null);
                } else if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
                    values.push(Cell::Text(array.value(row).to_string()));
                } else if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
                    values.push(Cell::Integer(array.value(row)));
                } else if let Some(array) = column.as_any().downcast_ref::<UInt64Array>() {
                    values.push(Cell::Integer(
                        i64::try_from(array.value(row)).expect("OLAP UInt64 fits i64"),
                    ));
                } else if let Some(array) = column.as_any().downcast_ref::<BooleanArray>() {
                    values.push(Cell::Boolean(array.value(row)));
                } else {
                    panic!(
                        "unsupported ForkTree OLAP Arrow array {:?}",
                        column.data_type()
                    );
                }
            }
            rows.push(values);
        }
    }
    rows
}

fn narrow_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("ordinal", DataType::Int64, false),
        Field::new("lane", DataType::Int64, false),
        Field::new("score", DataType::Int64, false),
        Field::new("active", DataType::Boolean, false),
    ]))
}

fn wide_schema() -> SchemaRef {
    let mut fields = narrow_schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.extend(
        (0..common::WIDE_COLUMNS)
            .map(|column| Field::new(format!("c{column:02}"), DataType::Int64, false)),
    );
    fields.push(Field::new("payload", DataType::Utf8, false));
    Arc::new(Schema::new(fields))
}

fn dimension_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("lane", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]))
}

fn nullable_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("note", DataType::Utf8, true),
        Field::new("score", DataType::Int64, true),
    ]))
}

fn nullable_rows() -> Vec<(Vec<u8>, Vec<u8>)> {
    [
        ("nullable-00", Some("alpha"), Some(10_i64)),
        ("nullable-01", None, Some(20)),
        ("nullable-02", Some("gamma"), None),
        ("nullable-03", None, None),
    ]
    .into_iter()
    .map(|(id, note, score)| {
        let mut value = Vec::new();
        match note {
            Some(note) => {
                value.push(1);
                value.extend_from_slice(&(note.len() as u32).to_be_bytes());
                value.extend_from_slice(note.as_bytes());
            }
            None => value.push(0),
        }
        match score {
            Some(score) => {
                value.push(1);
                value.extend_from_slice(&score.to_be_bytes());
            }
            None => value.push(0),
        }
        (common::key(NULLABLE_PREFIX, id), value)
    })
    .collect()
}

fn decode_row(
    kind: TableKind,
    schema: &SchemaRef,
    needed: &BTreeSet<usize>,
    encoded: &[u8],
) -> Result<Vec<Datum>> {
    let mut row = vec![Datum::Null; schema.fields().len()];
    match kind {
        TableKind::Narrow | TableKind::Wide => {
            let expected = if kind == TableKind::Narrow {
                25
            } else {
                25 + common::WIDE_COLUMNS * 8 + common::WIDE_PAYLOAD_BYTES
            };
            if encoded.len() != expected {
                return Err(DataFusionError::Execution(format!(
                    "noncanonical {:?} row width {} != {expected}",
                    kind,
                    encoded.len()
                )));
            }
            let active = match encoded[24] {
                0 => false,
                1 => true,
                tag => {
                    return Err(DataFusionError::Execution(format!(
                        "noncanonical boolean {tag}"
                    )));
                }
            };
            if kind == TableKind::Wide {
                std::str::from_utf8(&encoded[25 + common::WIDE_COLUMNS * 8..]).map_err(
                    |error| {
                        DataFusionError::Execution(format!("wide payload is not UTF-8: {error}"))
                    },
                )?;
            }
            for &index in needed {
                row[index] = match index {
                    0 => Datum::Null,
                    1 => Datum::Integer(i64_at(encoded, 0)?),
                    2 => Datum::Integer(i64_at(encoded, 8)?),
                    3 => Datum::Integer(i64_at(encoded, 16)?),
                    4 => Datum::Boolean(active),
                    5..=20 if kind == TableKind::Wide => {
                        Datum::Integer(i64_at(encoded, 25 + (index - 5) * 8)?)
                    }
                    21 if kind == TableKind::Wide => Datum::Text(
                        std::str::from_utf8(&encoded[25 + common::WIDE_COLUMNS * 8..])
                            .expect("validated wide UTF-8")
                            .to_string(),
                    ),
                    _ => {
                        return Err(DataFusionError::Execution(format!(
                            "invalid {:?} projected column {index}",
                            kind
                        )));
                    }
                };
            }
        }
        TableKind::Dimension => {
            let label = std::str::from_utf8(encoded).map_err(|error| {
                DataFusionError::Execution(format!("dimension label is not UTF-8: {error}"))
            })?;
            if needed.contains(&1) {
                row[1] = Datum::Text(label.to_string());
            }
        }
        TableKind::Nullable => {
            let mut offset = 0;
            let note = match take_byte(encoded, &mut offset)? {
                0 => None,
                1 => {
                    let length = take_u32(encoded, &mut offset)? as usize;
                    let bytes = take(encoded, &mut offset, length)?;
                    Some(std::str::from_utf8(bytes).map_err(|error| {
                        DataFusionError::Execution(format!("nullable note is not UTF-8: {error}"))
                    })?)
                }
                tag => {
                    return Err(DataFusionError::Execution(format!(
                        "invalid nullable note tag {tag}"
                    )));
                }
            };
            let score = match take_byte(encoded, &mut offset)? {
                0 => None,
                1 => Some(i64_at(encoded, offset)?),
                tag => {
                    return Err(DataFusionError::Execution(format!(
                        "invalid nullable score tag {tag}"
                    )));
                }
            };
            if score.is_some() {
                offset += 8;
            }
            if offset != encoded.len() {
                return Err(DataFusionError::Execution(
                    "trailing nullable row bytes".to_string(),
                ));
            }
            if needed.contains(&1) {
                row[1] = note.map_or(Datum::Null, |note| Datum::Text(note.to_string()));
            }
            if needed.contains(&2) {
                row[2] = score.map_or(Datum::Null, Datum::Integer);
            }
        }
    }
    Ok(row)
}

fn set_primary_key(
    kind: TableKind,
    schema: &SchemaRef,
    row: &mut [Datum],
    key: &[u8],
) -> Result<()> {
    let raw = common::strip_key(kind.prefix(), key);
    let index = schema.index_of(kind.primary_key())?;
    row[index] = match kind {
        TableKind::Dimension => Datum::Integer(raw.parse().map_err(|error| {
            DataFusionError::Execution(format!("dimension key is not integer: {error}"))
        })?),
        _ => Datum::Text(raw),
    };
    Ok(())
}

enum BatchBuilder {
    Utf8(StringBuilder),
    Int64(Int64Builder),
    Boolean(BooleanBuilder),
}

struct BatchBuilders {
    columns: Vec<BatchBuilder>,
    rows: usize,
}

impl BatchBuilders {
    fn new(source_schema: &SchemaRef, projection: &[usize]) -> Self {
        let columns = projection
            .iter()
            .map(|&index| match source_schema.field(index).data_type() {
                DataType::Utf8 => BatchBuilder::Utf8(StringBuilder::new()),
                DataType::Int64 => BatchBuilder::Int64(Int64Builder::new()),
                DataType::Boolean => BatchBuilder::Boolean(BooleanBuilder::new()),
                data_type => panic!("unsupported ForkTree batching type {data_type:?}"),
            })
            .collect();
        Self { columns, rows: 0 }
    }

    fn append(&mut self, row: &[Datum], projection: &[usize]) -> Result<()> {
        for (builder, &index) in self.columns.iter_mut().zip(projection) {
            let value = &row[index];
            match (builder, value) {
                (BatchBuilder::Utf8(builder), Datum::Null) => builder.append_null(),
                (BatchBuilder::Utf8(builder), Datum::Text(value)) => builder.append_value(value),
                (BatchBuilder::Int64(builder), Datum::Null) => builder.append_null(),
                (BatchBuilder::Int64(builder), Datum::Integer(value)) => {
                    builder.append_value(*value)
                }
                (BatchBuilder::Boolean(builder), Datum::Null) => builder.append_null(),
                (BatchBuilder::Boolean(builder), Datum::Boolean(value)) => {
                    builder.append_value(*value)
                }
                (builder, value) => {
                    return Err(DataFusionError::Execution(format!(
                        "ForkTree batching datum/type mismatch: {builder:?} and {value:?}"
                    )));
                }
            }
        }
        self.rows += 1;
        Ok(())
    }

    fn len(&self) -> usize {
        self.rows
    }

    fn finish(self, schema: &SchemaRef) -> Result<RecordBatch> {
        let arrays = self
            .columns
            .into_iter()
            .map(|builder| {
                let array: ArrayRef = match builder {
                    BatchBuilder::Utf8(mut builder) => Arc::new(builder.finish()),
                    BatchBuilder::Int64(mut builder) => Arc::new(builder.finish()),
                    BatchBuilder::Boolean(mut builder) => Arc::new(builder.finish()),
                };
                array
            })
            .collect();
        RecordBatch::try_new(Arc::clone(schema), arrays)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
    }
}

impl Debug for BatchBuilder {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("BatchBuilder")
    }
}

fn rows_to_batch(
    rows: &[Vec<Datum>],
    source_schema: &SchemaRef,
    projection: &[usize],
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let arrays = projection
        .iter()
        .map(|&index| -> Result<ArrayRef> {
            let values = rows.iter().map(|row| &row[index]);
            let array: ArrayRef = match source_schema.field(index).data_type() {
                DataType::Utf8 => Arc::new(StringArray::from(
                    values
                        .map(|value| match value {
                            Datum::Null => None,
                            Datum::Text(value) => Some(value.as_str()),
                            _ => unreachable!("validated UTF-8 datum"),
                        })
                        .collect::<Vec<_>>(),
                )),
                DataType::Int64 => Arc::new(Int64Array::from(
                    values
                        .map(|value| match value {
                            Datum::Null => None,
                            Datum::Integer(value) => Some(*value),
                            _ => unreachable!("validated Int64 datum"),
                        })
                        .collect::<Vec<_>>(),
                )),
                DataType::Boolean => Arc::new(BooleanArray::from(
                    values
                        .map(|value| match value {
                            Datum::Null => None,
                            Datum::Boolean(value) => Some(*value),
                            _ => unreachable!("validated Boolean datum"),
                        })
                        .collect::<Vec<_>>(),
                )),
                data_type => {
                    return Err(DataFusionError::Execution(format!(
                        "unsupported ForkTree provider type {data_type:?}"
                    )));
                }
            };
            Ok(array)
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(Arc::clone(schema), arrays)
        .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

fn filter_supported(expression: &Expr) -> bool {
    match expression {
        Expr::Column(_) | Expr::Literal(_, _) => true,
        Expr::BinaryExpr(binary) => {
            matches!(
                binary.op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
                    | Operator::And
                    | Operator::Or
                    | Operator::IsDistinctFrom
                    | Operator::IsNotDistinctFrom
            ) && filter_supported(&binary.left)
                && filter_supported(&binary.right)
        }
        Expr::Not(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsUnknown(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNotFalse(inner)
        | Expr::IsNotUnknown(inner) => filter_supported(inner),
        Expr::Between(between) => {
            filter_supported(&between.expr)
                && filter_supported(&between.low)
                && filter_supported(&between.high)
        }
        Expr::InList(list) => {
            filter_supported(&list.expr) && list.list.iter().all(filter_supported)
        }
        Expr::Alias(alias) => filter_supported(&alias.expr),
        _ => false,
    }
}

fn collect_filter_columns(
    expression: &Expr,
    schema: &SchemaRef,
    output: &mut BTreeSet<usize>,
) -> Result<()> {
    match expression {
        Expr::Column(column) => {
            output.insert(schema.index_of(&column.name)?);
        }
        Expr::BinaryExpr(binary) => {
            collect_filter_columns(&binary.left, schema, output)?;
            collect_filter_columns(&binary.right, schema, output)?;
        }
        Expr::Not(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsUnknown(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNotFalse(inner)
        | Expr::IsNotUnknown(inner) => collect_filter_columns(inner, schema, output)?,
        Expr::Between(between) => {
            collect_filter_columns(&between.expr, schema, output)?;
            collect_filter_columns(&between.low, schema, output)?;
            collect_filter_columns(&between.high, schema, output)?;
        }
        Expr::InList(list) => {
            collect_filter_columns(&list.expr, schema, output)?;
            for expression in &list.list {
                collect_filter_columns(expression, schema, output)?;
            }
        }
        Expr::Alias(alias) => collect_filter_columns(&alias.expr, schema, output)?,
        Expr::Literal(_, _) => {}
        other => {
            return Err(DataFusionError::Execution(format!(
                "unsupported pushed filter expression {other:?}"
            )));
        }
    }
    Ok(())
}

fn evaluate_filter(expression: &Expr, schema: &SchemaRef, row: &[Datum]) -> Result<Datum> {
    match expression {
        Expr::Column(column) => Ok(row[schema.index_of(&column.name)?].clone()),
        Expr::Literal(value, _) => scalar_datum(value),
        Expr::Alias(alias) => evaluate_filter(&alias.expr, schema, row),
        Expr::Not(inner) => Ok(match evaluate_filter(inner, schema, row)?.sql_bool()? {
            Some(value) => Datum::Boolean(!value),
            None => Datum::Null,
        }),
        Expr::IsNull(inner) => Ok(Datum::Boolean(matches!(
            evaluate_filter(inner, schema, row)?,
            Datum::Null
        ))),
        Expr::IsNotNull(inner) => Ok(Datum::Boolean(!matches!(
            evaluate_filter(inner, schema, row)?,
            Datum::Null
        ))),
        Expr::BinaryExpr(binary) => {
            let left = evaluate_filter(&binary.left, schema, row)?;
            let right = evaluate_filter(&binary.right, schema, row)?;
            evaluate_binary(&left, binary.op, &right)
        }
        Expr::Between(between) => {
            let value = evaluate_filter(&between.expr, schema, row)?;
            let low = evaluate_filter(&between.low, schema, row)?;
            let high = evaluate_filter(&between.high, schema, row)?;
            let result = sql_and(
                evaluate_binary(&value, Operator::GtEq, &low)?.sql_bool()?,
                evaluate_binary(&value, Operator::LtEq, &high)?.sql_bool()?,
            );
            Ok(result.map_or(Datum::Null, |result| {
                Datum::Boolean(if between.negated { !result } else { result })
            }))
        }
        Expr::InList(list) => {
            let value = evaluate_filter(&list.expr, schema, row)?;
            if value == Datum::Null {
                return Ok(Datum::Null);
            }
            let mut saw_null = false;
            for expression in &list.list {
                let candidate = evaluate_filter(expression, schema, row)?;
                if candidate == Datum::Null {
                    saw_null = true;
                } else if candidate == value {
                    return Ok(Datum::Boolean(!list.negated));
                }
            }
            if saw_null {
                Ok(Datum::Null)
            } else {
                Ok(Datum::Boolean(list.negated))
            }
        }
        Expr::IsTrue(inner) => Ok(Datum::Boolean(
            evaluate_filter(inner, schema, row)?.sql_bool()? == Some(true),
        )),
        Expr::IsFalse(inner) => Ok(Datum::Boolean(
            evaluate_filter(inner, schema, row)?.sql_bool()? == Some(false),
        )),
        Expr::IsUnknown(inner) => Ok(Datum::Boolean(
            evaluate_filter(inner, schema, row)?.sql_bool()?.is_none(),
        )),
        Expr::IsNotTrue(inner) => Ok(Datum::Boolean(
            evaluate_filter(inner, schema, row)?.sql_bool()? != Some(true),
        )),
        Expr::IsNotFalse(inner) => Ok(Datum::Boolean(
            evaluate_filter(inner, schema, row)?.sql_bool()? != Some(false),
        )),
        Expr::IsNotUnknown(inner) => Ok(Datum::Boolean(
            evaluate_filter(inner, schema, row)?.sql_bool()?.is_some(),
        )),
        other => Err(DataFusionError::Execution(format!(
            "unsupported pushed filter expression {other:?}"
        ))),
    }
}

fn evaluate_binary(left: &Datum, operator: Operator, right: &Datum) -> Result<Datum> {
    if operator == Operator::And {
        return Ok(sql_and(left.sql_bool()?, right.sql_bool()?).map_or(Datum::Null, Datum::Boolean));
    }
    if operator == Operator::Or {
        return Ok(sql_or(left.sql_bool()?, right.sql_bool()?).map_or(Datum::Null, Datum::Boolean));
    }
    if operator == Operator::IsDistinctFrom {
        return Ok(Datum::Boolean(match (left, right) {
            (Datum::Null, Datum::Null) => false,
            (Datum::Null, _) | (_, Datum::Null) => true,
            _ => left != right,
        }));
    }
    if operator == Operator::IsNotDistinctFrom {
        return evaluate_binary(left, Operator::IsDistinctFrom, right).map(|value| match value {
            Datum::Boolean(value) => Datum::Boolean(!value),
            _ => unreachable!(),
        });
    }
    if matches!(left, Datum::Null) || matches!(right, Datum::Null) {
        return Ok(Datum::Null);
    }
    let ordering = match (left, right) {
        (Datum::Integer(left), Datum::Integer(right)) => left.cmp(right),
        (Datum::Text(left), Datum::Text(right)) => left.cmp(right),
        (Datum::Boolean(left), Datum::Boolean(right)) => left.cmp(right),
        _ => {
            return Err(DataFusionError::Execution(format!(
                "incompatible pushed comparison {left:?} {operator:?} {right:?}"
            )));
        }
    };
    Ok(Datum::Boolean(match operator {
        Operator::Eq => ordering.is_eq(),
        Operator::NotEq => !ordering.is_eq(),
        Operator::Lt => ordering.is_lt(),
        Operator::LtEq => !ordering.is_gt(),
        Operator::Gt => ordering.is_gt(),
        Operator::GtEq => !ordering.is_lt(),
        _ => {
            return Err(DataFusionError::Execution(format!(
                "unsupported pushed comparison {operator:?}"
            )));
        }
    }))
}

fn scalar_datum(value: &ScalarValue) -> Result<Datum> {
    match value {
        ScalarValue::Utf8(Some(value)) | ScalarValue::Utf8View(Some(value)) => {
            Ok(Datum::Text(value.clone()))
        }
        ScalarValue::Int64(Some(value)) => Ok(Datum::Integer(*value)),
        ScalarValue::Boolean(Some(value)) => Ok(Datum::Boolean(*value)),
        value if value.is_null() => Ok(Datum::Null),
        other => Err(DataFusionError::Execution(format!(
            "unsupported pushed literal {other:?}"
        ))),
    }
}

fn sql_and(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

fn sql_or(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

fn range_bounds(kind: TableKind, filters: &[Expr]) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut lower = vec![kind.prefix(), b'/'];
    let mut upper = vec![kind.prefix(), b'0'];
    for filter in filters {
        apply_range_bound(filter, kind, &mut lower, &mut upper)?;
    }
    Ok((lower, upper))
}

fn apply_range_bound(
    expression: &Expr,
    kind: TableKind,
    lower: &mut Vec<u8>,
    upper: &mut Vec<u8>,
) -> Result<()> {
    let Expr::BinaryExpr(binary) = expression else {
        return Ok(());
    };
    if binary.op == Operator::And {
        apply_range_bound(&binary.left, kind, lower, upper)?;
        apply_range_bound(&binary.right, kind, lower, upper)?;
        return Ok(());
    }
    let (column, literal, operator) = match (&*binary.left, &*binary.right) {
        (Expr::Column(column), Expr::Literal(value, _)) => (column, value, binary.op),
        (Expr::Literal(value, _), Expr::Column(column)) => {
            let operator = match binary.op {
                Operator::Lt => Operator::Gt,
                Operator::LtEq => Operator::GtEq,
                Operator::Gt => Operator::Lt,
                Operator::GtEq => Operator::LtEq,
                operator => operator,
            };
            (column, value, operator)
        }
        _ => return Ok(()),
    };
    if column.name != kind.primary_key() {
        return Ok(());
    }
    let value = match (kind, scalar_datum(literal)?) {
        (TableKind::Dimension, Datum::Integer(value)) => format!("{value:02}"),
        (_, Datum::Text(value)) => value,
        _ => return Ok(()),
    };
    let key = common::key(kind.prefix(), &value);
    match operator {
        Operator::Eq => {
            *lower = key.clone();
            *upper = key;
        }
        Operator::Gt | Operator::GtEq => {
            if key > *lower {
                *lower = key;
            }
        }
        Operator::Lt | Operator::LtEq => {
            if key < *upper {
                *upper = key;
            }
        }
        _ => {}
    }
    Ok(())
}

fn i64_at(bytes: &[u8], offset: usize) -> Result<i64> {
    Ok(i64::from_be_bytes(
        bytes
            .get(offset..offset + 8)
            .ok_or_else(|| DataFusionError::Execution("truncated Int64 field".to_string()))?
            .try_into()
            .expect("validated Int64 width"),
    ))
}

fn take<'a>(bytes: &'a [u8], offset: &mut usize, length: usize) -> Result<&'a [u8]> {
    let end = offset
        .checked_add(length)
        .ok_or_else(|| DataFusionError::Execution("row offset overflow".to_string()))?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| DataFusionError::Execution("truncated row".to_string()))?;
    *offset = end;
    Ok(value)
}

fn take_byte(bytes: &[u8], offset: &mut usize) -> Result<u8> {
    Ok(take(bytes, offset, 1)?[0])
}

fn take_u32(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    Ok(u32::from_be_bytes(
        take(bytes, offset, 4)?
            .try_into()
            .expect("validated u32 width"),
    ))
}

fn hex_digest(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}
