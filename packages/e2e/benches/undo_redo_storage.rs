#![allow(clippy::large_futures)]

//! Destructive, real-backend undo/redo phase profiler.
//!
//! Setup and measurement are separate processes so fixture construction is
//! excluded from the operation window. Clone a prepared directory before each
//! measured sample because undo/redo append semantic commits.

use std::alloc::{GlobalAlloc, Layout};
use std::fmt::Write as _;
use std::future::Future;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use lix::storage::Storage;
use lix::storage::{
    BeginScanOptions, CommitResult, GetManyRequest, GetManyResult, Key, KeyRange, ProjectedValue,
    PutBatch, ReadOptions, ScanChunk, ScanCursor, StorageError, StorageRead, StorageScanSource,
    StorageSpace, StorageWrite, WriteOptions,
};
use lix::storage_bench::{
    CRUD_OWNERSHIP_ADAPTER, CRUD_OWNERSHIP_AUTHORITY, CRUD_OWNERSHIP_ROOT_PUBLICATION,
    CRUD_OWNERSHIP_WRITE_SET, begin_crud_ownership_accounting, has_durable_commit_root_for_bench,
    take_crud_commit_state_manifest_bytes, take_crud_ownership_accounting,
    take_crud_physical_write_accounting,
};
use lix::{ExecuteBatchStatement, Value};
use lix::{Lix, open_lix};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters, SlateDBIoSnapshot};

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

struct CountingAllocator;

static ALLOCATION_ENABLED: AtomicBool = AtomicBool::new(false);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !pointer.is_null() && ALLOCATION_ENABLED.load(Ordering::Relaxed) {
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { mimalloc::MiMalloc.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let replacement = unsafe { mimalloc::MiMalloc.realloc(pointer, layout, new_size) };
        if !replacement.is_null()
            && new_size > layout.size()
            && ALLOCATION_ENABLED.load(Ordering::Relaxed)
        {
            ALLOCATED_BYTES.fetch_add((new_size - layout.size()) as u64, Ordering::Relaxed);
            ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        replacement
    }
}

#[derive(Clone, Copy, Debug)]
enum Backend {
    Rocks,
    Slate,
}

impl Backend {
    fn parse(value: &str) -> Self {
        match value {
            "rocksdb" => Self::Rocks,
            "slatedb" => Self::Slate,
            _ => panic!("backend must be rocksdb or slatedb, got {value}"),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Rocks => "rocksdb",
            Self::Slate => "slatedb",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum ReadyState {
    Undo,
    Redo,
}

impl ReadyState {
    fn parse(value: &str) -> Self {
        match value {
            "undo" => Self::Undo,
            "redo" => Self::Redo,
            _ => panic!("ready state must be undo or redo, got {value}"),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Operation {
    Undo,
    Redo,
    Chain,
}

impl Operation {
    fn parse(value: &str) -> Self {
        match value {
            "undo" => Self::Undo,
            "redo" => Self::Redo,
            "chain" => Self::Chain,
            _ => panic!("operation must be undo, redo, or chain, got {value}"),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Undo => "undo",
            Self::Redo => "redo",
            Self::Chain => "chain",
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct IoStats {
    get_many_calls: u64,
    get_many_keys: u64,
    scan_calls: u64,
    scan_rows: u64,
    scan_value_bytes: u64,
    put_batches: u64,
    puts: u64,
    delete_batches: u64,
    deletes: u64,
    write_bytes: u64,
}

#[derive(Clone)]
struct CountingStorage<S> {
    inner: S,
    stats: Arc<Mutex<IoStats>>,
}

struct CountingRead<R> {
    inner: R,
    stats: Arc<Mutex<IoStats>>,
}

struct CountingWrite<W> {
    inner: W,
    stats: Arc<Mutex<IoStats>>,
}

impl<S> CountingStorage<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            stats: Arc::new(Mutex::new(IoStats::default())),
        }
    }

    fn reset(&self) {
        *self.stats.lock().expect("I/O stats mutex") = IoStats::default();
    }

    fn snapshot(&self) -> IoStats {
        *self.stats.lock().expect("I/O stats mutex")
    }
}

impl<S: Storage> Storage for CountingStorage<S> {
    type Read<'a>
        = CountingRead<S::Read<'a>>
    where
        Self: 'a;
    type Write<'a>
        = CountingWrite<S::Write<'a>>
    where
        Self: 'a;

    async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        Ok(CountingRead {
            inner: self.inner.begin_read(opts).await?,
            stats: Arc::clone(&self.stats),
        })
    }

    async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        Ok(CountingWrite {
            inner: self.inner.begin_write(opts).await?,
            stats: Arc::clone(&self.stats),
        })
    }
}

impl<R: StorageRead> StorageRead for CountingRead<R> {
    async fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> Result<GetManyResult, StorageError> {
        {
            let mut stats = self.stats.lock().expect("I/O stats mutex");
            stats.get_many_calls += 1;
            stats.get_many_keys += requests
                .iter()
                .map(|request| request.keys.len() as u64)
                .sum::<u64>();
        }
        self.inner.get_many(requests).await
    }

    async fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> Result<ScanCursor<'_>, StorageError> {
        let order = opts.order;
        self.stats.lock().expect("I/O stats mutex").scan_calls += 1;
        let inner = self.inner.begin_scan(space, range.clone(), opts).await?;
        ScanCursor::from_source(
            range,
            order,
            CountingScanSource {
                inner,
                stats: Arc::clone(&self.stats),
            },
        )
    }
}

struct CountingScanSource<'a> {
    inner: ScanCursor<'a>,
    stats: Arc<Mutex<IoStats>>,
}

impl StorageScanSource for CountingScanSource<'_> {
    fn next_page(
        &mut self,
        limit_rows: usize,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<ScanChunk, StorageError>> + Send + '_>> {
        Box::pin(async move {
            let (chunk, chunk_has_more) = self.inner.next_page(limit_rows).await?.into_parts();
            let mut stats = self.stats.lock().expect("I/O stats mutex");
            stats.scan_rows += chunk.len() as u64;
            stats.scan_value_bytes += chunk
                .iter()
                .map(|entry| match &entry.value {
                    ProjectedValue::KeyOnly => 0,
                    ProjectedValue::FullValue(value) => value.len() as u64,
                })
                .sum::<u64>();
            drop(stats);
            Ok(ScanChunk::new(chunk, chunk_has_more))
        })
    }
}

impl<W: StorageWrite> StorageWrite for CountingWrite<W> {
    async fn put_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        {
            let mut stats = self.stats.lock().expect("I/O stats mutex");
            stats.put_batches += 1;
            stats.puts += entries.entries.len() as u64;
            stats.write_bytes += entries
                .entries
                .iter()
                .map(|entry| (entry.key.0.len() + entry.value.bytes.len()) as u64)
                .sum::<u64>();
        }
        self.inner.put_many(space, entries).await
    }

    async fn replace_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        self.inner.replace_many(space, entries).await
    }

    async fn delete_many(&mut self, space: StorageSpace, keys: &[Key]) -> Result<(), StorageError> {
        {
            let mut stats = self.stats.lock().expect("I/O stats mutex");
            stats.delete_batches += 1;
            stats.deletes += keys.len() as u64;
        }
        self.inner.delete_many(space, keys).await
    }

    async fn delete_range(
        &mut self,
        space: StorageSpace,
        range: KeyRange,
    ) -> Result<(), StorageError> {
        self.inner.delete_range(space, range).await
    }

    async fn commit(self) -> Result<CommitResult, StorageError> {
        self.inner.commit().await
    }

    async fn rollback(self) -> Result<(), StorageError> {
        self.inner.rollback().await
    }
}

fn main() {
    init_perf_tracing();
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() < 4 {
        usage();
        return;
    }
    let mode = args[1].as_str();
    let backend = Backend::parse(&args[2]);
    let path = &args[3];
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create undo/redo benchmark runtime");
    match mode {
        "setup" => {
            let rows = parse_usize(args.get(4), "rows");
            let width = parse_usize(args.get(5), "transition width");
            let ready = args
                .get(6)
                .map(|value| ReadyState::parse(value))
                .unwrap_or(ReadyState::Undo);
            assert!(width <= rows, "transition width must not exceed rows");
            assert!(!Path::new(path).exists(), "refusing to overwrite {path}");
            runtime.block_on(async move {
                match backend {
                    Backend::Rocks => {
                        setup(
                            RocksDB::open(path).expect("open undo/redo RocksDB"),
                            backend,
                            rows,
                            width,
                            ready,
                        )
                        .await
                    }
                    Backend::Slate => {
                        setup(
                            SlateDB::open(path).expect("open undo/redo SlateDB"),
                            backend,
                            rows,
                            width,
                            ready,
                        )
                        .await
                    }
                }
            });
        }
        "measure" => {
            let operation = Operation::parse(args.get(4).expect("measure operation"));
            let steps = args.get(5).map_or(1, |value| {
                value.parse().expect("chain steps must be positive")
            });
            let width = parse_usize(args.get(6), "transition width");
            assert!(steps > 0, "steps must be positive");
            runtime.block_on(async move {
                match backend {
                    Backend::Rocks => {
                        let storage = CountingStorage::new(
                            RocksDB::open(path).expect("open measured undo/redo RocksDB"),
                        );
                        measure(storage, backend, path, operation, steps, width, None).await;
                    }
                    Backend::Slate => {
                        let counters = SlateDBIoCounters::default();
                        let storage = CountingStorage::new(
                            SlateDB::open_with_io_counters(path, counters.clone())
                                .expect("open measured undo/redo SlateDB"),
                        );
                        measure(
                            storage,
                            backend,
                            path,
                            operation,
                            steps,
                            width,
                            Some(counters),
                        )
                        .await;
                    }
                }
            });
        }
        _ => usage(),
    }
}

fn usage() {
    eprintln!(
        "usage:\n  undo_redo_storage setup <rocksdb|slatedb> <path> <rows> <width> [undo|redo]\n  \
         undo_redo_storage measure <rocksdb|slatedb> <path> <undo|redo|chain> <steps> <width>"
    );
}

fn parse_usize(value: Option<&String>, label: &str) -> usize {
    let value = value
        .unwrap_or_else(|| panic!("missing {label}"))
        .parse::<usize>()
        .unwrap_or_else(|_| panic!("{label} must be a positive integer"));
    assert!(value > 0, "{label} must be positive");
    value
}

fn init_perf_tracing() {
    if std::env::var_os("LIX_UNDO_REDO_TRACE").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("lix_perf=debug")
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
            .with_target(false)
            .try_init();
    }
}

async fn setup<S>(storage: S, backend: Backend, rows: usize, width: usize, ready: ReadyState)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let started = Instant::now();
    open_lix()
        .with_storage(storage.clone())
        .await
        .expect("initialize undo/redo storage");
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open undo/redo setup lix");
    let session = lix
        .open_another_session()
        .await
        .expect("open undo/redo setup session");
    register_schema(&session).await;
    seed_rows(&session, rows).await;
    let seed_commit_id = current_head_commit_id(&session).await;
    let seed_has_root = has_durable_commit_root_for_bench(storage.clone(), &seed_commit_id)
        .await
        .expect("inspect seeded undo benchmark root");
    stage_transition(&session, rows, width).await;
    let transition_commit_id = current_head_commit_id(&session).await;
    let transition_has_root =
        has_durable_commit_root_for_bench(storage.clone(), &transition_commit_id)
            .await
            .expect("inspect transition undo benchmark root");
    if matches!(ready, ReadyState::Redo) {
        session.undo().await.expect("prepare redo-ready fixture");
    }
    let expected_after = if matches!(ready, ReadyState::Undo) {
        width
    } else {
        0
    };
    assert_eq!(count_value(&session, "after").await, expected_after);
    println!(
        "undo_redo_storage setup backend={} rows={rows} width={width} ready={ready:?} setup_ms={:.3} seed_commit_id={seed_commit_id} seed_has_root={seed_has_root} transition_commit_id={transition_commit_id} transition_has_root={transition_has_root} backend_bytes={} backend_objects={}",
        backend.name(),
        millis(started.elapsed()),
        directory_bytes(Path::new(&std::env::args().nth(3).expect("setup path"))),
        directory_objects(Path::new(&std::env::args().nth(3).expect("setup path"))),
    );
}

async fn register_schema<S>(session: &Lix<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let schema = serde_json::json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "undo_bench_row",
        "columns": [
            { "name": "id", "type": "text", "nullable": false },
            { "name": "value", "type": "text", "nullable": false },
        ],
        "primary_key": ["id"],
    });
    let result = session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (CAST($1 AS JSONB), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register undo benchmark schema");
    assert_eq!(result.rows_affected(), 1);
}

async fn seed_rows<S>(session: &Lix<S>, rows: usize)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    const PAGE: usize = 1_000;
    let mut transaction = session
        .begin_transaction()
        .await
        .expect("begin undo benchmark seed transaction");
    for start in (0..rows).step_by(PAGE) {
        let end = (start + PAGE).min(rows);
        let mut sql = String::from("INSERT INTO undo_bench_row (id, value) VALUES ");
        let mut params = Vec::with_capacity((end - start) * 2);
        for (offset, index) in (start..end).enumerate() {
            if offset > 0 {
                sql.push(',');
            }
            let parameter = offset * 2;
            write!(sql, "(${}, ${})", parameter + 1, parameter + 2)
                .expect("format undo benchmark insert");
            params.push(Value::Text(format!("row-{index:08}")));
            params.push(Value::Text("before".to_string()));
        }
        let result = transaction
            .execute(&sql, &params)
            .await
            .expect("seed undo benchmark rows");
        assert_eq!(result.rows_affected() as usize, end - start);
    }
    transaction
        .commit()
        .await
        .expect("commit undo benchmark rows");
}

async fn stage_transition<S>(session: &Lix<S>, rows: usize, width: usize)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    if width == rows {
        let result = session
            .execute("UPDATE undo_bench_row SET value = 'after'", &[])
            .await
            .expect("stage full undo benchmark transition");
        assert_eq!(result.rows_affected() as usize, rows);
        return;
    }
    let statements = (0..width)
        .map(|index| ExecuteBatchStatement {
            label: None,
            sql: "UPDATE undo_bench_row SET value = $1 WHERE id = $2".to_string(),
            params: vec![
                Value::Text("after".to_string()),
                Value::Text(format!("row-{index:08}")),
            ],
        })
        .collect::<Vec<_>>();
    let results = session
        .execute_batch(&statements)
        .await
        .expect("stage sparse undo benchmark transition");
    assert_eq!(results.len(), width);
    assert!(results.iter().all(|result| result.rows_affected() == 1));
}

async fn measure<S>(
    storage: CountingStorage<S>,
    backend: Backend,
    path: &str,
    operation: Operation,
    steps: usize,
    width: usize,
    slate_counters: Option<SlateDBIoCounters>,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open measured undo/redo lix");
    let session = lix
        .open_another_session()
        .await
        .expect("open measured undo/redo session");
    let rows = count_rows(&session).await;
    let before_after = count_value(&session, "after").await;
    assert!(width <= rows, "transition width must not exceed rows");
    match operation {
        Operation::Undo | Operation::Chain => assert_eq!(before_after, width),
        Operation::Redo => assert_eq!(before_after, 0),
    }

    storage.reset();
    let _ = take_crud_physical_write_accounting();
    let _ = take_crud_commit_state_manifest_bytes();
    begin_crud_ownership_accounting();
    reset_allocations();
    let io_before = slate_counters
        .as_ref()
        .map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot);
    let bytes_before = directory_bytes(Path::new(path));
    let cpu_before = process_cpu_ticks();
    let rss_before = current_rss_kib();
    let (stop_sampler, peak_rss, sampler) = start_rss_sampler();
    let started = Instant::now();

    match operation {
        Operation::Undo => {
            session.undo().await.expect("measure undo");
        }
        Operation::Redo => {
            session.redo().await.expect("measure redo");
        }
        Operation::Chain => {
            for index in 0..steps {
                if index.is_multiple_of(2) {
                    session.undo().await.expect("measure chained undo");
                } else {
                    session.redo().await.expect("measure chained redo");
                }
            }
        }
    }

    let elapsed = started.elapsed();
    stop_sampler.store(true, Ordering::Release);
    sampler.join().expect("join RSS sampler");
    let cpu_ticks = process_cpu_ticks().saturating_sub(cpu_before);
    let (allocated_bytes, allocation_calls) = stop_allocations();
    let ownership = take_crud_ownership_accounting();
    let writes = take_crud_physical_write_accounting();
    let manifest_bytes = take_crud_commit_state_manifest_bytes();
    let io = storage.snapshot();
    let slate_io = slate_counters
        .as_ref()
        .map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot)
        .saturating_sub(io_before);
    let bytes_after = directory_bytes(Path::new(path));

    let expected_after = match operation {
        Operation::Undo => 0,
        Operation::Redo => width,
        Operation::Chain if steps.is_multiple_of(2) => width,
        Operation::Chain => 0,
    };
    assert_eq!(count_value(&session, "after").await, expected_after);

    let root = ownership.stages[CRUD_OWNERSHIP_ROOT_PUBLICATION];
    let authority = ownership.stages[CRUD_OWNERSHIP_AUTHORITY];
    let write_set = ownership.stages[CRUD_OWNERSHIP_WRITE_SET];
    let adapter = ownership.stages[CRUD_OWNERSHIP_ADAPTER];
    println!(
        "undo_redo_storage measure backend={} operation={} steps={steps} rows={rows} width={width} \
         wall_ms={:.3} cpu_ticks={cpu_ticks} allocated_bytes={allocated_bytes} allocation_calls={allocation_calls} \
         rss_before_kib={rss_before} peak_rss_kib={} hwm_kib={} \
         get_many_calls={} get_many_keys={} scan_calls={} scan_rows={} scan_value_bytes={} \
         put_batches={} puts={} delete_batches={} deletes={} logical_write_bytes={} \
         staged_puts={} staged_deletes={} staged_write_bytes={} manifest_bytes={} \
         root_rows={} root_key_bytes={} root_value_bytes={} authority_rows={} authority_key_bytes={} authority_value_bytes={} \
         write_set_rows={} write_set_key_bytes={} write_set_value_bytes={} adapter_rows={} adapter_value_bytes={} \
         slate_read_objects={} slate_read_bytes={} slate_write_objects={} slate_write_bytes={} \
         slate_reader_requests={} slate_main_requests={} backend_bytes_before={bytes_before} backend_bytes_after={bytes_after} backend_growth_bytes={}",
        backend.name(),
        operation.name(),
        millis(elapsed),
        peak_rss.load(Ordering::Relaxed),
        process_hwm_kib(),
        io.get_many_calls,
        io.get_many_keys,
        io.scan_calls,
        io.scan_rows,
        io.scan_value_bytes,
        io.put_batches,
        io.puts,
        io.delete_batches,
        io.deletes,
        io.write_bytes,
        writes.puts,
        writes.deletes,
        writes.written_bytes,
        manifest_bytes,
        root.rows,
        root.key_bytes,
        root.value_bytes,
        authority.rows,
        authority.key_bytes,
        authority.value_bytes,
        write_set.rows,
        write_set.key_bytes,
        write_set.value_bytes,
        adapter.rows,
        adapter.value_bytes,
        slate_io.read_objects,
        slate_io.read_bytes,
        slate_io.write_objects,
        slate_io.write_bytes,
        slate_io.reader.read_requests,
        slate_io.main.read_requests,
        bytes_after.saturating_sub(bytes_before),
    );
}

async fn count_rows<S>(session: &Lix<S>) -> usize
where
    S: Storage + Clone + Send + Sync + 'static,
{
    count_query(session, "SELECT COUNT(*) AS count FROM undo_bench_row").await
}

async fn current_head_commit_id<S>(session: &Lix<S>) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute("SELECT commit_id FROM lix_branch WHERE name = 'main'", &[])
        .await
        .expect("load main branch head")
        .rows()[0]
        .get::<String>("commit_id")
        .expect("decode main branch head")
}

async fn count_value<S>(session: &Lix<S>, value: &str) -> usize
where
    S: Storage + Clone + Send + Sync + 'static,
{
    count_query(
        session,
        &format!("SELECT COUNT(*) AS count FROM undo_bench_row WHERE value = '{value}'"),
    )
    .await
}

async fn count_query<S>(session: &Lix<S>, sql: &str) -> usize
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = session.execute(sql, &[]).await.expect("count undo rows");
    let count = result.rows()[0]
        .get::<i64>("count")
        .expect("decode undo row count");
    usize::try_from(count).expect("undo row count must be non-negative")
}

fn reset_allocations() {
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    ALLOCATION_CALLS.store(0, Ordering::Relaxed);
    ALLOCATION_ENABLED.store(true, Ordering::Release);
}

fn stop_allocations() -> (u64, u64) {
    ALLOCATION_ENABLED.store(false, Ordering::Release);
    (
        ALLOCATED_BYTES.load(Ordering::Relaxed),
        ALLOCATION_CALLS.load(Ordering::Relaxed),
    )
}

fn start_rss_sampler() -> (Arc<AtomicBool>, Arc<AtomicU64>, std::thread::JoinHandle<()>) {
    let stop = Arc::new(AtomicBool::new(false));
    let peak = Arc::new(AtomicU64::new(current_rss_kib()));
    let thread_stop = Arc::clone(&stop);
    let thread_peak = Arc::clone(&peak);
    let handle = std::thread::spawn(move || {
        while !thread_stop.load(Ordering::Acquire) {
            thread_peak.fetch_max(current_rss_kib(), Ordering::Relaxed);
            std::thread::sleep(Duration::from_millis(1));
        }
        thread_peak.fetch_max(current_rss_kib(), Ordering::Relaxed);
    });
    (stop, peak, handle)
}

fn current_rss_kib() -> u64 {
    proc_status_kib("VmRSS:")
}

fn process_hwm_kib() -> u64 {
    proc_status_kib("VmHWM:")
}

fn proc_status_kib(prefix: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find(|line| line.starts_with(prefix))
                .and_then(|line| line.split_whitespace().nth(1))
                .and_then(|value| value.parse().ok())
        })
        .unwrap_or(0)
}

fn process_cpu_ticks() -> u64 {
    let Ok(stat) = std::fs::read_to_string("/proc/self/stat") else {
        return 0;
    };
    let Some(after_name) = stat.rsplit_once(") ").map(|(_, tail)| tail) else {
        return 0;
    };
    let fields = after_name.split_whitespace().collect::<Vec<_>>();
    let user = fields.get(11).and_then(|value| value.parse::<u64>().ok());
    let system = fields.get(12).and_then(|value| value.parse::<u64>().ok());
    user.zip(system).map_or(0, |(user, system)| user + system)
}

fn directory_bytes(path: &Path) -> u64 {
    directory_accounting(path).0
}

fn directory_objects(path: &Path) -> u64 {
    directory_accounting(path).1
}

fn directory_accounting(path: &Path) -> (u64, u64) {
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return (0, 0);
    };
    if metadata.is_file() {
        return (metadata.len(), 1);
    }
    let Ok(entries) = std::fs::read_dir(path) else {
        return (0, 0);
    };
    entries.flatten().fold((0_u64, 0_u64), |totals, entry| {
        let child = directory_accounting(&entry.path());
        (totals.0.saturating_add(child.0), totals.1 + child.1)
    })
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}
