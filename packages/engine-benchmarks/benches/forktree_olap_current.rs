use std::alloc::GlobalAlloc;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use lix::Value;
use lix::integration::{Engine, SessionContext};
use lix::storage::{
    CommitResult, GetManyRequest, GetManyResult, Key, KeyRange, ProjectedValue, PutBatch,
    ReadOptions, ScanChunk, ScanOptions, Storage, StorageError, StorageRead, StorageWrite,
    WriteOptions,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters, SlateDBIoSnapshot};

#[path = "forktree_olap_common.rs"]
#[allow(dead_code)]
mod common;
use common::{Cell, Query};

#[global_allocator]
static ALLOCATOR: AllocationCounter = AllocationCounter;
struct AllocationCounter;
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOC_ON: AtomicBool = AtomicBool::new(false);

unsafe impl GlobalAlloc for AllocationCounter {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let pointer = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !pointer.is_null() && ALLOC_ON.load(Ordering::Relaxed) {
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: std::alloc::Layout) {
        unsafe { mimalloc::MiMalloc.dealloc(pointer, layout) };
    }
    unsafe fn realloc(
        &self,
        pointer: *mut u8,
        layout: std::alloc::Layout,
        new_size: usize,
    ) -> *mut u8 {
        let replacement = unsafe { mimalloc::MiMalloc.realloc(pointer, layout, new_size) };
        if !replacement.is_null() && new_size >= layout.size() && ALLOC_ON.load(Ordering::Relaxed) {
            ALLOC_BYTES.fetch_add((new_size - layout.size()) as u64, Ordering::Relaxed);
            ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        replacement
    }
}

#[derive(Clone, Default)]
struct IoStats {
    begin_reads: u64,
    get_calls: u64,
    get_keys: u64,
    get_values: u64,
    get_value_bytes: u64,
    scan_calls: u64,
    scan_entries: u64,
    scan_value_bytes: u64,
}

#[derive(Clone)]
struct Counted<S> {
    inner: S,
    stats: Arc<Mutex<IoStats>>,
}
struct CountedRead<R> {
    inner: R,
    stats: Arc<Mutex<IoStats>>,
}
struct CountedWrite<W>(W);

impl<S> Counted<S> {
    fn new(inner: S) -> (Self, Arc<Mutex<IoStats>>) {
        let stats = Arc::new(Mutex::new(IoStats::default()));
        (
            Self {
                inner,
                stats: Arc::clone(&stats),
            },
            stats,
        )
    }
}

impl<S: Storage> Storage for Counted<S> {
    type Read<'a>
        = CountedRead<S::Read<'a>>
    where
        Self: 'a;
    type Write<'a>
        = CountedWrite<S::Write<'a>>
    where
        Self: 'a;
    async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.stats.lock().expect("I/O stats").begin_reads += 1;
        Ok(CountedRead {
            inner: self.inner.begin_read(options).await?,
            stats: Arc::clone(&self.stats),
        })
    }
    async fn begin_write(&self, options: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        Ok(CountedWrite(self.inner.begin_write(options).await?))
    }
}

impl<R: StorageRead> StorageRead for CountedRead<R> {
    fn snapshot_cache_key(&self) -> Option<u128> {
        self.inner.snapshot_cache_key()
    }
    async fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> Result<GetManyResult, StorageError> {
        {
            let mut stats = self.stats.lock().expect("I/O stats");
            stats.get_calls += 1;
            stats.get_keys += requests
                .iter()
                .map(|request| request.keys.len() as u64)
                .sum::<u64>();
        }
        let result = self.inner.get_many(requests).await?;
        let mut stats = self.stats.lock().expect("I/O stats");
        for value in result.values.iter().flatten() {
            stats.get_values += 1;
            stats.get_value_bytes += projected_len(value) as u64;
        }
        drop(stats);
        Ok(result)
    }
    async fn scan(
        &self,
        space: lix::storage::StorageSpace,
        range: KeyRange,
        options: ScanOptions,
    ) -> Result<ScanChunk, StorageError> {
        self.stats.lock().expect("I/O stats").scan_calls += 1;
        let result = self.inner.scan(space, range, options).await?;
        let mut stats = self.stats.lock().expect("I/O stats");
        stats.scan_entries += result.entries.len() as u64;
        stats.scan_value_bytes += result
            .entries
            .iter()
            .map(|entry| projected_len(&entry.value) as u64)
            .sum::<u64>();
        drop(stats);
        Ok(result)
    }
}

impl<W: StorageWrite> StorageWrite for CountedWrite<W> {
    async fn put_many(
        &mut self,
        space: lix::storage::StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        self.0.put_many(space, entries).await
    }
    async fn delete_many(
        &mut self,
        space: lix::storage::StorageSpace,
        keys: &[Key],
    ) -> Result<(), StorageError> {
        self.0.delete_many(space, keys).await
    }
    async fn delete_range(
        &mut self,
        space: lix::storage::StorageSpace,
        range: KeyRange,
    ) -> Result<(), StorageError> {
        self.0.delete_range(space, range).await
    }
    async fn commit(self) -> Result<CommitResult, StorageError> {
        self.0.commit().await
    }
    async fn rollback(self) -> Result<(), StorageError> {
        self.0.rollback().await
    }
}

#[derive(Clone, Copy)]
enum Backend {
    RocksDb,
    SlateDb,
}
impl Backend {
    fn parse(value: &str) -> Self {
        match value {
            "rocksdb" => Self::RocksDb,
            "slatedb" => Self::SlateDb,
            other => panic!("unknown backend '{other}'"),
        }
    }
    const fn label(self) -> &'static str {
        match self {
            Self::RocksDb => "rocksdb",
            Self::SlateDb => "slatedb",
        }
    }
}
#[derive(Clone, Copy)]
struct Parameters {
    backend: Backend,
    rows: usize,
    samples: usize,
    warmups: usize,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let parameters = Parameters {
        backend: Backend::parse(args.get(1).map(String::as_str).unwrap_or("rocksdb")),
        rows: parse(args.get(2), 10_000),
        samples: parse(args.get(3), 5),
        warmups: parse(args.get(4), 1),
    };
    println!(
        "forktree_olap_boundary,sql_wiring=true,comparison=current_sql_datafusion,current_big_o=O(N+Q),claim=exact_current_main"
    );
    match parameters.backend {
        Backend::RocksDb => run_rocks(parameters).await,
        Backend::SlateDb => run_slate(parameters).await,
    }
}

async fn run_rocks(parameters: Parameters) {
    let directory = tempfile::tempdir().expect("current OLAP RocksDB directory");
    let database = RocksDB::open(directory.path()).expect("open current OLAP RocksDB");
    let (storage, stats) = Counted::new(database.clone());
    let session = prepare(storage, parameters.rows).await;
    database.flush().expect("flush RocksDB setup");
    run_queries(&session, parameters, &stats, directory.path(), None).await;
    database.flush().expect("flush RocksDB result");
    let disk = directory_bytes(directory.path());
    drop(session);
    drop(database);
    let (storage, _) = Counted::new(RocksDB::open(directory.path()).expect("reopen RocksDB"));
    let engine = Engine::new(storage).await.expect("reopen current engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("reopen session");
    verify(&session, parameters.rows).await;
    println!(
        "forktree_olap_reopen,backend=rocksdb,layout=current,rows={},exact_results=true,disk_bytes={disk}",
        parameters.rows
    );
}

async fn run_slate(parameters: Parameters) {
    let directory = tempfile::tempdir().expect("current OLAP SlateDB directory");
    let counters = SlateDBIoCounters::default();
    let database = SlateDB::open_with_io_counters(directory.path(), counters.clone())
        .expect("open current OLAP SlateDB");
    let (storage, stats) = Counted::new(database.clone());
    let session = prepare(storage, parameters.rows).await;
    database
        .flush_memtable_for_diagnostics()
        .await
        .expect("flush SlateDB setup");
    run_queries(
        &session,
        parameters,
        &stats,
        directory.path(),
        Some(&counters),
    )
    .await;
    database
        .flush_memtable_for_diagnostics()
        .await
        .expect("flush SlateDB result");
    let disk = directory_bytes(directory.path());
    drop(session);
    drop(database);
    let (storage, _) = Counted::new(SlateDB::open(directory.path()).expect("reopen SlateDB"));
    let engine = Engine::new(storage).await.expect("reopen current engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("reopen session");
    verify(&session, parameters.rows).await;
    println!(
        "forktree_olap_reopen,backend=slatedb,layout=current,rows={},exact_results=true,disk_bytes={disk}",
        parameters.rows
    );
}

async fn prepare<S>(storage: Counted<S>, rows: usize) -> SessionContext<Counted<S>>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    Engine::initialize(storage.clone())
        .await
        .expect("initialize current OLAP");
    let engine = Engine::new(storage).await.expect("open current OLAP");
    let session = engine
        .open_workspace_session()
        .await
        .expect("open current session");
    register_schemas(&session).await;
    let mut transaction = session
        .begin_transaction()
        .await
        .expect("begin current OLAP seed transaction");
    for start in (0..rows).step_by(256) {
        let end = rows.min(start + 256);
        transaction
            .execute(&narrow_insert_sql(start..end), &[])
            .await
            .expect("seed narrow rows");
        transaction
            .execute(&wide_insert_sql(start..end), &[])
            .await
            .expect("seed wide rows");
    }
    transaction
        .execute(&dimension_insert_sql(), &[])
        .await
        .expect("seed dimensions");
    transaction
        .execute(&nullable_insert_sql(), &[])
        .await
        .expect("seed nullable rows");
    transaction
        .commit()
        .await
        .expect("commit current OLAP seed transaction");
    session
}

fn narrow_insert_sql(range: std::ops::Range<usize>) -> String {
    let mut sql =
        String::from("INSERT INTO forktree_olap_narrow (id, ordinal, lane, score, active) VALUES ");
    for (position, ordinal) in range.enumerate() {
        let row = common::narrow_row(ordinal);
        if position != 0 {
            sql.push(',');
        }
        write!(
            sql,
            "('{}',{},{},{},{})",
            row.id,
            row.ordinal,
            row.lane,
            row.score,
            if row.active { "TRUE" } else { "FALSE" }
        )
        .expect("write narrow seed SQL");
    }
    sql
}

fn wide_insert_sql(range: std::ops::Range<usize>) -> String {
    let mut sql = String::from(
        "INSERT INTO forktree_olap_wide (id, ordinal, lane, score, active, c00, c01, c02, c03, c04, c05, c06, c07, c08, c09, c10, c11, c12, c13, c14, c15, payload) VALUES ",
    );
    for (position, ordinal) in range.enumerate() {
        let row = common::wide_row(ordinal);
        if position != 0 {
            sql.push(',');
        }
        write!(
            sql,
            "('{}',{},{},{},{}",
            row.base.id,
            row.base.ordinal,
            row.base.lane,
            row.base.score,
            if row.base.active { "TRUE" } else { "FALSE" }
        )
        .expect("write wide seed prefix");
        for value in row.columns {
            write!(sql, ",{value}").expect("write wide seed column");
        }
        write!(sql, ",'{}')", row.payload).expect("write wide seed payload");
    }
    sql
}

fn dimension_insert_sql() -> String {
    let mut sql = String::from("INSERT INTO forktree_olap_dim (lane, label) VALUES ");
    for (position, (lane, label)) in common::dimension_rows().into_iter().enumerate() {
        if position != 0 {
            sql.push(',');
        }
        write!(sql, "({lane},'{label}')").expect("write dimension seed SQL");
    }
    sql
}

fn nullable_insert_sql() -> &'static str {
    "INSERT INTO forktree_olap_nullable (id, note, score) VALUES \
     ('nullable-00','alpha',10),('nullable-01',NULL,20),\
     ('nullable-02','gamma',NULL),('nullable-03',NULL,NULL)"
}

async fn register_schemas<S>(session: &SessionContext<Counted<S>>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let base = serde_json::json!({"id":{"type":"string"},"ordinal":{"type":"integer"},"lane":{"type":"integer"},"score":{"type":"integer"},"active":{"type":"boolean"}});
    let narrow = serde_json::json!({"x-lix-key":"forktree_olap_narrow","x-lix-primary-key":["/id"],"type":"object","required":["id","ordinal","lane","score","active"],"properties":base,"additionalProperties":false});
    let mut wide_properties = base.as_object().expect("base properties").clone();
    for column in 0..common::WIDE_COLUMNS {
        wide_properties.insert(
            format!("c{column:02}"),
            serde_json::json!({"type":"integer"}),
        );
    }
    wide_properties.insert("payload".to_string(), serde_json::json!({"type":"string"}));
    let mut required = ["id", "ordinal", "lane", "score", "active"]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    required.extend((0..common::WIDE_COLUMNS).map(|column| format!("c{column:02}")));
    required.push("payload".to_string());
    let wide = serde_json::json!({"x-lix-key":"forktree_olap_wide","x-lix-primary-key":["/id"],"type":"object","required":required,"properties":wide_properties,"additionalProperties":false});
    let dimension = serde_json::json!({"x-lix-key":"forktree_olap_dim","x-lix-primary-key":["/lane"],"type":"object","required":["lane","label"],"properties":{"lane":{"type":"integer"},"label":{"type":"string"}},"additionalProperties":false});
    let nullable = serde_json::json!({"x-lix-key":"forktree_olap_nullable","x-lix-primary-key":["/id"],"type":"object","required":["id"],"properties":{"id":{"type":"string"},"note":{"type":["string","null"]},"score":{"type":["integer","null"]}},"additionalProperties":false});
    for schema in [narrow, wide, dimension, nullable] {
        session.execute("INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (lix_json($1), false, false)", &[Value::Text(schema.to_string())]).await.expect("register OLAP schema");
    }
}

async fn run_queries<S>(
    session: &SessionContext<Counted<S>>,
    parameters: Parameters,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    if parameters.rows == 1_000 {
        run_semantic_oracle(session).await;
    }
    for (query, digest, rows) in expected(parameters.rows) {
        for sample in 0..parameters.warmups + parameters.samples {
            let _ = take_stats(stats);
            let physical_before = counters.map(SlateDBIoCounters::snapshot);
            let rss_before = rss();
            let cpu_before = cpu_nanos();
            begin_alloc();
            let started = Instant::now();
            let result = execute(session, query).await;
            let wall_us = started.elapsed().as_secs_f64() * 1_000_000.0;
            let cpu_us = cpu_nanos().saturating_sub(cpu_before) as f64 / 1_000.0;
            let (alloc_bytes, alloc_calls) = end_alloc();
            let rss_after = rss();
            assert_eq!(result.len(), rows);
            assert_eq!(common::digest(&result), digest);
            let logical = take_stats(stats);
            let physical = physical_delta(counters, physical_before);
            if sample >= parameters.warmups {
                println!(
                    "forktree_olap,sample={},backend={},layout=current,rows={},query={},wall_us={wall_us:.3},cpu_us={cpu_us:.3},alloc_bytes={alloc_bytes},alloc_calls={alloc_calls},rss_before_bytes={rss_before},rss_after_bytes={rss_after},begin_reads={},get_calls={},get_keys={},get_values={},get_value_bytes={},scan_calls={},scan_entries={},scan_value_bytes={},physical_read_objects={},physical_read_bytes={},physical_write_objects={},physical_write_bytes={},logical_result_rows={},disk_bytes={}",
                    sample - parameters.warmups + 1,
                    parameters.backend.label(),
                    parameters.rows,
                    query.label(),
                    logical.begin_reads,
                    logical.get_calls,
                    logical.get_keys,
                    logical.get_values,
                    logical.get_value_bytes,
                    logical.scan_calls,
                    logical.scan_entries,
                    logical.scan_value_bytes,
                    physical.read_objects,
                    physical.read_bytes,
                    physical.write_objects,
                    physical.write_bytes,
                    result.len(),
                    directory_bytes(path)
                );
            }
            std::hint::black_box(result);
        }
    }
}

async fn run_semantic_oracle<S>(session: &SessionContext<Counted<S>>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
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
        let rows = execute_sql(session, sql).await;
        println!(
            "forktree_current_semantic,label={label},rows={},digest={}",
            rows.len(),
            hex_digest(common::digest(&rows))
        );
        let explain = session.execute(&format!("EXPLAIN {sql}"), &[]).await;
        println!(
            "forktree_current_plan,label={label},supported={},plan={:?}",
            explain.is_ok(),
            explain.ok().map(|result| result.rows().to_vec())
        );
    }
}

async fn execute<S>(session: &SessionContext<Counted<S>>, query: Query) -> Vec<Vec<Cell>>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute(query.sql(), &[])
        .await
        .expect("execute current OLAP query")
        .rows()
        .iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|value| match value {
                    Value::Integer(value) => Cell::Integer(*value),
                    Value::Text(value) => Cell::Text(value.clone()),
                    Value::Boolean(value) => Cell::Boolean(*value),
                    other => panic!("unexpected OLAP value {other:?}"),
                })
                .collect()
        })
        .collect()
}

async fn execute_sql<S>(session: &SessionContext<Counted<S>>, sql: &str) -> Vec<Vec<Cell>>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute(sql, &[])
        .await
        .expect("execute current semantic SQL")
        .rows()
        .iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|value| match value {
                    Value::Null => Cell::Null,
                    Value::Integer(value) => Cell::Integer(*value),
                    Value::Text(value) => Cell::Text(value.clone()),
                    Value::Boolean(value) => Cell::Boolean(*value),
                    other => panic!("unexpected semantic value {other:?}"),
                })
                .collect()
        })
        .collect()
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

fn expected(rows: usize) -> Vec<(Query, [u8; 32], usize)> {
    let narrow = (0..rows).map(common::narrow_row).collect::<Vec<_>>();
    let wide = (0..rows).map(common::wide_row).collect::<Vec<_>>();
    let dimensions = common::dimension_rows();
    Query::ALL
        .into_iter()
        .map(|query| {
            let result = common::evaluate(query, &narrow, &wide, &dimensions);
            (query, common::digest(&result), result.len())
        })
        .collect()
}

async fn verify<S>(session: &SessionContext<Counted<S>>, rows: usize)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    for (query, digest, row_count) in expected(rows) {
        let result = execute(session, query).await;
        assert_eq!(result.len(), row_count);
        assert_eq!(common::digest(&result), digest);
    }
}

fn begin_alloc() {
    ALLOC_BYTES.store(0, Ordering::Relaxed);
    ALLOC_CALLS.store(0, Ordering::Relaxed);
    ALLOC_ON.store(true, Ordering::Relaxed);
}
fn end_alloc() -> (u64, u64) {
    ALLOC_ON.store(false, Ordering::Relaxed);
    (
        ALLOC_BYTES.load(Ordering::Relaxed),
        ALLOC_CALLS.load(Ordering::Relaxed),
    )
}
fn take_stats(stats: &Arc<Mutex<IoStats>>) -> IoStats {
    std::mem::take(&mut *stats.lock().expect("I/O stats"))
}
fn physical_delta(
    counters: Option<&SlateDBIoCounters>,
    before: Option<SlateDBIoSnapshot>,
) -> SlateDBIoSnapshot {
    match (counters, before) {
        (Some(counters), Some(before)) => counters.snapshot().saturating_sub(before),
        _ => SlateDBIoSnapshot::default(),
    }
}
fn projected_len(value: &ProjectedValue) -> usize {
    match value {
        ProjectedValue::KeyOnly => 0,
        ProjectedValue::FullValue(value) => value.len(),
    }
}
fn rss() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find_map(|line| line.strip_prefix("VmRSS:"))
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse::<u64>().ok())
        })
        .map_or(0, |kb| kb * 1024)
}
fn cpu_nanos() -> u64 {
    let mut value = std::mem::MaybeUninit::<libc::timespec>::uninit();
    if unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, value.as_mut_ptr()) } != 0 {
        return 0;
    }
    let value = unsafe { value.assume_init() };
    u64::try_from(value.tv_sec)
        .unwrap_or(0)
        .saturating_mul(1_000_000_000)
        .saturating_add(u64::try_from(value.tv_nsec).unwrap_or(0))
}
fn directory_bytes(path: &std::path::Path) -> u64 {
    std::fs::read_dir(path).map_or(0, |entries| {
        entries.flatten().fold(0_u64, |total, entry| {
            let path = entry.path();
            total.saturating_add(if path.is_dir() {
                directory_bytes(&path)
            } else {
                entry.metadata().map_or(0, |metadata| metadata.len())
            })
        })
    })
}
fn parse(value: Option<&String>, default: usize) -> usize {
    value.map_or(default, |value| value.parse().expect("positive integer"))
}
