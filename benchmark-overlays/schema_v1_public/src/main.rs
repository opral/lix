//! Byte-identical public Schema-v1 qualification workload.
//!
//! This source intentionally depends only on public Lix, SQL, branch, file and
//! Storage APIs. It is copied unchanged into each comparator checkout.

use std::alloc::{GlobalAlloc, Layout};
use std::collections::BTreeMap;
use std::future::{Future, IntoFuture};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use blake3::Hasher;
use lix::storage::{
    BeginScanOptions, CommitResult, GetManyRequest, GetManyResult, Key, KeyRange, ProjectedValue,
    PutBatch, ReadOptions, ScanChunk, ScanCursor, Storage, StorageError, StorageRead,
    StorageScanSource, StorageSpace, StorageWrite, WriteOptions,
};
use lix::{
    CreateBranchOptions, ExecuteBatchStatement, ExecuteResult, Lix, MergeBranchOptions,
    SwitchBranchOptions, Value,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;

const SCHEMA_KEY: &str = "qualification_row";
const FILE_PATH: &str = "/qualification/payload.bin";

struct CountingAllocator;
static ALLOC_ON: AtomicBool = AtomicBool::new(false);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !pointer.is_null() && ALLOC_ON.load(Ordering::Relaxed) {
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { mimalloc::MiMalloc.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let replacement = unsafe { mimalloc::MiMalloc.realloc(pointer, layout, size) };
        if !replacement.is_null() && size > layout.size() && ALLOC_ON.load(Ordering::Relaxed) {
            ALLOC_BYTES.fetch_add((size - layout.size()) as u64, Ordering::Relaxed);
        }
        replacement
    }
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
struct Io {
    begin_reads: u64,
    begin_writes: u64,
    get_many_calls: u64,
    get_many_keys: u64,
    get_many_bytes: u64,
    scans: u64,
    scan_rows: u64,
    puts: u64,
    deletes: u64,
    delete_ranges: u64,
    logical_write_bytes: u64,
    commits: u64,
    backend_puts: u64,
    backend_deletes: u64,
    backend_deleted_ranges: u64,
    backend_storage_calls: u64,
    backend_bytes: u64,
}

#[derive(Clone)]
struct CountStorage<S> {
    inner: S,
    io: Arc<Mutex<Io>>,
}

impl<S> CountStorage<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            io: Arc::new(Mutex::new(Io::default())),
        }
    }

    fn reset(&self) {
        *self.io.lock().expect("I/O counter lock") = Io::default();
    }

    fn snapshot(&self) -> Io {
        *self.io.lock().expect("I/O counter lock")
    }
}

impl<S: Storage> Storage for CountStorage<S> {
    type Read<'a>
        = CountRead<S::Read<'a>>
    where
        Self: 'a;
    type Write<'a>
        = CountWrite<S::Write<'a>>
    where
        Self: 'a;

    async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.io.lock().expect("I/O counter lock").begin_reads += 1;
        Ok(CountRead {
            inner: self.inner.begin_read(options).await?,
            io: Arc::clone(&self.io),
        })
    }

    async fn begin_write(&self, options: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.io.lock().expect("I/O counter lock").begin_writes += 1;
        Ok(CountWrite {
            inner: self.inner.begin_write(options).await?,
            io: Arc::clone(&self.io),
        })
    }
}

struct CountRead<R> {
    inner: R,
    io: Arc<Mutex<Io>>,
}

impl<R: StorageRead> StorageRead for CountRead<R> {
    fn snapshot_cache_key(&self) -> Option<u128> {
        self.inner.snapshot_cache_key()
    }

    async fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> Result<GetManyResult, StorageError> {
        {
            let mut io = self.io.lock().expect("I/O counter lock");
            io.get_many_calls += 1;
            io.get_many_keys += requests
                .iter()
                .map(|request| request.keys.len() as u64)
                .sum::<u64>();
        }
        let result = self.inner.get_many(requests).await?;
        let mut io = self.io.lock().expect("I/O counter lock");
        for value in result.values.iter().flatten() {
            if let ProjectedValue::FullValue(value) = value {
                io.get_many_bytes += value.len() as u64;
            }
        }
        Ok(result)
    }

    async fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        options: BeginScanOptions,
    ) -> Result<ScanCursor<'_>, StorageError> {
        self.io.lock().expect("I/O counter lock").scans += 1;
        let order = options.order;
        let inner = self.inner.begin_scan(space, range.clone(), options).await?;
        ScanCursor::from_source(
            range,
            order,
            CountScan {
                inner,
                io: Arc::clone(&self.io),
            },
        )
    }
}

struct CountScan<'a> {
    inner: ScanCursor<'a>,
    io: Arc<Mutex<Io>>,
}

impl StorageScanSource for CountScan<'_> {
    fn next_page(
        &mut self,
        limit: usize,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<ScanChunk, StorageError>> + Send + '_>> {
        Box::pin(async move {
            let page = self.inner.next_page(limit).await?;
            let (entries, has_more) = page.into_parts();
            self.io.lock().expect("I/O counter lock").scan_rows += entries.len() as u64;
            Ok(ScanChunk::new(entries, has_more))
        })
    }
}

struct CountWrite<W> {
    inner: W,
    io: Arc<Mutex<Io>>,
}

impl<W: StorageWrite> StorageWrite for CountWrite<W> {
    async fn put_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        {
            let mut io = self.io.lock().expect("I/O counter lock");
            io.puts += entries.entries.len() as u64;
            io.logical_write_bytes += entries
                .entries
                .iter()
                .map(|entry| (entry.key.0.len() + entry.value.bytes.len()) as u64)
                .sum::<u64>();
        }
        self.inner.put_many(space, entries).await
    }

    async fn delete_many(&mut self, space: StorageSpace, keys: &[Key]) -> Result<(), StorageError> {
        self.io.lock().expect("I/O counter lock").deletes += keys.len() as u64;
        self.inner.delete_many(space, keys).await
    }

    async fn delete_range(
        &mut self,
        space: StorageSpace,
        range: KeyRange,
    ) -> Result<(), StorageError> {
        self.io.lock().expect("I/O counter lock").delete_ranges += 1;
        self.inner.delete_range(space, range).await
    }

    async fn commit(self) -> Result<CommitResult, StorageError> {
        let io = Arc::clone(&self.io);
        let result = self.inner.commit().await?;
        let mut stats = io.lock().expect("I/O counter lock");
        stats.commits += 1;
        stats.backend_puts += result.stats.put_entries;
        stats.backend_deletes += result.stats.deleted_entries;
        stats.backend_deleted_ranges += result.stats.deleted_ranges;
        stats.backend_storage_calls += result.stats.storage_calls;
        stats.backend_bytes += result.stats.written_bytes;
        Ok(result)
    }

    async fn rollback(self) -> Result<(), StorageError> {
        self.inner.rollback().await
    }
}

#[async_trait]
trait BenchBackend: Storage + Clone + Send + Sync + 'static {
    fn open(path: &Path) -> Self;
    async fn settle(&self);
}

#[async_trait]
impl BenchBackend for RocksDB {
    fn open(path: &Path) -> Self {
        RocksDB::open(path).expect("open RocksDB")
    }

    async fn settle(&self) {
        self.flush().expect("flush RocksDB");
    }
}

#[async_trait]
impl BenchBackend for SlateDB {
    fn open(path: &Path) -> Self {
        SlateDB::open(path).expect("open SlateDB")
    }

    async fn settle(&self) {
        self.flush().await.expect("flush SlateDB");
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Verify,
    Run,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Suite {
    Oltp,
    Vcs,
    Olap,
    File,
    All,
}

#[derive(Clone, Debug)]
struct Config {
    mode: Mode,
    backend: String,
    suite: Suite,
    rows: usize,
    delta: usize,
    samples: usize,
    history: usize,
    payload_bytes: usize,
}

#[derive(Clone, Debug, serde::Serialize)]
struct Metric {
    label: String,
    wall_us: u64,
    cpu_ticks: u64,
    alloc_bytes: u64,
    rss_hwm_kib: u64,
    disk_before: u64,
    disk_after: u64,
    io: Io,
    digest: String,
}

#[derive(Default)]
struct Results(BTreeMap<String, Vec<Metric>>);

trait MeasuredResultExt<T> {
    fn expect(self, message: &str) -> (T, Metric);
}

impl<T, E: std::fmt::Debug> MeasuredResultExt<T> for (Result<T, E>, Metric) {
    fn expect(self, message: &str) -> (T, Metric) {
        (self.0.expect(message), self.1)
    }
}

impl Results {
    fn push(&mut self, metric: Metric) {
        println!(
            "{}",
            serde_json::to_string(&metric).expect("serialize metric")
        );
        self.0.entry(metric.label.clone()).or_default().push(metric);
    }

    fn summarize(&self) {
        for (label, samples) in &self.0 {
            let mut wall = samples
                .iter()
                .map(|sample| sample.wall_us)
                .collect::<Vec<_>>();
            wall.sort_unstable();
            let p50 = percentile(&wall, 50);
            let p95 = percentile(&wall, 95);
            let digest = samples
                .first()
                .map(|sample| sample.digest.as_str())
                .unwrap_or("");
            assert!(samples.iter().all(|sample| sample.digest == digest));
            println!(
                "{}",
                serde_json::json!({
                    "event":"summary", "label":label, "samples":samples.len(),
                    "p50_us":p50, "p95_us":p95, "digest":digest,
                    "cpu_ticks":samples.iter().map(|sample| sample.cpu_ticks).sum::<u64>(),
                    "alloc_bytes":samples.iter().map(|sample| sample.alloc_bytes).sum::<u64>(),
                    "max_rss_hwm_kib":samples.iter().map(|sample| sample.rss_hwm_kib).max().unwrap_or(0),
                    "backend_written_bytes":samples.iter().map(|sample| sample.io.backend_bytes).sum::<u64>(),
                })
            );
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let config = parse_config();
    println!(
        "{}",
        serde_json::json!({"event":"config","mode":format!("{:?}",config.mode),"backend":config.backend,"suite":format!("{:?}",config.suite),"rows":config.rows,"delta":config.delta,"samples":config.samples,"history":config.history,"payload_bytes":config.payload_bytes})
    );
    let mut results = Results::default();
    let samples = if config.mode == Mode::Verify {
        1
    } else {
        config.samples
    };
    for sample in 0..samples {
        match config.backend.as_str() {
            "rocksdb" => run_sample::<RocksDB>(&config, sample, &mut results).await,
            "slatedb" => run_sample::<SlateDB>(&config, sample, &mut results).await,
            other => panic!("backend must be rocksdb or slatedb, got {other}"),
        }
    }
    results.summarize();
}

fn parse_config() -> Config {
    let mut args = std::env::args().skip(1);
    let mode = match args.next().as_deref() {
        Some("verify") => Mode::Verify,
        Some("run") => Mode::Run,
        _ => panic!("usage: <verify|run> <rocksdb|slatedb> <oltp|vcs|olap|file|all> N D samples"),
    };
    let backend = args.next().expect("backend");
    let suite = match args.next().as_deref() {
        Some("oltp") => Suite::Oltp,
        Some("vcs") => Suite::Vcs,
        Some("olap") => Suite::Olap,
        Some("file") => Suite::File,
        Some("all") => Suite::All,
        _ => panic!("suite must be oltp, vcs, olap, file, or all"),
    };
    let rows = args.next().expect("N").parse().expect("N is an integer");
    let delta = args.next().expect("D").parse().expect("D is an integer");
    let samples = args
        .next()
        .expect("samples")
        .parse()
        .expect("samples is an integer");
    assert!(rows > 0 && delta > 0 && samples > 0);
    Config {
        mode,
        backend,
        suite,
        rows,
        delta,
        samples,
        history: env_usize("LIX_BENCH_HISTORY", 10),
        payload_bytes: env_usize("LIX_BENCH_PAYLOAD_BYTES", 1 << 20),
    }
}

async fn run_sample<S: BenchBackend>(config: &Config, sample: usize, results: &mut Results) {
    let temporary = tempfile::Builder::new()
        .prefix("lix-schema-v1-public-")
        .tempdir()
        .expect("temporary benchmark directory");
    let path = temporary.path().join("db");
    let raw = S::open(&path);
    let storage = CountStorage::new(raw);
    let session = lix::open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open or initialize repository");
    register_schema(&session).await;
    seed_rows(&session, config.rows).await;
    storage.inner.settle().await;

    if matches!(config.suite, Suite::Oltp | Suite::All) {
        run_oltp(config, sample, &session, &storage, &path, results).await;
    }
    if matches!(config.suite, Suite::Olap | Suite::All) {
        run_olap(config, sample, &session, &storage, &path, results).await;
    }
    if matches!(config.suite, Suite::Vcs | Suite::All) {
        run_vcs(config, sample, &session, &storage, &path, results).await;
    }
    if matches!(config.suite, Suite::File | Suite::All) {
        run_file(config, sample, &session, &storage, &path, results).await;
    }

    let final_digest = digest_result(
        &session
            .execute(
                "SELECT id, uid, seq, score, enabled, name, payload, at FROM qualification_row ORDER BY id",
                &[],
            )
            .await
            .expect("final row digest"),
    );
    println!(
        "{}",
        serde_json::json!({"event":"sample_complete","sample":sample,"final_digest":final_digest,"settled_bytes":disk_bytes(&path)})
    );
    session.close().await.expect("close session");
    drop(session);
    storage.inner.settle().await;
    drop(storage);

    let reopened = CountStorage::new(S::open(&path));
    let reopened_session = lix::open_lix()
        .with_storage(reopened.clone())
        .await
        .expect("cold reopen main session");
    let reopened_result = measure(
        format!("cold_reopen/{:?}/N{}", config.suite, config.rows),
        &reopened,
        &path,
        reopened_session.execute("SELECT COUNT(*) AS n FROM qualification_row", &[]),
    )
    .await
    .expect("cold reopen query");
    results.push(result_metric(&reopened_result.0, reopened_result.1));
    reopened_session
        .close()
        .await
        .expect("close reopened session");
}

async fn register_schema<S>(session: &Lix<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let schema = serde_json::json!({
        "$schema":"https://lix.dev/schema-v1.json",
        "key":SCHEMA_KEY,
        "columns":[
            {"name":"id","type":"text","nullable":false},
            {"name":"uid","type":"uuid","nullable":false},
            {"name":"seq","type":"int8","nullable":false},
            {"name":"score","type":"float8","nullable":false},
            {"name":"enabled","type":"boolean","nullable":false},
            {"name":"name","type":"text","nullable":false},
            {"name":"payload","type":"jsonb","nullable":false},
            {"name":"at","type":"timestamptz","nullable":false}
        ],
        "primary_key":["id"]
    });
    session
        .execute(
            "INSERT INTO lix_registered_schema (schema_key, value) VALUES ($1, CAST($2 AS JSONB))",
            &[
                Value::Text(SCHEMA_KEY.to_owned()),
                Value::Text(schema.to_string()),
            ],
        )
        .await
        .expect("register seven-type schema");
}

async fn seed_rows<S>(session: &Lix<S>, rows: usize)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    for start in (0..rows).step_by(500) {
        let end = (start + 500).min(rows);
        let batch = (start..end).map(insert_statement).collect::<Vec<_>>();
        let result = session
            .execute_batch(&batch)
            .await
            .expect("seed Schema-v1 rows");
        assert_eq!(
            result.iter().map(ExecuteResult::rows_affected).sum::<u64>() as usize,
            end - start
        );
    }
}

fn insert_statement(index: usize) -> ExecuteBatchStatement {
    ExecuteBatchStatement {
        label: None,
        sql: "INSERT INTO qualification_row (id, uid, seq, score, enabled, name, payload, at) VALUES ($1, $2, $3, $4, $5, $6, CAST($7 AS JSONB), $8)".to_owned(),
        params: row_params(index),
    }
}

fn row_params(index: usize) -> Vec<Value> {
    vec![
        Value::Text(row_id(index)),
        Value::Text(row_uid(index)),
        Value::Integer(index as i64),
        Value::Real(index as f64 / 10.0),
        Value::Boolean(index.is_multiple_of(2)),
        Value::Text(format!("row-{index:08}")),
        Value::Text(format!("{{\"ordinal\":{index},\"group\":{}}}", index % 17)),
        Value::Text("2025-01-01T00:00:00Z".to_owned()),
    ]
}

fn row_id(index: usize) -> String {
    uuid::Uuid::from_u128(index as u128 + 1).to_string()
}

fn row_uid(index: usize) -> String {
    uuid::Uuid::from_u128((1_u128 << 64) + index as u128 + 1).to_string()
}

async fn run_oltp<S: Storage + Clone + Send + Sync + 'static>(
    config: &Config,
    sample: usize,
    session: &Lix<CountStorage<S>>,
    storage: &CountStorage<S>,
    path: &Path,
    results: &mut Results,
) {
    let hit = measure(
        label(config, sample, "oltp/point_hit"), storage, path,
        session.execute("SELECT id, uid, seq, score, enabled, name, payload, at FROM qualification_row WHERE id = $1", &[Value::Text(row_id(config.rows / 2))]),
    ).await.expect("point hit");
    assert_eq!(hit.0.rows().len(), 1);
    results.push(result_metric(&hit.0, hit.1));

    let miss = measure(
        label(config, sample, "oltp/point_miss"),
        storage,
        path,
        session.execute(
            "SELECT id FROM qualification_row WHERE id = $1",
            &[Value::Text(
                "ffffffff-ffff-ffff-ffff-ffffffffffff".to_owned(),
            )],
        ),
    )
    .await
    .expect("point miss");
    assert!(miss.0.rows().is_empty());
    results.push(result_metric(&miss.0, miss.1));

    let insert = measure(
        label(config, sample, "oltp/insert"),
        storage,
        path,
        session.execute_batch(&[insert_statement(config.rows + 1)]),
    )
    .await
    .expect("single insert");
    results.push(metric_for_batch(insert.1, &insert.0));

    for (name, count) in [
        ("update_d1", 1usize),
        ("update_d10", 10.min(config.rows)),
        ("update_1pct", config.rows.div_ceil(100).max(1)),
        ("update_d", config.delta.min(config.rows)),
    ] {
        let batch = (0..count)
            .map(|index| ExecuteBatchStatement {
                label: None,
                sql: "UPDATE qualification_row SET score = $1 WHERE id = $2".to_owned(),
                params: vec![
                    Value::Real(10_000.0 + index as f64),
                    Value::Text(row_id(index)),
                ],
            })
            .collect::<Vec<_>>();
        let measured = measure(
            label(config, sample, &format!("oltp/{name}")),
            storage,
            path,
            session.execute_batch(&batch),
        )
        .await
        .expect("update batch");
        results.push(metric_for_batch(measured.1, &measured.0));
    }

    let range = measure(
        label(config, sample, "oltp/range"),
        storage,
        path,
        session.execute(
            "SELECT id, seq, name FROM qualification_row WHERE seq >= $1 AND seq < $2 ORDER BY seq",
            &[
                Value::Integer((config.rows / 4) as i64),
                Value::Integer((config.rows / 4 + config.delta.min(config.rows)) as i64),
            ],
        ),
    )
    .await
    .expect("range");
    results.push(result_metric(&range.0, range.1));

    let scan = measure(
        label(config, sample, "oltp/full_scan"),
        storage,
        path,
        session.execute(
            "SELECT id, uid, seq, score, enabled, name, payload, at FROM qualification_row ORDER BY id",
            &[],
        ),
    )
    .await
    .expect("full scan");
    results.push(result_metric(&scan.0, scan.1));

    let transaction = (0..config.delta.min(config.rows))
        .map(|index| ExecuteBatchStatement {
            label: None,
            sql: "UPDATE qualification_row SET name = $1 WHERE id = $2".to_owned(),
            params: vec![
                Value::Text(format!("tx-{index}")),
                Value::Text(row_id(index)),
            ],
        })
        .collect::<Vec<_>>();
    let measured = measure(
        label(config, sample, "oltp/transaction"),
        storage,
        path,
        session.execute_batch(&transaction),
    )
    .await
    .expect("transaction batch");
    results.push(metric_for_batch(measured.1, &measured.0));
}

async fn run_olap<S: Storage + Clone + Send + Sync + 'static>(
    config: &Config,
    sample: usize,
    session: &Lix<CountStorage<S>>,
    storage: &CountStorage<S>,
    path: &Path,
    results: &mut Results,
) {
    for (name, sql) in [
        (
            "olap/projected_scan",
            "SELECT id, seq, enabled FROM qualification_row ORDER BY id",
        ),
        (
            "olap/full_scan",
            "SELECT id, uid, seq, score, enabled, name, payload, at FROM qualification_row ORDER BY id",
        ),
        (
            "olap/aggregate",
            "SELECT enabled, COUNT(*) AS n, SUM(seq) AS total FROM qualification_row GROUP BY enabled ORDER BY enabled",
        ),
    ] {
        let measured = measure(
            label(config, sample, name),
            storage,
            path,
            session.execute(sql, &[]),
        )
        .await
        .expect("OLAP query");
        results.push(result_metric(&measured.0, measured.1));
    }
}

async fn run_vcs<S: Storage + Clone + Send + Sync + 'static>(
    config: &Config,
    sample: usize,
    session: &Lix<CountStorage<S>>,
    storage: &CountStorage<S>,
    path: &Path,
    results: &mut Results,
) {
    let before = active_commit(session).await;
    for generation in 0..config.history {
        session
            .execute(
                "UPDATE qualification_row SET score = $1 WHERE id = $2",
                &[
                    Value::Real(1000.0 + generation as f64),
                    Value::Text(row_id(0)),
                ],
            )
            .await
            .expect("history update");
    }
    let after = active_commit(session).await;

    for (name, sql, params) in [
        (
            "vcs/history",
            "SELECT COUNT(*) AS n FROM lix_commit",
            Vec::new(),
        ),
        (
            "vcs/diff",
            "SELECT COUNT(*) AS n FROM lix_diff($1, $2)",
            vec![Value::Text(before.clone()), Value::Text(after.clone())],
        ),
        (
            "vcs/working_diff",
            "SELECT COUNT(*) AS n FROM lix_working_diff",
            Vec::new(),
        ),
    ] {
        let measured = measure(
            label(config, sample, name),
            storage,
            path,
            session.execute(sql, &params),
        )
        .await
        .expect("VCS SQL");
        results.push(result_metric(&measured.0, measured.1));
    }

    let checkpoint = measure(
        label(config, sample, "vcs/checkpoint"),
        storage,
        path,
        session.create_checkpoint(),
    )
    .await
    .expect("checkpoint");
    results.push(metric_from_marker(checkpoint.1, "checkpoint-ok"));

    let branch = measure(
        label(config, sample, "vcs/branch"),
        storage,
        path,
        session.create_branch(CreateBranchOptions {
            id: None,
            name: format!("qualification-{sample}"),
            from_commit_id: None,
        }),
    )
    .await
    .expect("create branch");
    let branch_id = branch.0.id.clone();
    results.push(metric_from_marker(branch.1, "branch-ok"));
    let main_branch_id = active_branch(session).await;
    session
        .switch_branch(SwitchBranchOptions {
            branch_id: branch_id.clone(),
        })
        .await
        .expect("switch branch");
    session
        .execute(
            "UPDATE qualification_row SET name = 'branch-edit' WHERE id = $1",
            &[Value::Text(row_id(1.min(config.rows - 1)))],
        )
        .await
        .expect("branch edit");
    session
        .switch_branch(SwitchBranchOptions {
            branch_id: main_branch_id,
        })
        .await
        .expect("switch main");
    let merge = measure(
        label(config, sample, "vcs/merge"),
        storage,
        path,
        session.merge_branch(MergeBranchOptions {
            source_branch_id: branch_id,
        }),
    )
    .await
    .expect("merge branch");
    results.push(metric_from_marker(merge.1, "merge-ok"));

    session
        .execute(
            "UPDATE qualification_row SET name = 'undo-target' WHERE id = $1",
            &[Value::Text(row_id(0))],
        )
        .await
        .expect("undo target");
    let undo = measure(
        label(config, sample, "vcs/undo"),
        storage,
        path,
        session.undo(),
    )
    .await
    .expect("undo");
    results.push(metric_from_marker(undo.1, "undo-ok"));
    let redo = measure(
        label(config, sample, "vcs/redo"),
        storage,
        path,
        session.redo(),
    )
    .await
    .expect("redo");
    results.push(metric_from_marker(redo.1, "redo-ok"));
}

async fn run_file<S: Storage + Clone + Send + Sync + 'static>(
    config: &Config,
    sample: usize,
    session: &Lix<CountStorage<S>>,
    storage: &CountStorage<S>,
    path: &Path,
    results: &mut Results,
) {
    let payload = deterministic_bytes(config.payload_bytes, 0x1234);
    let insert = measure(
        label(config, sample, "file/insert"),
        storage,
        path,
        session.execute(
            "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
            &[
                Value::Text(FILE_PATH.to_owned()),
                Value::Blob(payload.clone().into()),
            ],
        ),
    )
    .await
    .expect("file insert");
    results.push(result_metric(&insert.0, insert.1));
    let read = measure(
        label(config, sample, "file/read"),
        storage,
        path,
        session.execute(
            "SELECT content FROM lix_file WHERE path = $1",
            &[Value::Text(FILE_PATH.to_owned())],
        ),
    )
    .await
    .expect("file read");
    let read_bytes = single_blob(&read.0, "file read");
    assert_eq!(read_bytes, payload.as_slice());
    results.push(metric_from_bytes(read.1, read_bytes));

    let mut edited = payload.clone();
    let changed = edited.len().min(1 << 20);
    edited[..changed].fill(0xa5);
    let update = measure(
        label(config, sample, "file/update"),
        storage,
        path,
        session.execute(
            "UPDATE lix_file SET content = $1 WHERE path = $2",
            &[
                Value::Blob(edited.clone().into()),
                Value::Text(FILE_PATH.to_owned()),
            ],
        ),
    )
    .await
    .expect("file update");
    results.push(result_metric(&update.0, update.1));

    session
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/qualification/delete-control.bin', CAST('delete-control' AS BYTEA))",
            &[],
        )
        .await
        .expect("seed file delete control");
    let delete_control = measure(
        label(config, sample, "file/delete"),
        storage,
        path,
        session.execute(
            "DELETE FROM lix_file WHERE path = '/qualification/delete-control.bin'",
            &[],
        ),
    )
    .await
    .expect("file delete control");
    results.push(result_metric(&delete_control.0, delete_control.1));

    let main_branch_id = active_branch(session).await;
    let branch = session
        .create_branch(CreateBranchOptions {
            id: None,
            name: format!("file-share-{sample}"),
            from_commit_id: None,
        })
        .await
        .expect("file branch");
    session
        .switch_branch(SwitchBranchOptions {
            branch_id: branch.id,
        })
        .await
        .expect("switch to file branch");
    let branch_read = measure(
        label(config, sample, "file/branch_read"),
        storage,
        path,
        session.execute(
            "SELECT content FROM lix_file WHERE path = $1",
            &[Value::Text(FILE_PATH.to_owned())],
        ),
    )
    .await
    .expect("branch read");
    let branch_bytes = single_blob(&branch_read.0, "branch file");
    assert_eq!(branch_bytes, edited.as_slice());
    results.push(metric_from_bytes(branch_read.1, branch_bytes));
    session
        .switch_branch(SwitchBranchOptions {
            branch_id: main_branch_id,
        })
        .await
        .expect("switch back from file branch");

    let delete = measure(
        label(config, sample, "file/delete_after_branch"),
        storage,
        path,
        session.execute(
            "DELETE FROM lix_file WHERE path = $1",
            &[Value::Text(FILE_PATH.to_owned())],
        ),
    )
    .await
    .expect("file delete");
    results.push(result_metric(&delete.0, delete.1));
    assert!(
        session
            .execute(
                "SELECT content FROM lix_file WHERE path = $1",
                &[Value::Text(FILE_PATH.to_owned())],
            )
            .await
            .expect("deleted file lookup")
            .rows()
            .is_empty()
    );
}

fn single_blob<'a>(result: &'a ExecuteResult, context: &str) -> &'a [u8] {
    let rows = result.rows();
    assert_eq!(rows.len(), 1, "{context} must return one row");
    match &rows[0].values()[0] {
        Value::Blob(value) => value.as_ref(),
        value => panic!("{context} returned non-blob value {value:?}"),
    }
}

async fn active_commit<S>(session: &Lix<S>) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute("SELECT lix_active_branch_commit_id() AS id", &[])
        .await
        .expect("active commit")
        .rows()[0]
        .get::<String>("id")
        .expect("commit id")
        .to_owned()
}

async fn active_branch<S>(session: &Lix<S>) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute("SELECT lix_active_branch_id() AS id", &[])
        .await
        .expect("active branch")
        .rows()[0]
        .get::<String>("id")
        .expect("branch id")
        .to_owned()
}

async fn measure<T, F, S>(
    label: String,
    storage: &CountStorage<S>,
    path: &Path,
    future: F,
) -> (T, Metric)
where
    F: IntoFuture<Output = T>,
{
    storage.reset();
    let disk_before = disk_bytes(path);
    let cpu_before = cpu_ticks();
    ALLOC_BYTES.store(0, Ordering::Relaxed);
    ALLOC_ON.store(true, Ordering::Release);
    let started = Instant::now();
    let value = future.into_future().await;
    let wall_us = started.elapsed().as_micros() as u64;
    ALLOC_ON.store(false, Ordering::Release);
    let metric = Metric {
        label,
        wall_us,
        cpu_ticks: cpu_ticks().saturating_sub(cpu_before),
        alloc_bytes: ALLOC_BYTES.load(Ordering::Relaxed),
        rss_hwm_kib: rss_hwm_kib(),
        disk_before,
        disk_after: disk_bytes(path),
        io: storage.snapshot(),
        digest: String::new(),
    };
    (value, metric)
}

fn metric_for_batch(mut metric: Metric, results: &[ExecuteResult]) -> Metric {
    let mut hasher = Hasher::new();
    for result in results {
        hasher.update(digest_result(result).as_bytes());
    }
    metric.digest = hasher.finalize().to_hex().to_string();
    metric
}

fn metric_from_marker(mut metric: Metric, marker: &str) -> Metric {
    metric.digest = blake3::hash(marker.as_bytes()).to_hex().to_string();
    metric
}

fn metric_from_bytes(mut metric: Metric, value: &[u8]) -> Metric {
    metric.digest = blake3::hash(value).to_hex().to_string();
    metric
}

trait DigestOutput {
    fn with_digest(self, digest: String) -> Self;
}

impl DigestOutput for Metric {
    fn with_digest(mut self, digest: String) -> Self {
        self.digest = digest;
        self
    }
}

fn digest_result(result: &ExecuteResult) -> String {
    let mut hasher = Hasher::new();
    hasher.update(b"lix-schema-v1-public-qualification-v1\0");
    hasher.update(&(result.columns().len() as u64).to_le_bytes());
    for column in result.columns() {
        feed(&mut hasher, column.as_bytes());
    }
    hasher.update(&(result.rows().len() as u64).to_le_bytes());
    for row in result.rows() {
        for value in row.values() {
            feed(
                &mut hasher,
                &serde_json::to_vec(value).expect("serialize public value"),
            );
        }
    }
    hasher.finalize().to_hex().to_string()
}

fn feed(hasher: &mut Hasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

fn label(config: &Config, _sample: usize, operation: &str) -> String {
    format!("{operation}/N{}/D{}", config.rows, config.delta)
}

fn deterministic_bytes(size: usize, seed: u64) -> Vec<u8> {
    let mut bytes = vec![0; size];
    let mut state = seed;
    for chunk in bytes.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        chunk.copy_from_slice(&state.to_le_bytes()[..chunk.len()]);
    }
    bytes
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    sorted[((sorted.len() - 1) * percentile).div_ceil(100)]
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn cpu_ticks() -> u64 {
    std::fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|text| {
            text.rsplit_once(") ").and_then(|(_, rest)| {
                let fields = rest.split_whitespace().collect::<Vec<_>>();
                fields
                    .get(11)
                    .and_then(|value| value.parse::<u64>().ok())
                    .zip(fields.get(12).and_then(|value| value.parse::<u64>().ok()))
            })
        })
        .map(|(user, system)| user + system)
        .unwrap_or(0)
}

fn rss_hwm_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|text| {
            text.lines()
                .find(|line| line.starts_with("VmHWM:"))
                .and_then(|line| line.split_whitespace().nth(1))
                .and_then(|value| value.parse().ok())
        })
        .unwrap_or(0)
}

fn disk_bytes(path: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                disk_bytes(&path)
            } else {
                entry.metadata().map(|metadata| metadata.len()).unwrap_or(0)
            }
        })
        .sum()
}

// Every public result metric gets its digest at the call site. This helper is
// intentionally explicit so a future workload cannot accidentally time a
// query while discarding its semantic result.
fn result_metric(result: &ExecuteResult, metric: Metric) -> Metric {
    metric.with_digest(digest_result(result))
}
