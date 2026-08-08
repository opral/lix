//! Benchmark-only external SQLite OLTP comparator.
//!
//! This executable does not restore the deleted Lix SQLite adapter. SQLite is
//! opened directly through rusqlite as an external raw CRUD floor.

#![allow(clippy::large_futures)]

use std::alloc::GlobalAlloc;
use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use lix::integration::{Engine, SessionContext};
use lix::storage::{
    BeginScanOptions, CommitResult, GetManyRequest, GetManyResult, Key, KeyRange, PutBatch,
    ReadOptions, ScanCursor, Storage, StorageError, StorageRead, StorageSpace, StorageWrite,
    WriteOptions,
};
use lix::{PreparedDmlParameterBatch, Value};
use lix_storage_rocksdb::RocksDB;
use rusqlite::{Connection, TransactionBehavior, params};

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

struct CountingAllocator;

static PROFILE_ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static PROFILE_ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);
static PROFILE_ALLOCATION_ENABLED: AtomicBool = AtomicBool::new(false);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let pointer = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !pointer.is_null() && PROFILE_ALLOCATION_ENABLED.load(Ordering::Relaxed) {
            PROFILE_ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            PROFILE_ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
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
        if !replacement.is_null()
            && new_size >= layout.size()
            && PROFILE_ALLOCATION_ENABLED.load(Ordering::Relaxed)
        {
            PROFILE_ALLOCATED_BYTES.fetch_add((new_size - layout.size()) as u64, Ordering::Relaxed);
            PROFILE_ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        replacement
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PathKind {
    Lix,
    Sqlite,
}

impl PathKind {
    fn parse(value: &str) -> Self {
        match value {
            "lix" => Self::Lix,
            "sqlite" => Self::Sqlite,
            other => panic!("unknown path '{other}', expected lix or sqlite"),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Lix => "lix_a12_public_sql_rocksdb",
            Self::Sqlite => "standalone_sqlite_wal_full",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Setup,
    Run,
}

impl Mode {
    fn parse(value: &str) -> Self {
        match value {
            "setup" => Self::Setup,
            "run" => Self::Run,
            other => panic!("unknown mode '{other}', expected setup or run"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Operation {
    Insert,
    PointRead,
    RangeRead,
    UpdateOnePercent,
    UpdateTenPercent,
    Delete,
    Atomic18,
    Upsert,
    Returning,
}

impl Operation {
    fn parse(value: &str) -> Self {
        match value {
            "insert" => Self::Insert,
            "point_read" => Self::PointRead,
            "range_read" => Self::RangeRead,
            "update_1pct" => Self::UpdateOnePercent,
            "update_10pct" => Self::UpdateTenPercent,
            "delete" => Self::Delete,
            "atomic_18" => Self::Atomic18,
            "upsert" => Self::Upsert,
            "returning" => Self::Returning,
            other => panic!("unknown operation '{other}'"),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::PointRead => "point_read",
            Self::RangeRead => "range_read",
            Self::UpdateOnePercent => "update_1pct",
            Self::UpdateTenPercent => "update_10pct",
            Self::Delete => "delete",
            Self::Atomic18 => "atomic_18",
            Self::Upsert => "upsert",
            Self::Returning => "returning",
        }
    }

    const fn starts_empty(self) -> bool {
        matches!(self, Self::Insert)
    }
}

#[derive(Clone, Debug, Default)]
struct BackendStats {
    begin_reads: u64,
    begin_writes: u64,
    get_calls: u64,
    get_keys: u64,
    scan_calls: u64,
    put_calls: u64,
    put_entries: u64,
    put_bytes: u64,
    delete_calls: u64,
    delete_entries: u64,
    delete_range_calls: u64,
    commits: u64,
    rollbacks: u64,
}

#[derive(Clone)]
struct CountingStorage<S> {
    inner: S,
    stats: Arc<Mutex<BackendStats>>,
}

struct CountingRead<R> {
    inner: R,
    stats: Arc<Mutex<BackendStats>>,
}

struct CountingWrite<W> {
    inner: W,
    stats: Arc<Mutex<BackendStats>>,
}

impl<S> CountingStorage<S> {
    fn new(inner: S) -> (Self, Arc<Mutex<BackendStats>>) {
        let stats = Arc::new(Mutex::new(BackendStats::default()));
        (
            Self {
                inner,
                stats: Arc::clone(&stats),
            },
            stats,
        )
    }
}

impl<S> Storage for CountingStorage<S>
where
    S: Storage,
{
    type Read<'a>
        = CountingRead<S::Read<'a>>
    where
        Self: 'a;
    type Write<'a>
        = CountingWrite<S::Write<'a>>
    where
        Self: 'a;

    fn begin_read(
        &self,
        opts: ReadOptions,
    ) -> impl Future<Output = Result<Self::Read<'_>, StorageError>> + Send {
        async move {
            self.stats.lock().expect("stats mutex").begin_reads += 1;
            Ok(CountingRead {
                inner: self.inner.begin_read(opts).await?,
                stats: Arc::clone(&self.stats),
            })
        }
    }

    fn begin_write(
        &self,
        opts: WriteOptions,
    ) -> impl Future<Output = Result<Self::Write<'_>, StorageError>> + Send {
        async move {
            self.stats.lock().expect("stats mutex").begin_writes += 1;
            Ok(CountingWrite {
                inner: self.inner.begin_write(opts).await?,
                stats: Arc::clone(&self.stats),
            })
        }
    }
}

impl<R> StorageRead for CountingRead<R>
where
    R: StorageRead,
{
    fn snapshot_cache_key(&self) -> Option<u128> {
        self.inner.snapshot_cache_key()
    }

    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
        self.stats.lock().expect("stats mutex").get_calls += 1;
        self.stats.lock().expect("stats mutex").get_keys += requests
            .iter()
            .map(|request| request.keys.len() as u64)
            .sum::<u64>();
        self.inner.get_many(requests)
    }

    fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> impl Future<Output = Result<ScanCursor<'_>, StorageError>> + Send {
        self.stats.lock().expect("stats mutex").scan_calls += 1;
        self.inner.begin_scan(space, range, opts)
    }
}

impl<W> StorageWrite for CountingWrite<W>
where
    W: StorageWrite,
{
    fn put_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        {
            let mut stats = self.stats.lock().expect("stats mutex");
            stats.put_calls += 1;
            stats.put_entries += entries.entries.len() as u64;
            stats.put_bytes += entries
                .entries
                .iter()
                .map(|entry| (entry.key.0.len() + entry.value.bytes.len()) as u64)
                .sum::<u64>();
        }
        self.inner.put_many(space, entries)
    }

    fn delete_many(
        &mut self,
        space: StorageSpace,
        keys: &[Key],
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        {
            let mut stats = self.stats.lock().expect("stats mutex");
            stats.delete_calls += 1;
            stats.delete_entries += keys.len() as u64;
        }
        self.inner.delete_many(space, keys)
    }

    fn delete_range(
        &mut self,
        space: StorageSpace,
        range: KeyRange,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.stats.lock().expect("stats mutex").delete_range_calls += 1;
        self.inner.delete_range(space, range)
    }

    fn commit(self) -> impl Future<Output = Result<CommitResult, StorageError>> + Send {
        self.stats.lock().expect("stats mutex").commits += 1;
        self.inner.commit()
    }

    fn rollback(self) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.stats.lock().expect("stats mutex").rollbacks += 1;
        self.inner.rollback()
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct ProcessIo {
    syscr: u64,
    syscw: u64,
    read_bytes: u64,
    write_bytes: u64,
}

impl ProcessIo {
    fn delta(self, before: Self) -> Self {
        Self {
            syscr: self.syscr.saturating_sub(before.syscr),
            syscw: self.syscw.saturating_sub(before.syscw),
            read_bytes: self.read_bytes.saturating_sub(before.read_bytes),
            write_bytes: self.write_bytes.saturating_sub(before.write_bytes),
        }
    }
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    assert_eq!(
        args.len(),
        7,
        "usage: sqlite_oltp_comparator <setup|run> <lix|sqlite> <path> <rows> <operation> <batch_size>"
    );
    let mode = Mode::parse(&args[1]);
    let path_kind = PathKind::parse(&args[2]);
    let path = PathBuf::from(&args[3]);
    let rows = args[4].parse::<usize>().expect("rows must be an integer");
    let operation = Operation::parse(&args[5]);
    let batch_size = args[6]
        .parse::<usize>()
        .expect("batch size must be an integer");
    assert!(rows > 0 && batch_size > 0);

    println!(
        "oltp_comparator_contract,path={},mode={:?},rows={},operation={},batch_size={},sqlite_adapter_reintroduced=false,sqlite_journal=wal,sqlite_synchronous=full,connection_threads=1,setup_excluded=true,current_lix_semantics=branch_history_enabled,forktree_sql_integrated=false",
        path_kind.label(),
        mode,
        rows,
        operation.label(),
        batch_size
    );

    match (mode, path_kind) {
        (Mode::Setup, PathKind::Lix) => runtime().block_on(setup_lix(&path, rows, operation)),
        (Mode::Run, PathKind::Lix) => {
            runtime().block_on(run_lix(&path, rows, operation, batch_size))
        }
        (Mode::Setup, PathKind::Sqlite) => setup_sqlite(&path, rows, operation),
        (Mode::Run, PathKind::Sqlite) => run_sqlite(&path, rows, operation, batch_size),
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build comparator runtime")
}

fn base_rows(rows: usize) -> BTreeMap<String, String> {
    (0..rows)
        .map(|index| (row_key(index), row_value(index, "base")))
        .collect()
}

fn setup_rows(rows: usize, operation: Operation) -> BTreeMap<String, String> {
    if operation.starts_empty() {
        BTreeMap::new()
    } else {
        base_rows(rows)
    }
}

fn row_key(index: usize) -> String {
    format!("row-{index:09}")
}

fn new_key(index: usize) -> String {
    format!("new-{index:09}")
}

fn row_value(index: usize, lane: &str) -> String {
    format!(
        r#"{{"ordinal":{index},"lane":"{lane}","payload":"{:032}"}}"#,
        index % 10_000
    )
}

fn update_count(rows: usize, percent: usize) -> usize {
    (rows.saturating_mul(percent) / 100).max(1).min(rows)
}

fn target_keys(rows: usize, count: usize) -> Vec<usize> {
    if count >= rows {
        return (0..rows).collect();
    }
    let mut result = (0..count)
        .map(|ordinal| ordinal.saturating_mul(rows) / count)
        .collect::<Vec<_>>();
    result.sort_unstable();
    result.dedup();
    result
}

fn expected_after(rows: usize, operation: Operation) -> BTreeMap<String, String> {
    let mut expected = setup_rows(rows, operation);
    match operation {
        Operation::Insert => expected = base_rows(rows),
        Operation::PointRead | Operation::RangeRead => {}
        Operation::UpdateOnePercent => {
            for index in target_keys(rows, update_count(rows, 1)) {
                expected.insert(row_key(index), row_value(index, "update-1"));
            }
        }
        Operation::UpdateTenPercent | Operation::Returning => {
            for index in target_keys(rows, update_count(rows, 10)) {
                expected.insert(row_key(index), row_value(index, "update-10"));
            }
        }
        Operation::Delete => expected.clear(),
        Operation::Atomic18 => {
            for index in 0..6 {
                expected.insert(row_key(index), row_value(index, "atomic-update"));
            }
            for index in 6..12 {
                expected.remove(&row_key(index));
            }
            for index in 0..6 {
                expected.insert(new_key(index), row_value(index, "atomic-insert"));
            }
        }
        Operation::Upsert => {
            for index in 0..rows {
                if index % 2 == 0 {
                    expected.insert(row_key(index), row_value(index, "upsert-update"));
                } else {
                    expected.insert(new_key(index), row_value(index, "upsert-insert"));
                }
            }
        }
    }
    expected
}

fn digest_rows<'a>(rows: impl IntoIterator<Item = (&'a str, &'a str)>) -> String {
    let mut hasher = blake3::Hasher::new();
    let mut count = 0_u64;
    for (key, value) in rows {
        hasher.update(&(key.len() as u64).to_le_bytes());
        hasher.update(key.as_bytes());
        hasher.update(&(value.len() as u64).to_le_bytes());
        hasher.update(value.as_bytes());
        count += 1;
    }
    hasher.update(&count.to_le_bytes());
    hasher.finalize().to_hex().to_string()
}

fn map_digest(rows: &BTreeMap<String, String>) -> String {
    digest_rows(
        rows.iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    )
}

fn returned_expected(rows: usize) -> BTreeMap<String, String> {
    target_keys(rows, update_count(rows, 10))
        .into_iter()
        .map(|index| (row_key(index), row_value(index, "update-10")))
        .collect()
}

async fn setup_lix(path: &Path, rows: usize, operation: Operation) {
    std::fs::create_dir_all(path).expect("create Lix comparator directory");
    let database = RocksDB::open(path).expect("open setup RocksDB");
    Engine::initialize(database.clone())
        .await
        .expect("initialize setup engine");
    let engine = Engine::new(database.clone())
        .await
        .expect("open setup engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("open setup session");
    register_schema(&session).await;
    let initial = setup_rows(rows, operation);
    if !initial.is_empty() {
        lix_apply_batches(&session, Operation::Insert, &initial, rows.max(1)).await;
    }
    drop(session);
    drop(engine);
    database.flush().expect("flush Lix setup");
    drop(database);
    println!(
        "oltp_comparator_setup,path=lix_a12_public_sql_rocksdb,rows={},operation={},digest={},disk_bytes={}",
        rows,
        operation.label(),
        map_digest(&initial),
        directory_bytes(path)
    );
}

async fn register_schema<S>(session: &SessionContext<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let schema = serde_json::json!({
        "x-lix-key": "oltp_compare_row",
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "required": ["id", "value"],
        "properties": {
            "id": { "type": "string" },
            "value": { "type": "string" }
        },
        "additionalProperties": false
    });
    let affected = session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (lix_json($1), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register comparator schema")
        .rows_affected();
    assert_eq!(affected, 1);
}

async fn run_lix(path: &Path, rows: usize, operation: Operation, batch_size: usize) {
    let database = RocksDB::open(path).expect("open measured RocksDB");
    let (storage, stats) = CountingStorage::new(database.clone());
    let engine = Engine::new(storage).await.expect("open measured engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("open measured session");
    warm_lix_operation(&session, rows, operation).await;
    *stats.lock().expect("stats mutex") = BackendStats::default();

    let disk_before = directory_bytes(path);
    let io_before = process_io();
    let rss_before = current_rss_bytes();
    let peak_before = peak_rss_bytes();
    let cpu_before = process_cpu_nanos();
    start_allocations();
    let started = Instant::now();
    let result_digest = lix_operation(&session, rows, operation, batch_size).await;
    let wall_us = started.elapsed().as_secs_f64() * 1_000_000.0;
    let (alloc_bytes, alloc_calls) = stop_allocations();
    let cpu_us = process_cpu_nanos().saturating_sub(cpu_before) as f64 / 1_000.0;
    let io = process_io().delta(io_before);
    let rss_after = current_rss_bytes();
    let peak_after = peak_rss_bytes();
    let backend = stats.lock().expect("stats mutex").clone();
    let disk_after = directory_bytes(path);

    let expected = expected_after(rows, operation);
    let actual = lix_rows(&session).await;
    assert_eq!(actual, expected, "Lix post-operation rows");
    let digest = map_digest(&actual);
    assert_eq!(digest, map_digest(&expected));

    drop(session);
    drop(engine);
    let flush_started = Instant::now();
    database.flush().expect("flush measured Lix operation");
    let flush_us = flush_started.elapsed().as_secs_f64() * 1_000_000.0;
    drop(database);
    let settled_disk = directory_bytes(path);
    let reopened = RocksDB::open(path).expect("cold reopen Lix RocksDB");
    let reopened_engine = Engine::new(reopened.clone())
        .await
        .expect("cold reopen Lix engine");
    let reopened_session = reopened_engine
        .open_workspace_session()
        .await
        .expect("cold reopen Lix session");
    let cold = lix_rows(&reopened_session).await;
    assert_eq!(cold, expected, "Lix cold-reopen rows");
    let cold_digest = map_digest(&cold);
    assert_eq!(cold_digest, digest);

    println!(
        "oltp_comparator_result,path=lix_a12_public_sql_rocksdb,rows={},operation={},batch_size={},wall_us={:.3},cpu_us={:.3},alloc_bytes={},alloc_calls={},rss_before_bytes={},rss_after_bytes={},peak_before_bytes={},peak_after_bytes={},process_read_calls={},process_write_calls={},process_read_bytes={},process_write_bytes={},backend_begin_reads={},backend_begin_writes={},backend_get_calls={},backend_get_keys={},backend_scan_calls={},backend_put_calls={},backend_put_entries={},backend_put_bytes={},backend_delete_calls={},backend_delete_entries={},backend_delete_range_calls={},backend_commits={},backend_rollbacks={},disk_before_bytes={},disk_after_bytes={},flush_us={:.3},settled_disk_bytes={},result_digest={},state_digest={},cold_digest={},verified=true",
        rows,
        operation.label(),
        batch_size,
        wall_us,
        cpu_us,
        alloc_bytes,
        alloc_calls,
        rss_before,
        rss_after,
        peak_before,
        peak_after,
        io.syscr,
        io.syscw,
        io.read_bytes,
        io.write_bytes,
        backend.begin_reads,
        backend.begin_writes,
        backend.get_calls,
        backend.get_keys,
        backend.scan_calls,
        backend.put_calls,
        backend.put_entries,
        backend.put_bytes,
        backend.delete_calls,
        backend.delete_entries,
        backend.delete_range_calls,
        backend.commits,
        backend.rollbacks,
        disk_before,
        disk_after,
        flush_us,
        settled_disk,
        result_digest,
        digest,
        cold_digest
    );
}

async fn warm_lix_operation<S>(session: &SessionContext<S>, rows: usize, operation: Operation)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    match operation {
        Operation::PointRead => {
            session
                .execute(
                    "SELECT id, value FROM oltp_compare_row WHERE id = $1",
                    &[Value::Text(row_key(0))],
                )
                .await
                .expect("warm Lix point-read plan");
        }
        Operation::RangeRead => {
            session
                .execute(
                    "SELECT id, value FROM oltp_compare_row WHERE id >= $1 AND id < $2 ORDER BY id",
                    &[Value::Text(row_key(0)), Value::Text(row_key(rows.min(2)))],
                )
                .await
                .expect("warm Lix range-read plan");
        }
        _ => {
            let mut transaction = session
                .begin_transaction()
                .await
                .expect("begin Lix plan-warm transaction");
            match operation {
                Operation::Insert => {
                    transaction
                        .execute(
                            "INSERT INTO oltp_compare_row (id, value) VALUES ($1, $2)",
                            &[
                                Value::Text("warmup-row".to_string()),
                                Value::Text("warmup-value".to_string()),
                            ],
                        )
                        .await
                        .expect("warm Lix insert plan");
                }
                Operation::UpdateOnePercent | Operation::UpdateTenPercent => {
                    transaction
                        .execute(
                            "UPDATE oltp_compare_row SET value = $1 WHERE id = $2",
                            &[Value::Text(row_value(0, "warmup")), Value::Text(row_key(0))],
                        )
                        .await
                        .expect("warm Lix update plan");
                }
                Operation::Delete => {
                    transaction
                        .execute(
                            "DELETE FROM oltp_compare_row WHERE id = $1",
                            &[Value::Text(row_key(0))],
                        )
                        .await
                        .expect("warm Lix delete plan");
                }
                Operation::Atomic18 => {
                    transaction
                        .execute(
                            "UPDATE oltp_compare_row SET value = $1 WHERE id = $2",
                            &[Value::Text(row_value(0, "warmup")), Value::Text(row_key(0))],
                        )
                        .await
                        .expect("warm Lix atomic update plan");
                    transaction
                        .execute(
                            "DELETE FROM oltp_compare_row WHERE id = $1",
                            &[Value::Text(row_key(1))],
                        )
                        .await
                        .expect("warm Lix atomic delete plan");
                    transaction
                        .execute(
                            "INSERT INTO oltp_compare_row (id, value) VALUES ($1, $2)",
                            &[
                                Value::Text("warmup-row".to_string()),
                                Value::Text("warmup-value".to_string()),
                            ],
                        )
                        .await
                        .expect("warm Lix atomic insert plan");
                }
                Operation::Upsert => {
                    transaction
                        .execute(
                            "INSERT INTO oltp_compare_row (id, value) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET value = excluded.value",
                            &[Value::Text(row_key(0)), Value::Text(row_value(0, "warmup"))],
                        )
                        .await
                        .expect("warm Lix upsert plan");
                }
                Operation::Returning => {
                    transaction
                        .execute(
                            "UPDATE oltp_compare_row SET value = $1 WHERE id = $2 RETURNING id, value",
                            &[Value::Text(row_value(0, "warmup")), Value::Text(row_key(0))],
                        )
                        .await
                        .expect("warm Lix RETURNING plan");
                }
                Operation::PointRead | Operation::RangeRead => unreachable!(),
            }
            transaction
                .rollback()
                .await
                .expect("rollback Lix plan-warm transaction");
        }
    }
}

async fn lix_operation<S>(
    session: &SessionContext<S>,
    rows: usize,
    operation: Operation,
    batch_size: usize,
) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    match operation {
        Operation::Insert => {
            let expected = base_rows(rows);
            lix_apply_batches(session, Operation::Insert, &expected, batch_size).await;
            format!("affected:{rows}")
        }
        Operation::PointRead => {
            let mut result = BTreeMap::new();
            for index in 0..rows {
                let key = row_key(index);
                let query = session
                    .execute(
                        "SELECT id, value FROM oltp_compare_row WHERE id = $1",
                        &[Value::Text(key.clone())],
                    )
                    .await
                    .expect("execute Lix point read");
                assert_eq!(query.rows().len(), 1);
                let value = value_text(query.rows()[0].get_index(1));
                result.insert(key, value);
            }
            map_digest(&result)
        }
        Operation::RangeRead => {
            let count = update_count(rows, 10);
            let start = rows / 4;
            let end = (start + count).min(rows);
            let query = session
                .execute(
                    "SELECT id, value FROM oltp_compare_row WHERE id >= $1 AND id < $2 ORDER BY id",
                    &[Value::Text(row_key(start)), Value::Text(row_key(end))],
                )
                .await
                .expect("execute Lix range read");
            let result = query
                .rows()
                .iter()
                .map(|row| (value_text(row.get_index(0)), value_text(row.get_index(1))))
                .collect::<BTreeMap<_, _>>();
            assert_eq!(result.len(), end - start);
            map_digest(&result)
        }
        Operation::UpdateOnePercent | Operation::UpdateTenPercent => {
            let percent = if operation == Operation::UpdateOnePercent {
                1
            } else {
                10
            };
            let lane = if percent == 1 {
                "update-1"
            } else {
                "update-10"
            };
            let updates = target_keys(rows, update_count(rows, percent))
                .into_iter()
                .map(|index| (row_key(index), row_value(index, lane)))
                .collect::<BTreeMap<_, _>>();
            lix_apply_batches(session, operation, &updates, batch_size).await;
            format!("affected:{}", updates.len())
        }
        Operation::Delete => {
            let keys = (0..rows).map(row_key).collect::<Vec<_>>();
            for chunk in keys.chunks(batch_size) {
                let mut transaction = session
                    .begin_transaction()
                    .await
                    .expect("begin Lix delete transaction");
                for key in chunk {
                    let affected = transaction
                        .execute(
                            "DELETE FROM oltp_compare_row WHERE id = $1",
                            &[Value::Text(key.clone())],
                        )
                        .await
                        .expect("execute Lix delete")
                        .rows_affected();
                    assert_eq!(affected, 1);
                }
                transaction.commit().await.expect("commit Lix delete batch");
            }
            format!("affected:{rows}")
        }
        Operation::Atomic18 => {
            let mut transaction = session
                .begin_transaction()
                .await
                .expect("begin Lix atomic18");
            let mut affected = 0_u64;
            for index in 0..6 {
                affected += transaction
                    .execute(
                        "UPDATE oltp_compare_row SET value = $1 WHERE id = $2",
                        &[
                            Value::Text(row_value(index, "atomic-update")),
                            Value::Text(row_key(index)),
                        ],
                    )
                    .await
                    .expect("Lix atomic update")
                    .rows_affected();
            }
            for index in 6..12 {
                affected += transaction
                    .execute(
                        "DELETE FROM oltp_compare_row WHERE id = $1",
                        &[Value::Text(row_key(index))],
                    )
                    .await
                    .expect("Lix atomic delete")
                    .rows_affected();
            }
            for index in 0..6 {
                affected += transaction
                    .execute(
                        "INSERT INTO oltp_compare_row (id, value) VALUES ($1, $2)",
                        &[
                            Value::Text(new_key(index)),
                            Value::Text(row_value(index, "atomic-insert")),
                        ],
                    )
                    .await
                    .expect("Lix atomic insert")
                    .rows_affected();
            }
            transaction.commit().await.expect("commit Lix atomic18");
            assert_eq!(affected, 18);
            "affected:18".to_string()
        }
        Operation::Upsert => {
            let upserts = (0..rows)
                .map(|index| {
                    if index % 2 == 0 {
                        (row_key(index), row_value(index, "upsert-update"))
                    } else {
                        (new_key(index), row_value(index, "upsert-insert"))
                    }
                })
                .collect::<BTreeMap<_, _>>();
            lix_apply_batches(session, Operation::Upsert, &upserts, batch_size).await;
            format!("affected:{rows}")
        }
        Operation::Returning => {
            let targets = target_keys(rows, update_count(rows, 10));
            let mut returned = BTreeMap::new();
            for chunk in targets.chunks(batch_size) {
                let mut transaction = session
                    .begin_transaction()
                    .await
                    .expect("begin Lix RETURNING transaction");
                for &index in chunk {
                    let result = transaction
                        .execute(
                            "UPDATE oltp_compare_row SET value = $1 WHERE id = $2 RETURNING id, value",
                            &[
                                Value::Text(row_value(index, "update-10")),
                                Value::Text(row_key(index)),
                            ],
                        )
                        .await
                        .expect("execute Lix RETURNING");
                    assert_eq!(result.rows().len(), 1);
                    returned.insert(
                        value_text(result.rows()[0].get_index(0)),
                        value_text(result.rows()[0].get_index(1)),
                    );
                }
                transaction
                    .commit()
                    .await
                    .expect("commit Lix RETURNING batch");
            }
            let expected = returned_expected(rows);
            assert_eq!(returned, expected);
            map_digest(&returned)
        }
    }
}

async fn lix_apply_batches<S>(
    session: &SessionContext<S>,
    operation: Operation,
    values: &BTreeMap<String, String>,
    batch_size: usize,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    if operation == Operation::Upsert {
        let entries = values.iter().collect::<Vec<_>>();
        for chunk in entries.chunks(batch_size) {
            let mut transaction = session
                .begin_transaction()
                .await
                .expect("begin Lix upsert transaction");
            for (key, value) in chunk {
                let affected = transaction
                    .execute(
                        "INSERT INTO oltp_compare_row (id, value) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET value = excluded.value",
                        &[Value::Text((*key).clone()), Value::Text((*value).clone())],
                    )
                    .await
                    .expect("execute Lix upsert")
                    .rows_affected();
                assert_eq!(affected, 1);
            }
            transaction.commit().await.expect("commit Lix upsert batch");
        }
        return;
    }
    let sql = match operation {
        Operation::Insert => "INSERT INTO oltp_compare_row (id, value) VALUES ($1, $2)",
        Operation::UpdateOnePercent | Operation::UpdateTenPercent => {
            "UPDATE oltp_compare_row SET value = $1 WHERE id = $2"
        }
        other => panic!("unsupported prepared operation {other:?}"),
    };
    let entries = values.iter().collect::<Vec<_>>();
    for chunk in entries.chunks(batch_size) {
        let parameter_rows = chunk
            .iter()
            .map(|(key, value)| match operation {
                Operation::UpdateOnePercent | Operation::UpdateTenPercent => {
                    vec![Value::Text((*value).clone()), Value::Text((*key).clone())]
                }
                _ => vec![Value::Text((*key).clone()), Value::Text((*value).clone())],
            })
            .collect::<Vec<_>>();
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("begin prepared Lix transaction");
        let affected = transaction
            .execute_prepared_dml_batch(
                Arc::from(sql),
                PreparedDmlParameterBatch::from_rows(parameter_rows)
                    .expect("build Lix prepared parameter page"),
            )
            .await
            .expect("execute Lix prepared DML")
            .iter()
            .map(lix::ExecuteResult::rows_affected)
            .sum::<u64>();
        assert_eq!(affected, chunk.len() as u64);
        transaction
            .commit()
            .await
            .expect("commit prepared Lix transaction");
    }
}

async fn lix_rows<S>(session: &SessionContext<S>) -> BTreeMap<String, String>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute("SELECT id, value FROM oltp_compare_row ORDER BY id", &[])
        .await
        .expect("read all Lix comparator rows")
        .rows()
        .iter()
        .map(|row| (value_text(row.get_index(0)), value_text(row.get_index(1))))
        .collect()
}

fn value_text(value: Option<&Value>) -> String {
    match value {
        Some(Value::Text(value)) => value.clone(),
        other => panic!("expected text value, got {other:?}"),
    }
}

fn open_sqlite(path: &Path) -> Connection {
    let connection = Connection::open(path).expect("open standalone SQLite database");
    connection
        .execute_batch(
            "PRAGMA journal_mode=WAL;\
             PRAGMA synchronous=FULL;\
             PRAGMA foreign_keys=ON;\
             PRAGMA wal_autocheckpoint=0;\
             PRAGMA busy_timeout=5000;",
        )
        .expect("configure standalone SQLite");
    let journal: String = connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .expect("read SQLite journal mode");
    let synchronous: i64 = connection
        .query_row("PRAGMA synchronous", [], |row| row.get(0))
        .expect("read SQLite synchronous mode");
    assert_eq!(journal.to_ascii_lowercase(), "wal");
    assert_eq!(synchronous, 2, "SQLite synchronous=FULL");
    connection
}

fn setup_sqlite(path: &Path, rows: usize, operation: Operation) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create SQLite parent directory");
    }
    let mut connection = open_sqlite(path);
    connection
        .execute_batch(
            "CREATE TABLE oltp_compare_row (\
                id TEXT PRIMARY KEY NOT NULL,\
                value TEXT NOT NULL\
             ) WITHOUT ROWID;",
        )
        .expect("create SQLite comparator schema");
    let initial = setup_rows(rows, operation);
    if !initial.is_empty() {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("begin SQLite setup transaction");
        {
            let mut statement = transaction
                .prepare("INSERT INTO oltp_compare_row (id, value) VALUES (?1, ?2)")
                .expect("prepare SQLite setup insert");
            for (key, value) in &initial {
                assert_eq!(
                    statement
                        .execute(params![key, value])
                        .expect("execute SQLite setup insert"),
                    1
                );
            }
        }
        transaction.commit().expect("commit SQLite setup");
    }
    connection
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("settle SQLite setup WAL");
    drop(connection);
    println!(
        "oltp_comparator_setup,path=standalone_sqlite_wal_full,rows={},operation={},digest={},disk_bytes={}",
        rows,
        operation.label(),
        map_digest(&initial),
        sqlite_family_bytes(path)
    );
}

fn run_sqlite(path: &Path, rows: usize, operation: Operation, batch_size: usize) {
    let mut connection = open_sqlite(path);
    warm_sqlite_statements(&mut connection, operation);

    let disk_before = sqlite_family_bytes(path);
    let page_count_before = sqlite_pragma_u64(&connection, "page_count");
    let freelist_before = sqlite_pragma_u64(&connection, "freelist_count");
    let io_before = process_io();
    let rss_before = current_rss_bytes();
    let peak_before = peak_rss_bytes();
    let cpu_before = process_cpu_nanos();
    start_allocations();
    let started = Instant::now();
    let (result_digest, statements, transactions) =
        sqlite_operation(&mut connection, rows, operation, batch_size);
    let wall_us = started.elapsed().as_secs_f64() * 1_000_000.0;
    let (alloc_bytes, alloc_calls) = stop_allocations();
    let cpu_us = process_cpu_nanos().saturating_sub(cpu_before) as f64 / 1_000.0;
    let io = process_io().delta(io_before);
    let rss_after = current_rss_bytes();
    let peak_after = peak_rss_bytes();
    let disk_after = sqlite_family_bytes(path);
    let page_count_after = sqlite_pragma_u64(&connection, "page_count");
    let freelist_after = sqlite_pragma_u64(&connection, "freelist_count");

    let expected = expected_after(rows, operation);
    let actual = sqlite_rows(&connection);
    assert_eq!(actual, expected, "SQLite post-operation rows");
    let digest = map_digest(&actual);

    let checkpoint_started = Instant::now();
    connection
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("settle measured SQLite WAL");
    let flush_us = checkpoint_started.elapsed().as_secs_f64() * 1_000_000.0;
    drop(connection);
    let settled_disk = sqlite_family_bytes(path);
    let reopened = open_sqlite(path);
    let cold = sqlite_rows(&reopened);
    assert_eq!(cold, expected, "SQLite cold-reopen rows");
    let cold_digest = map_digest(&cold);
    assert_eq!(cold_digest, digest);

    println!(
        "oltp_comparator_result,path=standalone_sqlite_wal_full,rows={},operation={},batch_size={},wall_us={:.3},cpu_us={:.3},alloc_bytes={},alloc_calls={},rss_before_bytes={},rss_after_bytes={},peak_before_bytes={},peak_after_bytes={},process_read_calls={},process_write_calls={},process_read_bytes={},process_write_bytes={},sqlite_statements={},sqlite_transactions={},sqlite_page_count_before={},sqlite_page_count_after={},sqlite_freelist_before={},sqlite_freelist_after={},disk_before_bytes={},disk_after_bytes={},flush_us={:.3},settled_disk_bytes={},result_digest={},state_digest={},cold_digest={},verified=true",
        rows,
        operation.label(),
        batch_size,
        wall_us,
        cpu_us,
        alloc_bytes,
        alloc_calls,
        rss_before,
        rss_after,
        peak_before,
        peak_after,
        io.syscr,
        io.syscw,
        io.read_bytes,
        io.write_bytes,
        statements,
        transactions,
        page_count_before,
        page_count_after,
        freelist_before,
        freelist_after,
        disk_before,
        disk_after,
        flush_us,
        settled_disk,
        result_digest,
        digest,
        cold_digest
    );
}

fn warm_sqlite_statements(connection: &mut Connection, operation: Operation) {
    let statements: &[&str] = match operation {
        Operation::Insert => &["INSERT INTO oltp_compare_row (id, value) VALUES (?1, ?2)"],
        Operation::PointRead => &["SELECT id, value FROM oltp_compare_row WHERE id = ?1"],
        Operation::RangeRead => {
            &["SELECT id, value FROM oltp_compare_row WHERE id >= ?1 AND id < ?2 ORDER BY id"]
        }
        Operation::UpdateOnePercent | Operation::UpdateTenPercent => {
            &["UPDATE oltp_compare_row SET value = ?2 WHERE id = ?1"]
        }
        Operation::Delete => &["DELETE FROM oltp_compare_row WHERE id = ?1"],
        Operation::Atomic18 => &[
            "UPDATE oltp_compare_row SET value = ?1 WHERE id = ?2",
            "DELETE FROM oltp_compare_row WHERE id = ?1",
            "INSERT INTO oltp_compare_row (id, value) VALUES (?1, ?2)",
        ],
        Operation::Upsert => &[
            "INSERT INTO oltp_compare_row (id, value) VALUES (?1, ?2) ON CONFLICT(id) DO UPDATE SET value=excluded.value",
        ],
        Operation::Returning => {
            &["UPDATE oltp_compare_row SET value = ?1 WHERE id = ?2 RETURNING id, value"]
        }
    };
    for sql in statements {
        drop(
            connection
                .prepare_cached(sql)
                .expect("warm SQLite prepared statement"),
        );
    }
}

fn sqlite_operation(
    connection: &mut Connection,
    rows: usize,
    operation: Operation,
    batch_size: usize,
) -> (String, u64, u64) {
    match operation {
        Operation::Insert => {
            let values = base_rows(rows);
            let transactions = sqlite_write_batches(
                connection,
                &values,
                batch_size,
                "INSERT INTO oltp_compare_row (id, value) VALUES (?1, ?2)",
            );
            (format!("affected:{rows}"), rows as u64, transactions)
        }
        Operation::PointRead => {
            let mut result = BTreeMap::new();
            let mut statement = connection
                .prepare_cached("SELECT id, value FROM oltp_compare_row WHERE id = ?1")
                .expect("prepare SQLite point read");
            for index in 0..rows {
                let key = row_key(index);
                let (actual_key, value) = statement
                    .query_row(params![key], |row| Ok((row.get(0)?, row.get(1)?)))
                    .expect("execute SQLite point read");
                result.insert(actual_key, value);
            }
            (map_digest(&result), rows as u64, 0)
        }
        Operation::RangeRead => {
            let count = update_count(rows, 10);
            let start = rows / 4;
            let end = (start + count).min(rows);
            let mut statement = connection
                .prepare_cached(
                    "SELECT id, value FROM oltp_compare_row WHERE id >= ?1 AND id < ?2 ORDER BY id",
                )
                .expect("prepare SQLite range read");
            let result = statement
                .query_map(params![row_key(start), row_key(end)], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .expect("execute SQLite range read")
                .map(|row| row.expect("decode SQLite range row"))
                .collect::<BTreeMap<_, _>>();
            assert_eq!(result.len(), end - start);
            (map_digest(&result), 1, 0)
        }
        Operation::UpdateOnePercent | Operation::UpdateTenPercent => {
            let percent = if operation == Operation::UpdateOnePercent {
                1
            } else {
                10
            };
            let lane = if percent == 1 {
                "update-1"
            } else {
                "update-10"
            };
            let values = target_keys(rows, update_count(rows, percent))
                .into_iter()
                .map(|index| (row_key(index), row_value(index, lane)))
                .collect::<BTreeMap<_, _>>();
            let transactions = sqlite_write_batches(
                connection,
                &values,
                batch_size,
                "UPDATE oltp_compare_row SET value = ?2 WHERE id = ?1",
            );
            (
                format!("affected:{}", values.len()),
                values.len() as u64,
                transactions,
            )
        }
        Operation::Delete => {
            let keys = (0..rows).map(row_key).collect::<Vec<_>>();
            let mut transactions = 0_u64;
            for chunk in keys.chunks(batch_size) {
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .expect("begin SQLite delete batch");
                {
                    let mut statement = transaction
                        .prepare_cached("DELETE FROM oltp_compare_row WHERE id = ?1")
                        .expect("prepare SQLite delete");
                    for key in chunk {
                        assert_eq!(statement.execute(params![key]).expect("SQLite delete"), 1);
                    }
                }
                transaction.commit().expect("commit SQLite delete batch");
                transactions += 1;
            }
            (format!("affected:{rows}"), rows as u64, transactions)
        }
        Operation::Atomic18 => {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .expect("begin SQLite atomic18");
            {
                let mut update = transaction
                    .prepare_cached("UPDATE oltp_compare_row SET value = ?1 WHERE id = ?2")
                    .expect("prepare SQLite atomic update");
                for index in 0..6 {
                    assert_eq!(
                        update
                            .execute(params![row_value(index, "atomic-update"), row_key(index)])
                            .expect("SQLite atomic update"),
                        1
                    );
                }
            }
            {
                let mut delete = transaction
                    .prepare_cached("DELETE FROM oltp_compare_row WHERE id = ?1")
                    .expect("prepare SQLite atomic delete");
                for index in 6..12 {
                    assert_eq!(
                        delete
                            .execute(params![row_key(index)])
                            .expect("SQLite atomic delete"),
                        1
                    );
                }
            }
            {
                let mut insert = transaction
                    .prepare_cached("INSERT INTO oltp_compare_row (id, value) VALUES (?1, ?2)")
                    .expect("prepare SQLite atomic insert");
                for index in 0..6 {
                    assert_eq!(
                        insert
                            .execute(params![new_key(index), row_value(index, "atomic-insert")])
                            .expect("SQLite atomic insert"),
                        1
                    );
                }
            }
            transaction.commit().expect("commit SQLite atomic18");
            ("affected:18".to_string(), 18, 1)
        }
        Operation::Upsert => {
            let values = (0..rows)
                .map(|index| {
                    if index % 2 == 0 {
                        (row_key(index), row_value(index, "upsert-update"))
                    } else {
                        (new_key(index), row_value(index, "upsert-insert"))
                    }
                })
                .collect::<BTreeMap<_, _>>();
            let transactions = sqlite_write_batches(
                connection,
                &values,
                batch_size,
                "INSERT INTO oltp_compare_row (id, value) VALUES (?1, ?2) ON CONFLICT(id) DO UPDATE SET value=excluded.value",
            );
            (format!("affected:{rows}"), rows as u64, transactions)
        }
        Operation::Returning => {
            let targets = target_keys(rows, update_count(rows, 10));
            let mut returned = BTreeMap::new();
            let mut transactions = 0_u64;
            for chunk in targets.chunks(batch_size) {
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .expect("begin SQLite RETURNING batch");
                {
                    let mut statement = transaction
                        .prepare_cached(
                            "UPDATE oltp_compare_row SET value = ?1 WHERE id = ?2 RETURNING id, value",
                        )
                        .expect("prepare SQLite RETURNING");
                    for &index in chunk {
                        let (key, value) = statement
                            .query_row(
                                params![row_value(index, "update-10"), row_key(index)],
                                |row| Ok((row.get(0)?, row.get(1)?)),
                            )
                            .expect("execute SQLite RETURNING");
                        returned.insert(key, value);
                    }
                }
                transaction.commit().expect("commit SQLite RETURNING batch");
                transactions += 1;
            }
            let expected = returned_expected(rows);
            assert_eq!(returned, expected);
            (map_digest(&returned), targets.len() as u64, transactions)
        }
    }
}

fn sqlite_write_batches(
    connection: &mut Connection,
    values: &BTreeMap<String, String>,
    batch_size: usize,
    sql: &str,
) -> u64 {
    let entries = values.iter().collect::<Vec<_>>();
    let mut transactions = 0_u64;
    for chunk in entries.chunks(batch_size) {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("begin SQLite write batch");
        {
            let mut statement = transaction
                .prepare_cached(sql)
                .expect("prepare SQLite write");
            for (key, value) in chunk {
                assert_eq!(
                    statement
                        .execute(params![*key, *value])
                        .expect("execute SQLite prepared write"),
                    1
                );
            }
        }
        transaction.commit().expect("commit SQLite write batch");
        transactions += 1;
    }
    transactions
}

fn sqlite_rows(connection: &Connection) -> BTreeMap<String, String> {
    let mut statement = connection
        .prepare("SELECT id, value FROM oltp_compare_row ORDER BY id")
        .expect("prepare SQLite verification scan");
    statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .expect("execute SQLite verification scan")
        .map(|row| row.expect("decode SQLite verification row"))
        .collect()
}

fn sqlite_pragma_u64(connection: &Connection, name: &str) -> u64 {
    let value: i64 = connection
        .query_row(&format!("PRAGMA {name}"), [], |row| row.get(0))
        .expect("read SQLite pragma");
    u64::try_from(value).expect("SQLite pragma must be non-negative")
}

fn start_allocations() {
    PROFILE_ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    PROFILE_ALLOCATION_CALLS.store(0, Ordering::Relaxed);
    PROFILE_ALLOCATION_ENABLED.store(true, Ordering::Relaxed);
}

fn stop_allocations() -> (u64, u64) {
    PROFILE_ALLOCATION_ENABLED.store(false, Ordering::Relaxed);
    (
        PROFILE_ALLOCATED_BYTES.load(Ordering::Relaxed),
        PROFILE_ALLOCATION_CALLS.load(Ordering::Relaxed),
    )
}

fn process_cpu_nanos() -> u64 {
    let mut value = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let result = unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut value) };
    assert_eq!(result, 0, "read process CPU clock");
    (value.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(value.tv_nsec as u64)
}

fn process_io() -> ProcessIo {
    let text = std::fs::read_to_string("/proc/self/io").expect("read /proc/self/io");
    let mut result = ProcessIo::default();
    for line in text.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim().parse::<u64>().expect("parse /proc/self/io");
        match name {
            "syscr" => result.syscr = value,
            "syscw" => result.syscw = value,
            "read_bytes" => result.read_bytes = value,
            "write_bytes" => result.write_bytes = value,
            _ => {}
        }
    }
    result
}

fn current_rss_bytes() -> u64 {
    status_kib("VmRSS").saturating_mul(1024)
}

fn peak_rss_bytes() -> u64 {
    status_kib("VmHWM").saturating_mul(1024)
}

fn status_kib(field: &str) -> u64 {
    let text = std::fs::read_to_string("/proc/self/status").expect("read /proc/self/status");
    text.lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            (name == field).then(|| {
                value
                    .split_whitespace()
                    .next()
                    .expect("status value")
                    .parse::<u64>()
                    .expect("parse status KiB")
            })
        })
        .unwrap_or(0)
}

fn directory_bytes(path: &Path) -> u64 {
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return 0;
    };
    if metadata.is_file() {
        return metadata.len();
    }
    std::fs::read_dir(path)
        .expect("read comparator directory")
        .map(|entry| directory_bytes(&entry.expect("read directory entry").path()))
        .sum()
}

fn sqlite_family_bytes(path: &Path) -> u64 {
    let mut total = directory_bytes(path);
    for suffix in ["-wal", "-shm"] {
        let sibling = PathBuf::from(format!("{}{}", path.display(), suffix));
        total += directory_bytes(&sibling);
    }
    total
}
