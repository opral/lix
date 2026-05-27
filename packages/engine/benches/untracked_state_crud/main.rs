use std::fmt::Write as _;
use std::ops::Bound;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use lix_engine::backend::{
    Backend, BackendCapabilities, BackendError, BackendRead, BackendWrite, CommitResult,
    GetOptions, Key, KeyRange, PointVisitor, ProjectedValueRef, PutBatch, ReadOptions, ScanOptions,
    SpaceId, WriteOptions,
};
use lix_engine::storage::{
    InMemoryStorageBackend, PointReadPlan, ScanPlan, StorageContext, StorageCoreProjection,
    StorageGetOptions, StoragePrefix, StorageReadOptions, StorageScanOptions, StorageSpace,
    StorageValue, StorageWriteOptions,
};
use lix_engine::storage_bench;
use lix_engine::{Engine, SessionContext};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value as JsonValue;
use tempfile::TempDir;
use tokio::runtime::Runtime;

#[allow(dead_code)]
#[path = "../../tests/backend/support/redb_backend.rs"]
mod redb_backend;
#[allow(dead_code)]
#[path = "../../tests/backend/support/rocksdb_backend.rs"]
mod rocksdb_backend;
#[allow(dead_code)]
#[path = "../../tests/backend/support/sqlite_backend.rs"]
mod sqlite_backend;

use redb_backend::RedbBackend;
use rocksdb_backend::RocksDbBackend;
use sqlite_backend::SqliteBackend;

const SMOKE_ROWS: usize = 1_000;
const REAL_WORKLOAD_ROWS: usize = 10_000;
const PNPM_LOCK_JSON: &str = include_str!("../fixtures/pnpm-lock.fixture.json");
const JSON_POINTER_SCHEMA_JSON: &str = include_str!("../fixtures/json_pointer.schema.json");
const SESSION_INSERT_CHUNK_SIZE: usize = 500;
const ROW_SPACE: StorageSpace = StorageSpace::new(SpaceId(0x0001_0002), "untracked_state.row.v1");

#[derive(Clone)]
struct PointerRow {
    path: String,
    value_json: String,
    updated_value_json: String,
}

#[derive(Clone)]
struct BenchRow {
    key: Key,
    value: StorageValue,
    updated_value: StorageValue,
}

#[derive(Clone)]
struct RawUntrackedRow {
    branch_id: String,
    schema_key: String,
    entity_pk: String,
    file_id: String,
    snapshot_content: String,
    updated_snapshot_content: String,
    metadata: Option<String>,
    created_at: String,
    updated_at: String,
    global: bool,
}

#[derive(Clone, Copy)]
enum PhysicalLayout {
    FullRowValue,
    PayloadOnlyValue,
}

impl PhysicalLayout {
    fn name(self) -> &'static str {
        match self {
            Self::FullRowValue => "full_row_value",
            Self::PayloadOnlyValue => "payload_only_value",
        }
    }
}

struct RawSqliteFixture {
    conn: Connection,
    _dir: TempDir,
}

#[derive(Debug, Clone, Default)]
struct IoStats {
    get_calls: usize,
    get_keys: usize,
    get_key_bytes: usize,
    get_values: usize,
    get_value_bytes: usize,
    scan_entry_calls: usize,
    scan_entries: usize,
    scan_entry_key_bytes: usize,
    scan_entry_value_bytes: usize,
    write_batches: usize,
    write_puts: usize,
    write_deletes: usize,
    write_delete_ranges: usize,
    write_bytes: usize,
}

impl IoStats {
    fn reset(&mut self) {
        *self = Self::default();
    }

    fn read_ops(&self) -> usize {
        self.get_calls + self.scan_entry_calls
    }

    fn scan_calls(&self) -> usize {
        self.scan_entry_calls
    }

    fn read_rows(&self) -> usize {
        self.get_values + self.scan_entries
    }

    fn read_bytes(&self) -> usize {
        self.get_key_bytes
            + self.get_value_bytes
            + self.scan_entry_key_bytes
            + self.scan_entry_value_bytes
    }

    fn io_ops(&self) -> usize {
        self.read_ops() + self.write_batches
    }

    fn io_bytes(&self) -> usize {
        self.read_bytes() + self.write_bytes
    }
}

#[derive(Clone)]
struct CountingBackend<B> {
    inner: B,
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

impl<B> CountingBackend<B> {
    fn new(inner: B) -> (Self, Arc<Mutex<IoStats>>) {
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

impl<B> Backend for CountingBackend<B>
where
    B: Backend,
{
    type Read<'a>
        = CountingRead<B::Read<'a>>
    where
        Self: 'a;

    type Write<'a>
        = CountingWrite<B::Write<'a>>
    where
        Self: 'a;

    fn capabilities(&self) -> BackendCapabilities {
        self.inner.capabilities()
    }

    fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        Ok(CountingRead {
            inner: self.inner.begin_read(opts)?,
            stats: Arc::clone(&self.stats),
        })
    }

    fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        Ok(CountingWrite {
            inner: self.inner.begin_write(opts)?,
            stats: Arc::clone(&self.stats),
        })
    }
}

impl<R> BackendRead for CountingRead<R>
where
    R: BackendRead,
{
    type RangeScan<'cursor> = R::RangeScan<'cursor>;

    fn visit_keys<V>(
        &self,
        keys: &[Key],
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized,
    {
        {
            let mut stats = self.stats.lock().expect("io stats mutex");
            stats.get_calls += 1;
            stats.get_keys += keys.len();
            stats.get_key_bytes += keys.iter().map(|key| key.0.len()).sum::<usize>();
        }
        let mut counting = CountingPointVisitor {
            inner: visitor,
            stats: Arc::clone(&self.stats),
        };
        self.inner.visit_keys(keys, opts, &mut counting)
    }

    fn with_range_scan<T, F>(
        &self,
        range: KeyRange,
        opts: ScanOptions<'_>,
        f: F,
    ) -> Result<T, BackendError>
    where
        F: FnOnce(&mut Self::RangeScan<'_>) -> Result<T, BackendError>,
    {
        {
            let mut stats = self.stats.lock().expect("io stats mutex");
            stats.scan_entry_calls += 1;
        }
        self.inner.with_range_scan(range, opts, f)
    }

    fn close(self) -> Result<(), BackendError>
    where
        Self: Sized,
    {
        self.inner.close()
    }
}

struct CountingPointVisitor<'a, V: ?Sized> {
    inner: &'a mut V,
    stats: Arc<Mutex<IoStats>>,
}

impl<V> PointVisitor for CountingPointVisitor<'_, V>
where
    V: PointVisitor + ?Sized,
{
    fn visit(
        &mut self,
        index: usize,
        key: &Key,
        value: Option<ProjectedValueRef<'_>>,
    ) -> Result<(), BackendError> {
        if let Some(value) = value {
            let mut stats = self.stats.lock().expect("io stats mutex");
            stats.get_values += 1;
            stats.get_value_bytes += projected_value_len(value);
        }
        self.inner.visit(index, key, value)
    }
}

impl<W> BackendWrite for CountingWrite<W>
where
    W: BackendWrite,
{
    fn put_many(&mut self, entries: PutBatch) -> Result<(), BackendError> {
        {
            let mut stats = self.stats.lock().expect("io stats mutex");
            stats.write_batches += 1;
            stats.write_puts += entries.entries.len();
            stats.write_bytes += entries
                .entries
                .iter()
                .map(|entry| entry.key.0.len() + entry.value.bytes.len())
                .sum::<usize>();
        }
        self.inner.put_many(entries)
    }

    fn delete_many(&mut self, keys: &[Key]) -> Result<(), BackendError> {
        {
            let mut stats = self.stats.lock().expect("io stats mutex");
            stats.write_batches += 1;
            stats.write_deletes += keys.len();
            stats.write_bytes += keys.iter().map(|key| key.0.len()).sum::<usize>();
        }
        self.inner.delete_many(keys)
    }

    fn delete_range(&mut self, range: KeyRange) -> Result<(), BackendError> {
        {
            let mut stats = self.stats.lock().expect("io stats mutex");
            stats.write_batches += 1;
            stats.write_delete_ranges += 1;
            stats.write_bytes += range_bound_len(&range.lower) + range_bound_len(&range.upper);
        }
        self.inner.delete_range(range)
    }

    fn commit(self) -> Result<CommitResult, BackendError>
    where
        Self: Sized,
    {
        self.inner.commit()
    }

    fn rollback(self) -> Result<(), BackendError>
    where
        Self: Sized,
    {
        self.inner.rollback()
    }
}

#[derive(Clone, Copy)]
enum LixBackendProfile {
    Sqlite,
    RocksDb,
    Redb,
}

const LIX_BACKEND_PROFILES: [LixBackendProfile; 3] = [
    LixBackendProfile::Sqlite,
    LixBackendProfile::RocksDb,
    LixBackendProfile::Redb,
];

impl LixBackendProfile {
    fn name(self) -> &'static str {
        match self {
            Self::Sqlite => "lix_sqlite",
            Self::RocksDb => "lix_rocksdb",
            Self::Redb => "lix_redb",
        }
    }
}

fn untracked_state_crud_benches(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create tokio runtime for session execute benchmarks");
    let rows = fixture_rows();
    maybe_print_io_report(&rows);

    bench_raw_sqlite(c, &rows, SMOKE_ROWS, "smoke");
    bench_lix(c, &rows, SMOKE_ROWS, "smoke");
    bench_session_execute_untracked_insert(c, &runtime, &rows, SMOKE_ROWS, "smoke");
    bench_raw_sqlite(c, &rows, REAL_WORKLOAD_ROWS, "real_workload");
    bench_lix(c, &rows, REAL_WORKLOAD_ROWS, "real_workload");
    bench_lix_physical_layout(c, &rows, REAL_WORKLOAD_ROWS, "real_workload");
    bench_session_execute_untracked_insert(c, &runtime, &rows, REAL_WORKLOAD_ROWS, "real_workload");
}

fn maybe_print_io_report(all_rows: &[PointerRow]) {
    let Ok(mode) = std::env::var("LIX_UNTRACKED_STATE_CRUD_IO") else {
        return;
    };
    let workloads = match mode.as_str() {
        "smoke" => vec![("smoke", SMOKE_ROWS)],
        "real_workload" => vec![("real_workload", REAL_WORKLOAD_ROWS)],
        "1" | "all" => vec![("smoke", SMOKE_ROWS), ("real_workload", REAL_WORKLOAD_ROWS)],
        other => panic!(
            "unsupported LIX_UNTRACKED_STATE_CRUD_IO={other}; use smoke, real_workload, all, or 1"
        ),
    };

    println!("\nuntracked_state_crud/io");
    println!("logical storage_v2 backend request/result accounting; not physical disk, WAL, or compaction I/O");
    println!("| workload | backend | operation | logical rows | io ops | io ops/row | io bytes | io bytes/row | read calls | get calls | get keys | scan calls | read rows | read bytes | read bytes/row | write batches | puts | deletes | delete ranges | write bytes | write bytes/row |");
    println!("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |");

    for (label, row_count) in workloads {
        let rows = bench_rows(&all_rows[..row_count]);
        for profile in LIX_BACKEND_PROFILES {
            for operation in [
                "insert_all_rows",
                "select_all_rows",
                "select_keys_only",
                "select_one_by_pk",
                "select_all_by_pk",
                "update_all_rows",
                "update_one_by_pk",
                "delete_all_rows",
                "delete_one_by_pk",
            ] {
                let stats = measure_lix_io(profile, operation, &rows);
                let logical_rows = operation_logical_rows(operation, row_count);
                println!(
                    "| {label}/{} | {} | `{operation}` | {logical_rows} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                    row_label(row_count),
                    profile.name(),
                    stats.io_ops(),
                    ratio(stats.io_ops(), logical_rows),
                    stats.io_bytes(),
                    ratio(stats.io_bytes(), logical_rows),
                    stats.read_ops(),
                    stats.get_calls,
                    stats.get_keys,
                    stats.scan_calls(),
                    stats.read_rows(),
                    stats.read_bytes(),
                    ratio(stats.read_bytes(), logical_rows),
                    stats.write_batches,
                    stats.write_puts,
                    stats.write_deletes,
                    stats.write_delete_ranges,
                    stats.write_bytes,
                    ratio(stats.write_bytes, logical_rows),
                );
            }
        }
    }
    println!();
}

fn bench_raw_sqlite(c: &mut Criterion, all_rows: &[PointerRow], row_count: usize, label: &str) {
    let rows = raw_rows(&all_rows[..row_count]);
    let mut group = c.benchmark_group(format!("untracked_state_crud/raw_sqlite/{label}"));
    configure_group(&mut group, row_count);

    group.bench_function(format!("insert_all_rows/{}", row_label(row_count)), |b| {
        b.iter_batched(
            prepare_raw_sqlite_empty,
            |fixture| black_box(raw_sqlite_insert_all(fixture, &rows)),
            BatchSize::LargeInput,
        )
    });
    group.bench_function(
        format!(
            "insert_all_rows_unprepared_per_row/{}",
            row_label(row_count)
        ),
        |b| {
            b.iter_batched(
                prepare_raw_sqlite_empty,
                |fixture| black_box(raw_sqlite_insert_all_unprepared_per_row(fixture, &rows)),
                BatchSize::LargeInput,
            )
        },
    );
    group.bench_function(
        format!(
            "insert_all_rows_unprepared_chunked/{}",
            row_label(row_count)
        ),
        |b| {
            b.iter_batched(
                prepare_raw_sqlite_empty,
                |fixture| black_box(raw_sqlite_insert_all_unprepared_chunked(fixture, &rows)),
                BatchSize::LargeInput,
            )
        },
    );
    group.bench_function(format!("select_all_rows/{}", row_label(row_count)), |b| {
        b.iter_batched(
            || prepare_raw_sqlite_seeded(&rows),
            |fixture| black_box(raw_sqlite_select_all(fixture, row_count)),
            BatchSize::LargeInput,
        )
    });
    group.bench_function(format!("select_one_by_pk/{}", row_label(row_count)), |b| {
        b.iter_batched(
            || prepare_raw_sqlite_seeded(&rows),
            |fixture| black_box(raw_sqlite_select_one_by_pk(fixture, pick_pk_row(&rows))),
            BatchSize::LargeInput,
        )
    });
    group.bench_function(format!("update_all_rows/{}", row_label(row_count)), |b| {
        b.iter_batched(
            || prepare_raw_sqlite_seeded(&rows),
            |fixture| black_box(raw_sqlite_update_all(fixture, &rows)),
            BatchSize::LargeInput,
        )
    });
    group.bench_function(format!("update_one_by_pk/{}", row_label(row_count)), |b| {
        b.iter_batched(
            || prepare_raw_sqlite_seeded(&rows),
            |fixture| black_box(raw_sqlite_update_one_by_pk(fixture, pick_pk_row(&rows))),
            BatchSize::LargeInput,
        )
    });
    group.bench_function(format!("delete_all_rows/{}", row_label(row_count)), |b| {
        b.iter_batched(
            || prepare_raw_sqlite_seeded(&rows),
            |fixture| black_box(raw_sqlite_delete_all(fixture, row_count)),
            BatchSize::LargeInput,
        )
    });
    group.bench_function(format!("delete_one_by_pk/{}", row_label(row_count)), |b| {
        b.iter_batched(
            || prepare_raw_sqlite_seeded(&rows),
            |fixture| black_box(raw_sqlite_delete_one_by_pk(fixture, pick_pk_row(&rows))),
            BatchSize::LargeInput,
        )
    });
    group.finish();
}

fn bench_lix(c: &mut Criterion, all_rows: &[PointerRow], row_count: usize, label: &str) {
    let rows = bench_rows(&all_rows[..row_count]);
    for profile in LIX_BACKEND_PROFILES {
        let mut group =
            c.benchmark_group(format!("untracked_state_crud/{}/{label}", profile.name()));
        configure_group(&mut group, row_count);

        bench_lix_profile(&mut group, profile, &rows);
        group.finish();
    }
}

fn bench_lix_profile(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    profile: LixBackendProfile,
    rows: &[BenchRow],
) {
    group.bench_function(format!("insert_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || profile_storage(profile),
            |storage| black_box(storage.insert_all(rows)),
            BatchSize::LargeInput,
        )
    });
    group.bench_function(format!("select_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || prepare_lix_seeded(profile, rows),
            |storage| black_box(storage.select_all(rows.len(), StorageCoreProjection::FullValue)),
            BatchSize::LargeInput,
        )
    });
    group.bench_function(format!("select_keys_only/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || prepare_lix_seeded(profile, rows),
            |storage| black_box(storage.select_all(rows.len(), StorageCoreProjection::KeyOnly)),
            BatchSize::LargeInput,
        )
    });
    group.bench_function(format!("select_one_by_pk/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || prepare_lix_seeded(profile, rows),
            |storage| black_box(storage.select_points(std::slice::from_ref(&rows[rows.len() / 2]))),
            BatchSize::LargeInput,
        )
    });
    group.bench_function(format!("select_all_by_pk/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || prepare_lix_seeded(profile, rows),
            |storage| black_box(storage.select_points(rows)),
            BatchSize::LargeInput,
        )
    });
    group.bench_function(format!("update_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || prepare_lix_seeded(profile, rows),
            |storage| black_box(storage.update_all(rows)),
            BatchSize::LargeInput,
        )
    });
    group.bench_function(format!("update_one_by_pk/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || prepare_lix_seeded(profile, rows),
            |storage| black_box(storage.update_all(&rows[..1])),
            BatchSize::LargeInput,
        )
    });
    group.bench_function(format!("delete_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || prepare_lix_seeded(profile, rows),
            |storage| black_box(storage.delete_all()),
            BatchSize::LargeInput,
        )
    });
    group.bench_function(format!("delete_one_by_pk/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || prepare_lix_seeded(profile, rows),
            |storage| black_box(storage.delete_one(&rows[rows.len() / 2])),
            BatchSize::LargeInput,
        )
    });
}

fn bench_lix_physical_layout(
    c: &mut Criterion,
    all_rows: &[PointerRow],
    row_count: usize,
    label: &str,
) {
    let rows = &all_rows[..row_count];
    for profile in LIX_BACKEND_PROFILES {
        let mut group = c.benchmark_group(format!(
            "untracked_state_crud/physical_layout/{}/{label}",
            profile.name()
        ));
        configure_group(&mut group, row_count);

        for layout in [
            PhysicalLayout::FullRowValue,
            PhysicalLayout::PayloadOnlyValue,
        ] {
            group.bench_function(
                format!("insert_all_rows/{}/{}", layout.name(), row_label(row_count)),
                |b| {
                    b.iter_batched(
                        || (profile_storage(profile), physical_layout_rows(rows, layout)),
                        |(storage, rows)| black_box(storage.insert_all(&rows)),
                        BatchSize::LargeInput,
                    )
                },
            );
        }

        group.finish();
    }
}

fn bench_session_execute_untracked_insert(
    c: &mut Criterion,
    runtime: &Runtime,
    all_rows: &[PointerRow],
    row_count: usize,
    label: &str,
) {
    let rows = all_rows[..row_count].to_vec();
    let mut group = c.benchmark_group(format!(
        "untracked_state_crud/session_execute_untracked/in_memory/{label}"
    ));
    configure_group(&mut group, row_count);

    group.bench_function(format!("insert_all_rows/{}", row_label(row_count)), |b| {
        b.iter_batched(
            || runtime.block_on(prepare_session_empty()),
            |session| {
                runtime.block_on(insert_untracked_json_pointer_rows(&session, &rows));
                black_box(rows.len())
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

fn measure_lix_io(profile: LixBackendProfile, operation: &str, rows: &[BenchRow]) -> IoStats {
    match profile {
        LixBackendProfile::Sqlite => measure_lix_io_for_backend(sqlite_backend(), operation, rows),
        LixBackendProfile::RocksDb => {
            measure_lix_io_for_backend(rocksdb_backend(), operation, rows)
        }
        LixBackendProfile::Redb => measure_lix_io_for_backend(redb_backend(), operation, rows),
    }
}

fn measure_lix_io_for_backend<B>(backend: B, operation: &str, rows: &[BenchRow]) -> IoStats
where
    B: Backend,
{
    let (backend, stats) = CountingBackend::new(backend);
    let storage = StorageContext::new(backend);
    if !matches!(operation, "insert_all_rows") {
        lix_insert_all(&storage, rows);
        stats.lock().expect("io stats mutex").reset();
    }
    match operation {
        "insert_all_rows" => {
            lix_insert_all(&storage, rows);
        }
        "select_all_rows" => {
            lix_select_all(&storage, rows.len(), StorageCoreProjection::FullValue);
            record_scan_result(&stats, rows, true);
        }
        "select_keys_only" => {
            lix_select_all(&storage, rows.len(), StorageCoreProjection::KeyOnly);
            record_scan_result(&stats, rows, false);
        }
        "select_one_by_pk" => {
            lix_select_points(&storage, std::slice::from_ref(&rows[rows.len() / 2]));
        }
        "select_all_by_pk" => {
            lix_select_points(&storage, rows);
        }
        "update_all_rows" => {
            lix_update_all(&storage, rows);
        }
        "update_one_by_pk" => {
            lix_update_all(&storage, &rows[..1]);
        }
        "delete_all_rows" => {
            lix_delete_all(&storage);
        }
        "delete_one_by_pk" => {
            lix_delete_one(&storage, &rows[rows.len() / 2]);
        }
        _ => unreachable!("unknown operation"),
    }
    let snapshot = stats.lock().expect("io stats mutex").clone();
    snapshot
}

fn record_scan_result(stats: &Arc<Mutex<IoStats>>, rows: &[BenchRow], include_values: bool) {
    let mut stats = stats.lock().expect("io stats mutex");
    stats.scan_entries += rows.len();
    stats.scan_entry_key_bytes += rows.iter().map(|row| row.key.0.len()).sum::<usize>();
    if include_values {
        stats.scan_entry_value_bytes += rows.iter().map(|row| row.value.bytes.len()).sum::<usize>();
    }
}

fn lix_insert_all<B>(storage: &StorageContext<B>, rows: &[BenchRow]) -> usize
where
    B: Backend,
{
    let mut writes = storage.new_write_set();
    for row in rows {
        writes.put(ROW_SPACE, row.key.clone(), row.value.clone());
    }
    let (_commit, stats) = storage
        .commit_write_set(writes, StorageWriteOptions::default())
        .expect("commit insert rows");
    assert_eq!(stats.staged_puts, rows.len() as u64);
    rows.len()
}

fn lix_update_all<B>(storage: &StorageContext<B>, rows: &[BenchRow]) -> usize
where
    B: Backend,
{
    let mut writes = storage.new_write_set();
    for row in rows {
        writes.put(ROW_SPACE, row.key.clone(), row.updated_value.clone());
    }
    let (_commit, stats) = storage
        .commit_write_set(writes, StorageWriteOptions::default())
        .expect("commit update rows");
    assert_eq!(stats.staged_puts, rows.len() as u64);
    rows.len()
}

fn lix_delete_one<B>(storage: &StorageContext<B>, row: &BenchRow) -> usize
where
    B: Backend,
{
    let mut writes = storage.new_write_set();
    writes.delete(ROW_SPACE, row.key.clone());
    let (_commit, stats) = storage
        .commit_write_set(writes, StorageWriteOptions::default())
        .expect("commit delete row");
    assert_eq!(stats.staged_deletes, 1);
    1
}

fn lix_delete_all<B>(storage: &StorageContext<B>) -> usize
where
    B: Backend,
{
    storage
        .clear_space(ROW_SPACE, StorageWriteOptions::default())
        .expect("clear untracked rows");
    1
}

fn lix_select_all<B>(
    storage: &StorageContext<B>,
    expected_rows: usize,
    projection: StorageCoreProjection,
) -> usize
where
    B: Backend,
{
    let read = storage
        .begin_read(StorageReadOptions::default())
        .expect("begin read");
    let plan = ScanPlan::prefix(
        ROW_SPACE,
        StoragePrefix {
            bytes: Bytes::new(),
        },
    );
    let page = plan
        .collect(
            &read,
            StorageScanOptions {
                projection,
                limit_rows: expected_rows + 1,
                ..StorageScanOptions::default()
            },
        )
        .expect("scan rows");
    assert_eq!(page.value.entries.len(), expected_rows);
    expected_rows
}

fn lix_select_points<B>(storage: &StorageContext<B>, rows: &[BenchRow]) -> usize
where
    B: Backend,
{
    let read = storage
        .begin_read(StorageReadOptions::default())
        .expect("begin read");
    let keys = rows.iter().map(|row| row.key.clone()).collect::<Vec<_>>();
    let result = PointReadPlan::new(ROW_SPACE, &keys)
        .materialize(&read, StorageGetOptions::default())
        .expect("point read rows");
    assert_eq!(result.value.len(), rows.len());
    assert!(result.value.iter().all(Option::is_some));
    result.value.len()
}

fn prepare_lix_seeded(profile: LixBackendProfile, rows: &[BenchRow]) -> ProfileStorage {
    let storage = profile_storage(profile);
    storage.insert_all(rows);
    storage
}

fn profile_storage(profile: LixBackendProfile) -> ProfileStorage {
    match profile {
        LixBackendProfile::Sqlite => ProfileStorage::Sqlite(StorageContext::new(sqlite_backend())),
        LixBackendProfile::RocksDb => {
            ProfileStorage::RocksDb(StorageContext::new(rocksdb_backend()))
        }
        LixBackendProfile::Redb => ProfileStorage::Redb(StorageContext::new(redb_backend())),
    }
}

enum ProfileStorage {
    Sqlite(StorageContext<SqliteBackend>),
    RocksDb(StorageContext<RocksDbBackend>),
    Redb(StorageContext<RedbBackend>),
}

async fn prepare_session_empty() -> SessionContext<InMemoryStorageBackend> {
    let backend = InMemoryStorageBackend::new();
    Engine::initialize(backend.clone())
        .await
        .expect("initialize in-memory engine");
    let engine = Engine::new(backend).await.expect("open in-memory engine");
    let setup = engine
        .open_workspace_session()
        .await
        .expect("open in-memory setup session");
    register_json_pointer_schema(&setup).await;
    engine
        .open_workspace_session()
        .await
        .expect("open in-memory benchmark session")
}

async fn register_json_pointer_schema<B>(session: &SessionContext<B>)
where
    B: lix_engine::storage::StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Clone + Send + Sync + 'static,
    for<'backend> B::Write<'backend>: Send,
{
    let sql = format!(
        "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked)
         VALUES (lix_json('{}'), false, false)",
        sql_string(JSON_POINTER_SCHEMA_JSON)
    );
    let affected = session
        .execute(&sql, &[])
        .await
        .expect("register json_pointer schema")
        .rows_affected();
    assert_eq!(affected, 1);
}

async fn insert_untracked_json_pointer_rows<B>(session: &SessionContext<B>, rows: &[PointerRow])
where
    B: lix_engine::storage::StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Clone + Send + Sync + 'static,
    for<'backend> B::Write<'backend>: Send,
{
    for chunk in rows.chunks(SESSION_INSERT_CHUNK_SIZE) {
        let sql = insert_untracked_json_pointer_sql(chunk);
        let affected = session
            .execute(&sql, &[])
            .await
            .expect("insert untracked json_pointer rows")
            .rows_affected();
        assert_eq!(affected as usize, chunk.len());
    }
}

impl ProfileStorage {
    fn insert_all(&self, rows: &[BenchRow]) -> usize {
        match self {
            Self::Sqlite(storage) => lix_insert_all(storage, rows),
            Self::RocksDb(storage) => lix_insert_all(storage, rows),
            Self::Redb(storage) => lix_insert_all(storage, rows),
        }
    }

    fn update_all(&self, rows: &[BenchRow]) -> usize {
        match self {
            Self::Sqlite(storage) => lix_update_all(storage, rows),
            Self::RocksDb(storage) => lix_update_all(storage, rows),
            Self::Redb(storage) => lix_update_all(storage, rows),
        }
    }

    fn delete_one(&self, row: &BenchRow) -> usize {
        match self {
            Self::Sqlite(storage) => lix_delete_one(storage, row),
            Self::RocksDb(storage) => lix_delete_one(storage, row),
            Self::Redb(storage) => lix_delete_one(storage, row),
        }
    }

    fn delete_all(&self) -> usize {
        match self {
            Self::Sqlite(storage) => lix_delete_all(storage),
            Self::RocksDb(storage) => lix_delete_all(storage),
            Self::Redb(storage) => lix_delete_all(storage),
        }
    }

    fn select_all(&self, expected_rows: usize, projection: StorageCoreProjection) -> usize {
        match self {
            Self::Sqlite(storage) => lix_select_all(storage, expected_rows, projection),
            Self::RocksDb(storage) => lix_select_all(storage, expected_rows, projection),
            Self::Redb(storage) => lix_select_all(storage, expected_rows, projection),
        }
    }

    fn select_points(&self, rows: &[BenchRow]) -> usize {
        match self {
            Self::Sqlite(storage) => lix_select_points(storage, rows),
            Self::RocksDb(storage) => lix_select_points(storage, rows),
            Self::Redb(storage) => lix_select_points(storage, rows),
        }
    }
}

fn sqlite_backend() -> SqliteBackend {
    let dir = TempDir::new().expect("create sqlite backend tempdir");
    let path = dir.keep().join("bench.sqlite");
    SqliteBackend::open(path).expect("open sqlite backend")
}

fn rocksdb_backend() -> RocksDbBackend {
    let dir = TempDir::new().expect("create rocksdb backend tempdir");
    let path = dir.keep().join("bench.rocksdb");
    RocksDbBackend::open(path).expect("open rocksdb backend")
}

fn redb_backend() -> RedbBackend {
    let dir = TempDir::new().expect("create redb backend tempdir");
    let path = dir.keep().join("bench.redb");
    RedbBackend::open(path).expect("open redb backend")
}

fn configure_group(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    row_count: usize,
) {
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(250));
    group.measurement_time(if row_count >= REAL_WORKLOAD_ROWS {
        Duration::from_secs(2)
    } else {
        Duration::from_secs(1)
    });
}

fn fixture_rows() -> Vec<PointerRow> {
    let json: JsonValue = serde_json::from_str(PNPM_LOCK_JSON).expect("parse pnpm-lock fixture");
    let mut rows = Vec::new();
    flatten_json("", &json, &mut rows);
    rows.sort_by(|left, right| left.path.cmp(&right.path));
    assert!(rows.len() >= REAL_WORKLOAD_ROWS);
    rows
}

fn flatten_json(path: &str, value: &JsonValue, rows: &mut Vec<PointerRow>) {
    if !path.is_empty() {
        let value_json = serde_json::to_string(value).expect("serialize JSON pointer value");
        let updated_value_json = serde_json::to_string(&serde_json::json!({
            "path": path,
            "value": value,
            "updated": true
        }))
        .expect("serialize updated JSON pointer value");
        rows.push(PointerRow {
            path: path.to_string(),
            value_json,
            updated_value_json,
        });
    }

    match value {
        JsonValue::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                flatten_json(&format!("{path}/{index}"), item, rows);
            }
        }
        JsonValue::Object(map) => {
            for (key, item) in map {
                flatten_json(&format!("{path}/{}", escape_json_pointer(key)), item, rows);
            }
        }
        _ => {}
    }
}

fn escape_json_pointer(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}

fn bench_rows(rows: &[PointerRow]) -> Vec<BenchRow> {
    rows.iter()
        .map(|row| {
            let entity_pk = entity_pk(row);
            let value = snapshot_value(row.path.as_str(), row.value_json.as_str());
            let updated_value = snapshot_value(row.path.as_str(), row.updated_value_json.as_str());
            BenchRow {
                key: Key(Bytes::from(row_key(&entity_pk))),
                value: StorageValue {
                    bytes: Bytes::from(value),
                },
                updated_value: StorageValue {
                    bytes: Bytes::from(updated_value),
                },
            }
        })
        .collect()
}

fn physical_layout_rows(rows: &[PointerRow], layout: PhysicalLayout) -> Vec<BenchRow> {
    rows.iter()
        .map(|row| {
            let entity_pk = entity_pk(row);
            let value = snapshot_value(row.path.as_str(), row.value_json.as_str());
            let updated_value = snapshot_value(row.path.as_str(), row.updated_value_json.as_str());
            let (key, value) = match layout {
                PhysicalLayout::FullRowValue => {
                    storage_bench::untracked_state_full_row_key_value(&entity_pk, &value)
                }
                PhysicalLayout::PayloadOnlyValue => {
                    storage_bench::untracked_state_row_key_value(&entity_pk, &value)
                }
            };
            let (_, updated_value) = match layout {
                PhysicalLayout::FullRowValue => {
                    storage_bench::untracked_state_full_row_key_value(&entity_pk, &updated_value)
                }
                PhysicalLayout::PayloadOnlyValue => {
                    storage_bench::untracked_state_row_key_value(&entity_pk, &updated_value)
                }
            };
            BenchRow {
                key,
                value,
                updated_value,
            }
        })
        .collect()
}

fn raw_rows(rows: &[PointerRow]) -> Vec<RawUntrackedRow> {
    rows.iter()
        .map(|row| RawUntrackedRow {
            branch_id: "bench-branch".to_string(),
            schema_key: "json_pointer".to_string(),
            entity_pk: entity_pk(row),
            file_id: String::new(),
            snapshot_content: snapshot_value(row.path.as_str(), row.value_json.as_str()),
            updated_snapshot_content: snapshot_value(
                row.path.as_str(),
                row.updated_value_json.as_str(),
            ),
            metadata: None,
            created_at: "2026-01-01T00:00:00.000Z".to_string(),
            updated_at: "2026-01-01T00:00:00.000Z".to_string(),
            global: false,
        })
        .collect()
}

fn insert_untracked_json_pointer_sql(rows: &[PointerRow]) -> String {
    let mut sql = String::from("INSERT INTO json_pointer (path, value, lixcol_untracked) VALUES ");
    for (index, row) in rows.iter().enumerate() {
        if index > 0 {
            sql.push(',');
        }
        sql.push_str(&format!(
            "('{}', lix_json('{}'), true)",
            sql_string(row.path.as_str()),
            sql_string(row.value_json.as_str())
        ));
    }
    sql
}

fn entity_pk(row: &PointerRow) -> String {
    row.path.clone()
}

fn row_key(entity_pk: &str) -> Vec<u8> {
    let mut out = Vec::new();
    push_component(&mut out, "bench-branch");
    push_component(&mut out, "json_pointer");
    push_component(&mut out, entity_pk);
    push_component(&mut out, "");
    out
}

fn push_component(out: &mut Vec<u8>, value: &str) {
    let len = u32::try_from(value.len()).expect("component length fits u32");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value.as_bytes());
}

fn snapshot_value(path: &str, value_json: &str) -> String {
    format!(r#"{{"path":{},"value":{}}}"#, json_string(path), value_json)
}

fn json_string(value: &str) -> String {
    serde_json::to_string(value).expect("serialize JSON string")
}

fn sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

fn optional_sql_string(value: Option<&str>) -> String {
    match value {
        Some(value) => format!("'{}'", sql_string(value)),
        None => "NULL".to_string(),
    }
}

fn prepare_raw_sqlite_empty() -> RawSqliteFixture {
    let dir = TempDir::new().expect("create raw sqlite tempdir");
    let conn =
        Connection::open(dir.path().join("untracked_state.sqlite")).expect("open raw sqlite db");
    conn.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA temp_store = MEMORY;
        PRAGMA foreign_keys = ON;
        CREATE TABLE untracked_state (
            branch_id TEXT NOT NULL,
            schema_key TEXT NOT NULL,
            entity_pk TEXT NOT NULL,
            file_id TEXT NOT NULL,
            snapshot_content TEXT,
            metadata TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            global INTEGER NOT NULL,
            PRIMARY KEY (branch_id, schema_key, entity_pk, file_id)
        ) WITHOUT ROWID;
        ",
    )
    .expect("create raw sqlite table");
    RawSqliteFixture { conn, _dir: dir }
}

fn prepare_raw_sqlite_seeded(rows: &[RawUntrackedRow]) -> RawSqliteFixture {
    raw_sqlite_insert_all(prepare_raw_sqlite_empty(), rows)
}

fn raw_sqlite_insert_all(
    mut fixture: RawSqliteFixture,
    rows: &[RawUntrackedRow],
) -> RawSqliteFixture {
    let tx = fixture.conn.transaction().expect("begin raw sqlite insert");
    {
        let mut statement = tx
            .prepare_cached(
                "
                INSERT INTO untracked_state (
                    branch_id, schema_key, entity_pk, file_id, snapshot_content,
                    metadata, created_at, updated_at, global
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                ",
            )
            .expect("prepare raw sqlite insert");
        for row in rows {
            statement
                .execute(params![
                    row.branch_id,
                    row.schema_key,
                    row.entity_pk,
                    row.file_id,
                    row.snapshot_content,
                    row.metadata,
                    row.created_at,
                    row.updated_at,
                    row.global as i64,
                ])
                .expect("execute raw sqlite insert");
        }
    }
    tx.commit().expect("commit raw sqlite insert");
    fixture
}

fn raw_sqlite_insert_all_unprepared_per_row(
    mut fixture: RawSqliteFixture,
    rows: &[RawUntrackedRow],
) -> RawSqliteFixture {
    let tx = fixture
        .conn
        .transaction()
        .expect("begin raw sqlite unprepared insert");
    let mut affected = 0usize;
    for row in rows {
        let mut sql = String::from(
            "
            INSERT INTO untracked_state (
                branch_id, schema_key, entity_pk, file_id, snapshot_content,
                metadata, created_at, updated_at, global
            )
            VALUES ",
        );
        append_raw_sqlite_insert_values_tuple(&mut sql, row);
        affected += tx
            .execute(&sql, [])
            .expect("execute raw sqlite unprepared insert");
    }
    assert_eq!(affected, rows.len());
    tx.commit().expect("commit raw sqlite unprepared insert");
    fixture
}

fn raw_sqlite_insert_all_unprepared_chunked(
    mut fixture: RawSqliteFixture,
    rows: &[RawUntrackedRow],
) -> RawSqliteFixture {
    let tx = fixture
        .conn
        .transaction()
        .expect("begin raw sqlite chunked unprepared insert");
    let mut affected = 0usize;
    for chunk in rows.chunks(SESSION_INSERT_CHUNK_SIZE) {
        let mut sql = String::from(
            "
            INSERT INTO untracked_state (
                branch_id, schema_key, entity_pk, file_id, snapshot_content,
                metadata, created_at, updated_at, global
            )
            VALUES ",
        );
        for (index, row) in chunk.iter().enumerate() {
            if index > 0 {
                sql.push_str(", ");
            }
            append_raw_sqlite_insert_values_tuple(&mut sql, row);
        }
        affected += tx
            .execute(&sql, [])
            .expect("execute raw sqlite chunked unprepared insert");
    }
    assert_eq!(affected, rows.len());
    tx.commit()
        .expect("commit raw sqlite chunked unprepared insert");
    fixture
}

fn append_raw_sqlite_insert_values_tuple(sql: &mut String, row: &RawUntrackedRow) {
    write!(
        sql,
        "('{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', {})",
        sql_string(row.branch_id.as_str()),
        sql_string(row.schema_key.as_str()),
        sql_string(row.entity_pk.as_str()),
        sql_string(row.file_id.as_str()),
        sql_string(row.snapshot_content.as_str()),
        optional_sql_string(row.metadata.as_deref()),
        sql_string(row.created_at.as_str()),
        sql_string(row.updated_at.as_str()),
        row.global as i64,
    )
    .expect("write raw sqlite insert tuple SQL");
}

fn raw_sqlite_select_all(fixture: RawSqliteFixture, expected_rows: usize) -> usize {
    let mut statement = fixture
        .conn
        .prepare_cached(
            "
            SELECT branch_id, schema_key, entity_pk, file_id, snapshot_content, metadata,
                   created_at, updated_at, global
            FROM untracked_state
            ORDER BY branch_id, schema_key, entity_pk, file_id
            ",
        )
        .expect("prepare raw sqlite select all");
    let mut rows = statement.query([]).expect("execute raw sqlite select all");
    let mut count = 0;
    let mut materialized_bytes = 0usize;
    while let Some(row) = rows.next().expect("read raw sqlite row") {
        let branch_id: String = row.get(0).expect("read branch_id");
        let schema_key: String = row.get(1).expect("read schema_key");
        let entity_pk: String = row.get(2).expect("read entity_pk");
        let file_id: String = row.get(3).expect("read file_id");
        let snapshot_content: String = row.get(4).expect("read snapshot_content");
        let metadata: Option<String> = row.get(5).expect("read metadata");
        let created_at: String = row.get(6).expect("read created_at");
        let updated_at: String = row.get(7).expect("read updated_at");
        let global: i64 = row.get(8).expect("read global");
        materialized_bytes += branch_id.len()
            + schema_key.len()
            + entity_pk.len()
            + file_id.len()
            + snapshot_content.len()
            + metadata.as_ref().map_or(0, String::len)
            + created_at.len()
            + updated_at.len()
            + usize::from(global != 0);
        count += 1;
    }
    assert_eq!(count, expected_rows);
    assert!(expected_rows == 0 || materialized_bytes > 0);
    count
}

fn raw_sqlite_select_one_by_pk(fixture: RawSqliteFixture, row: &RawUntrackedRow) -> usize {
    let found = fixture
        .conn
        .query_row(
            "
            SELECT snapshot_content
            FROM untracked_state
            WHERE branch_id = ?1 AND schema_key = ?2 AND entity_pk = ?3 AND file_id = ?4
            ",
            params![row.branch_id, row.schema_key, row.entity_pk, row.file_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()
        .expect("execute raw sqlite select one")
        .is_some();
    assert!(found);
    usize::from(found)
}

fn raw_sqlite_update_all(mut fixture: RawSqliteFixture, rows: &[RawUntrackedRow]) -> usize {
    let tx = fixture
        .conn
        .transaction()
        .expect("begin raw sqlite update all");
    let mut affected = 0;
    {
        let mut statement = tx
            .prepare_cached(
                "
                UPDATE untracked_state
                SET snapshot_content = ?5, updated_at = ?6
                WHERE branch_id = ?1 AND schema_key = ?2 AND entity_pk = ?3 AND file_id = ?4
                ",
            )
            .expect("prepare raw sqlite update all");
        for row in rows {
            affected += statement
                .execute(params![
                    row.branch_id,
                    row.schema_key,
                    row.entity_pk,
                    row.file_id,
                    row.updated_snapshot_content,
                    row.updated_at,
                ])
                .expect("execute raw sqlite update all");
        }
    }
    tx.commit().expect("commit raw sqlite update all");
    assert_eq!(affected, rows.len());
    affected
}

fn raw_sqlite_update_one_by_pk(fixture: RawSqliteFixture, row: &RawUntrackedRow) -> usize {
    let affected = fixture
        .conn
        .execute(
            "
            UPDATE untracked_state
            SET snapshot_content = ?5, updated_at = ?6
            WHERE branch_id = ?1 AND schema_key = ?2 AND entity_pk = ?3 AND file_id = ?4
            ",
            params![
                row.branch_id,
                row.schema_key,
                row.entity_pk,
                row.file_id,
                row.updated_snapshot_content,
                row.updated_at,
            ],
        )
        .expect("execute raw sqlite update one");
    assert_eq!(affected, 1);
    affected
}

fn raw_sqlite_delete_all(fixture: RawSqliteFixture, expected_rows: usize) -> usize {
    let affected = fixture
        .conn
        .execute("DELETE FROM untracked_state", [])
        .expect("execute raw sqlite delete all");
    assert_eq!(affected, expected_rows);
    affected
}

fn raw_sqlite_delete_one_by_pk(fixture: RawSqliteFixture, row: &RawUntrackedRow) -> usize {
    let affected = fixture
        .conn
        .execute(
            "
            DELETE FROM untracked_state
            WHERE branch_id = ?1 AND schema_key = ?2 AND entity_pk = ?3 AND file_id = ?4
            ",
            params![row.branch_id, row.schema_key, row.entity_pk, row.file_id],
        )
        .expect("execute raw sqlite delete one");
    assert_eq!(affected, 1);
    affected
}

fn pick_pk_row(rows: &[RawUntrackedRow]) -> &RawUntrackedRow {
    &rows[rows.len() / 2]
}

fn operation_logical_rows(operation: &str, row_count: usize) -> usize {
    match operation {
        "select_one_by_pk" | "update_one_by_pk" | "delete_one_by_pk" => 1,
        _ => row_count,
    }
}

fn ratio(numerator: usize, denominator: usize) -> String {
    if denominator == 0 {
        "-".to_string()
    } else {
        format!("{:.2}", numerator as f64 / denominator as f64)
    }
}

fn row_label(row_count: usize) -> &'static str {
    match row_count {
        SMOKE_ROWS => "1k",
        REAL_WORKLOAD_ROWS => "10k",
        _ => "custom",
    }
}

fn projected_value_len(value: ProjectedValueRef<'_>) -> usize {
    match value {
        ProjectedValueRef::KeyOnly => 0,
        ProjectedValueRef::FullValue(value) => value.len(),
    }
}

fn range_bound_len(bound: &Bound<Key>) -> usize {
    match bound {
        Bound::Included(key) | Bound::Excluded(key) => key.0.len(),
        Bound::Unbounded => 0,
    }
}

criterion_group!(benches, untracked_state_crud_benches);
criterion_main!(benches);
