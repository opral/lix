use std::fmt::Write as _;
use std::ops::Bound;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
use lix_engine::storage::{
    CommitResult, GetManyResult, GetOptions, Key, KeyRange, ProjectedValue, PutBatch, ReadOptions,
    ScanChunk, ScanOptions, SpaceId, StorageError, StorageRead, StorageWrite, WriteOptions,
};
use lix_engine::storage_adapter::{
    PointReadPlan, ScanPlan, StorageAdapter, StorageCoreProjection, StorageGetOptions,
    StoragePrefix, StorageReadOptions, StorageScanOptions, StorageSpace, StorageValue,
    StorageWriteOptions,
};
use lix_engine::{Engine, SessionContext, Storage};
use lix_rocksdb_storage::RocksDB;
use lix_sqlite_storage::SQLite;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value as JsonValue;
use tempfile::TempDir;
use tokio::runtime::Runtime;

const SMOKE_ROWS: usize = 1_000;
const REAL_WORKLOAD_ROWS: usize = 10_000;
const PNPM_LOCK_JSON: &str = include_str!("../fixtures/pnpm-lock.fixture.json");
const JSON_POINTER_SCHEMA_JSON: &str = include_str!("../fixtures/json_pointer.schema.json");
const SESSION_INSERT_CHUNK_SIZE: usize = 500;
const ROW_SPACE: StorageSpace = StorageSpace::new(SpaceId(0x00ff_0001), "bench.untracked_row");

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

struct RawSQLiteFixture {
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
struct CountingStorage<StorageImpl> {
    inner: StorageImpl,
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

#[derive(Clone)]
struct TempStorage<StorageImpl> {
    inner: StorageImpl,
    _dir: Arc<TempDir>,
}

impl<StorageImpl> TempStorage<StorageImpl> {
    fn new(inner: StorageImpl, dir: TempDir) -> Self {
        Self {
            inner,
            _dir: Arc::new(dir),
        }
    }
}

impl<StorageImpl> Storage for TempStorage<StorageImpl>
where
    StorageImpl: Storage,
{
    type Read<'a>
        = StorageImpl::Read<'a>
    where
        Self: 'a;

    type Write<'a>
        = StorageImpl::Write<'a>
    where
        Self: 'a;

    async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.inner.begin_read(opts).await
    }

    async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.inner.begin_write(opts).await
    }
}

impl<StorageImpl> CountingStorage<StorageImpl> {
    fn new(inner: StorageImpl) -> (Self, Arc<Mutex<IoStats>>) {
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

impl<StorageImpl> Storage for CountingStorage<StorageImpl>
where
    StorageImpl: Storage,
{
    type Read<'a>
        = CountingRead<StorageImpl::Read<'a>>
    where
        Self: 'a;

    type Write<'a>
        = CountingWrite<StorageImpl::Write<'a>>
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

impl<R> StorageRead for CountingRead<R>
where
    R: StorageRead,
{
    async fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions,
    ) -> Result<GetManyResult, StorageError> {
        {
            let mut stats = self.stats.lock().expect("io stats mutex");
            stats.get_calls += 1;
            stats.get_keys += keys.len();
            stats.get_key_bytes += keys.iter().map(|key| key.0.len()).sum::<usize>();
        }
        let result = self.inner.get_many(space, keys, opts).await?;
        {
            let mut stats = self.stats.lock().expect("io stats mutex");
            for value in result.values.iter().flatten() {
                stats.get_values += 1;
                stats.get_value_bytes += projected_value_len(value);
            }
        }
        Ok(result)
    }

    async fn scan(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions,
    ) -> Result<ScanChunk, StorageError> {
        {
            let mut stats = self.stats.lock().expect("io stats mutex");
            stats.scan_entry_calls += 1;
        }
        let chunk = self.inner.scan(space, range, opts).await?;
        {
            let mut stats = self.stats.lock().expect("io stats mutex");
            stats.scan_entries += chunk.entries.len();
            stats.scan_entry_key_bytes += chunk
                .entries
                .iter()
                .map(|entry| entry.key.0.len())
                .sum::<usize>();
            stats.scan_entry_value_bytes += chunk
                .entries
                .iter()
                .map(|entry| projected_value_len(&entry.value))
                .sum::<usize>();
        }
        Ok(chunk)
    }
}

impl<W> StorageWrite for CountingWrite<W>
where
    W: StorageWrite,
{
    async fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), StorageError> {
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
        self.inner.put_many(space, entries).await
    }

    async fn delete_many(&mut self, space: SpaceId, keys: &[Key]) -> Result<(), StorageError> {
        {
            let mut stats = self.stats.lock().expect("io stats mutex");
            stats.write_batches += 1;
            stats.write_deletes += keys.len();
            stats.write_bytes += keys.iter().map(|key| key.0.len()).sum::<usize>();
        }
        self.inner.delete_many(space, keys).await
    }

    async fn delete_range(&mut self, space: SpaceId, range: KeyRange) -> Result<(), StorageError> {
        {
            let mut stats = self.stats.lock().expect("io stats mutex");
            stats.write_batches += 1;
            stats.write_delete_ranges += 1;
            stats.write_bytes += range_bound_len(&range.lower) + range_bound_len(&range.upper);
        }
        self.inner.delete_range(space, range).await
    }

    async fn commit(self) -> Result<CommitResult, StorageError>
    where
        Self: Sized,
    {
        self.inner.commit().await
    }

    async fn rollback(self) -> Result<(), StorageError>
    where
        Self: Sized,
    {
        self.inner.rollback().await
    }
}

#[derive(Clone, Copy)]
enum LixStorageProfile {
    SQLite,
    RocksDB,
}

const LIX_STORAGE_PROFILES: [LixStorageProfile; 2] =
    [LixStorageProfile::SQLite, LixStorageProfile::RocksDB];

impl LixStorageProfile {
    fn name(self) -> &'static str {
        match self {
            Self::SQLite => "lix_sqlite",
            Self::RocksDB => "lix_rocksdb",
        }
    }
}

fn untracked_state_crud_benches(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create tokio runtime for session execute benchmarks");
    let rows = fixture_rows();
    maybe_print_io_report(&runtime, &rows);

    bench_raw_sqlite(c, &rows, SMOKE_ROWS, "smoke");
    bench_lix(c, &runtime, &rows, SMOKE_ROWS, "smoke");
    bench_session_execute_untracked_insert(c, &runtime, &rows, SMOKE_ROWS, "smoke");
    bench_raw_sqlite(c, &rows, REAL_WORKLOAD_ROWS, "real_workload");
    bench_lix(c, &runtime, &rows, REAL_WORKLOAD_ROWS, "real_workload");
    bench_session_execute_untracked_insert(c, &runtime, &rows, REAL_WORKLOAD_ROWS, "real_workload");
}

fn maybe_print_io_report(runtime: &Runtime, all_rows: &[PointerRow]) {
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
    println!(
        "logical storage_v2 storage request/result accounting; not physical disk, WAL, or compaction I/O"
    );
    println!(
        "| workload | storage | operation | logical rows | io ops | io ops/row | io bytes | io bytes/row | read calls | get calls | get keys | scan calls | read rows | read bytes | read bytes/row | write batches | puts | deletes | delete ranges | write bytes | write bytes/row |"
    );
    println!(
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    );

    for (label, row_count) in workloads {
        let rows = bench_rows(&all_rows[..row_count]);
        for profile in LIX_STORAGE_PROFILES {
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
                let stats = runtime.block_on(measure_lix_io(profile, operation, &rows));
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
        );
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
            );
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
            );
        },
    );
    group.bench_function(format!("select_all_rows/{}", row_label(row_count)), |b| {
        b.iter_batched(
            || prepare_raw_sqlite_seeded(&rows),
            |fixture| black_box(raw_sqlite_select_all(fixture, row_count)),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("select_one_by_pk/{}", row_label(row_count)), |b| {
        b.iter_batched(
            || prepare_raw_sqlite_seeded(&rows),
            |fixture| black_box(raw_sqlite_select_one_by_pk(fixture, pick_pk_row(&rows))),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("update_all_rows/{}", row_label(row_count)), |b| {
        b.iter_batched(
            || prepare_raw_sqlite_seeded(&rows),
            |fixture| black_box(raw_sqlite_update_all(fixture, &rows)),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("update_one_by_pk/{}", row_label(row_count)), |b| {
        b.iter_batched(
            || prepare_raw_sqlite_seeded(&rows),
            |fixture| black_box(raw_sqlite_update_one_by_pk(fixture, pick_pk_row(&rows))),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("delete_all_rows/{}", row_label(row_count)), |b| {
        b.iter_batched(
            || prepare_raw_sqlite_seeded(&rows),
            |fixture| black_box(raw_sqlite_delete_all(fixture, row_count)),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("delete_one_by_pk/{}", row_label(row_count)), |b| {
        b.iter_batched(
            || prepare_raw_sqlite_seeded(&rows),
            |fixture| black_box(raw_sqlite_delete_one_by_pk(fixture, pick_pk_row(&rows))),
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

fn bench_lix(
    c: &mut Criterion,
    runtime: &Runtime,
    all_rows: &[PointerRow],
    row_count: usize,
    label: &str,
) {
    let rows = bench_rows(&all_rows[..row_count]);
    for profile in LIX_STORAGE_PROFILES {
        let mut group =
            c.benchmark_group(format!("untracked_state_crud/{}/{label}", profile.name()));
        configure_group(&mut group, row_count);

        bench_lix_profile(&mut group, runtime, profile, &rows);
        group.finish();
    }
}

fn bench_lix_profile(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    runtime: &Runtime,
    profile: LixStorageProfile,
    rows: &[BenchRow],
) {
    group.bench_function(format!("insert_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || profile_storage(profile),
            |storage| {
                retain_fixture(storage, |storage| {
                    runtime.block_on(storage.insert_all(rows))
                })
            },
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("select_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || runtime.block_on(prepare_lix_seeded(profile, rows)),
            |storage| {
                retain_fixture(storage, |storage| {
                    runtime
                        .block_on(storage.select_all(rows.len(), StorageCoreProjection::FullValue))
                })
            },
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("select_keys_only/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || runtime.block_on(prepare_lix_seeded(profile, rows)),
            |storage| {
                retain_fixture(storage, |storage| {
                    runtime.block_on(storage.select_all(rows.len(), StorageCoreProjection::KeyOnly))
                })
            },
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("select_one_by_pk/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || runtime.block_on(prepare_lix_seeded(profile, rows)),
            |storage| {
                retain_fixture(storage, |storage| {
                    runtime.block_on(
                        storage.select_points(std::slice::from_ref(&rows[rows.len() / 2])),
                    )
                })
            },
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("select_all_by_pk/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || runtime.block_on(prepare_lix_seeded(profile, rows)),
            |storage| {
                retain_fixture(storage, |storage| {
                    runtime.block_on(storage.select_points(rows))
                })
            },
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("update_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || runtime.block_on(prepare_lix_seeded(profile, rows)),
            |storage| {
                retain_fixture(storage, |storage| {
                    runtime.block_on(storage.update_all(rows))
                })
            },
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("update_one_by_pk/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || runtime.block_on(prepare_lix_seeded(profile, rows)),
            |storage| {
                retain_fixture(storage, |storage| {
                    runtime.block_on(storage.update_all(&rows[..1]))
                })
            },
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("delete_all_rows/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || runtime.block_on(prepare_lix_seeded(profile, rows)),
            |storage| retain_fixture(storage, |storage| runtime.block_on(storage.delete_all())),
            BatchSize::LargeInput,
        );
    });
    group.bench_function(format!("delete_one_by_pk/{}", row_label(rows.len())), |b| {
        b.iter_batched(
            || runtime.block_on(prepare_lix_seeded(profile, rows)),
            |storage| {
                retain_fixture(storage, |storage| {
                    runtime.block_on(storage.delete_one(&rows[rows.len() / 2]))
                })
            },
            BatchSize::LargeInput,
        );
    });
}

fn bench_session_execute_untracked_insert(
    c: &mut Criterion,
    runtime: &Runtime,
    all_rows: &[PointerRow],
    row_count: usize,
    label: &str,
) {
    let rows = all_rows[..row_count].to_vec();
    for profile in LIX_STORAGE_PROFILES {
        let mut group = c.benchmark_group(format!(
            "untracked_state_crud/session_execute_untracked/{}/{label}",
            profile.name()
        ));
        configure_group(&mut group, row_count);

        group.bench_function(format!("insert_all_rows/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || runtime.block_on(prepare_profile_session_empty(profile)),
                |session| {
                    retain_fixture(session, |session| {
                        runtime.block_on(session.insert_untracked_json_pointer_rows(&rows));
                        rows.len()
                    })
                },
                BatchSize::LargeInput,
            );
        });

        group.finish();
    }
}

/// Keeps fixture destruction (including recursive TempDir cleanup) outside
/// Criterion's measured routine interval.
fn retain_fixture<I, O>(fixture: I, routine: impl FnOnce(&I) -> O) -> (O, I) {
    let output = routine(&fixture);
    (output, fixture)
}

async fn measure_lix_io(profile: LixStorageProfile, operation: &str, rows: &[BenchRow]) -> IoStats {
    match profile {
        LixStorageProfile::SQLite => measure_lix_io_for_storage(sqlite(), operation, rows).await,
        LixStorageProfile::RocksDB => measure_lix_io_for_storage(rocksdb(), operation, rows).await,
    }
}

async fn measure_lix_io_for_storage<StorageImpl>(
    storage: StorageImpl,
    operation: &str,
    rows: &[BenchRow],
) -> IoStats
where
    StorageImpl: Storage,
{
    let (storage, stats) = CountingStorage::new(storage);
    let storage = StorageAdapter::new(storage);
    if !matches!(operation, "insert_all_rows") {
        lix_insert_all(&storage, rows).await;
        stats.lock().expect("io stats mutex").reset();
    }
    match operation {
        "insert_all_rows" => {
            lix_insert_all(&storage, rows).await;
        }
        "select_all_rows" => {
            lix_select_all(&storage, rows.len(), StorageCoreProjection::FullValue).await;
            record_scan_result(&stats, rows, true);
        }
        "select_keys_only" => {
            lix_select_all(&storage, rows.len(), StorageCoreProjection::KeyOnly).await;
            record_scan_result(&stats, rows, false);
        }
        "select_one_by_pk" => {
            lix_select_points(&storage, std::slice::from_ref(&rows[rows.len() / 2])).await;
        }
        "select_all_by_pk" => {
            lix_select_points(&storage, rows).await;
        }
        "update_all_rows" => {
            lix_update_all(&storage, rows).await;
        }
        "update_one_by_pk" => {
            lix_update_all(&storage, &rows[..1]).await;
        }
        "delete_all_rows" => {
            lix_delete_all(&storage).await;
        }
        "delete_one_by_pk" => {
            lix_delete_one(&storage, &rows[rows.len() / 2]).await;
        }
        _ => unreachable!("unknown operation"),
    }

    stats.lock().expect("io stats mutex").clone()
}

fn record_scan_result(stats: &Arc<Mutex<IoStats>>, rows: &[BenchRow], include_values: bool) {
    let mut stats = stats.lock().expect("io stats mutex");
    stats.scan_entries += rows.len();
    stats.scan_entry_key_bytes += rows.iter().map(|row| row.key.0.len()).sum::<usize>();
    if include_values {
        stats.scan_entry_value_bytes += rows.iter().map(|row| row.value.bytes.len()).sum::<usize>();
    }
}

async fn lix_insert_all<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
    rows: &[BenchRow],
) -> usize
where
    StorageImpl: Storage,
{
    let mut writes = storage.new_write_set();
    for row in rows {
        writes.put(ROW_SPACE, row.key.clone(), row.value.clone());
    }
    let (_commit, stats) = storage
        .commit_write_set(writes, StorageWriteOptions::default())
        .await
        .expect("commit insert rows");
    assert_eq!(stats.staged_puts, rows.len() as u64);
    rows.len()
}

async fn lix_update_all<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
    rows: &[BenchRow],
) -> usize
where
    StorageImpl: Storage,
{
    let mut writes = storage.new_write_set();
    for row in rows {
        writes.put(ROW_SPACE, row.key.clone(), row.updated_value.clone());
    }
    let (_commit, stats) = storage
        .commit_write_set(writes, StorageWriteOptions::default())
        .await
        .expect("commit update rows");
    assert_eq!(stats.staged_puts, rows.len() as u64);
    rows.len()
}

async fn lix_delete_one<StorageImpl>(storage: &StorageAdapter<StorageImpl>, row: &BenchRow) -> usize
where
    StorageImpl: Storage,
{
    let mut writes = storage.new_write_set();
    writes.delete(ROW_SPACE, row.key.clone());
    let (_commit, stats) = storage
        .commit_write_set(writes, StorageWriteOptions::default())
        .await
        .expect("commit delete row");
    assert_eq!(stats.staged_deletes, 1);
    1
}

async fn lix_delete_all<StorageImpl>(storage: &StorageAdapter<StorageImpl>) -> usize
where
    StorageImpl: Storage,
{
    storage
        .clear_space(ROW_SPACE, StorageWriteOptions::default())
        .await
        .expect("clear untracked rows");
    1
}

async fn lix_select_all<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
    expected_rows: usize,
    projection: StorageCoreProjection,
) -> usize
where
    StorageImpl: Storage,
{
    let read = storage
        .begin_read(StorageReadOptions::default())
        .await
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
        .await
        .expect("scan rows");
    assert_eq!(page.value.entries.len(), expected_rows);
    expected_rows
}

async fn lix_select_points<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
    rows: &[BenchRow],
) -> usize
where
    StorageImpl: Storage,
{
    let read = storage
        .begin_read(StorageReadOptions::default())
        .await
        .expect("begin read");
    let keys = rows.iter().map(|row| row.key.clone()).collect::<Vec<_>>();
    let result = PointReadPlan::new(ROW_SPACE, &keys)
        .materialize(&read, StorageGetOptions::default())
        .await
        .expect("point read rows");
    assert_eq!(result.value.len(), rows.len());
    assert!(result.value.iter().all(Option::is_some));
    result.value.len()
}

async fn prepare_lix_seeded(profile: LixStorageProfile, rows: &[BenchRow]) -> ProfileStorage {
    let storage = profile_storage(profile);
    storage.insert_all(rows).await;
    storage
}

fn profile_storage(profile: LixStorageProfile) -> ProfileStorage {
    match profile {
        LixStorageProfile::SQLite => ProfileStorage::SQLite(StorageAdapter::new(sqlite())),
        LixStorageProfile::RocksDB => ProfileStorage::RocksDB(StorageAdapter::new(rocksdb())),
    }
}

enum ProfileStorage {
    SQLite(StorageAdapter<TempStorage<SQLite>>),
    RocksDB(StorageAdapter<TempStorage<RocksDB>>),
}

enum ProfileSession {
    SQLite(SessionContext<TempStorage<SQLite>>),
    RocksDB(SessionContext<TempStorage<RocksDB>>),
}

async fn prepare_profile_session_empty(profile: LixStorageProfile) -> ProfileSession {
    match profile {
        LixStorageProfile::SQLite => {
            ProfileSession::SQLite(Box::pin(prepare_session_empty(sqlite())).await)
        }
        LixStorageProfile::RocksDB => {
            ProfileSession::RocksDB(Box::pin(prepare_session_empty(rocksdb())).await)
        }
    }
}

async fn prepare_session_empty<StorageImpl>(storage: StorageImpl) -> SessionContext<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    Engine::initialize(storage.clone())
        .await
        .expect("initialize benchmark engine");
    let engine = Engine::new(storage).await.expect("open in-memory engine");
    let setup = engine
        .open_workspace_session()
        .await
        .expect("open benchmark setup session");
    register_json_pointer_schema(&setup).await;
    engine
        .open_workspace_session()
        .await
        .expect("open benchmark session")
}

async fn register_json_pointer_schema<StorageImpl>(session: &SessionContext<StorageImpl>)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
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

#[expect(clippy::cast_possible_truncation)]
async fn insert_untracked_json_pointer_rows<StorageImpl>(
    session: &SessionContext<StorageImpl>,
    rows: &[PointerRow],
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
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
    async fn insert_all(&self, rows: &[BenchRow]) -> usize {
        match self {
            Self::SQLite(storage) => lix_insert_all(storage, rows).await,
            Self::RocksDB(storage) => lix_insert_all(storage, rows).await,
        }
    }

    async fn update_all(&self, rows: &[BenchRow]) -> usize {
        match self {
            Self::SQLite(storage) => lix_update_all(storage, rows).await,
            Self::RocksDB(storage) => lix_update_all(storage, rows).await,
        }
    }

    async fn delete_one(&self, row: &BenchRow) -> usize {
        match self {
            Self::SQLite(storage) => lix_delete_one(storage, row).await,
            Self::RocksDB(storage) => lix_delete_one(storage, row).await,
        }
    }

    async fn delete_all(&self) -> usize {
        match self {
            Self::SQLite(storage) => lix_delete_all(storage).await,
            Self::RocksDB(storage) => lix_delete_all(storage).await,
        }
    }

    async fn select_all(&self, expected_rows: usize, projection: StorageCoreProjection) -> usize {
        match self {
            Self::SQLite(storage) => lix_select_all(storage, expected_rows, projection).await,
            Self::RocksDB(storage) => lix_select_all(storage, expected_rows, projection).await,
        }
    }

    async fn select_points(&self, rows: &[BenchRow]) -> usize {
        match self {
            Self::SQLite(storage) => lix_select_points(storage, rows).await,
            Self::RocksDB(storage) => lix_select_points(storage, rows).await,
        }
    }
}

impl ProfileSession {
    async fn insert_untracked_json_pointer_rows(&self, rows: &[PointerRow]) {
        match self {
            Self::SQLite(session) => insert_untracked_json_pointer_rows(session, rows).await,
            Self::RocksDB(session) => insert_untracked_json_pointer_rows(session, rows).await,
        }
    }
}

fn sqlite() -> TempStorage<SQLite> {
    let dir = TempDir::new().expect("create sqlite storage tempdir");
    let path = dir.path().join("bench.sqlite");
    TempStorage::new(SQLite::open(path).expect("open sqlite storage"), dir)
}

fn rocksdb() -> TempStorage<RocksDB> {
    let dir = TempDir::new().expect("create rocksdb storage tempdir");
    let path = dir.path().join("bench.rocksdb");
    TempStorage::new(RocksDB::open(path).expect("open rocksdb storage"), dir)
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
        let _ = write!(
            sql,
            "('{}', lix_json('{}'), true)",
            sql_string(row.path.as_str()),
            sql_string(row.value_json.as_str())
        );
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
    value.map_or_else(
        || "NULL".to_string(),
        |value| format!("'{}'", sql_string(value)),
    )
}

fn prepare_raw_sqlite_empty() -> RawSQLiteFixture {
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
    RawSQLiteFixture { conn, _dir: dir }
}

fn prepare_raw_sqlite_seeded(rows: &[RawUntrackedRow]) -> RawSQLiteFixture {
    raw_sqlite_insert_all(prepare_raw_sqlite_empty(), rows)
}

fn raw_sqlite_insert_all(
    mut fixture: RawSQLiteFixture,
    rows: &[RawUntrackedRow],
) -> RawSQLiteFixture {
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
                    i64::from(row.global),
                ])
                .expect("execute raw sqlite insert");
        }
    }
    tx.commit().expect("commit raw sqlite insert");
    fixture
}

fn raw_sqlite_insert_all_unprepared_per_row(
    mut fixture: RawSQLiteFixture,
    rows: &[RawUntrackedRow],
) -> RawSQLiteFixture {
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
    mut fixture: RawSQLiteFixture,
    rows: &[RawUntrackedRow],
) -> RawSQLiteFixture {
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
        i64::from(row.global),
    )
    .expect("write raw sqlite insert tuple SQL");
}

fn raw_sqlite_select_all(fixture: RawSQLiteFixture, expected_rows: usize) -> usize {
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

fn raw_sqlite_select_one_by_pk(fixture: RawSQLiteFixture, row: &RawUntrackedRow) -> usize {
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

fn raw_sqlite_update_all(mut fixture: RawSQLiteFixture, rows: &[RawUntrackedRow]) -> usize {
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

fn raw_sqlite_update_one_by_pk(fixture: RawSQLiteFixture, row: &RawUntrackedRow) -> usize {
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

fn raw_sqlite_delete_all(fixture: RawSQLiteFixture, expected_rows: usize) -> usize {
    let affected = fixture
        .conn
        .execute("DELETE FROM untracked_state", [])
        .expect("execute raw sqlite delete all");
    assert_eq!(affected, expected_rows);
    affected
}

fn raw_sqlite_delete_one_by_pk(fixture: RawSQLiteFixture, row: &RawUntrackedRow) -> usize {
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

#[expect(clippy::cast_precision_loss)]
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

fn projected_value_len(value: &ProjectedValue) -> usize {
    match value {
        ProjectedValue::KeyOnly => 0,
        ProjectedValue::FullValue(value) => value.len(),
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
