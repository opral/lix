use std::fmt::Write as _;
use std::future::Future;
use std::ops::Bound;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[cfg(all(
    not(target_family = "wasm"),
    not(feature = "system-allocation-profiler")
))]
use std::alloc::GlobalAlloc;
#[cfg(all(
    not(target_family = "wasm"),
    not(feature = "system-allocation-profiler")
))]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bytes::Bytes;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use lix::integration::{Engine, SessionContext};
use lix::storage::{
    BeginScanOptions, CommitResult, GetManyRequest, GetManyResult, Key, KeyRange, ProjectedValue,
    PutBatch, ReadOptions, ScanChunk, ScanCursor, SpaceId, Storage, StorageError, StorageRead,
    StorageScanSource, StorageWrite, WriteOptions,
};
use lix::storage_adapter::{
    PointReadPlan, StorageAdapter, StorageAdapterRead, StorageBeginScanOptions,
    StorageCoreProjection, StorageGetOptions, StoragePrefix, StorageReadOptions, StorageSpace,
    StorageValue, StorageWriteOptions,
};
use lix::{PreparedDmlParameterBatch, Value};
use lix_storage_rocksdb::RocksDB;
#[cfg(feature = "slatedb")]
use lix_storage_slatedb::SlateDB;
use serde_json::Value as JsonValue;
use tempfile::TempDir;
use tokio::runtime::Runtime;

#[cfg(all(
    not(target_family = "wasm"),
    not(feature = "system-allocation-profiler")
))]
#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

#[cfg(all(
    not(target_family = "wasm"),
    not(feature = "system-allocation-profiler")
))]
struct CountingAllocator;

#[cfg(all(
    not(target_family = "wasm"),
    not(feature = "system-allocation-profiler")
))]
static PROFILE_ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
#[cfg(all(
    not(target_family = "wasm"),
    not(feature = "system-allocation-profiler")
))]
static PROFILE_ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);
#[cfg(all(
    not(target_family = "wasm"),
    not(feature = "system-allocation-profiler")
))]
static PROFILE_ALLOCATION_ENABLED: AtomicBool = AtomicBool::new(false);

#[cfg(all(
    not(target_family = "wasm"),
    not(feature = "system-allocation-profiler")
))]
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

#[cfg(all(
    not(target_family = "wasm"),
    not(feature = "system-allocation-profiler")
))]
fn reset_profile_allocations() {
    PROFILE_ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    PROFILE_ALLOCATION_CALLS.store(0, Ordering::Relaxed);
    PROFILE_ALLOCATION_ENABLED.store(true, Ordering::Relaxed);
}

#[cfg(any(target_family = "wasm", feature = "system-allocation-profiler"))]
fn reset_profile_allocations() {}

#[cfg(all(
    not(target_family = "wasm"),
    not(feature = "system-allocation-profiler")
))]
fn profile_allocations() -> (u64, u64) {
    PROFILE_ALLOCATION_ENABLED.store(false, Ordering::Relaxed);
    (
        PROFILE_ALLOCATED_BYTES.load(Ordering::Relaxed),
        PROFILE_ALLOCATION_CALLS.load(Ordering::Relaxed),
    )
}

#[cfg(any(target_family = "wasm", feature = "system-allocation-profiler"))]
fn profile_allocations() -> (u64, u64) {
    (0, 0)
}

const SMOKE_ROWS: usize = 1_000;
const REAL_WORKLOAD_ROWS: usize = 10_000;
const PNPM_LOCK_JSON: &str = include_str!("../fixtures/pnpm-lock.fixture.json");
const JSON_POINTER_SCHEMA_JSON: &str = include_str!("../fixtures/json_pointer.schema.json");
const SESSION_INSERT_CHUNK_SIZE: usize = 500;
const ROW_SPACE: StorageSpace = StorageSpace::mutable(SpaceId(0x00ff_0001), "bench.untracked_row");

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
        requests: &[GetManyRequest<'_>],
    ) -> Result<GetManyResult, StorageError> {
        {
            let mut stats = self.stats.lock().expect("io stats mutex");
            stats.get_calls += 1;
            stats.get_keys += requests
                .iter()
                .map(|request| request.keys.len())
                .sum::<usize>();
            stats.get_key_bytes += requests
                .iter()
                .flat_map(|request| request.keys)
                .map(|key| key.0.len())
                .sum::<usize>();
        }
        let result = self.inner.get_many(requests).await?;
        {
            let mut stats = self.stats.lock().expect("io stats mutex");
            for value in result.values.iter().flatten() {
                stats.get_values += 1;
                stats.get_value_bytes += projected_value_len(value);
            }
        }
        Ok(result)
    }

    async fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> Result<ScanCursor<'_>, StorageError> {
        let order = opts.order;
        {
            let mut stats = self.stats.lock().expect("io stats mutex");
            stats.scan_entry_calls += 1;
        }
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
            let mut stats = self.stats.lock().expect("io stats mutex");
            stats.scan_entries += chunk.len();
            stats.scan_entry_key_bytes += chunk
                .iter()
                .map(|entry| entry.key.0.len())
                .sum::<usize>();
            stats.scan_entry_value_bytes += chunk
                .iter()
                .map(|entry| projected_value_len(&entry.value))
                .sum::<usize>();
            drop(stats);
            Ok(ScanChunk::new(chunk, chunk_has_more))
        })
    }
}

impl<W> StorageWrite for CountingWrite<W>
where
    W: StorageWrite,
{
    async fn put_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
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

    async fn delete_many(&mut self, space: StorageSpace, keys: &[Key]) -> Result<(), StorageError> {
        {
            let mut stats = self.stats.lock().expect("io stats mutex");
            stats.write_batches += 1;
            stats.write_deletes += keys.len();
            stats.write_bytes += keys.iter().map(|key| key.0.len()).sum::<usize>();
        }
        self.inner.delete_many(space, keys).await
    }

    async fn delete_range(
        &mut self,
        space: StorageSpace,
        range: KeyRange,
    ) -> Result<(), StorageError> {
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
    RocksDB,
    #[cfg(feature = "slatedb")]
    SlateDB,
}

#[cfg(not(feature = "slatedb"))]
const LIX_STORAGE_PROFILES: &[LixStorageProfile] = &[LixStorageProfile::RocksDB];
#[cfg(feature = "slatedb")]
const LIX_STORAGE_PROFILES: &[LixStorageProfile] =
    &[LixStorageProfile::RocksDB, LixStorageProfile::SlateDB];

impl LixStorageProfile {
    fn name(self) -> &'static str {
        match self {
            Self::RocksDB => "lix_rocksdb",
            #[cfg(feature = "slatedb")]
            Self::SlateDB => "lix_slatedb",
        }
    }
}

fn untracked_state_crud_benches(c: &mut Criterion) {
    if std::env::var_os("LIX_UNTRACKED_STATE_CRUD_TRACE").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("lix_perf=debug")
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
            .with_target(false)
            .try_init();
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create tokio runtime for session execute benchmarks");
    let rows = fixture_rows();
    if std::env::var_os("LIX_UNTRACKED_STATE_CRUD_PROFILE").is_some() {
        profile_session_untracked_crud(&runtime, &rows);
        return;
    }
    maybe_print_io_report(&runtime, &rows);

    bench_lix(c, &runtime, &rows, SMOKE_ROWS, "smoke");
    bench_session_execute_untracked_insert(c, &runtime, &rows, SMOKE_ROWS, "smoke");
    bench_lix(c, &runtime, &rows, REAL_WORKLOAD_ROWS, "real_workload");
    bench_session_execute_untracked_insert(c, &runtime, &rows, REAL_WORKLOAD_ROWS, "real_workload");
}

fn profile_row_count(max_rows: usize) -> usize {
    std::env::var("LIX_UNTRACKED_STATE_CRUD_PROFILE_ROWS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(REAL_WORKLOAD_ROWS)
        .min(max_rows)
}

fn profile_sample_count() -> usize {
    std::env::var("LIX_UNTRACKED_STATE_CRUD_PROFILE_SAMPLES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|count| *count > 0)
        .unwrap_or(3)
}

fn profile_session_untracked_crud(runtime: &Runtime, all_rows: &[PointerRow]) {
    let rows = &all_rows[..profile_row_count(all_rows.len())];
    let sample_count = profile_sample_count();
    println!(
        "untracked_state_crud/profile rows={} samples={} slate_db={} benchmark_profile=sql_session",
        rows.len(),
        sample_count,
        if cfg!(feature = "slatedb") {
            "enabled"
        } else {
            "unavailable"
        }
    );
    println!(
        "| storage | operation | sample | wall_ms | alloc_bytes | alloc_calls | rss_before_bytes | rss_after_bytes | rows | certified_batches | physical_puts | physical_deletes | physical_written_bytes |"
    );
    println!(
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    );

    let selected_storage = std::env::var("LIX_UNTRACKED_STATE_CRUD_PROFILE_STORAGE").ok();
    let selected_operation = std::env::var("LIX_UNTRACKED_STATE_CRUD_PROFILE_OPERATION").ok();
    let homogeneous = std::env::var_os("LIX_UNTRACKED_STATE_CRUD_PROFILE_HOMOGENEOUS").is_some();
    for &profile in LIX_STORAGE_PROFILES {
        if selected_storage
            .as_deref()
            .is_some_and(|name| name != profile.name())
        {
            continue;
        }
        for operation in ["insert", "update", "delete"] {
            if selected_operation
                .as_deref()
                .is_some_and(|name| name != operation)
            {
                continue;
            }
            for sample in 0..sample_count {
                let session = runtime.block_on(prepare_profile_session_empty(profile));
                if operation != "insert" {
                    runtime.block_on(session.insert_untracked_json_pointer_rows(rows));
                }
                let rss_before = process_resident_bytes();
                let _ =
                    lix::storage_bench::take_certified_entity_insert_parameter_batch_executions();
                let _ = lix::storage_bench::take_crud_physical_write_accounting();
                reset_profile_allocations();
                let started = Instant::now();
                let affected = match operation {
                    "insert" => {
                        if homogeneous {
                            runtime.block_on(
                                session.insert_untracked_json_pointer_rows_homogeneous(rows),
                            );
                        } else {
                            runtime.block_on(session.insert_untracked_json_pointer_rows(rows));
                        }
                        rows.len()
                    }
                    "update" => {
                        runtime.block_on(session.update_untracked_json_pointer_rows(rows));
                        rows.len()
                    }
                    "delete" => runtime.block_on(session.delete_untracked_json_pointer_rows()),
                    _ => unreachable!(),
                };
                let wall_ms = started.elapsed().as_secs_f64() * 1000.0;
                let (alloc_bytes, alloc_calls) = profile_allocations();
                let rss_after = process_resident_bytes();
                let certified_batches =
                    lix::storage_bench::take_certified_entity_insert_parameter_batch_executions();
                let physical = lix::storage_bench::take_crud_physical_write_accounting();
                println!(
                    "| {} | {} | {} | {:.3} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                    profile.name(),
                    operation,
                    sample + 1,
                    wall_ms,
                    alloc_bytes,
                    alloc_calls,
                    rss_before,
                    rss_after,
                    affected,
                    certified_batches,
                    physical.puts,
                    physical.deletes,
                    physical.written_bytes,
                );
            }
        }
    }
}

fn process_resident_bytes() -> u64 {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .map_or(0, |kilobytes| kilobytes.saturating_mul(1024))
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
        for &profile in LIX_STORAGE_PROFILES {
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

fn bench_lix(
    c: &mut Criterion,
    runtime: &Runtime,
    all_rows: &[PointerRow],
    row_count: usize,
    label: &str,
) {
    let rows = bench_rows(&all_rows[..row_count]);
    for &profile in LIX_STORAGE_PROFILES {
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
    for &profile in LIX_STORAGE_PROFILES {
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
        LixStorageProfile::RocksDB => measure_lix_io_for_storage(rocksdb(), operation, rows).await,
        #[cfg(feature = "slatedb")]
        LixStorageProfile::SlateDB => measure_lix_io_for_storage(slatedb(), operation, rows).await,
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
    let mut cursor = read
        .begin_scan(
            ROW_SPACE,
            StoragePrefix {
                bytes: Bytes::new(),
            }
            .to_range()
            .expect("empty prefix range"),
            StorageBeginScanOptions {
                projection,
                ..StorageBeginScanOptions::default()
            },
        )
        .await
        .expect("begin row scan");
    let (page, _page_has_more) = cursor
        .next_page(expected_rows + 1)
        .await
        .expect("scan rows").into_parts();
    assert_eq!(page.len(), expected_rows);
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
        LixStorageProfile::RocksDB => ProfileStorage::RocksDB(StorageAdapter::new(rocksdb())),
        #[cfg(feature = "slatedb")]
        LixStorageProfile::SlateDB => ProfileStorage::SlateDB(StorageAdapter::new(slatedb())),
    }
}

enum ProfileStorage {
    RocksDB(StorageAdapter<TempStorage<RocksDB>>),
    #[cfg(feature = "slatedb")]
    SlateDB(StorageAdapter<TempStorage<SlateDB>>),
}

enum ProfileSession {
    RocksDB(SessionContext<TempStorage<RocksDB>>),
    #[cfg(feature = "slatedb")]
    SlateDB(SessionContext<TempStorage<SlateDB>>),
}

async fn prepare_profile_session_empty(profile: LixStorageProfile) -> ProfileSession {
    match profile {
        LixStorageProfile::RocksDB => {
            ProfileSession::RocksDB(prepare_session_empty(rocksdb()).await)
        }
        #[cfg(feature = "slatedb")]
        LixStorageProfile::SlateDB => {
            ProfileSession::SlateDB(prepare_session_empty(slatedb()).await)
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
        .open_session()
        .await
        .expect("open benchmark setup session");
    register_json_pointer_schema(&setup).await;
    engine
        .open_session()
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
    let chunk_size = std::env::var("LIX_UNTRACKED_STATE_CRUD_PROFILE_CHUNK")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|size| *size > 0)
        .unwrap_or(SESSION_INSERT_CHUNK_SIZE);
    for chunk in rows.chunks(chunk_size) {
        let sql = insert_untracked_json_pointer_sql(chunk);
        let affected = session
            .execute(&sql, &[])
            .await
            .expect("insert untracked json_pointer rows")
            .rows_affected();
        assert_eq!(affected as usize, chunk.len());
    }
}

async fn insert_untracked_json_pointer_rows_homogeneous<StorageImpl>(
    session: &SessionContext<StorageImpl>,
    rows: &[PointerRow],
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let parameter_batch = PreparedDmlParameterBatch::from_rows(rows.iter().map(|row| {
        vec![
            Value::Text(row.path.clone()),
            Value::Text(row.value_json.clone()),
        ]
    }))
    .expect("untracked parameter batch is rectangular");
    let results = session
        .execute_prepared_dml_batch(
            Arc::<str>::from(
                "INSERT INTO json_pointer (path, value, lixcol_untracked) VALUES ($1, lix_json($2), true)",
            ),
            parameter_batch,
        )
        .await
        .expect("homogeneous insert untracked json_pointer rows");
    assert_eq!(results.len(), rows.len());
    assert!(results.iter().all(|result| result.rows_affected() == 1));
}

async fn update_untracked_json_pointer_rows<StorageImpl>(
    session: &SessionContext<StorageImpl>,
    rows: &[PointerRow],
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let sql = update_untracked_json_pointer_sql(rows);
    let affected = session
        .execute(&sql, &[])
        .await
        .expect("update untracked json_pointer rows")
        .rows_affected();
    assert_eq!(affected as usize, rows.len());
}

async fn delete_untracked_json_pointer_rows<StorageImpl>(
    session: &SessionContext<StorageImpl>,
) -> usize
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute(
            "DELETE FROM json_pointer WHERE lixcol_untracked = true",
            &[],
        )
        .await
        .expect("delete untracked json_pointer rows")
        .rows_affected() as usize
}

impl ProfileStorage {
    async fn insert_all(&self, rows: &[BenchRow]) -> usize {
        match self {
            Self::RocksDB(storage) => lix_insert_all(storage, rows).await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(storage) => lix_insert_all(storage, rows).await,
        }
    }

    async fn update_all(&self, rows: &[BenchRow]) -> usize {
        match self {
            Self::RocksDB(storage) => lix_update_all(storage, rows).await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(storage) => lix_update_all(storage, rows).await,
        }
    }

    async fn delete_one(&self, row: &BenchRow) -> usize {
        match self {
            Self::RocksDB(storage) => lix_delete_one(storage, row).await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(storage) => lix_delete_one(storage, row).await,
        }
    }

    async fn delete_all(&self) -> usize {
        match self {
            Self::RocksDB(storage) => lix_delete_all(storage).await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(storage) => lix_delete_all(storage).await,
        }
    }

    async fn select_all(&self, expected_rows: usize, projection: StorageCoreProjection) -> usize {
        match self {
            Self::RocksDB(storage) => lix_select_all(storage, expected_rows, projection).await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(storage) => lix_select_all(storage, expected_rows, projection).await,
        }
    }

    async fn select_points(&self, rows: &[BenchRow]) -> usize {
        match self {
            Self::RocksDB(storage) => lix_select_points(storage, rows).await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(storage) => lix_select_points(storage, rows).await,
        }
    }
}

impl ProfileSession {
    async fn insert_untracked_json_pointer_rows(&self, rows: &[PointerRow]) {
        match self {
            Self::RocksDB(session) => insert_untracked_json_pointer_rows(session, rows).await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(session) => insert_untracked_json_pointer_rows(session, rows).await,
        }
    }

    async fn insert_untracked_json_pointer_rows_homogeneous(&self, rows: &[PointerRow]) {
        match self {
            Self::RocksDB(session) => {
                insert_untracked_json_pointer_rows_homogeneous(session, rows).await;
            }
            #[cfg(feature = "slatedb")]
            Self::SlateDB(session) => {
                insert_untracked_json_pointer_rows_homogeneous(session, rows).await;
            }
        }
    }

    async fn update_untracked_json_pointer_rows(&self, rows: &[PointerRow]) {
        match self {
            Self::RocksDB(session) => update_untracked_json_pointer_rows(session, rows).await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(session) => update_untracked_json_pointer_rows(session, rows).await,
        }
    }

    async fn delete_untracked_json_pointer_rows(&self) -> usize {
        match self {
            Self::RocksDB(session) => delete_untracked_json_pointer_rows(session).await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(session) => delete_untracked_json_pointer_rows(session).await,
        }
    }
}

fn rocksdb() -> TempStorage<RocksDB> {
    let dir = TempDir::new().expect("create rocksdb storage tempdir");
    let path = dir.path().join("bench.rocksdb");
    TempStorage::new(RocksDB::open(path).expect("open rocksdb storage"), dir)
}

#[cfg(feature = "slatedb")]
fn slatedb() -> TempStorage<SlateDB> {
    let dir = TempDir::new().expect("create slatedb storage tempdir");
    let path = dir.path().join("bench.slatedb");
    TempStorage::new(SlateDB::open(path).expect("open slatedb storage"), dir)
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

fn update_untracked_json_pointer_sql(rows: &[PointerRow]) -> String {
    let value = rows
        .first()
        .map_or("{}", |row| row.updated_value_json.as_str());
    format!(
        "UPDATE json_pointer SET value = lix_json('{}') WHERE lixcol_untracked = true",
        sql_string(value)
    )
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
