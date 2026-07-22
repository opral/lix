use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use futures_util::StreamExt;
use futures_util::stream::{self, BoxStream};
use lix_engine::storage::{
    GetOptions as StorageGetOptions, Key, ProjectedValue, PutBatch, PutEntry, ReadOptions, SpaceId,
    StorageRead, StorageWrite, StoredValue, WriteOptions,
};
#[cfg(feature = "storage-benches")]
use lix_engine::storage_adapter::StorageAdapter;
#[cfg(feature = "storage-benches")]
use lix_engine::transaction::bench::{
    BenchTransactionFixture, BenchTransactionRow, BenchWriteAccounting,
};
use lix_engine::{Engine, SessionContext, Storage, Value};
use lix_slatedb_storage::{SlateDB, SlateDBCacheOptions, SlateDBObjectStoreOptions};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result as ObjectStoreResult,
};
use serde_json::json;
use tempfile::TempDir;

const DELAYS_MS: &[u64] = &[0, 10, 25, 50];
const SEED_FILE_COUNT: usize = 100;
const FILE_SIZE_BYTES: usize = 4096;
const UPLOAD_BATCH_SIZE: usize = 10;
const LIXRAY_UPLOAD_BATCH_FILES: usize = 10;
const BENCH_DISK_CACHE_BYTES: usize = 64 * 1024 * 1024;
const BENCH_BLOCK_CACHE_BYTES: u64 = 16 * 1024 * 1024;
const BENCH_METADATA_CACHE_BYTES: u64 = 4 * 1024 * 1024;
const FRESH_ENGINE_DELAYS_MS: &[u64] = &[0, 25];
const CONCURRENCY_DELAY_MS: u64 = 10;
const CONCURRENCY_REQUESTS: usize = 4;
const CONCURRENCY_VALUE_BYTES: usize = 16 * 1024;
const CONCURRENCY_SPACE: SpaceId = SpaceId(0x00ff_0001);
#[cfg(feature = "storage-benches")]
const SINGLETON_ACCOUNTING_ROWS: usize = 1_000;
#[cfg(feature = "storage-benches")]
const SINGLETON_ACCOUNTING_SAMPLES: usize = 50;

static NEXT_DB_ID: AtomicU64 = AtomicU64::new(0);

fn slatedb_file_api_benches(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create tokio runtime for slatedb_file_api benchmarks");
    maybe_print_write_accounting(&runtime);
    #[cfg(feature = "storage-benches")]
    maybe_print_singleton_write_accounting(&runtime);
    let mut group = c.benchmark_group("slatedb_file_api_warm_cache");
    group.sample_size(10);

    for &delay_ms in DELAYS_MS {
        let delay = Duration::from_millis(delay_ms);
        let delay_label = format!("{delay_ms}ms");

        group.bench_with_input(
            BenchmarkId::new("accept_overwrite_file_after_preload", &delay_label),
            &delay,
            |b, &delay| {
                b.iter_custom(|iterations| {
                    let fixture = runtime.block_on(UploadBenchFixture::seeded(delay));
                    black_box(runtime.block_on(fixture.upload_overwrite_file()));
                    measure_iterations(iterations, || {
                        runtime.block_on(fixture.upload_overwrite_file())
                    })
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("upload_overwrite_10_files_after_preload", &delay_label),
            &delay,
            |b, &delay| {
                b.iter_custom(|iterations| {
                    let fixture = runtime.block_on(UploadBenchFixture::seeded(delay));
                    black_box(runtime.block_on(fixture.upload_overwrite_file_batch()));
                    measure_iterations(iterations, || {
                        runtime.block_on(fixture.upload_overwrite_file_batch())
                    })
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("list_root_directory_after_preload", &delay_label),
            &delay,
            |b, &delay| {
                b.iter_custom(|iterations| {
                    let fixture = runtime.block_on(ReadBenchFixture::seeded(delay));
                    black_box(runtime.block_on(fixture.list_root_directory()));
                    measure_iterations(iterations, || {
                        runtime.block_on(fixture.list_root_directory())
                    })
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("download_file_after_preload", delay_label),
            &delay,
            |b, &delay| {
                b.iter_custom(|iterations| {
                    let fixture = runtime.block_on(ReadBenchFixture::seeded(delay));
                    black_box(runtime.block_on(fixture.download_file()));
                    measure_iterations(iterations, || runtime.block_on(fixture.download_file()))
                });
            },
        );
    }

    group.finish();

    cached_cold_lifecycle_benches(c, &runtime);
    cached_preloaded_request_benches(c, &runtime);
    fresh_engine_select_benches(c, &runtime);
    storage_concurrency_benches(c);
}

#[cfg(feature = "storage-benches")]
fn maybe_print_singleton_write_accounting(runtime: &tokio::runtime::Runtime) {
    if std::env::var_os("LIX_SLATEDB_SINGLETON_ACCOUNTING").is_none() {
        return;
    }
    let samples = std::env::var("LIX_SLATEDB_SINGLETON_SAMPLES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(SINGLETON_ACCOUNTING_SAMPLES)
        .max(1);

    let mut fixture = runtime.block_on(SingletonWriteFixture::new());
    fixture.object_store.reset_write_stats();
    let bulk_started = Instant::now();
    let bulk_accounting = runtime.block_on(fixture.transaction.insert_all_accounting());
    let bulk_elapsed = bulk_started.elapsed();
    let bulk_flush_started = Instant::now();
    runtime
        .block_on(fixture.storage.flush())
        .expect("flush singleton bulk insert");
    let bulk_flush_elapsed = bulk_flush_started.elapsed();
    print_singleton_sample(
        "insert_1k",
        Duration::ZERO,
        bulk_elapsed,
        bulk_flush_elapsed,
        bulk_accounting,
        &fixture.object_store.write_stats(),
    );

    for delay in [Duration::ZERO, Duration::from_millis(25)] {
        fixture.object_store.set_delay(delay);
        let mut elapsed = Vec::with_capacity(samples);
        let mut logical_value_bytes = 0u64;
        let mut staged_puts = 0u64;
        fixture.object_store.reset_write_stats();
        for _ in 0..samples {
            let started = Instant::now();
            let accounting = runtime.block_on(fixture.transaction.update_one_by_pk_accounting());
            elapsed.push(started.elapsed());
            logical_value_bytes += accounting.written_bytes;
            staged_puts += accounting.staged_puts;
        }
        let flush_started = Instant::now();
        runtime
            .block_on(fixture.storage.flush())
            .expect("flush singleton update burst");
        let flush_elapsed = flush_started.elapsed();
        let stats = fixture.object_store.write_stats();
        let wal = stats.wal_totals();
        elapsed.sort_unstable();
        let p50 = percentile(&elapsed, 50);
        let p95 = percentile(&elapsed, 95);
        let sample_count = u64::try_from(samples).expect("sample count fits u64");
        println!(
            "slatedb-singleton-update delay_ms={} samples={samples} accepted_p50_ns={} accepted_p95_ns={} flush_ns={} staged_puts_avg={} logical_value_bytes_avg={} object_put_calls_total={} object_put_bytes_total={} wal_put_calls_total={} wal_put_bytes_total={}",
            delay.as_millis(),
            p50.as_nanos(),
            p95.as_nanos(),
            flush_elapsed.as_nanos(),
            staged_puts / sample_count,
            logical_value_bytes / sample_count,
            stats.put_calls,
            stats.put_bytes,
            wal.put_calls,
            wal.put_bytes,
        );
    }

    let mut delete_fixture = runtime.block_on(SingletonWriteFixture::new());
    runtime.block_on(delete_fixture.transaction.seed());
    runtime
        .block_on(delete_fixture.storage.flush())
        .expect("flush singleton delete seed");
    delete_fixture.object_store.reset_write_stats();
    let delete_started = Instant::now();
    let delete_accounting =
        runtime.block_on(delete_fixture.transaction.delete_one_by_pk_accounting());
    let delete_elapsed = delete_started.elapsed();
    let delete_flush_started = Instant::now();
    runtime
        .block_on(delete_fixture.storage.flush())
        .expect("flush singleton delete");
    let delete_flush_elapsed = delete_flush_started.elapsed();
    print_singleton_sample(
        "delete_one",
        Duration::ZERO,
        delete_elapsed,
        delete_flush_elapsed,
        delete_accounting,
        &delete_fixture.object_store.write_stats(),
    );
}

#[cfg(feature = "storage-benches")]
fn print_singleton_sample(
    operation: &str,
    delay: Duration,
    accepted: Duration,
    flush: Duration,
    accounting: BenchWriteAccounting,
    stats: &ObjectStoreWriteStats,
) {
    let wal = stats.wal_totals();
    println!(
        "slatedb-singleton-sample operation={operation} delay_ms={} accepted_ns={} flush_ns={} logical_rows={} staged_puts={} staged_deletes={} logical_value_bytes={} object_put_calls={} object_put_bytes={} wal_put_calls={} wal_put_bytes={}",
        delay.as_millis(),
        accepted.as_nanos(),
        flush.as_nanos(),
        accounting.logical_rows,
        accounting.staged_puts,
        accounting.staged_deletes,
        accounting.written_bytes,
        stats.put_calls,
        stats.put_bytes,
        wal.put_calls,
        wal.put_bytes,
    );
}

#[cfg(feature = "storage-benches")]
fn percentile(samples: &[Duration], percentile: usize) -> Duration {
    let index = (samples.len() - 1) * percentile / 100;
    samples[index]
}

#[cfg(feature = "storage-benches")]
struct SingletonWriteFixture {
    transaction: BenchTransactionFixture<SlateDB>,
    storage: SlateDB,
    object_store: Arc<DelayedObjectStore>,
}

#[cfg(feature = "storage-benches")]
impl SingletonWriteFixture {
    async fn new() -> Self {
        let db_id = NEXT_DB_ID.fetch_add(1, Ordering::Relaxed);
        let db_path = format!("slatedb-singleton-bench-{}-{db_id}", std::process::id());
        let object_store = Arc::new(DelayedObjectStore::new(
            Arc::new(InMemory::new()),
            Duration::ZERO,
        ));
        let storage = SlateDB::open_object_store(db_path, object_store_handle(&object_store))
            .expect("open SlateDB singleton accounting storage");
        let rows = (0..SINGLETON_ACCOUNTING_ROWS)
            .map(|index| {
                let path = format!("/singleton/{index:04}");
                BenchTransactionRow {
                    schema_key: "json_pointer".to_string(),
                    file_id: None,
                    entity_pk: path.clone(),
                    value: json!({"path": path, "value": format!("seed-{index:04}")}),
                    updated_value: json!({
                        "path": format!("/singleton/{index:04}"),
                        "value": format!("updated-{index:04}"),
                    }),
                }
            })
            .collect();
        let transaction =
            BenchTransactionFixture::new_deterministic(StorageAdapter::new(storage.clone()), rows)
                .await;
        storage
            .flush()
            .await
            .expect("flush singleton accounting setup");
        object_store.reset_write_stats();
        Self {
            transaction,
            storage,
            object_store,
        }
    }
}

fn maybe_print_write_accounting(runtime: &tokio::runtime::Runtime) {
    if std::env::var_os("LIX_SLATEDB_WRITE_ACCOUNTING").is_none() {
        return;
    }
    let fixture = runtime.block_on(UploadBenchFixture::seeded(Duration::ZERO));
    black_box(runtime.block_on(fixture.upload_overwrite_file()));
    runtime
        .block_on(fixture.storage.flush())
        .expect("flush upload accounting warmup");
    fixture.object_store.reset_write_stats();
    let started = Instant::now();
    let rows_affected = runtime.block_on(fixture.upload_overwrite_file());
    let accepted = started.elapsed();
    let flush_started = Instant::now();
    runtime
        .block_on(fixture.storage.flush())
        .expect("flush upload accounting write");
    let flush = flush_started.elapsed();
    let stats = fixture.object_store.write_stats();
    println!(
        "slatedb-write-accounting rows_affected={rows_affected} accepted_ns={} flush_ns={} put_calls={} put_bytes={}",
        accepted.as_nanos(),
        flush.as_nanos(),
        stats.put_calls,
        stats.put_bytes,
    );
    for (path, path_stats) in stats.by_path {
        println!(
            "slatedb-object-write path={path} put_calls={} put_bytes={}",
            path_stats.put_calls, path_stats.put_bytes,
        );
    }
}

fn cached_cold_lifecycle_benches(c: &mut Criterion, runtime: &tokio::runtime::Runtime) {
    let mut group = c.benchmark_group("slatedb_file_api_cached_cold_lifecycle");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));

    for &delay_ms in FRESH_ENGINE_DELAYS_MS {
        let delay = Duration::from_millis(delay_ms);
        for stage in CachedLifecycleStage::ALL {
            group.bench_with_input(
                BenchmarkId::new(stage.label(), format!("{delay_ms}ms")),
                &(delay, stage),
                |b, &(delay, stage)| {
                    b.iter_custom(|iterations| {
                        let fixture = runtime.block_on(CachedLifecycleBenchFixture::seeded(delay));
                        measure_prepared_iterations(
                            iterations,
                            // Keep cache-directory creation and teardown outside the
                            // cumulative startup interval.
                            || tempfile::tempdir().expect("create lifecycle cache directory"),
                            |cache_dir| runtime.block_on(fixture.run(stage, cache_dir)),
                        )
                    });
                },
            );
        }
    }

    group.finish();
}

fn cached_preloaded_request_benches(c: &mut Criterion, runtime: &tokio::runtime::Runtime) {
    let mut group = c.benchmark_group("slatedb_file_api_cached_preloaded_request");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));

    for &delay_ms in FRESH_ENGINE_DELAYS_MS {
        let delay = Duration::from_millis(delay_ms);
        for stage in CachedRequestStage::ALL {
            group.bench_with_input(
                BenchmarkId::new(stage.label(), format!("{delay_ms}ms")),
                &(delay, stage),
                |b, &(delay, stage)| {
                    b.iter_custom(|iterations| {
                        let fixture = runtime.block_on(CachedRequestBenchFixture::seeded(delay));
                        // Reuse only the populated disk cache. Each iteration gets a
                        // fresh storage so its in-memory caches start empty.
                        measure_prepared_iterations(
                            iterations,
                            || runtime.block_on(fixture.prepare_request(stage)),
                            |engine| {
                                runtime.block_on(lixray_download_file(
                                    engine,
                                    &fixture.main_branch_id,
                                    &fixture.file_id,
                                ))
                            },
                        )
                    });
                },
            );
        }
    }

    group.finish();
}

fn fresh_engine_select_benches(c: &mut Criterion, runtime: &tokio::runtime::Runtime) {
    let mut group = c.benchmark_group("slatedb_file_api_fresh_engine_select");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));

    for &delay_ms in FRESH_ENGINE_DELAYS_MS {
        let delay = Duration::from_millis(delay_ms);

        group.bench_with_input(
            BenchmarkId::new("download_file", format!("{delay_ms}ms")),
            &delay,
            |b, &delay| {
                b.iter_custom(|iterations| {
                    let fixture = runtime.block_on(FreshEngineSelectBenchFixture::seeded(delay));
                    // Constructing the Engine and session can itself fetch
                    // repository state. Prepare each fresh session outside the
                    // manual timer so this measures only its first SELECT.
                    measure_prepared_iterations(
                        iterations,
                        || runtime.block_on(fixture.open_session()),
                        |session| runtime.block_on(download_file(session, &fixture.file_id)),
                    )
                });
            },
        );
    }

    group.finish();
}

fn storage_concurrency_benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("slatedb_concurrency");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));

    group.bench_function("one_point_read", |b| {
        b.iter_custom(|iterations| {
            let fixture =
                StorageConcurrencyFixture::seeded(Duration::from_millis(CONCURRENCY_DELAY_MS));
            measure_iterations(iterations, || fixture.read_sequential(1))
        });
    });

    group.bench_function("four_sequential_point_reads", |b| {
        b.iter_custom(|iterations| {
            let fixture =
                StorageConcurrencyFixture::seeded(Duration::from_millis(CONCURRENCY_DELAY_MS));
            measure_iterations(iterations, || fixture.read_sequential(CONCURRENCY_REQUESTS))
        });
    });

    group.bench_function("four_parallel_point_reads", |b| {
        b.iter_custom(|iterations| {
            let fixture =
                StorageConcurrencyFixture::seeded(Duration::from_millis(CONCURRENCY_DELAY_MS));
            measure_iterations(iterations, || fixture.read_parallel(CONCURRENCY_REQUESTS))
        });
    });

    group.finish();
}

fn measure_iterations<T>(iterations: u64, mut operation: impl FnMut() -> T) -> Duration {
    let mut elapsed = Duration::ZERO;
    for _ in 0..iterations {
        let started = Instant::now();
        let result = operation();
        elapsed += started.elapsed();
        black_box(result);
    }
    elapsed
}

fn measure_prepared_iterations<P, T>(
    iterations: u64,
    mut prepare: impl FnMut() -> P,
    mut operation: impl FnMut(&P) -> T,
) -> Duration {
    let mut elapsed = Duration::ZERO;
    for _ in 0..iterations {
        let prepared = prepare();
        let started = Instant::now();
        let result = operation(&prepared);
        elapsed += started.elapsed();
        black_box(result);
    }
    elapsed
}

struct SeededStore {
    object_store: Arc<DelayedObjectStore>,
    db_path: String,
    main_branch_id: String,
    file_id: String,
    upload_path: String,
}

impl SeededStore {
    async fn create() -> Self {
        let db_id = NEXT_DB_ID.fetch_add(1, Ordering::Relaxed);
        let db_path = format!("slatedb-file-api-bench-{}-{db_id}", std::process::id());
        let seed_object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let storage = SlateDB::open_object_store(db_path.clone(), Arc::clone(&seed_object_store))
            .expect("open SlateDB seed storage");
        let init_receipt = Engine::initialize(storage.clone())
            .await
            .expect("initialize SlateDB file benchmark engine");
        let engine = Engine::new(storage.clone())
            .await
            .expect("open SlateDB file benchmark engine");
        let session = engine
            .open_session(init_receipt.main_branch_id.clone())
            .await
            .expect("open SlateDB file benchmark session");

        seed_files(&session).await;
        let upload_path = file_path(0);
        let file_id = lookup_file_id(&session, &upload_path).await;
        storage
            .flush()
            .await
            .expect("flush seeded file benchmark store");
        let object_store = Arc::new(DelayedObjectStore::new(seed_object_store, Duration::ZERO));

        Self {
            object_store,
            db_path,
            main_branch_id: init_receipt.main_branch_id,
            file_id,
            upload_path,
        }
    }

    async fn open_session(&self) -> (SessionContext<SlateDB>, TempDir) {
        let cache_dir = tempfile::tempdir().expect("create SlateDB benchmark cache directory");
        let storage = SlateDB::open_object_store_with_options(
            self.db_path.clone(),
            object_store_handle(&self.object_store),
            cached_object_store_options(&cache_dir),
        )
        .expect("reopen delayed SlateDB storage");
        let engine = Engine::new(storage)
            .await
            .expect("reopen SlateDB file benchmark engine");
        let session = engine
            .open_session(self.main_branch_id.clone())
            .await
            .expect("reopen SlateDB file benchmark session");
        (session, cache_dir)
    }
}

struct UploadBenchFixture {
    session: SessionContext<SlateDB>,
    storage: SlateDB,
    _cache_dir: TempDir,
    object_store: Arc<DelayedObjectStore>,
    next_upload_version: AtomicU64,
    upload_path: String,
    upload_batch_paths: Vec<String>,
    upload_batch_sql: String,
}

impl UploadBenchFixture {
    async fn seeded(delay: Duration) -> Self {
        let seeded = SeededStore::create().await;
        let cache_dir = tempfile::tempdir().expect("create SlateDB upload cache directory");
        let storage = SlateDB::open_object_store_with_options(
            seeded.db_path.clone(),
            object_store_handle(&seeded.object_store),
            cached_object_store_options(&cache_dir),
        )
        .expect("reopen delayed SlateDB storage for upload benchmark");
        let engine = Engine::new(storage.clone())
            .await
            .expect("open SlateDB upload benchmark engine");
        let session = engine
            .open_session(seeded.main_branch_id)
            .await
            .expect("open SlateDB upload benchmark session");
        seeded.object_store.set_delay(delay);

        Self {
            session,
            storage,
            _cache_dir: cache_dir,
            object_store: seeded.object_store,
            next_upload_version: AtomicU64::new(0),
            upload_path: seeded.upload_path,
            upload_batch_paths: (0..LIXRAY_UPLOAD_BATCH_FILES).map(file_path).collect(),
            upload_batch_sql: upload_batch_sql(LIXRAY_UPLOAD_BATCH_FILES),
        }
    }

    async fn upload_overwrite_file(&self) -> u64 {
        let version = self.next_upload_version.fetch_add(1, Ordering::Relaxed);
        let result = self
            .session
            .execute(
                "INSERT INTO lix_file (path, data, lixcol_metadata) VALUES ($1, $2, $3) \
                 ON CONFLICT (path) DO UPDATE SET data = excluded.data, \
                 lixcol_metadata = excluded.lixcol_metadata",
                &[
                    Value::Text(self.upload_path.clone()),
                    Value::Blob(upload_file_bytes(version)),
                    upload_file_metadata(version),
                ],
            )
            .await
            .expect("overwrite benchmark file");
        result.rows_affected()
    }

    async fn upload_overwrite_file_batch(&self) -> u64 {
        let version = self.next_upload_version.fetch_add(1, Ordering::Relaxed);
        let mut params = Vec::with_capacity(self.upload_batch_paths.len() * 3);
        for (file_index, path) in self.upload_batch_paths.iter().enumerate() {
            let file_version = version
                .wrapping_mul(self.upload_batch_paths.len() as u64)
                .wrapping_add(file_index as u64);
            params.push(Value::Text(path.clone()));
            params.push(Value::Blob(upload_file_bytes(file_version)));
            params.push(upload_file_metadata(file_version));
        }
        let result = self
            .session
            .execute(&self.upload_batch_sql, &params)
            .await
            .expect("overwrite benchmark file batch");
        result.rows_affected()
    }
}

fn upload_batch_sql(row_count: usize) -> String {
    let placeholders = (0..row_count)
        .map(|row_index| {
            format!(
                "(${}, ${}, ${})",
                row_index * 3 + 1,
                row_index * 3 + 2,
                row_index * 3 + 3
            )
        })
        .collect::<Vec<_>>();
    format!(
        "INSERT INTO lix_file (path, data, lixcol_metadata) VALUES {} \
         ON CONFLICT (path) DO UPDATE SET data = excluded.data, \
         lixcol_metadata = excluded.lixcol_metadata",
        placeholders.join(", ")
    )
}

struct ReadBenchFixture {
    session: SessionContext<SlateDB>,
    _cache_dir: TempDir,
    file_id: String,
}

impl ReadBenchFixture {
    async fn seeded(delay: Duration) -> Self {
        let seeded = SeededStore::create().await;
        let (session, cache_dir) = seeded.open_session().await;
        seeded.object_store.set_delay(delay);

        Self {
            session,
            _cache_dir: cache_dir,
            file_id: seeded.file_id,
        }
    }

    async fn list_root_directory(&self) -> usize {
        let dirs = self
            .session
            .execute(
                "SELECT id, path, name, lixcol_updated_at \
                 FROM lix_directory WHERE parent_id IS NULL ORDER BY name",
                &[],
            )
            .await
            .expect("list root directories");
        let files = self
            .session
            .execute(
                "SELECT id, path, name, lixcol_metadata, lixcol_updated_at \
                 FROM lix_file WHERE directory_id IS NULL ORDER BY name",
                &[],
            )
            .await
            .expect("list root files");
        dirs.len() + files.len()
    }

    async fn download_file(&self) -> usize {
        download_file(&self.session, &self.file_id).await
    }
}

struct FreshEngineSelectBenchFixture {
    storage: SlateDB,
    main_branch_id: String,
    file_id: String,
}

#[derive(Clone, Copy)]
enum CachedLifecycleStage {
    StorageOpen,
    EngineOpen,
    FirstRequest,
    SecondRequest,
}

impl CachedLifecycleStage {
    const ALL: [Self; 4] = [
        Self::StorageOpen,
        Self::EngineOpen,
        Self::FirstRequest,
        Self::SecondRequest,
    ];

    const fn label(self) -> &'static str {
        match self {
            Self::StorageOpen => "startup_through_storage_open",
            Self::EngineOpen => "startup_through_engine_open",
            Self::FirstRequest => "startup_through_first_request",
            Self::SecondRequest => "startup_through_second_request",
        }
    }
}

struct CachedLifecycleBenchFixture {
    seeded: SeededStore,
}

#[derive(Clone, Copy)]
enum CachedRequestStage {
    First,
    Second,
}

impl CachedRequestStage {
    const ALL: [Self; 2] = [Self::First, Self::Second];

    const fn label(self) -> &'static str {
        match self {
            Self::First => "first_request",
            Self::Second => "second_request",
        }
    }
}

struct CachedRequestBenchFixture {
    object_store: Arc<DelayedObjectStore>,
    db_path: String,
    cache_dir: TempDir,
    main_branch_id: String,
    file_id: String,
}

impl CachedLifecycleBenchFixture {
    async fn seeded(delay: Duration) -> Self {
        let seeded = SeededStore::create().await;
        seeded.object_store.set_delay(delay);
        Self { seeded }
    }

    async fn run(
        &self,
        stage: CachedLifecycleStage,
        cache_dir: &TempDir,
    ) -> CachedLifecycleBenchResult {
        let storage = SlateDB::open_object_store_with_options(
            self.seeded.db_path.clone(),
            object_store_handle(&self.seeded.object_store),
            cached_object_store_options(cache_dir),
        )
        .expect("open cached lifecycle storage");
        if matches!(stage, CachedLifecycleStage::StorageOpen) {
            return CachedLifecycleBenchResult {
                _value: 1,
                _storage: storage,
                _engine: None,
            };
        }

        let engine = Engine::new(storage.clone())
            .await
            .expect("open cached lifecycle engine");
        if matches!(stage, CachedLifecycleStage::EngineOpen) {
            return CachedLifecycleBenchResult {
                _value: 1,
                _storage: storage,
                _engine: Some(engine),
            };
        }

        let first =
            lixray_download_file(&engine, &self.seeded.main_branch_id, &self.seeded.file_id).await;
        if matches!(stage, CachedLifecycleStage::FirstRequest) {
            return CachedLifecycleBenchResult {
                _value: first,
                _storage: storage,
                _engine: Some(engine),
            };
        }

        let second =
            lixray_download_file(&engine, &self.seeded.main_branch_id, &self.seeded.file_id).await;
        CachedLifecycleBenchResult {
            _value: first + second,
            _storage: storage,
            _engine: Some(engine),
        }
    }
}

struct CachedLifecycleBenchResult {
    _value: usize,
    _engine: Option<Engine<SlateDB>>,
    _storage: SlateDB,
}

impl CachedRequestBenchFixture {
    async fn seeded(delay: Duration) -> Self {
        let seeded = SeededStore::create().await;
        seeded.object_store.set_delay(delay);
        let cache_dir = tempfile::tempdir().expect("create preloaded request cache directory");
        let storage = SlateDB::open_object_store_with_options(
            seeded.db_path.clone(),
            object_store_handle(&seeded.object_store),
            cached_object_store_options(&cache_dir),
        )
        .expect("open cached preloaded request storage");
        drop(storage);

        Self {
            object_store: seeded.object_store,
            db_path: seeded.db_path,
            cache_dir,
            main_branch_id: seeded.main_branch_id,
            file_id: seeded.file_id,
        }
    }

    async fn prepare_request(&self, stage: CachedRequestStage) -> Engine<SlateDB> {
        let storage = SlateDB::open_object_store_with_options(
            self.db_path.clone(),
            object_store_handle(&self.object_store),
            cached_object_store_options(&self.cache_dir),
        )
        .expect("reopen cached preloaded request storage");
        let engine = Engine::new(storage)
            .await
            .expect("open cached preloaded request engine");
        if matches!(stage, CachedRequestStage::Second) {
            black_box(lixray_download_file(&engine, &self.main_branch_id, &self.file_id).await);
        }
        engine
    }
}

impl FreshEngineSelectBenchFixture {
    async fn seeded(delay: Duration) -> Self {
        let seeded = SeededStore::create().await;
        let storage = SlateDB::open_object_store(
            seeded.db_path.clone(),
            object_store_handle(&seeded.object_store),
        )
        .expect("reopen uncached SlateDB file benchmark storage");
        seeded.object_store.set_delay(delay);

        Self {
            storage,
            main_branch_id: seeded.main_branch_id,
            file_id: seeded.file_id,
        }
    }

    async fn open_session(&self) -> SessionContext<SlateDB> {
        // A fresh Engine owns a fresh immutable-node cache. The benchmark
        // creates this session before starting its manual request timer.
        let engine = Engine::new(self.storage.clone())
            .await
            .expect("open fresh-engine SlateDB benchmark engine");
        engine
            .open_session(self.main_branch_id.clone())
            .await
            .expect("open fresh-engine SlateDB benchmark session")
    }
}

async fn download_file(session: &SessionContext<SlateDB>, file_id: &str) -> usize {
    let result = session
        .execute(
            "SELECT data FROM lix_file WHERE id = $1",
            &[Value::Text(file_id.to_string())],
        )
        .await
        .expect("download benchmark file");
    let value = result
        .rows()
        .first()
        .and_then(|row| row.values().first())
        .expect("download query should return one data value");
    match value {
        Value::Blob(bytes) => bytes.len(),
        other => panic!("download query returned non-blob value: {other:?}"),
    }
}

async fn lixray_download_file(engine: &Engine<SlateDB>, branch_id: &str, file_id: &str) -> usize {
    assert!(
        engine
            .load_branch_head_commit_id(branch_id)
            .await
            .expect("validate lifecycle branch")
            .is_some(),
        "seeded lifecycle branch should exist"
    );
    let session = engine
        .open_session(branch_id.to_string())
        .await
        .expect("open lifecycle session");
    download_file(&session, file_id).await
}

struct StorageConcurrencyFixture {
    readers: StorageReadWorkers,
}

struct StorageReadRequest {
    barrier: Arc<Barrier>,
    result: std::sync::mpsc::Sender<usize>,
}

struct StorageReadWorkers {
    commands: Vec<std::sync::mpsc::Sender<StorageReadRequest>>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl StorageConcurrencyFixture {
    fn seeded(delay: Duration) -> Self {
        let db_id = NEXT_DB_ID.fetch_add(1, Ordering::Relaxed);
        let db_path = format!("slatedb-concurrency-bench-{}-{db_id}", std::process::id());
        let seed_object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let keys = (0..CONCURRENCY_REQUESTS)
            .map(|index| Key(Bytes::from(format!("concurrency-key-{index}"))))
            .collect::<Vec<_>>();

        {
            let storage =
                SlateDB::open_object_store(db_path.clone(), Arc::clone(&seed_object_store))
                    .expect("open SlateDB concurrency seed storage");
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("create SlateDB concurrency seed runtime");
            runtime.block_on(async {
                let mut write = storage
                    .begin_write(WriteOptions::default())
                    .await
                    .expect("begin SlateDB concurrency seed write");
                write
                    .put_many(
                        CONCURRENCY_SPACE,
                        PutBatch {
                            entries: keys
                                .iter()
                                .cloned()
                                .map(|key| PutEntry {
                                    key,
                                    value: StoredValue {
                                        bytes: Bytes::from(vec![0x5a; CONCURRENCY_VALUE_BYTES]),
                                    },
                                })
                                .collect(),
                        },
                    )
                    .await
                    .expect("stage SlateDB concurrency seed value");
                write
                    .commit()
                    .await
                    .expect("commit SlateDB concurrency seed value");
            });
        }

        let object_store = Arc::new(DelayedObjectStore::new(seed_object_store, Duration::ZERO));
        let storage = SlateDB::open_object_store(db_path, object_store_handle(&object_store))
            .expect("reopen SlateDB concurrency benchmark storage");
        let readers = StorageReadWorkers::new(&storage, &keys);
        object_store.set_delay(delay);

        Self { readers }
    }

    fn read_sequential(&self, request_count: usize) -> usize {
        self.readers.read_sequential(request_count)
    }

    fn read_parallel(&self, request_count: usize) -> usize {
        self.readers.read_parallel(request_count)
    }
}

impl StorageReadWorkers {
    fn new(storage: &SlateDB, keys: &[Key]) -> Self {
        let mut commands = Vec::with_capacity(keys.len());
        let mut threads = Vec::with_capacity(keys.len());
        for key in keys {
            let (command_tx, command_rx) = std::sync::mpsc::channel::<StorageReadRequest>();
            let storage = storage.clone();
            let key = key.clone();
            threads.push(std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("create persistent SlateDB reader runtime");
                while let Ok(request) = command_rx.recv() {
                    request.barrier.wait();
                    let result = runtime.block_on(read_storage_key(&storage, &key));
                    let _ = request.result.send(result);
                }
            }));
            commands.push(command_tx);
        }
        Self { commands, threads }
    }

    fn read_sequential(&self, request_count: usize) -> usize {
        assert!(
            request_count > 0 && request_count <= self.commands.len(),
            "benchmark request count must fit the persistent reader pool"
        );
        let barrier = Arc::new(Barrier::new(1));
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let mut result = 0;
        for command in self.commands.iter().take(request_count) {
            command
                .send(StorageReadRequest {
                    barrier: Arc::clone(&barrier),
                    result: result_tx.clone(),
                })
                .expect("dispatch sequential SlateDB point read");
            result += result_rx
                .recv()
                .expect("receive sequential SlateDB point read");
        }
        result
    }

    fn read_parallel(&self, request_count: usize) -> usize {
        assert!(
            request_count > 0 && request_count <= self.commands.len(),
            "benchmark request count must fit the persistent reader pool"
        );
        let barrier = Arc::new(Barrier::new(request_count));
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        for command in self.commands.iter().take(request_count) {
            command
                .send(StorageReadRequest {
                    barrier: Arc::clone(&barrier),
                    result: result_tx.clone(),
                })
                .expect("dispatch parallel SlateDB point read");
        }
        drop(result_tx);
        result_rx.into_iter().sum()
    }
}

impl Drop for StorageReadWorkers {
    fn drop(&mut self) {
        self.commands.clear();
        for thread in self.threads.drain(..) {
            thread.join().expect("join persistent SlateDB reader");
        }
    }
}

async fn read_storage_key(storage: &SlateDB, key: &Key) -> usize {
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("begin SlateDB concurrency read");
    let result = read
        .get_many(
            CONCURRENCY_SPACE,
            std::slice::from_ref(key),
            StorageGetOptions::default(),
        )
        .await
        .expect("read SlateDB concurrency key");
    match result.values.into_iter().next().flatten() {
        Some(ProjectedValue::FullValue(value)) => value.len(),
        Some(ProjectedValue::KeyOnly) => panic!("concurrency read returned key-only value"),
        None => panic!("concurrency read did not find seeded key"),
    }
}

async fn seed_files(session: &SessionContext<SlateDB>) {
    for chunk_start in (0..SEED_FILE_COUNT).step_by(UPLOAD_BATCH_SIZE) {
        let chunk_end = (chunk_start + UPLOAD_BATCH_SIZE).min(SEED_FILE_COUNT);
        let mut placeholders = Vec::with_capacity(chunk_end - chunk_start);
        let mut params = Vec::with_capacity((chunk_end - chunk_start) * 3);

        for (row_index, file_index) in (chunk_start..chunk_end).enumerate() {
            placeholders.push(format!(
                "(${}, ${}, ${})",
                row_index * 3 + 1,
                row_index * 3 + 2,
                row_index * 3 + 3
            ));
            params.push(Value::Text(file_path(file_index)));
            params.push(Value::Blob(file_bytes(file_index)));
            params.push(file_metadata());
        }

        let sql = format!(
            "INSERT INTO lix_file (path, data, lixcol_metadata) VALUES {} \
             ON CONFLICT (path) DO UPDATE SET data = excluded.data, \
             lixcol_metadata = excluded.lixcol_metadata",
            placeholders.join(", ")
        );
        session
            .execute(&sql, &params)
            .await
            .expect("seed file benchmark fixture");
    }
}

async fn lookup_file_id(session: &SessionContext<SlateDB>, path: &str) -> String {
    let result = session
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await
        .expect("look up benchmark file id");
    let value = result
        .rows()
        .first()
        .and_then(|row| row.values().first())
        .expect("seeded file should be queryable by path");
    match value {
        Value::Text(id) => id.clone(),
        other => panic!("file id query returned non-text value: {other:?}"),
    }
}

fn file_path(index: usize) -> String {
    format!("/bench-file-{index:04}.bin")
}

fn file_bytes(index: usize) -> Vec<u8> {
    vec![u8::try_from(index % 251).expect("byte pattern fits in u8"); FILE_SIZE_BYTES]
}

fn file_metadata() -> Value {
    Value::Json(json!({ "size": FILE_SIZE_BYTES }))
}

fn upload_file_bytes(version: u64) -> Vec<u8> {
    let seed_file_count = u64::try_from(SEED_FILE_COUNT).expect("seed file count fits in u64");
    let byte = u8::try_from((version % 251 + seed_file_count % 251) % 251)
        .expect("upload byte pattern fits in u8");
    vec![byte; FILE_SIZE_BYTES]
}

fn upload_file_metadata(version: u64) -> Value {
    Value::Json(json!({
        "bench_version": version,
        "size": FILE_SIZE_BYTES,
    }))
}

fn object_store_handle(object_store: &Arc<DelayedObjectStore>) -> Arc<dyn ObjectStore> {
    object_store.clone()
}

fn cached_object_store_options(cache_dir: &TempDir) -> SlateDBObjectStoreOptions {
    SlateDBObjectStoreOptions {
        cache: Some(SlateDBCacheOptions {
            root_folder: cache_dir.path().join("object-cache"),
            max_disk_cache_bytes: BENCH_DISK_CACHE_BYTES,
            block_cache_bytes: BENCH_BLOCK_CACHE_BYTES,
            metadata_cache_bytes: BENCH_METADATA_CACHE_BYTES,
        }),
    }
}

#[derive(Clone, Debug)]
struct DelayedObjectStore {
    inner: Arc<dyn ObjectStore>,
    delay_nanos: Arc<AtomicU64>,
    record_write_stats: Arc<AtomicBool>,
    write_stats: Arc<Mutex<ObjectStoreWriteStats>>,
}

#[derive(Clone, Debug, Default)]
struct ObjectStoreWriteStats {
    put_calls: u64,
    put_bytes: u64,
    by_path: BTreeMap<String, ObjectStorePathWriteStats>,
}

#[derive(Clone, Debug, Default)]
struct ObjectStorePathWriteStats {
    put_calls: u64,
    put_bytes: u64,
}

impl ObjectStoreWriteStats {
    #[cfg(feature = "storage-benches")]
    fn wal_totals(&self) -> ObjectStorePathWriteStats {
        self.by_path
            .iter()
            .filter(|(path, _)| path.split('/').any(|segment| segment == "wal"))
            .fold(
                ObjectStorePathWriteStats::default(),
                |mut total, (_, stats)| {
                    total.put_calls += stats.put_calls;
                    total.put_bytes += stats.put_bytes;
                    total
                },
            )
    }
}

impl DelayedObjectStore {
    fn new(inner: Arc<dyn ObjectStore>, delay: Duration) -> Self {
        Self {
            inner,
            delay_nanos: Arc::new(AtomicU64::new(duration_nanos(delay))),
            record_write_stats: Arc::new(AtomicBool::new(false)),
            write_stats: Arc::new(Mutex::new(ObjectStoreWriteStats::default())),
        }
    }

    fn set_delay(&self, delay: Duration) {
        self.delay_nanos
            .store(duration_nanos(delay), Ordering::Relaxed);
    }

    fn delay(&self) -> Duration {
        Duration::from_nanos(self.delay_nanos.load(Ordering::Relaxed))
    }

    fn reset_write_stats(&self) {
        *self.write_stats.lock().expect("object write stats mutex") =
            ObjectStoreWriteStats::default();
        self.record_write_stats.store(true, Ordering::Relaxed);
    }

    fn write_stats(&self) -> ObjectStoreWriteStats {
        self.write_stats
            .lock()
            .expect("object write stats mutex")
            .clone()
    }
}

impl Display for DelayedObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DelayedObjectStore(delay={:?}, inner={})",
            self.delay(),
            self.inner.as_ref()
        )
    }
}

#[async_trait]
impl ObjectStore for DelayedObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        if self.record_write_stats.load(Ordering::Relaxed) {
            let bytes = u64::try_from(payload.content_length())
                .expect("object-store benchmark payload length fits u64");
            let mut stats = self.write_stats.lock().expect("object write stats mutex");
            stats.put_calls += 1;
            stats.put_bytes += bytes;
            let path_stats = stats.by_path.entry(location.to_string()).or_default();
            path_stats.put_calls += 1;
            path_stats.put_bytes += bytes;
        }
        delay_once(self.delay()).await;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        delay_once(self.delay()).await;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        delay_once(self.delay()).await;
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> ObjectStoreResult<Vec<Bytes>> {
        delay_once(self.delay()).await;
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        let inner = Arc::clone(&self.inner);
        let delay_nanos = Arc::clone(&self.delay_nanos);
        stream::once(async move {
            delay_once(current_delay(&delay_nanos)).await;
            inner.delete_stream(locations)
        })
        .flatten()
        .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.cloned();
        let delay_nanos = Arc::clone(&self.delay_nanos);
        stream::once(async move {
            delay_once(current_delay(&delay_nanos)).await;
            inner.list(prefix.as_ref())
        })
        .flatten()
        .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.cloned();
        let offset = offset.clone();
        let delay_nanos = Arc::clone(&self.delay_nanos);
        stream::once(async move {
            delay_once(current_delay(&delay_nanos)).await;
            inner.list_with_offset(prefix.as_ref(), &offset)
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        delay_once(self.delay()).await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        delay_once(self.delay()).await;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> ObjectStoreResult<()> {
        delay_once(self.delay()).await;
        self.inner.rename_opts(from, to, options).await
    }
}

fn duration_nanos(delay: Duration) -> u64 {
    u64::try_from(delay.as_nanos()).expect("benchmark delay fits in u64 nanoseconds")
}

fn current_delay(delay_nanos: &AtomicU64) -> Duration {
    Duration::from_nanos(delay_nanos.load(Ordering::Relaxed))
}

async fn delay_once(delay: Duration) {
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
}

criterion_group!(benches, slatedb_file_api_benches);
criterion_main!(benches);
