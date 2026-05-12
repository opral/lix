use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use lix_engine::{
    storage_bench, Backend, BackendKvEntryPage, BackendKvGetRequest, BackendKvKeyPage,
    BackendKvScanRequest, BackendKvValueBatch, BackendKvValuePage, BackendKvWriteBatch,
    BackendKvWriteStats, BackendReadTransaction, BackendWriteTransaction, LixError,
};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value as JsonValue;
use tempfile::TempDir;
use tokio::runtime::Runtime;

#[path = "../storage/rocksdb_backend.rs"]
mod rocksdb_backend;
#[path = "../storage/sqlite_backend.rs"]
mod sqlite_backend;

use rocksdb_backend::RocksDbBenchBackend;
use sqlite_backend::SqliteBenchBackend;

const SMOKE_ROWS: usize = 1_000;
const REAL_WORKLOAD_ROWS: usize = 10_000;
const PNPM_LOCK_JSON: &str = include_str!("pnpm-lock.fixture.json");

#[derive(Clone)]
struct PointerRow {
    path: String,
    value_json: String,
    updated_value_json: String,
}

#[derive(Clone)]
struct RawUntrackedRow {
    version_id: String,
    schema_key: String,
    entity_id: String,
    file_id: String,
    snapshot_content: String,
    updated_snapshot_content: String,
    metadata: Option<String>,
    created_at: String,
    updated_at: String,
    global: bool,
}

#[derive(Clone, Copy)]
enum LixBackendProfile {
    Sqlite,
    RocksDb,
}

impl LixBackendProfile {
    fn name(self) -> &'static str {
        match self {
            Self::Sqlite => "lix_sqlite",
            Self::RocksDb => "lix_rocksdb",
        }
    }

    fn backend(self) -> Arc<dyn Backend + Send + Sync> {
        match self {
            Self::Sqlite => Arc::new(
                SqliteBenchBackend::tempfile().expect("create sqlite untracked-state backend"),
            ),
            Self::RocksDb => Arc::new(
                RocksDbBenchBackend::new().expect("create rocksdb untracked-state backend"),
            ),
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
    exists_calls: usize,
    exists_keys: usize,
    exists_key_bytes: usize,
    scan_key_calls: usize,
    scan_keys: usize,
    scan_key_bytes: usize,
    scan_value_calls: usize,
    scan_values: usize,
    scan_value_bytes: usize,
    scan_entry_calls: usize,
    scan_entries: usize,
    scan_entry_key_bytes: usize,
    scan_entry_value_bytes: usize,
    write_batches: usize,
    write_puts: usize,
    write_deletes: usize,
    write_bytes: usize,
}

impl IoStats {
    fn reset(&mut self) {
        *self = Self::default();
    }

    fn read_ops(&self) -> usize {
        self.get_calls
            + self.exists_calls
            + self.scan_key_calls
            + self.scan_value_calls
            + self.scan_entry_calls
    }

    fn scan_calls(&self) -> usize {
        self.scan_key_calls + self.scan_value_calls + self.scan_entry_calls
    }

    fn read_rows(&self) -> usize {
        self.get_values + self.scan_keys + self.scan_values + self.scan_entries + self.exists_keys
    }

    fn read_bytes(&self) -> usize {
        self.get_key_bytes
            + self.get_value_bytes
            + self.exists_key_bytes
            + self.scan_key_bytes
            + self.scan_value_bytes
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

struct CountingBackend {
    inner: Arc<dyn Backend + Send + Sync>,
    stats: Arc<Mutex<IoStats>>,
}

struct CountingReadTransaction {
    inner: Box<dyn BackendReadTransaction + Send + Sync + 'static>,
    stats: Arc<Mutex<IoStats>>,
}

struct CountingWriteTransaction {
    inner: Box<dyn BackendWriteTransaction + Send + Sync + 'static>,
    stats: Arc<Mutex<IoStats>>,
}

#[async_trait]
impl Backend for CountingBackend {
    async fn begin_read_transaction(
        &self,
    ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError> {
        Ok(Box::new(CountingReadTransaction {
            inner: self.inner.begin_read_transaction().await?,
            stats: Arc::clone(&self.stats),
        }))
    }

    async fn begin_write_transaction(
        &self,
    ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError> {
        Ok(Box::new(CountingWriteTransaction {
            inner: self.inner.begin_write_transaction().await?,
            stats: Arc::clone(&self.stats),
        }))
    }
}

#[async_trait]
impl BackendReadTransaction for CountingReadTransaction {
    async fn get_values(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvValueBatch, LixError> {
        record_get_request(&self.stats, &request, false);
        let batch = self.inner.get_values(request).await?;
        record_value_batch(&self.stats, &batch);
        Ok(batch)
    }

    async fn exists_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<lix_engine::BackendKvExistsBatch, LixError> {
        record_get_request(&self.stats, &request, true);
        self.inner.exists_many(request).await
    }

    async fn scan_keys(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvKeyPage, LixError> {
        let page = self.inner.scan_keys(request).await?;
        record_scan_keys(&self.stats, &page);
        Ok(page)
    }

    async fn scan_values(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvValuePage, LixError> {
        let page = self.inner.scan_values(request).await?;
        record_scan_values(&self.stats, &page);
        Ok(page)
    }

    async fn scan_entries(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvEntryPage, LixError> {
        let page = self.inner.scan_entries(request).await?;
        record_scan_entries(&self.stats, &page);
        Ok(page)
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        self.inner.rollback().await
    }
}

#[async_trait]
impl BackendReadTransaction for CountingWriteTransaction {
    async fn get_values(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvValueBatch, LixError> {
        record_get_request(&self.stats, &request, false);
        let batch = self.inner.get_values(request).await?;
        record_value_batch(&self.stats, &batch);
        Ok(batch)
    }

    async fn exists_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<lix_engine::BackendKvExistsBatch, LixError> {
        record_get_request(&self.stats, &request, true);
        self.inner.exists_many(request).await
    }

    async fn scan_keys(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvKeyPage, LixError> {
        let page = self.inner.scan_keys(request).await?;
        record_scan_keys(&self.stats, &page);
        Ok(page)
    }

    async fn scan_values(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvValuePage, LixError> {
        let page = self.inner.scan_values(request).await?;
        record_scan_values(&self.stats, &page);
        Ok(page)
    }

    async fn scan_entries(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvEntryPage, LixError> {
        let page = self.inner.scan_entries(request).await?;
        record_scan_entries(&self.stats, &page);
        Ok(page)
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        self.inner.rollback().await
    }
}

#[async_trait]
impl BackendWriteTransaction for CountingWriteTransaction {
    async fn write_kv_batch(
        &mut self,
        batch: BackendKvWriteBatch,
    ) -> Result<BackendKvWriteStats, LixError> {
        let write_stats = self.inner.write_kv_batch(batch).await?;
        let mut stats = self.stats.lock().expect("io stats mutex should lock");
        stats.write_batches += 1;
        stats.write_puts += write_stats.puts;
        stats.write_deletes += write_stats.deletes;
        stats.write_bytes += write_stats.bytes_written;
        Ok(write_stats)
    }

    async fn commit(self: Box<Self>) -> Result<(), LixError> {
        self.inner.commit().await
    }
}

fn record_get_request(
    stats: &Arc<Mutex<IoStats>>,
    request: &BackendKvGetRequest,
    exists_only: bool,
) {
    let mut stats = stats.lock().expect("io stats mutex should lock");
    let keys: usize = request.groups.iter().map(|group| group.keys.len()).sum();
    let key_bytes: usize = request
        .groups
        .iter()
        .flat_map(|group| group.keys.iter())
        .map(Vec::len)
        .sum();
    if exists_only {
        stats.exists_calls += 1;
        stats.exists_keys += keys;
        stats.exists_key_bytes += key_bytes;
    } else {
        stats.get_calls += 1;
        stats.get_keys += keys;
        stats.get_key_bytes += key_bytes;
    }
}

fn record_value_batch(stats: &Arc<Mutex<IoStats>>, batch: &BackendKvValueBatch) {
    let mut stats = stats.lock().expect("io stats mutex should lock");
    for group in &batch.groups {
        for value in group.values_iter().flatten() {
            stats.get_values += 1;
            stats.get_value_bytes += value.len();
        }
    }
}

fn record_scan_keys(stats: &Arc<Mutex<IoStats>>, page: &BackendKvKeyPage) {
    let mut stats = stats.lock().expect("io stats mutex should lock");
    stats.scan_key_calls += 1;
    stats.scan_keys += page.keys.len();
    stats.scan_key_bytes += page.keys.iter().map(|bytes| bytes.len()).sum::<usize>();
}

fn record_scan_values(stats: &Arc<Mutex<IoStats>>, page: &BackendKvValuePage) {
    let mut stats = stats.lock().expect("io stats mutex should lock");
    stats.scan_value_calls += 1;
    stats.scan_values += page.values.len();
    stats.scan_value_bytes += page.values.iter().map(|bytes| bytes.len()).sum::<usize>();
}

fn record_scan_entries(stats: &Arc<Mutex<IoStats>>, page: &BackendKvEntryPage) {
    let mut stats = stats.lock().expect("io stats mutex should lock");
    stats.scan_entry_calls += 1;
    stats.scan_entries += page.len();
    stats.scan_entry_key_bytes += page.keys.iter().map(|bytes| bytes.len()).sum::<usize>();
    stats.scan_entry_value_bytes += page.values.iter().map(|bytes| bytes.len()).sum::<usize>();
}

fn counting_backend(
    profile: LixBackendProfile,
) -> (Arc<dyn Backend + Send + Sync>, Arc<Mutex<IoStats>>) {
    let stats = Arc::new(Mutex::new(IoStats::default()));
    let backend = Arc::new(CountingBackend {
        inner: profile.backend(),
        stats: Arc::clone(&stats),
    });
    (backend, stats)
}

fn reset_io_stats(stats: &Arc<Mutex<IoStats>>) {
    stats.lock().expect("io stats mutex should lock").reset();
}

fn snapshot_io_stats(stats: &Arc<Mutex<IoStats>>) -> IoStats {
    stats.lock().expect("io stats mutex should lock").clone()
}

fn untracked_state_crud_benches(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create tokio runtime for untracked-state CRUD benchmarks");
    let rows = fixture_rows();
    maybe_print_io_report(&runtime, &rows);

    bench_raw_sqlite(c, &rows, SMOKE_ROWS, "smoke");
    bench_lix(c, &runtime, &rows, SMOKE_ROWS, "smoke");
    bench_raw_sqlite(c, &rows, REAL_WORKLOAD_ROWS, "real_workload");
    bench_lix(c, &runtime, &rows, REAL_WORKLOAD_ROWS, "real_workload");
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
        "| workload | backend | operation | io ops | io bytes | read calls | get calls | get keys | scan calls | read rows | read bytes | write batches | puts | deletes | write bytes |"
    );
    println!(
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    );

    for (label, row_count) in workloads {
        let rows = storage_rows(&all_rows[..row_count]);
        for profile in [LixBackendProfile::Sqlite, LixBackendProfile::RocksDb] {
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
                let stats = measure_lix_io(runtime, profile, operation, &rows);
                println!(
                    "| {label}/{rows_label} | {} | `{operation}` | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                    profile.name(),
                    stats.io_ops(),
                    stats.io_bytes(),
                    stats.read_ops(),
                    stats.get_calls,
                    stats.get_keys,
                    stats.scan_calls(),
                    stats.read_rows(),
                    stats.read_bytes(),
                    stats.write_batches,
                    stats.write_puts,
                    stats.write_deletes,
                    stats.write_bytes,
                    rows_label = row_label(row_count),
                );
            }
        }
    }
    println!();
}

fn measure_lix_io(
    runtime: &Runtime,
    profile: LixBackendProfile,
    operation: &str,
    rows: &[storage_bench::JsonPointerStorageRow],
) -> IoStats {
    let (backend, stats) = counting_backend(profile);
    match operation {
        "insert_all_rows" => {
            let fixture = runtime
                .block_on(storage_bench::prepare_json_pointer_untracked_state_write_rows(rows))
                .expect("prepare untracked_state insert io");
            reset_io_stats(&stats);
            runtime
                .block_on(storage_bench::untracked_state_write_rows_only_prepared(
                    &backend, &fixture,
                ))
                .expect("measure untracked_state insert io");
        }
        "select_all_rows" => {
            let fixture = runtime
                .block_on(storage_bench::prepare_json_pointer_untracked_state_read(
                    &backend, rows,
                ))
                .expect("prepare untracked_state select all io");
            reset_io_stats(&stats);
            runtime
                .block_on(
                    storage_bench::json_pointer_untracked_state_scan_full_rows_prepared(
                        &backend, &fixture,
                    ),
                )
                .expect("measure untracked_state select all io");
        }
        "select_keys_only" => {
            let fixture = runtime
                .block_on(storage_bench::prepare_json_pointer_untracked_state_read(
                    &backend, rows,
                ))
                .expect("prepare untracked_state select keys io");
            reset_io_stats(&stats);
            runtime
                .block_on(
                    storage_bench::json_pointer_untracked_state_scan_keys_only_prepared(
                        &backend, &fixture,
                    ),
                )
                .expect("measure untracked_state select keys io");
        }
        "select_one_by_pk" => {
            let fixture = runtime
                .block_on(storage_bench::prepare_json_pointer_untracked_state_read(
                    &backend, rows,
                ))
                .expect("prepare untracked_state select one io");
            reset_io_stats(&stats);
            runtime
                .block_on(
                    storage_bench::json_pointer_untracked_state_read_point_hit_constant_prepared(
                        &backend, &fixture, 1,
                    ),
                )
                .expect("measure untracked_state select one io");
        }
        "select_all_by_pk" => {
            let fixture = runtime
                .block_on(storage_bench::prepare_json_pointer_untracked_state_read(
                    &backend, rows,
                ))
                .expect("prepare untracked_state select all by pk io");
            reset_io_stats(&stats);
            runtime
                .block_on(
                    storage_bench::json_pointer_untracked_state_read_point_hit_prepared(
                        &backend, &fixture,
                    ),
                )
                .expect("measure untracked_state select all by pk io");
        }
        "update_all_rows" => {
            let fixture = runtime
                .block_on(
                    storage_bench::prepare_json_pointer_untracked_state_overwrite_rows(
                        &backend,
                        rows,
                        rows.len(),
                    ),
                )
                .expect("prepare untracked_state update all io");
            reset_io_stats(&stats);
            runtime
                .block_on(storage_bench::untracked_state_write_rows_only_prepared(
                    &backend, &fixture,
                ))
                .expect("measure untracked_state update all io");
        }
        "update_one_by_pk" => {
            let fixture = runtime
                .block_on(
                    storage_bench::prepare_json_pointer_untracked_state_overwrite_rows(
                        &backend, rows, 1,
                    ),
                )
                .expect("prepare untracked_state update one io");
            reset_io_stats(&stats);
            runtime
                .block_on(storage_bench::untracked_state_write_rows_only_prepared(
                    &backend, &fixture,
                ))
                .expect("measure untracked_state update one io");
        }
        "delete_all_rows" => {
            let fixture = runtime
                .block_on(
                    storage_bench::prepare_json_pointer_untracked_state_delete_rows(
                        &backend,
                        rows,
                        rows.len(),
                    ),
                )
                .expect("prepare untracked_state delete all io");
            reset_io_stats(&stats);
            runtime
                .block_on(
                    storage_bench::untracked_state_delete_existing_only_prepared(
                        &backend, &fixture,
                    ),
                )
                .expect("measure untracked_state delete all io");
        }
        "delete_one_by_pk" => {
            let fixture = runtime
                .block_on(
                    storage_bench::prepare_json_pointer_untracked_state_delete_rows(
                        &backend, rows, 1,
                    ),
                )
                .expect("prepare untracked_state delete one io");
            reset_io_stats(&stats);
            runtime
                .block_on(
                    storage_bench::untracked_state_delete_existing_only_prepared(
                        &backend, &fixture,
                    ),
                )
                .expect("measure untracked_state delete one io");
        }
        _ => unreachable!("unknown untracked_state io operation"),
    }
    snapshot_io_stats(&stats)
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

fn bench_lix(
    c: &mut Criterion,
    runtime: &Runtime,
    all_rows: &[PointerRow],
    row_count: usize,
    label: &str,
) {
    let rows = storage_rows(&all_rows[..row_count]);
    for profile in [LixBackendProfile::Sqlite, LixBackendProfile::RocksDb] {
        let mut group =
            c.benchmark_group(format!("untracked_state_crud/{}/{label}", profile.name()));
        configure_group(&mut group, row_count);

        group.bench_function(format!("insert_all_rows/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || {
                    let backend = profile.backend();
                    let fixture = runtime
                        .block_on(
                            storage_bench::prepare_json_pointer_untracked_state_write_rows(&rows),
                        )
                        .expect("prepare untracked_state insert");
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::untracked_state_write_rows_only_prepared(
                                &backend, &fixture,
                            ))
                            .expect("untracked_state insert"),
                    )
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function(format!("select_all_rows/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || prepare_lix_read(runtime, profile, &rows),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(
                                storage_bench::json_pointer_untracked_state_scan_full_rows_prepared(
                                    &backend, &fixture,
                                ),
                            )
                            .expect("untracked_state scan full rows"),
                    )
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function(format!("select_keys_only/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || prepare_lix_read(runtime, profile, &rows),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(
                                storage_bench::json_pointer_untracked_state_scan_keys_only_prepared(
                                    &backend, &fixture,
                                ),
                            )
                            .expect("untracked_state scan keys"),
                    )
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function(format!("select_one_by_pk/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || prepare_lix_read(runtime, profile, &rows),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(
                                storage_bench::json_pointer_untracked_state_read_point_hit_constant_prepared(
                                    &backend, &fixture, 1,
                                ),
                            )
                            .expect("untracked_state point hit"),
                    )
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function(format!("select_all_by_pk/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || prepare_lix_read(runtime, profile, &rows),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(
                                storage_bench::json_pointer_untracked_state_read_point_hit_prepared(
                                    &backend, &fixture,
                                ),
                            )
                            .expect("untracked_state point hits"),
                    )
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function(format!("update_all_rows/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || {
                    let backend = profile.backend();
                    let fixture = runtime
                        .block_on(
                            storage_bench::prepare_json_pointer_untracked_state_overwrite_rows(
                                &backend, &rows, row_count,
                            ),
                        )
                        .expect("prepare untracked_state update all");
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::untracked_state_write_rows_only_prepared(
                                &backend, &fixture,
                            ))
                            .expect("untracked_state update all"),
                    )
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function(format!("update_one_by_pk/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || {
                    let backend = profile.backend();
                    let fixture = runtime
                        .block_on(
                            storage_bench::prepare_json_pointer_untracked_state_overwrite_rows(
                                &backend, &rows, 1,
                            ),
                        )
                        .expect("prepare untracked_state update one");
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_bench::untracked_state_write_rows_only_prepared(
                                &backend, &fixture,
                            ))
                            .expect("untracked_state update one"),
                    )
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function(format!("delete_all_rows/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || {
                    let backend = profile.backend();
                    let fixture = runtime
                        .block_on(
                            storage_bench::prepare_json_pointer_untracked_state_delete_rows(
                                &backend, &rows, row_count,
                            ),
                        )
                        .expect("prepare untracked_state delete all");
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(
                                storage_bench::untracked_state_delete_existing_only_prepared(
                                    &backend, &fixture,
                                ),
                            )
                            .expect("untracked_state delete all"),
                    )
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function(format!("delete_one_by_pk/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || {
                    let backend = profile.backend();
                    let fixture = runtime
                        .block_on(
                            storage_bench::prepare_json_pointer_untracked_state_delete_rows(
                                &backend, &rows, 1,
                            ),
                        )
                        .expect("prepare untracked_state delete one");
                    (backend, fixture)
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(
                                storage_bench::untracked_state_delete_existing_only_prepared(
                                    &backend, &fixture,
                                ),
                            )
                            .expect("untracked_state delete one"),
                    )
                },
                BatchSize::LargeInput,
            )
        });

        group.finish();
    }
}

fn prepare_lix_read(
    runtime: &Runtime,
    profile: LixBackendProfile,
    rows: &[storage_bench::JsonPointerStorageRow],
) -> (
    Arc<dyn Backend + Send + Sync>,
    storage_bench::JsonPointerUntrackedStateReadFixture,
) {
    let backend = profile.backend();
    let fixture = runtime
        .block_on(storage_bench::prepare_json_pointer_untracked_state_read(
            &backend, rows,
        ))
        .expect("prepare untracked_state read");
    (backend, fixture)
}

fn prepare_raw_sqlite_empty() -> RawSqliteFixture {
    let dir = TempDir::new().expect("create raw sqlite tempdir");
    let conn = Connection::open(dir.path().join("untracked_state.sqlite"))
        .expect("open raw sqlite database");
    conn.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA temp_store = MEMORY;
        PRAGMA foreign_keys = ON;
        CREATE TABLE untracked_state (
            version_id TEXT NOT NULL,
            schema_key TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            file_id TEXT NOT NULL,
            snapshot_content TEXT,
            metadata TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            global INTEGER NOT NULL,
            PRIMARY KEY (version_id, schema_key, entity_id, file_id)
        ) WITHOUT ROWID;
        ",
    )
    .expect("create raw sqlite untracked_state table");
    RawSqliteFixture { conn, _dir: dir }
}

fn prepare_raw_sqlite_seeded(rows: &[RawUntrackedRow]) -> RawSqliteFixture {
    let fixture = prepare_raw_sqlite_empty();
    raw_sqlite_insert_all(fixture, rows)
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
                    version_id, schema_key, entity_id, file_id, snapshot_content,
                    metadata, created_at, updated_at, global
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                ",
            )
            .expect("prepare raw sqlite insert");
        for row in rows {
            statement
                .execute(params![
                    row.version_id,
                    row.schema_key,
                    row.entity_id,
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

fn raw_sqlite_select_all(fixture: RawSqliteFixture, expected_rows: usize) -> usize {
    let mut statement = fixture
        .conn
        .prepare_cached(
            "
            SELECT version_id, schema_key, entity_id, file_id, snapshot_content, metadata,
                   created_at, updated_at, global
            FROM untracked_state
            ORDER BY version_id, schema_key, entity_id, file_id
            ",
        )
        .expect("prepare raw sqlite select all");
    let count = statement
        .query_map([], |_| Ok(()))
        .expect("execute raw sqlite select all")
        .count();
    assert_eq!(count, expected_rows);
    count
}

fn raw_sqlite_select_one_by_pk(fixture: RawSqliteFixture, row: &RawUntrackedRow) -> usize {
    let found = fixture
        .conn
        .query_row(
            "
            SELECT snapshot_content
            FROM untracked_state
            WHERE version_id = ?1 AND schema_key = ?2 AND entity_id = ?3 AND file_id = ?4
            ",
            params![row.version_id, row.schema_key, row.entity_id, row.file_id],
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
                WHERE version_id = ?1 AND schema_key = ?2 AND entity_id = ?3 AND file_id = ?4
                ",
            )
            .expect("prepare raw sqlite update all");
        for row in rows {
            affected += statement
                .execute(params![
                    row.version_id,
                    row.schema_key,
                    row.entity_id,
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
            WHERE version_id = ?1 AND schema_key = ?2 AND entity_id = ?3 AND file_id = ?4
            ",
            params![
                row.version_id,
                row.schema_key,
                row.entity_id,
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
            WHERE version_id = ?1 AND schema_key = ?2 AND entity_id = ?3 AND file_id = ?4
            ",
            params![row.version_id, row.schema_key, row.entity_id, row.file_id],
        )
        .expect("execute raw sqlite delete one");
    assert_eq!(affected, 1);
    affected
}

fn fixture_rows() -> Vec<PointerRow> {
    let root: JsonValue = serde_json::from_str(PNPM_LOCK_JSON).expect("pnpm lock JSON fixture");
    let mut rows = Vec::new();
    flatten_json("", &root, &mut rows);
    assert!(
        rows.len() >= REAL_WORKLOAD_ROWS,
        "pnpm lock fixture should have at least {REAL_WORKLOAD_ROWS} pointer rows, got {}",
        rows.len()
    );
    rows
}

fn storage_rows(rows: &[PointerRow]) -> Vec<storage_bench::JsonPointerStorageRow> {
    rows.iter()
        .map(|row| storage_bench::JsonPointerStorageRow {
            path: row.path.clone(),
            value_json: row.value_json.clone(),
            updated_value_json: row.updated_value_json.clone(),
        })
        .collect()
}

fn raw_rows(rows: &[PointerRow]) -> Vec<RawUntrackedRow> {
    rows.iter()
        .map(|index| RawUntrackedRow {
            version_id: "bench-version".to_string(),
            schema_key: "json_pointer".to_string(),
            entity_id: index.path.clone(),
            file_id: "".to_string(),
            snapshot_content: json_pointer_snapshot(index, false),
            updated_snapshot_content: json_pointer_snapshot(index, true),
            metadata: None,
            created_at: timestamp(0),
            updated_at: timestamp(1),
            global: false,
        })
        .collect()
}

fn configure_group(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    _row_count: usize,
) {
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(250));
    group.measurement_time(Duration::from_secs(1));
}

fn pick_pk_row(rows: &[RawUntrackedRow]) -> &RawUntrackedRow {
    &rows[rows.len() / 2]
}

fn row_label(rows: usize) -> String {
    if rows >= 1_000 {
        format!("{}k", rows / 1_000)
    } else {
        rows.to_string()
    }
}

fn flatten_json(path: &str, value: &JsonValue, rows: &mut Vec<PointerRow>) {
    rows.push(PointerRow {
        path: path.to_string(),
        value_json: value.to_string(),
        updated_value_json: updated_value_for(path),
    });

    match value {
        JsonValue::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                let child_path = format!("{path}/{}", index);
                flatten_json(&child_path, item, rows);
            }
        }
        JsonValue::Object(map) => {
            for (key, child) in map {
                let child_path = format!("{path}/{}", escape_pointer_token(key));
                flatten_json(&child_path, child, rows);
            }
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {}
    }
}

fn json_pointer_snapshot(row: &PointerRow, updated: bool) -> String {
    let value_json = if updated {
        row.updated_value_json.as_str()
    } else {
        row.value_json.as_str()
    };
    let value = serde_json::from_str::<JsonValue>(value_json)
        .unwrap_or_else(|_| JsonValue::String(value_json.to_string()));
    serde_json::json!({
        "path": row.path,
        "value": value,
    })
    .to_string()
}

fn updated_value_for(path: &str) -> String {
    serde_json::json!({
        "updated": true,
        "path": path,
    })
    .to_string()
}

fn escape_pointer_token(token: &str) -> String {
    token.replace('~', "~0").replace('/', "~1")
}

fn timestamp(index: usize) -> String {
    format!("2024-01-01T00:00:{:02}.000Z", index % 60)
}

criterion_group!(benches, untracked_state_crud_benches);
criterion_main!(benches);
