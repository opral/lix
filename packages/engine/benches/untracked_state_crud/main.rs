use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use lix_engine::{
    storage_bench, Backend, BackendKvAccessSegment, BackendKvEntryPage, BackendKvGetRequest,
    BackendKvKeyPage, BackendKvKeySpace, BackendKvRead4Order, BackendKvRead4Page,
    BackendKvRead4Projection, BackendKvRead4ValuePart, BackendKvReadV3Page,
    BackendKvReadV3Projection, BackendKvReadV3Request, BackendKvReadV3Source, BackendKvScanRequest,
    BackendKvTableId, BackendKvTableReadRequest, BackendKvValueBatch, BackendKvValuePage,
    BackendKvWriteBatch, BackendKvWriteStats, BackendReadTransaction, BackendWriteTransaction,
    LixError,
};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value as JsonValue;
use tempfile::TempDir;
use tokio::runtime::Runtime;

mod backends;

use backends::{LixBackendProfile, LIX_BACKEND_PROFILES};

const SMOKE_ROWS: usize = 1_000;
const REAL_WORKLOAD_ROWS: usize = 10_000;
const PNPM_LOCK_JSON: &str = include_str!("pnpm-lock.fixture.json");
const UNTRACKED_ROW_NAMESPACE: &str = "u3";

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

struct RawSqliteFixture {
    conn: Connection,
    _dir: TempDir,
}

#[derive(Clone)]
struct RawProjectedRow {
    key: Vec<u8>,
    header: Vec<u8>,
    payload: Vec<u8>,
}

struct RawProjectedSqliteFixture {
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
    write_delete_ranges: usize,
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

    async fn read_v3(
        &mut self,
        request: BackendKvReadV3Request,
    ) -> Result<BackendKvReadV3Page, LixError> {
        let page = self.inner.read_v3(request.clone()).await?;
        record_read_v3(&self.stats, &request, &page);
        Ok(page)
    }

    async fn read4(
        &mut self,
        request: BackendKvTableReadRequest,
    ) -> Result<BackendKvRead4Page, LixError> {
        let page = self.inner.read4(request.clone()).await?;
        record_read4(&self.stats, &request, &page);
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

    async fn read_v3(
        &mut self,
        request: BackendKvReadV3Request,
    ) -> Result<BackendKvReadV3Page, LixError> {
        let page = self.inner.read_v3(request.clone()).await?;
        record_read_v3(&self.stats, &request, &page);
        Ok(page)
    }

    async fn read4(
        &mut self,
        request: BackendKvTableReadRequest,
    ) -> Result<BackendKvRead4Page, LixError> {
        let page = self.inner.read4(request.clone()).await?;
        record_read4(&self.stats, &request, &page);
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
        stats.write_delete_ranges += write_stats.delete_ranges;
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

fn record_read_v3(
    stats: &Arc<Mutex<IoStats>>,
    request: &BackendKvReadV3Request,
    page: &BackendKvReadV3Page,
) {
    let mut stats = stats.lock().expect("io stats mutex should lock");
    let key_bytes = page.keys.iter().map(|bytes| bytes.len()).sum::<usize>();
    let value_bytes = page
        .values
        .iter()
        .flat_map(|values| values.iter())
        .map(|bytes| bytes.len())
        .sum::<usize>();
    let is_key_only = matches!(request.projection, BackendKvReadV3Projection::KeysOnly);
    let point_like = matches!(request.source, BackendKvReadV3Source::Keys { .. });
    if point_like {
        if is_key_only {
            stats.exists_calls += 1;
            stats.exists_keys += page.presence_len();
            stats.exists_key_bytes += key_bytes;
        } else {
            stats.get_calls += 1;
            stats.get_keys += page.presence_len();
            stats.get_key_bytes += key_bytes;
            stats.get_values += page.present_count();
            stats.get_value_bytes += value_bytes;
        }
    } else if is_key_only {
        stats.scan_key_calls += 1;
        stats.scan_keys += page.keys.len();
        stats.scan_key_bytes += key_bytes;
    } else {
        stats.scan_entry_calls += 1;
        stats.scan_entries += page.keys.len();
        stats.scan_entry_key_bytes += key_bytes;
        stats.scan_entry_value_bytes += value_bytes;
    }
}

fn record_read4(
    stats: &Arc<Mutex<IoStats>>,
    request: &BackendKvTableReadRequest,
    page: &BackendKvRead4Page,
) {
    let mut stats = stats.lock().expect("io stats mutex should lock");
    let key_bytes = page.keys.iter().map(|bytes| bytes.len()).sum::<usize>();
    let value_bytes = page
        .values
        .iter()
        .flat_map(|values| values.iter())
        .map(|bytes| bytes.len())
        .sum::<usize>();
    let is_key_only = matches!(request.projection, BackendKvRead4Projection::KeysOnly);
    let point_like = request
        .access
        .iter()
        .all(|segment| !matches!(segment, BackendKvAccessSegment::Span { .. }));
    if point_like {
        if is_key_only {
            stats.exists_calls += 1;
            stats.exists_keys += page.presence_len();
            stats.exists_key_bytes += key_bytes;
        } else {
            stats.get_calls += 1;
            stats.get_keys += page.presence_len();
            stats.get_key_bytes += key_bytes;
            stats.get_values += page.present_count();
            stats.get_value_bytes += value_bytes;
        }
    } else if is_key_only {
        stats.scan_key_calls += 1;
        stats.scan_keys += page.keys.len();
        stats.scan_key_bytes += key_bytes;
    } else {
        stats.scan_entry_calls += 1;
        stats.scan_entries += page.keys.len();
        stats.scan_entry_key_bytes += key_bytes;
        stats.scan_entry_value_bytes += value_bytes;
    }
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

    bench_storage_plan_smoke(c, &runtime, &rows);
    bench_read4_density_smoke(c, &runtime, &rows);
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
        "logical backend KV request/result accounting; not physical disk, WAL, or compaction I/O"
    );
    println!(
        "| workload | backend | operation | logical rows | io ops | io ops/row | io bytes | io bytes/row | read calls | get calls | get keys | scan calls | read rows | read bytes | read bytes/row | write batches | puts | deletes | delete ranges | write bytes | write bytes/row |"
    );
    println!(
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    );

    for (label, row_count) in workloads {
        let rows = storage_rows(&all_rows[..row_count]);
        for profile in LIX_BACKEND_PROFILES {
            for operation in [
                "insert_all_rows",
                "select_all_rows",
                "select_keys_only",
                "select_headers_only",
                "select_one_by_pk",
                "select_all_by_pk",
                "update_all_rows",
                "update_one_by_pk",
                "delete_all_rows",
                "delete_one_by_pk",
            ] {
                let stats = measure_lix_io(runtime, profile, operation, &rows);
                let logical_rows = operation_logical_rows(operation, row_count);
                println!(
                    "| {label}/{rows_label} | {} | `{operation}` | {logical_rows} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
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
        "select_headers_only" => {
            let fixture = runtime
                .block_on(storage_bench::prepare_json_pointer_untracked_state_read(
                    &backend, rows,
                ))
                .expect("prepare untracked_state select headers io");
            reset_io_stats(&stats);
            runtime
                .block_on(
                    storage_bench::json_pointer_untracked_state_scan_headers_only_prepared(
                        &backend, &fixture,
                    ),
                )
                .expect("measure untracked_state select headers io");
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

fn operation_logical_rows(operation: &str, row_count: usize) -> usize {
    match operation {
        "select_one_by_pk" | "update_one_by_pk" | "delete_one_by_pk" => 1,
        _ => row_count,
    }
}

fn ratio(numerator: usize, denominator: usize) -> String {
    if denominator == 0 {
        return "-".to_string();
    }
    format!("{:.2}", numerator as f64 / denominator as f64)
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
    for profile in LIX_BACKEND_PROFILES {
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

        group.bench_function(format!("select_headers_only/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || prepare_lix_read(runtime, profile, &rows),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(
                                storage_bench::json_pointer_untracked_state_scan_headers_only_prepared(
                                    &backend, &fixture,
                                ),
                            )
                            .expect("untracked_state scan headers"),
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

fn bench_storage_plan_smoke(c: &mut Criterion, runtime: &Runtime, all_rows: &[PointerRow]) {
    let rows = storage_rows(&all_rows[..SMOKE_ROWS]);
    for profile in LIX_BACKEND_PROFILES {
        let mut group = c.benchmark_group(format!(
            "untracked_state_crud/storage_plans/{}/smoke",
            profile.name()
        ));
        configure_group(&mut group, SMOKE_ROWS);

        group.bench_function(
            format!("read4_span_header/{}", row_label(SMOKE_ROWS)),
            |b| {
                b.iter_batched(
                    || prepare_lix_read(runtime, profile, &rows),
                    |(backend, _fixture)| {
                        black_box(
                            runtime
                                .block_on(storage_plan_read4_header(&backend, SMOKE_ROWS))
                                .expect("storage plan read4 header"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );

        group.bench_function(
            format!("read4_span_header_payload/{}", row_label(SMOKE_ROWS)),
            |b| {
                b.iter_batched(
                    || prepare_lix_read(runtime, profile, &rows),
                    |(backend, _fixture)| {
                        black_box(
                            runtime
                                .block_on(storage_plan_read4_header_payload(&backend, SMOKE_ROWS))
                                .expect("storage plan read4 header payload"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );

        group.bench_function(format!("read4_span_full/{}", row_label(SMOKE_ROWS)), |b| {
            b.iter_batched(
                || prepare_lix_read(runtime, profile, &rows),
                |(backend, _fixture)| {
                    black_box(
                        runtime
                            .block_on(storage_plan_read4_full(&backend, SMOKE_ROWS))
                            .expect("storage plan read4 full"),
                    )
                },
                BatchSize::LargeInput,
            )
        });

        group.finish();
    }

    let raw_rows = raw_projected_rows(&all_rows[..SMOKE_ROWS]);
    let mut group =
        c.benchmark_group("untracked_state_crud/storage_plans/raw_sqlite_projected/smoke");
    configure_group(&mut group, SMOKE_ROWS);
    group.bench_function(format!("scan_keys/{}", row_label(SMOKE_ROWS)), |b| {
        b.iter_batched(
            || prepare_raw_projected_sqlite_seeded(&raw_rows),
            |fixture| black_box(raw_projected_sqlite_scan_keys(fixture, SMOKE_ROWS)),
            BatchSize::LargeInput,
        )
    });
    group.bench_function(format!("scan_header/{}", row_label(SMOKE_ROWS)), |b| {
        b.iter_batched(
            || prepare_raw_projected_sqlite_seeded(&raw_rows),
            |fixture| black_box(raw_projected_sqlite_scan_header(fixture, SMOKE_ROWS)),
            BatchSize::LargeInput,
        )
    });
    group.bench_function(format!("scan_full/{}", row_label(SMOKE_ROWS)), |b| {
        b.iter_batched(
            || prepare_raw_projected_sqlite_seeded(&raw_rows),
            |fixture| black_box(raw_projected_sqlite_scan_full(fixture, SMOKE_ROWS)),
            BatchSize::LargeInput,
        )
    });
    group.finish();
}

#[derive(Debug, Clone, Copy)]
enum Read4DensityShape {
    Points,
    Run,
    Span,
}

impl Read4DensityShape {
    fn label(self) -> &'static str {
        match self {
            Self::Points => "points",
            Self::Run => "run",
            Self::Span => "span",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Read4DensityProjection {
    KeysOnly,
    Header,
    PayloadRef,
    FullValue,
}

impl Read4DensityProjection {
    fn label(self) -> &'static str {
        match self {
            Self::KeysOnly => "keys",
            Self::Header => "header",
            Self::PayloadRef => "payload_ref",
            Self::FullValue => "full",
        }
    }

    fn projection(self) -> BackendKvRead4Projection {
        match self {
            Self::KeysOnly => BackendKvRead4Projection::KeysOnly,
            Self::Header => BackendKvRead4Projection::Parts(vec![BackendKvRead4ValuePart::Header]),
            Self::PayloadRef => {
                BackendKvRead4Projection::Parts(vec![BackendKvRead4ValuePart::PayloadRef])
            }
            Self::FullValue => {
                BackendKvRead4Projection::Parts(vec![BackendKvRead4ValuePart::FullValue])
            }
        }
    }

    fn part_count(self) -> usize {
        match self {
            Self::KeysOnly => 0,
            Self::Header | Self::PayloadRef | Self::FullValue => 1,
        }
    }
}

fn bench_read4_density_smoke(c: &mut Criterion, runtime: &Runtime, all_rows: &[PointerRow]) {
    let rows = storage_rows(&all_rows[..SMOKE_ROWS]);
    let projections = [
        Read4DensityProjection::KeysOnly,
        Read4DensityProjection::Header,
        Read4DensityProjection::PayloadRef,
        Read4DensityProjection::FullValue,
    ];
    for profile in LIX_BACKEND_PROFILES {
        let mut group = c.benchmark_group(format!(
            "untracked_state_crud/read4_density/{}/smoke",
            profile.name()
        ));
        configure_group(&mut group, SMOKE_ROWS);

        for density in [1usize, 10, 50, 100] {
            for ordered in [true, false] {
                for shape in [Read4DensityShape::Points, Read4DensityShape::Run] {
                    for projection in projections {
                        group.bench_function(
                            format!(
                                "{}/{}/{}/{}/{}",
                                shape.label(),
                                if ordered { "sorted" } else { "random" },
                                projection.label(),
                                density,
                                row_label(SMOKE_ROWS)
                            ),
                            |b| {
                                b.iter_batched(
                                    || {
                                        let (backend, _fixture) =
                                            prepare_lix_read(runtime, profile, &rows);
                                        let all_keys = runtime
                                            .block_on(collect_untracked_keys(&backend, SMOKE_ROWS))
                                            .expect("collect untracked keys");
                                        let selected =
                                            select_density_keys(&all_keys, density, ordered);
                                        (backend, selected)
                                    },
                                    |(backend, selected)| {
                                        black_box(
                                            runtime
                                                .block_on(storage_plan_read4_density(
                                                    &backend, shape, projection, selected,
                                                    SMOKE_ROWS,
                                                ))
                                                .expect("read4 density plan"),
                                        )
                                    },
                                    BatchSize::LargeInput,
                                )
                            },
                        );
                    }
                }
            }
        }

        for projection in projections {
            group.bench_function(
                format!(
                    "span/key_order/{}/100/{}",
                    projection.label(),
                    row_label(SMOKE_ROWS)
                ),
                |b| {
                    b.iter_batched(
                        || {
                            let (backend, _fixture) = prepare_lix_read(runtime, profile, &rows);
                            let all_keys = runtime
                                .block_on(collect_untracked_keys(&backend, SMOKE_ROWS))
                                .expect("collect untracked keys");
                            (backend, all_keys)
                        },
                        |(backend, keys)| {
                            black_box(
                                runtime
                                    .block_on(storage_plan_read4_density(
                                        &backend,
                                        Read4DensityShape::Span,
                                        projection,
                                        keys,
                                        SMOKE_ROWS,
                                    ))
                                    .expect("read4 span density plan"),
                            )
                        },
                        BatchSize::LargeInput,
                    )
                },
            );
        }

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

async fn collect_untracked_keys(
    backend: &Arc<dyn Backend + Send + Sync>,
    expected_rows: usize,
) -> Result<Vec<Vec<u8>>, LixError> {
    let mut tx = backend.begin_read_transaction().await?;
    let page = tx
        .read4(BackendKvTableReadRequest {
            table: BackendKvTableId {
                namespace: UNTRACKED_ROW_NAMESPACE.to_string(),
            },
            key_space: BackendKvKeySpace::OrderedBytes,
            access: vec![BackendKvAccessSegment::Span {
                lower: Vec::new(),
                upper: Vec::new(),
            }],
            after: None,
            projection: BackendKvRead4Projection::KeysOnly,
            residual_filter: None,
            output_order: BackendKvRead4Order::KeyOrder,
            limit: Some(expected_rows),
            session: None,
        })
        .await?;
    tx.rollback().await?;
    assert_eq!(page.keys.len(), expected_rows);
    Ok(page.keys.iter().map(<[u8]>::to_vec).collect())
}

fn select_density_keys(keys: &[Vec<u8>], density: usize, sorted: bool) -> Vec<Vec<u8>> {
    let count = keys.len().saturating_mul(density).div_ceil(100).max(1);
    let mut selected = if sorted {
        keys.iter().take(count).cloned().collect::<Vec<_>>()
    } else {
        let step = if keys.len() > 1 { keys.len() - 3 } else { 1 };
        (0..count)
            .map(|index| keys[(index * step) % keys.len()].clone())
            .collect::<Vec<_>>()
    };
    if sorted {
        selected.sort();
    }
    selected
}

async fn storage_plan_read4_density(
    backend: &Arc<dyn Backend + Send + Sync>,
    shape: Read4DensityShape,
    projection: Read4DensityProjection,
    keys: Vec<Vec<u8>>,
    expected_total_rows: usize,
) -> Result<usize, LixError> {
    let expected_rows = match shape {
        Read4DensityShape::Span => expected_total_rows,
        Read4DensityShape::Points | Read4DensityShape::Run => keys.len(),
    };
    let access = read4_density_access(shape, &keys)?;
    let mut tx = backend.begin_read_transaction().await?;
    let page = tx
        .read4(BackendKvTableReadRequest {
            table: BackendKvTableId {
                namespace: UNTRACKED_ROW_NAMESPACE.to_string(),
            },
            key_space: BackendKvKeySpace::OrderedBytes,
            access,
            after: None,
            projection: projection.projection(),
            residual_filter: None,
            output_order: match shape {
                Read4DensityShape::Span => BackendKvRead4Order::KeyOrder,
                Read4DensityShape::Points | Read4DensityShape::Run => {
                    BackendKvRead4Order::RequestOrder
                }
            },
            limit: Some(expected_rows),
            session: None,
        })
        .await?;
    tx.rollback().await?;
    assert_eq!(page.keys.len(), expected_rows);
    assert_eq!(page.values.len(), projection.part_count());
    for values in &page.values {
        assert_eq!(values.len(), expected_rows);
    }
    Ok(page.keys.iter().map(|key| key.len()).sum::<usize>()
        + page
            .values
            .iter()
            .flat_map(|values| values.iter())
            .map(|value| value.len())
            .sum::<usize>())
}

fn read4_density_access(
    shape: Read4DensityShape,
    keys: &[Vec<u8>],
) -> Result<Vec<BackendKvAccessSegment>, LixError> {
    let request_indexes = (0..keys.len())
        .map(|index| u32::try_from(index).map_err(|_| LixError::unknown("read4 index overflow")))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(match shape {
        Read4DensityShape::Points => vec![BackendKvAccessSegment::Points {
            keys: keys.to_vec(),
            request_indexes,
        }],
        Read4DensityShape::Run => {
            let mut sorted = keys.to_vec();
            sorted.sort();
            let lower = sorted.first().cloned().unwrap_or_default();
            let mut upper = sorted.last().cloned().unwrap_or_default();
            upper.push(0);
            vec![BackendKvAccessSegment::Run {
                lower,
                upper,
                keys: keys.to_vec(),
                request_indexes,
            }]
        }
        Read4DensityShape::Span => vec![BackendKvAccessSegment::Span {
            lower: Vec::new(),
            upper: Vec::new(),
        }],
    })
}

async fn storage_plan_read4_header(
    backend: &Arc<dyn Backend + Send + Sync>,
    expected_rows: usize,
) -> Result<usize, LixError> {
    storage_plan_read4_value_parts(backend, expected_rows, &[BackendKvRead4ValuePart::Header]).await
}

async fn storage_plan_read4_header_payload(
    backend: &Arc<dyn Backend + Send + Sync>,
    expected_rows: usize,
) -> Result<usize, LixError> {
    storage_plan_read4_value_parts(
        backend,
        expected_rows,
        &[
            BackendKvRead4ValuePart::Header,
            BackendKvRead4ValuePart::Payload,
        ],
    )
    .await
}

async fn storage_plan_read4_full(
    backend: &Arc<dyn Backend + Send + Sync>,
    expected_rows: usize,
) -> Result<usize, LixError> {
    storage_plan_read4_value_parts(
        backend,
        expected_rows,
        &[BackendKvRead4ValuePart::FullValue],
    )
    .await
}

async fn storage_plan_read4_value_parts(
    backend: &Arc<dyn Backend + Send + Sync>,
    expected_rows: usize,
    parts: &[BackendKvRead4ValuePart],
) -> Result<usize, LixError> {
    let mut tx = backend.begin_read_transaction().await?;
    let page = tx
        .read4(BackendKvTableReadRequest {
            table: BackendKvTableId {
                namespace: UNTRACKED_ROW_NAMESPACE.to_string(),
            },
            key_space: BackendKvKeySpace::OrderedBytes,
            access: vec![BackendKvAccessSegment::Span {
                lower: Vec::new(),
                upper: Vec::new(),
            }],
            after: None,
            projection: BackendKvRead4Projection::Parts(parts.to_vec()),
            residual_filter: None,
            output_order: BackendKvRead4Order::KeyOrder,
            limit: Some(expected_rows),
            session: None,
        })
        .await?;
    tx.rollback().await?;
    assert_eq!(page.keys.len(), expected_rows);
    assert_eq!(page.values.len(), parts.len());
    for values in &page.values {
        assert_eq!(values.len(), expected_rows);
    }
    Ok(page.keys.iter().map(|key| key.len()).sum::<usize>()
        + page
            .values
            .iter()
            .flat_map(|values| values.iter())
            .map(|value| value.len())
            .sum::<usize>())
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

fn prepare_raw_projected_sqlite_empty() -> RawProjectedSqliteFixture {
    let dir = TempDir::new().expect("create raw projected sqlite tempdir");
    let conn = Connection::open(dir.path().join("untracked_projected.sqlite"))
        .expect("open raw projected sqlite database");
    conn.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA temp_store = MEMORY;
        PRAGMA foreign_keys = ON;
        CREATE TABLE untracked_projected (
            key BLOB NOT NULL PRIMARY KEY,
            header BLOB NOT NULL,
            payload BLOB NOT NULL
        ) WITHOUT ROWID;
        ",
    )
    .expect("create raw projected sqlite untracked table");
    RawProjectedSqliteFixture { conn, _dir: dir }
}

fn prepare_raw_sqlite_seeded(rows: &[RawUntrackedRow]) -> RawSqliteFixture {
    let fixture = prepare_raw_sqlite_empty();
    raw_sqlite_insert_all(fixture, rows)
}

fn prepare_raw_projected_sqlite_seeded(rows: &[RawProjectedRow]) -> RawProjectedSqliteFixture {
    let mut fixture = prepare_raw_projected_sqlite_empty();
    let tx = fixture
        .conn
        .transaction()
        .expect("begin raw projected sqlite insert");
    {
        let mut statement = tx
            .prepare_cached(
                "
                INSERT INTO untracked_projected (key, header, payload)
                VALUES (?1, ?2, ?3)
                ",
            )
            .expect("prepare raw projected sqlite insert");
        for row in rows {
            statement
                .execute(params![row.key, row.header, row.payload])
                .expect("execute raw projected sqlite insert");
        }
    }
    tx.commit().expect("commit raw projected sqlite insert");
    fixture
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

fn raw_projected_sqlite_scan_keys(
    fixture: RawProjectedSqliteFixture,
    expected_rows: usize,
) -> usize {
    let mut statement = fixture
        .conn
        .prepare_cached("SELECT key FROM untracked_projected ORDER BY key")
        .expect("prepare raw projected keys scan");
    let bytes = statement
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .expect("execute raw projected keys scan")
        .map(|row| row.expect("raw projected key row").len())
        .sum::<usize>();
    assert!(bytes > expected_rows);
    bytes
}

fn raw_projected_sqlite_scan_header(
    fixture: RawProjectedSqliteFixture,
    expected_rows: usize,
) -> usize {
    let mut statement = fixture
        .conn
        .prepare_cached("SELECT key, header FROM untracked_projected ORDER BY key")
        .expect("prepare raw projected header scan");
    let mut rows = 0usize;
    let bytes = statement
        .query_map([], |row| {
            Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
        })
        .expect("execute raw projected header scan")
        .map(|row| {
            rows += 1;
            let (key, header) = row.expect("raw projected header row");
            key.len() + header.len()
        })
        .sum::<usize>();
    assert_eq!(rows, expected_rows);
    bytes
}

fn raw_projected_sqlite_scan_full(
    fixture: RawProjectedSqliteFixture,
    expected_rows: usize,
) -> usize {
    let mut statement = fixture
        .conn
        .prepare_cached("SELECT key, header, payload FROM untracked_projected ORDER BY key")
        .expect("prepare raw projected full scan");
    let mut rows = 0usize;
    let bytes = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })
        .expect("execute raw projected full scan")
        .map(|row| {
            rows += 1;
            let (key, header, payload) = row.expect("raw projected full row");
            key.len() + header.len() + payload.len()
        })
        .sum::<usize>();
    assert_eq!(rows, expected_rows);
    bytes
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

fn raw_projected_rows(rows: &[PointerRow]) -> Vec<RawProjectedRow> {
    rows.iter()
        .map(|row| RawProjectedRow {
            key: row.path.as_bytes().to_vec(),
            header: format!(
                "{{\"version_id\":\"bench-version\",\"schema_key\":\"json_pointer\",\"entity_id\":{},\"created_at\":\"{}\",\"updated_at\":\"{}\",\"global\":false}}",
                serde_json::to_string(&row.path).expect("path serializes"),
                timestamp(0),
                timestamp(1)
            )
            .into_bytes(),
            payload: json_pointer_snapshot(row, false).into_bytes(),
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
