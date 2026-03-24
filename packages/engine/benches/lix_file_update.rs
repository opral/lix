use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use lix_engine::{
    boot, BootArgs, BootKeyValue, Engine, LixBackend, LixBackendTransaction, LixError,
    NoopWasmRuntime, QueryResult, SqlDialect, Value,
};
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use tempfile::TempDir;
use tokio::runtime::Runtime;

#[path = "support/mod.rs"]
mod support;

use support::sqlite_backend::BenchSqliteBackend;

// This benchmark measures the steady-state cost of an exact-id `lix_file` update without the
// deep-history fixture. Keep `lix_file_recursive_update` as the history-sensitivity guardrail.
//
// The goal here is to answer a different question: once history lookups are no longer dominating,
// how expensive is one ordinary file update on the hot path, and how much of that cost comes from
// mutating the file payload versus descriptor/commit overhead?
const FILE_ID: &str = "bench-update-file";
const FILE_PATH: &str = "/bench/update/file.bin";
const DATA_UPDATE_BYTES: usize = 100 * 1024;
const TRACE_LIMIT: usize = 12;

fn bench_lix_file_update(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create tokio runtime");
    maybe_print_trace_reports(&runtime);
    let mut group = c.benchmark_group("lix_file");
    group.sample_size(10);
    group.throughput(Throughput::Elements(1));

    group.bench_function("update_existing_row/data_update_100kb", |b| {
        b.iter_batched_ref(
            || build_fixture(&runtime),
            |fixture| fixture.update_data_once(&runtime),
            BatchSize::SmallInput,
        );
    });

    group.bench_function("update_existing_row/metadata_only", |b| {
        b.iter_batched_ref(
            || build_fixture(&runtime),
            |fixture| fixture.update_metadata_once(&runtime),
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

struct BenchFixture {
    engine: Engine,
    next_revision: usize,
    _tempdir: TempDir,
}

#[derive(Debug, Clone)]
struct SqlTraceOperation {
    sequence: u64,
    kind: &'static str,
    sql: Option<String>,
    duration_ms: f64,
}

#[derive(Debug, Default)]
struct SqlTraceCollector {
    next_sequence: AtomicU64,
    operations: Mutex<Vec<SqlTraceOperation>>,
}

impl SqlTraceCollector {
    fn push(&self, kind: &'static str, sql: Option<&str>, duration_ms: f64) {
        let mut guard = self
            .operations
            .lock()
            .expect("sql trace collector mutex should not be poisoned");
        guard.push(SqlTraceOperation {
            sequence: self.next_sequence.fetch_add(1, Ordering::Relaxed),
            kind,
            sql: sql.map(ToOwned::to_owned),
            duration_ms,
        });
    }

    fn snapshot(&self) -> Vec<SqlTraceOperation> {
        self.operations
            .lock()
            .expect("sql trace collector mutex should not be poisoned")
            .clone()
    }

    fn clear(&self) {
        self.operations
            .lock()
            .expect("sql trace collector mutex should not be poisoned")
            .clear();
        self.next_sequence.store(0, Ordering::Relaxed);
    }
}

struct TracingBenchBackend {
    inner: BenchSqliteBackend,
    collector: Arc<SqlTraceCollector>,
}

struct TracingBenchTransaction<'a> {
    inner: Box<dyn LixBackendTransaction + 'a>,
    collector: Arc<SqlTraceCollector>,
}

impl BenchFixture {
    fn update_data_once(&mut self, runtime: &Runtime) {
        let payload = data_payload_for_revision(self.next_revision);
        runtime
            .block_on(self.engine.execute(
                "UPDATE lix_file SET data = ? WHERE id = ?",
                &[Value::Blob(payload), Value::Text(FILE_ID.to_string())],
            ))
            .expect("data update should succeed");
        self.next_revision += 1;
    }

    fn update_metadata_once(&mut self, runtime: &Runtime) {
        let metadata = format!(
            "{{\"owner\":\"bench\",\"revision\":{},\"kind\":\"metadata-only\"}}",
            self.next_revision
        );
        runtime
            .block_on(self.engine.execute(
                "UPDATE lix_file SET metadata = ? WHERE id = ?",
                &[Value::Text(metadata), Value::Text(FILE_ID.to_string())],
            ))
            .expect("metadata update should succeed");
        self.next_revision += 1;
    }
}

fn build_fixture(runtime: &Runtime) -> BenchFixture {
    build_fixture_with_trace(runtime, None)
}

fn build_fixture_with_trace(
    runtime: &Runtime,
    trace_collector: Option<Arc<SqlTraceCollector>>,
) -> BenchFixture {
    let tempdir = TempDir::new().expect("tempdir should be created");
    let db_path = tempdir.path().join("fixture.sqlite");
    let backend = BenchSqliteBackend::file_backed(&db_path).expect("file-backed sqlite backend");
    let backend: Box<dyn LixBackend + Send + Sync> = match trace_collector {
        Some(collector) => Box::new(TracingBenchBackend {
            inner: backend,
            collector,
        }),
        None => Box::new(backend),
    };

    let mut boot_args = BootArgs::new(backend, Arc::new(NoopWasmRuntime));
    boot_args.key_values.push(BootKeyValue {
        key: "lix_deterministic_mode".to_string(),
        value: json!({ "enabled": true }),
        lixcol_global: Some(true),
        lixcol_untracked: None,
    });

    let engine = boot(boot_args);
    runtime
        .block_on(engine.initialize())
        .expect("engine initialization should succeed");

    runtime
        .block_on(engine.execute(
            "INSERT INTO lix_file (id, path, data, metadata) VALUES (?, ?, ?, ?)",
            &[
                Value::Text(FILE_ID.to_string()),
                Value::Text(FILE_PATH.to_string()),
                Value::Blob(data_payload_for_revision(0)),
                Value::Text("{\"owner\":\"bench\",\"revision\":0}".to_string()),
            ],
        ))
        .expect("seed file insert should succeed");

    BenchFixture {
        engine,
        next_revision: 1,
        _tempdir: tempdir,
    }
}

fn maybe_print_trace_reports(runtime: &Runtime) {
    if !trace_report_enabled() {
        return;
    }

    let collector = Arc::new(SqlTraceCollector::default());

    {
        let mut fixture = build_fixture_with_trace(runtime, Some(Arc::clone(&collector)));
        collector.clear();
        fixture.update_metadata_once(runtime);
        print_trace_report("metadata_only", &collector.snapshot());
    }

    {
        let mut fixture = build_fixture_with_trace(runtime, Some(Arc::clone(&collector)));
        collector.clear();
        fixture.update_data_once(runtime);
        print_trace_report("data_update_100kb", &collector.snapshot());
    }
}

fn print_trace_report(label: &str, operations: &[SqlTraceOperation]) {
    let total_ms = operations.iter().map(|op| op.duration_ms).sum::<f64>();
    let tx_exec_count = operations
        .iter()
        .filter(|op| op.kind == "transaction_execute")
        .count();
    let backend_exec_count = operations
        .iter()
        .filter(|op| op.kind == "backend_execute")
        .count();

    eprintln!(
        "[bench-trace] lix_file/update_existing_row/{} total_ops={} backend_exec={} tx_exec={} total_ms={:.3}",
        label,
        operations.len(),
        backend_exec_count,
        tx_exec_count,
        total_ms,
    );

    let mut phase_rows = BTreeMap::<&'static str, TraceSummary>::new();
    for op in operations {
        let phase = classify_phase(op);
        let entry = phase_rows.entry(phase).or_default();
        entry.count += 1;
        entry.total_ms += op.duration_ms;
        entry.max_ms = entry.max_ms.max(op.duration_ms);
        entry.first_sequence = entry.first_sequence.min(op.sequence);
        entry.kind = op.kind;
    }
    let mut phase_rows = phase_rows.into_iter().collect::<Vec<_>>();
    phase_rows.sort_by(|left, right| {
        right
            .1
            .total_ms
            .partial_cmp(&left.1.total_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    for (phase, summary) in phase_rows {
        eprintln!(
            "[bench-trace] phase label={} phase={} count={} total_ms={:.3} avg_ms={:.3} max_ms={:.3}",
            label,
            phase,
            summary.count,
            summary.total_ms,
            summary.total_ms / summary.count as f64,
            summary.max_ms,
        );
    }

    let mut grouped = BTreeMap::<String, TraceSummary>::new();
    for op in operations.iter().filter(|op| op.sql.is_some()) {
        let normalized = normalize_sql(op.sql.as_deref().unwrap_or_default());
        let entry = grouped.entry(normalized).or_default();
        entry.count += 1;
        entry.total_ms += op.duration_ms;
        entry.max_ms = entry.max_ms.max(op.duration_ms);
        entry.first_sequence = entry.first_sequence.min(op.sequence);
        entry.kind = op.kind;
    }

    let mut rows = grouped.into_iter().collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .1
            .total_ms
            .partial_cmp(&left.1.total_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.1.first_sequence.cmp(&right.1.first_sequence))
    });

    for (index, (sql, summary)) in rows.into_iter().take(TRACE_LIMIT).enumerate() {
        eprintln!(
            "[bench-trace] top_sql label={} rank={} kind={} count={} total_ms={:.3} avg_ms={:.3} max_ms={:.3} sql={}",
            label,
            index + 1,
            summary.kind,
            summary.count,
            summary.total_ms,
            summary.total_ms / summary.count as f64,
            summary.max_ms,
            summarize_sql(&sql),
        );
    }

    if std::env::var_os("LIX_BENCH_TRACE_VERBOSE").is_some() {
        for op in operations {
            eprintln!(
                "[bench-trace] raw label={} seq={} kind={} ms={:.3} sql={}",
                label,
                op.sequence,
                op.kind,
                op.duration_ms,
                summarize_sql(op.sql.as_deref().unwrap_or("<no-sql>")),
            );
        }
    }
}

fn classify_phase(operation: &SqlTraceOperation) -> &'static str {
    let Some(sql) = operation.sql.as_deref() else {
        return match operation.kind {
            "begin_transaction" => "transaction_boundary",
            "transaction_commit" => "transaction_boundary",
            "transaction_rollback" => "transaction_boundary",
            _ => "uncategorized",
        };
    };

    if sql.contains("lix_internal_observe_tick") || sql.contains("lix_internal_live_untracked_v1") {
        return "runtime_bookkeeping";
    }
    if sql.contains("lix_internal_binary_blob") || sql.contains("lix_internal_snapshot") {
        return "snapshot_blob";
    }
    if sql.contains("lix_internal_live_v1_lix_change_set")
        || sql.contains("lix_internal_live_v1_lix_change_set_element")
        || sql.contains("lix_internal_live_v1_lix_commit")
        || sql.contains("lix_internal_live_v1_lix_commit_edge")
        || sql.contains("lix_internal_live_v1_lix_version_ref")
    {
        return "commit_bookkeeping";
    }
    if sql.contains("lix_internal_live_v1_lix_file_descriptor")
        || sql.contains("lix_internal_live_v1_lix_directory_descriptor")
        || sql.contains("resolved_file")
        || sql.contains("WITH RECURSIVE directory_path")
    {
        return "filesystem_live_lookup";
    }
    if sql.contains("lix_internal_stored_schema_bootstrap") || sql.contains("sqlite_master") {
        return "bootstrap_lookup";
    }

    "other_sql"
}

#[derive(Debug)]
struct TraceSummary {
    kind: &'static str,
    count: usize,
    total_ms: f64,
    max_ms: f64,
    first_sequence: u64,
}

impl Default for TraceSummary {
    fn default() -> Self {
        Self {
            kind: "unknown",
            count: 0,
            total_ms: 0.0,
            max_ms: 0.0,
            first_sequence: u64::MAX,
        }
    }
}

fn trace_report_enabled() -> bool {
    std::env::var("LIX_BENCH_TRACE_UPDATE")
        .map(|raw| {
            let normalized = raw.trim().to_ascii_lowercase();
            !normalized.is_empty() && normalized != "0" && normalized != "false"
        })
        .unwrap_or(false)
}

fn normalize_sql(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn summarize_sql(sql: &str) -> String {
    const MAX_SQL_CHARS: usize = 220;
    if sql.len() <= MAX_SQL_CHARS {
        return sql.to_string();
    }
    format!("{}...", &sql[..MAX_SQL_CHARS])
}

fn data_payload_for_revision(revision: usize) -> Vec<u8> {
    let prefix = format!("data-update:{revision:08}|");
    let mut payload = Vec::with_capacity(DATA_UPDATE_BYTES);
    while payload.len() < DATA_UPDATE_BYTES {
        payload.extend_from_slice(prefix.as_bytes());
    }
    payload.truncate(DATA_UPDATE_BYTES);
    payload
}

#[async_trait(?Send)]
impl LixBackend for TracingBenchBackend {
    fn dialect(&self) -> SqlDialect {
        self.inner.dialect()
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let started = std::time::Instant::now();
        let result = self.inner.execute(sql, params).await;
        self.collector.push(
            "backend_execute",
            Some(sql),
            started.elapsed().as_secs_f64() * 1000.0,
        );
        result
    }

    async fn begin_transaction(&self) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        let started = std::time::Instant::now();
        let tx = self.inner.begin_transaction().await?;
        self.collector.push(
            "begin_transaction",
            None,
            started.elapsed().as_secs_f64() * 1000.0,
        );
        Ok(Box::new(TracingBenchTransaction {
            inner: tx,
            collector: Arc::clone(&self.collector),
        }))
    }

    async fn begin_savepoint(
        &self,
        name: &str,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        let started = std::time::Instant::now();
        let tx = self.inner.begin_savepoint(name).await?;
        self.collector.push(
            "begin_savepoint",
            Some(name),
            started.elapsed().as_secs_f64() * 1000.0,
        );
        Ok(Box::new(TracingBenchTransaction {
            inner: tx,
            collector: Arc::clone(&self.collector),
        }))
    }
}

#[async_trait(?Send)]
impl LixBackendTransaction for TracingBenchTransaction<'_> {
    fn dialect(&self) -> SqlDialect {
        self.inner.dialect()
    }

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let started = std::time::Instant::now();
        let result = self.inner.execute(sql, params).await;
        self.collector.push(
            "transaction_execute",
            Some(sql),
            started.elapsed().as_secs_f64() * 1000.0,
        );
        result
    }

    async fn commit(self: Box<Self>) -> Result<(), LixError> {
        let Self { inner, collector } = *self;
        let started = std::time::Instant::now();
        let result = inner.commit().await;
        collector.push(
            "transaction_commit",
            None,
            started.elapsed().as_secs_f64() * 1000.0,
        );
        result
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        let Self { inner, collector } = *self;
        let started = std::time::Instant::now();
        let result = inner.rollback().await;
        collector.push(
            "transaction_rollback",
            None,
            started.elapsed().as_secs_f64() * 1000.0,
        );
        result
    }
}

criterion_group!(benches, bench_lix_file_update);
criterion_main!(benches);
