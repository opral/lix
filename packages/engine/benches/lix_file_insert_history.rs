use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use lix_engine::{
    boot, BootArgs, BootKeyValue, Engine, LixBackend, LixError, LixTransaction, NoopWasmRuntime,
    PreparedBatch, QueryResult, SqlDialect, Value,
};
use serde_json::json;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use tempfile::TempDir;
use tokio::runtime::Runtime;

#[path = "support/mod.rs"]
mod support;

use support::sqlite_backend::BenchSqliteBackend;

// This bench isolates the remaining "history-loaded repo insert" regression without needing a
// full git replay. The fixture builds many prior tracked insert commits into a template SQLite DB,
// then measures one multi-row tracked insert into brand new file ids/paths on a copy of that DB.
// That keeps the measured operation small while still reproducing the "history-loaded repo insert"
// shape we care about in CI.
const HISTORY_DEPTHS: &[usize] = &[0, 512];
const INSERT_ROW_COUNT: usize = 3;
const INSERT_PATHS: [&str; INSERT_ROW_COUNT] = [
    "/bench/history-insert/a.json",
    "/bench/history-insert/b.json",
    "/bench/history-insert/c.json",
];
const PAYLOAD_BYTES: usize = 1024;
const EXISTING_BUCKET_COUNT: usize = 32;
const TRACE_LIMIT: usize = 12;

fn bench_lix_file_insert_history(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create tokio runtime");
    maybe_print_trace_reports(&runtime);
    let mut group = c.benchmark_group("lix_file");
    group.sample_size(10);
    group.throughput(Throughput::Elements(INSERT_ROW_COUNT as u64));
    let templates: Vec<(usize, BenchTemplate)> = HISTORY_DEPTHS
        .iter()
        .map(|&history_depth| (history_depth, build_template(&runtime, history_depth)))
        .collect();

    for (history_depth, template) in &templates {
        group.bench_with_input(
            BenchmarkId::new("insert_new_rows_deep_history", *history_depth),
            history_depth,
            |b, _| {
                b.iter_batched_ref(
                    || build_fixture_from_template(&runtime, template),
                    |fixture| fixture.insert_once(&runtime),
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

struct BenchFixture {
    engine: Engine,
    _tempdir: TempDir,
}

struct BenchTemplate {
    db_path: PathBuf,
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
    inner: Box<dyn LixTransaction + 'a>,
    collector: Arc<SqlTraceCollector>,
}

impl BenchFixture {
    fn insert_once(&mut self, runtime: &Runtime) {
        let mut params = Vec::with_capacity(INSERT_ROW_COUNT * 3);
        for (index, path) in INSERT_PATHS.iter().enumerate() {
            params.push(Value::Text(format!("bench-insert-{index}")));
            params.push(Value::Text((*path).to_string()));
            params.push(Value::Blob(insert_payload_for_row(index)));
        }
        runtime
            .block_on(self.engine.execute(
                "INSERT INTO lix_file (id, path, data) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)",
                &params,
            ))
            .expect("timed tracked insert should succeed");
    }
}

fn build_template(runtime: &Runtime, history_depth: usize) -> BenchTemplate {
    build_template_with_trace(runtime, history_depth, None)
}

fn build_template_with_trace(
    runtime: &Runtime,
    history_depth: usize,
    trace_collector: Option<Arc<SqlTraceCollector>>,
) -> BenchTemplate {
    let tempdir = TempDir::new().expect("tempdir should be created");
    let db_path = tempdir.path().join("template.sqlite");
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

    for revision in 0..history_depth {
        let bucket = revision % EXISTING_BUCKET_COUNT;
        runtime
            .block_on(engine.execute(
                "INSERT INTO lix_file (id, path, data) VALUES (?, ?, ?)",
                &[
                    Value::Text(format!("bench-existing-{revision:04}")),
                    Value::Text(format!(
                        "/bench/existing/{bucket:02}/existing-{revision:04}.json"
                    )),
                    Value::Blob(history_payload_for_revision(revision)),
                ],
            ))
            .expect("history seeding insert should succeed");
    }

    BenchTemplate {
        db_path,
        _tempdir: tempdir,
    }
}

fn build_fixture_from_template(runtime: &Runtime, template: &BenchTemplate) -> BenchFixture {
    build_fixture_from_template_with_trace(runtime, template, None)
}

fn build_fixture_from_template_with_trace(
    runtime: &Runtime,
    template: &BenchTemplate,
    trace_collector: Option<Arc<SqlTraceCollector>>,
) -> BenchFixture {
    let tempdir = TempDir::new().expect("tempdir should be created");
    let db_path = tempdir.path().join("fixture.sqlite");
    fs::copy(&template.db_path, &db_path).expect("template db copy should succeed");
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
        .block_on(engine.open_existing())
        .expect("existing template db should open");

    BenchFixture {
        engine,
        _tempdir: tempdir,
    }
}

fn history_payload_for_revision(revision: usize) -> Vec<u8> {
    repeated_payload(format!("history:{revision:08}|"))
}

fn insert_payload_for_row(index: usize) -> Vec<u8> {
    repeated_payload(format!("insert-row:{index:02}|"))
}

fn repeated_payload(prefix: String) -> Vec<u8> {
    let mut payload = Vec::with_capacity(PAYLOAD_BYTES);
    while payload.len() < PAYLOAD_BYTES {
        payload.extend_from_slice(prefix.as_bytes());
    }
    payload.truncate(PAYLOAD_BYTES);
    payload
}

fn maybe_print_trace_reports(runtime: &Runtime) {
    if !trace_report_enabled() {
        return;
    }

    let collector = Arc::new(SqlTraceCollector::default());
    for &history_depth in HISTORY_DEPTHS {
        let template =
            build_template_with_trace(runtime, history_depth, Some(Arc::clone(&collector)));
        let mut fixture = build_fixture_from_template_with_trace(
            runtime,
            &template,
            Some(Arc::clone(&collector)),
        );
        collector.clear();
        fixture.insert_once(runtime);
        print_trace_report(&format!("depth_{history_depth}"), &collector.snapshot());
    }
}

fn print_trace_report(label: &str, operations: &[SqlTraceOperation]) {
    let total_ms = operations.iter().map(|op| op.duration_ms).sum::<f64>();
    let tx_exec_count = operations
        .iter()
        .filter(|op| op.kind == "transaction_execute")
        .count();
    let tx_batch_count = operations
        .iter()
        .filter(|op| op.kind == "transaction_execute_batch")
        .count();

    eprintln!(
        "[bench-trace] lix_file/insert_new_rows_deep_history/{} total_ops={} tx_exec={} tx_batch={} total_ms={:.3}",
        label,
        operations.len(),
        tx_exec_count,
        tx_batch_count,
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
    for op in operations {
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

fn classify_phase(op: &SqlTraceOperation) -> &'static str {
    let Some(sql) = op.sql.as_deref() else {
        return op.kind;
    };

    let normalized = normalize_sql(sql);
    if normalized == "BEGIN" {
        return "begin";
    }
    if normalized == "COMMIT" {
        return "commit";
    }
    if normalized.starts_with("SELECT row_kind, value, metadata_value") {
        return "append_preflight";
    }
    if normalized.contains("INSERT INTO lix_internal_binary_blob_manifest")
        || normalized.contains("INSERT INTO lix_internal_binary_blob_store")
        || normalized.contains("INSERT INTO lix_internal_binary_chunk_store")
        || normalized.contains("INSERT INTO lix_internal_binary_blob_manifest_chunk")
        || normalized.contains("INSERT INTO lix_internal_snapshot")
    {
        return "write_batch";
    }
    if normalized.contains("lix_directory_descriptor") {
        return "directory_lookup";
    }
    if normalized.contains("lix_file_descriptor") {
        return "file_lookup";
    }
    if normalized.contains("WITH RECURSIVE reachable_commits")
        || normalized.contains("WITH RECURSIVE reachable(commit_id")
    {
        return "reachable_commits";
    }
    op.kind
}

fn trace_report_enabled() -> bool {
    std::env::var("LIX_BENCH_TRACE_INSERT_HISTORY")
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
    const MAX_SQL_CHARS: usize = 2000;
    if sql.len() <= MAX_SQL_CHARS {
        return sql.to_string();
    }
    format!("{}...", &sql[..MAX_SQL_CHARS])
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

    async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
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
}

#[async_trait(?Send)]
impl LixTransaction for TracingBenchTransaction<'_> {
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

    async fn execute_batch(&mut self, batch: &PreparedBatch) -> Result<QueryResult, LixError> {
        let started = std::time::Instant::now();
        let result = self.inner.execute_batch(batch).await;
        let collapsed =
            lix_engine::collapse_prepared_batch_for_dialect(batch, self.inner.dialect())?;
        self.collector.push(
            "transaction_execute_batch",
            Some(&collapsed.sql),
            started.elapsed().as_secs_f64() * 1000.0,
        );
        result
    }

    async fn commit(self: Box<Self>) -> Result<(), LixError> {
        let this = self;
        let started = std::time::Instant::now();
        let result = this.inner.commit().await;
        this.collector.push(
            "transaction_commit",
            Some("COMMIT"),
            started.elapsed().as_secs_f64() * 1000.0,
        );
        result
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        self.inner.rollback().await
    }
}

criterion_group!(benches, bench_lix_file_insert_history);
criterion_main!(benches);
