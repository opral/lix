use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use lix_engine::wasm::NoopWasmRuntime;
use lix_engine::{
    boot, BootArgs, BootKeyValue, LixBackend, LixBackendTransaction, LixError, QueryResult,
    Session, SqlDialect, TransactionMode, Value,
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

// This bench isolates the regression shape we observed in `lix exp git-replay` on paraglide-js:
// a small `lix_file` update becomes materially slower as history depth grows.
//
// The traced replay path showed that the slowdown is not the physical SQLite `UPDATE` itself.
// Instead, the write path performs several expensive `WITH RECURSIVE` reads to resolve effective
// file, directory, commit, and version state before applying the mutation. A single-file update
// after deep linear history is the smallest reproducible fixture for that behavior.
//
// This benchmark therefore seeds one tracked file, builds linear history through repeated updates,
// and then measures one more `UPDATE lix_file SET data = ? WHERE id = ?` at different depths.
const FILE_ID: &str = "bench-recursive-file";
const FILE_PATH: &str = "/bench/deep/nested/path/file.json";
const PAYLOAD_BYTES: usize = 1024;
const HISTORY_DEPTHS: &[usize] = &[1, 128];
const TRACE_HISTORY_DEPTH: usize = 128;
const TRACE_LIMIT: usize = 12;

fn bench_lix_file_recursive_update(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create tokio runtime");
    maybe_print_trace_report(&runtime);
    let mut group = c.benchmark_group("lix_file");
    group.sample_size(10);
    group.throughput(Throughput::Elements(1));

    for &history_depth in HISTORY_DEPTHS {
        group.bench_with_input(
            BenchmarkId::new("update_existing_row_deep_history", history_depth),
            &history_depth,
            |b, &history_depth| {
                b.iter_batched_ref(
                    || build_fixture(&runtime, history_depth),
                    |fixture| fixture.update_once(&runtime),
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

struct BenchFixture {
    session: Session,
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
    fn update_once(&mut self, runtime: &Runtime) {
        let payload = payload_for_revision(self.next_revision);
        runtime
            .block_on(self.session.execute(
                "UPDATE lix_file SET data = ? WHERE id = ?",
                &[Value::Blob(payload), Value::Text(FILE_ID.to_string())],
            ))
            .expect("timed lix_file update should succeed");
        self.next_revision += 1;
    }
}

fn build_fixture(runtime: &Runtime, history_depth: usize) -> BenchFixture {
    build_fixture_with_trace(runtime, history_depth, None)
}

fn build_fixture_with_trace(
    runtime: &Runtime,
    history_depth: usize,
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

    let engine = Arc::new(boot(boot_args));
    runtime
        .block_on(engine.initialize())
        .expect("engine initialization should succeed");
    let session = runtime
        .block_on(engine.open_session())
        .expect("workspace session should open");

    runtime
        .block_on(session.execute(
            "INSERT INTO lix_file (id, path, data) VALUES (?, ?, ?)",
            &[
                Value::Text(FILE_ID.to_string()),
                Value::Text(FILE_PATH.to_string()),
                Value::Blob(payload_for_revision(0)),
            ],
        ))
        .expect("seed file insert should succeed");

    for revision in 1..=history_depth {
        runtime
            .block_on(session.execute(
                "UPDATE lix_file SET data = ? WHERE id = ?",
                &[
                    Value::Blob(payload_for_revision(revision)),
                    Value::Text(FILE_ID.to_string()),
                ],
            ))
            .expect("history seeding update should succeed");
    }

    BenchFixture {
        session,
        next_revision: history_depth + 1,
        _tempdir: tempdir,
    }
}

fn payload_for_revision(revision: usize) -> Vec<u8> {
    let prefix = format!("revision:{revision:08}|");
    let mut payload = Vec::with_capacity(PAYLOAD_BYTES);
    while payload.len() < PAYLOAD_BYTES {
        payload.extend_from_slice(prefix.as_bytes());
    }
    payload.truncate(PAYLOAD_BYTES);
    payload
}

fn maybe_print_trace_report(runtime: &Runtime) {
    if !trace_report_enabled() {
        return;
    }

    let collector = Arc::new(SqlTraceCollector::default());
    let mut fixture =
        build_fixture_with_trace(runtime, TRACE_HISTORY_DEPTH, Some(Arc::clone(&collector)));
    collector.clear();
    fixture.update_once(runtime);
    let operations = collector.snapshot();

    let recursive_operations = operations
        .iter()
        .filter(|op| {
            op.sql
                .as_deref()
                .is_some_and(|sql| sql.contains("WITH RECURSIVE"))
        })
        .cloned()
        .collect::<Vec<_>>();
    let total_ms = operations.iter().map(|op| op.duration_ms).sum::<f64>();
    let recursive_ms = recursive_operations
        .iter()
        .map(|op| op.duration_ms)
        .sum::<f64>();

    eprintln!(
        "[bench-trace] lix_file/update_existing_row_deep_history depth={} total_ops={} total_ms={:.3} recursive_ops={} recursive_ms={:.3}",
        TRACE_HISTORY_DEPTH,
        operations.len(),
        total_ms,
        recursive_operations.len(),
        recursive_ms,
    );

    let mut grouped = BTreeMap::<String, TraceSummary>::new();
    for op in recursive_operations {
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
            "[bench-trace] top_recursive_sql rank={} kind={} count={} total_ms={:.3} avg_ms={:.3} max_ms={:.3} sql={}",
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

fn trace_report_enabled() -> bool {
    std::env::var("LIX_BENCH_TRACE_RECURSIVE")
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

    async fn begin_transaction(
        &self,
        mode: TransactionMode,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        let started = std::time::Instant::now();
        let tx = self.inner.begin_transaction(mode).await?;
        self.collector.push(
            "begin_transaction",
            Some(match mode {
                TransactionMode::Read => "read",
                TransactionMode::Write => "write",
                TransactionMode::Deferred => "deferred",
            }),
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

    fn mode(&self) -> TransactionMode {
        self.inner.mode()
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

criterion_group!(benches, bench_lix_file_recursive_update);
criterion_main!(benches);
