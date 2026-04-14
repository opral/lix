use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use lix_engine::{Lix, Value};
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::Runtime;

#[path = "support/mod.rs"]
mod support;

use support::blob_fixture::{
    boot_new_file_backed_lix, open_existing_file_backed_lix, repeated_payload, temp_db,
};
use support::trace::{
    normalize_sql, summarize_sql, trace_flag_enabled, SqlTraceCollector, SqlTraceOperation,
    TraceSummary,
};
use support::verify::{assert_row_count, scalar_blob, scalar_text};

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
const TRACE_ENV: &str = "LIX_BENCH_TRACE_INSERT_HISTORY";

fn bench_file_insert_history(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create tokio runtime");
    maybe_print_trace_reports(&runtime);
    let mut group = c.benchmark_group("file_insert_history");
    group.sample_size(10);
    group.throughput(Throughput::Elements(INSERT_ROW_COUNT as u64));
    let templates: Vec<(usize, BenchTemplate)> = HISTORY_DEPTHS
        .iter()
        .map(|&history_depth| (history_depth, build_template(&runtime, history_depth)))
        .collect();

    for (history_depth, template) in &templates {
        group.bench_with_input(
            BenchmarkId::from_parameter(*history_depth),
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
    lix: Arc<Lix>,
    expected_total_rows: i64,
    _tempdir: TempDir,
}

struct BenchTemplate {
    db_path: PathBuf,
    history_depth: usize,
    _tempdir: TempDir,
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
            .block_on(self.lix.execute(
                "INSERT INTO lix_file (id, path, data) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)",
                &params,
            ))
            .expect("timed tracked insert should succeed");
        assert_row_count(
            runtime,
            &self.lix,
            "SELECT COUNT(*) FROM lix_file",
            &[],
            self.expected_total_rows,
        );
        for (index, path) in INSERT_PATHS.iter().enumerate() {
            assert_row_count(
                runtime,
                &self.lix,
                "SELECT COUNT(*) FROM lix_file WHERE id = ?",
                &[Value::Text(format!("bench-insert-{index}"))],
                1,
            );
            let stored_path = scalar_text(
                runtime,
                &self.lix,
                "SELECT path FROM lix_file WHERE id = ? LIMIT 1",
                &[Value::Text(format!("bench-insert-{index}"))],
            );
            assert_eq!(
                stored_path, *path,
                "file_insert_history inserted path mismatch"
            );
            let stored_data = scalar_blob(
                runtime,
                &self.lix,
                "SELECT data FROM lix_file WHERE id = ? LIMIT 1",
                &[Value::Text(format!("bench-insert-{index}"))],
            );
            assert_eq!(
                stored_data,
                insert_payload_for_row(index),
                "file_insert_history inserted payload mismatch"
            );
        }
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
    let (tempdir, db_path) = temp_db("template.sqlite");
    let lix = boot_new_file_backed_lix(runtime, &db_path, trace_collector, false);

    for revision in 0..history_depth {
        let bucket = revision % EXISTING_BUCKET_COUNT;
        runtime
            .block_on(lix.execute(
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
        history_depth,
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
    let (tempdir, db_path) = temp_db("fixture.sqlite");
    fs::copy(&template.db_path, &db_path).expect("template db copy should succeed");
    let lix = open_existing_file_backed_lix(runtime, &db_path, trace_collector);

    BenchFixture {
        lix,
        expected_total_rows: (template.history_depth + INSERT_ROW_COUNT) as i64,
        _tempdir: tempdir,
    }
}

fn history_payload_for_revision(revision: usize) -> Vec<u8> {
    repeated_payload(&format!("history:{revision:08}|"), PAYLOAD_BYTES)
}

fn insert_payload_for_row(index: usize) -> Vec<u8> {
    repeated_payload(&format!("insert-row:{index:02}|"), PAYLOAD_BYTES)
}

fn maybe_print_trace_reports(runtime: &Runtime) {
    if !trace_flag_enabled(TRACE_ENV) {
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
        "[bench-trace] file_insert_history/{} total_ops={} tx_exec={} tx_batch={} total_ms={:.3}",
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
            summarize_sql(&sql, 2000),
        );
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

criterion_group!(benches, bench_file_insert_history);
criterion_main!(benches);
