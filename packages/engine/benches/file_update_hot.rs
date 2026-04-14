use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use lix_engine::{Lix, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::Runtime;

#[path = "support/mod.rs"]
mod support;

use support::blob_fixture::{boot_new_file_backed_lix, repeated_payload, temp_db};
use support::trace::{
    normalize_sql, summarize_sql, trace_flag_enabled, SqlTraceCollector, SqlTraceOperation,
    TraceSummary,
};
use support::verify::{assert_row_count, scalar_blob, scalar_text};

const FILE_ID: &str = "bench-update-file";
const FILE_PATH: &str = "/bench/update/file.bin";
const DATA_UPDATE_BYTES: usize = 100 * 1024;
const TRACE_LIMIT: usize = 12;
const TRACE_ENV: &str = "LIX_BENCH_TRACE_UPDATE";

fn bench_file_update_hot(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create tokio runtime");
    maybe_print_trace_reports(&runtime);
    let mut group = c.benchmark_group("file_update_hot");
    group.sample_size(10);
    group.throughput(Throughput::Elements(1));

    group.bench_function("data_update_100kb", |b| {
        b.iter_batched_ref(
            || build_fixture(&runtime),
            |fixture| fixture.update_data_once(&runtime),
            BatchSize::SmallInput,
        );
    });

    group.bench_function("metadata_only", |b| {
        b.iter_batched_ref(
            || build_fixture(&runtime),
            |fixture| fixture.update_metadata_once(&runtime),
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

struct BenchFixture {
    lix: Arc<Lix>,
    next_revision: usize,
    _tempdir: TempDir,
}

impl BenchFixture {
    fn update_data_once(&mut self, runtime: &Runtime) {
        let payload = data_payload_for_revision(self.next_revision);
        runtime
            .block_on(self.lix.execute(
                "UPDATE lix_file SET data = ? WHERE id = ?",
                &[
                    Value::Blob(payload.clone()),
                    Value::Text(FILE_ID.to_string()),
                ],
            ))
            .expect("data update should succeed");
        assert_row_count(
            runtime,
            &self.lix,
            "SELECT COUNT(*) FROM lix_file WHERE id = ?",
            &[Value::Text(FILE_ID.to_string())],
            1,
        );
        let stored_data = scalar_blob(
            runtime,
            &self.lix,
            "SELECT data FROM lix_file WHERE id = ? LIMIT 1",
            &[Value::Text(FILE_ID.to_string())],
        );
        assert_eq!(
            stored_data, payload,
            "file_update_hot data payload mismatch"
        );
        self.next_revision += 1;
    }

    fn update_metadata_once(&mut self, runtime: &Runtime) {
        let metadata = format!(
            "{{\"owner\":\"bench\",\"revision\":{},\"kind\":\"metadata-only\"}}",
            self.next_revision
        );
        runtime
            .block_on(self.lix.execute(
                "UPDATE lix_file SET metadata = ? WHERE id = ?",
                &[
                    Value::Text(metadata.clone()),
                    Value::Text(FILE_ID.to_string()),
                ],
            ))
            .expect("metadata update should succeed");
        assert_row_count(
            runtime,
            &self.lix,
            "SELECT COUNT(*) FROM lix_file WHERE id = ?",
            &[Value::Text(FILE_ID.to_string())],
            1,
        );
        let stored_metadata = scalar_text(
            runtime,
            &self.lix,
            "SELECT metadata FROM lix_file WHERE id = ? LIMIT 1",
            &[Value::Text(FILE_ID.to_string())],
        );
        assert_eq!(
            stored_metadata, metadata,
            "file_update_hot metadata payload mismatch"
        );
        let stored_data = scalar_blob(
            runtime,
            &self.lix,
            "SELECT data FROM lix_file WHERE id = ? LIMIT 1",
            &[Value::Text(FILE_ID.to_string())],
        );
        assert_eq!(
            stored_data,
            data_payload_for_revision(0),
            "file_update_hot metadata-only case should preserve file bytes"
        );
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
    let (tempdir, db_path) = temp_db("fixture.sqlite");
    let lix = boot_new_file_backed_lix(runtime, &db_path, trace_collector, true);

    runtime
        .block_on(lix.execute(
            "INSERT INTO lix_file (id, path, data, metadata) VALUES (?, ?, ?, ?)",
            &[
                Value::Text(FILE_ID.to_string()),
                Value::Text(FILE_PATH.to_string()),
                Value::Blob(data_payload_for_revision(0)),
                Value::Text("{\"owner\":\"bench\",\"revision\":0}".to_string()),
            ],
        ))
        .expect("seed file insert should succeed");
    assert_row_count(
        runtime,
        &lix,
        "SELECT COUNT(*) FROM lix_file WHERE id = ?",
        &[Value::Text(FILE_ID.to_string())],
        1,
    );

    BenchFixture {
        lix,
        next_revision: 1,
        _tempdir: tempdir,
    }
}

fn maybe_print_trace_reports(runtime: &Runtime) {
    if !trace_flag_enabled(TRACE_ENV) {
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
        "[bench-trace] file_update_hot/{} total_ops={} backend_exec={} tx_exec={} total_ms={:.3}",
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
            summarize_sql(&sql, 220),
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
                summarize_sql(op.sql.as_deref().unwrap_or("<no-sql>"), 220),
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

fn data_payload_for_revision(revision: usize) -> Vec<u8> {
    repeated_payload(&format!("data-update:{revision:08}|"), DATA_UPDATE_BYTES)
}

criterion_group!(benches, bench_file_update_hot);
criterion_main!(benches);
