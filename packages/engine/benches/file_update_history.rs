use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use lix_engine::{Lix, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::Runtime;

#[path = "support/mod.rs"]
mod support;

use support::blob_fixture::{boot_new_file_backed_lix, repeated_payload, temp_db};
use support::trace::{
    normalize_sql, summarize_sql, trace_flag_enabled, SqlTraceCollector, TraceSummary,
};
use support::verify::{assert_row_count, scalar_blob};

const FILE_ID: &str = "bench-recursive-file";
const FILE_PATH: &str = "/bench/deep/nested/path/file.json";
const PAYLOAD_BYTES: usize = 1024;
const HISTORY_DEPTHS: &[usize] = &[1, 128];
const TRACE_HISTORY_DEPTH: usize = 128;
const TRACE_LIMIT: usize = 12;
const TRACE_ENV: &str = "LIX_BENCH_TRACE_RECURSIVE";

fn bench_file_update_history(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create tokio runtime");
    maybe_print_trace_report(&runtime);
    let mut group = c.benchmark_group("file_update_history");
    group.sample_size(10);
    group.throughput(Throughput::Elements(1));

    for &history_depth in HISTORY_DEPTHS {
        group.bench_with_input(
            BenchmarkId::from_parameter(history_depth),
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
    lix: Arc<Lix>,
    next_revision: usize,
    _tempdir: TempDir,
}

impl BenchFixture {
    fn update_once(&mut self, runtime: &Runtime) {
        let payload = payload_for_revision(self.next_revision);
        runtime
            .block_on(self.lix.execute(
                "UPDATE lix_file SET data = ? WHERE id = ?",
                &[
                    Value::Blob(payload.clone()),
                    Value::Text(FILE_ID.to_string()),
                ],
            ))
            .expect("timed lix_file update should succeed");
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
            "file_update_history final file bytes mismatch"
        );
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
    let (tempdir, db_path) = temp_db("fixture.sqlite");
    let lix = boot_new_file_backed_lix(runtime, &db_path, trace_collector, true);

    runtime
        .block_on(lix.execute(
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
            .block_on(lix.execute(
                "UPDATE lix_file SET data = ? WHERE id = ?",
                &[
                    Value::Blob(payload_for_revision(revision)),
                    Value::Text(FILE_ID.to_string()),
                ],
            ))
            .expect("history seeding update should succeed");
    }

    BenchFixture {
        lix,
        next_revision: history_depth + 1,
        _tempdir: tempdir,
    }
}

fn payload_for_revision(revision: usize) -> Vec<u8> {
    repeated_payload(&format!("revision:{revision:08}|"), PAYLOAD_BYTES)
}

fn maybe_print_trace_report(runtime: &Runtime) {
    if !trace_flag_enabled(TRACE_ENV) {
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
        "[bench-trace] file_update_history depth={} total_ops={} total_ms={:.3} recursive_ops={} recursive_ms={:.3}",
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
            summarize_sql(&sql, 220),
        );
    }
}

criterion_group!(benches, bench_file_update_history);
criterion_main!(benches);
