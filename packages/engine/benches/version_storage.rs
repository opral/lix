use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use lix_engine::{AdditionalSessionOptions, CreateVersionOptions, Lix, Value};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::runtime::Runtime;

#[path = "support/mod.rs"]
mod support;

use support::blob_fixture::{boot_new_file_backed_lix, temp_db};
use support::state_fixture::{
    build_state_insert_sql_batches_with_prefix, register_bench_state_schema, BENCH_STATE_FILE_ID,
    BENCH_STATE_SCHEMA_KEY,
};

const STATE_ROW_COUNTS: &[usize] = &[0, 10_000, 100_000];
const INSERT_CHUNK_SIZE: usize = 250;
const INITIAL_VALUE_PREFIX: &str = "version-storage-initial";
const CREATED_VERSION_ID: &str = "bench-version-storage-created";
const STORAGE_REPORT_ENV: &str = "LIX_BENCH_TRACE_VERSION_STORAGE";

fn bench_version_storage(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create tokio runtime");
    maybe_print_storage_reports(&runtime);

    let mut group = c.benchmark_group("version_storage");
    group.sample_size(10);
    group.throughput(Throughput::Elements(1));

    for &state_rows in STATE_ROW_COUNTS {
        group.bench_with_input(
            BenchmarkId::from_parameter(state_rows),
            &state_rows,
            |b, &state_rows| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let fixture = build_fixture(&runtime, state_rows);
                        total += fixture.create_version_and_measure(&runtime).elapsed;
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

struct BenchFixture {
    lix: Arc<Lix>,
    db_path: PathBuf,
    expected_visible_rows: i64,
    source_version_id: String,
    source_commit_id: String,
    target_version_id: String,
    before_storage_bytes: u64,
    before_version_rows: i64,
    _tempdir: TempDir,
}

struct StorageOutcome {
    elapsed: Duration,
    storage_delta_bytes: i64,
    version_row_delta: i64,
}

impl BenchFixture {
    fn create_version_and_measure(self, runtime: &Runtime) -> StorageOutcome {
        let started = Instant::now();
        let result = runtime
            .block_on(self.lix.create_version(CreateVersionOptions {
                id: Some(self.target_version_id.clone()),
                name: Some(self.target_version_id.clone()),
                source_version_id: None,
                hidden: false,
            }))
            .expect("create_version should succeed");
        let elapsed = started.elapsed();

        assert_eq!(
            result.parent_version_id, self.source_version_id,
            "version_storage parent version mismatch"
        );
        assert_eq!(
            result.parent_commit_id, self.source_commit_id,
            "version_storage parent commit mismatch"
        );

        let after_storage_bytes = sqlite_storage_bytes(&self.db_path);
        let after_version_rows =
            scalar_count(runtime, &self.lix, "SELECT COUNT(*) FROM lix_version", &[]);
        let created_commit_id = scalar_text(
            runtime,
            &self.lix,
            "SELECT commit_id FROM lix_version WHERE id = ?1 LIMIT 1",
            &[Value::Text(self.target_version_id.clone())],
        );
        assert_eq!(
            created_commit_id, self.source_commit_id,
            "version_storage created head commit mismatch"
        );

        let source_visible_rows = scalar_count(
            runtime,
            &self.lix,
            "SELECT COUNT(*) \
             FROM lix_state \
             WHERE file_id = ?1 \
               AND schema_key = ?2",
            &[
                Value::Text(BENCH_STATE_FILE_ID.to_string()),
                Value::Text(BENCH_STATE_SCHEMA_KEY.to_string()),
            ],
        );
        assert_eq!(
            source_visible_rows, self.expected_visible_rows,
            "version_storage source visible state count mismatch"
        );

        let scoped = runtime
            .block_on(self.lix.open_additional_session(AdditionalSessionOptions {
                active_version_id: Some(self.target_version_id),
                active_account_ids: None,
            }))
            .expect("additional session for created version should open");
        let target_visible_rows = first_integer(
            &runtime
                .block_on(scoped.execute(
                    "SELECT COUNT(*) \
                     FROM lix_state \
                     WHERE file_id = ?1 \
                       AND schema_key = ?2",
                    &[
                        Value::Text(BENCH_STATE_FILE_ID.to_string()),
                        Value::Text(BENCH_STATE_SCHEMA_KEY.to_string()),
                    ],
                ))
                .expect("created version visible row count query should succeed"),
        );
        assert_eq!(
            target_visible_rows, self.expected_visible_rows,
            "version_storage target visible state count mismatch"
        );

        StorageOutcome {
            elapsed,
            storage_delta_bytes: after_storage_bytes as i64 - self.before_storage_bytes as i64,
            version_row_delta: after_version_rows - self.before_version_rows,
        }
    }
}

fn build_fixture(runtime: &Runtime, state_rows: usize) -> BenchFixture {
    let (tempdir, db_path) = temp_db("version-storage.sqlite");
    let lix = boot_new_file_backed_lix(runtime, &db_path, None, true);
    register_bench_state_schema(runtime, &lix);

    if state_rows > 0 {
        let insert_batches = build_state_insert_sql_batches_with_prefix(
            state_rows,
            INSERT_CHUNK_SIZE,
            INITIAL_VALUE_PREFIX,
        )
        .expect("version storage seed insert batches");
        for sql in &insert_batches {
            runtime
                .block_on(lix.execute(sql, &[]))
                .expect("version storage seed insert should succeed");
        }
    }

    let source_version_id = runtime
        .block_on(lix.active_version_id())
        .expect("source active version id should load");
    let source_commit_id = scalar_text(
        runtime,
        &lix,
        "SELECT commit_id FROM lix_version WHERE id = ?1 LIMIT 1",
        &[Value::Text(source_version_id.clone())],
    );
    let before_storage_bytes = sqlite_storage_bytes(&db_path);
    let before_version_rows = scalar_count(runtime, &lix, "SELECT COUNT(*) FROM lix_version", &[]);

    BenchFixture {
        lix,
        db_path,
        expected_visible_rows: state_rows as i64,
        source_version_id,
        source_commit_id,
        target_version_id: CREATED_VERSION_ID.to_string(),
        before_storage_bytes,
        before_version_rows,
        _tempdir: tempdir,
    }
}

fn maybe_print_storage_reports(runtime: &Runtime) {
    if !storage_report_enabled() {
        return;
    }

    for &state_rows in STATE_ROW_COUNTS {
        let fixture = build_fixture(runtime, state_rows);
        let outcome = fixture.create_version_and_measure(runtime);
        eprintln!(
            "[bench-storage] version_storage state_rows={} storage_delta_bytes={} version_row_delta={} elapsed_ms={:.3}",
            state_rows,
            outcome.storage_delta_bytes,
            outcome.version_row_delta,
            outcome.elapsed.as_secs_f64() * 1000.0,
        );
    }
}

fn storage_report_enabled() -> bool {
    std::env::var(STORAGE_REPORT_ENV)
        .map(|raw| {
            let normalized = raw.trim().to_ascii_lowercase();
            !normalized.is_empty() && normalized != "0" && normalized != "false"
        })
        .unwrap_or(false)
}

fn sqlite_storage_bytes(db_path: &Path) -> u64 {
    let mut total = 0u64;
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let path = if suffix.is_empty() {
            db_path.to_path_buf()
        } else {
            PathBuf::from(format!("{}{}", db_path.display(), suffix))
        };
        if let Ok(metadata) = std::fs::metadata(path) {
            total += metadata.len();
        }
    }
    total
}

fn scalar_count(runtime: &Runtime, lix: &Arc<Lix>, sql: &str, params: &[Value]) -> i64 {
    let result = runtime
        .block_on(lix.execute(sql, params))
        .expect("integer verification query should succeed");
    first_integer(&result)
}

fn scalar_text(runtime: &Runtime, lix: &Arc<Lix>, sql: &str, params: &[Value]) -> String {
    let result = runtime
        .block_on(lix.execute(sql, params))
        .expect("text verification query should succeed");
    match result
        .statements
        .first()
        .and_then(|statement| statement.rows.first())
        .and_then(|row| row.first())
    {
        Some(Value::Text(value)) => value.clone(),
        Some(Value::Integer(value)) => value.to_string(),
        other => panic!("expected text-like verification row, got {other:?}"),
    }
}

fn first_integer(result: &lix_engine::ExecuteResult) -> i64 {
    match result
        .statements
        .first()
        .and_then(|statement| statement.rows.first())
        .and_then(|row| row.first())
    {
        Some(Value::Integer(value)) => *value,
        other => panic!("expected integer result, got {other:?}"),
    }
}

criterion_group!(benches, bench_version_storage);
criterion_main!(benches);
