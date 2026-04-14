use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use lix_engine::{AdditionalSessionOptions, CreateVersionOptions, Lix, Value};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::runtime::Runtime;

#[path = "support/mod.rs"]
mod support;

use support::blob_fixture::{boot_new_file_backed_lix, temp_db};
use support::state_fixture::{
    build_state_insert_sql_batches_with_prefix, build_state_update_sql_batches,
    register_bench_state_schema, BENCH_STATE_FILE_ID, BENCH_STATE_SCHEMA_KEY,
};

const STATE_ROW_COUNTS: &[usize] = &[0, 10_000, 100_000];
const HISTORY_DEPTHS: &[usize] = &[0, 128];
const HISTORY_SWEEP_STATE_ROWS: usize = 10_000;
const INSERT_CHUNK_SIZE: usize = 250;
const INITIAL_VALUE_PREFIX: &str = "version-create-initial";
const HISTORY_VALUE_PREFIX: &str = "version-create-history";
const CREATED_VERSION_ID: &str = "bench-version-created";

fn bench_version_create(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create tokio runtime");

    let mut size_group = c.benchmark_group("version_create");
    size_group.sample_size(10);
    size_group.throughput(Throughput::Elements(1));
    for &state_rows in STATE_ROW_COUNTS {
        size_group.bench_with_input(
            BenchmarkId::new("state_rows", state_rows),
            &state_rows,
            |b, &state_rows| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let fixture = build_fixture(&runtime, state_rows, 0);
                        total += fixture.create_version_only(&runtime);
                    }
                    total
                });
            },
        );
    }
    size_group.finish();

    let mut history_group = c.benchmark_group("version_create_history");
    history_group.sample_size(10);
    history_group.throughput(Throughput::Elements(1));
    for &history_depth in HISTORY_DEPTHS {
        history_group.bench_with_input(
            BenchmarkId::new("history_depth", history_depth),
            &history_depth,
            |b, &history_depth| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let fixture =
                            build_fixture(&runtime, HISTORY_SWEEP_STATE_ROWS, history_depth);
                        total += fixture.create_version_only(&runtime);
                    }
                    total
                });
            },
        );
    }
    history_group.finish();
}

struct BenchFixture {
    lix: Arc<Lix>,
    expected_visible_rows: i64,
    source_version_id: String,
    source_commit_id: String,
    target_version_id: String,
    _tempdir: TempDir,
}

impl BenchFixture {
    fn create_version_only(self, runtime: &Runtime) -> Duration {
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
            "version_create parent version mismatch"
        );
        assert_eq!(
            result.parent_commit_id, self.source_commit_id,
            "version_create parent commit mismatch"
        );

        let created_commit_id = scalar_text(
            runtime,
            &self.lix,
            "SELECT commit_id FROM lix_version WHERE id = ?1 LIMIT 1",
            &[Value::Text(self.target_version_id.clone())],
        );
        assert_eq!(
            created_commit_id, self.source_commit_id,
            "version_create created head commit mismatch"
        );

        let scoped = runtime
            .block_on(self.lix.open_additional_session(AdditionalSessionOptions {
                active_version_id: Some(self.target_version_id),
                active_account_ids: None,
            }))
            .expect("additional session for created version should open");

        let visible_rows = runtime
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
            .expect("created version visible row count query should succeed");
        let visible_count = first_integer(&visible_rows);
        assert_eq!(
            visible_count, self.expected_visible_rows,
            "version_create visible state count mismatch"
        );

        elapsed
    }
}

fn build_fixture(runtime: &Runtime, state_rows: usize, history_depth: usize) -> BenchFixture {
    let (tempdir, db_path) = temp_db("version-create.sqlite");
    let lix = boot_new_file_backed_lix(runtime, &db_path, None, true);
    register_bench_state_schema(runtime, &lix);

    if state_rows > 0 {
        let insert_batches = build_state_insert_sql_batches_with_prefix(
            state_rows,
            INSERT_CHUNK_SIZE,
            INITIAL_VALUE_PREFIX,
        )
        .expect("version create seed insert batches");
        for sql in &insert_batches {
            runtime
                .block_on(lix.execute(sql, &[]))
                .expect("version create seed insert should succeed");
        }
    }

    if history_depth > 0 {
        let update_batches = build_state_update_sql_batches(1, HISTORY_VALUE_PREFIX)
            .expect("version create history update batches");
        for revision in 0..history_depth {
            let sql = update_batches[0].replace(
                &format!("{HISTORY_VALUE_PREFIX}-00000"),
                &format!("{HISTORY_VALUE_PREFIX}-{revision:05}"),
            );
            runtime
                .block_on(lix.execute(&sql, &[]))
                .expect("version create history update should succeed");
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

    BenchFixture {
        lix,
        expected_visible_rows: state_rows as i64,
        source_version_id,
        source_commit_id,
        target_version_id: CREATED_VERSION_ID.to_string(),
        _tempdir: tempdir,
    }
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

criterion_group!(benches, bench_version_create);
criterion_main!(benches);
