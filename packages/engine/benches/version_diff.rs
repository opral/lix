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

const DOC_SIZE: usize = 10_000;
const CHANGED_COUNTS: &[usize] = &[0, 1];
const HISTORY_DEPTHS: &[usize] = &[0, 128];
const HISTORY_SWEEP_CHANGED_ROWS: usize = 1;
const INSERT_CHUNK_SIZE: usize = 250;
const INITIAL_VALUE_PREFIX: &str = "version-diff-initial";
const UPDATED_VALUE_PREFIX: &str = "version-diff-updated";
const HISTORY_VALUE_PREFIX: &str = "version-diff-history";
const TARGET_VERSION_ID: &str = "bench-version-diff-target";

fn bench_version_diff(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create tokio runtime");

    let mut diff_group = c.benchmark_group("version_diff");
    diff_group.sample_size(10);
    diff_group.throughput(Throughput::Elements(1));
    for &changed_count in CHANGED_COUNTS {
        diff_group.bench_with_input(
            BenchmarkId::new("changed_rows", changed_count),
            &changed_count,
            |b, &changed_count| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let fixture = build_fixture(&runtime, changed_count, 0);
                        total += fixture.execute_diff_only(&runtime);
                    }
                    total
                });
            },
        );
    }
    diff_group.finish();

    let mut history_group = c.benchmark_group("version_diff_history");
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
                            build_fixture(&runtime, HISTORY_SWEEP_CHANGED_ROWS, history_depth);
                        total += fixture.execute_diff_only(&runtime);
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
    source_version_id: String,
    target_version_id: String,
    expected_diff_rows: i64,
    _tempdir: TempDir,
}

impl BenchFixture {
    fn execute_diff_only(self, runtime: &Runtime) -> Duration {
        let diff_sql = build_diff_count_sql(&self.source_version_id, &self.target_version_id);

        let started = Instant::now();
        let result = runtime
            .block_on(self.lix.execute(&diff_sql, &[]))
            .expect("version diff query should succeed");
        let elapsed = started.elapsed();

        let diff_count = first_integer(&result);
        assert_eq!(
            diff_count, self.expected_diff_rows,
            "version_diff changed row count mismatch"
        );

        elapsed
    }
}

fn build_fixture(runtime: &Runtime, changed_count: usize, history_depth: usize) -> BenchFixture {
    assert!(
        changed_count <= DOC_SIZE,
        "changed_count must not exceed doc size"
    );

    let (tempdir, db_path) = temp_db("version-diff.sqlite");
    let lix = boot_new_file_backed_lix(runtime, &db_path, None, true);
    register_bench_state_schema(runtime, &lix);

    let insert_batches = build_state_insert_sql_batches_with_prefix(
        DOC_SIZE,
        INSERT_CHUNK_SIZE,
        INITIAL_VALUE_PREFIX,
    )
    .expect("version diff seed insert batches");
    for sql in &insert_batches {
        runtime
            .block_on(lix.execute(sql, &[]))
            .expect("version diff seed insert should succeed");
    }

    let source_version_id = runtime
        .block_on(lix.active_version_id())
        .expect("source active version id should load");
    runtime
        .block_on(lix.create_version(CreateVersionOptions {
            id: Some(TARGET_VERSION_ID.to_string()),
            name: Some(TARGET_VERSION_ID.to_string()),
            source_version_id: None,
            hidden: false,
        }))
        .expect("target version should be created");

    if changed_count > 0 || history_depth > 0 {
        let target_session = runtime
            .block_on(lix.open_additional_session(AdditionalSessionOptions {
                active_version_id: Some(TARGET_VERSION_ID.to_string()),
                active_account_ids: None,
            }))
            .expect("target version session should open");

        if changed_count > 0 {
            let update_batches =
                build_state_update_sql_batches(changed_count, UPDATED_VALUE_PREFIX)
                    .expect("version diff update batches");
            for sql in &update_batches {
                runtime
                    .block_on(target_session.execute(sql, &[]))
                    .expect("target version changed-row update should succeed");
            }
        }

        if history_depth > 0 {
            let history_updates = build_state_update_sql_batches(1, HISTORY_VALUE_PREFIX)
                .expect("version diff history update batches");
            for revision in 0..history_depth {
                let sql = history_updates[0].replace(
                    &format!("{HISTORY_VALUE_PREFIX}-00000"),
                    &format!("{HISTORY_VALUE_PREFIX}-{revision:05}"),
                );
                runtime
                    .block_on(target_session.execute(&sql, &[]))
                    .expect("target version history update should succeed");
            }
        }
    }

    BenchFixture {
        lix,
        source_version_id,
        target_version_id: TARGET_VERSION_ID.to_string(),
        expected_diff_rows: changed_count as i64,
        _tempdir: tempdir,
    }
}

fn build_diff_count_sql(source_version_id: &str, target_version_id: &str) -> String {
    format!(
        "WITH \
           source_rows AS (\
             SELECT entity_id, file_id, schema_key, snapshot_content \
             FROM lix_state_by_version \
             WHERE version_id = '{source_version_id}' \
               AND file_id = '{file_id}' \
               AND schema_key = '{schema_key}' \
               AND snapshot_content IS NOT NULL\
           ), \
           target_rows AS (\
             SELECT entity_id, file_id, schema_key, snapshot_content \
             FROM lix_state_by_version \
             WHERE version_id = '{target_version_id}' \
               AND file_id = '{file_id}' \
               AND schema_key = '{schema_key}' \
               AND snapshot_content IS NOT NULL\
           ), \
           diff_rows AS (\
             SELECT source_rows.entity_id AS entity_id \
             FROM source_rows \
             LEFT JOIN target_rows \
               ON target_rows.entity_id = source_rows.entity_id \
              AND target_rows.file_id = source_rows.file_id \
              AND target_rows.schema_key = source_rows.schema_key \
             WHERE target_rows.entity_id IS NULL \
                OR target_rows.snapshot_content <> source_rows.snapshot_content \
             UNION ALL \
             SELECT target_rows.entity_id AS entity_id \
             FROM target_rows \
             LEFT JOIN source_rows \
               ON source_rows.entity_id = target_rows.entity_id \
              AND source_rows.file_id = target_rows.file_id \
              AND source_rows.schema_key = target_rows.schema_key \
             WHERE source_rows.entity_id IS NULL\
           ) \
         SELECT COUNT(*) FROM diff_rows",
        source_version_id = escape_sql_string(source_version_id),
        target_version_id = escape_sql_string(target_version_id),
        file_id = BENCH_STATE_FILE_ID,
        schema_key = BENCH_STATE_SCHEMA_KEY,
    )
}

fn escape_sql_string(input: &str) -> String {
    input.replace('\'', "''")
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

criterion_group!(benches, bench_version_diff);
criterion_main!(benches);
