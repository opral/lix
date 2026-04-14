use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use lix_engine::{Lix, Value};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::runtime::Runtime;

#[path = "support/mod.rs"]
mod support;

use support::blob_fixture::{boot_new_file_backed_lix, open_existing_file_backed_lix, temp_db};
use support::state_fixture::{
    build_state_insert_sql_batches_with_range, register_bench_state_schema, BENCH_STATE_FILE_ID,
    BENCH_STATE_SCHEMA_KEY,
};
use support::verify::scalar_count;

const HISTORY_DEPTHS: &[usize] = &[0, 512];
const INSERT_ROW_COUNTS: &[usize] = &[1_000];
const INSERT_CHUNK_SIZE: usize = 250;
const HISTORY_VALUE_PREFIX: &str = "history";
const INSERT_VALUE_PREFIX: &str = "insert";

fn bench_state_insert_history(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create tokio runtime");
    let mut group = c.benchmark_group("state_insert_history");
    group.sample_size(10);

    let templates: Vec<(usize, BenchTemplate)> = HISTORY_DEPTHS
        .iter()
        .map(|&history_depth| (history_depth, build_template(&runtime, history_depth)))
        .collect();

    for (history_depth, template) in &templates {
        for &insert_row_count in INSERT_ROW_COUNTS {
            group.throughput(Throughput::Elements(insert_row_count as u64));
            group.bench_with_input(
                BenchmarkId::new(
                    format!("depth_{history_depth}"),
                    format!("rows_{insert_row_count}"),
                ),
                &insert_row_count,
                |b, &insert_row_count| {
                    b.iter_custom(|iters| {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let mut fixture =
                                build_fixture_from_template(&runtime, template, insert_row_count);
                            total += fixture.execute_writes_only(&runtime);
                        }
                        total
                    });
                },
            );
        }
    }

    group.finish();
}

struct BenchTemplate {
    db_path: PathBuf,
    history_depth: usize,
    _tempdir: TempDir,
}

struct BenchFixture {
    lix: Arc<Lix>,
    active_version_id: String,
    sql_batches: Vec<String>,
    expected_rows: i64,
    _tempdir: TempDir,
}

impl BenchFixture {
    fn execute_writes_only(&mut self, runtime: &Runtime) -> Duration {
        let mut transaction = runtime
            .block_on(self.lix.begin_transaction_with_options(Default::default()))
            .expect("bench transaction should start");

        let started = Instant::now();
        for sql in &self.sql_batches {
            runtime
                .block_on(transaction.execute(sql, &[]))
                .expect("state insert batch should succeed");
        }
        let elapsed = started.elapsed();

        runtime
            .block_on(transaction.commit())
            .expect("state insert history transaction should commit");

        let committed_rows = scalar_count(
            runtime,
            &self.lix,
            "SELECT COUNT(*) \
             FROM lix_state_by_version \
             WHERE file_id = ?1 \
               AND version_id = ?2 \
               AND schema_key = ?3 \
               AND snapshot_content IS NOT NULL",
            &[
                Value::Text(BENCH_STATE_FILE_ID.to_string()),
                Value::Text(self.active_version_id.clone()),
                Value::Text(BENCH_STATE_SCHEMA_KEY.to_string()),
            ],
        );
        assert_eq!(
            committed_rows, self.expected_rows,
            "state_insert_history committed row count mismatch"
        );

        elapsed
    }
}

fn build_template(runtime: &Runtime, history_depth: usize) -> BenchTemplate {
    let (tempdir, db_path) = temp_db("state-insert-history-template.sqlite");
    let lix = boot_new_file_backed_lix(runtime, &db_path, None, true);
    register_bench_state_schema(runtime, &lix);

    for revision in 0..history_depth {
        let sql = build_state_insert_sql_batches_with_range(revision, 1, 1, HISTORY_VALUE_PREFIX)
            .expect("history seed sql");

        runtime
            .block_on(lix.execute(&sql[0], &[]))
            .expect("history seed insert should succeed");
    }

    BenchTemplate {
        db_path,
        history_depth,
        _tempdir: tempdir,
    }
}

fn build_fixture_from_template(
    runtime: &Runtime,
    template: &BenchTemplate,
    insert_row_count: usize,
) -> BenchFixture {
    let (tempdir, db_path) = temp_db("state-insert-history-fixture.sqlite");
    fs::copy(&template.db_path, &db_path).expect("template db copy should succeed");
    let lix = open_existing_file_backed_lix(runtime, &db_path, None);
    let active_version_id = runtime
        .block_on(lix.active_version_id())
        .expect("active version id should load");
    let sql_batches = build_state_insert_sql_batches_with_range(
        template.history_depth,
        insert_row_count,
        INSERT_CHUNK_SIZE,
        INSERT_VALUE_PREFIX,
    )
    .expect("state insert history batches");

    BenchFixture {
        lix,
        active_version_id,
        sql_batches,
        expected_rows: (template.history_depth + insert_row_count) as i64,
        _tempdir: tempdir,
    }
}

criterion_group!(benches, bench_state_insert_history);
criterion_main!(benches);
