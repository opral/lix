use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use lix_engine::{ExecuteOptions, Lix, Value};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::runtime::Runtime;

#[path = "support/mod.rs"]
mod support;

use support::blob_fixture::{boot_new_file_backed_lix, temp_db};
use support::state_fixture::{
    build_state_insert_sql_batches, register_bench_state_schema, BENCH_STATE_FILE_ID,
    BENCH_STATE_SCHEMA_KEY,
};
use support::verify::scalar_count;

const ROW_COUNTS: &[usize] = &[1_000, 10_000];
const CHUNK_SIZES: &[usize] = &[10_000];

fn bench_state_insert_bulk(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create tokio runtime");
    let mut group = c.benchmark_group("state_insert_bulk");
    group.sample_size(10);

    for &row_count in ROW_COUNTS {
        group.throughput(Throughput::Elements(row_count as u64));
        for &chunk_size in CHUNK_SIZES {
            group.bench_with_input(
                BenchmarkId::new(format!("rows_{row_count}"), format!("chunk_{chunk_size}")),
                &(row_count, chunk_size),
                |b, &(row_count, chunk_size)| {
                    b.iter_custom(|iters| {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let fixture = build_fixture(&runtime, row_count, chunk_size);
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

struct BenchFixture {
    lix: Arc<Lix>,
    active_version_id: String,
    sql_batches: Vec<String>,
    expected_rows: i64,
    _tempdir: TempDir,
}

impl BenchFixture {
    fn execute_writes_only(self, runtime: &Runtime) -> Duration {
        let mut transaction = runtime
            .block_on(
                self.lix
                    .begin_transaction_with_options(ExecuteOptions::default()),
            )
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
            .expect("state insert transaction should commit");

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
                Value::Text(self.active_version_id),
                Value::Text(BENCH_STATE_SCHEMA_KEY.to_string()),
            ],
        );
        assert_eq!(
            committed_rows, self.expected_rows,
            "state_insert_bulk committed row count mismatch"
        );

        elapsed
    }
}

fn build_fixture(runtime: &Runtime, row_count: usize, chunk_size: usize) -> BenchFixture {
    let (tempdir, db_path) = temp_db("state-insert-bulk.sqlite");
    let lix = boot_new_file_backed_lix(runtime, &db_path, None, true);
    register_bench_state_schema(runtime, &lix);
    let active_version_id = runtime
        .block_on(lix.active_version_id())
        .expect("active version id should load");
    let sql_batches =
        build_state_insert_sql_batches(row_count, chunk_size).expect("state insert batches");

    BenchFixture {
        lix,
        active_version_id,
        sql_batches,
        expected_rows: row_count as i64,
        _tempdir: tempdir,
    }
}

criterion_group!(benches, bench_state_insert_bulk);
criterion_main!(benches);
