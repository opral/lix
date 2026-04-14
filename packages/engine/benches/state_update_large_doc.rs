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
    build_state_insert_sql_batches_with_prefix, build_state_update_sql_batches,
    register_bench_state_schema, BENCH_STATE_FILE_ID, BENCH_STATE_SCHEMA_KEY,
};
use support::verify::scalar_count;

const DOC_SIZES: &[usize] = &[10_000];
const CHANGED_COUNTS: &[usize] = &[1, 100];
const INSERT_CHUNK_SIZE: usize = 250;
const INITIAL_VALUE_PREFIX: &str = "initial";
const UPDATED_VALUE_PREFIX: &str = "updated";

fn bench_state_update_large_doc(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create tokio runtime");
    let mut group = c.benchmark_group("state_update_large_doc");
    group.sample_size(10);

    for &doc_size in DOC_SIZES {
        for &changed_count in CHANGED_COUNTS {
            group.throughput(Throughput::Elements(changed_count as u64));
            group.bench_with_input(
                BenchmarkId::new(
                    format!("doc_{doc_size}"),
                    format!("changed_{changed_count}"),
                ),
                &(doc_size, changed_count),
                |b, &(doc_size, changed_count)| {
                    b.iter_custom(|iters| {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let fixture = build_fixture(&runtime, doc_size, changed_count);
                            total += fixture.execute_updates_only(&runtime);
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
    update_sql_batches: Vec<String>,
    expected_rows: i64,
    changed_count: usize,
    doc_size: usize,
    _tempdir: TempDir,
}

impl BenchFixture {
    fn execute_updates_only(self, runtime: &Runtime) -> Duration {
        let mut transaction = runtime
            .block_on(
                self.lix
                    .begin_transaction_with_options(ExecuteOptions::default()),
            )
            .expect("bench transaction should start");

        let started = Instant::now();
        for sql in &self.update_sql_batches {
            runtime
                .block_on(transaction.execute(sql, &[]))
                .expect("state update batch should succeed");
        }
        let elapsed = started.elapsed();

        runtime
            .block_on(transaction.commit())
            .expect("state update transaction should commit");

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
            "state_update_large_doc total row count mismatch"
        );

        let changed_rows = scalar_count(
            runtime,
            &self.lix,
            "SELECT COUNT(*) \
             FROM lix_state_by_version \
             WHERE file_id = ?1 \
               AND version_id = ?2 \
               AND schema_key = ?3 \
               AND entity_id IN (SELECT entity_id FROM lix_state_by_version \
                                 WHERE file_id = ?1 \
                                   AND version_id = ?2 \
                                   AND schema_key = ?3 \
                                 ORDER BY entity_id ASC \
                                 LIMIT ?4) \
               AND snapshot_content LIKE ?5",
            &[
                Value::Text(BENCH_STATE_FILE_ID.to_string()),
                Value::Text(self.active_version_id.clone()),
                Value::Text(BENCH_STATE_SCHEMA_KEY.to_string()),
                Value::Integer(self.changed_count as i64),
                Value::Text(format!("{{\"value\":\"{UPDATED_VALUE_PREFIX}-%")),
            ],
        );
        assert_eq!(
            changed_rows, self.changed_count as i64,
            "state_update_large_doc changed row count mismatch"
        );

        let untouched_entity_id = format!("entity-{:05}", self.doc_size - 1);
        let untouched_rows = scalar_count(
            runtime,
            &self.lix,
            "SELECT COUNT(*) \
             FROM lix_state_by_version \
             WHERE file_id = ?1 \
               AND version_id = ?2 \
               AND schema_key = ?3 \
               AND entity_id = ?4 \
               AND snapshot_content = ?5",
            &[
                Value::Text(BENCH_STATE_FILE_ID.to_string()),
                Value::Text(self.active_version_id),
                Value::Text(BENCH_STATE_SCHEMA_KEY.to_string()),
                Value::Text(untouched_entity_id.clone()),
                Value::Text(format!(
                    "{{\"value\":\"{INITIAL_VALUE_PREFIX}-{:05}\"}}",
                    self.doc_size - 1
                )),
            ],
        );
        assert_eq!(
            untouched_rows, 1,
            "state_update_large_doc untouched sentinel row should remain stable"
        );

        elapsed
    }
}

fn build_fixture(runtime: &Runtime, doc_size: usize, changed_count: usize) -> BenchFixture {
    assert!(
        changed_count <= doc_size,
        "changed_count must not exceed doc_size"
    );

    let (tempdir, db_path) = temp_db("state-update-large-doc.sqlite");
    let lix = boot_new_file_backed_lix(runtime, &db_path, None, true);
    register_bench_state_schema(runtime, &lix);
    let active_version_id = runtime
        .block_on(lix.active_version_id())
        .expect("active version id should load");

    let initial_insert_sql = build_state_insert_sql_batches_with_prefix(
        doc_size,
        INSERT_CHUNK_SIZE,
        INITIAL_VALUE_PREFIX,
    )
    .expect("initial state insert batches");
    {
        let mut transaction = runtime
            .block_on(lix.begin_transaction_with_options(ExecuteOptions::default()))
            .expect("seed transaction should start");
        for sql in &initial_insert_sql {
            runtime
                .block_on(transaction.execute(sql, &[]))
                .expect("seed state insert should succeed");
        }
        runtime
            .block_on(transaction.commit())
            .expect("seed state insert should commit");
    }

    let update_sql_batches = build_state_update_sql_batches(changed_count, UPDATED_VALUE_PREFIX)
        .expect("state update batches");

    BenchFixture {
        lix,
        active_version_id,
        update_sql_batches,
        expected_rows: doc_size as i64,
        changed_count,
        doc_size,
        _tempdir: tempdir,
    }
}

criterion_group!(benches, bench_state_update_large_doc);
criterion_main!(benches);
