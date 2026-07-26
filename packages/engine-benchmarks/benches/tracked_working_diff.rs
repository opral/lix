//! Two-phase populated tracked-working-diff benchmark for RocksDB.
//!
//! The fixture deliberately remains *between* checkpoints. The old checkpoint
//! scale harness only measured `lix_working_change` after a checkpoint, when
//! there is no interval to diff. This harness records the real common shapes:
//! repeated edits to a small working set and disjoint edits across a large
//! working set.
//!
//! ```text
//! cargo bench -p lix_engine_benchmarks --features storage-benches --bench tracked_working_diff -- \
//!   setup repeated /tmp/lix-working-diff-repeated 10000 1000 10
//! cargo bench -p lix_engine_benchmarks --features storage-benches --bench tracked_working_diff -- \
//!   measure /tmp/lix-working-diff-repeated 11
//! ```
//!
//! `setup` refuses to overwrite an existing directory. `measure` is read-only
//! and can therefore be used for profiles against the same warmed fixture.

use std::fmt::Write as _;
use std::path::Path;
use std::time::{Duration, Instant};

use lix_engine::storage_adapter::StorageAdapter;
use lix_engine::storage_bench::diff_tracked_commits_for_bench;
use lix_engine::{Engine, ExecuteBatchStatement, Storage, Value};
use lix_rocksdb_storage::RocksDB;

const DEFAULT_ROW_COUNT: usize = 10_000;
const DEFAULT_COMMIT_COUNT: usize = 1_000;
const DEFAULT_CHANGES_PER_COMMIT: usize = 10;
const DEFAULT_MEASURE_REPETITIONS: usize = 11;
const INSERT_BATCH_SIZE: usize = 500;
const WORKING_DIFF_SQL: &str = "SELECT entity_pk, schema_key, change_kind, before_change_id, after_change_id \
    FROM lix_working_change ORDER BY schema_key, entity_pk";

#[derive(Clone, Copy)]
enum Shape {
    Repeated,
    Disjoint,
}

impl Shape {
    fn parse(value: &str) -> Self {
        match value {
            "repeated" => Self::Repeated,
            "disjoint" => Self::Disjoint,
            _ => panic!("shape must be 'repeated' or 'disjoint', got '{value}'"),
        }
    }

    fn expected_changes(
        self,
        row_count: usize,
        commit_count: usize,
        changes_per_commit: usize,
    ) -> usize {
        match self {
            Self::Repeated => changes_per_commit,
            Self::Disjoint => (commit_count * changes_per_commit).min(row_count),
        }
    }
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let Some(mode) = args.get(1).map(String::as_str) else {
        print_usage();
        return;
    };
    let Some(path) = args.get(2).map(String::as_str) else {
        print_usage();
        return;
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create tracked-working-diff benchmark runtime");

    match mode {
        "setup" => {
            let Some(shape) = args.get(3).map(|value| Shape::parse(value)) else {
                print_usage();
                return;
            };
            let row_count = parse_usize(args.get(4), DEFAULT_ROW_COUNT, "row count");
            let commit_count = parse_usize(args.get(5), DEFAULT_COMMIT_COUNT, "commit count");
            let changes_per_commit = parse_usize(
                args.get(6),
                DEFAULT_CHANGES_PER_COMMIT,
                "changes per commit",
            );
            runtime.block_on(setup(
                Path::new(path),
                shape,
                row_count,
                commit_count,
                changes_per_commit,
            ));
        }
        "measure" => {
            let repetitions = parse_usize(
                args.get(3),
                DEFAULT_MEASURE_REPETITIONS,
                "measurement repetitions",
            );
            runtime.block_on(measure(Path::new(path), repetitions));
        }
        "measure-history" => {
            let Some(base_commit_id) = args.get(3) else {
                print_usage();
                return;
            };
            let Some(head_commit_id) = args.get(4) else {
                print_usage();
                return;
            };
            let repetitions = parse_usize(
                args.get(5),
                DEFAULT_MEASURE_REPETITIONS,
                "measurement repetitions",
            );
            runtime.block_on(measure_history(
                Path::new(path),
                base_commit_id,
                head_commit_id,
                repetitions,
            ));
        }
        "checkpoint" => runtime.block_on(checkpoint(Path::new(path))),
        _ => print_usage(),
    }
}

fn print_usage() {
    eprintln!(
        "usage:\n  tracked_working_diff setup <directory> <repeated|disjoint> \
         [rows] [commits] [changes-per-commit]\n  \
         tracked_working_diff measure <directory> [repetitions]\n  \
         tracked_working_diff measure-history <directory> <base-commit-id> <head-commit-id> [repetitions]\n  \
         tracked_working_diff checkpoint <directory>"
    );
}

fn parse_usize(value: Option<&String>, default: usize, label: &str) -> usize {
    let value = value.map_or(default, |value| {
        value
            .parse::<usize>()
            .unwrap_or_else(|_| panic!("{label} must be a positive integer"))
    });
    assert!(value > 0, "{label} must be a positive integer");
    value
}

async fn setup(
    path: &Path,
    shape: Shape,
    row_count: usize,
    commit_count: usize,
    changes_per_commit: usize,
) {
    assert!(
        !path.exists(),
        "refusing to overwrite existing fixture {}",
        path.display()
    );
    assert!(
        changes_per_commit <= row_count,
        "changes per commit must not exceed row count"
    );

    let storage = RocksDB::open(path).expect("open tracked-working-diff RocksDB");
    Engine::initialize(storage.clone())
        .await
        .expect("initialize tracked-working-diff storage");
    let engine = Engine::new(storage.clone())
        .await
        .expect("open tracked-working-diff engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("open tracked-working-diff session");
    register_schema(&session).await;

    let seed_start = Instant::now();
    seed_rows(&session, row_count).await;
    let seed_elapsed = seed_start.elapsed();
    let initial_checkpoint_start = Instant::now();
    let initial_checkpoint = session
        .create_checkpoint()
        .await
        .expect("create tracked-working-diff initial checkpoint");
    let initial_checkpoint_elapsed = initial_checkpoint_start.elapsed();

    let writes_start = Instant::now();
    for commit_index in 0..commit_count {
        update_commit(&session, shape, row_count, commit_index, changes_per_commit).await;
    }
    let writes_elapsed = writes_start.elapsed();
    let expected_changes = shape.expected_changes(row_count, commit_count, changes_per_commit);
    let working_changes = working_change_count(&session).await;
    assert_eq!(
        working_changes, expected_changes,
        "populated working-diff fixture must expose every expected identity"
    );
    let branch_id = session
        .active_branch_id()
        .await
        .expect("load tracked-working-diff branch id");
    let head_commit_id = engine
        .load_branch_head_commit_id(&branch_id)
        .await
        .expect("load tracked-working-diff branch head")
        .expect("tracked-working-diff fixture must have a branch head");
    drop(session);
    drop(engine);
    storage.flush().expect("flush tracked-working-diff RocksDB");
    println!(
        "tracked_working_diff setup backend=rocksdb shape={} rows={row_count} commits={commit_count} \
         changes_per_commit={changes_per_commit} working_changes={working_changes} \
         base_commit_id={} head_commit_id={} seed_ms={:.3} initial_checkpoint_ms={:.3} writes_ms={:.3}",
        shape_name(shape),
        initial_checkpoint.commit_id,
        head_commit_id,
        millis(seed_elapsed),
        millis(initial_checkpoint_elapsed),
        millis(writes_elapsed),
    );
}

async fn measure(path: &Path, repetitions: usize) {
    assert!(path.exists(), "fixture {} does not exist", path.display());
    let storage = RocksDB::open(path).expect("open tracked-working-diff RocksDB");
    let engine = Engine::new(storage.clone())
        .await
        .expect("open tracked-working-diff engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("open tracked-working-diff session");

    let expected_changes = working_change_count(&session).await;
    assert!(
        expected_changes > 0,
        "fixture has no populated working changes"
    );
    // Warm SQL/provider construction and the RocksDB block cache outside the
    // reported samples. Repeated measurements remain read-only.
    assert_eq!(working_change_count(&session).await, expected_changes);
    let mut latencies = Vec::with_capacity(repetitions);
    for _ in 0..repetitions {
        let start = Instant::now();
        let count = profile_working_change_query(&session).await;
        latencies.push(start.elapsed());
        assert_eq!(count, expected_changes);
    }
    let mut sorted = latencies.clone();
    sorted.sort_unstable();
    println!(
        "tracked_working_diff measure backend=rocksdb working_changes={expected_changes} \
         repetitions={repetitions} p50_ms={:.3} mean_ms={:.3} min_ms={:.3} max_ms={:.3}",
        millis(sorted[sorted.len() / 2]),
        mean_millis(&latencies),
        millis(sorted[0]),
        millis(*sorted.last().expect("measurement samples are non-empty")),
    );
}

/// Measures the cold historical diff path rather than `lix_working_change`'s
/// serving-head accelerator. The setup checkpoint is a durable root and the
/// later ordinary commits are rootless, which makes this a populated
/// first-parent replay interval.
async fn measure_history(
    path: &Path,
    base_commit_id: &str,
    head_commit_id: &str,
    repetitions: usize,
) {
    assert!(path.exists(), "fixture {} does not exist", path.display());
    let storage = RocksDB::open(path).expect("open tracked-working-diff RocksDB");
    let adapter = StorageAdapter::new(storage.clone());
    let engine = Engine::new(storage)
        .await
        .expect("open tracked-working-diff engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("open tracked-working-diff session");
    let expected_changes = working_change_count(&session).await;
    assert!(
        expected_changes > 0,
        "fixture has no populated working changes"
    );

    let warm = diff_tracked_commits_for_bench(&adapter, base_commit_id, head_commit_id)
        .await
        .expect("warm historical tracked diff");
    assert_eq!(warm.entries, expected_changes);
    assert!(
        warm.left_has_durable_root && !warm.right_has_durable_root,
        "measure-history requires checkpoint -> rootless first-parent replay"
    );

    let mut latencies = Vec::with_capacity(repetitions);
    for _ in 0..repetitions {
        let start = Instant::now();
        let result = diff_tracked_commits_for_bench(&adapter, base_commit_id, head_commit_id)
            .await
            .expect("measure historical tracked diff");
        latencies.push(start.elapsed());
        assert_eq!(result.entries, expected_changes);
        assert!(
            result.left_has_durable_root && !result.right_has_durable_root,
            "historical measurement must remain checkpoint -> rootless"
        );
    }
    let mut sorted = latencies.clone();
    sorted.sort_unstable();
    println!(
        "tracked_working_diff measure-history backend=rocksdb working_changes={expected_changes} \
         repetitions={repetitions} p50_ms={:.3} mean_ms={:.3} min_ms={:.3} max_ms={:.3}",
        millis(sorted[sorted.len() / 2]),
        mean_millis(&latencies),
        millis(sorted[0]),
        millis(*sorted.last().expect("measurement samples are non-empty")),
    );
}

async fn checkpoint(path: &Path) {
    assert!(path.exists(), "fixture {} does not exist", path.display());
    let storage = RocksDB::open(path).expect("open tracked-working-diff RocksDB");
    let engine = Engine::new(storage.clone())
        .await
        .expect("open tracked-working-diff engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("open tracked-working-diff session");
    let before = working_change_count(&session).await;
    assert!(before > 0, "fixture has no populated working changes");
    let start = Instant::now();
    session
        .create_checkpoint()
        .await
        .expect("checkpoint populated working-diff fixture");
    let elapsed = start.elapsed();
    assert_eq!(working_change_count(&session).await, 0);
    println!(
        "tracked_working_diff checkpoint backend=rocksdb working_changes_before={before} \
         checkpoint_ms={:.3}",
        millis(elapsed),
    );
}

async fn register_schema<StorageImpl>(session: &lix_engine::SessionContext<StorageImpl>)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let schema = serde_json::json!({
        "x-lix-key": "working_diff_row",
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "required": ["id", "value"],
        "properties": {
            "id": { "type": "string" },
            "value": { "type": "string" }
        },
        "additionalProperties": false
    });
    let affected = session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (lix_json($1), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register tracked-working-diff schema")
        .rows_affected();
    assert_eq!(affected, 1);
}

async fn seed_rows<StorageImpl>(session: &lix_engine::SessionContext<StorageImpl>, row_count: usize)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let mut transaction = session
        .begin_transaction()
        .await
        .expect("begin tracked-working-diff seed transaction");
    for start in (0..row_count).step_by(INSERT_BATCH_SIZE) {
        let end = (start + INSERT_BATCH_SIZE).min(row_count);
        let mut sql = String::from("INSERT INTO working_diff_row (id, value) VALUES ");
        let mut params = Vec::with_capacity((end - start) * 2);
        for (offset, row_index) in (start..end).enumerate() {
            if offset > 0 {
                sql.push(',');
            }
            let parameter = offset * 2;
            write!(sql, "(${}, ${})", parameter + 1, parameter + 2)
                .expect("write tracked-working-diff insert parameters");
            params.push(Value::Text(row_id(row_index)));
            params.push(Value::Text("baseline".to_string()));
        }
        let affected = transaction
            .execute(&sql, &params)
            .await
            .expect("seed tracked-working-diff rows")
            .rows_affected();
        let affected = usize::try_from(affected).expect("affected row count fits usize");
        assert_eq!(affected, end - start);
    }
    transaction
        .commit()
        .await
        .expect("commit tracked-working-diff seed transaction");
}

async fn update_commit<StorageImpl>(
    session: &lix_engine::SessionContext<StorageImpl>,
    shape: Shape,
    row_count: usize,
    commit_index: usize,
    changes_per_commit: usize,
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let statements = (0..changes_per_commit)
        .map(|offset| {
            let row_index = match shape {
                Shape::Repeated => offset,
                Shape::Disjoint => (commit_index * changes_per_commit + offset) % row_count,
            };
            ExecuteBatchStatement {
                sql: "UPDATE working_diff_row SET value = $1 WHERE id = $2".to_string(),
                params: vec![
                    Value::Text(format!("commit-{commit_index:05}")),
                    Value::Text(row_id(row_index)),
                ],
            }
        })
        .collect::<Vec<_>>();
    let results = session
        .execute_batch(&statements)
        .await
        .expect("update tracked-working-diff commit");
    assert!(
        results.iter().all(|result| result.rows_affected() == 1),
        "every working-diff update must affect exactly one row"
    );
}

#[inline(never)]
async fn profile_working_change_query<StorageImpl>(
    session: &lix_engine::SessionContext<StorageImpl>,
) -> usize
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute(WORKING_DIFF_SQL, &[])
        .await
        .expect("query populated working diff")
        .len()
}

async fn working_change_count<StorageImpl>(
    session: &lix_engine::SessionContext<StorageImpl>,
) -> usize
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    profile_working_change_query(session).await
}

fn row_id(row_index: usize) -> String {
    format!("row-{row_index:05}")
}

const fn shape_name(shape: Shape) -> &'static str {
    match shape {
        Shape::Repeated => "repeated",
        Shape::Disjoint => "disjoint",
    }
}

fn mean_millis(durations: &[Duration]) -> f64 {
    let sample_count = u32::try_from(durations.len()).expect("benchmark sample count fits u32");
    durations
        .iter()
        .map(|duration| millis(*duration))
        .sum::<f64>()
        / f64::from(sample_count)
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}
