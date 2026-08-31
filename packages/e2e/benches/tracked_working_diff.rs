#![allow(clippy::large_futures)]
#![recursion_limit = "256"]

//! Two-phase populated tracked-working-diff benchmark for storage adapters.
//!
//! The fixture deliberately remains *between* checkpoints. The old checkpoint
//! scale harness only measured one-argument `lix_diff` after a checkpoint, when
//! there is no interval to diff. This harness records the real common shapes:
//! repeated edits to a small working set and disjoint edits across a large
//! working set.
//!
//! ```text
//! cargo bench -p lix_e2e --features storage-benches --bench tracked_working_diff -- \
//!   setup rocksdb /tmp/lix-working-diff-repeated repeated 10000 1000 10
//! cargo bench -p lix_e2e --features storage-benches --bench tracked_working_diff -- \
//!   measure rocksdb /tmp/lix-working-diff-repeated 11
//! ```
//!
//! `setup` refuses to overwrite an existing directory. `measure` is read-only
//! and can therefore be used for profiles against the same warmed fixture.
//! Merge modes accept `LIX_WORKING_DIFF_UNRELATED_HISTORY`,
//! `LIX_WORKING_DIFF_UNRELATED_HISTORY_WIDTH`, and
//! `LIX_WORKING_DIFF_SETTLE_MS` to separate total-history scaling from
//! relevant divergence and backend lifecycle state.

use std::fmt::Write as _;
use std::path::Path;
use std::time::{Duration, Instant};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

use lix::storage::Storage;
use lix::storage_adapter::StorageAdapter;
use lix::storage_bench::diff_tracked_commits_for_bench;
use lix::tracked_state::bench::seed_packed_history;
use lix::{
    CreateBranchOptions, ExecuteBatchStatement, MergeBranchOptions, MergeBranchOutcome,
    MergeBranchPreviewOptions, Value,
};
use lix::{Lix, open_lix};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;

const DEFAULT_ROW_COUNT: usize = 10_000;
const DEFAULT_COMMIT_COUNT: usize = 1_000;
const DEFAULT_CHANGES_PER_COMMIT: usize = 10;
const DEFAULT_MEASURE_REPETITIONS: usize = 11;
const DEFAULT_UNRELATED_HISTORY_WIDTH: usize = 1;
const UNRELATED_HISTORY_STORAGE_BATCH: usize = 100_000;
const INSERT_BATCH_SIZE: usize = 500;
const MERGE_PREVIEW_SOURCE_BRANCH_ID: &str = "01920000-0000-7000-8000-000000000901";
const WORKING_DIFF_SQL: &str = "SELECT row_ref, key, diff_type, row_count \
    FROM lix_diff('working_diff_row', $1, lix_active_branch_commit_id()) \
    ORDER BY key";

#[derive(Clone, Copy)]
enum Shape {
    Repeated,
    Disjoint,
}

#[derive(Clone, Copy)]
enum Backend {
    Rocks,
    Slate,
}

impl Backend {
    fn parse(value: &str) -> Self {
        match value {
            "rocksdb" => Self::Rocks,
            "slatedb" => Self::Slate,
            _ => panic!("backend must be 'rocksdb' or 'slatedb', got '{value}'"),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Rocks => "rocksdb",
            Self::Slate => "slatedb",
        }
    }
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
    init_perf_tracing();
    let args = std::env::args().collect::<Vec<_>>();
    let Some(mode) = args.get(1).map(String::as_str) else {
        print_usage();
        return;
    };
    let Some(backend) = args.get(2).map(|value| Backend::parse(value)) else {
        print_usage();
        return;
    };
    let Some(path) = args.get(3).map(String::as_str) else {
        print_usage();
        return;
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create tracked-working-diff benchmark runtime");

    match mode {
        "setup" => {
            assert!(
                !Path::new(path).exists(),
                "refusing to overwrite existing fixture {path}"
            );
            let Some(shape) = args.get(4).map(|value| Shape::parse(value)) else {
                print_usage();
                return;
            };
            let row_count = parse_usize(args.get(5), DEFAULT_ROW_COUNT, "row count");
            let commit_count = parse_usize(args.get(6), DEFAULT_COMMIT_COUNT, "commit count");
            let changes_per_commit = parse_usize(
                args.get(7),
                DEFAULT_CHANGES_PER_COMMIT,
                "changes per commit",
            );
            runtime.block_on(async {
                match backend {
                    Backend::Rocks => {
                        let storage =
                            RocksDB::open(path).expect("open tracked-working-diff RocksDB");
                        setup(
                            storage.clone(),
                            backend,
                            shape,
                            row_count,
                            commit_count,
                            changes_per_commit,
                        )
                        .await;
                        storage.flush().expect("flush tracked-working-diff RocksDB");
                    }
                    Backend::Slate => {
                        let storage =
                            SlateDB::open(path).expect("open tracked-working-diff SlateDB");
                        setup(
                            storage.clone(),
                            backend,
                            shape,
                            row_count,
                            commit_count,
                            changes_per_commit,
                        )
                        .await;
                        storage
                            .flush()
                            .await
                            .expect("flush tracked-working-diff SlateDB");
                    }
                }
            });
        }
        "measure" => {
            let repetitions = parse_usize(
                args.get(4),
                DEFAULT_MEASURE_REPETITIONS,
                "measurement repetitions",
            );
            runtime.block_on(async {
                match backend {
                    Backend::Rocks => {
                        measure(
                            RocksDB::open(path).expect("open tracked-working-diff RocksDB"),
                            backend,
                            Path::new(path),
                            repetitions,
                        )
                        .await;
                    }
                    Backend::Slate => {
                        measure(
                            SlateDB::open(path).expect("open tracked-working-diff SlateDB"),
                            backend,
                            Path::new(path),
                            repetitions,
                        )
                        .await;
                    }
                }
            });
        }
        "measure-history" => {
            let Some(base_commit_id) = args.get(4) else {
                print_usage();
                return;
            };
            let Some(head_commit_id) = args.get(5) else {
                print_usage();
                return;
            };
            let repetitions = parse_usize(
                args.get(6),
                DEFAULT_MEASURE_REPETITIONS,
                "measurement repetitions",
            );
            runtime.block_on(async {
                match backend {
                    Backend::Rocks => {
                        measure_history(
                            RocksDB::open(path).expect("open tracked-working-diff RocksDB"),
                            backend,
                            Path::new(path),
                            base_commit_id,
                            head_commit_id,
                            repetitions,
                        )
                        .await;
                    }
                    Backend::Slate => {
                        measure_history(
                            SlateDB::open(path).expect("open tracked-working-diff SlateDB"),
                            backend,
                            Path::new(path),
                            base_commit_id,
                            head_commit_id,
                            repetitions,
                        )
                        .await;
                    }
                }
            });
        }
        "merge-preview" => {
            assert!(
                !Path::new(path).exists(),
                "refusing to overwrite existing fixture {path}"
            );
            let row_count = parse_usize(args.get(4), DEFAULT_ROW_COUNT, "row count");
            let commit_count = parse_usize(args.get(5), DEFAULT_COMMIT_COUNT, "commit count");
            let changes_per_commit = parse_usize(
                args.get(6),
                DEFAULT_CHANGES_PER_COMMIT,
                "changes per commit",
            );
            let repetitions = parse_usize(
                args.get(7),
                DEFAULT_MEASURE_REPETITIONS,
                "measurement repetitions",
            );
            runtime.block_on(async {
                match backend {
                    Backend::Rocks => {
                        measure_merge_preview(
                            RocksDB::open(path).expect("open merge-preview RocksDB"),
                            backend,
                            row_count,
                            commit_count,
                            changes_per_commit,
                            repetitions,
                        )
                        .await;
                    }
                    Backend::Slate => {
                        measure_merge_preview(
                            SlateDB::open(path).expect("open merge-preview SlateDB"),
                            backend,
                            row_count,
                            commit_count,
                            changes_per_commit,
                            repetitions,
                        )
                        .await;
                    }
                }
            });
        }
        "merge-commit" => {
            assert!(
                !Path::new(path).exists(),
                "refusing to overwrite existing fixture {path}"
            );
            let repetitions = parse_usize(
                args.get(4),
                DEFAULT_MEASURE_REPETITIONS,
                "measurement repetitions",
            );
            let changes_per_side =
                parse_usize(args.get(5), DEFAULT_CHANGES_PER_COMMIT, "changes per side");
            runtime.block_on(async {
                match backend {
                    Backend::Rocks => {
                        measure_merge_commit(
                            RocksDB::open(path).expect("open merge-commit RocksDB"),
                            backend,
                            repetitions,
                            changes_per_side,
                        )
                        .await;
                    }
                    Backend::Slate => {
                        measure_merge_commit(
                            SlateDB::open(path).expect("open merge-commit SlateDB"),
                            backend,
                            repetitions,
                            changes_per_side,
                        )
                        .await;
                    }
                }
            });
        }
        "checkpoint" => runtime.block_on(async {
            match backend {
                Backend::Rocks => {
                    checkpoint(
                        RocksDB::open(path).expect("open tracked-working-diff RocksDB"),
                        backend,
                        Path::new(path),
                    )
                    .await;
                }
                Backend::Slate => {
                    checkpoint(
                        SlateDB::open(path).expect("open tracked-working-diff SlateDB"),
                        backend,
                        Path::new(path),
                    )
                    .await;
                }
            }
        }),
        _ => print_usage(),
    }
}

fn init_perf_tracing() {
    if std::env::var_os("LIX_WORKING_DIFF_TRACE").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("lix_perf=debug")
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
            .with_target(false)
            .try_init();
    }
}

fn print_usage() {
    eprintln!(
        "usage:\n  tracked_working_diff setup <rocksdb|slatedb> <directory> <repeated|disjoint> \
         [rows] [commits] [changes-per-commit]\n  \
         tracked_working_diff measure <rocksdb|slatedb> <directory> [repetitions]\n  \
         tracked_working_diff measure-history <rocksdb|slatedb> <directory> <base-commit-id> <head-commit-id> [repetitions]\n  \
         tracked_working_diff merge-preview <rocksdb|slatedb> <directory> [rows] [commits-per-side] [changes-per-commit] [repetitions]\n  \
         tracked_working_diff merge-commit <rocksdb|slatedb> <directory> [repetitions] [changes-per-side]\n  \
         tracked_working_diff checkpoint <rocksdb|slatedb> <directory>"
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

async fn setup<StorageImpl>(
    storage: StorageImpl,
    backend: Backend,
    shape: Shape,
    row_count: usize,
    commit_count: usize,
    changes_per_commit: usize,
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    assert!(
        changes_per_commit <= row_count,
        "changes per commit must not exceed row count"
    );

    open_lix()
        .with_storage(storage.clone())
        .await
        .expect("initialize tracked-working-diff storage");
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open tracked-working-diff lix");
    let session = lix
        .open_another_session()
        .await
        .expect("open tracked-working-diff session");
    register_schema(&session).await;

    let seed_start = Instant::now();
    seed_rows(&session, row_count).await;
    let seed_elapsed = seed_start.elapsed();
    let initial_checkpoint_start = Instant::now();
    let initial_checkpoint = session
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .expect("create tracked-working-diff initial checkpoint")
        .rows()[0]
        .get::<String>("commit_id")
        .expect("checkpoint commit id decodes");
    let initial_checkpoint_elapsed = initial_checkpoint_start.elapsed();

    let writes_start = Instant::now();
    for commit_index in 0..commit_count {
        update_commit(&session, shape, row_count, commit_index, changes_per_commit).await;
    }
    let writes_elapsed = writes_start.elapsed();
    let expected_changes = shape.expected_changes(row_count, commit_count, changes_per_commit);
    let working_diffs = working_diff_count(&session).await;
    assert_eq!(
        working_diffs, expected_changes,
        "populated working-diff fixture must expose every expected identity"
    );
    let branch_id = session
        .active_branch_id()
        .await
        .expect("load tracked-working-diff branch id");
    let head_commit_id = lix
        .execute(
            "SELECT commit_id FROM lix_branch WHERE id = $1",
            &[Value::Text(branch_id)],
        )
        .await
        .expect("load tracked-working-diff branch head")
        .rows()
        .first()
        .expect("tracked-working-diff fixture must have a branch head")
        .get::<String>("commit_id")
        .expect("tracked-working-diff head commit id must be text");
    drop(session);
    drop(lix);
    println!(
        "tracked_working_diff setup backend={} shape={} rows={row_count} commits={commit_count} \
         changes_per_commit={changes_per_commit} working_diffs={working_diffs} \
         base_commit_id={} head_commit_id={} seed_ms={:.3} initial_checkpoint_ms={:.3} writes_ms={:.3}",
        backend.name(),
        shape_name(shape),
        initial_checkpoint,
        head_commit_id,
        millis(seed_elapsed),
        millis(initial_checkpoint_elapsed),
        millis(writes_elapsed),
    );
}

async fn measure<StorageImpl>(
    storage: StorageImpl,
    backend: Backend,
    path: &Path,
    repetitions: usize,
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    assert!(path.exists(), "fixture {} does not exist", path.display());
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open tracked-working-diff lix");
    let session = lix
        .open_another_session()
        .await
        .expect("open tracked-working-diff session");

    let expected_changes = working_diff_count(&session).await;
    assert!(
        expected_changes > 0,
        "fixture has no populated working diffs"
    );
    // Warm SQL/provider construction and the RocksDB block cache outside the
    // reported samples. Repeated measurements remain read-only.
    assert_eq!(working_diff_count(&session).await, expected_changes);
    let mut latencies = Vec::with_capacity(repetitions);
    for _ in 0..repetitions {
        let start = Instant::now();
        let count = profile_working_diff_query(&session).await;
        latencies.push(start.elapsed());
        assert_eq!(count, expected_changes);
    }
    let mut sorted = latencies.clone();
    sorted.sort_unstable();
    println!(
        "tracked_working_diff measure backend={} working_diffs={expected_changes} \
         repetitions={repetitions} p50_ms={:.3} mean_ms={:.3} min_ms={:.3} max_ms={:.3}",
        backend.name(),
        millis(sorted[sorted.len() / 2]),
        mean_millis(&latencies),
        millis(sorted[0]),
        millis(*sorted.last().expect("measurement samples are non-empty")),
    );
}

/// Measures the cold historical diff path rather than one-argument `lix_diff`'s
/// serving-head accelerator. The setup checkpoint is a durable root and the
/// later ordinary commits are rootless, which makes this a populated
/// first-parent replay interval.
async fn measure_history<StorageImpl>(
    storage: StorageImpl,
    backend: Backend,
    path: &Path,
    base_commit_id: &str,
    head_commit_id: &str,
    repetitions: usize,
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    assert!(path.exists(), "fixture {} does not exist", path.display());
    let adapter = StorageAdapter::new(storage.clone());
    let lix = open_lix()
        .with_storage(storage)
        .await
        .expect("open tracked-working-diff lix");
    let session = lix
        .open_another_session()
        .await
        .expect("open tracked-working-diff session");
    let expected_changes = working_diff_count(&session).await;
    assert!(
        expected_changes > 0,
        "fixture has no populated working diffs"
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
        "tracked_working_diff measure-history backend={} working_diffs={expected_changes} \
         repetitions={repetitions} p50_ms={:.3} mean_ms={:.3} min_ms={:.3} max_ms={:.3}",
        backend.name(),
        millis(sorted[sorted.len() / 2]),
        mean_millis(&latencies),
        millis(sorted[0]),
        millis(*sorted.last().expect("measurement samples are non-empty")),
    );
}

async fn checkpoint<StorageImpl>(storage: StorageImpl, backend: Backend, path: &Path)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    assert!(path.exists(), "fixture {} does not exist", path.display());
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open tracked-working-diff lix");
    let session = lix
        .open_another_session()
        .await
        .expect("open tracked-working-diff session");
    let before = working_diff_count(&session).await;
    assert!(before > 0, "fixture has no populated working diffs");
    let start = Instant::now();
    session
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .expect("checkpoint populated working-diff fixture");
    let elapsed = start.elapsed();
    assert_eq!(working_diff_count(&session).await, 0);
    println!(
        "tracked_working_diff checkpoint backend={} working_diffs_before={before} \
         checkpoint_ms={:.3}",
        backend.name(),
        millis(elapsed),
    );
}

async fn measure_merge_preview<StorageImpl>(
    storage: StorageImpl,
    backend: Backend,
    row_count: usize,
    commit_count: usize,
    changes_per_commit: usize,
    repetitions: usize,
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let changes_per_side = commit_count * changes_per_commit;
    assert!(
        row_count >= changes_per_side * 2,
        "merge preview needs disjoint target and source rows"
    );
    let adapter = StorageAdapter::new(storage.clone());
    open_lix()
        .with_storage(storage.clone())
        .await
        .expect("initialize merge-preview storage");
    let (unrelated_history, settle_ms) = seed_unrelated_history(&adapter).await;
    let lix = open_lix()
        .with_storage(storage)
        .await
        .expect("open merge-preview lix");
    let target = lix
        .open_another_session()
        .await
        .expect("open merge-preview target session");
    register_schema(&target).await;
    seed_rows(&target, row_count).await;
    target
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .expect("checkpoint merge-preview base");
    target
        .create_branch(CreateBranchOptions {
            id: Some(MERGE_PREVIEW_SOURCE_BRANCH_ID.to_string()),
            name: "Merge source".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("create merge-preview source branch");
    let source = lix
        .open_another_session()
        .await
        .expect("open merge-preview source session");
    source
        .switch_branch(lix::SwitchBranchOptions {
            branch_id: (MERGE_PREVIEW_SOURCE_BRANCH_ID).to_string(),
        })
        .await
        .expect("switch session branch");

    for commit_index in 0..commit_count {
        update_commit_range(&target, 0, commit_index, changes_per_commit, "target").await;
        update_commit_range(
            &source,
            changes_per_side,
            commit_index,
            changes_per_commit,
            "source",
        )
        .await;
    }

    let options = MergeBranchPreviewOptions {
        source_branch_id: MERGE_PREVIEW_SOURCE_BRANCH_ID.to_string(),
    };
    let warm = target
        .merge_branch_preview(options.clone())
        .await
        .expect("warm merge preview");
    assert_eq!(warm.outcome, MergeBranchOutcome::MergeCommitted);
    assert_eq!(warm.change_stats.total, changes_per_side);
    assert!(warm.conflicts.is_empty());

    let mut latencies = Vec::with_capacity(repetitions);
    for _ in 0..repetitions {
        let start = Instant::now();
        let preview = target
            .merge_branch_preview(options.clone())
            .await
            .expect("measure merge preview");
        latencies.push(start.elapsed());
        assert_eq!(preview.outcome, MergeBranchOutcome::MergeCommitted);
        assert_eq!(preview.change_stats.total, changes_per_side);
        assert!(preview.conflicts.is_empty());
    }
    let mut sorted = latencies.clone();
    sorted.sort_unstable();
    println!(
        "tracked_working_diff merge-preview backend={} rows={row_count} commits_per_side={commit_count} \
         changes_per_commit={changes_per_commit} source_changes={changes_per_side} repetitions={repetitions} \
         unrelated_history_changes={unrelated_history} settle_ms={settle_ms} \
         p50_ms={:.3} p95_ms={:.3} p99_ms={:.3} mean_ms={:.3} min_ms={:.3} max_ms={:.3}",
        backend.name(),
        millis(percentile(&sorted, 50)),
        millis(percentile(&sorted, 95)),
        millis(percentile(&sorted, 99)),
        mean_millis(&latencies),
        millis(sorted[0]),
        millis(*sorted.last().expect("measurement samples are non-empty")),
    );
}

async fn measure_merge_commit<StorageImpl>(
    storage: StorageImpl,
    backend: Backend,
    repetitions: usize,
    changes_per_side: usize,
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let row_count = repetitions * changes_per_side * 2;
    let adapter = StorageAdapter::new(storage.clone());
    open_lix()
        .with_storage(storage.clone())
        .await
        .expect("initialize merge-commit storage");
    let (unrelated_history, settle_ms) = seed_unrelated_history(&adapter).await;
    let lix = open_lix()
        .with_storage(storage)
        .await
        .expect("open merge-commit lix");
    let target = lix
        .open_another_session()
        .await
        .expect("open merge-commit target session");
    register_schema(&target).await;
    seed_rows(&target, row_count).await;
    target
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .expect("checkpoint merge-commit base");

    let mut latencies = Vec::with_capacity(repetitions);
    for sample in 0..repetitions {
        let source_branch_id = format!("01920000-0000-7000-8000-{sample:012x}");
        target
            .create_branch(CreateBranchOptions {
                id: Some(source_branch_id.clone()),
                name: format!("Merge source {sample}"),
                from_commit_id: None,
            })
            .await
            .expect("create merge-commit source branch");
        let source = lix
            .open_another_session()
            .await
            .expect("open merge-commit source session");
        source
            .switch_branch(lix::SwitchBranchOptions {
                branch_id: (&source_branch_id).to_string(),
            })
            .await
            .expect("switch session branch");
        update_commit_range(
            &target,
            sample * changes_per_side,
            0,
            changes_per_side,
            "target",
        )
        .await;
        update_commit_range(
            &source,
            repetitions * changes_per_side + sample * changes_per_side,
            0,
            changes_per_side,
            "source",
        )
        .await;

        let start = Instant::now();
        let receipt = target
            .merge_branch(MergeBranchOptions {
                source_branch_id: source_branch_id.clone(),
            })
            .await
            .expect("measure committed merge");
        latencies.push(start.elapsed());
        assert_eq!(receipt.outcome, MergeBranchOutcome::MergeCommitted);
        assert_eq!(receipt.change_stats.total, changes_per_side);
        assert!(receipt.created_merge_commit_id.is_some());
    }
    let mut sorted = latencies.clone();
    sorted.sort_unstable();
    println!(
        "tracked_working_diff merge-commit backend={} changes_per_side={changes_per_side} \
         unrelated_history_changes={unrelated_history} settle_ms={settle_ms} \
         repetitions={repetitions} p50_ms={:.3} p95_ms={:.3} p99_ms={:.3} \
         mean_ms={:.3} min_ms={:.3} max_ms={:.3}",
        backend.name(),
        millis(percentile(&sorted, 50)),
        millis(percentile(&sorted, 95)),
        millis(percentile(&sorted, 99)),
        mean_millis(&latencies),
        millis(sorted[0]),
        millis(*sorted.last().expect("measurement samples are non-empty")),
    );
}

async fn seed_unrelated_history<StorageImpl>(storage: &StorageAdapter<StorageImpl>) -> (usize, u64)
where
    StorageImpl: Storage,
{
    let changes = std::env::var("LIX_WORKING_DIFF_UNRELATED_HISTORY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    if changes == 0 {
        return (0, 0);
    }
    let width = std::env::var("LIX_WORKING_DIFF_UNRELATED_HISTORY_WIDTH")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_UNRELATED_HISTORY_WIDTH);
    assert!(
        changes.is_multiple_of(width),
        "unrelated history must divide evenly by its commit width"
    );
    seed_packed_history(
        storage,
        changes,
        width,
        UNRELATED_HISTORY_STORAGE_BATCH.max(width),
    )
    .await;
    let settle_ms = std::env::var("LIX_WORKING_DIFF_SETTLE_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    tokio::time::sleep(Duration::from_millis(settle_ms)).await;
    (changes, settle_ms)
}

async fn register_schema<StorageImpl>(session: &Lix<StorageImpl>)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let schema = serde_json::json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "working_diff_row",
        "columns": [
            { "name": "id", "type": "text", "nullable": false },
            { "name": "value", "type": "text", "nullable": false },
        ],
        "primary_key": ["id"],
    });
    let affected = session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (CAST($1 AS JSONB), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register tracked-working-diff schema")
        .rows_affected();
    assert_eq!(affected, 1);
}

async fn seed_rows<StorageImpl>(session: &Lix<StorageImpl>, row_count: usize)
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
    session: &Lix<StorageImpl>,
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
                label: None,
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

async fn update_commit_range<StorageImpl>(
    session: &Lix<StorageImpl>,
    row_offset: usize,
    commit_index: usize,
    changes_per_commit: usize,
    value_prefix: &str,
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let statements = (0..changes_per_commit)
        .map(|offset| {
            let row_index = row_offset + commit_index * changes_per_commit + offset;
            ExecuteBatchStatement {
                label: None,
                sql: "UPDATE working_diff_row SET value = $1 WHERE id = $2".to_string(),
                params: vec![
                    Value::Text(format!("{value_prefix}-{commit_index:05}")),
                    Value::Text(row_id(row_index)),
                ],
            }
        })
        .collect::<Vec<_>>();
    let results = session
        .execute_batch(&statements)
        .await
        .expect("update merge-preview rows");
    assert!(
        results.iter().all(|result| result.rows_affected() == 1),
        "every merge-preview update must affect exactly one row"
    );
}

#[inline(never)]
async fn profile_working_diff_query<StorageImpl>(session: &Lix<StorageImpl>) -> usize
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let checkpoint = session
        .execute(
            "SELECT checkpoint.commit_id \
             FROM lix_checkpoint AS checkpoint \
             JOIN lix_commit_ancestry() AS ancestry \
               ON ancestry.commit_id = checkpoint.commit_id \
             ORDER BY ancestry.depth LIMIT 1",
            &[],
        )
        .await
        .expect("query current working-diff checkpoint")
        .rows()[0]
        .get::<String>("commit_id")
        .expect("working-diff checkpoint ID");
    session
        .execute(WORKING_DIFF_SQL, &[Value::Text(checkpoint)])
        .await
        .expect("query populated working diff")
        .len()
}

async fn working_diff_count<StorageImpl>(session: &Lix<StorageImpl>) -> usize
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    profile_working_diff_query(session).await
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

fn percentile(sorted: &[Duration], percentile: usize) -> Duration {
    let rank = sorted.len().saturating_mul(percentile).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}
