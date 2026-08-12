//! How much of the settled repository is byte-identical duplicate content?
//!
//! The commit-delta segment plane is keyed `commit_id ++ segment_index`, not by
//! a content digest, so nothing in the engine can notice that two commits wrote
//! the same segment bytes. Every other high-volume immutable plane in the engine
//! *is* content-addressed. The obvious question is whether that difference costs
//! anything in practice.
//!
//! This example answers it by measurement rather than by assumption. Each
//! workload builds a repository through the real `SessionContext` SQL commit
//! path, then reads every storage space back and reports, per space:
//!
//! * `rows` / `value_bytes` — what settled.
//! * `distinct` — distinct value byte strings.
//! * `dup_bytes` — what a perfect content-addressed store would never have
//!   written: `(occurrences - 1) * len` summed over distinct values. This is an
//!   upper bound on the win, since it charges nothing for the indirection a real
//!   content-addressed layout would need.
//!
//! For the delta-segment plane it additionally reports a nearest-neighbour
//! analysis over equal-length segments: if two segments that "should" be
//! identical differ in a handful of bytes, the payload carries per-commit
//! identity that a naive CAS key would not have caught anyway.
//!
//! Usage:
//! ```sh
//! cargo run -p lix_e2e --release --features storage-benches \
//!   --example delta_segment_duplication -- [rows] [workloads]
//! ```
//! `workloads` is a comma-separated subset of the names in `WORKLOADS`.

use std::path::Path;

use lix::integration::{Engine, SessionContext};
use lix::storage::Storage;
use lix::storage_adapter::{StorageAdapter, StorageReadOptions};
use lix::storage_bench::{
    CommitDeltaSegmentSimilarity, StorageValueDuplication, commit_delta_segment_similarity,
    space_value_duplication,
};
use lix::{CreateBranchOptions, MergeBranchOptions, Value};
use lix_storage_rocksdb::RocksDB;

const WORKLOADS: &[&str] = &[
    // Ordinary forward development: every commit writes new content.
    "linear_disjoint",
    "linear_hot_window",
    // Real workloads that plausibly re-create content already stored.
    "repeat_identical_write",
    "revert_roundtrip",
    "undo_roundtrip",
    "merge_replay",
    // Synthetic upper bound: N branches making byte-identical edits.
    "branches_2_identical",
    "branches_2_disjoint",
    "branches_10_identical",
    "branches_10_disjoint",
];

const PAD: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const SEED_BATCH_ROWS: usize = 5_000;
/// Commits applied by every multi-commit workload, so their per-commit costs
/// are directly comparable.
const WORKLOAD_COMMITS: usize = 10;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut arguments = std::env::args().skip(1);
    let rows = arguments
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10_000);
    // Rows touched by one workload commit. The segment plane only receives a
    // commit whose mutation count clears the ordered-part geometry, so the edit
    // width is the variable that decides whether this plane is exercised at all.
    let window = arguments
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or_else(|| (rows / 100).max(1));
    let selected = arguments.next().map(|value| {
        value
            .split(',')
            .map(str::trim)
            .map(str::to_owned)
            .collect::<Vec<_>>()
    });

    for workload in WORKLOADS {
        if let Some(selected) = selected.as_ref()
            && !selected.iter().any(|value| value == workload)
        {
            continue;
        }
        run_workload(workload, rows, window).await;
    }
}

async fn run_workload(workload: &str, rows: usize, window: usize) {
    let directory = tempfile::tempdir().expect("create duplication-audit directory");
    let storage = RocksDB::open(directory.path()).expect("open duplication-audit RocksDB");
    Engine::initialize(storage.clone())
        .await
        .expect("initialize duplication-audit repository");
    let engine = Engine::new(storage.clone())
        .await
        .expect("open duplication-audit engine");
    let main = engine
        .open_workspace_session()
        .await
        .expect("open duplication-audit workspace");
    register_schema(&main).await;
    seed_rows(&main, rows).await;

    apply_workload(&engine, &main, workload, window).await;

    storage.flush().expect("flush duplication-audit RocksDB");
    let adapter = StorageAdapter::new(storage.clone());
    let read = adapter
        .begin_read(StorageReadOptions::default())
        .await
        .expect("open duplication-audit snapshot");
    let duplication = space_value_duplication(&read).await;
    let similarity = commit_delta_segment_similarity(&read).await;
    drop(read);

    report(
        workload,
        rows,
        window,
        directory.path(),
        &duplication,
        similarity,
    );
}

async fn apply_workload<S>(
    engine: &Engine<S>,
    main: &SessionContext<S>,
    workload: &str,
    window: usize,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    match workload {
        // Each commit edits a different window: no commit can repeat another.
        "linear_disjoint" => {
            for index in 0..WORKLOAD_COMMITS {
                edit_rows(main, index * window, window, index + 1).await;
            }
        }
        // Every commit rewrites the same rows with fresh content.
        "linear_hot_window" => {
            for index in 0..WORKLOAD_COMMITS {
                edit_rows(main, 0, window, index + 1).await;
            }
        }
        // The same statement, committed repeatedly. Every commit after the
        // first re-states content the repository already holds.
        "repeat_identical_write" => {
            for _ in 0..WORKLOAD_COMMITS {
                edit_rows(main, 0, window, 1).await;
            }
        }
        // Edit away and back again. Every even commit restores content that the
        // repository stored one commit earlier.
        "revert_roundtrip" => {
            for index in 0..WORKLOAD_COMMITS / 2 {
                edit_rows(main, 0, window, index + 1).await;
                restore_rows(main, 0, window).await;
            }
        }
        // Same shape, driven through the engine's own undo path.
        "undo_roundtrip" => {
            for index in 0..WORKLOAD_COMMITS / 2 {
                edit_rows(main, 0, window, index + 1).await;
                main.undo().await.expect("undo duplication-audit commit");
            }
        }
        // Two branches off the same base make the same edit; both are merged
        // into main. The merge commits replay content already stored.
        "merge_replay" => {
            let first = create_branch(main, "replay-0").await;
            edit_on_branch(engine, &first, 0, window, 1).await;
            let second = create_branch(main, "replay-1").await;
            edit_on_branch(engine, &second, 0, window, 1).await;
            main.merge_branch(MergeBranchOptions {
                source_branch_id: first,
            })
            .await
            .expect("merge duplication-audit branch");
            let _ = main
                .merge_branch(MergeBranchOptions {
                    source_branch_id: second,
                })
                .await;
        }
        "branches_2_identical" => branch_fanout(engine, main, 2, window, false).await,
        "branches_2_disjoint" => branch_fanout(engine, main, 2, window, true).await,
        "branches_10_identical" => branch_fanout(engine, main, 10, window, false).await,
        "branches_10_disjoint" => branch_fanout(engine, main, 10, window, true).await,
        other => panic!("unknown duplication-audit workload '{other}'"),
    }
}

async fn branch_fanout<S>(
    engine: &Engine<S>,
    main: &SessionContext<S>,
    branches: usize,
    window: usize,
    disjoint: bool,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    for index in 0..branches {
        let branch = create_branch(main, &format!("fanout-{index}")).await;
        let start = if disjoint { index * window } else { 0 };
        edit_on_branch(engine, &branch, start, window, 1).await;
    }
}

fn report(
    workload: &str,
    rows: usize,
    window: usize,
    directory: &Path,
    duplication: &[StorageValueDuplication],
    similarity: CommitDeltaSegmentSimilarity,
) {
    let settled_value_bytes: u64 = duplication.iter().map(|entry| entry.value_bytes).sum();
    let settled_key_bytes: u64 = duplication.iter().map(|entry| entry.key_bytes).sum();
    let duplicate_bytes: u64 = duplication
        .iter()
        .map(|entry| entry.duplicate_value_bytes)
        .sum();
    let segment = duplication
        .iter()
        .find(|entry| entry.space_id == 0x0004_001a)
        .copied()
        .unwrap_or_default();

    println!(
        "delta_dup,workload={workload},rows={rows},window={window},\
physical_bytes={},settled_key_bytes={settled_key_bytes},settled_value_bytes={settled_value_bytes},\
settled_dup_bytes={duplicate_bytes},settled_dup_pct={:.3},\
segment_rows={},segment_value_bytes={},segment_distinct={},segment_dup_rows={},\
segment_dup_bytes={},segment_dup_pct={:.3},segment_max_occurrences={},\
segment_share_of_settled_pct={:.2},\
sim_distinct={},sim_same_length={},sim_pairs={},sim_near_identical={},\
sim_min_differing_bytes={},sim_min_pair_len={},sim_min_pair_common_prefix={},\
sim_min_pair_common_suffix={},\
norm_dup_segments={},norm_dup_bytes={},norm_dup_pct_of_segment={:.2},\
norm_dup_pct_of_settled={:.2},norm_shared_classes={}",
        directory_bytes(directory),
        percent(duplicate_bytes, settled_value_bytes),
        segment.rows,
        segment.value_bytes,
        segment.distinct_values,
        segment.duplicate_rows,
        segment.duplicate_value_bytes,
        segment.duplicate_fraction() * 100.0,
        segment.max_occurrences,
        percent(segment.value_bytes, settled_value_bytes),
        similarity.distinct_values,
        similarity.same_length_distinct_values,
        similarity.compared_pairs,
        similarity.near_identical_pairs,
        similarity.min_differing_bytes,
        similarity.min_differing_pair_len,
        similarity.min_differing_pair_common_prefix,
        similarity.min_differing_pair_common_suffix,
        similarity.identity_normalized_duplicate_segments,
        similarity.identity_normalized_duplicate_bytes,
        percent(
            similarity.identity_normalized_duplicate_bytes,
            segment.value_bytes,
        ),
        percent(
            similarity.identity_normalized_duplicate_bytes,
            settled_value_bytes,
        ),
        similarity.identity_normalized_shared_classes,
    );

    let mut by_bytes: Vec<&StorageValueDuplication> = duplication
        .iter()
        .filter(|entry| entry.value_bytes > 0)
        .collect();
    by_bytes.sort_unstable_by_key(|entry| std::cmp::Reverse(entry.value_bytes));
    for entry in by_bytes.iter().take(12) {
        println!(
            "  delta_dup_space,workload={workload},rows={rows},space=0x{:08x}/{},\
rows={},key_bytes={},value_bytes={},distinct={},dup_rows={},dup_bytes={},dup_pct={:.3},\
max_occurrences={}",
            entry.space_id,
            entry.space,
            entry.rows,
            entry.key_bytes,
            entry.value_bytes,
            entry.distinct_values,
            entry.duplicate_rows,
            entry.duplicate_value_bytes,
            entry.duplicate_fraction() * 100.0,
            entry.max_occurrences,
        );
    }
}

fn percent(part: u64, whole: u64) -> f64 {
    if whole == 0 {
        0.0
    } else {
        part as f64 * 100.0 / whole as f64
    }
}

async fn create_branch<S>(main: &SessionContext<S>, name: &str) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    main.create_branch(CreateBranchOptions {
        id: None,
        name: name.to_owned(),
        from_commit_id: None,
    })
    .await
    .expect("create duplication-audit branch")
    .id
}

async fn edit_on_branch<S>(
    engine: &Engine<S>,
    branch: &str,
    start: usize,
    count: usize,
    generation: usize,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let session = engine
        .open_session(branch.to_owned())
        .await
        .expect("open duplication-audit branch session");
    edit_rows(&session, start, count, generation).await;
}

/// The written value depends only on the row index and the generation, so two
/// commits given the same `(start, count, generation)` produce byte-identical
/// content whatever branch or order they run in.
async fn edit_rows<S>(session: &SessionContext<S>, start: usize, count: usize, generation: usize)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let mut written = 0usize;
    while written < count {
        let batch = (count - written).min(SEED_BATCH_ROWS);
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("begin duplication-audit edit");
        for offset in 0..batch {
            let index = start + written + offset;
            transaction
                .execute(
                    "UPDATE dup_fixture SET value = lix_json($1) WHERE path = $2",
                    &[
                        Value::Text(format!(
                            r#"{{"seed":{index},"generation":{generation},"pad":"{PAD}"}}"#
                        )),
                        Value::Text(row_path(index)),
                    ],
                )
                .await
                .expect("stage duplication-audit edit");
        }
        transaction
            .commit()
            .await
            .expect("commit duplication-audit edit");
        written += batch;
    }
}

/// Restores the exact bytes `seed_rows` wrote, so the resulting commit's payload
/// is content the repository already stored.
async fn restore_rows<S>(session: &SessionContext<S>, start: usize, count: usize)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let mut transaction = session
        .begin_transaction()
        .await
        .expect("begin duplication-audit restore");
    for offset in 0..count {
        let index = start + offset;
        transaction
            .execute(
                "UPDATE dup_fixture SET value = lix_json($1) WHERE path = $2",
                &[
                    Value::Text(seed_value(index)),
                    Value::Text(row_path(index)),
                ],
            )
            .await
            .expect("stage duplication-audit restore");
    }
    transaction
        .commit()
        .await
        .expect("commit duplication-audit restore");
}

fn row_path(index: usize) -> String {
    format!("/dup/fixture/{index:09}")
}

fn seed_value(index: usize) -> String {
    format!(r#"{{"seed":{index},"pad":"{PAD}"}}"#)
}

async fn register_schema<S>(session: &SessionContext<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let schema = serde_json::json!({
        "x-lix-key": "dup_fixture",
        "x-lix-primary-key": ["/path"],
        "type": "object",
        "required": ["path", "value"],
        "properties": {
            "path": { "type": "string" },
            "value": {
                "type": ["object", "array", "string", "number", "integer", "boolean", "null"]
            }
        },
        "additionalProperties": false
    });
    session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (lix_json($1), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register duplication-audit schema");
}

async fn seed_rows<S>(session: &SessionContext<S>, rows: usize)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let mut written = 0usize;
    while written < rows {
        let batch = (rows - written).min(SEED_BATCH_ROWS);
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("begin duplication-audit seed");
        for offset in 0..batch {
            let index = written + offset;
            transaction
                .execute(
                    "INSERT INTO dup_fixture (path, value) VALUES ($1, lix_json($2))",
                    &[Value::Text(row_path(index)), Value::Text(seed_value(index))],
                )
                .await
                .expect("stage duplication-audit seed row");
        }
        transaction
            .commit()
            .await
            .expect("commit duplication-audit seed");
        written += batch;
    }
}

fn directory_bytes(path: &Path) -> u64 {
    std::fs::read_dir(path).map_or(0, |entries| {
        entries
            .flatten()
            .map(|entry| {
                let path = entry.path();
                entry.metadata().map_or(0, |metadata| {
                    if metadata.is_dir() {
                        directory_bytes(&path)
                    } else {
                        metadata.len()
                    }
                })
            })
            .sum()
    })
}
