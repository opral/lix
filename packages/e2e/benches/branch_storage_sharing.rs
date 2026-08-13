//! How much storage do two branches actually share?
//!
//! Seeds a realistic workspace through the real `SessionContext` SQL commit
//! path, records settled bytes, then applies one branching scenario and records
//! settled bytes again. The delta between the two snapshots is the price of the
//! scenario.
//!
//! Two independent measurements are taken per snapshot:
//!
//! * `physical_bytes` — a recursive filesystem walk of the backend directory
//!   after a memtable flush. Covers every byte the backend wrote, including
//!   spaces that are absent from the layout catalog, but is subject to LSM
//!   compaction timing.
//! * `layout_accounting` — exact logical `rows / key_bytes / value_bytes` per
//!   storage space. Deterministic and attributable: it says *which* plane paid.
//!
//! Run:
//! ```text
//! LIX_BRANCH_SHARING_ROWS=1000,10000,100000 \
//! LIX_BRANCH_SHARING_BACKENDS=rocksdb,slatedb \
//! cargo bench -p lix_e2e --features storage-benches,slatedb --bench branch_storage_sharing
//! ```

#![allow(clippy::large_futures)]

use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::time::{Duration, Instant};

use lix::storage::{ReadOptions, Storage};
use lix::storage_adapter::StorageAdapter;
use lix::storage_bench::{collect_repository_gc_for_bench, layout_accounting};
use lix::{CreateBranchOptions, MergeBranchOptions, Value};
use lix::{Lix, open_lix};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;

const DEFAULT_SCENARIOS: &[&str] = &[
    "base",
    "branch_noop",
    "branch_1row",
    "branch_1pct",
    "branch_10pct",
    "branches_10",
    "branches_100",
    "branches_10_1row",
    "branches_2_disjoint_1pct",
    "branches_2_identical_1pct",
    "merge_1pct",
    "delete_gc_1pct",
];

const SEED_BATCH_ROWS: usize = 5_000;

fn main() {
    let rows_list = env_usizes("LIX_BRANCH_SHARING_ROWS", &[1_000, 10_000, 100_000]);
    let scenarios: Vec<String> = std::env::var("LIX_BRANCH_SHARING_SCENARIOS")
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_else(|_| DEFAULT_SCENARIOS.iter().map(|s| (*s).to_owned()).collect());
    let settle_ms = env_u64("LIX_BRANCH_SHARING_SETTLE_MS", 250);
    let repetition = env_usize("LIX_BRANCH_SHARING_REP", 0);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create branch-sharing runtime");

    runtime.block_on(async {
        for rows in rows_list {
            for scenario in &scenarios {
                if selected("LIX_BRANCH_SHARING_BACKENDS", "rocksdb") {
                    let directory = tempfile::tempdir().expect("create RocksDB directory");
                    let storage =
                        RocksDB::open(directory.path()).expect("open branch-sharing RocksDB");
                    run_case(
                        "rocksdb",
                        storage,
                        directory.path(),
                        rows,
                        scenario,
                        settle_ms,
                        repetition,
                    )
                    .await;
                }
                if selected("LIX_BRANCH_SHARING_BACKENDS", "slatedb") {
                    let directory = tempfile::tempdir().expect("create SlateDB directory");
                    let storage =
                        SlateDB::open(directory.path()).expect("open branch-sharing SlateDB");
                    run_case(
                        "slatedb",
                        storage,
                        directory.path(),
                        rows,
                        scenario,
                        settle_ms,
                        repetition,
                    )
                    .await;
                }
            }
        }
    });
}

/// Backend-specific settling so `physical_bytes` samples SST state rather than
/// a memtable.
trait BenchBackend: Storage + Clone + Send + Sync + 'static {
    fn settle(&self) -> impl Future<Output = ()>;
}

impl BenchBackend for RocksDB {
    async fn settle(&self) {
        self.flush().expect("flush branch-sharing RocksDB");
    }
}

impl BenchBackend for SlateDB {
    async fn settle(&self) {
        self.flush_memtable_for_diagnostics()
            .await
            .expect("flush branch-sharing SlateDB memtable");
    }
}

#[derive(Clone, Default)]
struct Snapshot {
    physical_bytes: u64,
    physical_objects: u64,
    spaces: BTreeMap<&'static str, SpaceCounts>,
}

#[derive(Clone, Copy, Default)]
struct SpaceCounts {
    rows: u64,
    key_bytes: u64,
    value_bytes: u64,
}

impl Snapshot {
    fn logical_rows(&self) -> u64 {
        self.spaces.values().map(|counts| counts.rows).sum()
    }

    fn logical_bytes(&self) -> u64 {
        self.spaces
            .values()
            .map(|counts| counts.key_bytes + counts.value_bytes)
            .sum()
    }
}

async fn snapshot<S>(storage: &S, directory: &Path, settle_ms: u64) -> Snapshot
where
    S: BenchBackend,
{
    storage.settle().await;
    if settle_ms > 0 {
        tokio::time::sleep(Duration::from_millis(settle_ms)).await;
    }
    let adapter = StorageAdapter::new(storage.clone());
    let read = adapter
        .begin_read(ReadOptions::default())
        .await
        .expect("open branch-sharing layout read");
    let accounting = layout_accounting(&read).await;
    drop(read);
    let mut spaces = BTreeMap::new();
    for entry in accounting {
        spaces.insert(
            entry.space,
            SpaceCounts {
                rows: entry.rows,
                key_bytes: entry.key_bytes,
                value_bytes: entry.value_bytes,
            },
        );
    }
    Snapshot {
        physical_bytes: directory_bytes(directory),
        physical_objects: directory_objects(directory),
        spaces,
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_case<S>(
    backend: &str,
    storage: S,
    directory: &Path,
    rows: usize,
    scenario: &str,
    settle_ms: u64,
    repetition: usize,
) where
    S: BenchBackend,
{
    open_lix()
        .with_storage(storage.clone())
        .await
        .expect("initialize branch-sharing repository");
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open branch-sharing lix");
    let main = lix
        .open_another_session()
        .await
        .expect("open branch-sharing session");
    register_schema(&main).await;
    let seed_started = Instant::now();
    seed_rows(&main, rows).await;
    let seed_elapsed = seed_started.elapsed();

    let base = snapshot(&storage, directory, settle_ms).await;

    let scenario_started = Instant::now();
    let branches = apply_scenario(&lix, &main, &storage, scenario, rows).await;
    let scenario_elapsed = scenario_started.elapsed();

    let after = snapshot(&storage, directory, settle_ms).await;

    let changed_rows = scenario_changed_rows(scenario, rows);
    let physical_delta = after.physical_bytes as i64 - base.physical_bytes as i64;
    let logical_delta = after.logical_bytes() as i64 - base.logical_bytes() as i64;
    let logical_row_delta = after.logical_rows() as i64 - base.logical_rows() as i64;

    println!(
        "branch_sharing,rep={repetition},backend={backend},rows={rows},scenario={scenario},\
branches={branches},changed_rows={changed_rows},\
base_physical_bytes={},after_physical_bytes={},delta_physical_bytes={physical_delta},\
base_physical_objects={},after_physical_objects={},\
base_logical_bytes={},after_logical_bytes={},delta_logical_bytes={logical_delta},\
base_logical_rows={},after_logical_rows={},delta_logical_rows={logical_row_delta},\
bytes_per_changed_row={:.2},seed_ms={:.3},scenario_ms={:.3}",
        base.physical_bytes,
        after.physical_bytes,
        base.physical_objects,
        after.physical_objects,
        base.logical_bytes(),
        after.logical_bytes(),
        base.logical_rows(),
        after.logical_rows(),
        if changed_rows == 0 {
            0.0
        } else {
            logical_delta as f64 / changed_rows as f64
        },
        seed_elapsed.as_secs_f64() * 1_000.0,
        scenario_elapsed.as_secs_f64() * 1_000.0,
    );

    if scenario == "base" {
        for (name, counts) in &base.spaces {
            if counts.rows == 0 {
                continue;
            }
            println!(
                "branch_sharing_base_space,rep={repetition},backend={backend},rows={rows},\
space={name},base_rows={},base_key_bytes={},base_value_bytes={}",
                counts.rows, counts.key_bytes, counts.value_bytes,
            );
        }
    }

    let mut names: Vec<&'static str> = base.spaces.keys().copied().collect();
    for name in after.spaces.keys() {
        if !names.contains(name) {
            names.push(name);
        }
    }
    names.sort_unstable();
    for name in names {
        let before = base.spaces.get(name).copied().unwrap_or_default();
        let now = after.spaces.get(name).copied().unwrap_or_default();
        let d_rows = now.rows as i64 - before.rows as i64;
        let d_key = now.key_bytes as i64 - before.key_bytes as i64;
        let d_value = now.value_bytes as i64 - before.value_bytes as i64;
        if d_rows == 0 && d_key == 0 && d_value == 0 {
            continue;
        }
        println!(
            "branch_sharing_space,rep={repetition},backend={backend},rows={rows},scenario={scenario},\
space={name},base_rows={},after_rows={},d_rows={d_rows},\
base_bytes={},after_bytes={},d_bytes={}",
            before.rows,
            now.rows,
            before.key_bytes + before.value_bytes,
            now.key_bytes + now.value_bytes,
            d_key + d_value,
        );
    }
}

fn scenario_changed_rows(scenario: &str, rows: usize) -> usize {
    match scenario {
        "branch_1row" => 1,
        "branch_1pct" | "merge_1pct" | "delete_gc_1pct" => (rows / 100).max(1),
        "branch_10pct" => (rows / 10).max(1),
        "branches_10_1row" => 10,
        "branches_2_disjoint_1pct" | "branches_2_identical_1pct" => 2 * (rows / 100).max(1),
        _ => 0,
    }
}

async fn apply_scenario<S>(
    lix: &Lix<S>,
    main: &Lix<S>,
    storage: &S,
    scenario: &str,
    rows: usize,
) -> usize
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let one_percent = (rows / 100).max(1);
    let ten_percent = (rows / 10).max(1);
    match scenario {
        "base" => 0,
        "branch_noop" => {
            create_branch(main, "branch-0").await;
            1
        }
        "branch_1row" => {
            let branch = create_branch(main, "branch-0").await;
            modify_rows(lix, &branch, 0, 1).await;
            1
        }
        "branch_1pct" => {
            let branch = create_branch(main, "branch-0").await;
            modify_rows(lix, &branch, 0, one_percent).await;
            1
        }
        "branch_10pct" => {
            let branch = create_branch(main, "branch-0").await;
            modify_rows(lix, &branch, 0, ten_percent).await;
            1
        }
        "branches_10" => {
            for index in 0..10 {
                create_branch(main, &format!("branch-{index}")).await;
            }
            10
        }
        "branches_100" => {
            for index in 0..100 {
                create_branch(main, &format!("branch-{index}")).await;
            }
            100
        }
        "branches_10_1row" => {
            for index in 0..10 {
                let branch = create_branch(main, &format!("branch-{index}")).await;
                modify_rows(lix, &branch, index * 7, 1).await;
            }
            10
        }
        // Two branches, disjoint 1% windows: an honest two-diff price.
        "branches_2_disjoint_1pct" => {
            let first = create_branch(main, "branch-0").await;
            modify_rows(lix, &first, 0, one_percent).await;
            let second = create_branch(main, "branch-1").await;
            modify_rows(lix, &second, one_percent, one_percent).await;
            2
        }
        // Two branches making byte-identical edits to the same 1% window. If
        // the payload planes were content-addressed end to end this would cost
        // materially less than the disjoint case.
        "branches_2_identical_1pct" => {
            let first = create_branch(main, "branch-0").await;
            modify_rows(lix, &first, 0, one_percent).await;
            let second = create_branch(main, "branch-1").await;
            modify_rows(lix, &second, 0, one_percent).await;
            2
        }
        "merge_1pct" => {
            let branch = create_branch(main, "branch-0").await;
            modify_rows(lix, &branch, 0, one_percent).await;
            main.merge_branch(MergeBranchOptions {
                source_branch_id: branch.clone(),
            })
            .await
            .expect("merge branch-sharing branch");
            1
        }
        "delete_gc_1pct" => {
            let branch = create_branch(main, "branch-0").await;
            modify_rows(lix, &branch, 0, one_percent).await;
            main.execute(
                "DELETE FROM lix_branch WHERE id = $1",
                &[Value::Text(branch.clone())],
            )
            .await
            .expect("delete branch-sharing branch");
            let adapter = StorageAdapter::new(storage.clone());
            collect_repository_gc_for_bench(&adapter)
                .await
                .expect("collect branch-sharing repository GC");
            1
        }
        other => panic!("unknown LIX_BRANCH_SHARING_SCENARIOS entry '{other}'"),
    }
}

async fn create_branch<S>(main: &Lix<S>, name: &str) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    main.create_branch(CreateBranchOptions {
        id: None,
        name: name.to_owned(),
        from_commit_id: None,
    })
    .await
    .expect("create branch-sharing branch")
    .id
}

/// Updates the `count` rows starting at `start` inside `branch`. The written
/// value depends only on the row index, so two branches given the same window
/// produce byte-identical content — which is what makes cross-branch content
/// dedup observable.
async fn modify_rows<S>(lix: &Lix<S>, branch: &str, start: usize, count: usize)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let session = lix
        .open_another_session()
        .await
        .expect("open branch-sharing branch session");
    session
        .switch_branch(lix::SwitchBranchOptions {
            branch_id: (branch.to_owned()).to_string(),
        })
        .await
        .expect("switch session branch");
    let mut written = 0usize;
    while written < count {
        let batch = (count - written).min(SEED_BATCH_ROWS);
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("begin branch-sharing modification");
        for offset in 0..batch {
            let index = start + written + offset;
            transaction
                .execute(
                    "UPDATE branch_fixture SET value = CAST($1 AS JSONB) WHERE path = $2",
                    &[
                        Value::Text(format!(r#"{{"seed":{index},"edited":true,"pad":"{PAD}"}}"#)),
                        Value::Text(row_path(index)),
                    ],
                )
                .await
                .expect("stage branch-sharing modification");
        }
        transaction
            .commit()
            .await
            .expect("commit branch-sharing modification");
        written += batch;
    }
}

const PAD: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

fn row_path(index: usize) -> String {
    format!("/branch/fixture/{index:09}")
}

async fn register_schema<S>(session: &Lix<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let schema = serde_json::json!({
        "x-lix-key": "branch_fixture",
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
             VALUES (CAST($1 AS JSONB), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register branch-sharing schema");
}

async fn seed_rows<S>(session: &Lix<S>, rows: usize)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let mut written = 0usize;
    while written < rows {
        let batch = (rows - written).min(SEED_BATCH_ROWS);
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("begin branch-sharing seed");
        for offset in 0..batch {
            let index = written + offset;
            transaction
                .execute(
                    "INSERT INTO branch_fixture (path, value) VALUES ($1, CAST($2 AS JSONB))",
                    &[
                        Value::Text(row_path(index)),
                        Value::Text(format!(r#"{{"seed":{index},"pad":"{PAD}"}}"#)),
                    ],
                )
                .await
                .expect("stage branch-sharing seed row");
        }
        transaction
            .commit()
            .await
            .expect("commit branch-sharing seed");
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

fn directory_objects(path: &Path) -> u64 {
    std::fs::read_dir(path).map_or(0, |entries| {
        entries
            .flatten()
            .map(|entry| {
                let path = entry.path();
                entry.metadata().map_or(0, |metadata| {
                    if metadata.is_dir() {
                        directory_objects(&path)
                    } else {
                        1
                    }
                })
            })
            .sum()
    })
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usizes(name: &str, default: &[usize]) -> Vec<usize> {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| value.parse().expect("numeric branch-sharing list entry"))
                .collect()
        })
        .unwrap_or_else(|| default.to_vec())
}

fn selected(name: &str, candidate: &str) -> bool {
    std::env::var(name).map_or(true, |selection| {
        selection
            .split(',')
            .map(str::trim)
            .any(|value| value == candidate)
    })
}
