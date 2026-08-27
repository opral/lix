#![recursion_limit = "256"]

//! Growth curves for the storage spaces layout accounting could not see.
//!
//! Before PR #1308 `native_storage_spaces()` enumerated 32 of 46 physical
//! spaces, so `layout_accounting`, `layout_space_catalog` and every report
//! built on them (`storage_layout`, `space_growth`, the `tracked_state_crud`
//! layout table) were blind to 14 real planes. Every per-space byte number this
//! optimization program published before that fix is therefore a lower bound.
//!
//! `space_growth` answers "which spaces grow per commit" for one axis only.
//! This example sweeps the axes that can distinguish the three classifications
//! that matter:
//!
//! * **bounded** — flat, or converging to a constant, in every axis.
//! * **proportional** — grows with *live* state (rows, branches, files). Paying
//!   bytes for data that is still reachable is what a store is for.
//! * **unbounded** — grows with *history* and is never reclaimed. That is a
//!   leak, and no amount of GC brings it back.
//!
//! Scenarios (each prints one `expv,` line per space per sample):
//!
//! ```text
//! commits     <samples-csv> <rows_per_commit> [gc_rounds]
//! rows        <commits> <rows-csv>
//! branches    <samples-csv> <rows_per_commit>
//! checkpoints <samples-csv> <rows_per_commit>
//! idempotency <samples-csv>
//! uploads     <samples-csv> <bytes_per_upload> <keep|delete>
//! ```
//!
//! `samples-csv` is an ascending list of axis values; the axis is driven
//! forward incrementally and layout accounting is sampled at each value, so one
//! process produces a whole curve without rebuilding the fixture.

use std::collections::BTreeMap;
use std::sync::Arc;

use lix::storage::Storage;
use lix::storage_adapter::{StorageAdapter, StorageReadOptions};
use lix::storage_bench::{collect_repository_gc_for_bench, layout_accounting};
use lix::{CreateBranchOptions, Value};
use lix::{Lix, open_lix};
use lix_storage_rocksdb::RocksDB;

#[derive(Clone, Copy, Default)]
struct SpaceUsage {
    rows: u64,
    key_bytes: u64,
    value_bytes: u64,
}

type Usage = BTreeMap<(u32, &'static str), SpaceUsage>;

#[tokio::main]
async fn main() {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    let scenario = arguments.first().map(String::as_str).unwrap_or("commits");
    match scenario {
        "commits" => {
            let samples = parse_samples(arguments.get(1));
            let rows = parse_usize(arguments.get(2), 10);
            let gc_rounds = parse_usize(arguments.get(3), 0);
            commits_scenario(&samples, rows, gc_rounds).await;
        }
        "rows" => {
            let commits = parse_usize(arguments.get(1), 20);
            let samples = parse_samples(arguments.get(2));
            rows_scenario(commits, &samples).await;
        }
        "branches" => {
            let samples = parse_samples(arguments.get(1));
            let rows = parse_usize(arguments.get(2), 10);
            branches_scenario(&samples, rows).await;
        }
        "checkpoints" => {
            let samples = parse_samples(arguments.get(1));
            let rows = parse_usize(arguments.get(2), 10);
            checkpoints_scenario(&samples, rows).await;
        }
        "row_commits" => {
            let samples = parse_samples(arguments.get(1));
            let rows = parse_usize(arguments.get(2), 10);
            row_commits_scenario(&samples, rows).await;
        }
        "checkpoint_then_gc" => {
            let commits = parse_usize(arguments.get(1), 200);
            let rows = parse_usize(arguments.get(2), 10);
            let gc_rounds = parse_usize(arguments.get(3), 12);
            checkpoint_then_gc_scenario(commits, rows, gc_rounds).await;
        }
        "uploads" => {
            let samples = parse_samples(arguments.get(1));
            let bytes = parse_usize(arguments.get(2), 4096);
            let delete = arguments.get(3).map(String::as_str) == Some("delete");
            uploads_scenario(&samples, bytes, delete).await;
        }
        other => panic!("unknown scenario '{other}'"),
    }
}

// ---------------------------------------------------------------- scenarios

/// Commit count is the axis that separates "proportional" from "unbounded":
/// row count, branch count and file count are all held fixed, so any plane that
/// keeps growing is growing with history alone.
async fn commits_scenario(samples: &[usize], rows_per_commit: usize, gc_rounds: usize) {
    let fixture = Fixture::open().await;
    register_schema(&fixture.session).await;
    let mut committed = 0usize;
    for &target in samples {
        while committed < target {
            commit_batch(&fixture.session, committed, rows_per_commit).await;
            committed += 1;
        }
        report(
            "commits",
            "live",
            target as u64,
            &usage(&fixture.storage).await,
        );
    }
    for round in 1..=gc_rounds {
        let result = collect_repository_gc_for_bench(&StorageAdapter::new(fixture.storage.clone()))
            .await
            .expect("commit repository GC");
        println!(
            "expv_gc,scenario=commits,round={round},commits={committed},staged_deletes={},swept_commits={},plan_us={},commit_us={}",
            result.staged_deletes, result.swept_commits, result.plan_us, result.commit_us
        );
        report(
            "commits",
            "gc",
            round as u64,
            &usage(&fixture.storage).await,
        );
    }
}

/// Live-row count at a fixed commit count. A plane that moves here and not on
/// the commit axis is proportional to live state, which is expected.
async fn rows_scenario(commits: usize, samples: &[usize]) {
    for &rows in samples {
        let fixture = Fixture::open().await;
        register_schema(&fixture.session).await;
        for index in 0..commits {
            commit_batch(&fixture.session, index, rows).await;
        }
        report(
            "rows",
            "live",
            (rows * commits) as u64,
            &usage(&fixture.storage).await,
        );
    }
}

async fn branches_scenario(samples: &[usize], rows_per_commit: usize) {
    let fixture = Fixture::open().await;
    register_schema(&fixture.session).await;
    commit_batch(&fixture.session, 0, rows_per_commit).await;
    let mut created = 0usize;
    for &target in samples {
        while created < target {
            fixture
                .session
                .create_branch(CreateBranchOptions {
                    id: None,
                    name: format!("expv-branch-{created}"),
                    from_commit_id: None,
                })
                .await
                .expect("create branch");
            created += 1;
        }
        report(
            "branches",
            "live",
            target as u64,
            &usage(&fixture.storage).await,
        );
    }
}

async fn checkpoints_scenario(samples: &[usize], rows_per_commit: usize) {
    let fixture = Fixture::open().await;
    register_schema(&fixture.session).await;
    let mut created = 0usize;
    for &target in samples {
        while created < target {
            commit_batch(&fixture.session, created, rows_per_commit).await;
            fixture
                .session
                .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
                .await
                .expect("create checkpoint");
            created += 1;
        }
        report(
            "checkpoints",
            "live",
            target as u64,
            &usage(&fixture.storage).await,
        );
    }
}

/// A strictly typed flat schema surface, which is what
/// `encode_registered_row_groups` needs before it publishes anything
/// into the two `row.columnar_row_group_*` spaces. The JSON-valued schema
/// the other scenarios use never reaches that encoder, so those two spaces stay
/// empty there and would be misread as "bounded" without this workload.
async fn row_commits_scenario(samples: &[usize], rows_per_commit: usize) {
    let fixture = Fixture::open().await;
    let schema = serde_json::json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "expv_row",
        "columns": [
            { "name": "id", "type": "text", "nullable": false },
            { "name": "name", "type": "text", "nullable": false },
            { "name": "amount", "type": "int8", "nullable": false },
        ],
        "primary_key": ["id"],
    });
    fixture
        .session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (CAST($1 AS JSONB), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register row schema");
    let mut committed = 0usize;
    for &target in samples {
        while committed < target {
            let mut transaction = fixture
                .session
                .begin_transaction()
                .await
                .expect("begin commit");
            for index in 0..rows_per_commit {
                transaction
                    .execute(
                        "INSERT INTO expv_row (id, name, amount) VALUES ($1, $2, $3)",
                        &[
                            Value::Text(format!("{committed:08}-{index:08}")),
                            Value::Text(format!("name-{committed}-{index}")),
                            Value::Integer((committed * 1000 + index) as i64),
                        ],
                    )
                    .await
                    .expect("insert row");
            }
            transaction.commit().await.expect("commit row batch");
            committed += 1;
        }
        report(
            "row_commits",
            "live",
            target as u64,
            &usage(&fixture.storage).await,
        );
    }
}

/// Isolates the drain rate of the authenticated reachability queue.
///
/// Without a checkpoint the whole commit chain is the retained undo interval
/// (`gc.rs:118-159` walks head → `serving_checkpoint_commit_id`), so no old
/// root is retirable and no queue row is consumable. One checkpoint lowers that
/// floor; the explicit GC rounds afterwards then measure how many queue rows
/// one sweep actually consumes.
async fn checkpoint_then_gc_scenario(commits: usize, rows_per_commit: usize, gc_rounds: usize) {
    let fixture = Fixture::open().await;
    register_schema(&fixture.session).await;
    for index in 0..commits {
        commit_batch(&fixture.session, index, rows_per_commit).await;
    }
    report(
        "checkpoint_then_gc",
        "before_checkpoint",
        commits as u64,
        &usage(&fixture.storage).await,
    );
    fixture
        .session
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .expect("create checkpoint");
    report(
        "checkpoint_then_gc",
        "after_checkpoint",
        0,
        &usage(&fixture.storage).await,
    );
    for round in 1..=gc_rounds {
        let result = collect_repository_gc_for_bench(&StorageAdapter::new(fixture.storage.clone()))
            .await
            .expect("commit repository GC");
        println!(
            "expv_gc,scenario=checkpoint_then_gc,round={round},commits={commits},staged_deletes={},swept_commits={},plan_us={},commit_us={}",
            result.staged_deletes, result.swept_commits, result.plan_us, result.commit_us
        );
        report(
            "checkpoint_then_gc",
            "gc",
            round as u64,
            &usage(&fixture.storage).await,
        );
    }
}

/// Resumable-upload receipts. `keep` leaves every uploaded file live; `delete`
/// removes each file again, which is the shape that tells retained-receipt
/// bytes apart from live-file bytes.
async fn uploads_scenario(samples: &[usize], bytes_per_upload: usize, delete: bool) {
    let fixture = Fixture::open().await;
    let mut uploaded = 0usize;
    for &target in samples {
        while uploaded < target {
            let path = format!("/expv/upload-{uploaded:08}.bin");
            let payload = vec![(uploaded % 251) as u8; bytes_per_upload];
            fixture
                .session
                .execute(
                    "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
                     ON CONFLICT (path) DO UPDATE SET content = excluded.content",
                    &[Value::Text(path.clone()), Value::Blob(payload.into())],
                )
                .await
                .expect("write uploaded file through SQL");
            if delete {
                fixture
                    .session
                    .execute("DELETE FROM lix_file WHERE path = $1", &[Value::Text(path)])
                    .await
                    .expect("delete uploaded file");
            }
            uploaded += 1;
        }
        report(
            "uploads",
            if delete { "live_deleted" } else { "live_kept" },
            target as u64,
            &usage(&fixture.storage).await,
        );
    }
    let result = collect_repository_gc_for_bench(&StorageAdapter::new(fixture.storage.clone()))
        .await
        .expect("commit repository GC");
    println!(
        "expv_gc,scenario=uploads,round=1,uploads={uploaded},staged_deletes={},swept_commits={}",
        result.staged_deletes, result.swept_commits
    );
    report("uploads", "gc", 1, &usage(&fixture.storage).await);
}

// ------------------------------------------------------------------ harness

struct Fixture {
    storage: RocksDB,
    session: Arc<Lix<RocksDB>>,
    _directory: Option<tempfile::TempDir>,
}

impl Fixture {
    /// Logical accounting is the default answer, so the fixture normally lives
    /// in a temporary directory. Setting `LIX_EXPV_DIR` keeps the backend files
    /// at a fixed path instead, which is what turns a logical byte curve into a
    /// physical one (`flush`, then measure the directory).
    async fn open() -> Self {
        let (directory, path) = match std::env::var_os("LIX_EXPV_DIR") {
            Some(path) => (None, std::path::PathBuf::from(path)),
            None => {
                let directory = tempfile::tempdir().expect("create RocksDB directory");
                let path = directory.path().to_path_buf();
                (Some(directory), path)
            }
        };
        let storage = RocksDB::open(&path).expect("open RocksDB");
        open_lix()
            .with_storage(storage.clone())
            .await
            .expect("initialize repository");
        let lix = open_lix()
            .with_storage(storage.clone())
            .await
            .expect("open lix");
        let session = lix.open_another_session().await.expect("open workspace");
        Self {
            storage,
            session: Arc::new(session),
            _directory: directory,
        }
    }
}

fn report(scenario: &str, phase: &str, axis: u64, usage: &Usage) {
    for ((id, name), value) in usage {
        println!(
            "expv,scenario={scenario},phase={phase},axis={axis},space_id=0x{id:08x},space={name},rows={},key_bytes={},value_bytes={}",
            value.rows, value.key_bytes, value.value_bytes
        );
    }
}

async fn usage<S>(storage: &S) -> Usage
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let adapter = StorageAdapter::new(storage.clone());
    let read = adapter
        .begin_read(StorageReadOptions::default())
        .await
        .expect("open storage snapshot");
    let accounting = layout_accounting(&read)
        .await
        .into_iter()
        .map(|entry| {
            (
                (entry.space_id, entry.space),
                SpaceUsage {
                    rows: entry.rows,
                    key_bytes: entry.key_bytes,
                    value_bytes: entry.value_bytes,
                },
            )
        })
        .collect();
    drop(read);
    accounting
}

async fn commit_batch<S>(session: &Lix<S>, batch: usize, rows: usize)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let mut transaction = session.begin_transaction().await.expect("begin commit");
    for index in 0..rows {
        transaction
            .execute(
                "INSERT INTO space_growth (path, value) VALUES ($1, CAST($2 AS JSONB))",
                &[
                    Value::Text(format!("/row/{batch:08}/{index:08}")),
                    Value::Text(format!(r#"{{"batch":{batch},"index":{index}}}"#)),
                ],
            )
            .await
            .expect("insert row");
    }
    transaction.commit().await.expect("commit batch");
}

async fn register_schema<S>(session: &Lix<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let schema = serde_json::json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "space_growth",
        "columns": [
            { "name": "path", "type": "text", "nullable": false },
            { "name": "value", "type": "jsonb", "nullable": false },
        ],
        "primary_key": ["path"],
    });
    session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (CAST($1 AS JSONB), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register schema");
}

fn parse_samples(argument: Option<&String>) -> Vec<usize> {
    let samples = argument
        .map(|value| {
            value
                .split(',')
                .map(|entry| entry.trim().parse::<usize>().expect("sample must be usize"))
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| vec![1, 10, 100, 1000]);
    assert!(!samples.is_empty(), "need at least one sample point");
    for pair in samples.windows(2) {
        assert!(pair[0] < pair[1], "sample points must ascend");
    }
    samples
}

fn parse_usize(argument: Option<&String>, fallback: usize) -> usize {
    argument
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(fallback)
}
