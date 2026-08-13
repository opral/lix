//! `e53_hot_locality`, on SlateDB, where a block fetch is an object-store GET.
//!
//! The RocksDB run of this experiment found the eight-space layout costs eight
//! separate iterator positionings, growing by one block fetch per space per
//! doubling of the store -- but every one of those fetches was a *block cache
//! hit*, worth tens of nanoseconds, so the defect landed at a few percent of
//! the retire's cost. That result says nothing about SlateDB, where the same
//! structural fetch is a range GET against an object store.
//!
//! This drives the identical probe against SlateDB and counts
//! `SlateDBIoSnapshot::read_objects` -- actual object-store GETs -- around it.
//! The `phantom` probe holds the garbage at exactly zero, so its GET count is
//! purely the cost of positioning eight iterators in eight regions of one
//! keyspace.
//!
//! Usage: `e53_hot_locality_slatedb [rows_per_commit] [checkpoint...]`

use lix::Value;
use lix::{Lix, open_lix};
use lix::storage::Storage;
use lix::storage_adapter::{StorageAdapter, StorageReadOptions};
use lix::storage_bench::{hot_generation_branches, layout_accounting, probe_hot_generation_planes};
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters, SlateDBIoSnapshot};

const PROBE_REPS: usize = 3;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let mut args = std::env::args().skip(1);
    let rows_per_commit = args
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10);
    let mut checkpoints = args
        .filter_map(|value| value.parse::<usize>().ok())
        .collect::<Vec<_>>();
    if checkpoints.is_empty() {
        checkpoints = vec![50, 100, 200, 400, 800];
    }
    checkpoints.sort_unstable();

    let directory = tempfile::tempdir().expect("create SlateDB directory");
    let path = directory.path().to_path_buf();
    {
        let counters = SlateDBIoCounters::default();
        let storage =
            SlateDB::open_with_io_counters(&path, counters.clone()).expect("open SlateDB");
        let session = open_lix()
            .with_storage(storage.clone())
            .await
            .expect("open workspace");
        register_schema(&session).await;
        storage.flush().await.expect("flush slatedb");
    }
    let branch = {
        let counters = SlateDBIoCounters::default();
        let storage =
            SlateDB::open_with_io_counters(&path, counters.clone()).expect("open SlateDB");
        let picked = pick_branch(&storage).await;
        storage.flush().await.expect("flush slatedb");
        picked
    };
    println!(
        "e53_hot_locality_slatedb rows_per_commit={rows_per_commit} probe_reps={PROBE_REPS} branch={branch}"
    );
    println!(
        "{:>8} {:>12} {:>13} {:>8} {:>6} {:>8} {:>10} {:>12} {:>11} {:>10} {:>12}",
        "commits",
        "store_rows",
        "store_bytes",
        "probe",
        "rep",
        "deleted",
        "read_objs",
        "read_bytes",
        "cache_rds",
        "list_ops",
        "nanos",
    );

    let mut committed = 0usize;
    for checkpoint in checkpoints {
        {
            // Writer handle: committed, flushed, then dropped so the probe
            // handle below opens against a cold cache.
            let counters = SlateDBIoCounters::default();
            let storage =
                SlateDB::open_with_io_counters(&path, counters.clone()).expect("open SlateDB");
            let session = open_lix()
                .with_storage(storage.clone())
                .await
                .expect("open workspace");
            while committed < checkpoint {
                commit_batch(&session, committed, rows_per_commit).await;
                committed += 1;
            }
            storage.flush().await.expect("flush slatedb");
        }

        // Repository size comes off a DISPOSABLE handle. A full layout scan
        // pulls the whole database into SlateDB's block cache, so running it on
        // the probe handle would warm exactly the cache the probe is supposed
        // to arrive cold to -- which is what the first version of this example
        // did, and it read as "the probe does no object-store IO".
        let (store_rows, store_bytes) = {
            let counters = SlateDBIoCounters::default();
            let storage =
                SlateDB::open_with_io_counters(&path, counters.clone()).expect("open SlateDB");
            let size = store_size(&storage).await;
            drop(storage);
            size
        };

        // Fresh handle, fresh counters: every block this probe needs must be
        // fetched from the object store. Rep 0 is the cold number; later reps
        // measure the warm steady state.
        let counters = SlateDBIoCounters::default();
        let storage =
            SlateDB::open_with_io_counters(&path, counters.clone()).expect("open SlateDB");

        for (label, phantom) in [("phantom", true), ("live", false)] {
            let mut per_space = Vec::new();
            for rep in 0..PROBE_REPS {
                let adapter = StorageAdapter::new(storage.clone());
                let read = adapter
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("open storage snapshot");
                let before = counters.snapshot();
                let probe = probe_hot_generation_planes(&read, &branch, phantom)
                    .await
                    .expect("probe hot generation planes");
                let after = counters.snapshot();
                drop(read);
                let delta = diff(&before, &after);
                println!(
                    "{committed:>8} {store_rows:>12} {store_bytes:>13} {label:>8} {rep:>6} {:>8} {:>10} {:>12} {:>11} {:>10} {:>12}",
                    probe.deleted_rows,
                    delta.0,
                    delta.1,
                    delta.2,
                    delta.3,
                    probe.total_nanos,
                );
                per_space = probe.spaces;
            }
            for space in &per_space {
                println!(
                    "    [{label}] space=0x{:08x} rows={} pages={} open_nanos={} total_nanos={}",
                    space.space_id, space.rows, space.pages, space.open_nanos, space.total_nanos
                );
            }
        }
        let before_check = counters.snapshot();
        let _ = store_size(&storage).await;
        let after_check = counters.snapshot();
        eprintln!(
            "engagement check @{committed}: a full layout scan on the probe handle moved \
             read_objects by {} (read_bytes {}) -- run AFTER the probes so it cannot warm them",
            after_check.read_objects - before_check.read_objects,
            after_check.read_bytes - before_check.read_bytes,
        );
        drop(storage);
    }
}

/// `(read_objects, read_bytes, cache_filesystem_reads, list_operations)`.
fn diff(before: &SlateDBIoSnapshot, after: &SlateDBIoSnapshot) -> (u64, u64, u64, u64) {
    (
        after.read_objects.saturating_sub(before.read_objects),
        after.read_bytes.saturating_sub(before.read_bytes),
        after
            .cache_filesystem_reads
            .saturating_sub(before.cache_filesystem_reads),
        after.list_operations.saturating_sub(before.list_operations),
    )
}

async fn pick_branch<S>(storage: &S) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let adapter = StorageAdapter::new(storage.clone());
    let read = adapter
        .begin_read(StorageReadOptions::default())
        .await
        .expect("open storage snapshot");
    let branches = hot_generation_branches(&read)
        .await
        .expect("enumerate branch controls");
    drop(read);
    eprintln!("branch controls: {branches:?}");
    branches
        .iter()
        .find(|branch_id| branch_id.as_str() != lix::GLOBAL_BRANCH_ID)
        .or_else(|| branches.first())
        .expect("at least one branch control")
        .clone()
}

async fn store_size<S>(storage: &S) -> (u64, u64)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let adapter = StorageAdapter::new(storage.clone());
    let read = adapter
        .begin_read(StorageReadOptions::default())
        .await
        .expect("open storage snapshot");
    let accounting = layout_accounting(&read).await;
    let rows = accounting.iter().map(|entry| entry.rows).sum();
    let bytes = accounting
        .iter()
        .map(|entry| entry.key_bytes + entry.value_bytes)
        .sum();
    drop(read);
    (rows, bytes)
}

async fn commit_batch<S>(session: &Lix<S>, batch: usize, rows: usize)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let mut transaction = session.begin_transaction().await.expect("begin commit");
    for index in 0..rows {
        transaction
            .execute(
                "INSERT INTO e53_locality (path, value) VALUES ($1, CAST($2 AS JSONB))",
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
        "x-lix-key": "e53_locality",
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
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (CAST($1 AS JSONB), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register schema");
}
