//! Does retiring a *fixed* amount of hot-generation garbage cost more as the
//! repository grows?
//!
//! All ~41 storage spaces share one flat physical keyspace: `encode_physical_key`
//! writes a four-byte space id in front of every logical key, and on RocksDB
//! every mutable space lives in one column family. So the space id is the
//! outermost sort dimension of the whole store, and the eight
//! `GENERATION_SCOPED_SPACES` that make up one branch generation's serving
//! planes sit in eight regions separated by the entire contents of the
//! intervening spaces.
//!
//! `stage_retire_hot_generation` therefore performs eight independent prefix
//! seeks to retire one generation. This example measures what those eight seeks
//! cost, and whether that cost grows with total repository size.
//!
//! Two probes per checkpoint:
//!
//! * `phantom` -- the same eight scans against a generation uuid no row can
//!   carry. Garbage is exactly zero by construction, so every block the engine
//!   fetches is the fixed cost of *positioning* eight iterators. This is the
//!   falsifiable curve: fix the garbage at zero, grow the repository, and see
//!   whether the cost moves.
//! * `live` -- the same eight scans against the branch's live generation, which
//!   is also the multi-plane hot read over ROW/FILE/INDEX/PACKED_CURRENT_BASE
//!   for one `(branch, generation)`.
//!
//! Block fetches are counted with RocksDB's thread-local perf context, which is
//! why this runs on a `current_thread` runtime.
//!
//! Usage: `e53_hot_locality [rows_per_commit] [checkpoint...]`
//! Default checkpoints: 25 50 100 200 400 800.

use lix::Value;
use lix::integration::{Engine, SessionContext};
use lix::storage::Storage;
use lix::storage_adapter::{StorageAdapter, StorageReadOptions};
use lix::storage_bench::{hot_generation_branches, layout_accounting, probe_hot_generation_planes};
use lix_storage_rocksdb::{BlockFetchCounters, PerfProbe, RocksDB};

const PROBE_REPS: usize = 3;

#[tokio::main(flavor = "current_thread")]
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
        checkpoints = vec![25, 50, 100, 200, 400, 800];
    }
    checkpoints.sort_unstable();

    let directory = tempfile::tempdir().expect("create RocksDB directory");
    let storage = RocksDB::open(directory.path()).expect("open RocksDB");
    Engine::initialize(storage.clone())
        .await
        .expect("initialize repository");
    let engine = Engine::new(storage.clone()).await.expect("open engine");
    let session = engine.open_session().await.expect("open workspace");
    register_schema(&session).await;

    let mut perf = PerfProbe::new();

    let branch = pick_branch(&storage).await;
    println!(
        "e53_hot_locality rows_per_commit={rows_per_commit} probe_reps={PROBE_REPS} branch={branch}"
    );
    println!(
        "{:>8} {:>12} {:>13} {:>6} {:>8} {:>10} {:>10} {:>12} {:>12} {:>12}",
        "commits",
        "store_rows",
        "store_bytes",
        "probe",
        "deleted",
        "fetches",
        "reads",
        "read_bytes",
        "key_cmps",
        "nanos",
    );

    let mut committed = 0usize;
    for checkpoint in checkpoints {
        while committed < checkpoint {
            commit_batch(&session, committed, rows_per_commit).await;
            committed += 1;
        }
        // Retire measurements must see SSTs, not a memtable.
        storage.flush().expect("flush rocksdb");

        let (store_rows, store_bytes) = store_size(&storage).await;

        for (label, phantom) in [("phantom", true), ("live", false)] {
            let mut last = (0_u64, 0_u64, BlockFetchCounters::default());
            let mut per_space = Vec::new();
            for _ in 0..PROBE_REPS {
                let adapter = StorageAdapter::new(storage.clone());
                let read = adapter
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("open storage snapshot");
                perf.reset();
                let probe = probe_hot_generation_planes(&read, &branch, phantom)
                    .await
                    .expect("probe hot generation planes");
                let counters = perf.read();
                drop(read);
                last = (probe.deleted_rows, probe.total_nanos, counters);
                per_space = probe.spaces;
            }
            let (deleted, nanos, counters) = last;
            println!(
                "{committed:>8} {store_rows:>12} {store_bytes:>13} {label:>6} {deleted:>8} {:>10} {:>10} {:>12} {:>12} {nanos:>12}",
                counters.block_fetches(),
                counters.block_reads,
                counters.block_read_bytes,
                counters.user_key_comparisons,
            );
            if label == "phantom" {
                for space in &per_space {
                    println!(
                        "    space=0x{:08x} rows={} pages={} open_nanos={} total_nanos={}",
                        space.space_id, space.rows, space.pages, space.open_nanos, space.total_nanos
                    );
                }
            } else {
                for space in &per_space {
                    println!(
                        "  L space=0x{:08x} rows={} pages={} open_nanos={} total_nanos={}",
                        space.space_id, space.rows, space.pages, space.open_nanos, space.total_nanos
                    );
                }
            }
        }
    }
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

async fn commit_batch<S>(session: &SessionContext<S>, batch: usize, rows: usize)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let mut transaction = session.begin_transaction().await.expect("begin commit");
    for index in 0..rows {
        transaction
            .execute(
                "INSERT INTO e53_locality (path, value) VALUES ($1, lix_json($2))",
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

async fn register_schema<S>(session: &SessionContext<S>)
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
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (lix_json($1), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register schema");
}
