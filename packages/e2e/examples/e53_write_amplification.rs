//! Does a commit's physical write cost grow with repository size?
//!
//! A probe replay of `stage_retire_hot_generation` against a branch's live
//! generation found it would delete the branch's *entire* row set -- 8 026 rows
//! at 800 commits. That is a measurement of how many rows one generation holds.
//! It is **not** a measurement of how often a generation is retired, and the
//! difference decides whether ordinary commits are O(repository) or O(batch):
//! `stage_retire_hot_generation` is called only when a publication moves the
//! branch's `tracked_generation`, so a lane that never rotates the generation
//! pays nothing.
//!
//! This measures the real commit lane directly, per commit:
//!
//! * `staged_puts` / `staged_deletes` / `written_bytes` from the write set
//! * real `stage_retire_hot_generation` invocations and rows they deleted
//! * which packed-current-base publication route fired, if any
//!
//! `PACKED_CURRENT_BASE_MIN_ROWS` is a floor on the rows staged by a *single
//! transaction*, so `--rows-per-commit` sweeps across it decide whether the
//! packed base is unreachable in ordinary incremental use or merely unused by
//! small commits.
//!
//! Usage: `e53_write_amplification [rows_per_commit] [commits] [report_every]`

use lix::Value;
use lix::integration::{Engine, SessionContext};
use lix::storage::Storage;
use lix::storage_adapter::{StorageAdapter, StorageReadOptions};
use lix::storage_bench::{
    PACKED_CURRENT_BASE_MIN_ROWS_VALUE, hot_generation_branches, layout_accounting,
    probe_hot_generation_planes, take_crud_physical_write_accounting, take_hot_retire_invocations,
    take_packed_base_publication_census,
};
use lix_storage_rocksdb::RocksDB;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut args = std::env::args().skip(1);
    let rows_per_commit = args
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10);
    let commits = args
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(400);
    let report_every = args
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(50);

    let directory = tempfile::tempdir().expect("create RocksDB directory");
    let storage = RocksDB::open(directory.path()).expect("open RocksDB");
    Engine::initialize(storage.clone())
        .await
        .expect("initialize repository");
    let engine = Engine::new(storage.clone()).await.expect("open engine");
    let session = engine.open_session().await.expect("open workspace");
    register_schema(&session).await;

    println!(
        "e53_write_amplification rows_per_commit={rows_per_commit} commits={commits} \
         packed_min_rows={PACKED_CURRENT_BASE_MIN_ROWS_VALUE} backend=rocksdb checkpoints=none"
    );
    println!(
        "{:>8} {:>12} {:>12} {:>10} {:>10} {:>10} {:>12} {:>8} {:>12} {:>8} {:>8} {:>8} {:>12}",
        "commit",
        "store_rows",
        "store_bytes",
        "grew_bytes",
        "puts",
        "deletes",
        "wr_bytes",
        "retires",
        "retired_rws",
        "pk_ord",
        "pk_col",
        "pk_rep",
        "nanos",
    );

    // Drain anything the schema registration left in the process-global counters.
    let _ = take_crud_physical_write_accounting();
    let _ = take_hot_retire_invocations();
    let _ = take_packed_base_publication_census();
    let mut last_bytes = 0_u64;

    for index in 0..commits {
        let start = std::time::Instant::now();
        commit_batch(&session, index, rows_per_commit).await;
        let nanos = start.elapsed().as_nanos();

        let physical = take_crud_physical_write_accounting();
        let (retires, retired_rows) = take_hot_retire_invocations();
        let packed = take_packed_base_publication_census();

        let report = index + 1 == commits
            || (index + 1) % report_every == 0
            || index < 3
            || retires > 0
            || packed.ordered + packed.certified_columnar + packed.complete_replacement > 0;
        if report {
            let (store_rows, store_bytes) = store_rows(&storage).await;
            let grew = store_bytes.saturating_sub(last_bytes);
            last_bytes = store_bytes;
            println!(
                "{:>8} {store_rows:>12} {store_bytes:>12} {grew:>10} {:>10} {:>10} {:>12} {retires:>8} {retired_rows:>12} {:>8} {:>8} {:>8} {nanos:>12}",
                index + 1,
                physical.puts,
                physical.deletes,
                physical.written_bytes,
                packed.ordered,
                packed.certified_columnar,
                packed.complete_replacement,
            );
        }
    }

    // ENGAGEMENT CHECK. "retires = 0" is only a fact about the commit lane if
    // the counter is capable of moving in this process. Force one real
    // `stage_retire_hot_generation` through the probe and read it back; if this
    // prints 0 the counter is dead and every zero above is meaningless.
    let adapter = StorageAdapter::new(storage.clone());
    let read = adapter
        .begin_read(StorageReadOptions::default())
        .await
        .expect("open storage snapshot");
    let branches = hot_generation_branches(&read)
        .await
        .expect("enumerate branch controls");
    let branch = branches
        .iter()
        .find(|branch_id| branch_id.as_str() != lix::GLOBAL_BRANCH_ID)
        .or_else(|| branches.first())
        .expect("at least one branch control")
        .clone();
    let _ = take_hot_retire_invocations();
    let probe = probe_hot_generation_planes(&read, &branch, false)
        .await
        .expect("probe hot generation planes");
    drop(read);
    let (forced_calls, forced_rows) = take_hot_retire_invocations();
    println!(
        "engagement check: forcing one retire moved the counter to calls={forced_calls} \
         rows={forced_rows} (probe reported deleted={}) -- if calls==0 the zeros above are a dead \
         instrument, not a result",
        probe.deleted_rows
    );
}

async fn store_rows<S>(storage: &S) -> (u64, u64)
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
                "INSERT INTO e53_amp (path, value) VALUES ($1, lix_json($2))",
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
        "x-lix-key": "e53_amp",
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
