//! Experiment-Q attribution driver for `tracked_state.tree_chunk` growth.
//!
//! Runs ordinary SQL commits through the real `SessionContext` commit path
//! with `LIX_TREE_CHUNK_TRACE=1` set in the lix crate, printing commit
//! boundary markers to stderr so the per-chunk trace lines emitted by the
//! tree writer and the RocksDB adapter can be grouped per commit.
//!
//! Usage:
//!   tree_chunk_trace <mode> <commits> <rows_per_commit> <initial_rows> [gc]
//!   mode: insert  — append new rows each commit (space_growth workload)
//!         update  — update existing rows each commit (spread across the pk range)
//!
//! With `gc` as the trailing argument the example publishes a checkpoint,
//! runs one repository GC pass, and reports tree_chunk rows/bytes before and
//! after so dead-chunk share at steady state is measurable.

use lix::Value;
use lix::integration::{Engine, SessionContext};
use lix::storage::Storage;
use lix::storage_adapter::{StorageAdapter, StorageReadOptions};
use lix::storage_bench::{collect_repository_gc_for_bench, layout_accounting};
use lix_storage_rocksdb::RocksDB;

const TREE_CHUNK_SPACE: &str = "tracked_state.tree_chunk";

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| "insert".to_owned());
    let commits = args
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(20);
    let rows_per_commit = args
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1);
    let initial_rows = args
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(2000);
    let run_gc = args.next().as_deref() == Some("gc");
    assert!(matches!(mode.as_str(), "insert" | "update"), "bad mode");

    let directory = tempfile::tempdir().expect("create RocksDB directory");
    let storage = RocksDB::open(directory.path()).expect("open RocksDB");
    Engine::initialize(storage.clone())
        .await
        .expect("initialize repository");
    let engine = Engine::new(storage.clone()).await.expect("open engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("open workspace");
    register_schema(&session).await;

    // Seed phase: one bulk commit so measured commits run against a
    // realistically sized tree.
    eprintln!("TCTRACE seed_begin rows={initial_rows}");
    if initial_rows > 0 {
        let mut transaction = session.begin_transaction().await.expect("begin seed");
        for index in 0..initial_rows {
            transaction
                .execute(
                    "INSERT INTO tree_trace (path, value) VALUES ($1, lix_json($2))",
                    &[
                        Value::Text(row_path(0, index)),
                        Value::Text(format!(r#"{{"seed":{index}}}"#)),
                    ],
                )
                .await
                .expect("insert seed row");
        }
        transaction.commit().await.expect("commit seed batch");
    }
    eprintln!("TCTRACE seed_end");

    let before = tree_chunk_usage(&storage).await;
    println!(
        "before_measured tree_chunk rows={} bytes={}",
        before.0, before.1
    );

    for commit in 0..commits {
        eprintln!("TCTRACE commit_begin {commit}");
        let mut transaction = session.begin_transaction().await.expect("begin commit");
        for index in 0..rows_per_commit {
            match mode.as_str() {
                "insert" => {
                    transaction
                        .execute(
                            "INSERT INTO tree_trace (path, value) VALUES ($1, lix_json($2))",
                            &[
                                Value::Text(row_path(commit + 1, index)),
                                Value::Text(format!(r#"{{"batch":{commit},"index":{index}}}"#)),
                            ],
                        )
                        .await
                        .expect("insert row");
                }
                _ => {
                    // Deterministic spread over the seeded pk range.
                    let target = (index * 7919 + commit * 104_729) % initial_rows.max(1);
                    transaction
                        .execute(
                            "UPDATE tree_trace SET value = lix_json($1) WHERE path = $2",
                            &[
                                Value::Text(format!(r#"{{"commit":{commit},"index":{index}}}"#)),
                                Value::Text(row_path(0, target)),
                            ],
                        )
                        .await
                        .expect("update row");
                }
            }
        }
        transaction.commit().await.expect("commit batch");
        eprintln!("TCTRACE commit_end {commit}");
    }

    let after = tree_chunk_usage(&storage).await;
    let growth =
        (after.1.saturating_sub(before.1)) as f64 / commits.max(1) as f64;
    println!(
        "after_measured tree_chunk rows={} bytes={} bytes_per_commit={growth:.1}",
        after.0, after.1
    );

    if run_gc {
        eprintln!("TCTRACE gc_begin");
        session
            .create_checkpoint()
            .await
            .expect("publish checkpoint");
        let adapter = StorageAdapter::new(storage.clone());
        let sweep = collect_repository_gc_for_bench(&adapter)
            .await
            .expect("repository GC should commit");
        eprintln!("TCTRACE gc_end");
        let collected = tree_chunk_usage(&storage).await;
        println!(
            "after_gc tree_chunk rows={} bytes={} swept_commits={} staged_deletes={}",
            collected.0, collected.1, sweep.swept_commits, sweep.staged_deletes
        );
        let live_share = collected.1 as f64 / after.1.max(1) as f64;
        println!("gc_summary live_bytes_share={live_share:.4}");
    }
}

fn row_path(batch: usize, index: usize) -> String {
    format!("/row/{batch:08}/{index:08}")
}

async fn tree_chunk_usage<S>(storage: &S) -> (u64, u64)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let adapter = StorageAdapter::new(storage.clone());
    let read = adapter
        .begin_read(StorageReadOptions::default())
        .await
        .expect("open storage snapshot");
    let accounting = layout_accounting(&read).await;
    drop(read);
    accounting
        .into_iter()
        .find(|entry| entry.space == TREE_CHUNK_SPACE)
        .map(|entry| (entry.rows, entry.key_bytes + entry.value_bytes))
        .unwrap_or((0, 0))
}

async fn register_schema<S>(session: &SessionContext<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let schema = serde_json::json!({
        "x-lix-key": "tree_trace",
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
