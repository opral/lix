//! Per-space physical growth across a real multi-commit history.
//!
//! `storage_layout` reports one snapshot of a fixture that some other tool
//! built, and the layout table in `tracked_state_crud` reports a single
//! commit. Neither answers the question a space-id ordering argument actually
//! rests on: *which spaces grow per commit*, and by how much.
//!
//! This example drives N ordinary SQL commits through the real
//! `SessionContext` commit path and reports every space's row and byte count
//! after commit 1 and after commit N, plus the per-commit delta. A space whose
//! delta is zero or a handful of bytes is not a high-volume plane no matter how
//! many rows it eventually accumulates.
//!
//! Usage: `space_growth [commits] [rows_per_commit]` (defaults: 200 commits,
//! 10 rows).

use std::collections::BTreeMap;

use lix::Value;
use lix::storage::Storage;
use lix::storage_adapter::{StorageAdapter, StorageReadOptions};
use lix::storage_bench::layout_accounting;
use lix::{Lix, open_lix};
use lix_storage_rocksdb::RocksDB;

#[derive(Clone, Copy, Default)]
struct SpaceUsage {
    rows: u64,
    key_bytes: u64,
    value_bytes: u64,
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let commits = args
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(200);
    let rows_per_commit = args
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10);
    assert!(commits >= 2, "need at least two commits to measure growth");

    let directory = tempfile::tempdir().expect("create RocksDB directory");
    let storage = RocksDB::open(directory.path()).expect("open RocksDB");
    open_lix()
        .with_storage(storage.clone())
        .await
        .expect("initialize repository");
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open lix");
    let session = lix.open_another_session().await.expect("open workspace");
    register_schema(&session).await;

    commit_batch(&session, 0, rows_per_commit).await;
    let first = usage(&storage).await;
    for index in 1..commits {
        commit_batch(&session, index, rows_per_commit).await;
    }
    let last = usage(&storage).await;

    println!("space_growth commits={commits} rows_per_commit={rows_per_commit}");
    println!(
        "{:<12} {:<52} {:>8} {:>12} {:>12} {:>12} {:>14}",
        "space_id", "space", "rows_1", "rows_n", "bytes_1", "bytes_n", "bytes_per_commit"
    );
    let mut spaces = first.keys().copied().collect::<Vec<_>>();
    spaces.extend(last.keys().copied());
    spaces.sort_unstable();
    spaces.dedup();
    for (id, name) in spaces {
        let one = first.get(&(id, name)).copied().unwrap_or_default();
        let many = last.get(&(id, name)).copied().unwrap_or_default();
        let bytes_one = one.key_bytes + one.value_bytes;
        let bytes_many = many.key_bytes + many.value_bytes;
        if bytes_many == 0 {
            continue;
        }
        let per_commit =
            (bytes_many.saturating_sub(bytes_one)) as f64 / (commits.saturating_sub(1)) as f64;
        println!(
            "0x{id:08x}   {name:<52} {:>8} {:>12} {:>12} {:>12} {per_commit:>14.1}",
            one.rows, many.rows, bytes_one, bytes_many
        );
    }
}

async fn usage<S>(storage: &S) -> BTreeMap<(u32, &'static str), SpaceUsage>
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
                "INSERT INTO space_growth (path, value) VALUES ($1, lix_json($2))",
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
        "x-lix-key": "space_growth",
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
