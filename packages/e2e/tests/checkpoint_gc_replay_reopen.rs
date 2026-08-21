#![recursion_limit = "256"]

use std::future::Future;
use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use lix::storage::Storage;
use lix::{Value, open_lix};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;

const CHECKPOINT_GC_INTERVAL: usize = 64;
const ROW_COUNT: usize = 100;
const SCHEMA_KEY: &str = "checkpoint_gc_replay_row";

#[async_trait]
trait ReopenStorage: Storage + Clone + Send + Sync + Sized + 'static {
    fn open(path: &Path) -> Self;
    async fn flush(&self);
}

#[async_trait]
impl ReopenStorage for RocksDB {
    fn open(path: &Path) -> Self {
        Self::open(path).expect("open checkpoint-GC RocksDB fixture")
    }

    async fn flush(&self) {
        self.flush().expect("flush checkpoint-GC RocksDB fixture");
    }
}

#[async_trait]
impl ReopenStorage for SlateDB {
    fn open(path: &Path) -> Self {
        Self::open(path).expect("open checkpoint-GC SlateDB fixture")
    }

    async fn flush(&self) {
        self.flush()
            .await
            .expect("flush checkpoint-GC SlateDB fixture");
    }
}

#[test]
fn rocksdb_checkpoint_gc_retains_replay_and_selected_owners_after_reopen() {
    run_on_large_stack(|| {
        checkpoint_gc_retains_replay_and_selected_owners_after_reopen::<RocksDB>()
    });
}

#[test]
fn slatedb_checkpoint_gc_retains_replay_and_selected_owners_after_reopen() {
    run_on_large_stack(|| {
        checkpoint_gc_retains_replay_and_selected_owners_after_reopen::<SlateDB>()
    });
}

fn run_on_large_stack<F, Fut>(make_future: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + 'static,
{
    std::thread::Builder::new()
        .name("checkpoint-gc-replay".to_owned())
        .stack_size(16 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build checkpoint-GC test runtime")
                .block_on(make_future());
        })
        .expect("spawn checkpoint-GC test thread")
        .join()
        .expect("checkpoint-GC test thread should not panic");
}

async fn checkpoint_gc_retains_replay_and_selected_owners_after_reopen<S: ReopenStorage>() {
    let temp = tempfile::tempdir().expect("create durable checkpoint-GC fixture");
    let path = temp.path().join("database");
    {
        let storage = S::open(&path);
        let lix = open_lix()
            .with_storage(storage.clone())
            .await
            .expect("initialize durable checkpoint-GC Lix");
        register_schema(&lix).await;
        seed_rows(&lix).await;
        let _seed_head = newest_commit_id(&lix).await;
        churn_once(&lix, 1, false).await;
        let _first_head = newest_commit_id(&lix).await;
        churn_once(&lix, 2, true).await;
        let superseded_head = newest_commit_id(&lix).await;

        lix.undo().await.expect("undo second churn commit");
        assert_generation(&lix, 1, false).await;
        let redo = lix.redo().await.expect("redo second churn commit");
        assert_eq!(redo.target_commit_id, superseded_head);
        let replay_owner = redo.replay_commit_id;
        assert_generation(&lix, 2, true).await;

        let compacted_owner = lix
            .create_checkpoint()
            .await
            .expect("create compacting checkpoint")
            .commit_id;
        for _ in 1..CHECKPOINT_GC_INTERVAL {
            lix.create_checkpoint()
                .await
                .expect("advance production checkpoint-GC cadence");
        }
        // The 64th checkpoint schedules production GC. Corrected undo/redo
        // publishes a new canonical current-state owner, and the first
        // checkpoint then compacts that replayed state. The pre-undo target and
        // replay publication may retire; the compacted checkpoint remains an
        // authenticated history/root owner through the later queue cadence.
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_ne!(replay_owner, compacted_owner);
        assert_commit_present(&lix, &compacted_owner).await;
        assert_generation(&lix, 2, true).await;
        assert_history_readable(&lix).await;

        lix.close().await.expect("close checkpoint-GC Lix");
        drop(lix);
        storage.flush().await;
    }

    let storage = S::open(&path);
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("cold reopen checkpoint-GC Lix");
    assert_generation(&lix, 2, true).await;
    assert_history_readable(&lix).await;
    lix.close().await.expect("close reopened checkpoint-GC Lix");
    drop(lix);
    storage.flush().await;
}

async fn register_schema<S: Storage + Clone + Send + Sync + 'static>(lix: &lix::Lix<S>) {
    let schema = serde_json::json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": SCHEMA_KEY,
        "columns": [
            { "name": "id", "type": "text", "nullable": false },
            { "name": "indexed_value", "type": "text", "nullable": false },
            { "name": "note", "type": "text", "nullable": false },
            { "name": "generation", "type": "int8", "nullable": false },
        ],
        "primary_key": ["id"],
        "unique": [["indexed_value"]],
    });
    lix.execute(
        "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (CAST($1 AS JSONB), false, false)",
        &[Value::Text(schema.to_string())],
    )
    .await
    .expect("register checkpoint-GC schema");
}

async fn seed_rows<S: Storage + Clone + Send + Sync + 'static>(lix: &lix::Lix<S>) {
    let mut transaction = lix
        .begin_transaction()
        .await
        .expect("begin checkpoint-GC seed");
    for index in 0..ROW_COUNT {
        transaction
            .execute(
                &format!(
                    "INSERT INTO {SCHEMA_KEY} (id, indexed_value, note, generation) VALUES ($1, $2, $3, 0)"
                ),
                &[
                    Value::Text(format!("row-{index}")),
                    Value::Text(format!("indexed-{index}-0")),
                    Value::Text(format!("seed-{index}")),
                ],
            )
            .await
            .expect("seed checkpoint-GC row");
    }
    transaction
        .commit()
        .await
        .expect("commit checkpoint-GC seed");
}

async fn churn_once<S: Storage + Clone + Send + Sync + 'static>(
    lix: &lix::Lix<S>,
    generation: usize,
    insert_replacement: bool,
) {
    let mut transaction = lix
        .begin_transaction()
        .await
        .expect("begin checkpoint-GC churn");
    transaction
        .execute(
            &format!(
                "UPDATE {SCHEMA_KEY} SET indexed_value = $1, generation = $2 WHERE id = 'row-0'"
            ),
            &[
                Value::Text(format!("indexed-0-{generation}")),
                Value::Integer(generation as i64),
            ],
        )
        .await
        .expect("update indexed checkpoint-GC row");
    transaction
        .execute(
            &format!("UPDATE {SCHEMA_KEY} SET note = $1, generation = $2 WHERE id = 'row-1'"),
            &[
                Value::Text(if generation == 1 { "one" } else { "two" }.to_owned()),
                Value::Integer(generation as i64),
            ],
        )
        .await
        .expect("update nonindexed checkpoint-GC row");
    if insert_replacement {
        transaction
            .execute(
                &format!(
                    "INSERT INTO {SCHEMA_KEY} (id, indexed_value, note, generation) VALUES ('row-99', 'indexed-99-2', 'replacement', 2)"
                ),
                &[],
            )
            .await
            .expect("insert checkpoint-GC replacement row");
    } else {
        transaction
            .execute(
                &format!("DELETE FROM {SCHEMA_KEY} WHERE id = 'row-99'"),
                &[],
            )
            .await
            .expect("delete checkpoint-GC replacement row");
    }
    transaction
        .commit()
        .await
        .expect("commit checkpoint-GC churn");
}

async fn newest_commit_id<S: Storage + Clone + Send + Sync + 'static>(lix: &lix::Lix<S>) -> String {
    let result = lix
        .execute("SELECT id FROM lix_commit ORDER BY id DESC LIMIT 1", &[])
        .await
        .expect("load newest checkpoint-GC commit");
    result.rows()[0]
        .get::<String>("id")
        .expect("decode newest checkpoint-GC commit")
}

async fn assert_commit_present<S: Storage + Clone + Send + Sync + 'static>(
    lix: &lix::Lix<S>,
    commit_id: &str,
) {
    let result = lix
        .execute(
            &format!("SELECT id FROM lix_commit WHERE id = '{commit_id}'"),
            &[],
        )
        .await
        .expect("query retained checkpoint-GC commit");
    assert!(
        !result.is_empty(),
        "live state owner {commit_id} was reclaimed"
    );
}

async fn assert_generation<S: Storage + Clone + Send + Sync + 'static>(
    lix: &lix::Lix<S>,
    generation: i64,
    replacement_present: bool,
) {
    let state = lix
        .execute(
            &format!("SELECT indexed_value, note, generation FROM {SCHEMA_KEY} WHERE id = 'row-1'"),
            &[],
        )
        .await
        .expect("read checkpoint-GC live state");
    assert_eq!(
        state.rows()[0].get::<i64>("generation").unwrap(),
        generation
    );
    assert_eq!(
        state.rows()[0].get::<String>("note").unwrap(),
        if generation == 1 { "one" } else { "two" }
    );
    let replacement = lix
        .execute(
            &format!("SELECT id FROM {SCHEMA_KEY} WHERE id = 'row-99'"),
            &[],
        )
        .await
        .expect("read checkpoint-GC replacement row");
    assert_eq!(!replacement.is_empty(), replacement_present);
}

async fn assert_history_readable<S: Storage + Clone + Send + Sync + 'static>(lix: &lix::Lix<S>) {
    let history = lix
        .execute(
            &format!("SELECT note FROM lix_history('{SCHEMA_KEY}') WHERE id = 'row-1'"),
            &[],
        )
        .await
        .expect("read compacted checkpoint-GC history");
    assert!(!history.is_empty());
}
