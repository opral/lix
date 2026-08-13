use std::path::Path;

use async_trait::async_trait;
use lix::storage::Storage;
use lix::{ExecuteBatchStatement, Value, open_lix};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;

const ROW_COUNT: usize = 512;
const SCHEMA_KEY: &str = "certified_replacement_delete_probe";

#[async_trait]
trait ReopenStorage: Storage + Clone + Send + Sync + Sized + 'static {
    fn open(path: &Path) -> Self;
    async fn flush(&self);
}

#[async_trait]
impl ReopenStorage for RocksDB {
    fn open(path: &Path) -> Self {
        Self::open(path).expect("open RocksDB fixture")
    }

    async fn flush(&self) {
        self.flush().expect("flush RocksDB fixture");
    }
}

#[async_trait]
impl ReopenStorage for SlateDB {
    fn open(path: &Path) -> Self {
        Self::open(path).expect("open SlateDB fixture")
    }

    async fn flush(&self) {
        self.flush().await.expect("flush SlateDB fixture");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rocksdb_reopens_after_certified_replacement_delete_checkpoint() {
    replacement_delete_checkpoint_reopens::<RocksDB>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slatedb_reopens_after_certified_replacement_delete_checkpoint() {
    replacement_delete_checkpoint_reopens::<SlateDB>().await;
}

async fn replacement_delete_checkpoint_reopens<S: ReopenStorage>() {
    let temp = tempfile::tempdir().expect("create durable replacement fixture");
    let path = temp.path().join("database");
    {
        let storage = S::open(&path);
        let lix = open_lix()
            .with_storage(storage.clone())
            .await
            .expect("initialize durable Lix");
        register_schema(&lix).await;

        let inserts = (0..ROW_COUNT)
            .map(|index| ExecuteBatchStatement {
                label: None,
                sql: format!(
                    "INSERT INTO {SCHEMA_KEY} (path, value) VALUES ($1, CAST($2 AS JSONB))"
                ),
                params: vec![
                    Value::Text(format!("/{index:04}")),
                    Value::Text(format!(r#"{{"generation":0,"index":{index}}}"#)),
                ],
            })
            .collect::<Vec<_>>();
        let inserted = lix
            .execute_batch(&inserts)
            .await
            .expect("insert complete collection");
        assert_eq!(
            inserted
                .iter()
                .map(|result| result.rows_affected())
                .sum::<u64>(),
            ROW_COUNT as u64
        );

        let replacements = (0..ROW_COUNT)
            .map(|index| ExecuteBatchStatement {
                label: None,
                sql: format!("UPDATE {SCHEMA_KEY} SET value = CAST($1 AS JSONB) WHERE path = $2"),
                params: vec![
                    Value::Text(format!(r#"{{"generation":1,"index":{index}}}"#)),
                    Value::Text(format!("/{index:04}")),
                ],
            })
            .collect::<Vec<_>>();
        let replaced = lix
            .execute_batch(&replacements)
            .await
            .expect("replace complete collection");
        assert_eq!(
            replaced
                .iter()
                .map(|result| result.rows_affected())
                .sum::<u64>(),
            ROW_COUNT as u64
        );

        assert_eq!(
            lix.execute(&format!("DELETE FROM {SCHEMA_KEY}"), &[])
                .await
                .expect("delete replacement collection")
                .rows_affected(),
            ROW_COUNT as u64
        );
        lix.create_checkpoint()
            .await
            .expect("checkpoint deleted replacement collection");
        assert_collection_empty(&lix).await;

        lix.close().await.expect("close durable Lix");
        drop(lix);
        storage.flush().await;
    }

    let storage = S::open(&path);
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("reopen durable Lix");
    assert_collection_empty(&lix).await;
    let checkpoints = lix
        .execute("SELECT COUNT(*) AS count FROM lix_checkpoint", &[])
        .await
        .expect("read checkpoints after reopen");
    assert!(checkpoints.rows()[0].get::<i64>("count").unwrap() >= 1);
    lix.close().await.expect("close reopened durable Lix");
    drop(lix);
    storage.flush().await;
}

async fn register_schema<S: Storage + Clone + Send + Sync + 'static>(lix: &lix::Lix<S>) {
    let schema = serde_json::json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": SCHEMA_KEY,
        "columns": [
            { "name": "path", "type": "text", "nullable": false },
            { "name": "value", "type": "jsonb", "nullable": false }
        ],
        "primary_key": ["path"]
    });
    lix.execute(
        "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
        &[Value::Text(schema.to_string())],
    )
    .await
    .expect("register replacement schema");
}

async fn assert_collection_empty<S: Storage + Clone + Send + Sync + 'static>(lix: &lix::Lix<S>) {
    let count = lix
        .execute(&format!("SELECT COUNT(*) AS count FROM {SCHEMA_KEY}"), &[])
        .await
        .expect("read replacement collection count");
    assert_eq!(count.rows()[0].get::<i64>("count").unwrap(), 0);
}
