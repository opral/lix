use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use lix::storage::Storage;
use lix::{PreparedDmlParameterBatch, Value, open_lix};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;

// Cross one authenticated 256-entry page boundary without making the debug
// test future itself dominate the platform test-thread stack.
const ROW_COUNT: usize = 257;
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

#[test]
fn rocksdb_reopens_after_certified_replacement_delete_checkpoint() {
    run_reopen_test::<RocksDB>();
}

#[test]
fn slatedb_reopens_after_certified_replacement_delete_checkpoint() {
    run_reopen_test::<SlateDB>();
}

fn run_reopen_test<S: ReopenStorage>() {
    std::thread::Builder::new()
        .name("certified-replacement-reopen".to_string())
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_stack_size(8 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("build replacement test runtime")
                .block_on(replacement_delete_checkpoint_reopens::<S>());
        })
        .expect("spawn replacement test thread")
        .join()
        .expect("replacement test thread completes");
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

        let inserts = PreparedDmlParameterBatch::from_rows((0..ROW_COUNT).map(|index| {
            vec![
                Value::Text(format!("/{index:04}")),
                Value::Text(format!(r#"{{"generation":0,"index":{index}}}"#)),
            ]
        }))
        .expect("build insert parameter batch");
        let inserted = lix
            .execute_prepared_dml_batch(
                Arc::from(format!(
                    "INSERT INTO {SCHEMA_KEY} (path, value) VALUES ($1, lix_json($2))"
                )),
                inserts,
            )
            .await
            .expect("insert complete collection");
        assert_eq!(
            inserted
                .iter()
                .map(|result| result.rows_affected())
                .sum::<u64>(),
            ROW_COUNT as u64
        );

        let replacements = PreparedDmlParameterBatch::from_rows((0..ROW_COUNT).map(|index| {
            vec![
                Value::Text(format!(r#"{{"generation":1,"index":{index}}}"#)),
                Value::Text(format!("/{index:04}")),
            ]
        }))
        .expect("build replacement parameter batch");
        let replaced = lix
            .execute_prepared_dml_batch(
                Arc::from(format!(
                    "UPDATE {SCHEMA_KEY} SET value = lix_json($1) WHERE path = $2"
                )),
                replacements,
            )
            .await
            .expect("replace complete collection");
        assert_eq!(
            replaced
                .iter()
                .map(|result| result.rows_affected())
                .sum::<u64>(),
            ROW_COUNT as u64
        );
        assert_schema_history(&lix, ROW_COUNT * 2).await;

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
    assert_schema_history(&lix, ROW_COUNT * 2).await;
    let checkpoints = lix
        .execute("SELECT COUNT(*) AS count FROM lix_checkpoint", &[])
        .await
        .expect("read checkpoints after reopen");
    assert!(checkpoints.rows()[0].get::<i64>("count").unwrap() >= 1);
    lix.close().await.expect("close reopened durable Lix");
    drop(lix);
    storage.flush().await;
}

async fn assert_schema_history<S: Storage + Clone + Send + Sync + 'static>(
    lix: &lix::Lix<S>,
    minimum: usize,
) {
    let rows = lix
        .execute(
            "SELECT id FROM lix_change WHERE schema_key = $1 ORDER BY id",
            &[Value::Text(SCHEMA_KEY.to_string())],
        )
        .await
        .expect("read replacement collection history");
    assert!(rows.rows().len() >= minimum);
    let change_id = rows
        .rows()
        .last()
        .map(|row| row.get::<String>("id"))
        .transpose()
        .expect("replacement change id is valid")
        .expect("replacement history has a change id");
    let exact = lix
        .execute(
            "SELECT schema_key FROM lix_change WHERE id = $1",
            &[Value::Text(change_id)],
        )
        .await
        .expect("read directly addressed replacement change");
    assert_eq!(exact.rows().len(), 1);
    assert_eq!(
        exact.rows()[0]
            .get::<String>("schema_key")
            .expect("replacement schema key is valid"),
        SCHEMA_KEY
    );
}

async fn register_schema<S: Storage + Clone + Send + Sync + 'static>(lix: &lix::Lix<S>) {
    let schema = serde_json::json!({
        "x-lix-key": SCHEMA_KEY,
        "x-lix-primary-key": ["/path"],
        "type": "object",
        "properties": {
            "path": { "type": "string" },
            "value": {
                "type": ["object", "array", "string", "number", "integer", "boolean", "null"]
            }
        },
        "required": ["path", "value"],
        "additionalProperties": false
    });
    lix.execute(
        "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
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
