use std::path::Path;

use async_trait::async_trait;
use lix::storage::Storage;
use lix::{Value, open_lix};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;

const SCHEMA_KEY: &str = "native_schema_point_reopen_probe";

#[async_trait]
trait ReopenStorage: Storage + Clone + Send + Sync + Sized + 'static {
    fn open(path: &Path) -> Self;
    async fn flush(&self);
}

#[async_trait]
impl ReopenStorage for RocksDB {
    fn open(path: &Path) -> Self {
        Self::open(path).expect("open RocksDB point fixture")
    }

    async fn flush(&self) {
        self.flush().expect("flush RocksDB point fixture");
    }
}

#[async_trait]
impl ReopenStorage for SlateDB {
    fn open(path: &Path) -> Self {
        Self::open(path).expect("open SlateDB point fixture")
    }

    async fn flush(&self) {
        self.flush().await.expect("flush SlateDB point fixture");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rocksdb_native_schema_point_survives_cold_reopen() {
    native_schema_point_survives_cold_reopen::<RocksDB>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slatedb_native_schema_point_survives_cold_reopen() {
    native_schema_point_survives_cold_reopen::<SlateDB>().await;
}

async fn native_schema_point_survives_cold_reopen<S: ReopenStorage>() {
    let temp = tempfile::tempdir().expect("create native point fixture");
    let path = temp.path().join("database");
    let initial = r#"{"a":[true,null],"z":7}"#;
    let expected = r#"{"a":[false,{"root":"replacement"}],"z":8}"#;
    {
        let storage = S::open(&path);
        let lix = open_lix()
            .with_storage(storage.clone())
            .await
            .expect("initialize native point fixture");
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": SCHEMA_KEY,
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "payload", "type": "jsonb", "nullable": false }
            ],
            "primary_key": ["id"]
        });
        lix.execute(
            "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register native point schema");
        lix.execute(
            &format!("INSERT INTO {SCHEMA_KEY} (id, payload) VALUES ($1, CAST($2 AS JSONB))"),
            &[Value::Text("row-a".into()), Value::Text(initial.into())],
        )
        .await
        .expect("insert native point row");
        assert_point(&lix, initial).await;
        lix.execute(
            &format!("UPDATE {SCHEMA_KEY} SET payload = CAST($2 AS JSONB) WHERE id = $1"),
            &[Value::Text("row-a".into()), Value::Text(expected.into())],
        )
        .await
        .expect("replace native point row under a new authenticated root");
        assert_point(&lix, expected).await;
        assert_point(&lix, expected).await;
        lix.close().await.expect("close native point fixture");
        drop(lix);
        storage.flush().await;
    }

    let storage = S::open(&path);
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("cold reopen native point fixture");
    assert_point(&lix, expected).await;
    lix.close()
        .await
        .expect("close reopened native point fixture");
    drop(lix);
    storage.flush().await;
}

async fn assert_point<S: Storage + Clone + Send + Sync + 'static>(
    lix: &lix::Lix<S>,
    expected: &str,
) {
    let result = lix
        .execute(
            &format!("SELECT payload AS body FROM {SCHEMA_KEY} WHERE id = $1 LIMIT 1"),
            &[Value::Text("row-a".into())],
        )
        .await
        .expect("read native point row");
    assert_eq!(result.columns(), ["body"]);
    assert_eq!(result.len(), 1);
    let Value::Jsonb(actual) = result.rows()[0].get_index(0).expect("point payload") else {
        panic!("point payload must remain JSONB");
    };
    assert_eq!(actual.as_str(), expected);
}
