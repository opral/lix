//! Public semantic carrier acceptance. Source authority is enforced separately
//! by `scripts/acceptance/no_json_current_state_gate.py`.

use std::future::Future;
use std::path::Path;

use async_trait::async_trait;
use lix::storage::{Memory, Storage};
use lix::{Lix, Value, open_lix};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;
use serde_json::json;

const SCALAR_SCHEMA: &str = "no_json_scalar_probe";
const JSONB_SCHEMA: &str = "declared_jsonb_probe";

#[async_trait]
trait ReopenStorage: Storage + Clone + Send + Sync + Sized + 'static {
    fn open_backend(path: &Path) -> Self;
    async fn flush_backend(&self);
}

#[async_trait]
impl ReopenStorage for RocksDB {
    fn open_backend(path: &Path) -> Self {
        Self::open(path).expect("open RocksDB acceptance fixture")
    }

    async fn flush_backend(&self) {
        self.flush().expect("flush RocksDB acceptance fixture");
    }
}

#[async_trait]
impl ReopenStorage for SlateDB {
    fn open_backend(path: &Path) -> Self {
        Self::open(path).expect("open SlateDB acceptance fixture")
    }

    async fn flush_backend(&self) {
        self.flush()
            .await
            .expect("flush SlateDB acceptance fixture");
    }
}

#[tokio::test]
async fn memory_scalar_and_declared_jsonb_crud_remain_distinct() {
    let lix = open_lix()
        .with_storage(Memory::new())
        .await
        .expect("open Memory acceptance fixture");
    register_and_write(&lix).await;
    assert_rows(&lix).await;
    lix.close().await.expect("close Memory fixture");
}

#[test]
fn rocksdb_scalar_and_declared_jsonb_survive_cold_reopen() {
    run_on_large_stack(durable_reopen::<RocksDB>);
}

#[test]
fn slatedb_scalar_and_declared_jsonb_survive_cold_reopen() {
    run_on_large_stack(durable_reopen::<SlateDB>);
}

fn run_on_large_stack<F, Fut>(make_future: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + 'static,
{
    std::thread::Builder::new()
        .name("no-json-current-state-acceptance".into())
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build acceptance runtime")
                .block_on(make_future());
        })
        .expect("spawn acceptance thread")
        .join()
        .expect("acceptance thread panicked");
}

async fn durable_reopen<S: ReopenStorage>() {
    let directory = tempfile::tempdir().expect("create acceptance directory");
    let path = directory.path().join("database");
    {
        let storage = S::open_backend(&path);
        let lix = open_lix()
            .with_storage(storage.clone())
            .await
            .expect("open durable acceptance fixture");
        register_and_write(&lix).await;
        assert_rows(&lix).await;
        lix.close().await.expect("close durable fixture");
        drop(lix);
        storage.flush_backend().await;
    }
    let storage = S::open_backend(&path);
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("cold reopen acceptance fixture");
    assert_rows(&lix).await;
    lix.close().await.expect("close reopened fixture");
    drop(lix);
    storage.flush_backend().await;
}

async fn register_and_write<S: Storage + Clone + Send + Sync + 'static>(lix: &Lix<S>) {
    let scalar = json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": SCALAR_SCHEMA,
        "columns": [
            {"name": "id", "type": "uuid", "nullable": false},
            {"name": "label", "type": "text", "nullable": false},
            {"name": "count", "type": "int8", "nullable": false},
            {"name": "ratio", "type": "float8", "nullable": false},
            {"name": "active", "type": "boolean", "nullable": false},
            {"name": "created_at", "type": "timestamptz", "nullable": false,
             "default_expression": "CURRENT_TIMESTAMP"}
        ],
        "primary_key": ["id"]
    });
    let declared_jsonb = json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": JSONB_SCHEMA,
        "columns": [
            {"name": "id", "type": "text", "nullable": false},
            {"name": "payload", "type": "jsonb", "nullable": false}
        ],
        "primary_key": ["id"]
    });
    for (key, schema) in [(SCALAR_SCHEMA, scalar), (JSONB_SCHEMA, declared_jsonb)] {
        lix.execute(
            "INSERT INTO lix_registered_schema (schema_key, value) VALUES ($1, CAST($2 AS JSONB))",
            &[Value::Text(key.into()), Value::Text(schema.to_string())],
        )
        .await
        .expect("register acceptance schema");
    }
    lix.execute(
        &format!(
            "INSERT INTO {SCALAR_SCHEMA} (id, label, count, ratio, active) \
             VALUES ('01920000-0000-7000-8000-0000000000a1', 'typed', 42, 1.5, true)"
        ),
        &[],
    )
    .await
    .expect("insert scalar-only row");
    lix.execute(
        &format!(
            "INSERT INTO {JSONB_SCHEMA} (id, payload) \
             VALUES ('json', '{{\"b\":2,\"a\":1}}'::jsonb)"
        ),
        &[],
    )
    .await
    .expect("insert declared-jsonb row");
    lix.execute(
        &format!(
            "UPDATE {JSONB_SCHEMA} SET payload = '{{\"a\":1,\"b\":3}}'::jsonb WHERE id = 'json'"
        ),
        &[],
    )
    .await
    .expect("update declared-jsonb cell");
}

async fn assert_rows<S: Storage + Clone + Send + Sync + 'static>(lix: &Lix<S>) {
    let scalar = lix
        .execute(
            &format!(
                "SELECT id, label, count, ratio, active, created_at FROM {SCALAR_SCHEMA} WHERE id = \
                 '01920000-0000-7000-8000-0000000000a1'"
            ),
            &[],
        )
        .await
        .expect("read scalar-only row");
    let values = scalar.rows()[0].values();
    assert_eq!(values[1], Value::Text("typed".into()));
    assert_eq!(values[2], Value::Integer(42));
    assert_eq!(values[3], Value::Real(1.5));
    assert_eq!(values[4], Value::Boolean(true));
    assert!(matches!(values[5], Value::Timestamp(_)));

    let jsonb = lix
        .execute(
            &format!("SELECT payload, payload ->> 'b' AS b FROM {JSONB_SCHEMA} WHERE id = 'json'"),
            &[],
        )
        .await
        .expect("read declared-jsonb row");
    assert_eq!(
        jsonb.rows()[0].values()[0],
        Value::Json(json!({"a": 1, "b": 3}).into())
    );
    assert_eq!(jsonb.rows()[0].values()[1], Value::Text("3".into()));

    // System-current commit rows are a derived authenticated topology
    // surface. This proves that writes made through a dynamically registered
    // native plan remain observable without a current-state JSON carrier.
    let commits = lix
        .execute("SELECT COUNT(*) AS count FROM lix_commit", &[])
        .await
        .expect("read derived lix_commit topology");
    assert!(matches!(commits.rows()[0].values()[0], Value::Integer(count) if count > 0));
}
