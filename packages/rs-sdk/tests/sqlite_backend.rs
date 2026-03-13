use lix_engine::{ImageChunkReader, ImageChunkWriter, LixError};
use lix_rs_sdk::{LixBackend, SqliteBackend, Value};

struct VecImageWriter {
    bytes: Vec<u8>,
}

#[async_trait::async_trait(?Send)]
impl ImageChunkWriter for VecImageWriter {
    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), LixError> {
        self.bytes.extend_from_slice(chunk);
        Ok(())
    }
}

struct VecImageReader {
    bytes: Option<Vec<u8>>,
}

#[async_trait::async_trait(?Send)]
impl ImageChunkReader for VecImageReader {
    async fn read_chunk(&mut self) -> Result<Option<Vec<u8>>, LixError> {
        Ok(self.bytes.take())
    }
}

#[tokio::test]
async fn sqlite_backend_transaction_commit_persists_changes() {
    let backend = SqliteBackend::in_memory().expect("in-memory backend should initialize");

    backend
        .execute(
            "CREATE TABLE tx_test (id TEXT PRIMARY KEY, payload BLOB NOT NULL)",
            &[],
        )
        .await
        .expect("schema setup should succeed");

    let mut tx = backend
        .begin_transaction()
        .await
        .expect("begin_transaction should succeed");
    tx.execute(
        "INSERT INTO tx_test (id, payload) VALUES (?, ?)",
        &[
            Value::Text("commit-row".to_string()),
            Value::Blob(vec![1, 2, 3]),
        ],
    )
    .await
    .expect("insert inside transaction should succeed");
    tx.commit().await.expect("commit should succeed");

    let rows = backend
        .execute(
            "SELECT COUNT(*) FROM tx_test WHERE id = 'commit-row' AND length(payload) = 3",
            &[],
        )
        .await
        .expect("verification query should succeed");
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0][0], Value::Integer(1));
}

#[tokio::test]
async fn sqlite_backend_transaction_rollback_discards_changes() {
    let backend = SqliteBackend::in_memory().expect("in-memory backend should initialize");

    backend
        .execute(
            "CREATE TABLE tx_test (id TEXT PRIMARY KEY, payload BLOB NOT NULL)",
            &[],
        )
        .await
        .expect("schema setup should succeed");

    let mut tx = backend
        .begin_transaction()
        .await
        .expect("begin_transaction should succeed");
    tx.execute(
        "INSERT INTO tx_test (id, payload) VALUES ('rollback-row', X'AA')",
        &[],
    )
    .await
    .expect("insert inside transaction should succeed");
    tx.rollback().await.expect("rollback should succeed");

    let rows = backend
        .execute(
            "SELECT COUNT(*) FROM tx_test WHERE id = 'rollback-row'",
            &[],
        )
        .await
        .expect("verification query should succeed");
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0][0], Value::Integer(0));
}

#[tokio::test]
async fn sqlite_backend_export_and_restore_image_roundtrip() {
    let backend = SqliteBackend::in_memory().expect("in-memory backend should initialize");

    backend
        .execute(
            "CREATE TABLE snapshot_test (id TEXT PRIMARY KEY, payload BLOB NOT NULL)",
            &[],
        )
        .await
        .expect("schema setup should succeed");
    backend
        .execute(
            "INSERT INTO snapshot_test (id, payload) VALUES ('snap-1', X'CAFE')",
            &[],
        )
        .await
        .expect("seed insert should succeed");

    let mut writer = VecImageWriter { bytes: Vec::new() };
    backend
        .export_image(&mut writer)
        .await
        .expect("export_image should succeed");
    assert!(
        !writer.bytes.is_empty(),
        "export_image should emit sqlite bytes"
    );

    backend
        .execute("DELETE FROM snapshot_test WHERE id = 'snap-1'", &[])
        .await
        .expect("delete should succeed");

    let mut reader = VecImageReader {
        bytes: Some(writer.bytes),
    };
    backend
        .restore_from_image(&mut reader)
        .await
        .expect("restore_from_image should succeed");

    let rows = backend
        .execute(
            "SELECT COUNT(*) FROM snapshot_test WHERE id = 'snap-1' AND hex(payload) = 'CAFE'",
            &[],
        )
        .await
        .expect("verification query should succeed");
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0][0], Value::Integer(1));
}
