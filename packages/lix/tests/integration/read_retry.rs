use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use crate::storage::{
    BeginScanOptions, GetManyRequest, GetManyResult, KeyRange, Memory, MemoryRead, MemoryWrite,
    ReadOptions, ScanCursor, Storage, StorageError, StorageRead, StorageSpace, WriteOptions,
};
use crate::{ExecuteBatchStatement, Value};

#[derive(Clone)]
struct ExpiringReadStorage {
    inner: Memory,
    expire_next_read_call: Arc<AtomicBool>,
    expired_calls: Arc<AtomicUsize>,
}

impl ExpiringReadStorage {
    fn new() -> Self {
        Self {
            inner: Memory::new(),
            expire_next_read_call: Arc::new(AtomicBool::new(false)),
            expired_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn expire_next_read_call(&self) {
        self.expire_next_read_call.store(true, Ordering::Release);
    }

    fn expired_calls(&self) -> usize {
        self.expired_calls.load(Ordering::Acquire)
    }
}

struct ExpiringRead {
    inner: MemoryRead,
    expire_next_read_call: Arc<AtomicBool>,
    expired_calls: Arc<AtomicUsize>,
}

impl ExpiringRead {
    fn expire_if_armed(&self) -> Result<(), StorageError> {
        if self.expire_next_read_call.swap(false, Ordering::AcqRel) {
            self.expired_calls.fetch_add(1, Ordering::AcqRel);
            return Err(StorageError::ReadExpired);
        }
        Ok(())
    }
}

impl Storage for ExpiringReadStorage {
    type Read<'a>
        = ExpiringRead
    where
        Self: 'a;
    type Write<'a>
        = MemoryWrite
    where
        Self: 'a;

    async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        Ok(ExpiringRead {
            inner: self.inner.begin_read(options).await?,
            expire_next_read_call: Arc::clone(&self.expire_next_read_call),
            expired_calls: Arc::clone(&self.expired_calls),
        })
    }

    async fn begin_write(&self, options: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.inner.begin_write(options).await
    }
}

impl StorageRead for ExpiringRead {
    fn snapshot_cache_key(&self) -> Option<u128> {
        self.inner.snapshot_cache_key()
    }

    async fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> Result<GetManyResult, StorageError> {
        self.expire_if_armed()?;
        self.inner.get_many(requests).await
    }

    async fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        options: BeginScanOptions,
    ) -> Result<ScanCursor<'_>, StorageError> {
        self.expire_if_armed()?;
        self.inner.begin_scan(space, range, options).await
    }
}

#[tokio::test]
async fn active_branch_id_does_not_open_a_storage_snapshot() {
    let storage = ExpiringReadStorage::new();
    let lix = crate::open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open Lix");
    let expected = lix.active_branch_id().await.expect("active branch");

    storage.expire_next_read_call();
    let actual = lix.active_branch_id().await.expect("active branch");

    assert_eq!(actual, expected);
    assert_eq!(
        storage.expired_calls(),
        0,
        "the in-memory session selector must not touch coherent storage",
    );
}

#[tokio::test]
async fn auto_commit_query_restarts_after_its_snapshot_expires() {
    let storage = ExpiringReadStorage::new();
    let lix = crate::open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open Lix");
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
        &[Value::Text("read-retry".into()), Value::Integer(42)],
    )
    .await
    .expect("seed value");

    storage.expire_next_read_call();
    let result = lix
        .execute(
            "SELECT value FROM lix_key_value WHERE key = $1",
            &[Value::Text("read-retry".into())],
        )
        .await
        .expect("expired read should restart transparently");

    assert_eq!(
        result.rows()[0].get::<serde_json::Value>("value").unwrap(),
        serde_json::json!(42)
    );
    assert_eq!(storage.expired_calls(), 1);
}

#[tokio::test]
async fn expired_read_waits_for_one_write_quiescent_retry() {
    let storage = ExpiringReadStorage::new();
    let lix = crate::open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open Lix");
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
        &[Value::Text("read-quiescence".into()), Value::Integer(42)],
    )
    .await
    .expect("seed value");

    let writer = lix.lock_collaboration_writes().await;
    storage.expire_next_read_call();
    let params = [Value::Text("read-quiescence".into())];
    let mut read = Box::pin(async {
        lix.execute("SELECT value FROM lix_key_value WHERE key = $1", &params)
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), read.as_mut())
            .await
            .is_err(),
        "the expired read must wait until the in-flight writer releases the gate",
    );

    drop(writer);
    let result = tokio::time::timeout(Duration::from_secs(1), read)
        .await
        .expect("quiescent retry should make forward progress")
        .expect("quiescent retry should succeed");
    assert_eq!(
        result.rows()[0].get::<serde_json::Value>("value").unwrap(),
        serde_json::json!(42)
    );
    assert_eq!(storage.expired_calls(), 1);
}

#[tokio::test]
async fn auto_commit_mutation_restarts_after_its_planning_snapshot_expires() {
    let storage = ExpiringReadStorage::new();
    let lix = crate::open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open Lix");

    storage.expire_next_read_call();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
        &[
            Value::Text("write-retry".into()),
            Value::Text("committed".into()),
        ],
    )
    .await
    .expect("the complete auto-commit mutation should restart");

    let result = lix
        .execute(
            "SELECT value FROM lix_key_value WHERE key = $1",
            &[Value::Text("write-retry".into())],
        )
        .await
        .expect("read committed value");
    assert_eq!(
        result.rows()[0].get::<serde_json::Value>("value").unwrap(),
        serde_json::json!("committed")
    );
    assert_eq!(storage.expired_calls(), 1);
}

#[tokio::test]
async fn mutation_batch_restarts_atomically_after_snapshot_expiry() {
    let storage = ExpiringReadStorage::new();
    let lix = crate::open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open Lix");

    storage.expire_next_read_call();
    let statements = [
        ExecuteBatchStatement {
            label: None,
            sql: "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)".to_string(),
            params: vec![Value::Text("batch-left".into()), Value::Integer(1)],
        },
        ExecuteBatchStatement {
            label: None,
            sql: "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)".to_string(),
            params: vec![Value::Text("batch-right".into()), Value::Integer(2)],
        },
    ];
    lix.execute_batch(&statements)
        .await
        .expect("the complete mutation batch should restart");

    for (key, expected) in [("batch-left", 1), ("batch-right", 2)] {
        let result = lix
            .execute(
                "SELECT value FROM lix_key_value WHERE key = $1",
                &[Value::Text(key.into())],
            )
            .await
            .expect("read committed batch value");
        assert_eq!(
            result.rows()[0].get::<serde_json::Value>("value").unwrap(),
            serde_json::json!(expected)
        );
    }
    assert_eq!(storage.expired_calls(), 1);
}

#[tokio::test]
async fn read_only_batch_restarts_as_one_coherent_unit_after_expiry() {
    let storage = ExpiringReadStorage::new();
    let lix = crate::open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open Lix");

    storage.expire_next_read_call();
    let statements = [
        ExecuteBatchStatement {
            label: None,
            sql: "SELECT 1 AS value".to_string(),
            params: Vec::new(),
        },
        ExecuteBatchStatement {
            label: None,
            sql: "SELECT 2 AS value".to_string(),
            params: Vec::new(),
        },
    ];
    let results = lix
        .execute_batch(&statements)
        .await
        .expect("the complete read batch should restart");

    assert_eq!(results[0].rows()[0].get::<i64>("value").unwrap(), 1);
    assert_eq!(results[1].rows()[0].get::<i64>("value").unwrap(), 2);
    assert_eq!(storage.expired_calls(), 1);
}

#[tokio::test]
async fn coherent_read_batch_restarts_as_one_snapshot_after_expiry() {
    let storage = ExpiringReadStorage::new();
    let lix = crate::open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open Lix");

    storage.expire_next_read_call();
    let empty_params: &[Value] = &[];
    let statements = [
        ("SELECT 1 AS value", empty_params),
        ("SELECT 2 AS value", empty_params),
    ];
    let batch = lix
        .execute_coherent_read_batch(&statements)
        .await
        .expect("the coherent batch should restart");

    assert_eq!(batch.results[0].rows()[0].get::<i64>("value").unwrap(), 1);
    assert_eq!(batch.results[1].rows()[0].get::<i64>("value").unwrap(), 2);
    assert_eq!(storage.expired_calls(), 1);
}
