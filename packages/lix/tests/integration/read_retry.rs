use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::storage::{
    BeginScanOptions, GetManyRequest, GetManyResult, KeyRange, Memory, MemoryRead, MemoryWrite,
    ReadOptions, ScanCursor, Storage, StorageError, StorageRead, StorageSpace, WriteOptions,
};
use crate::{ExecuteBatchStatement, Value};

#[derive(Clone)]
struct ExpiringReadStorage {
    inner: Memory,
    remaining_expired_read_calls: Arc<AtomicUsize>,
    remaining_expired_scan_calls: Arc<AtomicUsize>,
    read_calls_before_expiry: Arc<AtomicUsize>,
    expired_calls: Arc<AtomicUsize>,
}

impl ExpiringReadStorage {
    fn new() -> Self {
        Self {
            inner: Memory::new(),
            remaining_expired_read_calls: Arc::new(AtomicUsize::new(0)),
            remaining_expired_scan_calls: Arc::new(AtomicUsize::new(0)),
            read_calls_before_expiry: Arc::new(AtomicUsize::new(usize::MAX)),
            expired_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn expire_next_read_call(&self) {
        self.expire_read_calls(1);
    }

    fn expire_read_calls(&self, count: usize) {
        self.remaining_expired_read_calls
            .store(count, Ordering::Release);
    }

    fn expire_read_call_after(&self, calls_before_expiry: usize) {
        self.read_calls_before_expiry
            .store(calls_before_expiry, Ordering::Release);
    }

    fn expire_next_scan_call(&self) {
        self.remaining_expired_scan_calls
            .store(1, Ordering::Release);
    }

    fn allow_reads(&self) {
        self.remaining_expired_read_calls
            .store(0, Ordering::Release);
    }

    fn expired_calls(&self) -> usize {
        self.expired_calls.load(Ordering::Acquire)
    }
}

struct ExpiringRead {
    inner: MemoryRead,
    remaining_expired_read_calls: Arc<AtomicUsize>,
    remaining_expired_scan_calls: Arc<AtomicUsize>,
    read_calls_before_expiry: Arc<AtomicUsize>,
    expired_calls: Arc<AtomicUsize>,
}

impl ExpiringRead {
    fn expire_if_armed(&self) -> Result<(), StorageError> {
        let countdown_expired = self
            .read_calls_before_expiry
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| match remaining {
                usize::MAX => None,
                0 => Some(usize::MAX),
                remaining => Some(remaining - 1),
            })
            .is_ok_and(|remaining| remaining == 0);
        let repeated_expiry = self
            .remaining_expired_read_calls
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok();
        if countdown_expired || repeated_expiry {
            self.expired_calls.fetch_add(1, Ordering::AcqRel);
            return Err(StorageError::ReadExpired);
        }
        Ok(())
    }

    fn expire_scan_if_armed(&self) -> Result<(), StorageError> {
        if self
            .remaining_expired_scan_calls
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
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
            remaining_expired_read_calls: Arc::clone(&self.remaining_expired_read_calls),
            remaining_expired_scan_calls: Arc::clone(&self.remaining_expired_scan_calls),
            read_calls_before_expiry: Arc::clone(&self.read_calls_before_expiry),
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
        self.expire_scan_if_armed()?;
        self.inner.begin_scan(space, range, options).await
    }
}

#[tokio::test]
async fn repository_open_restarts_after_its_snapshot_expires() {
    let storage = ExpiringReadStorage::new();
    storage.expire_next_read_call();

    let lix = crate::open_lix()
        .with_storage(storage.clone())
        .await
        .expect("repository open should restart as one coherent unit");
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
        &[
            Value::Text("open-read-retry".into()),
            Value::Text("ready".into()),
        ],
    )
    .await
    .expect("opened repository accepts writes");
    let result = lix
        .execute(
            "SELECT value FROM lix_key_value WHERE key = $1",
            &[Value::Text("open-read-retry".into())],
        )
        .await
        .expect("opened repository accepts reads");

    assert_eq!(
        result.rows()[0].get::<serde_json::Value>("value").unwrap(),
        serde_json::json!("ready")
    );
    assert_eq!(storage.expired_calls(), 1);
}

#[tokio::test]
async fn initialized_repository_reopen_restarts_each_early_coherent_read() {
    let storage = ExpiringReadStorage::new();
    crate::open_lix()
        .with_storage(storage.clone())
        .await
        .expect("initialize repository")
        .close()
        .await
        .expect("close initializer");

    for calls_before_expiry in 0..12 {
        let expired_before = storage.expired_calls();
        storage.expire_read_call_after(calls_before_expiry);
        crate::open_lix()
            .with_storage(storage.clone())
            .await
            .expect("initialized reopen should restart as one coherent unit")
            .close()
            .await
            .expect("close reopened repository");
        assert_eq!(
            storage.expired_calls(),
            expired_before + 1,
            "the requested early read ordinal must be exercised"
        );
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
async fn filesystem_provider_scan_preserves_expired_read_for_session_retry() {
    let storage = ExpiringReadStorage::new();
    let lix = crate::open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open Lix");
    lix.execute("INSERT INTO lix_directory (path) VALUES ('/docs')", &[])
        .await
        .expect("seed directory");
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ('/docs/readme.md', $1)",
        &[Value::Blob(b"hello".to_vec().into())],
    )
    .await
    .expect("seed file");

    for sql in [
        "SELECT id FROM lix_directory",
        "SELECT content FROM lix_file",
    ] {
        let expired_before = storage.expired_calls();
        storage.expire_next_scan_call();
        lix.execute(sql, &[])
            .await
            .expect("filesystem provider must preserve the typed retry signal");
        assert_eq!(storage.expired_calls(), expired_before + 1);
    }
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
async fn expired_read_backs_off_until_cross_context_writer_settles() {
    let storage = ExpiringReadStorage::new();
    let lix = crate::open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open Lix");
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
        &[
            Value::Text("read-external-quiescence".into()),
            Value::Integer(42),
        ],
    )
    .await
    .expect("seed value");

    // A writer in another browser context does not share this engine's write
    // gate. Keep expiring fresh snapshots until that external churn settles.
    storage.expire_read_calls(usize::MAX);
    let settling_storage = storage.clone();
    tokio::spawn(async move {
        // A cross-context browser commit can include OPFS durability and a
        // remote push before the next coherent read generation is available.
        // Keep this longer than the old count-derived ~1 second retry window.
        tokio::time::sleep(Duration::from_secs(2)).await;
        settling_storage.allow_reads();
    });

    let result = tokio::time::timeout(
        Duration::from_secs(4),
        lix.execute(
            "SELECT value FROM lix_key_value WHERE key = $1",
            &[Value::Text("read-external-quiescence".into())],
        ),
    )
    .await
    .expect("bounded retry should wait for external writes to settle")
    .expect("expired read should stay behind the SQL API boundary");

    assert_eq!(
        result.rows()[0].get::<serde_json::Value>("value").unwrap(),
        serde_json::json!(42)
    );
    assert!(
        storage.expired_calls() > 64,
        "the regression must cross the old count-derived retry cap",
    );
}

#[tokio::test]
async fn perpetually_expired_read_remains_bounded() {
    let storage = ExpiringReadStorage::new();
    let lix = crate::open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open Lix");

    storage.expire_read_calls(usize::MAX);
    let error = tokio::time::timeout(
        Duration::from_secs(4),
        lix.execute("SELECT key FROM lix_key_value", &[]),
    )
    .await
    .expect("retry policy must remain bounded")
    .expect_err("perpetual invalidation must eventually surface");

    assert_eq!(error.code, "LIX_STORAGE_READ_EXPIRED");
    assert!(
        storage.expired_calls() > 1,
        "the bounded path must cover repeated invalidation",
    );
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
