use super::super::*;
use crate::storage::{Memory, MemoryRead, MemoryWrite, ReadOptions, StorageError, WriteOptions};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone)]
struct ExpiringStorage {
    inner: Memory,
    expirations: Arc<AtomicUsize>,
    reads_before_expiry: Arc<AtomicUsize>,
    reads: Arc<AtomicUsize>,
}

impl Storage for ExpiringStorage {
    type Read<'a> = MemoryRead;
    type Write<'a> = MemoryWrite;

    async fn acquire_session(&self) -> Result<crate::storage::StorageSessionToken, StorageError> {
        self.inner.acquire_session().await
    }
    async fn begin_read(&self, options: ReadOptions) -> Result<MemoryRead, StorageError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let skip_expiry = self
            .reads_before_expiry
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if !skip_expiry
            && self
                .expirations
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
        {
            return Err(StorageError::ReadExpired);
        }
        self.inner.begin_read(options).await
    }
    async fn begin_write(&self, options: WriteOptions) -> Result<MemoryWrite, StorageError> {
        self.inner.begin_write(options).await
    }
}

async fn replica() -> (Lix<ExpiringStorage>, ExpiringStorage) {
    let storage = ExpiringStorage {
        inner: Memory::new(),
        expirations: Arc::default(),
        reads_before_expiry: Arc::default(),
        reads: Arc::default(),
    };
    let lix = crate::open_lix()
        .with_storage(storage.clone())
        .await
        .unwrap();
    let adapter = lix.storage_adapter();
    let mut writes = adapter.new_write_set();
    writes.put(
        super::super::super::SYNC_REPLICA_STATE_SPACE,
        &b"repository"[..],
        serde_json::to_vec(&serde_json::json!({
            "activeAccountId": lix.active_account_id(), "cursor": 0,
            "authoritativeBranches": {}, "certifiedBranchRoots": {},
            "authorityKnownCommitIds": []
        }))
        .unwrap(),
    );
    adapter
        .commit_write_set(writes, WriteOptions::default())
        .await
        .unwrap();
    (lix, storage)
}

#[tokio::test]
async fn reconnect_validation_retries_a_concurrent_receipt_read_expiry() {
    let (lix, storage) = replica().await;
    // Exercise both the account receipt read and the HOT certificate read.
    for successful_reads in [0, 1] {
        storage
            .reads_before_expiry
            .store(successful_reads, Ordering::SeqCst);
        storage.expirations.store(1, Ordering::SeqCst);
        validate_connected_authority(
            &lix,
            "https://sync.example/lix/repository",
            lix.lix_id(),
            lix.active_account_id(),
        )
        .await
        .expect("a concurrent commit must not be mistaken for changed authority");
        assert_eq!(storage.expirations.load(Ordering::SeqCst), 0);
    }
    lix.close().await.unwrap();
}

#[tokio::test]
async fn reconnect_validation_preserves_authority_mismatches_without_retry() {
    let (lix, storage) = replica().await;
    storage.reads.store(0, Ordering::SeqCst);
    let error = validate_connected_authority(
        &lix,
        "https://sync.example/lix/repository",
        lix.lix_id(),
        "different-account",
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
    assert!(!is_retryable_authority_validation_error(&error));
    assert_eq!(storage.reads.load(Ordering::SeqCst), 1);
    let error = validate_connected_authority(
        &lix,
        "https://sync.example/lix/repository",
        "00000000-0000-4000-8000-000000000001",
        lix.active_account_id(),
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.code,
        super::super::super::SYNC_REPOSITORY_ID_MISMATCH_CODE
    );
    assert!(!is_retryable_authority_validation_error(&error));
    assert_eq!(
        storage.reads.load(Ordering::SeqCst),
        1,
        "identity mismatch is checked before local reads"
    );
    lix.close().await.unwrap();
}

#[tokio::test]
async fn reconnect_validation_caps_persistent_expired_reads() {
    let (lix, storage) = replica().await;
    storage.expirations.store(100_000, Ordering::SeqCst);
    storage.reads.store(0, Ordering::SeqCst);
    let error = tokio::time::timeout(
        Duration::from_secs(6),
        validate_connected_authority(
            &lix,
            "https://sync.example/lix/repository",
            lix.lix_id(),
            lix.active_account_id(),
        ),
    )
    .await
    .expect("validation must finish within its bounded read retry budget")
    .unwrap_err();
    assert_eq!(error.code, LixError::CODE_STORAGE_READ_EXPIRED);
    assert!(
        is_retryable_authority_validation_error(&error),
        "exhaustion must back off rather than poison observers"
    );
    let reads = storage.reads.load(Ordering::SeqCst);
    assert!(
        reads > 1 && reads < 1_000,
        "retry backoff must bound storage work: {reads}"
    );
    storage.expirations.store(0, Ordering::SeqCst);
    lix.close().await.unwrap();
}
