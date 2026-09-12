use super::*;
use crate::storage::{MemoryRead, MemoryWrite, ReadOptions, StorageError, WriteOptions};
use std::collections::VecDeque;
use std::sync::Mutex;

#[derive(Clone)]
struct ReadFaultStorage {
    inner: Memory,
    faults: Arc<Mutex<VecDeque<Option<StorageError>>>>,
    read_calls: Arc<AtomicUsize>,
    failures: Arc<AtomicUsize>,
}

impl Storage for ReadFaultStorage {
    type Read<'a>
        = MemoryRead
    where
        Self: 'a;
    type Write<'a>
        = MemoryWrite
    where
        Self: 'a;

    async fn acquire_session(&self) -> Result<crate::storage::StorageSessionToken, StorageError> {
        self.inner.acquire_session().await
    }
    async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.read_calls.fetch_add(1, Ordering::SeqCst);
        let fault = self.faults.lock().unwrap().pop_front().flatten();
        if let Some(error) = fault {
            self.failures.fetch_add(1, Ordering::SeqCst);
            return Err(error);
        }
        self.inner.begin_read(options).await
    }
    async fn begin_write(&self, options: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.inner.begin_write(options).await
    }
}

#[tokio::test]
async fn child_session_retries_expired_admission_reads_without_retaining_failed_children() {
    let storage = ReadFaultStorage {
        inner: Memory::new(),
        faults: Arc::default(),
        read_calls: Arc::default(),
        failures: Arc::default(),
    };
    let root = open_lix().with_storage(storage.clone()).await.unwrap();
    let baseline = Arc::strong_count(&root.engine);
    // Expire both the first branch read and a later validation read. Each
    // attempt owns fresh canonical Memory reads and constructs no failed child.
    for before_failure in [0, 1] {
        let mut faults = vec![None; before_failure];
        faults.extend([
            Some(StorageError::ReadExpired),
            Some(StorageError::ReadExpired),
        ]);
        *storage.faults.lock().unwrap() = faults.into();
        let child = root.open_another_session().await.unwrap();
        assert!(
            storage.faults.lock().unwrap().is_empty(),
            "admission consumed every injected read failure before child publication"
        );
        assert_eq!(
            child.active_branch_id().await.unwrap(),
            root.active_branch_id().await.unwrap()
        );
        child.execute("SELECT 1", &[]).await.unwrap();
        child.close().await.unwrap();
        drop(child);
        assert_eq!(Arc::strong_count(&root.engine), baseline);
    }
    assert_eq!(storage.failures.load(Ordering::SeqCst), 4);
    root.execute("SELECT 1", &[]).await.unwrap();
    root.close().await.unwrap();
}

#[tokio::test]
async fn child_session_preserves_real_storage_errors_and_parent_lifecycle() {
    let storage = ReadFaultStorage {
        inner: Memory::new(),
        faults: Arc::default(),
        read_calls: Arc::default(),
        failures: Arc::default(),
    };
    let root = open_lix().with_storage(storage.clone()).await.unwrap();
    let baseline = Arc::strong_count(&root.engine);
    storage.read_calls.store(0, Ordering::SeqCst);
    *storage.faults.lock().unwrap() =
        [Some(StorageError::Corruption("admission-corrupt".into()))].into();
    let error = root
        .open_another_session()
        .await
        .err()
        .expect("real storage failure must escape");
    assert_eq!(error.code, LixError::CODE_STORAGE_ERROR);
    assert!(error.message.contains("admission-corrupt"));
    assert_eq!(storage.read_calls.load(Ordering::SeqCst), 1);
    assert_eq!(Arc::strong_count(&root.engine), baseline);
    let child = root.open_another_session().await.unwrap();
    child.close().await.unwrap();
    root.execute("SELECT 1", &[]).await.unwrap();
    root.close().await.unwrap();
}
