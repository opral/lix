use super::*;
use crate::storage::{ReadOptions, StorageError, StorageSessionToken, WriteOptions};
use crate::storage_adapter::{MemoryRead, MemoryWrite};
use std::sync::Mutex;

#[derive(Clone, Default)]
struct RecordingStorage {
    inner: Memory,
    writes: Arc<Mutex<Vec<bool>>>,
}

impl Storage for RecordingStorage {
    type Read<'a> = MemoryRead;
    type Write<'a> = MemoryWrite;

    async fn acquire_session(&self) -> Result<StorageSessionToken, StorageError> {
        self.inner.acquire_session().await
    }

    async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.inner.begin_read(options).await
    }

    async fn begin_write(&self, options: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.writes.lock().unwrap().push(options.await_durable);
        self.inner.begin_write(options).await
    }
}

impl RecordingStorage {
    fn take_writes(&self) -> Vec<bool> {
        std::mem::take(&mut *self.writes.lock().unwrap())
    }
}

#[tokio::test]
async fn repository_policy_covers_automatic_explicit_and_additional_sessions() {
    for policy in [Durability::Durable, Durability::Buffered] {
        let storage = RecordingStorage::default();
        let lix = open_lix()
            .with_storage(storage.clone())
            .with_durability(policy)
            .await
            .unwrap();
        storage.take_writes();
        lix.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('durability-test', '1')",
            &[],
        )
        .await
        .unwrap();
        let automatic = storage.take_writes();
        assert!(!automatic.is_empty());
        assert!(
            automatic
                .iter()
                .all(|value| *value == (policy == Durability::Durable))
        );

        lix.execute_batch(&[crate::ExecuteBatchStatement {
            sql: "UPDATE lix_key_value SET value = 'batch' WHERE key = 'durability-test'"
                .to_owned(),
            params: Vec::new(),
            label: None,
        }])
        .await
        .unwrap();
        let batch = storage.take_writes();
        assert!(!batch.is_empty());
        assert!(
            batch
                .iter()
                .all(|value| *value == (policy == Durability::Durable))
        );

        let mut transaction = lix.begin_transaction().await.unwrap();
        transaction
            .execute(
                "UPDATE lix_key_value SET value = '2' WHERE key = 'durability-test'",
                &[],
            )
            .await
            .unwrap();
        // Staging an explicit SQL transaction must not acknowledge a storage commit.
        assert!(storage.take_writes().is_empty());
        transaction.commit().await.unwrap();
        let explicit = storage.take_writes();
        assert!(!explicit.is_empty());
        assert!(
            explicit
                .iter()
                .all(|value| *value == (policy == Durability::Durable))
        );

        let session = lix.open_another_session().await.unwrap();
        storage.take_writes();
        session
            .execute(
                "UPDATE lix_key_value SET value = '3' WHERE key = 'durability-test'",
                &[],
            )
            .await
            .unwrap();
        let additional = storage.take_writes();
        assert!(!additional.is_empty());
        assert!(
            additional
                .iter()
                .all(|value| *value == (policy == Durability::Durable))
        );
        session.close().await.unwrap();
        lix.close().await.unwrap();
    }
}

#[tokio::test]
async fn durable_is_default_and_buffered_does_not_weaken_forced_writes() {
    let storage = RecordingStorage::default();
    let lix = open_lix().with_storage(storage.clone()).await.unwrap();
    assert!(!storage.take_writes().is_empty());
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('durability-default', '1')",
        &[],
    )
    .await
    .unwrap();
    let default_writes = storage.take_writes();
    assert!(!default_writes.is_empty());
    assert!(default_writes.iter().all(|value| *value));
    let adapter = lix.engine.storage().with_durability(Durability::Buffered);
    adapter
        .commit_write_set(
            adapter.new_write_set(),
            WriteOptions {
                await_durable: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(storage.take_writes(), vec![true]);
    lix.close().await.unwrap();
}
