use lix::Value;
use lix::open_lix;
use lix::storage::{
    Memory, MemoryRead, MemoryWrite, ReadOptions, Storage, StorageError, StorageSessionToken,
    WriteOptions,
};

/// Models an adapter living in an unrelated crate. Integration tests compile
/// as external crates, so this can only use Lix's published API.
#[derive(Clone)]
struct DelegatingStorage(Memory);

impl Storage for DelegatingStorage {
    type Read<'a>
        = MemoryRead
    where
        Self: 'a;
    type Write<'a>
        = MemoryWrite
    where
        Self: 'a;

    async fn acquire_session(&self) -> Result<StorageSessionToken, StorageError> {
        self.0.acquire_session().await
    }

    async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.0.begin_read(options).await
    }

    async fn begin_write(&self, options: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.0.begin_write(options).await
    }
}

#[tokio::test]
async fn a_third_party_storage_adapter_needs_only_lix() {
    let lix = open_lix()
        .with_storage(DelegatingStorage(Memory::new()))
        .await
        .expect("open Lix with external storage adapter");

    let result = lix
        .execute("SELECT $1 AS value", &[Value::Integer(42)])
        .await
        .unwrap();
    assert_eq!(result.rows()[0].get::<i64>("value").unwrap(), 42);
}
