use std::sync::Arc;

use async_trait::async_trait;

use crate::backend::{KvStore, KvWriter};
use crate::binary_cas::BinaryBlobWrite;
use crate::{LixBackend, LixError};

#[async_trait]
pub(crate) trait BlobDataReader: Send + Sync {
    async fn load_blob_data_by_hash(&self, blob_hash: &str) -> Result<Option<Vec<u8>>, LixError>;
}

/// Long-lived Binary CAS context factory.
///
/// The context does not own storage. Callers explicitly provide a KV store via
/// `reader(...)` or `writer(...)`, keeping backend and transaction ownership at
/// the execution layer.
pub(crate) struct BinaryCasContext;

impl BinaryCasContext {
    pub(crate) fn new() -> Self {
        Self
    }

    /// Creates a Binary CAS reader over any backend KV store.
    ///
    /// The store can be the shared backend outside a transaction or the active
    /// transaction handle when reads must participate in transaction-local
    /// visibility.
    pub(crate) fn reader<S>(&self, store: S) -> BinaryCasReader<S>
    where
        S: KvStore,
    {
        BinaryCasReader { store }
    }

    pub(crate) fn writer<S>(&self, store: S) -> BinaryCasWriter<S>
    where
        S: KvWriter,
    {
        BinaryCasWriter { store }
    }
}

#[async_trait]
impl BlobDataReader for BinaryCasReader<Arc<dyn LixBackend + Send + Sync>> {
    async fn load_blob_data_by_hash(&self, blob_hash: &str) -> Result<Option<Vec<u8>>, LixError> {
        let mut reader = BinaryCasReader {
            store: Arc::clone(&self.store),
        };
        BinaryCasReader::load_blob_data_by_hash(&mut reader, blob_hash).await
    }
}

/// Binary CAS reader over a caller-supplied KV store.
pub(crate) struct BinaryCasReader<S> {
    store: S,
}

impl<S> BinaryCasReader<S>
where
    S: KvStore,
{
    pub(crate) async fn load_blob_data_by_hash(
        &mut self,
        blob_hash: &str,
    ) -> Result<Option<Vec<u8>>, LixError> {
        crate::binary_cas::kv::load_blob_data_by_hash(&mut self.store, blob_hash).await
    }

    pub(crate) async fn blob_exists(&mut self, blob_hash: &str) -> Result<bool, LixError> {
        crate::binary_cas::kv::blob_exists(&mut self.store, blob_hash).await
    }
}

/// Transaction-scoped Binary CAS writer.
///
/// This type does not begin, commit, or roll back transactions. It only writes
/// CAS data into the transaction supplied by the caller.
pub(crate) struct BinaryCasWriter<S> {
    store: S,
}

impl<S> BinaryCasWriter<S>
where
    S: KvWriter,
{
    pub(crate) async fn put_blob_writes(
        &mut self,
        writes: &[BinaryBlobWrite<'_>],
    ) -> Result<(), LixError> {
        crate::binary_cas::kv::persist_blob_writes_in_transaction(&mut self.store, writes).await
    }
}
