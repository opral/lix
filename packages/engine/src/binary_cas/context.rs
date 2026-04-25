use std::sync::Arc;

use crate::binary_cas::store::BinaryCasTransactionRef;
use crate::binary_cas::BinaryBlobWrite;
use crate::{LixBackend, LixError};

/// Long-lived Binary CAS context.
///
/// Reads are context-level because they can use the shared backend directly.
/// Writes are exposed through `writer(...)` so callers keep ownership of
/// transaction boundaries.
pub(crate) struct BinaryCasContext {
    backend: Arc<dyn LixBackend + Send + Sync>,
}

impl BinaryCasContext {
    pub(crate) fn new(backend: Arc<dyn LixBackend + Send + Sync>) -> Self {
        Self { backend }
    }

    pub(crate) async fn load_blob_data_by_hash(
        &self,
        blob_hash: &str,
    ) -> Result<Option<Vec<u8>>, LixError> {
        crate::binary_cas::kv::load_blob_data_by_hash(self.backend.as_ref(), blob_hash).await
    }

    #[allow(dead_code)]
    pub(crate) async fn blob_exists(&self, blob_hash: &str) -> Result<bool, LixError> {
        crate::binary_cas::kv::blob_exists(self.backend.as_ref(), blob_hash).await
    }

    pub(crate) fn writer<'a>(
        &'a self,
        transaction: BinaryCasTransactionRef<'a>,
    ) -> BinaryCasWriter<'a> {
        BinaryCasWriter { transaction }
    }
}

/// Transaction-scoped Binary CAS writer.
///
/// This type does not begin, commit, or roll back transactions. It only writes
/// CAS data into the transaction supplied by the caller.
pub(crate) struct BinaryCasWriter<'a> {
    transaction: BinaryCasTransactionRef<'a>,
}

impl BinaryCasWriter<'_> {
    pub(crate) async fn put_blob_writes(
        &mut self,
        writes: &[BinaryBlobWrite<'_>],
    ) -> Result<(), LixError> {
        crate::binary_cas::kv::persist_blob_writes_in_transaction(self.transaction, writes).await
    }
}
