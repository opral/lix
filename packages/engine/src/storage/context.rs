use std::sync::Arc;

use async_trait::async_trait;

use crate::backend::{Backend, BackendReadTransaction, BackendWriteTransaction};
use crate::storage::types::{KvWriteBatch, StorageWriter};
use crate::storage::{
    KvEntryPage, KvExistsBatch, KvGetRequest, KvKeyPage, KvScanRequest, KvValueBatch, KvValuePage,
    KvWriteStats, StorageReadTransaction, StorageReader, StorageWriteTransaction,
};
use crate::LixError;

#[derive(Clone)]
pub(crate) struct StorageContext {
    backend: Arc<dyn Backend + Send + Sync>,
}

impl StorageContext {
    pub(crate) fn new(backend: Arc<dyn Backend + Send + Sync>) -> Self {
        Self { backend }
    }

    pub(crate) async fn begin_read_transaction(
        &self,
    ) -> Result<Box<dyn StorageReadTransaction + Send + Sync + 'static>, LixError> {
        let transaction = self.backend.begin_read_transaction().await?;
        Ok(Box::new(StorageContextReadTransaction { transaction }))
    }

    pub(crate) async fn begin_write_transaction(
        &self,
    ) -> Result<Box<dyn StorageWriteTransaction + Send + Sync + 'static>, LixError> {
        let transaction = self.backend.begin_write_transaction().await?;
        Ok(Box::new(StorageContextWriteTransaction { transaction }))
    }

    pub(crate) async fn close(&self) -> Result<(), LixError> {
        self.backend.close().await
    }

    pub(crate) async fn destroy(&self) -> Result<(), LixError> {
        self.backend.destroy().await
    }
}

#[cfg(any(test, feature = "storage-benches"))]
#[async_trait]
impl StorageReader for StorageContext {
    async fn get_values(&mut self, request: KvGetRequest) -> Result<KvValueBatch, LixError> {
        let mut transaction = self.begin_read_transaction().await?;
        let result = transaction.get_values(request).await;
        match result {
            Ok(result) => {
                transaction.rollback().await?;
                Ok(result)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    async fn exists_many(&mut self, request: KvGetRequest) -> Result<KvExistsBatch, LixError> {
        let mut transaction = self.begin_read_transaction().await?;
        let result = transaction.exists_many(request).await;
        match result {
            Ok(result) => {
                transaction.rollback().await?;
                Ok(result)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    async fn scan_keys(&mut self, request: KvScanRequest) -> Result<KvKeyPage, LixError> {
        let mut transaction = self.begin_read_transaction().await?;
        let result = transaction.scan_keys(request).await;
        match result {
            Ok(result) => {
                transaction.rollback().await?;
                Ok(result)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    async fn scan_values(&mut self, request: KvScanRequest) -> Result<KvValuePage, LixError> {
        let mut transaction = self.begin_read_transaction().await?;
        let result = transaction.scan_values(request).await;
        match result {
            Ok(result) => {
                transaction.rollback().await?;
                Ok(result)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    async fn scan_entries(&mut self, request: KvScanRequest) -> Result<KvEntryPage, LixError> {
        let mut transaction = self.begin_read_transaction().await?;
        let result = transaction.scan_entries(request).await;
        match result {
            Ok(result) => {
                transaction.rollback().await?;
                Ok(result)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }
}

struct StorageContextReadTransaction {
    transaction: Box<dyn BackendReadTransaction + Send + Sync + 'static>,
}

struct StorageContextWriteTransaction {
    transaction: Box<dyn BackendWriteTransaction + Send + Sync + 'static>,
}

#[async_trait]
impl StorageReader for StorageContextReadTransaction {
    async fn get_values(&mut self, request: KvGetRequest) -> Result<KvValueBatch, LixError> {
        self.transaction
            .get_values(request.into())
            .await
            .map(Into::into)
    }

    async fn exists_many(&mut self, request: KvGetRequest) -> Result<KvExistsBatch, LixError> {
        self.transaction
            .exists_many(request.into())
            .await
            .map(Into::into)
    }

    async fn scan_keys(&mut self, request: KvScanRequest) -> Result<KvKeyPage, LixError> {
        self.transaction
            .scan_keys(request.into())
            .await
            .map(Into::into)
    }

    async fn scan_values(&mut self, request: KvScanRequest) -> Result<KvValuePage, LixError> {
        self.transaction
            .scan_values(request.into())
            .await
            .map(Into::into)
    }

    async fn scan_entries(&mut self, request: KvScanRequest) -> Result<KvEntryPage, LixError> {
        self.transaction
            .scan_entries(request.into())
            .await
            .map(Into::into)
    }
}

#[async_trait]
impl StorageReadTransaction for StorageContextReadTransaction {
    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        self.transaction.rollback().await
    }
}

#[async_trait]
impl StorageReader for StorageContextWriteTransaction {
    async fn get_values(&mut self, request: KvGetRequest) -> Result<KvValueBatch, LixError> {
        self.transaction
            .get_values(request.into())
            .await
            .map(Into::into)
    }

    async fn exists_many(&mut self, request: KvGetRequest) -> Result<KvExistsBatch, LixError> {
        self.transaction
            .exists_many(request.into())
            .await
            .map(Into::into)
    }

    async fn scan_keys(&mut self, request: KvScanRequest) -> Result<KvKeyPage, LixError> {
        self.transaction
            .scan_keys(request.into())
            .await
            .map(Into::into)
    }

    async fn scan_values(&mut self, request: KvScanRequest) -> Result<KvValuePage, LixError> {
        self.transaction
            .scan_values(request.into())
            .await
            .map(Into::into)
    }

    async fn scan_entries(&mut self, request: KvScanRequest) -> Result<KvEntryPage, LixError> {
        self.transaction
            .scan_entries(request.into())
            .await
            .map(Into::into)
    }
}

#[async_trait]
impl StorageWriter for StorageContextWriteTransaction {
    async fn write_kv_batch(&mut self, batch: KvWriteBatch) -> Result<KvWriteStats, LixError> {
        self.transaction
            .write_kv_batch(batch.into())
            .await
            .map(Into::into)
    }
}

#[async_trait]
impl StorageReadTransaction for StorageContextWriteTransaction {
    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        self.transaction.rollback().await
    }
}

#[async_trait]
impl StorageWriteTransaction for StorageContextWriteTransaction {
    async fn commit(self: Box<Self>) -> Result<(), LixError> {
        self.transaction.commit().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::backend::testing::UnitTestBackend;
    use crate::storage::types::KvWriteBatch;
    use crate::storage::{KvGetGroup, KvScanRange, StorageWriteSet};

    use super::*;

    #[tokio::test]
    async fn storage_context_roundtrips_batched_writes_and_reads() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend);
        let mut tx = storage
            .begin_write_transaction()
            .await
            .expect("transaction opens");

        let mut batch = KvWriteBatch::new();
        batch.put("ns", b"a".to_vec(), b"1".to_vec());
        batch.put("ns", b"b".to_vec(), b"2".to_vec());
        let stats = tx.write_kv_batch(batch).await.expect("batch writes");
        assert_eq!(stats.puts, 2);
        tx.commit().await.expect("commit succeeds");

        let mut tx = storage
            .begin_read_transaction()
            .await
            .expect("read transaction opens");
        let result = tx
            .get_values(KvGetRequest {
                groups: vec![KvGetGroup {
                    namespace: "ns".to_string(),
                    keys: vec![b"a".to_vec(), b"b".to_vec()],
                }],
            })
            .await
            .expect("batch reads");
        assert_eq!(result.groups[0].value(0), Some(Some(b"1".as_slice())));
        assert_eq!(result.groups[0].value(1), Some(Some(b"2".as_slice())));

        let exists = tx
            .exists_many(KvGetRequest {
                groups: vec![KvGetGroup {
                    namespace: "ns".to_string(),
                    keys: vec![b"a".to_vec(), b"missing".to_vec()],
                }],
            })
            .await
            .expect("existence reads");
        assert_eq!(exists.groups[0].exists, vec![true, false]);

        let result = tx
            .scan_entries(KvScanRequest {
                namespace: "ns".to_string(),
                range: KvScanRange::prefix(Vec::new()),
                after: Some(b"a".to_vec()),
                limit: 1,
            })
            .await
            .expect("scan reads");
        assert_eq!(result.key(0).expect("key exists"), b"b");
        assert_eq!(result.value(0).expect("value exists"), b"2");

        let key_only = tx
            .scan_keys(KvScanRequest {
                namespace: "ns".to_string(),
                range: KvScanRange::prefix(Vec::new()),
                after: None,
                limit: 2,
            })
            .await
            .expect("key-only scan reads");
        assert_eq!(key_only.keys.iter().collect::<Vec<_>>(), vec![b"a", b"b"]);
        tx.rollback().await.expect("rollback succeeds");
    }

    #[tokio::test]
    async fn storage_write_set_applies_as_one_batch() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend);
        let mut tx = storage
            .begin_write_transaction()
            .await
            .expect("transaction opens");

        let mut writes = StorageWriteSet::new();
        assert!(writes.is_empty());
        writes.put("ns", b"a".to_vec(), b"1".to_vec());
        writes.put("ns", b"b".to_vec(), b"2".to_vec());
        writes.delete("ns", b"missing".to_vec());
        assert!(!writes.is_empty());

        let stats = writes.apply(tx.as_mut()).await.expect("write set applies");
        assert_eq!(stats.puts, 2);
        assert_eq!(stats.deletes, 1);
        tx.commit().await.expect("commit succeeds");

        let mut tx = storage
            .begin_read_transaction()
            .await
            .expect("read transaction opens");
        let result = tx
            .get_values(KvGetRequest {
                groups: vec![KvGetGroup {
                    namespace: "ns".to_string(),
                    keys: vec![b"a".to_vec(), b"b".to_vec()],
                }],
            })
            .await
            .expect("batch reads");
        assert_eq!(result.groups[0].value(0), Some(Some(&b"1"[..])));
        assert_eq!(result.groups[0].value(1), Some(Some(&b"2"[..])));
        tx.rollback().await.expect("rollback succeeds");
    }
}
