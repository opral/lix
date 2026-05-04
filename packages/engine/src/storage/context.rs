use std::sync::Arc;

use async_trait::async_trait;

use crate::backend::{Backend, BackendReadTransaction, BackendWriteTransaction};
use crate::storage::{
    KvGetBatch, KvGetRequest, KvScanBatch, KvScanRequest, KvWriteBatch, KvWriteStats,
    StorageReadTransaction, StorageReader, StorageWriteTransaction, StorageWriter,
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
    async fn get_kv_many(&mut self, request: KvGetRequest) -> Result<KvGetBatch, LixError> {
        let mut transaction = self.begin_read_transaction().await?;
        let result = transaction.get_kv_many(request).await;
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

    async fn scan_kv(&mut self, request: KvScanRequest) -> Result<KvScanBatch, LixError> {
        let mut transaction = self.begin_read_transaction().await?;
        let result = transaction.scan_kv(request).await;
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
    async fn get_kv_many(&mut self, request: KvGetRequest) -> Result<KvGetBatch, LixError> {
        self.transaction
            .get_kv_many(request.into())
            .await
            .map(Into::into)
    }

    async fn scan_kv(&mut self, request: KvScanRequest) -> Result<KvScanBatch, LixError> {
        self.transaction
            .scan_kv(request.into())
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
    async fn get_kv_many(&mut self, request: KvGetRequest) -> Result<KvGetBatch, LixError> {
        self.transaction
            .get_kv_many(request.into())
            .await
            .map(Into::into)
    }

    async fn scan_kv(&mut self, request: KvScanRequest) -> Result<KvScanBatch, LixError> {
        self.transaction
            .scan_kv(request.into())
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
    use crate::storage::{
        KvGetGroup, KvGetProjection, KvScanProjection, KvScanRange, KvWriteBatch,
    };

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
            .get_kv_many(KvGetRequest {
                groups: vec![KvGetGroup {
                    namespace: "ns".to_string(),
                    keys: vec![b"a".to_vec(), b"b".to_vec()],
                }],
                projection: KvGetProjection::Values,
            })
            .await
            .expect("batch reads");
        assert_eq!(result.groups[0].rows.key(0), Some(&b"a"[..]));
        assert_eq!(result.groups[0].rows.value(0), Some(&b"1"[..]));
        assert_eq!(result.groups[0].rows.key(1), Some(&b"b"[..]));
        assert_eq!(result.groups[0].rows.value(1), Some(&b"2"[..]));

        let exists = tx
            .get_kv_many(KvGetRequest {
                groups: vec![KvGetGroup {
                    namespace: "ns".to_string(),
                    keys: vec![b"a".to_vec(), b"missing".to_vec()],
                }],
                projection: KvGetProjection::Existence,
            })
            .await
            .expect("existence reads");
        assert_eq!(exists.groups[0].rows.key(0), Some(&b"a"[..]));
        assert!(exists.groups[0].rows.exists(0));
        assert_eq!(exists.groups[0].rows.value(0), None);
        assert_eq!(exists.groups[0].rows.key(1), Some(&b"missing"[..]));
        assert!(!exists.groups[0].rows.exists(1));

        let result = tx
            .scan_kv(KvScanRequest {
                namespace: "ns".to_string(),
                range: KvScanRange::prefix(Vec::new()),
                after: Some(b"a".to_vec()),
                limit: 1,
                projection: KvScanProjection::KeysAndValues,
            })
            .await
            .expect("scan reads");
        assert_eq!(result.rows.key(0), Some(&b"b"[..]));
        assert_eq!(result.rows.value(0), Some(&b"2"[..]));

        let key_only = tx
            .scan_kv(KvScanRequest {
                namespace: "ns".to_string(),
                range: KvScanRange::prefix(Vec::new()),
                after: None,
                limit: 2,
                projection: KvScanProjection::KeysOnly,
            })
            .await
            .expect("key-only scan reads");
        assert_eq!(key_only.rows.key(0), Some(&b"a"[..]));
        assert_eq!(key_only.rows.value(0), None);
        assert_eq!(key_only.rows.key(1), Some(&b"b"[..]));
        assert_eq!(key_only.rows.value(1), None);
        tx.rollback().await.expect("rollback succeeds");
    }
}
