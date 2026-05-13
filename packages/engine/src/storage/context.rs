use std::sync::Arc;

use async_trait::async_trait;

use crate::backend::{Backend, BackendReadTransaction, BackendWriteTransaction};
use crate::storage::types::{KvWriteBatch, StorageWriter};
use crate::storage::{
    KvEntryPage, KvExistsBatch, KvGetRequest, KvKeyPage, KvRead4Page, KvReadV3Page,
    KvReadV3Request, KvScan2Page, KvScan2Request, KvScanRequest, KvTableReadRequest, KvValueBatch,
    KvValuePage, KvWriteStats, StorageReadTransaction, StorageReader, StorageWriteTransaction,
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

    async fn scan2(&mut self, request: KvScan2Request) -> Result<KvScan2Page, LixError> {
        let mut transaction = self.begin_read_transaction().await?;
        let result = transaction.scan2(request).await;
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

    async fn read_v3(&mut self, request: KvReadV3Request) -> Result<KvReadV3Page, LixError> {
        let mut transaction = self.begin_read_transaction().await?;
        let result = transaction.read_v3(request).await;
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

    async fn read4(&mut self, request: KvTableReadRequest) -> Result<KvRead4Page, LixError> {
        let mut transaction = self.begin_read_transaction().await?;
        let result = transaction.read4(request).await;
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

    async fn scan2(&mut self, request: KvScan2Request) -> Result<KvScan2Page, LixError> {
        self.transaction.scan2(request.into()).await.map(Into::into)
    }

    async fn read_v3(&mut self, request: KvReadV3Request) -> Result<KvReadV3Page, LixError> {
        self.transaction
            .read_v3(request.into())
            .await
            .map(Into::into)
    }

    async fn read4(&mut self, request: KvTableReadRequest) -> Result<KvRead4Page, LixError> {
        self.transaction.read4(request.into()).await.map(Into::into)
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

    async fn scan2(&mut self, request: KvScan2Request) -> Result<KvScan2Page, LixError> {
        self.transaction.scan2(request.into()).await.map(Into::into)
    }

    async fn read_v3(&mut self, request: KvReadV3Request) -> Result<KvReadV3Page, LixError> {
        self.transaction
            .read_v3(request.into())
            .await
            .map(Into::into)
    }

    async fn read4(&mut self, request: KvTableReadRequest) -> Result<KvRead4Page, LixError> {
        self.transaction.read4(request.into()).await.map(Into::into)
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
    use crate::storage::{
        KvGetGroup, KvHeaderPayloadFramePart, KvKeySpan, KvReadV3Order, KvReadV3Projection,
        KvReadV3Request, KvReadV3Source, KvReadV3Strategy, KvReadV3ValuePart, KvScan2Projection,
        KvScanRange, KvValuePart, StorageWriteSet,
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

    #[tokio::test]
    async fn scan2_keys_only_prefix_scan_returns_ordered_keys_and_cursor() {
        let storage = seeded_scan2_storage().await;
        let mut tx = storage
            .begin_read_transaction()
            .await
            .expect("read transaction opens");

        let page = tx
            .scan2(KvScan2Request {
                namespace: "primary".to_string(),
                range: KvScanRange::prefix(b"a/"),
                after: None,
                page_size: 1,
                projection: KvScan2Projection::KeysOnly,
            })
            .await
            .expect("scan2 reads");

        assert_eq!(page.keys.iter().collect::<Vec<_>>(), vec![b"a/1"]);
        assert_eq!(page.values, None);
        assert_eq!(page.resume_after, Some(b"a/1".to_vec()));
        tx.rollback().await.expect("rollback succeeds");
    }

    #[tokio::test]
    async fn scan2_full_values_match_scan_entries() {
        let storage = seeded_scan2_storage().await;
        let mut tx = storage
            .begin_read_transaction()
            .await
            .expect("read transaction opens");

        let entries = tx
            .scan_entries(KvScanRequest {
                namespace: "primary".to_string(),
                range: KvScanRange::prefix(Vec::new()),
                after: None,
                limit: 3,
            })
            .await
            .expect("scan entries reads");
        let page = tx
            .scan2(KvScan2Request {
                namespace: "primary".to_string(),
                range: KvScanRange::prefix(Vec::new()),
                after: None,
                page_size: 3,
                projection: KvScan2Projection::FullValue,
            })
            .await
            .expect("scan2 reads");

        assert_eq!(page.keys, entries.keys);
        let values = page.values.as_ref().expect("values should be projected");
        for index in 0..entries.len() {
            assert_eq!(
                values.get(index),
                Some(entries.value(index).expect("entry value exists"))
            );
        }
        assert_eq!(page.resume_after, entries.resume_after);
        tx.rollback().await.expect("rollback succeeds");
    }

    #[tokio::test]
    async fn scan2_value_part_projection_returns_framed_slices() {
        let storage = seeded_scan2_storage().await;
        let mut tx = storage
            .begin_read_transaction()
            .await
            .expect("read transaction opens");

        let page = tx
            .scan2(KvScan2Request {
                namespace: "packed".to_string(),
                range: KvScanRange::prefix(Vec::new()),
                after: None,
                page_size: 2,
                projection: KvScan2Projection::ValuePart(KvValuePart::HeaderPayloadFrame(
                    KvHeaderPayloadFramePart::Header,
                )),
            })
            .await
            .expect("scan2 reads");

        assert_eq!(page.keys.iter().collect::<Vec<_>>(), vec![b"k1", b"k2"]);
        let values = page.values.as_ref().expect("values should be projected");
        assert_eq!(values.get(0), Some(b"h1".as_slice()));
        assert_eq!(values.get(1), Some(b"h2".as_slice()));

        let payload_page = tx
            .scan2(KvScan2Request {
                namespace: "packed".to_string(),
                range: KvScanRange::prefix(Vec::new()),
                after: None,
                page_size: 2,
                projection: KvScan2Projection::ValuePart(KvValuePart::HeaderPayloadFrame(
                    KvHeaderPayloadFramePart::Payload,
                )),
            })
            .await
            .expect("scan2 payload reads");
        let values = payload_page
            .values
            .as_ref()
            .expect("values should be projected");
        assert_eq!(values.get(0), Some(b"payload-1".as_slice()));
        assert_eq!(values.get(1), Some(b"payload-2".as_slice()));
        tx.rollback().await.expect("rollback succeeds");
    }

    #[tokio::test]
    async fn scan2_malformed_framed_projection_returns_error() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend);
        let mut tx = storage
            .begin_write_transaction()
            .await
            .expect("transaction opens");
        let mut writes = StorageWriteSet::new();
        writes.put("packed", b"k".to_vec(), b"short".to_vec());
        writes.apply(tx.as_mut()).await.expect("writes");
        tx.commit().await.expect("commit succeeds");

        let mut tx = storage
            .begin_read_transaction()
            .await
            .expect("read transaction opens");
        let error = tx
            .scan2(KvScan2Request {
                namespace: "packed".to_string(),
                range: KvScanRange::prefix(Vec::new()),
                after: None,
                page_size: 1,
                projection: KvScan2Projection::ValuePart(KvValuePart::HeaderPayloadFrame(
                    KvHeaderPayloadFramePart::Header,
                )),
            })
            .await
            .expect_err("malformed frame should fail");
        assert!(error.message.contains("frame"));
        tx.rollback().await.expect("rollback succeeds");
    }

    #[tokio::test]
    async fn scan2_paging_with_after_resumes_after_previous_key() {
        let storage = seeded_scan2_storage().await;
        let mut tx = storage
            .begin_read_transaction()
            .await
            .expect("read transaction opens");

        let first = tx
            .scan2(KvScan2Request {
                namespace: "primary".to_string(),
                range: KvScanRange::prefix(Vec::new()),
                after: None,
                page_size: 2,
                projection: KvScan2Projection::KeysOnly,
            })
            .await
            .expect("first page reads");
        let second = tx
            .scan2(KvScan2Request {
                namespace: "primary".to_string(),
                range: KvScanRange::prefix(Vec::new()),
                after: first.resume_after.clone(),
                page_size: 2,
                projection: KvScan2Projection::KeysOnly,
            })
            .await
            .expect("second page reads");

        assert_eq!(first.keys.iter().collect::<Vec<_>>(), vec![b"a/1", b"a/2"]);
        assert_eq!(first.resume_after, Some(b"a/2".to_vec()));
        assert_eq!(second.keys.iter().collect::<Vec<_>>(), vec![b"b/1"]);
        assert_eq!(second.resume_after, None);
        tx.rollback().await.expect("rollback succeeds");
    }

    #[tokio::test]
    async fn read_v3_keys_only_multi_span_returns_global_order_and_cursor() {
        let storage = seeded_scan2_storage().await;
        let mut tx = storage
            .begin_read_transaction()
            .await
            .expect("read transaction opens");

        let first = tx
            .read_v3(KvReadV3Request {
                namespace: "primary".to_string(),
                source: KvReadV3Source::Spans {
                    spans: vec![span_prefix(b"a/"), span_prefix(b"b/")],
                    after: None,
                },
                projection: KvReadV3Projection::KeysOnly,
                order: KvReadV3Order::KeyOrder,
                page_size: Some(2),
                strategy: KvReadV3Strategy::Scan,
            })
            .await
            .expect("first read_v3 page reads");
        let second = tx
            .read_v3(KvReadV3Request {
                namespace: "primary".to_string(),
                source: KvReadV3Source::Spans {
                    spans: vec![span_prefix(b"a/"), span_prefix(b"b/")],
                    after: first.resume_after.clone(),
                },
                projection: KvReadV3Projection::KeysOnly,
                order: KvReadV3Order::KeyOrder,
                page_size: Some(2),
                strategy: KvReadV3Strategy::Scan,
            })
            .await
            .expect("second read_v3 page reads");

        assert_eq!(first.keys.iter().collect::<Vec<_>>(), vec![b"a/1", b"a/2"]);
        assert_eq!(first.resume_after, Some(b"a/2".to_vec()));
        assert_eq!(second.keys.iter().collect::<Vec<_>>(), vec![b"b/1"]);
        assert_eq!(second.resume_after, None);
        assert!(first.values.is_empty());
        tx.rollback().await.expect("rollback succeeds");
    }

    #[tokio::test]
    async fn read_v3_full_values_match_scan_entries() {
        let storage = seeded_scan2_storage().await;
        let mut tx = storage
            .begin_read_transaction()
            .await
            .expect("read transaction opens");

        let entries = tx
            .scan_entries(KvScanRequest {
                namespace: "primary".to_string(),
                range: KvScanRange::prefix(Vec::new()),
                after: None,
                limit: 3,
            })
            .await
            .expect("scan entries reads");
        let page = tx
            .read_v3(KvReadV3Request {
                namespace: "primary".to_string(),
                source: KvReadV3Source::Spans {
                    spans: vec![KvKeySpan::all()],
                    after: None,
                },
                projection: KvReadV3Projection::ValueParts(vec![KvReadV3ValuePart::FullValue]),
                order: KvReadV3Order::KeyOrder,
                page_size: Some(3),
                strategy: KvReadV3Strategy::Scan,
            })
            .await
            .expect("read_v3 reads");

        assert_eq!(page.keys, entries.keys);
        assert_eq!(page.values.len(), 1);
        for index in 0..entries.len() {
            assert_eq!(
                page.values[0].get(index),
                Some(entries.value(index).expect("entry value exists"))
            );
        }
        assert_eq!(page.resume_after, entries.resume_after);
        tx.rollback().await.expect("rollback succeeds");
    }

    #[tokio::test]
    async fn read_v3_multiple_value_parts_return_aligned_pages() {
        let storage = seeded_scan2_storage().await;
        let mut tx = storage
            .begin_read_transaction()
            .await
            .expect("read transaction opens");

        let page = tx
            .read_v3(KvReadV3Request {
                namespace: "packed".to_string(),
                source: KvReadV3Source::Spans {
                    spans: vec![KvKeySpan::all()],
                    after: None,
                },
                projection: KvReadV3Projection::ValueParts(vec![
                    KvReadV3ValuePart::Header,
                    KvReadV3ValuePart::Payload,
                ]),
                order: KvReadV3Order::KeyOrder,
                page_size: Some(2),
                strategy: KvReadV3Strategy::Scan,
            })
            .await
            .expect("read_v3 reads");

        assert_eq!(page.keys.iter().collect::<Vec<_>>(), vec![b"k1", b"k2"]);
        assert_eq!(page.values.len(), 2);
        assert_eq!(page.values[0].get(0), Some(b"h1".as_slice()));
        assert_eq!(page.values[0].get(1), Some(b"h2".as_slice()));
        assert_eq!(page.values[1].get(0), Some(b"payload-1".as_slice()));
        assert_eq!(page.values[1].get(1), Some(b"payload-2".as_slice()));
        tx.rollback().await.expect("rollback succeeds");
    }

    #[tokio::test]
    async fn read_v3_projected_points_preserve_request_order_and_misses() {
        let storage = seeded_scan2_storage().await;
        let mut tx = storage
            .begin_read_transaction()
            .await
            .expect("read transaction opens");

        let page = tx
            .read_v3(KvReadV3Request {
                namespace: "packed".to_string(),
                source: KvReadV3Source::Keys {
                    keys: vec![b"k2".to_vec(), b"missing".to_vec(), b"k1".to_vec()],
                },
                projection: KvReadV3Projection::ValueParts(vec![KvReadV3ValuePart::Header]),
                order: KvReadV3Order::RequestOrder,
                page_size: None,
                strategy: KvReadV3Strategy::Auto,
            })
            .await
            .expect("read_v3 reads");

        assert_eq!(
            page.keys.iter().collect::<Vec<_>>(),
            vec![b"k2".as_slice(), b"missing".as_slice(), b"k1".as_slice()]
        );
        assert_eq!(page.presence_vec(), vec![true, false, true]);
        assert_eq!(page.values.len(), 1);
        assert_eq!(page.values[0].get(0), Some(b"h2".as_slice()));
        assert_eq!(page.values[0].get(1), Some(b"".as_slice()));
        assert_eq!(page.values[0].get(2), Some(b"h1".as_slice()));
        tx.rollback().await.expect("rollback succeeds");
    }

    #[tokio::test]
    async fn read_v3_span_scan_uses_key_order_and_cursor() {
        let storage = seeded_scan2_storage().await;
        let mut tx = storage
            .begin_read_transaction()
            .await
            .expect("read transaction opens");

        let page = tx
            .read_v3(KvReadV3Request {
                namespace: "primary".to_string(),
                source: KvReadV3Source::Spans {
                    spans: vec![KvKeySpan::new(b"a/".to_vec(), b"a0".to_vec())],
                    after: None,
                },
                projection: KvReadV3Projection::KeysOnly,
                order: KvReadV3Order::KeyOrder,
                page_size: Some(1),
                strategy: KvReadV3Strategy::Scan,
            })
            .await
            .expect("read_v3 scans");

        assert_eq!(page.keys.iter().collect::<Vec<_>>(), vec![b"a/1"]);
        assert_eq!(page.presence_vec(), vec![true]);
        assert_eq!(page.resume_after, Some(b"a/1".to_vec()));
        tx.rollback().await.expect("rollback succeeds");
    }

    #[tokio::test]
    async fn read_v3_overlapping_spans_do_not_duplicate_keys() {
        let storage = seeded_scan2_storage().await;
        let mut tx = storage
            .begin_read_transaction()
            .await
            .expect("read transaction opens");

        let page = tx
            .read_v3(KvReadV3Request {
                namespace: "primary".to_string(),
                source: KvReadV3Source::Spans {
                    spans: vec![span_prefix(b"a/"), span_exact(b"a/1")],
                    after: None,
                },
                projection: KvReadV3Projection::KeysOnly,
                order: KvReadV3Order::KeyOrder,
                page_size: Some(10),
                strategy: KvReadV3Strategy::Scan,
            })
            .await
            .expect("read_v3 reads");

        assert_eq!(page.keys.iter().collect::<Vec<_>>(), vec![b"a/1", b"a/2"]);
        assert_eq!(page.resume_after, None);
        tx.rollback().await.expect("rollback succeeds");
    }

    async fn seeded_scan2_storage() -> StorageContext {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend);
        let mut tx = storage
            .begin_write_transaction()
            .await
            .expect("write transaction opens");
        let mut batch = KvWriteBatch::new();
        batch.put("primary", b"a/1".to_vec(), b"p1".to_vec());
        batch.put("primary", b"a/2".to_vec(), b"p2".to_vec());
        batch.put("primary", b"b/1".to_vec(), b"p3".to_vec());
        batch.put("joined", b"a/1".to_vec(), b"j1".to_vec());
        batch.put("joined", b"b/1".to_vec(), b"j3".to_vec());
        batch.put("other", b"a/2".to_vec(), b"other".to_vec());
        batch.put("packed", b"k1".to_vec(), framed_value(b"h1", b"payload-1"));
        batch.put("packed", b"k2".to_vec(), framed_value(b"h2", b"payload-2"));
        tx.write_kv_batch(batch).await.expect("seed writes");
        tx.commit().await.expect("seed commit succeeds");
        storage
    }

    fn framed_value(header: &[u8], payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(b"LXU2");
        out.push(0);
        out.extend_from_slice(format!("{:010}", header.len()).as_bytes());
        out.extend_from_slice(format!("{:010}", payload.len()).as_bytes());
        out.extend_from_slice(header);
        out.extend_from_slice(payload);
        out
    }

    fn span_exact(key: &[u8]) -> KvKeySpan {
        let mut end = key.to_vec();
        end.push(0);
        KvKeySpan::new(key.to_vec(), end)
    }

    fn span_prefix(prefix: &[u8]) -> KvKeySpan {
        KvKeySpan::new(
            prefix.to_vec(),
            prefix_upper_bound(prefix).expect("test prefix has an upper bound"),
        )
    }

    fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
        let mut upper = prefix.to_vec();
        for index in (0..upper.len()).rev() {
            if upper[index] != 0xFF {
                upper[index] += 1;
                upper.truncate(index + 1);
                return Some(upper);
            }
        }
        None
    }
}
