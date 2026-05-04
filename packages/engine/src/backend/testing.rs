use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::backend::{
    Backend, BackendKvGetBatch, BackendKvGetBatchGroup, BackendKvGetEntry, BackendKvGetRequest,
    BackendKvScanBatch, BackendKvScanProjection, BackendKvScanRange, BackendKvScanRequest,
    BackendKvScanRow, BackendKvWriteBatch, BackendKvWriteStats, BackendReadTransaction,
    BackendWriteTransaction,
};
use crate::LixError;

type KvMap = BTreeMap<(String, Vec<u8>), Vec<u8>>;

/// In-memory backend for unit tests that need backend KV semantics without SQL.
///
/// SQL execution intentionally returns an error so new tests do not accidentally
/// couple to raw SQL while exercising storage-facing APIs.
#[derive(Debug, Clone, Default)]
pub(crate) struct UnitTestBackend {
    kv: Arc<Mutex<KvMap>>,
}

impl UnitTestBackend {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Backend for UnitTestBackend {
    async fn begin_read_transaction(
        &self,
    ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError> {
        let snapshot = self
            .kv
            .lock()
            .map_err(|_| lock_error("unit test backend kv"))?
            .clone();
        Ok(Box::new(UnitTestTransaction {
            parent: Arc::clone(&self.kv),
            kv: snapshot,
        }))
    }

    async fn begin_write_transaction(
        &self,
    ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError> {
        let snapshot = self
            .kv
            .lock()
            .map_err(|_| lock_error("unit test backend kv"))?
            .clone();
        Ok(Box::new(UnitTestTransaction {
            parent: Arc::clone(&self.kv),
            kv: snapshot,
        }))
    }
}

struct UnitTestTransaction {
    parent: Arc<Mutex<KvMap>>,
    kv: KvMap,
}

#[async_trait]
impl BackendReadTransaction for UnitTestTransaction {
    async fn get_kv_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvGetBatch, LixError> {
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            let mut entries = Vec::with_capacity(group.keys.len());
            for key in group.keys {
                entries.push(BackendKvGetEntry::for_projection(
                    self.kv.get(&(group.namespace.clone(), key)).cloned(),
                    request.projection,
                ));
            }
            groups.push(BackendKvGetBatchGroup {
                namespace: group.namespace,
                entries,
            });
        }
        Ok(BackendKvGetBatch { groups })
    }

    async fn scan_kv(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvScanBatch, LixError> {
        Ok(scan_map_request(&self.kv, request))
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        Ok(())
    }
}

#[async_trait]
impl BackendWriteTransaction for UnitTestTransaction {
    async fn write_kv_batch(
        &mut self,
        batch: BackendKvWriteBatch,
    ) -> Result<BackendKvWriteStats, LixError> {
        let mut stats = BackendKvWriteStats::default();
        for group in batch.groups {
            for put in group.puts {
                stats.puts += 1;
                stats.bytes_written += put.key.len() + put.value.len();
                self.kv
                    .insert((group.namespace.clone(), put.key), put.value);
            }
            for key in group.deletes {
                stats.deletes += 1;
                stats.bytes_written += key.len();
                self.kv.remove(&(group.namespace.clone(), key));
            }
        }
        Ok(stats)
    }

    async fn commit(self: Box<Self>) -> Result<(), LixError> {
        *self
            .parent
            .lock()
            .map_err(|_| lock_error("unit test backend kv"))? = self.kv;
        Ok(())
    }
}

#[async_trait]
impl Backend for Arc<UnitTestBackend> {
    async fn begin_read_transaction(
        &self,
    ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError> {
        self.as_ref().begin_read_transaction().await
    }

    async fn begin_write_transaction(
        &self,
    ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError> {
        self.as_ref().begin_write_transaction().await
    }
}

pub(crate) fn scan_map(
    kv: &KvMap,
    namespace: &str,
    range: &BackendKvScanRange,
    limit: Option<usize>,
    projection: BackendKvScanProjection,
) -> Vec<BackendKvScanRow> {
    let mut pairs = kv
        .iter()
        .filter(|((candidate_namespace, key), _)| {
            candidate_namespace == namespace && key_matches_range(key, range)
        })
        .map(|((_, key), value)| {
            BackendKvScanRow::for_projection(key.clone(), value.clone(), projection)
        })
        .collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.key.cmp(&right.key));
    if let Some(limit) = limit {
        pairs.truncate(limit);
    }
    pairs
}

fn scan_map_request(kv: &KvMap, request: BackendKvScanRequest) -> BackendKvScanBatch {
    let scan_limit = request
        .limit
        .checked_add(1 + usize::from(request.after.is_some()))
        .unwrap_or(request.limit);
    let rows = scan_map(
        kv,
        &request.namespace,
        &request.range,
        Some(scan_limit),
        request.projection,
    );
    let mut rows = rows
        .into_iter()
        .filter(|row| {
            request
                .after
                .as_deref()
                .is_none_or(|after| row.key.as_slice() > after)
        })
        .collect::<Vec<_>>();
    let has_more = rows.len() > request.limit;
    rows.truncate(request.limit);
    let resume_after = has_more
        .then(|| rows.last().map(|row| row.key.clone()))
        .flatten();
    BackendKvScanBatch { rows, resume_after }
}

fn key_matches_range(key: &[u8], range: &BackendKvScanRange) -> bool {
    match range {
        BackendKvScanRange::Prefix(prefix) => key.starts_with(prefix),
        BackendKvScanRange::Range { start, end } => start.as_slice() <= key && key < end.as_slice(),
    }
}

fn lock_error(name: &str) -> LixError {
    LixError::new("LIX_ERROR_UNKNOWN", format!("{name} lock poisoned"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{
        BackendKvGetGroup, BackendKvGetRequest, BackendKvPut, BackendKvScanRequest,
        BackendKvWriteBatch, BackendKvWriteGroup,
    };

    async fn put(
        transaction: &mut (dyn BackendWriteTransaction + Send + Sync),
        namespace: &str,
        key: &[u8],
        value: &[u8],
    ) {
        transaction
            .write_kv_batch(BackendKvWriteBatch {
                groups: vec![BackendKvWriteGroup {
                    namespace: namespace.to_string(),
                    puts: vec![BackendKvPut {
                        key: key.to_vec(),
                        value: value.to_vec(),
                    }],
                    deletes: Vec::new(),
                }],
            })
            .await
            .expect("put should succeed");
    }

    async fn delete(
        transaction: &mut (dyn BackendWriteTransaction + Send + Sync),
        namespace: &str,
        key: &[u8],
    ) {
        transaction
            .write_kv_batch(BackendKvWriteBatch {
                groups: vec![BackendKvWriteGroup {
                    namespace: namespace.to_string(),
                    puts: Vec::new(),
                    deletes: vec![key.to_vec()],
                }],
            })
            .await
            .expect("delete should succeed");
    }

    async fn get(backend: &UnitTestBackend, namespace: &str, key: &[u8]) -> Option<Vec<u8>> {
        let mut transaction = backend
            .begin_read_transaction()
            .await
            .expect("read transaction should open");
        let result = transaction
            .get_kv_many(BackendKvGetRequest {
                groups: vec![BackendKvGetGroup {
                    namespace: namespace.to_string(),
                    keys: vec![key.to_vec()],
                }],
                projection: crate::backend::BackendKvGetProjection::Values,
            })
            .await
            .expect("get should succeed");
        transaction
            .rollback()
            .await
            .expect("rollback should succeed");
        result
            .groups
            .into_iter()
            .next()
            .and_then(|mut group| group.entries.pop())
            .and_then(|entry| entry.value)
    }

    async fn scan(
        backend: &UnitTestBackend,
        namespace: &str,
        range: BackendKvScanRange,
        limit: usize,
    ) -> Vec<BackendKvScanRow> {
        let mut transaction = backend
            .begin_read_transaction()
            .await
            .expect("read transaction should open");
        let result = transaction
            .scan_kv(BackendKvScanRequest {
                namespace: namespace.to_string(),
                range,
                after: None,
                limit,
                projection: BackendKvScanProjection::KeysAndValues,
            })
            .await
            .expect("scan should succeed");
        transaction
            .rollback()
            .await
            .expect("rollback should succeed");
        result.rows
    }

    async fn scan_request(
        backend: &UnitTestBackend,
        after: Option<&[u8]>,
        limit: usize,
        projection: BackendKvScanProjection,
    ) -> BackendKvScanBatch {
        let mut transaction = backend
            .begin_read_transaction()
            .await
            .expect("read transaction should open");
        let result = transaction
            .scan_kv(BackendKvScanRequest {
                namespace: "ns".to_string(),
                range: BackendKvScanRange::prefix(Vec::new()),
                after: after.map(Vec::from),
                limit,
                projection,
            })
            .await
            .expect("scan should succeed");
        transaction
            .rollback()
            .await
            .expect("rollback should succeed");
        result
    }

    #[tokio::test]
    async fn committed_put_is_visible_to_backend_reads() {
        let backend = UnitTestBackend::new();
        let mut transaction = backend
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        put(transaction.as_mut(), "live_state", b"key", b"value").await;
        transaction.commit().await.expect("commit should succeed");

        assert_eq!(
            get(&backend, "live_state", b"key").await,
            Some(b"value".to_vec())
        );
    }

    #[tokio::test]
    async fn rollback_discards_puts() {
        let backend = UnitTestBackend::new();
        let mut transaction = backend
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        put(transaction.as_mut(), "live_state", b"key", b"value").await;
        transaction
            .rollback()
            .await
            .expect("rollback should succeed");

        assert_eq!(get(&backend, "live_state", b"key").await, None);
    }

    #[tokio::test]
    async fn close_is_idempotent_and_does_not_destroy_data() {
        let backend = UnitTestBackend::new();
        let mut transaction = backend
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        put(transaction.as_mut(), "live_state", b"key", b"value").await;
        transaction.commit().await.expect("commit should succeed");

        backend.close().await.expect("first close should succeed");
        backend.close().await.expect("second close should succeed");

        assert_eq!(
            get(&backend, "live_state", b"key").await,
            Some(b"value".to_vec())
        );
    }

    #[tokio::test]
    async fn delete_removes_key_on_commit() {
        let backend = UnitTestBackend::new();
        let mut seed = backend
            .begin_write_transaction()
            .await
            .expect("seed transaction should open");
        put(seed.as_mut(), "live_state", b"key", b"value").await;
        seed.commit().await.expect("seed commit should succeed");

        let mut transaction = backend
            .begin_write_transaction()
            .await
            .expect("delete transaction should open");
        delete(transaction.as_mut(), "live_state", b"key").await;
        transaction.commit().await.expect("commit should succeed");

        assert_eq!(get(&backend, "live_state", b"key").await, None);
    }

    #[tokio::test]
    async fn prefix_scan_returns_lexicographic_order_with_limit() {
        let backend = UnitTestBackend::new();
        let mut transaction = backend
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        put(transaction.as_mut(), "ns", b"b/2", b"2").await;
        put(transaction.as_mut(), "ns", b"a/2", b"2").await;
        put(transaction.as_mut(), "ns", b"a/1", b"1").await;
        put(transaction.as_mut(), "other", b"a/0", b"0").await;
        transaction.commit().await.unwrap();

        let pairs = scan(&backend, "ns", BackendKvScanRange::prefix(b"a/"), 1).await;
        assert_eq!(pairs, vec![BackendKvScanRow::new(b"a/1", b"1")]);
    }

    #[tokio::test]
    async fn scan_sets_resume_after_only_when_more_rows_exist() {
        let backend = UnitTestBackend::new();
        let mut transaction = backend
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        put(transaction.as_mut(), "ns", b"a", b"1").await;
        put(transaction.as_mut(), "ns", b"b", b"2").await;
        put(transaction.as_mut(), "ns", b"c", b"3").await;
        transaction.commit().await.unwrap();

        let first_page =
            scan_request(&backend, None, 2, BackendKvScanProjection::KeysAndValues).await;
        assert_eq!(
            first_page.rows,
            vec![
                BackendKvScanRow::new(b"a", b"1"),
                BackendKvScanRow::new(b"b", b"2")
            ]
        );
        assert_eq!(first_page.resume_after, Some(b"b".to_vec()));

        let second_page = scan_request(
            &backend,
            first_page.resume_after.as_deref(),
            2,
            BackendKvScanProjection::KeysAndValues,
        )
        .await;
        assert_eq!(second_page.rows, vec![BackendKvScanRow::new(b"c", b"3")]);
        assert_eq!(second_page.resume_after, None);
    }

    #[tokio::test]
    async fn scan_exact_page_size_has_no_resume_after() {
        let backend = UnitTestBackend::new();
        let mut transaction = backend
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        put(transaction.as_mut(), "ns", b"a", b"1").await;
        put(transaction.as_mut(), "ns", b"b", b"2").await;
        transaction.commit().await.unwrap();

        let page = scan_request(&backend, None, 2, BackendKvScanProjection::KeysAndValues).await;
        assert_eq!(
            page.rows,
            vec![
                BackendKvScanRow::new(b"a", b"1"),
                BackendKvScanRow::new(b"b", b"2")
            ]
        );
        assert_eq!(page.resume_after, None);
    }

    #[tokio::test]
    async fn key_only_scan_omits_values() {
        let backend = UnitTestBackend::new();
        let mut transaction = backend
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        put(transaction.as_mut(), "ns", b"a", b"1").await;
        put(transaction.as_mut(), "ns", b"b", b"2").await;
        transaction.commit().await.unwrap();

        let page = scan_request(&backend, None, 2, BackendKvScanProjection::KeysOnly).await;
        assert_eq!(
            page.rows,
            vec![
                BackendKvScanRow::key_only(b"a"),
                BackendKvScanRow::key_only(b"b")
            ]
        );
        assert_eq!(page.resume_after, None);
    }

    #[tokio::test]
    async fn existence_get_omits_values() {
        let backend = UnitTestBackend::new();
        let mut transaction = backend
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        put(transaction.as_mut(), "ns", b"a", b"1").await;
        transaction.commit().await.unwrap();

        let mut transaction = backend
            .begin_read_transaction()
            .await
            .expect("read transaction should open");
        let result = transaction
            .get_kv_many(BackendKvGetRequest {
                groups: vec![BackendKvGetGroup {
                    namespace: "ns".to_string(),
                    keys: vec![b"a".to_vec(), b"missing".to_vec()],
                }],
                projection: crate::backend::BackendKvGetProjection::Existence,
            })
            .await
            .expect("existence get should succeed");
        transaction
            .rollback()
            .await
            .expect("rollback should succeed");

        assert_eq!(
            result.groups[0].entries,
            vec![BackendKvGetEntry::exists(), BackendKvGetEntry::missing()]
        );
    }

    #[tokio::test]
    async fn range_scan_is_half_open() {
        let backend = UnitTestBackend::new();
        let mut transaction = backend
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        put(transaction.as_mut(), "ns", b"a", b"a").await;
        put(transaction.as_mut(), "ns", b"b", b"b").await;
        put(transaction.as_mut(), "ns", b"c", b"c").await;
        transaction.commit().await.unwrap();

        let pairs = scan(
            &backend,
            "ns",
            BackendKvScanRange::range(b"a", b"c"),
            usize::MAX,
        )
        .await;
        assert_eq!(
            pairs,
            vec![
                BackendKvScanRow::new(b"a", b"a"),
                BackendKvScanRow::new(b"b", b"b")
            ]
        );
    }
}
