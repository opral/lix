use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::backend::{
    Backend, BackendKvEntry, BackendKvEntryPage, BackendKvExistsBatch, BackendKvExistsGroup,
    BackendKvGetRequest, BackendKvKeyPage, BackendKvScanRange, BackendKvScanRequest,
    BackendKvValueBatch, BackendKvValueGroup, BackendKvValuePage, BackendKvWriteBatch,
    BackendKvWriteStats, BackendReadTransaction, BackendWriteTransaction,
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
    async fn get_values(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvValueBatch, LixError> {
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            let values = group
                .keys
                .into_iter()
                .map(|key| self.kv.get(&(group.namespace.clone(), key)).cloned())
                .collect();
            groups.push(BackendKvValueGroup {
                namespace: group.namespace,
                values,
            });
        }
        Ok(BackendKvValueBatch { groups })
    }

    async fn exists_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvExistsBatch, LixError> {
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            let exists = group
                .keys
                .into_iter()
                .map(|key| self.kv.contains_key(&(group.namespace.clone(), key)))
                .collect();
            groups.push(BackendKvExistsGroup {
                namespace: group.namespace,
                exists,
            });
        }
        Ok(BackendKvExistsBatch { groups })
    }

    async fn scan_keys(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvKeyPage, LixError> {
        Ok(scan_map_keys(&self.kv, request))
    }

    async fn scan_values(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvValuePage, LixError> {
        Ok(scan_map_values(&self.kv, request))
    }

    async fn scan_entries(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvEntryPage, LixError> {
        Ok(scan_map_entries(&self.kv, request))
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

fn scan_pairs<'a>(
    kv: &'a KvMap,
    namespace: &str,
    range: &BackendKvScanRange,
    limit: Option<usize>,
) -> Vec<(&'a Vec<u8>, &'a Vec<u8>)> {
    let pairs = kv
        .iter()
        .filter(|((candidate_namespace, key), _)| {
            candidate_namespace == namespace && key_matches_range(key, range)
        })
        .collect::<Vec<_>>();
    let mut pairs = pairs;
    pairs.sort_by(|left, right| left.0 .1.cmp(&right.0 .1));
    if let Some(limit) = limit {
        pairs.truncate(limit);
    }
    pairs
        .into_iter()
        .map(|((_, key), value)| (key, value))
        .collect()
}

pub(crate) fn scan_map_keys(kv: &KvMap, request: BackendKvScanRequest) -> BackendKvKeyPage {
    let pairs = scan_filtered_pairs(kv, &request);
    let has_more = pairs.len() > request.limit;
    let keys = pairs
        .iter()
        .take(request.limit)
        .map(|(key, _)| (*key).clone())
        .collect::<Vec<_>>();
    let resume_after = has_more.then(|| keys.last().cloned()).flatten();
    BackendKvKeyPage { keys, resume_after }
}

pub(crate) fn scan_map_values(kv: &KvMap, request: BackendKvScanRequest) -> BackendKvValuePage {
    let pairs = scan_filtered_pairs(kv, &request);
    let has_more = pairs.len() > request.limit;
    let resume_after = has_more
        .then(|| {
            pairs
                .get(request.limit.saturating_sub(1))
                .map(|(key, _)| (*key).clone())
        })
        .flatten();
    let values = pairs
        .into_iter()
        .take(request.limit)
        .map(|(_, value)| value.clone())
        .collect();
    BackendKvValuePage {
        values,
        resume_after,
    }
}

pub(crate) fn scan_map_entries(kv: &KvMap, request: BackendKvScanRequest) -> BackendKvEntryPage {
    let pairs = scan_filtered_pairs(kv, &request);
    let has_more = pairs.len() > request.limit;
    let entries = pairs
        .iter()
        .take(request.limit)
        .map(|(key, value)| BackendKvEntry {
            key: (*key).clone(),
            value: (*value).clone(),
        })
        .collect::<Vec<_>>();
    let resume_after = has_more
        .then(|| entries.last().map(|entry| entry.key.clone()))
        .flatten();
    BackendKvEntryPage {
        entries,
        resume_after,
    }
}

fn scan_filtered_pairs<'a>(
    kv: &'a KvMap,
    request: &BackendKvScanRequest,
) -> Vec<(&'a Vec<u8>, &'a Vec<u8>)> {
    let scan_limit = request
        .limit
        .checked_add(1 + usize::from(request.after.is_some()))
        .unwrap_or(request.limit);
    scan_pairs(kv, &request.namespace, &request.range, Some(scan_limit))
        .into_iter()
        .filter(|(key, _)| {
            request
                .after
                .as_deref()
                .is_none_or(|after| key.as_slice() > after)
        })
        .collect()
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
            .get_values(BackendKvGetRequest {
                groups: vec![BackendKvGetGroup {
                    namespace: namespace.to_string(),
                    keys: vec![key.to_vec()],
                }],
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
            .and_then(|mut group| group.pop_value())
    }

    async fn scan(
        backend: &UnitTestBackend,
        namespace: &str,
        range: BackendKvScanRange,
        limit: usize,
    ) -> BackendKvEntryPage {
        let mut transaction = backend
            .begin_read_transaction()
            .await
            .expect("read transaction should open");
        let result = transaction
            .scan_entries(BackendKvScanRequest {
                namespace: namespace.to_string(),
                range,
                after: None,
                limit,
            })
            .await
            .expect("scan should succeed");
        transaction
            .rollback()
            .await
            .expect("rollback should succeed");
        result
    }

    fn assert_entries(page: &BackendKvEntryPage, expected: &[(&[u8], &[u8])]) {
        assert_eq!(page.entries.len(), expected.len());
        for (entry, (key, value)) in page.entries.iter().zip(expected) {
            assert_eq!(entry.key, *key);
            assert_eq!(entry.value, *value);
        }
    }

    async fn scan_entries_request(
        backend: &UnitTestBackend,
        after: Option<&[u8]>,
        limit: usize,
    ) -> BackendKvEntryPage {
        let mut transaction = backend
            .begin_read_transaction()
            .await
            .expect("read transaction should open");
        let result = transaction
            .scan_entries(BackendKvScanRequest {
                namespace: "ns".to_string(),
                range: BackendKvScanRange::prefix(Vec::new()),
                after: after.map(Vec::from),
                limit,
            })
            .await
            .expect("scan should succeed");
        transaction
            .rollback()
            .await
            .expect("rollback should succeed");
        result
    }

    async fn scan_keys_request(
        backend: &UnitTestBackend,
        after: Option<&[u8]>,
        limit: usize,
    ) -> BackendKvKeyPage {
        let mut transaction = backend
            .begin_read_transaction()
            .await
            .expect("read transaction should open");
        let result = transaction
            .scan_keys(BackendKvScanRequest {
                namespace: "ns".to_string(),
                range: BackendKvScanRange::prefix(Vec::new()),
                after: after.map(Vec::from),
                limit,
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
        assert_entries(&pairs, &[(b"a/1", b"1")]);
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

        let first_page = scan_entries_request(&backend, None, 2).await;
        assert_entries(&first_page, &[(b"a", b"1"), (b"b", b"2")]);
        assert_eq!(first_page.resume_after, Some(b"b".to_vec()));

        let second_page =
            scan_entries_request(&backend, first_page.resume_after.as_deref(), 2).await;
        assert_entries(&second_page, &[(b"c", b"3")]);
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

        let page = scan_entries_request(&backend, None, 2).await;
        assert_entries(&page, &[(b"a", b"1"), (b"b", b"2")]);
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

        let page = scan_keys_request(&backend, None, 2).await;
        assert_eq!(page.keys, vec![b"a".to_vec(), b"b".to_vec()]);
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
            .exists_many(BackendKvGetRequest {
                groups: vec![BackendKvGetGroup {
                    namespace: "ns".to_string(),
                    keys: vec![b"a".to_vec(), b"missing".to_vec()],
                }],
            })
            .await
            .expect("existence get should succeed");
        transaction
            .rollback()
            .await
            .expect("rollback should succeed");

        assert_eq!(result.groups[0].exists, vec![true, false]);
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
        assert_entries(&pairs, &[(b"a", b"a"), (b"b", b"b")]);
    }
}
