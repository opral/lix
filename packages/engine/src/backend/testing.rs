use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::backend::{
    project_backend_read4_value_part, Backend, BackendKvAccessSegment, BackendKvEntryPage,
    BackendKvExistsBatch, BackendKvExistsGroup, BackendKvGetRequest, BackendKvKeyPage,
    BackendKvRead4Order, BackendKvRead4Page, BackendKvRead4Projection, BackendKvReadV3Presence,
    BackendKvScanRange, BackendKvScanRequest, BackendKvTableReadRequest, BackendKvValueBatch,
    BackendKvValueGroup, BackendKvValuePage, BackendKvWriteBatch, BackendKvWriteOp,
    BackendKvWriteStats, BackendReadTransaction, BackendWriteTransaction, BytePageBuilder,
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
            let namespace = group.namespace.clone();
            let mut values = BytePageBuilder::with_capacity(group.keys.len(), 0);
            let mut present = Vec::with_capacity(group.keys.len());
            for key in group.keys {
                if let Some(value) = self.kv.get(&(namespace.clone(), key)) {
                    values.push(value);
                    present.push(true);
                } else {
                    values.push([]);
                    present.push(false);
                }
            }
            groups.push(BackendKvValueGroup::new(
                namespace,
                values.finish(),
                present,
            ));
        }
        Ok(BackendKvValueBatch { groups })
    }

    async fn exists_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvExistsBatch, LixError> {
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            let namespace = group.namespace.clone();
            let exists = group
                .keys
                .into_iter()
                .map(|key| self.kv.contains_key(&(namespace.clone(), key)))
                .collect();
            groups.push(BackendKvExistsGroup { namespace, exists });
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

    async fn read4(
        &mut self,
        request: BackendKvTableReadRequest,
    ) -> Result<BackendKvRead4Page, LixError> {
        read4_map(&self.kv, request)
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
            let namespace = group.namespace().to_string();
            for op in group.ops() {
                match op {
                    BackendKvWriteOp::Put { key, value } => {
                        stats.puts += 1;
                        stats.bytes_written += key.len() + value.len();
                        self.kv
                            .insert((namespace.clone(), key.clone()), value.clone());
                    }
                    BackendKvWriteOp::Delete { key } => {
                        stats.deletes += 1;
                        stats.bytes_written += key.len();
                        self.kv.remove(&(namespace.clone(), key.clone()));
                    }
                    BackendKvWriteOp::DeleteRange { range } => {
                        stats.delete_ranges += 1;
                        stats.bytes_written += delete_range_bytes(range);
                        self.kv.retain(|(candidate_namespace, key), _| {
                            candidate_namespace != &namespace || !key_matches_range(key, range)
                        });
                    }
                }
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
    let mut keys = BytePageBuilder::with_capacity(request.limit.min(pairs.len()), 0);
    let mut resume_after = None;
    for (index, (key, _)) in pairs.into_iter().enumerate() {
        if index >= request.limit {
            break;
        }
        resume_after = Some(key.clone());
        keys.push(key);
    }
    let resume_after = has_more.then_some(resume_after).flatten();
    BackendKvKeyPage {
        keys: keys.finish(),
        resume_after,
    }
}

pub(crate) fn scan_map_values(kv: &KvMap, request: BackendKvScanRequest) -> BackendKvValuePage {
    let pairs = scan_filtered_pairs(kv, &request);
    let has_more = pairs.len() > request.limit;
    let mut values = BytePageBuilder::with_capacity(request.limit.min(pairs.len()), 0);
    let mut resume_after = None;
    for (index, (key, value)) in pairs.into_iter().enumerate() {
        if index >= request.limit {
            break;
        }
        resume_after = Some(key.clone());
        values.push(value);
    }
    let resume_after = has_more.then_some(resume_after).flatten();
    BackendKvValuePage {
        values: values.finish(),
        resume_after,
    }
}

pub(crate) fn scan_map_entries(kv: &KvMap, request: BackendKvScanRequest) -> BackendKvEntryPage {
    let pairs = scan_filtered_pairs(kv, &request);
    let has_more = pairs.len() > request.limit;
    let mut keys = BytePageBuilder::with_capacity(request.limit.min(pairs.len()), 0);
    let mut values = BytePageBuilder::with_capacity(request.limit.min(pairs.len()), 0);
    let mut resume_after = None;
    for (index, (key, value)) in pairs.into_iter().enumerate() {
        if index >= request.limit {
            break;
        }
        resume_after = Some(key.clone());
        keys.push(key);
        values.push(value);
    }
    let resume_after = has_more.then_some(resume_after).flatten();
    BackendKvEntryPage {
        keys: keys.finish(),
        values: values.finish(),
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

fn read4_map(
    kv: &KvMap,
    request: BackendKvTableReadRequest,
) -> Result<BackendKvRead4Page, LixError> {
    if request.residual_filter.is_some() {
        return Err(LixError::unknown(
            "unit test backend read4 cannot apply residual filters",
        ));
    }
    let namespace = request.table.namespace;
    let limit = request.limit.unwrap_or(usize::MAX);
    let mut keyed = Vec::new();
    let mut spans = Vec::new();
    let mut saw_points_or_runs = false;
    let mut saw_spans = false;
    for segment in request.access {
        match segment {
            BackendKvAccessSegment::Points {
                keys,
                request_indexes,
            } => {
                saw_points_or_runs = true;
                read4_push_indexed_keys(&mut keyed, keys, request_indexes)?;
            }
            BackendKvAccessSegment::Run {
                lower,
                upper,
                keys,
                request_indexes,
            } => {
                saw_points_or_runs = true;
                spans.push(read4_scan_range(lower, upper));
                read4_push_indexed_keys(&mut keyed, keys, request_indexes)?;
            }
            BackendKvAccessSegment::Span { lower, upper } => {
                saw_spans = true;
                spans.push(read4_scan_range(lower, upper));
            }
        }
    }
    if saw_points_or_runs && saw_spans {
        return Err(LixError::unknown(
            "unit test backend read4 cannot mix spans with point/run access",
        ));
    }
    if saw_points_or_runs {
        read4_map_points(
            kv,
            namespace,
            keyed,
            request.projection,
            request.output_order,
        )
    } else {
        read4_map_spans(
            kv,
            namespace,
            spans,
            request.after,
            limit,
            request.projection,
        )
    }
}

fn read4_push_indexed_keys(
    output: &mut Vec<(u32, Vec<u8>)>,
    keys: Vec<Vec<u8>>,
    request_indexes: Vec<u32>,
) -> Result<(), LixError> {
    if keys.len() != request_indexes.len() {
        return Err(LixError::unknown(
            "unit test backend read4 key/index mismatch",
        ));
    }
    output.extend(request_indexes.into_iter().zip(keys));
    Ok(())
}

fn read4_scan_range(lower: Vec<u8>, upper: Vec<u8>) -> BackendKvScanRange {
    if lower.is_empty() && upper.is_empty() {
        BackendKvScanRange::Prefix(Vec::new())
    } else {
        BackendKvScanRange::Range {
            start: lower,
            end: upper,
        }
    }
}

fn read4_map_points(
    kv: &KvMap,
    namespace: String,
    mut keyed: Vec<(u32, Vec<u8>)>,
    projection: BackendKvRead4Projection,
    order: BackendKvRead4Order,
) -> Result<BackendKvRead4Page, LixError> {
    match order {
        BackendKvRead4Order::RequestOrder => keyed.sort_by_key(|(index, _)| *index),
        BackendKvRead4Order::KeyOrder => keyed.sort_by(|left, right| left.1.cmp(&right.1)),
    }
    let mut keys = BytePageBuilder::with_capacity(keyed.len(), 0);
    let mut present = Vec::with_capacity(keyed.len());
    let mut value_builders = read4_value_builders(&projection);
    let mut request_indexes = match order {
        BackendKvRead4Order::RequestOrder => None,
        BackendKvRead4Order::KeyOrder => Some(Vec::new()),
    };
    for (index, key) in keyed {
        let value = kv.get(&(namespace.clone(), key.clone()));
        keys.push(&key);
        present.push(value.is_some());
        if let Some(indexes) = request_indexes.as_mut() {
            indexes.push(index);
        }
        if let BackendKvRead4Projection::Parts(parts) = &projection {
            for (part, builder) in parts.iter().zip(value_builders.iter_mut()) {
                if let Some(value) = value {
                    builder.push(project_backend_read4_value_part(value, *part)?);
                } else {
                    builder.push([]);
                }
            }
        }
    }
    Ok(BackendKvRead4Page {
        keys: keys.finish(),
        presence: BackendKvReadV3Presence::bitmap(present),
        values: value_builders
            .into_iter()
            .map(BytePageBuilder::finish)
            .collect(),
        request_indexes,
        resume_after: None,
    })
}

fn read4_map_spans(
    kv: &KvMap,
    namespace: String,
    spans: Vec<BackendKvScanRange>,
    after: Option<Vec<u8>>,
    limit: usize,
    projection: BackendKvRead4Projection,
) -> Result<BackendKvRead4Page, LixError> {
    let mut pairs = Vec::new();
    for range in spans {
        pairs.extend(
            scan_pairs(kv, &namespace, &range, None)
                .into_iter()
                .filter(|(key, _)| after.as_deref().is_none_or(|after| key.as_slice() > after)),
        );
    }
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    pairs.dedup_by(|left, right| left.0 == right.0);
    let has_more = pairs.len() > limit;
    let mut keys = BytePageBuilder::with_capacity(limit.min(pairs.len()), 0);
    let mut value_builders = read4_value_builders(&projection);
    let mut resume_after = None;
    for (index, (key, value)) in pairs.into_iter().enumerate() {
        if index >= limit {
            break;
        }
        resume_after = Some(key.clone());
        keys.push(key);
        if let BackendKvRead4Projection::Parts(parts) = &projection {
            for (part, builder) in parts.iter().zip(value_builders.iter_mut()) {
                builder.push(project_backend_read4_value_part(value, *part)?);
            }
        }
    }
    Ok(BackendKvRead4Page {
        keys: keys.finish(),
        presence: BackendKvReadV3Presence::All,
        values: value_builders
            .into_iter()
            .map(BytePageBuilder::finish)
            .collect(),
        request_indexes: None,
        resume_after: has_more.then_some(resume_after).flatten(),
    })
}

fn read4_value_builders(projection: &BackendKvRead4Projection) -> Vec<BytePageBuilder> {
    match projection {
        BackendKvRead4Projection::KeysOnly => Vec::new(),
        BackendKvRead4Projection::Parts(parts) => {
            parts.iter().map(|_| BytePageBuilder::new()).collect()
        }
    }
}

fn key_matches_range(key: &[u8], range: &BackendKvScanRange) -> bool {
    match range {
        BackendKvScanRange::Prefix(prefix) => key.starts_with(prefix),
        BackendKvScanRange::Range { start, end } => start.as_slice() <= key && key < end.as_slice(),
    }
}

fn delete_range_bytes(range: &BackendKvScanRange) -> usize {
    match range {
        BackendKvScanRange::Prefix(prefix) => prefix.len(),
        BackendKvScanRange::Range { start, end } => start.len() + end.len(),
    }
}

fn lock_error(name: &str) -> LixError {
    LixError::new("LIX_ERROR_UNKNOWN", format!("{name} lock poisoned"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{
        BackendKvGetGroup, BackendKvGetRequest, BackendKvScanRequest, BackendKvWriteBatch,
        BackendKvWriteGroup,
    };

    async fn put(
        transaction: &mut (dyn BackendWriteTransaction + Send + Sync),
        namespace: &str,
        key: &[u8],
        value: &[u8],
    ) {
        transaction
            .write_kv_batch(BackendKvWriteBatch {
                groups: {
                    let mut group = BackendKvWriteGroup::new(namespace);
                    group.put(key, value);
                    vec![group]
                },
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
                groups: {
                    let mut group = BackendKvWriteGroup::new(namespace);
                    group.delete(key);
                    vec![group]
                },
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
            .and_then(|group| group.value(0).flatten().map(<[u8]>::to_vec))
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
        assert_eq!(page.len(), expected.len());
        for (index, (key, value)) in expected.iter().enumerate() {
            assert_eq!(page.key(index).expect("key exists"), *key);
            assert_eq!(page.value(index).expect("value exists"), *value);
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
        assert_eq!(page.keys.iter().collect::<Vec<_>>(), vec![b"a", b"b"]);
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
