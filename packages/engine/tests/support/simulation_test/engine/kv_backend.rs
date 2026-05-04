use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lix_engine::{
    Backend, BackendKvGetBatch, BackendKvGetBatchGroup, BackendKvGetGroup, BackendKvGetProjection,
    BackendKvGetRequest, BackendKvPut, BackendKvRowBatch, BackendKvScanBatch,
    BackendKvScanProjection, BackendKvScanRange, BackendKvScanRequest, BackendKvWriteBatch,
    BackendKvWriteGroup, BackendKvWriteStats, BackendReadTransaction, BackendWriteTransaction,
    LixError,
};

pub(crate) type KvKey = (String, Vec<u8>);
pub(crate) type KvMap = BTreeMap<KvKey, Vec<u8>>;

/// KV-only backend used by simulation tests.
#[derive(Clone, Default)]
pub(crate) struct InMemoryKvBackend {
    data: Arc<Mutex<KvMap>>,
}

impl InMemoryKvBackend {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn from_snapshot(snapshot: KvMap) -> Self {
        Self {
            data: Arc::new(Mutex::new(snapshot)),
        }
    }

    pub(crate) fn snapshot(&self) -> KvMap {
        self.data
            .lock()
            .expect("in-memory backend lock poisoned")
            .clone()
    }
}

#[async_trait]
impl Backend for InMemoryKvBackend {
    async fn begin_read_transaction(
        &self,
    ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError> {
        Ok(Box::new(InMemoryKvTransaction {
            data: Arc::clone(&self.data),
            pending: BTreeMap::new(),
            closed: false,
        }))
    }

    async fn begin_write_transaction(
        &self,
    ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError> {
        Ok(Box::new(InMemoryKvTransaction {
            data: Arc::clone(&self.data),
            pending: BTreeMap::new(),
            closed: false,
        }))
    }
}

struct InMemoryKvTransaction {
    data: Arc<Mutex<KvMap>>,
    pending: BTreeMap<KvKey, Option<Vec<u8>>>,
    closed: bool,
}

#[async_trait]
impl BackendReadTransaction for InMemoryKvTransaction {
    async fn get_kv_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvGetBatch, LixError> {
        let data = self.data.lock().expect("in-memory backend lock poisoned");
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            let mut rows = BackendKvRowBatch::with_capacity(group.keys.len());
            for key in group.keys {
                let identity = (group.namespace.clone(), key.clone());
                rows.push_get_projection(
                    key,
                    self.pending
                        .get(&identity)
                        .cloned()
                        .unwrap_or_else(|| data.get(&identity).cloned()),
                    request.projection,
                );
            }
            groups.push(BackendKvGetBatchGroup {
                namespace: group.namespace,
                rows,
            });
        }
        Ok(BackendKvGetBatch { groups })
    }

    async fn scan_kv(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvScanBatch, LixError> {
        let mut visible = self
            .data
            .lock()
            .expect("in-memory backend lock poisoned")
            .clone();
        for (key, value) in &self.pending {
            match value {
                Some(value) => {
                    visible.insert(key.clone(), value.clone());
                }
                None => {
                    visible.remove(key);
                }
            }
        }
        let scan_limit = request
            .limit
            .checked_add(1 + usize::from(request.after.is_some()))
            .unwrap_or(request.limit);
        let rows = scan_map(
            &visible,
            &request.namespace,
            &request.range,
            Some(scan_limit),
        );
        let mut filtered = BackendKvRowBatch::new();
        for index in 0..rows.len() {
            let key = rows.key(index).expect("row key exists");
            if request.after.as_deref().is_none_or(|after| key > after) {
                match request.projection {
                    BackendKvScanProjection::KeysOnly => filtered.push_key_only(key.to_vec()),
                    BackendKvScanProjection::KeysAndValues => filtered.push_value(
                        key.to_vec(),
                        rows.value(index).expect("scan value exists").to_vec(),
                    ),
                }
            }
        }
        let has_more = filtered.len() > request.limit;
        filtered.truncate(request.limit);
        let resume_after = has_more.then(|| filtered.last_key_cloned()).flatten();
        Ok(BackendKvScanBatch {
            rows: filtered,
            resume_after,
        })
    }

    async fn rollback(mut self: Box<Self>) -> Result<(), LixError> {
        self.pending.clear();
        self.closed = true;
        Ok(())
    }
}

#[async_trait]
impl BackendWriteTransaction for InMemoryKvTransaction {
    async fn write_kv_batch(
        &mut self,
        batch: BackendKvWriteBatch,
    ) -> Result<BackendKvWriteStats, LixError> {
        let mut stats = BackendKvWriteStats::default();
        for group in batch.groups {
            for put in group.puts {
                stats.puts += 1;
                stats.bytes_written += put.key.len() + put.value.len();
                self.pending
                    .insert((group.namespace.clone(), put.key), Some(put.value));
            }
            for key in group.deletes {
                stats.deletes += 1;
                stats.bytes_written += key.len();
                self.pending.insert((group.namespace.clone(), key), None);
            }
        }
        Ok(stats)
    }

    async fn commit(mut self: Box<Self>) -> Result<(), LixError> {
        if self.closed {
            return Ok(());
        }
        let mut guard = self.data.lock().expect("in-memory backend lock poisoned");
        for (key, value) in std::mem::take(&mut self.pending) {
            match value {
                Some(value) => {
                    guard.insert(key, value);
                }
                None => {
                    guard.remove(&key);
                }
            }
        }
        self.closed = true;
        Ok(())
    }
}

fn scan_map(
    map: &KvMap,
    namespace: &str,
    range: &BackendKvScanRange,
    limit: Option<usize>,
) -> BackendKvRowBatch {
    let mut pairs = map
        .iter()
        .filter_map(|((entry_namespace, key), value)| {
            if entry_namespace != namespace || !key_in_range(key, range) {
                return None;
            }
            Some((key.clone(), value.clone()))
        })
        .collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(&right.0));
    if let Some(limit) = limit {
        pairs.truncate(limit);
    }
    let mut rows = BackendKvRowBatch::with_capacity(pairs.len());
    for (key, value) in pairs {
        rows.push_value(key, value);
    }
    rows
}

fn key_in_range(key: &[u8], range: &BackendKvScanRange) -> bool {
    match range {
        BackendKvScanRange::Prefix(prefix) => key.starts_with(prefix),
        BackendKvScanRange::Range { start, end } => key >= start.as_slice() && key < end.as_slice(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn put(
        tx: &mut Box<dyn BackendWriteTransaction + Send + Sync>,
        namespace: &str,
        key: &[u8],
        value: &[u8],
    ) {
        tx.write_kv_batch(BackendKvWriteBatch {
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
        tx: &mut Box<dyn BackendWriteTransaction + Send + Sync>,
        namespace: &str,
        key: &[u8],
    ) {
        tx.write_kv_batch(BackendKvWriteBatch {
            groups: vec![BackendKvWriteGroup {
                namespace: namespace.to_string(),
                puts: Vec::new(),
                deletes: vec![key.to_vec()],
            }],
        })
        .await
        .expect("delete should succeed");
    }

    async fn get(
        tx: &mut (dyn BackendReadTransaction + Send + Sync),
        namespace: &str,
        key: &[u8],
    ) -> Option<Vec<u8>> {
        tx.get_kv_many(BackendKvGetRequest {
            groups: vec![BackendKvGetGroup {
                namespace: namespace.to_string(),
                keys: vec![key.to_vec()],
            }],
            projection: BackendKvGetProjection::Values,
        })
        .await
        .expect("get should succeed")
        .groups
        .remove(0)
        .rows
        .pop_value()
    }

    async fn committed_get(
        backend: &InMemoryKvBackend,
        namespace: &str,
        key: &[u8],
    ) -> Option<Vec<u8>> {
        let mut tx = backend
            .begin_read_transaction()
            .await
            .expect("read transaction should open");
        let value = get(tx.as_mut(), namespace, key).await;
        tx.rollback().await.expect("rollback should succeed");
        value
    }

    async fn scan(
        tx: &mut (dyn BackendReadTransaction + Send + Sync),
        namespace: &str,
        range: BackendKvScanRange,
        limit: Option<usize>,
    ) -> BackendKvRowBatch {
        tx.scan_kv(BackendKvScanRequest {
            namespace: namespace.to_string(),
            range,
            after: None,
            limit: limit.unwrap_or(usize::MAX),
            projection: BackendKvScanProjection::KeysAndValues,
        })
        .await
        .expect("scan should succeed")
        .rows
    }

    #[tokio::test]
    async fn transaction_put_commit_makes_value_visible() {
        let backend = InMemoryKvBackend::new();
        let mut tx = backend
            .begin_write_transaction()
            .await
            .expect("transaction should open");

        put(&mut tx, "ns", b"a", b"one").await;
        assert_eq!(get(tx.as_mut(), "ns", b"a").await, Some(b"one".to_vec()));
        tx.commit().await.expect("commit should succeed");

        assert_eq!(
            committed_get(&backend, "ns", b"a").await,
            Some(b"one".to_vec())
        );
    }

    #[tokio::test]
    async fn rollback_discards_pending_values() {
        let backend = InMemoryKvBackend::new();
        let mut tx = backend
            .begin_write_transaction()
            .await
            .expect("transaction should open");

        put(&mut tx, "ns", b"a", b"one").await;
        tx.rollback().await.expect("rollback should succeed");

        assert_eq!(committed_get(&backend, "ns", b"a").await, None);
    }

    #[tokio::test]
    async fn scan_overlays_pending_write_and_delete() {
        let backend = InMemoryKvBackend::new();
        let mut seed = backend
            .begin_write_transaction()
            .await
            .expect("seed transaction should open");
        put(&mut seed, "ns", b"a", b"old").await;
        put(&mut seed, "ns", b"b", b"two").await;
        seed.commit().await.unwrap();

        let mut tx = backend
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        put(&mut tx, "ns", b"a", b"new").await;
        delete(&mut tx, "ns", b"b").await;
        put(&mut tx, "ns", b"c", b"three").await;

        let rows = scan(
            tx.as_mut(),
            "ns",
            BackendKvScanRange::Prefix(Vec::new()),
            None,
        )
        .await;
        assert_eq!(rows.key(0), Some(&b"a"[..]));
        assert_eq!(rows.value(0), Some(&b"new"[..]));
        assert_eq!(rows.key(1), Some(&b"c"[..]));
        assert_eq!(rows.value(1), Some(&b"three"[..]));
    }
}
