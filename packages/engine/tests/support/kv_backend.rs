use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lix_engine::{
    Backend, BackendKvEntryPage, BackendKvExistsBatch, BackendKvExistsGroup, BackendKvGetGroup,
    BackendKvGetRequest, BackendKvKeyPage, BackendKvScanRange, BackendKvScanRequest,
    BackendKvValueBatch, BackendKvValueGroup, BackendKvValuePage, BackendKvWriteBatch,
    BackendKvWriteGroup, BackendKvWriteStats, BackendReadTransaction, BackendWriteTransaction,
    BytePageBuilder, LixError,
};

pub(crate) type KvKey = (String, Vec<u8>);
pub(crate) type KvMap = BTreeMap<KvKey, Vec<u8>>;

/// KV-only backend used by engine integration tests.
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
    async fn get_values(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvValueBatch, LixError> {
        let data = self.data.lock().expect("in-memory backend lock poisoned");
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            let namespace = group.namespace.clone();
            let mut values = BytePageBuilder::with_capacity(group.keys.len(), 0);
            let mut present = Vec::with_capacity(group.keys.len());
            for key in group.keys {
                let identity = (namespace.clone(), key.clone());
                let value = self
                    .pending
                    .get(&identity)
                    .cloned()
                    .unwrap_or_else(|| data.get(&identity).cloned());
                if let Some(value) = value {
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
        let data = self.data.lock().expect("in-memory backend lock poisoned");
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            let namespace = group.namespace.clone();
            let mut exists = Vec::with_capacity(group.keys.len());
            for key in group.keys {
                let identity = (namespace.clone(), key.clone());
                let present = self
                    .pending
                    .get(&identity)
                    .map(|value| value.is_some())
                    .unwrap_or_else(|| data.contains_key(&identity));
                exists.push(present);
            }
            groups.push(BackendKvExistsGroup { namespace, exists });
        }
        Ok(BackendKvExistsBatch { groups })
    }

    async fn scan_keys(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvKeyPage, LixError> {
        let entries = self.scan_visible_entries(request)?;
        Ok(BackendKvKeyPage {
            keys: entries.keys,
            resume_after: entries.resume_after,
        })
    }

    async fn scan_values(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvValuePage, LixError> {
        let entries = self.scan_visible_entries(request)?;
        Ok(BackendKvValuePage {
            values: entries.values,
            resume_after: entries.resume_after,
        })
    }

    async fn scan_entries(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvEntryPage, LixError> {
        self.scan_visible_entries(request)
    }

    async fn rollback(mut self: Box<Self>) -> Result<(), LixError> {
        self.pending.clear();
        self.closed = true;
        Ok(())
    }
}

impl InMemoryKvTransaction {
    fn scan_visible_entries(
        &self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvEntryPage, LixError> {
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
        Ok(scan_map(&visible, &request))
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
            let namespace = group.namespace().to_string();
            for index in 0..group.put_count() {
                let key = group.put_key(index).ok_or_else(|| {
                    LixError::new("LIX_ERROR_UNKNOWN", "backend write batch missing put key")
                })?;
                let value = group.put_value(index).ok_or_else(|| {
                    LixError::new("LIX_ERROR_UNKNOWN", "backend write batch missing put value")
                })?;
                stats.puts += 1;
                stats.bytes_written += key.len() + value.len();
                self.pending
                    .insert((namespace.clone(), key.to_vec()), Some(value.to_vec()));
            }
            for index in 0..group.delete_count() {
                let key = group.delete_key(index).ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        "backend write batch missing delete key",
                    )
                })?;
                stats.deletes += 1;
                stats.bytes_written += key.len();
                self.pending.insert((namespace.clone(), key.to_vec()), None);
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

fn scan_map(map: &KvMap, request: &BackendKvScanRequest) -> BackendKvEntryPage {
    let mut pairs = map
        .iter()
        .filter_map(|((entry_namespace, key), value)| {
            if entry_namespace != &request.namespace || !key_in_range(key, &request.range) {
                return None;
            }
            if request
                .after
                .as_deref()
                .is_some_and(|after| key.as_slice() <= after)
            {
                return None;
            }
            Some((key.clone(), value.clone()))
        })
        .collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(&right.0));
    let has_more = pairs.len() > request.limit;
    pairs.truncate(request.limit);
    let resume_after = has_more
        .then(|| pairs.last().map(|(key, _)| key.clone()))
        .flatten();
    let mut keys = BytePageBuilder::with_capacity(pairs.len(), 0);
    let mut values = BytePageBuilder::with_capacity(pairs.len(), 0);
    for (key, value) in pairs {
        keys.push(key);
        values.push(value);
    }
    BackendKvEntryPage {
        keys: keys.finish(),
        values: values.finish(),
        resume_after,
    }
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
        tx: &mut Box<dyn BackendWriteTransaction + Send + Sync>,
        namespace: &str,
        key: &[u8],
    ) {
        tx.write_kv_batch(BackendKvWriteBatch {
            groups: {
                let mut group = BackendKvWriteGroup::new(namespace);
                group.delete(key);
                vec![group]
            },
        })
        .await
        .expect("delete should succeed");
    }

    async fn get(
        tx: &mut (dyn BackendReadTransaction + Send + Sync),
        namespace: &str,
        key: &[u8],
    ) -> Option<Vec<u8>> {
        tx.get_values(BackendKvGetRequest {
            groups: vec![BackendKvGetGroup {
                namespace: namespace.to_string(),
                keys: vec![key.to_vec()],
            }],
        })
        .await
        .expect("get should succeed")
        .groups
        .remove(0)
        .value(0)
        .flatten()
        .map(<[u8]>::to_vec)
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
    ) -> BackendKvEntryPage {
        tx.scan_entries(BackendKvScanRequest {
            namespace: namespace.to_string(),
            range,
            after: None,
            limit: limit.unwrap_or(usize::MAX),
        })
        .await
        .expect("scan should succeed")
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
        assert_eq!(rows.key(0).expect("key exists"), b"a");
        assert_eq!(rows.value(0).expect("value exists"), b"new");
        assert_eq!(rows.key(1).expect("key exists"), b"c");
        assert_eq!(rows.value(1).expect("value exists"), b"three");
    }
}
