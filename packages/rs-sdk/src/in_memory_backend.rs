use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lix_engine::{
    Backend, BackendKvGetBatch, BackendKvGetBatchGroup, BackendKvGetRequest, BackendKvRowBatch,
    BackendKvScanBatch, BackendKvScanProjection, BackendKvScanRange, BackendKvScanRequest,
    BackendKvWriteBatch, BackendKvWriteStats, BackendReadTransaction, BackendWriteTransaction,
    LixError,
};

type KvKey = (String, Vec<u8>);
type KvMap = BTreeMap<KvKey, Vec<u8>>;

#[derive(Debug, Clone, Default)]
pub(crate) struct InMemoryBackend {
    kv: Arc<Mutex<KvMap>>,
}

impl InMemoryBackend {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Backend for InMemoryBackend {
    async fn begin_read_transaction(
        &self,
    ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError> {
        let snapshot = self
            .kv
            .lock()
            .map_err(|_| lock_error("rs-sdk in-memory backend kv"))?
            .clone();
        Ok(Box::new(InMemoryReadTransaction { kv: snapshot }))
    }

    async fn begin_write_transaction(
        &self,
    ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError> {
        let snapshot = self
            .kv
            .lock()
            .map_err(|_| lock_error("rs-sdk in-memory backend kv"))?
            .clone();
        Ok(Box::new(InMemoryWriteTransaction {
            parent: Arc::clone(&self.kv),
            kv: snapshot,
        }))
    }
}

struct InMemoryReadTransaction {
    kv: KvMap,
}

#[async_trait]
impl BackendReadTransaction for InMemoryReadTransaction {
    async fn get_kv_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvGetBatch, LixError> {
        Ok(get_many_from_map(&self.kv, request))
    }

    async fn scan_kv(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvScanBatch, LixError> {
        Ok(scan_map(&self.kv, request))
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        Ok(())
    }
}

struct InMemoryWriteTransaction {
    parent: Arc<Mutex<KvMap>>,
    kv: KvMap,
}

#[async_trait]
impl BackendReadTransaction for InMemoryWriteTransaction {
    async fn get_kv_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvGetBatch, LixError> {
        Ok(get_many_from_map(&self.kv, request))
    }

    async fn scan_kv(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvScanBatch, LixError> {
        Ok(scan_map(&self.kv, request))
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        Ok(())
    }
}

#[async_trait]
impl BackendWriteTransaction for InMemoryWriteTransaction {
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
            .map_err(|_| lock_error("rs-sdk in-memory backend kv"))? = self.kv;
        Ok(())
    }
}

fn get_many_from_map(kv: &KvMap, request: BackendKvGetRequest) -> BackendKvGetBatch {
    let mut groups = Vec::with_capacity(request.groups.len());
    for group in request.groups {
        let mut rows = BackendKvRowBatch::with_capacity(group.keys.len());
        for key in group.keys {
            rows.push_get_projection(
                key.clone(),
                kv.get(&(group.namespace.clone(), key)).cloned(),
                request.projection,
            );
        }
        groups.push(BackendKvGetBatchGroup {
            namespace: group.namespace,
            rows,
        });
    }
    BackendKvGetBatch { groups }
}

fn scan_map(kv: &KvMap, request: BackendKvScanRequest) -> BackendKvScanBatch {
    let scan_limit = request
        .limit
        .checked_add(1 + usize::from(request.after.is_some()))
        .unwrap_or(request.limit);
    let mut pairs = kv
        .iter()
        .filter(|((candidate_namespace, key), _)| {
            candidate_namespace == &request.namespace && key_matches_range(key, &request.range)
        })
        .filter(|((_, key), _)| {
            request
                .after
                .as_deref()
                .is_none_or(|after| key.as_slice() > after)
        })
        .collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0 .1.cmp(&right.0 .1));
    pairs.truncate(scan_limit);

    let has_more = pairs.len() > request.limit;
    pairs.truncate(request.limit);

    let mut rows = BackendKvRowBatch::with_capacity(pairs.len());
    for ((_, key), value) in pairs {
        match request.projection {
            BackendKvScanProjection::KeysOnly => rows.push_key_only(key.clone()),
            BackendKvScanProjection::KeysAndValues => rows.push_value(key.clone(), value.clone()),
        }
    }
    let resume_after = has_more.then(|| rows.last_key_cloned()).flatten();
    BackendKvScanBatch { rows, resume_after }
}

fn key_matches_range(key: &[u8], range: &BackendKvScanRange) -> bool {
    match range {
        BackendKvScanRange::Prefix(prefix) => key.starts_with(prefix),
        BackendKvScanRange::Range { start, end } => start.as_slice() <= key && key < end.as_slice(),
    }
}

fn lock_error(name: &str) -> LixError {
    LixError::new("LIX_ERROR_UNKNOWN", format!("{name} mutex was poisoned"))
}
