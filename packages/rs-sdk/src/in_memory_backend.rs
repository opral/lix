use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lix_engine::{
    Backend, BackendKvGetRequest, BackendKvGetResult, BackendKvGetResultGroup, BackendKvPair,
    BackendKvScanRange, BackendKvScanRequest, BackendKvScanResult, BackendKvWriteBatch,
    BackendKvWriteStats, BackendReadTransaction, BackendWriteTransaction, LixError,
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
    ) -> Result<BackendKvGetResult, LixError> {
        Ok(get_many_from_map(&self.kv, request))
    }

    async fn scan_kv(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvScanResult, LixError> {
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
    ) -> Result<BackendKvGetResult, LixError> {
        Ok(get_many_from_map(&self.kv, request))
    }

    async fn scan_kv(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvScanResult, LixError> {
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

fn get_many_from_map(kv: &KvMap, request: BackendKvGetRequest) -> BackendKvGetResult {
    let mut groups = Vec::with_capacity(request.groups.len());
    for group in request.groups {
        let mut values = Vec::with_capacity(group.keys.len());
        for key in group.keys {
            values.push(kv.get(&(group.namespace.clone(), key)).cloned());
        }
        groups.push(BackendKvGetResultGroup {
            namespace: group.namespace,
            values,
        });
    }
    BackendKvGetResult { groups }
}

fn scan_map(kv: &KvMap, request: BackendKvScanRequest) -> BackendKvScanResult {
    let scan_limit = request
        .limit
        .checked_add(1 + usize::from(request.after.is_some()))
        .unwrap_or(request.limit);
    let mut rows = kv
        .iter()
        .filter(|((candidate_namespace, key), _)| {
            candidate_namespace == &request.namespace && key_matches_range(key, &request.range)
        })
        .map(|((_, key), value)| BackendKvPair::new(key.clone(), value.clone()))
        .filter(|row| {
            request
                .after
                .as_deref()
                .is_none_or(|after| row.key.as_slice() > after)
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.key.cmp(&right.key));
    rows.truncate(scan_limit);
    let has_more = rows.len() > request.limit;
    rows.truncate(request.limit);
    let resume_after = has_more
        .then(|| rows.last().map(|row| row.key.clone()))
        .flatten();
    BackendKvScanResult { rows, resume_after }
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
