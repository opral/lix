use async_trait::async_trait;
use lix_engine::{
    Backend, BackendKvEntry, BackendKvEntryPage, BackendKvExistsBatch, BackendKvExistsGroup,
    BackendKvGetRequest, BackendKvKeyPage, BackendKvScanRange, BackendKvScanRequest,
    BackendKvValueBatch, BackendKvValueGroup, BackendKvValuePage, BackendKvWriteBatch,
    BackendKvWriteStats, BackendReadTransaction, BackendWriteTransaction, LixError,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

type Store = BTreeMap<(String, Vec<u8>), Vec<u8>>;

#[derive(Clone, Default)]
pub(crate) struct BenchBackend {
    store: Arc<Mutex<Store>>,
}

pub(crate) struct BenchTransaction {
    store: Arc<Mutex<Store>>,
    finalized: bool,
}

impl BenchBackend {
    pub(crate) fn new() -> Arc<dyn Backend + Send + Sync> {
        Arc::new(Self::default())
    }
}

#[async_trait]
impl Backend for BenchBackend {
    async fn begin_read_transaction(
        &self,
    ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError> {
        Ok(Box::new(BenchTransaction {
            store: Arc::clone(&self.store),
            finalized: false,
        }))
    }

    async fn begin_write_transaction(
        &self,
    ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError> {
        Ok(Box::new(BenchTransaction {
            store: Arc::clone(&self.store),
            finalized: false,
        }))
    }
}

#[async_trait]
impl BackendReadTransaction for BenchTransaction {
    async fn get_values(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvValueBatch, LixError> {
        let store = self.lock_store()?;
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            let values = group
                .keys
                .into_iter()
                .map(|key| store.get(&(group.namespace.clone(), key)).cloned())
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
        let store = self.lock_store()?;
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            let exists = group
                .keys
                .into_iter()
                .map(|key| store.contains_key(&(group.namespace.clone(), key)))
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
        let store = self.lock_store()?;
        Ok(scan_store_keys(&store, request))
    }

    async fn scan_values(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvValuePage, LixError> {
        let store = self.lock_store()?;
        Ok(scan_store_values(&store, request))
    }

    async fn scan_entries(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvEntryPage, LixError> {
        let store = self.lock_store()?;
        Ok(scan_store_entries(&store, request))
    }

    async fn rollback(mut self: Box<Self>) -> Result<(), LixError> {
        self.finalized = true;
        Ok(())
    }
}

#[async_trait]
impl BackendWriteTransaction for BenchTransaction {
    async fn write_kv_batch(
        &mut self,
        batch: BackendKvWriteBatch,
    ) -> Result<BackendKvWriteStats, LixError> {
        let mut store = self.lock_store()?;
        let mut stats = BackendKvWriteStats::default();
        for group in batch.groups {
            for put in group.puts {
                stats.puts += 1;
                stats.bytes_written += put.key.len() + put.value.len();
                store.insert((group.namespace.clone(), put.key), put.value);
            }
            for key in group.deletes {
                stats.deletes += 1;
                stats.bytes_written += key.len();
                store.remove(&(group.namespace.clone(), key));
            }
        }
        Ok(stats)
    }

    async fn commit(mut self: Box<Self>) -> Result<(), LixError> {
        self.finalized = true;
        Ok(())
    }
}

impl BenchTransaction {
    fn lock_store(&self) -> Result<std::sync::MutexGuard<'_, Store>, LixError> {
        self.store
            .lock()
            .map_err(|_| LixError::new("LIX_ERROR_UNKNOWN", "bench store mutex poisoned"))
    }
}

fn scan_store_keys(store: &Store, request: BackendKvScanRequest) -> BackendKvKeyPage {
    let start_key = scan_start_key(&request);
    let lower_bound = (request.namespace.clone(), start_key);
    let mut keys = Vec::new();
    for ((row_namespace, key), _value) in store.range(lower_bound..) {
        if row_namespace != &request.namespace {
            break;
        }
        if let Some(after) = request.after.as_deref() {
            if key.as_slice() <= after {
                continue;
            }
        }
        if !key_matches_range(key, &request.range) {
            break;
        }
        keys.push(key.clone());
        if keys.len() > request.limit {
            break;
        }
    }
    let has_more = keys.len() > request.limit;
    keys.truncate(request.limit);
    let resume_after = has_more.then(|| keys.last().cloned()).flatten();
    BackendKvKeyPage { keys, resume_after }
}

fn scan_store_values(store: &Store, request: BackendKvScanRequest) -> BackendKvValuePage {
    let start_key = scan_start_key(&request);
    let lower_bound = (request.namespace.clone(), start_key);
    let mut values = Vec::new();
    let mut resume_after_candidate = None;
    for ((row_namespace, key), value) in store.range(lower_bound..) {
        if row_namespace != &request.namespace {
            break;
        }
        if let Some(after) = request.after.as_deref() {
            if key.as_slice() <= after {
                continue;
            }
        }
        if !key_matches_range(key, &request.range) {
            break;
        }
        if values.len() < request.limit {
            resume_after_candidate = Some(key.clone());
        }
        values.push(value.clone());
        if values.len() > request.limit {
            break;
        }
    }
    let has_more = values.len() > request.limit;
    values.truncate(request.limit);
    let resume_after = has_more.then_some(resume_after_candidate).flatten();
    BackendKvValuePage {
        values,
        resume_after,
    }
}

fn scan_store_entries(store: &Store, request: BackendKvScanRequest) -> BackendKvEntryPage {
    let start_key = scan_start_key(&request);
    let lower_bound = (request.namespace.clone(), start_key);
    let mut entries = Vec::new();
    for ((row_namespace, key), value) in store.range(lower_bound..) {
        if row_namespace != &request.namespace {
            break;
        }
        if let Some(after) = request.after.as_deref() {
            if key.as_slice() <= after {
                continue;
            }
        }
        if !key_matches_range(key, &request.range) {
            break;
        }
        entries.push(BackendKvEntry {
            key: key.clone(),
            value: value.clone(),
        });
        if entries.len() > request.limit {
            break;
        }
    }
    let has_more = entries.len() > request.limit;
    entries.truncate(request.limit);
    let resume_after = has_more
        .then(|| entries.last().map(|entry| entry.key.clone()))
        .flatten();
    BackendKvEntryPage {
        entries,
        resume_after,
    }
}

fn key_matches_range(key: &[u8], range: &BackendKvScanRange) -> bool {
    match range {
        BackendKvScanRange::Prefix(prefix) => key.starts_with(prefix),
        BackendKvScanRange::Range { start, end } => key >= start.as_slice() && key < end.as_slice(),
    }
}

fn scan_start_key(request: &BackendKvScanRequest) -> Vec<u8> {
    let range_start = match &request.range {
        BackendKvScanRange::Prefix(prefix) => prefix.as_slice(),
        BackendKvScanRange::Range { start, .. } => start.as_slice(),
    };
    match request.after.as_deref() {
        Some(after) if after > range_start => after.to_vec(),
        _ => range_start.to_vec(),
    }
}
