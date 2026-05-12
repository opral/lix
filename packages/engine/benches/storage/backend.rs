use async_trait::async_trait;
use lix_engine::{
    Backend, BackendKvEntryPage, BackendKvExistsBatch, BackendKvExistsGroup, BackendKvGetRequest,
    BackendKvKeyPage, BackendKvScanRange, BackendKvScanRequest, BackendKvValueBatch,
    BackendKvValueGroup, BackendKvValuePage, BackendKvWriteBatch, BackendKvWriteOp,
    BackendKvWriteStats, BackendReadTransaction, BackendWriteTransaction, BytePageBuilder,
    LixError,
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
            let namespace = group.namespace.clone();
            let mut values = BytePageBuilder::with_capacity(group.keys.len(), 0);
            let mut present = Vec::with_capacity(group.keys.len());
            for key in group.keys {
                if let Some(value) = store.get(&(namespace.clone(), key)) {
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
        let store = self.lock_store()?;
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            let namespace = group.namespace.clone();
            let exists = group
                .keys
                .into_iter()
                .map(|key| store.contains_key(&(namespace.clone(), key)))
                .collect();
            groups.push(BackendKvExistsGroup { namespace, exists });
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
            let namespace = group.namespace().to_string();
            for op in group.ops() {
                match op {
                    BackendKvWriteOp::Put { key, value } => {
                        stats.puts += 1;
                        stats.bytes_written += key.len() + value.len();
                        store.insert((namespace.clone(), key.clone()), value.clone());
                    }
                    BackendKvWriteOp::Delete { key } => {
                        stats.deletes += 1;
                        stats.bytes_written += key.len();
                        store.remove(&(namespace.clone(), key.clone()));
                    }
                    BackendKvWriteOp::DeleteRange { range } => {
                        stats.delete_ranges += 1;
                        stats.bytes_written += delete_range_bytes(range);
                        store.retain(|(candidate_namespace, key), _| {
                            candidate_namespace != &namespace || !key_matches_range(key, range)
                        });
                    }
                }
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
    let mut keys = BytePageBuilder::new();
    let mut count = 0;
    let mut resume_after_candidate = None;
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
        if count < request.limit {
            resume_after_candidate = Some(key.clone());
            keys.push(key);
        }
        count += 1;
        if count > request.limit {
            break;
        }
    }
    let resume_after = (count > request.limit)
        .then_some(resume_after_candidate)
        .flatten();
    BackendKvKeyPage {
        keys: keys.finish(),
        resume_after,
    }
}

fn scan_store_values(store: &Store, request: BackendKvScanRequest) -> BackendKvValuePage {
    let start_key = scan_start_key(&request);
    let lower_bound = (request.namespace.clone(), start_key);
    let mut values = BytePageBuilder::new();
    let mut count = 0;
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
        if count < request.limit {
            resume_after_candidate = Some(key.clone());
            values.push(value);
        }
        count += 1;
        if count > request.limit {
            break;
        }
    }
    let resume_after = (count > request.limit)
        .then_some(resume_after_candidate)
        .flatten();
    BackendKvValuePage {
        values: values.finish(),
        resume_after,
    }
}

fn scan_store_entries(store: &Store, request: BackendKvScanRequest) -> BackendKvEntryPage {
    let start_key = scan_start_key(&request);
    let lower_bound = (request.namespace.clone(), start_key);
    let mut keys = BytePageBuilder::new();
    let mut values = BytePageBuilder::new();
    let mut count = 0;
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
        if count < request.limit {
            resume_after_candidate = Some(key.clone());
            keys.push(key);
            values.push(value);
        }
        count += 1;
        if count > request.limit {
            break;
        }
    }
    let resume_after = (count > request.limit)
        .then_some(resume_after_candidate)
        .flatten();
    BackendKvEntryPage {
        keys: keys.finish(),
        values: values.finish(),
        resume_after,
    }
}

fn key_matches_range(key: &[u8], range: &BackendKvScanRange) -> bool {
    match range {
        BackendKvScanRange::Prefix(prefix) => key.starts_with(prefix),
        BackendKvScanRange::Range { start, end } => key >= start.as_slice() && key < end.as_slice(),
    }
}

fn delete_range_bytes(range: &BackendKvScanRange) -> usize {
    match range {
        BackendKvScanRange::Prefix(prefix) => prefix.len(),
        BackendKvScanRange::Range { start, end } => start.len() + end.len(),
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
