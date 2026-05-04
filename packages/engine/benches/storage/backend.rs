use async_trait::async_trait;
use lix_engine::{
    Backend, BackendKvGetRequest, BackendKvGetResult, BackendKvGetResultGroup, BackendKvPair,
    BackendKvScanRange, BackendKvScanRequest, BackendKvScanResult, BackendKvWriteBatch,
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
    async fn get_kv_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvGetResult, LixError> {
        let store = self.lock_store()?;
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            let mut values = Vec::with_capacity(group.keys.len());
            for key in group.keys {
                values.push(store.get(&(group.namespace.clone(), key)).cloned());
            }
            groups.push(BackendKvGetResultGroup {
                namespace: group.namespace,
                values,
            });
        }
        Ok(BackendKvGetResult { groups })
    }

    async fn scan_kv(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvScanResult, LixError> {
        let store = self.lock_store()?;
        Ok(scan_store_request(&store, request))
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

fn scan_store_request(store: &Store, request: BackendKvScanRequest) -> BackendKvScanResult {
    let start_key = scan_start_key(&request);
    let lower_bound = (request.namespace.clone(), start_key);
    let mut rows = Vec::new();
    for ((row_namespace, key), value) in store.range(lower_bound..) {
        if row_namespace != &request.namespace {
            break;
        }
        if let Some(after) = request.after.as_deref() {
            if key.as_slice() <= after {
                continue;
            }
        }
        let matches = match &request.range {
            BackendKvScanRange::Prefix(prefix) => key.starts_with(prefix),
            BackendKvScanRange::Range { start, end } => {
                key.as_slice() >= start.as_slice() && key.as_slice() < end.as_slice()
            }
        };
        if !matches {
            break;
        }
        rows.push(BackendKvPair::new(key.clone(), value.clone()));
        if rows.len() > request.limit {
            break;
        }
    }
    let has_more = rows.len() > request.limit;
    rows.truncate(request.limit);
    let resume_after = has_more
        .then(|| rows.last().map(|row| row.key.clone()))
        .flatten();
    BackendKvScanResult { rows, resume_after }
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
