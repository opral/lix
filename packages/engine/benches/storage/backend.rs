use async_trait::async_trait;
use lix_engine::{
    Backend, BackendTransaction, KvPair, KvScanRange, LixError, TransactionBeginMode,
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
    mode: TransactionBeginMode,
    finalized: bool,
}

impl BenchBackend {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    fn lock_store(&self) -> Result<std::sync::MutexGuard<'_, Store>, LixError> {
        self.store
            .lock()
            .map_err(|_| LixError::new("LIX_ERROR_UNKNOWN", "bench store mutex poisoned"))
    }
}

#[async_trait]
impl Backend for BenchBackend {
    async fn begin_transaction(
        &self,
        mode: TransactionBeginMode,
    ) -> Result<Box<dyn BackendTransaction + Send + Sync + 'static>, LixError> {
        Ok(Box::new(BenchTransaction {
            store: Arc::clone(&self.store),
            mode,
            finalized: false,
        }))
    }

    async fn kv_get(&self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, LixError> {
        Ok(self
            .lock_store()?
            .get(&(namespace.to_string(), key.to_vec()))
            .cloned())
    }

    async fn kv_scan(
        &self,
        namespace: &str,
        range: KvScanRange,
        limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LixError> {
        let store = self.lock_store()?;
        Ok(scan_store(&store, namespace, range, limit))
    }
}

#[async_trait]
impl BackendTransaction for BenchTransaction {
    fn mode(&self) -> TransactionBeginMode {
        self.mode
    }

    async fn kv_get(&mut self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, LixError> {
        Ok(self
            .lock_store()?
            .get(&(namespace.to_string(), key.to_vec()))
            .cloned())
    }

    async fn kv_scan(
        &mut self,
        namespace: &str,
        range: KvScanRange,
        limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LixError> {
        let store = self.lock_store()?;
        Ok(scan_store(&store, namespace, range, limit))
    }

    async fn kv_put(&mut self, namespace: &str, key: &[u8], value: &[u8]) -> Result<(), LixError> {
        self.lock_store()?
            .insert((namespace.to_string(), key.to_vec()), value.to_vec());
        Ok(())
    }

    async fn kv_delete(&mut self, namespace: &str, key: &[u8]) -> Result<(), LixError> {
        self.lock_store()?
            .remove(&(namespace.to_string(), key.to_vec()));
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<(), LixError> {
        self.finalized = true;
        Ok(())
    }

    async fn rollback(mut self: Box<Self>) -> Result<(), LixError> {
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

fn scan_store(
    store: &Store,
    namespace: &str,
    range: KvScanRange,
    limit: Option<usize>,
) -> Vec<KvPair> {
    let mut pairs = Vec::new();
    for ((row_namespace, key), value) in store.iter() {
        if row_namespace != namespace {
            continue;
        }
        let matches = match &range {
            KvScanRange::Prefix(prefix) => key.starts_with(prefix),
            KvScanRange::Range { start, end } => key >= start && key < end,
        };
        if matches {
            pairs.push(KvPair::new(key.clone(), value.clone()));
        }
        if limit.is_some_and(|limit| pairs.len() >= limit) {
            break;
        }
    }
    pairs
}
