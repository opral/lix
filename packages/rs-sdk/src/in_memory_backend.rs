use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lix_engine::{
    KvPair, KvScanRange, LixBackend, LixBackendTransaction, LixError, TransactionBeginMode,
};

type KvMap = BTreeMap<(String, Vec<u8>), Vec<u8>>;

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
impl LixBackend for InMemoryBackend {
    async fn begin_transaction(
        &self,
        mode: TransactionBeginMode,
    ) -> Result<Box<dyn LixBackendTransaction + Send + Sync + 'static>, LixError> {
        let snapshot = self
            .kv
            .lock()
            .map_err(|_| lock_error("rs-sdk in-memory backend kv"))?
            .clone();
        Ok(Box::new(InMemoryTransaction {
            mode,
            parent: Arc::clone(&self.kv),
            kv: snapshot,
        }))
    }

    async fn kv_get(&self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, LixError> {
        Ok(self
            .kv
            .lock()
            .map_err(|_| lock_error("rs-sdk in-memory backend kv"))?
            .get(&(namespace.to_string(), key.to_vec()))
            .cloned())
    }

    async fn kv_scan(
        &self,
        namespace: &str,
        range: KvScanRange,
        limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LixError> {
        let guard = self
            .kv
            .lock()
            .map_err(|_| lock_error("rs-sdk in-memory backend kv"))?;
        Ok(scan_map(&guard, namespace, &range, limit))
    }
}

struct InMemoryTransaction {
    mode: TransactionBeginMode,
    parent: Arc<Mutex<KvMap>>,
    kv: KvMap,
}

#[async_trait]
impl LixBackendTransaction for InMemoryTransaction {
    fn mode(&self) -> TransactionBeginMode {
        self.mode
    }

    async fn kv_get(&mut self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, LixError> {
        Ok(self.kv.get(&(namespace.to_string(), key.to_vec())).cloned())
    }

    async fn kv_scan(
        &mut self,
        namespace: &str,
        range: KvScanRange,
        limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LixError> {
        Ok(scan_map(&self.kv, namespace, &range, limit))
    }

    async fn kv_put(&mut self, namespace: &str, key: &[u8], value: &[u8]) -> Result<(), LixError> {
        self.kv
            .insert((namespace.to_string(), key.to_vec()), value.to_vec());
        Ok(())
    }

    async fn kv_delete(&mut self, namespace: &str, key: &[u8]) -> Result<(), LixError> {
        self.kv.remove(&(namespace.to_string(), key.to_vec()));
        Ok(())
    }

    async fn commit(self: Box<Self>) -> Result<(), LixError> {
        *self
            .parent
            .lock()
            .map_err(|_| lock_error("rs-sdk in-memory backend kv"))? = self.kv;
        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        Ok(())
    }
}

fn scan_map(kv: &KvMap, namespace: &str, range: &KvScanRange, limit: Option<usize>) -> Vec<KvPair> {
    let mut pairs = kv
        .iter()
        .filter(|((candidate_namespace, key), _)| {
            candidate_namespace == namespace && key_matches_range(key, range)
        })
        .map(|((_, key), value)| KvPair::new(key.clone(), value.clone()))
        .collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.key.cmp(&right.key));
    if let Some(limit) = limit {
        pairs.truncate(limit);
    }
    pairs
}

fn key_matches_range(key: &[u8], range: &KvScanRange) -> bool {
    match range {
        KvScanRange::Prefix(prefix) => key.starts_with(prefix),
        KvScanRange::Range { start, end } => start.as_slice() <= key && key < end.as_slice(),
    }
}

fn lock_error(name: &str) -> LixError {
    LixError::new("LIX_ERROR_UNKNOWN", format!("{name} mutex was poisoned"))
}
