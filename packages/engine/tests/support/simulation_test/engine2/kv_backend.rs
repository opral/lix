use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lix_engine::{
    KvPair, KvScanRange, LixBackend, LixBackendTransaction, LixError, PreparedBatch, QueryResult,
    SqlDialect, TransactionBeginMode, Value,
};

pub(crate) type KvKey = (String, Vec<u8>);
pub(crate) type KvMap = BTreeMap<KvKey, Vec<u8>>;

/// KV-only backend used by engine2 simulation tests.
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
impl LixBackend for InMemoryKvBackend {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Sqlite
    }

    async fn execute(&self, _sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
        Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "simulation_test2 backend does not support raw SQL execution",
        ))
    }

    async fn begin_transaction(
        &self,
        mode: TransactionBeginMode,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        Ok(Box::new(InMemoryKvTransaction {
            data: Arc::clone(&self.data),
            pending: BTreeMap::new(),
            mode,
            closed: false,
        }))
    }

    async fn begin_savepoint(
        &self,
        _name: &str,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "simulation_test2 backend does not support savepoints",
        ))
    }

    async fn kv_get(&self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, LixError> {
        Ok(self
            .data
            .lock()
            .expect("in-memory backend lock poisoned")
            .get(&(namespace.to_string(), key.to_vec()))
            .cloned())
    }

    async fn kv_scan(
        &self,
        namespace: &str,
        range: KvScanRange,
        limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LixError> {
        let guard = self.data.lock().expect("in-memory backend lock poisoned");
        Ok(scan_map(&guard, namespace, &range, limit))
    }
}

struct InMemoryKvTransaction {
    data: Arc<Mutex<KvMap>>,
    pending: BTreeMap<KvKey, Option<Vec<u8>>>,
    mode: TransactionBeginMode,
    closed: bool,
}

#[async_trait]
impl LixBackendTransaction for InMemoryKvTransaction {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Sqlite
    }

    fn mode(&self) -> TransactionBeginMode {
        self.mode
    }

    async fn execute(&mut self, _sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
        Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "simulation_test2 transaction does not support raw SQL execution",
        ))
    }

    async fn kv_get(&mut self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, LixError> {
        let identity = (namespace.to_string(), key.to_vec());
        if let Some(value) = self.pending.get(&identity) {
            return Ok(value.clone());
        }
        Ok(self
            .data
            .lock()
            .expect("in-memory backend lock poisoned")
            .get(&identity)
            .cloned())
    }

    async fn kv_scan(
        &mut self,
        namespace: &str,
        range: KvScanRange,
        limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LixError> {
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
        Ok(scan_map(&visible, namespace, &range, limit))
    }

    async fn kv_put(&mut self, namespace: &str, key: &[u8], value: &[u8]) -> Result<(), LixError> {
        self.pending
            .insert((namespace.to_string(), key.to_vec()), Some(value.to_vec()));
        Ok(())
    }

    async fn kv_delete(&mut self, namespace: &str, key: &[u8]) -> Result<(), LixError> {
        self.pending
            .insert((namespace.to_string(), key.to_vec()), None);
        Ok(())
    }

    async fn execute_batch(&mut self, batch: &PreparedBatch) -> Result<QueryResult, LixError> {
        let _ = batch;
        Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "simulation_test2 transaction does not support SQL batches",
        ))
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

    async fn rollback(mut self: Box<Self>) -> Result<(), LixError> {
        self.pending.clear();
        self.closed = true;
        Ok(())
    }
}

fn scan_map(
    map: &KvMap,
    namespace: &str,
    range: &KvScanRange,
    limit: Option<usize>,
) -> Vec<KvPair> {
    let mut pairs = map
        .iter()
        .filter_map(|((entry_namespace, key), value)| {
            if entry_namespace != namespace || !key_in_range(key, range) {
                return None;
            }
            Some(KvPair::new(key.clone(), value.clone()))
        })
        .collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.key.cmp(&right.key));
    if let Some(limit) = limit {
        pairs.truncate(limit);
    }
    pairs
}

fn key_in_range(key: &[u8], range: &KvScanRange) -> bool {
    match range {
        KvScanRange::Prefix(prefix) => key.starts_with(prefix),
        KvScanRange::Range { start, end } => key >= start.as_slice() && key < end.as_slice(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn transaction_put_commit_makes_value_visible() {
        let backend = InMemoryKvBackend::new();
        let mut tx = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");

        tx.kv_put("ns", b"a", b"one")
            .await
            .expect("put should succeed");
        assert_eq!(tx.kv_get("ns", b"a").await.unwrap(), Some(b"one".to_vec()));
        tx.commit().await.expect("commit should succeed");

        assert_eq!(
            backend.kv_get("ns", b"a").await.unwrap(),
            Some(b"one".to_vec())
        );
    }

    #[tokio::test]
    async fn rollback_discards_pending_values() {
        let backend = InMemoryKvBackend::new();
        let mut tx = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");

        tx.kv_put("ns", b"a", b"one")
            .await
            .expect("put should succeed");
        tx.rollback().await.expect("rollback should succeed");

        assert_eq!(backend.kv_get("ns", b"a").await.unwrap(), None);
    }

    #[tokio::test]
    async fn scan_overlays_pending_write_and_delete() {
        let backend = InMemoryKvBackend::new();
        let mut seed = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("seed transaction should open");
        seed.kv_put("ns", b"a", b"old").await.unwrap();
        seed.kv_put("ns", b"b", b"two").await.unwrap();
        seed.commit().await.unwrap();

        let mut tx = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        tx.kv_put("ns", b"a", b"new").await.unwrap();
        tx.kv_delete("ns", b"b").await.unwrap();
        tx.kv_put("ns", b"c", b"three").await.unwrap();

        let rows = tx
            .kv_scan("ns", KvScanRange::Prefix(Vec::new()), None)
            .await
            .unwrap();
        assert_eq!(
            rows,
            vec![
                KvPair::new(b"a".to_vec(), b"new".to_vec()),
                KvPair::new(b"c".to_vec(), b"three".to_vec()),
            ]
        );
    }
}
