use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::backend::{
    KvPair, KvScanRange, LixBackend, LixBackendTransaction, TransactionBeginMode,
};
use crate::LixError;

type KvMap = BTreeMap<(String, Vec<u8>), Vec<u8>>;

/// In-memory backend for unit tests that need backend KV semantics without SQL.
///
/// SQL execution intentionally returns an error so new tests do not accidentally
/// couple to raw SQL while exercising storage-facing APIs.
#[derive(Debug, Clone, Default)]
pub(crate) struct UnitTestBackend {
    kv: Arc<Mutex<KvMap>>,
}

impl UnitTestBackend {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl LixBackend for UnitTestBackend {
    async fn begin_transaction(
        &self,
        mode: TransactionBeginMode,
    ) -> Result<Box<dyn LixBackendTransaction + Send + Sync + 'static>, LixError> {
        let snapshot = self
            .kv
            .lock()
            .map_err(|_| lock_error("unit test backend kv"))?
            .clone();
        Ok(Box::new(UnitTestTransaction {
            mode,
            parent: Arc::clone(&self.kv),
            kv: snapshot,
        }))
    }

    async fn kv_get(&self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, LixError> {
        Ok(self
            .kv
            .lock()
            .map_err(|_| lock_error("unit test backend kv"))?
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
            .map_err(|_| lock_error("unit test backend kv"))?;
        Ok(scan_map(&guard, namespace, &range, limit))
    }
}

struct UnitTestTransaction {
    mode: TransactionBeginMode,
    parent: Arc<Mutex<KvMap>>,
    kv: KvMap,
}

#[async_trait]
impl LixBackendTransaction for UnitTestTransaction {
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
            .map_err(|_| lock_error("unit test backend kv"))? = self.kv;
        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        Ok(())
    }
}

#[async_trait]
impl LixBackend for Arc<UnitTestBackend> {
    async fn begin_transaction(
        &self,
        mode: TransactionBeginMode,
    ) -> Result<Box<dyn LixBackendTransaction + Send + Sync + 'static>, LixError> {
        self.as_ref().begin_transaction(mode).await
    }

    async fn kv_get(&self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, LixError> {
        self.as_ref().kv_get(namespace, key).await
    }

    async fn kv_scan(
        &self,
        namespace: &str,
        range: KvScanRange,
        limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LixError> {
        self.as_ref().kv_scan(namespace, range, limit).await
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
    LixError::new("LIX_ERROR_UNKNOWN", format!("{name} lock poisoned"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn committed_put_is_visible_to_backend_reads() {
        let backend = UnitTestBackend::new();
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        transaction
            .kv_put("live_state", b"key", b"value")
            .await
            .expect("put should succeed");
        transaction.commit().await.expect("commit should succeed");

        assert_eq!(
            backend
                .kv_get("live_state", b"key")
                .await
                .expect("get should succeed"),
            Some(b"value".to_vec())
        );
    }

    #[tokio::test]
    async fn rollback_discards_puts() {
        let backend = UnitTestBackend::new();
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        transaction
            .kv_put("live_state", b"key", b"value")
            .await
            .expect("put should succeed");
        transaction
            .rollback()
            .await
            .expect("rollback should succeed");

        assert_eq!(
            backend
                .kv_get("live_state", b"key")
                .await
                .expect("get should succeed"),
            None
        );
    }

    #[tokio::test]
    async fn close_is_idempotent_and_does_not_destroy_data() {
        let backend = UnitTestBackend::new();
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        transaction
            .kv_put("live_state", b"key", b"value")
            .await
            .expect("put should succeed");
        transaction.commit().await.expect("commit should succeed");

        backend.close().await.expect("first close should succeed");
        backend.close().await.expect("second close should succeed");

        assert_eq!(
            backend
                .kv_get("live_state", b"key")
                .await
                .expect("get should succeed"),
            Some(b"value".to_vec())
        );
    }

    #[tokio::test]
    async fn delete_removes_key_on_commit() {
        let backend = UnitTestBackend::new();
        let mut seed = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("seed transaction should open");
        seed.kv_put("live_state", b"key", b"value")
            .await
            .expect("seed put should succeed");
        seed.commit().await.expect("seed commit should succeed");

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("delete transaction should open");
        transaction
            .kv_delete("live_state", b"key")
            .await
            .expect("delete should succeed");
        transaction.commit().await.expect("commit should succeed");

        assert_eq!(
            backend
                .kv_get("live_state", b"key")
                .await
                .expect("get should succeed"),
            None
        );
    }

    #[tokio::test]
    async fn prefix_scan_returns_lexicographic_order_with_limit() {
        let backend = UnitTestBackend::new();
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        transaction.kv_put("ns", b"b/2", b"2").await.unwrap();
        transaction.kv_put("ns", b"a/2", b"2").await.unwrap();
        transaction.kv_put("ns", b"a/1", b"1").await.unwrap();
        transaction.kv_put("other", b"a/0", b"0").await.unwrap();
        transaction.commit().await.unwrap();

        let pairs = backend
            .kv_scan("ns", KvScanRange::prefix(b"a/"), Some(1))
            .await
            .expect("scan should succeed");
        assert_eq!(pairs, vec![KvPair::new(b"a/1", b"1")]);
    }

    #[tokio::test]
    async fn range_scan_is_half_open() {
        let backend = UnitTestBackend::new();
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        transaction.kv_put("ns", b"a", b"a").await.unwrap();
        transaction.kv_put("ns", b"b", b"b").await.unwrap();
        transaction.kv_put("ns", b"c", b"c").await.unwrap();
        transaction.commit().await.unwrap();

        let pairs = backend
            .kv_scan("ns", KvScanRange::range(b"a", b"c"), None)
            .await
            .expect("scan should succeed");
        assert_eq!(
            pairs,
            vec![KvPair::new(b"a", b"a"), KvPair::new(b"b", b"b")]
        );
    }
}
