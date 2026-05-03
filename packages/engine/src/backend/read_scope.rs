use std::sync::Arc;

use crate::backend::{KvPair, KvScanRange, KvStore};
use crate::LixError;
use tokio::sync::Mutex;

/// Shared read visibility over one KV store handle.
///
/// This lets multiple subsystem readers share the same transaction/backend view
/// even when the underlying handle itself is not cloneable.
pub(crate) struct ReadScope<S> {
    store: Arc<Mutex<S>>,
}

impl<S> ReadScope<S>
where
    S: KvStore,
{
    pub(crate) fn new(store: S) -> Self {
        Self {
            store: Arc::new(Mutex::new(store)),
        }
    }

    pub(crate) fn store(&self) -> ScopedKvStore<S> {
        ScopedKvStore {
            store: Arc::clone(&self.store),
        }
    }
}

pub(crate) struct ScopedKvStore<S> {
    store: Arc<Mutex<S>>,
}

impl<S> Clone for ScopedKvStore<S> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
        }
    }
}

#[async_trait::async_trait]
impl<S> KvStore for ScopedKvStore<S>
where
    S: KvStore,
{
    async fn kv_get(&mut self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, LixError> {
        let mut store = self.store.lock().await;
        store.kv_get(namespace, key).await
    }

    async fn kv_scan(
        &mut self,
        namespace: &str,
        range: KvScanRange,
        limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LixError> {
        let mut store = self.store.lock().await;
        store.kv_scan(namespace, range, limit).await
    }
}
