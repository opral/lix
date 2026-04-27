#![allow(dead_code)]

use std::sync::Arc;

use async_trait::async_trait;

use crate::backend::{LixBackend, LixBackendTransaction};
use crate::LixError;

/// One key/value pair returned by a backend KV scan.
///
/// Keys and values are byte-oriented on purpose. Higher layers own encoding,
/// ordering, and schema decisions so storage can move from SQLite to a prolly
/// tree without changing higher-level callers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvPair {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl KvPair {
    pub fn new(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// Ordered byte range for backend KV scans.
///
/// Ranges are half-open: `start <= key < end`. `Prefix` is explicit because it
/// is a common access pattern and lets each backend choose the safest
/// implementation for its storage engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvScanRange {
    Prefix(Vec<u8>),
    Range { start: Vec<u8>, end: Vec<u8> },
}

impl KvScanRange {
    pub fn prefix(prefix: impl Into<Vec<u8>>) -> Self {
        Self::Prefix(prefix.into())
    }

    pub fn range(start: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) -> Self {
        Self::Range {
            start: start.into(),
            end: end.into(),
        }
    }
}

/// Read-only key/value access shared by backend and transaction handles.
///
/// Higher-level stores should depend on this trait so the same reader API works
/// both outside and inside a transaction. Outside a transaction, callers can
/// pass `&dyn LixBackend`; inside a transaction, callers can pass
/// `&mut dyn LixBackendTransaction`.
#[async_trait]
pub(crate) trait KvStore: Send {
    async fn kv_get(&mut self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, LixError>;

    async fn kv_scan(
        &mut self,
        namespace: &str,
        range: KvScanRange,
        limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LixError>;
}

/// Writable key/value access for transaction-scoped mutation.
///
/// Writes intentionally require a transaction-backed handle. Storage contexts
/// can expose `writer(tx)` without opening hidden transactions internally.
#[async_trait]
pub(crate) trait KvWriter: KvStore {
    async fn kv_put(&mut self, namespace: &str, key: &[u8], value: &[u8]) -> Result<(), LixError>;

    async fn kv_delete(&mut self, namespace: &str, key: &[u8]) -> Result<(), LixError>;
}

#[async_trait]
impl<T> KvStore for &T
where
    T: LixBackend + ?Sized,
{
    async fn kv_get(&mut self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, LixError> {
        (*self).kv_get(namespace, key).await
    }

    async fn kv_scan(
        &mut self,
        namespace: &str,
        range: KvScanRange,
        limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LixError> {
        (*self).kv_scan(namespace, range, limit).await
    }
}

#[async_trait]
impl<T> KvStore for Arc<T>
where
    T: LixBackend + ?Sized,
{
    async fn kv_get(&mut self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, LixError> {
        self.as_ref().kv_get(namespace, key).await
    }

    async fn kv_scan(
        &mut self,
        namespace: &str,
        range: KvScanRange,
        limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LixError> {
        self.as_ref().kv_scan(namespace, range, limit).await
    }
}

#[async_trait]
impl KvStore for &mut dyn KvStore {
    async fn kv_get(&mut self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, LixError> {
        (**self).kv_get(namespace, key).await
    }

    async fn kv_scan(
        &mut self,
        namespace: &str,
        range: KvScanRange,
        limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LixError> {
        (**self).kv_scan(namespace, range, limit).await
    }
}

#[async_trait]
impl<T> KvStore for &mut T
where
    T: LixBackendTransaction + ?Sized,
{
    async fn kv_get(&mut self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, LixError> {
        (**self).kv_get(namespace, key).await
    }

    async fn kv_scan(
        &mut self,
        namespace: &str,
        range: KvScanRange,
        limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LixError> {
        (**self).kv_scan(namespace, range, limit).await
    }
}

#[async_trait]
impl KvStore for &mut dyn KvWriter {
    async fn kv_get(&mut self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, LixError> {
        (**self).kv_get(namespace, key).await
    }

    async fn kv_scan(
        &mut self,
        namespace: &str,
        range: KvScanRange,
        limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LixError> {
        (**self).kv_scan(namespace, range, limit).await
    }
}

#[async_trait]
impl KvWriter for &mut dyn KvWriter {
    async fn kv_put(&mut self, namespace: &str, key: &[u8], value: &[u8]) -> Result<(), LixError> {
        (**self).kv_put(namespace, key, value).await
    }

    async fn kv_delete(&mut self, namespace: &str, key: &[u8]) -> Result<(), LixError> {
        (**self).kv_delete(namespace, key).await
    }
}

#[async_trait]
impl<T> KvWriter for &mut T
where
    T: LixBackendTransaction + ?Sized,
{
    async fn kv_put(&mut self, namespace: &str, key: &[u8], value: &[u8]) -> Result<(), LixError> {
        (**self).kv_put(namespace, key, value).await
    }

    async fn kv_delete(&mut self, namespace: &str, key: &[u8]) -> Result<(), LixError> {
        (**self).kv_delete(namespace, key).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::backend::testing::UnitTestBackend;
    use crate::backend::TransactionBeginMode;

    #[tokio::test]
    async fn backend_and_transaction_handles_share_kv_read_trait() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        write_sample(&mut transaction.as_mut())
            .await
            .expect("sample should write");
        transaction
            .commit()
            .await
            .expect("transaction should persist");

        let mut backend_store = backend.as_ref();
        assert_eq!(
            read_sample(&mut backend_store)
                .await
                .expect("backend read should succeed"),
            Some(b"value".to_vec())
        );

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        assert_eq!(
            read_sample(&mut transaction.as_mut())
                .await
                .expect("transaction read should succeed"),
            Some(b"value".to_vec())
        );
        transaction
            .rollback()
            .await
            .expect("transaction should roll back");
    }

    async fn read_sample(store: &mut impl KvStore) -> Result<Option<Vec<u8>>, LixError> {
        store.kv_get("backend.kv_trait_test", b"key").await
    }

    async fn write_sample(writer: &mut impl KvWriter) -> Result<(), LixError> {
        writer
            .kv_put("backend.kv_trait_test", b"key", b"value")
            .await
    }
}
