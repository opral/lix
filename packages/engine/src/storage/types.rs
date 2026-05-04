use async_trait::async_trait;

use crate::backend;
use crate::LixError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvPair {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
}

impl KvPair {
    pub(crate) fn new(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KvScanRange {
    Prefix(Vec<u8>),
    Range { start: Vec<u8>, end: Vec<u8> },
}

impl KvScanRange {
    pub(crate) fn prefix(prefix: impl Into<Vec<u8>>) -> Self {
        Self::Prefix(prefix.into())
    }

    pub(crate) fn range(start: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) -> Self {
        Self::Range {
            start: start.into(),
            end: end.into(),
        }
    }
}

#[async_trait]
pub(crate) trait StorageReader: Send {
    async fn get_kv_many(&mut self, request: KvGetRequest) -> Result<KvGetResult, LixError>;

    async fn scan_kv(&mut self, request: KvScanRequest) -> Result<KvScanResult, LixError>;
}

#[async_trait]
pub(crate) trait StorageWriter: StorageReader {
    async fn write_kv_batch(&mut self, batch: KvWriteBatch) -> Result<KvWriteStats, LixError>;
}

#[async_trait]
pub(crate) trait StorageReadTransaction: StorageReader + Send + Sync {
    async fn rollback(self: Box<Self>) -> Result<(), LixError>;
}

#[async_trait]
pub(crate) trait StorageWriteTransaction:
    StorageReadTransaction + StorageWriter + Send + Sync
{
    async fn commit(self: Box<Self>) -> Result<(), LixError>;
}

#[async_trait]
impl<T> StorageReader for &mut T
where
    T: StorageReader + ?Sized,
{
    async fn get_kv_many(&mut self, request: KvGetRequest) -> Result<KvGetResult, LixError> {
        (**self).get_kv_many(request).await
    }

    async fn scan_kv(&mut self, request: KvScanRequest) -> Result<KvScanResult, LixError> {
        (**self).scan_kv(request).await
    }
}

#[async_trait]
impl<T> StorageReader for Box<T>
where
    T: StorageReader + ?Sized,
{
    async fn get_kv_many(&mut self, request: KvGetRequest) -> Result<KvGetResult, LixError> {
        (**self).get_kv_many(request).await
    }

    async fn scan_kv(&mut self, request: KvScanRequest) -> Result<KvScanResult, LixError> {
        (**self).scan_kv(request).await
    }
}

#[async_trait]
impl<T> StorageWriter for &mut T
where
    T: StorageWriter + ?Sized,
{
    async fn write_kv_batch(&mut self, batch: KvWriteBatch) -> Result<KvWriteStats, LixError> {
        (**self).write_kv_batch(batch).await
    }
}

#[async_trait]
impl<T> StorageWriter for Box<T>
where
    T: StorageWriter + ?Sized,
{
    async fn write_kv_batch(&mut self, batch: KvWriteBatch) -> Result<KvWriteStats, LixError> {
        (**self).write_kv_batch(batch).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvGetRequest {
    pub(crate) groups: Vec<KvGetGroup>,
}

impl From<KvGetRequest> for backend::BackendKvGetRequest {
    fn from(request: KvGetRequest) -> Self {
        Self {
            groups: request.groups.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvGetGroup {
    pub(crate) namespace: String,
    pub(crate) keys: Vec<Vec<u8>>,
}

impl From<KvGetGroup> for backend::BackendKvGetGroup {
    fn from(group: KvGetGroup) -> Self {
        Self {
            namespace: group.namespace,
            keys: group.keys,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvGetResult {
    pub(crate) groups: Vec<KvGetResultGroup>,
}

impl From<backend::BackendKvGetResult> for KvGetResult {
    fn from(result: backend::BackendKvGetResult) -> Self {
        Self {
            groups: result.groups.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvGetResultGroup {
    pub(crate) namespace: String,
    pub(crate) values: Vec<Option<Vec<u8>>>,
}

impl From<backend::BackendKvGetResultGroup> for KvGetResultGroup {
    fn from(group: backend::BackendKvGetResultGroup) -> Self {
        Self {
            namespace: group.namespace,
            values: group.values,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvScanRequest {
    pub(crate) namespace: String,
    pub(crate) range: KvScanRange,
    pub(crate) after: Option<Vec<u8>>,
    pub(crate) limit: usize,
}

impl From<KvScanRequest> for backend::BackendKvScanRequest {
    fn from(request: KvScanRequest) -> Self {
        Self {
            namespace: request.namespace,
            range: request.range.into(),
            after: request.after,
            limit: request.limit,
        }
    }
}

impl From<KvScanRange> for backend::BackendKvScanRange {
    fn from(range: KvScanRange) -> Self {
        match range {
            KvScanRange::Prefix(prefix) => Self::Prefix(prefix),
            KvScanRange::Range { start, end } => Self::Range { start, end },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvScanResult {
    pub(crate) rows: Vec<KvPair>,
    pub(crate) resume_after: Option<Vec<u8>>,
}

impl From<backend::BackendKvScanResult> for KvScanResult {
    fn from(result: backend::BackendKvScanResult) -> Self {
        Self {
            rows: result.rows.into_iter().map(Into::into).collect(),
            resume_after: result.resume_after,
        }
    }
}

impl From<backend::BackendKvPair> for KvPair {
    fn from(pair: backend::BackendKvPair) -> Self {
        Self {
            key: pair.key,
            value: pair.value,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct KvWriteBatch {
    pub(crate) groups: Vec<KvWriteGroup>,
}

impl KvWriteBatch {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn put(
        &mut self,
        namespace: impl Into<String>,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) {
        let namespace = namespace.into();
        let group = self.group_mut(namespace);
        group.puts.push(KvPut {
            key: key.into(),
            value: value.into(),
        });
    }

    pub(crate) fn delete(&mut self, namespace: impl Into<String>, key: impl Into<Vec<u8>>) {
        let namespace = namespace.into();
        let group = self.group_mut(namespace);
        group.deletes.push(key.into());
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.groups
            .iter()
            .all(|group| group.puts.is_empty() && group.deletes.is_empty())
    }

    fn group_mut(&mut self, namespace: String) -> &mut KvWriteGroup {
        if let Some(index) = self
            .groups
            .iter()
            .position(|group| group.namespace == namespace)
        {
            return &mut self.groups[index];
        }
        self.groups.push(KvWriteGroup {
            namespace,
            puts: Vec::new(),
            deletes: Vec::new(),
        });
        self.groups.last_mut().expect("group just pushed")
    }
}

impl From<KvWriteBatch> for backend::BackendKvWriteBatch {
    fn from(batch: KvWriteBatch) -> Self {
        Self {
            groups: batch.groups.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvWriteGroup {
    pub(crate) namespace: String,
    pub(crate) puts: Vec<KvPut>,
    pub(crate) deletes: Vec<Vec<u8>>,
}

impl From<KvWriteGroup> for backend::BackendKvWriteGroup {
    fn from(group: KvWriteGroup) -> Self {
        Self {
            namespace: group.namespace,
            puts: group.puts.into_iter().map(Into::into).collect(),
            deletes: group.deletes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvPut {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
}

impl From<KvPut> for backend::BackendKvPut {
    fn from(put: KvPut) -> Self {
        Self {
            key: put.key,
            value: put.value,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct KvWriteStats {
    pub(crate) puts: usize,
    pub(crate) deletes: usize,
    pub(crate) bytes_written: usize,
}

impl KvWriteStats {
    pub(crate) fn record_put(&mut self, key: &[u8], value: &[u8]) {
        self.puts += 1;
        self.bytes_written += key.len() + value.len();
    }

    pub(crate) fn record_delete(&mut self, key: &[u8]) {
        self.deletes += 1;
        self.bytes_written += key.len();
    }
}

impl From<backend::BackendKvWriteStats> for KvWriteStats {
    fn from(stats: backend::BackendKvWriteStats) -> Self {
        Self {
            puts: stats.puts,
            deletes: stats.deletes,
            bytes_written: stats.bytes_written,
        }
    }
}
