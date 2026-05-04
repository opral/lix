use async_trait::async_trait;

use crate::backend;
use crate::LixError;

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

impl From<KvScanRange> for backend::BackendKvScanRange {
    fn from(range: KvScanRange) -> Self {
        match range {
            KvScanRange::Prefix(prefix) => Self::Prefix(prefix),
            KvScanRange::Range { start, end } => Self::Range { start, end },
        }
    }
}

#[async_trait]
pub(crate) trait StorageReader: Send {
    async fn get_values(&mut self, request: KvGetRequest) -> Result<KvValueBatch, LixError>;

    async fn exists_many(&mut self, request: KvGetRequest) -> Result<KvExistsBatch, LixError>;

    async fn scan_keys(&mut self, request: KvScanRequest) -> Result<KvKeyPage, LixError>;

    async fn scan_values(&mut self, request: KvScanRequest) -> Result<KvValuePage, LixError>;

    async fn scan_entries(&mut self, request: KvScanRequest) -> Result<KvEntryPage, LixError>;
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
    async fn get_values(&mut self, request: KvGetRequest) -> Result<KvValueBatch, LixError> {
        (**self).get_values(request).await
    }

    async fn exists_many(&mut self, request: KvGetRequest) -> Result<KvExistsBatch, LixError> {
        (**self).exists_many(request).await
    }

    async fn scan_keys(&mut self, request: KvScanRequest) -> Result<KvKeyPage, LixError> {
        (**self).scan_keys(request).await
    }

    async fn scan_values(&mut self, request: KvScanRequest) -> Result<KvValuePage, LixError> {
        (**self).scan_values(request).await
    }

    async fn scan_entries(&mut self, request: KvScanRequest) -> Result<KvEntryPage, LixError> {
        (**self).scan_entries(request).await
    }
}

#[async_trait]
impl<T> StorageReader for Box<T>
where
    T: StorageReader + ?Sized,
{
    async fn get_values(&mut self, request: KvGetRequest) -> Result<KvValueBatch, LixError> {
        (**self).get_values(request).await
    }

    async fn exists_many(&mut self, request: KvGetRequest) -> Result<KvExistsBatch, LixError> {
        (**self).exists_many(request).await
    }

    async fn scan_keys(&mut self, request: KvScanRequest) -> Result<KvKeyPage, LixError> {
        (**self).scan_keys(request).await
    }

    async fn scan_values(&mut self, request: KvScanRequest) -> Result<KvValuePage, LixError> {
        (**self).scan_values(request).await
    }

    async fn scan_entries(&mut self, request: KvScanRequest) -> Result<KvEntryPage, LixError> {
        (**self).scan_entries(request).await
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
pub(crate) struct KvValueBatch {
    pub(crate) groups: Vec<KvValueGroup>,
}

impl From<backend::BackendKvValueBatch> for KvValueBatch {
    fn from(result: backend::BackendKvValueBatch) -> Self {
        Self {
            groups: result.groups.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvValueGroup {
    pub(crate) namespace: String,
    pub(crate) values: Vec<Option<Vec<u8>>>,
}

impl From<backend::BackendKvValueGroup> for KvValueGroup {
    fn from(group: backend::BackendKvValueGroup) -> Self {
        Self {
            namespace: group.namespace,
            values: group.values,
        }
    }
}

impl KvValueGroup {
    pub(crate) fn pop_value(&mut self) -> Option<Vec<u8>> {
        self.values.pop().flatten()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvExistsBatch {
    pub(crate) groups: Vec<KvExistsGroup>,
}

impl From<backend::BackendKvExistsBatch> for KvExistsBatch {
    fn from(result: backend::BackendKvExistsBatch) -> Self {
        Self {
            groups: result.groups.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvExistsGroup {
    pub(crate) namespace: String,
    pub(crate) exists: Vec<bool>,
}

impl From<backend::BackendKvExistsGroup> for KvExistsGroup {
    fn from(group: backend::BackendKvExistsGroup) -> Self {
        Self {
            namespace: group.namespace,
            exists: group.exists,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvKeyPage {
    pub(crate) keys: Vec<Vec<u8>>,
    pub(crate) resume_after: Option<Vec<u8>>,
}

impl From<backend::BackendKvKeyPage> for KvKeyPage {
    fn from(result: backend::BackendKvKeyPage) -> Self {
        Self {
            keys: result.keys,
            resume_after: result.resume_after,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvValuePage {
    pub(crate) values: Vec<Vec<u8>>,
    pub(crate) resume_after: Option<Vec<u8>>,
}

impl From<backend::BackendKvValuePage> for KvValuePage {
    fn from(result: backend::BackendKvValuePage) -> Self {
        Self {
            values: result.values,
            resume_after: result.resume_after,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvEntryPage {
    pub(crate) entries: Vec<KvEntry>,
    pub(crate) resume_after: Option<Vec<u8>>,
}

impl From<backend::BackendKvEntryPage> for KvEntryPage {
    fn from(result: backend::BackendKvEntryPage) -> Self {
        Self {
            entries: result.entries.into_iter().map(Into::into).collect(),
            resume_after: result.resume_after,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvEntry {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
}

impl From<backend::BackendKvEntry> for KvEntry {
    fn from(entry: backend::BackendKvEntry) -> Self {
        Self {
            key: entry.key,
            value: entry.value,
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

impl From<backend::BackendKvWriteStats> for KvWriteStats {
    fn from(stats: backend::BackendKvWriteStats) -> Self {
        Self {
            puts: stats.puts,
            deletes: stats.deletes,
            bytes_written: stats.bytes_written,
        }
    }
}
