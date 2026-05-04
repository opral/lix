use async_trait::async_trait;

use crate::backend;
use crate::LixError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvScanRow {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Option<Vec<u8>>,
}

impl KvScanRow {
    pub(crate) fn new(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: Some(value.into()),
        }
    }

    pub(crate) fn key_only(key: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: None,
        }
    }

    pub(crate) fn value(&self) -> Result<&[u8], LixError> {
        self.value.as_deref().ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "storage scan row was requested without values",
            )
        })
    }

    pub(crate) fn into_value(self) -> Result<Vec<u8>, LixError> {
        self.value.ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "storage scan row was requested without values",
            )
        })
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
    async fn get_kv_many(&mut self, request: KvGetRequest) -> Result<KvGetBatch, LixError>;

    async fn scan_kv(&mut self, request: KvScanRequest) -> Result<KvScanBatch, LixError>;
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
    async fn get_kv_many(&mut self, request: KvGetRequest) -> Result<KvGetBatch, LixError> {
        (**self).get_kv_many(request).await
    }

    async fn scan_kv(&mut self, request: KvScanRequest) -> Result<KvScanBatch, LixError> {
        (**self).scan_kv(request).await
    }
}

#[async_trait]
impl<T> StorageReader for Box<T>
where
    T: StorageReader + ?Sized,
{
    async fn get_kv_many(&mut self, request: KvGetRequest) -> Result<KvGetBatch, LixError> {
        (**self).get_kv_many(request).await
    }

    async fn scan_kv(&mut self, request: KvScanRequest) -> Result<KvScanBatch, LixError> {
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
    pub(crate) projection: KvGetProjection,
}

impl From<KvGetRequest> for backend::BackendKvGetRequest {
    fn from(request: KvGetRequest) -> Self {
        Self {
            groups: request.groups.into_iter().map(Into::into).collect(),
            projection: request.projection.into(),
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
pub(crate) struct KvGetBatch {
    pub(crate) groups: Vec<KvGetBatchGroup>,
}

impl From<backend::BackendKvGetBatch> for KvGetBatch {
    fn from(result: backend::BackendKvGetBatch) -> Self {
        Self {
            groups: result.groups.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvGetBatchGroup {
    pub(crate) namespace: String,
    pub(crate) entries: Vec<KvGetEntry>,
}

impl From<backend::BackendKvGetBatchGroup> for KvGetBatchGroup {
    fn from(group: backend::BackendKvGetBatchGroup) -> Self {
        Self {
            namespace: group.namespace,
            entries: group.entries.into_iter().map(Into::into).collect(),
        }
    }
}

impl KvGetBatchGroup {
    pub(crate) fn pop_value(&mut self) -> Result<Option<Vec<u8>>, LixError> {
        self.entries
            .pop()
            .map(KvGetEntry::into_value)
            .transpose()
            .map(Option::flatten)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KvGetProjection {
    Values,
    Existence,
}

impl From<KvGetProjection> for backend::BackendKvGetProjection {
    fn from(projection: KvGetProjection) -> Self {
        match projection {
            KvGetProjection::Values => Self::Values,
            KvGetProjection::Existence => Self::Existence,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvGetEntry {
    pub(crate) exists: bool,
    pub(crate) value: Option<Vec<u8>>,
}

impl KvGetEntry {
    pub(crate) fn missing() -> Self {
        Self {
            exists: false,
            value: None,
        }
    }

    pub(crate) fn value(value: impl Into<Vec<u8>>) -> Self {
        Self {
            exists: true,
            value: Some(value.into()),
        }
    }

    pub(crate) fn exists() -> Self {
        Self {
            exists: true,
            value: None,
        }
    }

    pub(crate) fn into_value(self) -> Result<Option<Vec<u8>>, LixError> {
        if self.exists && self.value.is_none() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "storage get entry was requested without values",
            ));
        }
        Ok(self.value)
    }
}

impl From<backend::BackendKvGetEntry> for KvGetEntry {
    fn from(entry: backend::BackendKvGetEntry) -> Self {
        Self {
            exists: entry.exists,
            value: entry.value,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KvScanProjection {
    KeysOnly,
    KeysAndValues,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvScanRequest {
    pub(crate) namespace: String,
    pub(crate) range: KvScanRange,
    pub(crate) after: Option<Vec<u8>>,
    pub(crate) limit: usize,
    pub(crate) projection: KvScanProjection,
}

impl From<KvScanRequest> for backend::BackendKvScanRequest {
    fn from(request: KvScanRequest) -> Self {
        Self {
            namespace: request.namespace,
            range: request.range.into(),
            after: request.after,
            limit: request.limit,
            projection: request.projection.into(),
        }
    }
}

impl From<KvScanProjection> for backend::BackendKvScanProjection {
    fn from(projection: KvScanProjection) -> Self {
        match projection {
            KvScanProjection::KeysOnly => Self::KeysOnly,
            KvScanProjection::KeysAndValues => Self::KeysAndValues,
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
pub(crate) struct KvScanBatch {
    pub(crate) rows: Vec<KvScanRow>,
    pub(crate) resume_after: Option<Vec<u8>>,
}

impl KvScanBatch {
    pub(crate) fn len(&self) -> usize {
        self.rows.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub(crate) fn key(&self, index: usize) -> Option<&[u8]> {
        self.rows.get(index).map(|row| row.key.as_slice())
    }

    pub(crate) fn value(&self, index: usize) -> Result<Option<&[u8]>, LixError> {
        self.rows.get(index).map(|row| row.value()).transpose()
    }

    pub(crate) fn resume_after(&self) -> Option<&[u8]> {
        self.resume_after.as_deref()
    }

    pub(crate) fn into_rows(self) -> Vec<KvScanRow> {
        self.rows
    }
}

impl From<backend::BackendKvScanBatch> for KvScanBatch {
    fn from(result: backend::BackendKvScanBatch) -> Self {
        Self {
            rows: result.rows.into_iter().map(Into::into).collect(),
            resume_after: result.resume_after,
        }
    }
}

impl From<backend::BackendKvScanRow> for KvScanRow {
    fn from(pair: backend::BackendKvScanRow) -> Self {
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
