use async_trait::async_trait;

use crate::backend;
use crate::LixError;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct KvRowBatch {
    rows: Vec<KvRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct KvRow {
    key: Vec<u8>,
    exists: bool,
    value: Option<Vec<u8>>,
}

impl KvRowBatch {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            rows: Vec::with_capacity(capacity),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.rows.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub(crate) fn key(&self, index: usize) -> Option<&[u8]> {
        self.rows.get(index).map(|row| row.key.as_slice())
    }

    pub(crate) fn exists(&self, index: usize) -> bool {
        self.rows.get(index).is_some_and(|row| row.exists)
    }

    pub(crate) fn value(&self, index: usize) -> Option<&[u8]> {
        self.rows.get(index).and_then(|row| row.value.as_deref())
    }

    pub(crate) fn value_count(&self) -> usize {
        self.rows.iter().filter(|row| row.value.is_some()).count()
    }

    pub(crate) fn existence_count(&self) -> usize {
        self.rows.iter().filter(|row| row.exists).count()
    }

    pub(crate) fn missing_count(&self) -> usize {
        self.rows.iter().filter(|row| !row.exists).count()
    }

    pub(crate) fn push_missing(&mut self, key: impl Into<Vec<u8>>) {
        self.rows.push(KvRow {
            key: key.into(),
            exists: false,
            value: None,
        });
    }

    pub(crate) fn push_exists(&mut self, key: impl Into<Vec<u8>>) {
        self.rows.push(KvRow {
            key: key.into(),
            exists: true,
            value: None,
        });
    }

    pub(crate) fn push_value(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.rows.push(KvRow {
            key: key.into(),
            exists: true,
            value: Some(value.into()),
        });
    }

    pub(crate) fn push_key_only(&mut self, key: impl Into<Vec<u8>>) {
        self.rows.push(KvRow {
            key: key.into(),
            exists: true,
            value: None,
        });
    }

    pub(crate) fn truncate(&mut self, len: usize) {
        self.rows.truncate(len);
    }

    pub(crate) fn pop_value(&mut self) -> Result<Option<Vec<u8>>, LixError> {
        let Some(row) = self.rows.pop() else {
            return Ok(None);
        };
        if row.exists && row.value.is_none() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "storage get row was requested without values",
            ));
        }
        Ok(row.value)
    }

    pub(crate) fn value_required(&self, index: usize) -> Result<&[u8], LixError> {
        self.value(index).ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "storage row was requested without values",
            )
        })
    }

    pub(crate) fn into_values_required(self) -> Result<Vec<Vec<u8>>, LixError> {
        self.rows
            .into_iter()
            .map(|row| {
                row.value.ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        "storage row was requested without values",
                    )
                })
            })
            .collect()
    }
}

impl From<backend::BackendKvRowBatch> for KvRowBatch {
    fn from(batch: backend::BackendKvRowBatch) -> Self {
        Self {
            rows: batch
                .into_parts()
                .into_iter()
                .map(|(key, exists, value)| KvRow { key, exists, value })
                .collect(),
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
    pub(crate) rows: KvRowBatch,
}

impl From<backend::BackendKvGetBatchGroup> for KvGetBatchGroup {
    fn from(group: backend::BackendKvGetBatchGroup) -> Self {
        Self {
            namespace: group.namespace,
            rows: group.rows.into(),
        }
    }
}

impl KvGetBatchGroup {
    pub(crate) fn pop_value(&mut self) -> Result<Option<Vec<u8>>, LixError> {
        self.rows.pop_value()
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
    pub(crate) rows: KvRowBatch,
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
        self.rows.key(index)
    }

    pub(crate) fn exists(&self, index: usize) -> bool {
        self.rows.exists(index)
    }

    pub(crate) fn value(&self, index: usize) -> Option<&[u8]> {
        self.rows.value(index)
    }

    pub(crate) fn value_required(&self, index: usize) -> Result<&[u8], LixError> {
        self.rows.value_required(index)
    }

    pub(crate) fn resume_after(&self) -> Option<&[u8]> {
        self.resume_after.as_deref()
    }

    pub(crate) fn into_rows(self) -> KvRowBatch {
        self.rows
    }
}

impl From<backend::BackendKvScanBatch> for KvScanBatch {
    fn from(result: backend::BackendKvScanBatch) -> Self {
        Self {
            rows: result.rows.into(),
            resume_after: result.resume_after,
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
