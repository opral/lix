use async_trait::async_trait;

use crate::backend;
use crate::backend::BytePage;
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
    namespace: String,
    values: BytePage,
    present: Vec<bool>,
}

impl From<backend::BackendKvValueGroup> for KvValueGroup {
    fn from(group: backend::BackendKvValueGroup) -> Self {
        let (namespace, values, present) = group.into_parts();
        Self {
            namespace,
            values,
            present,
        }
    }
}

impl KvValueGroup {
    pub(crate) fn namespace(&self) -> &str {
        &self.namespace
    }

    pub(crate) fn len(&self) -> usize {
        self.present.len()
    }

    pub(crate) fn value(&self, index: usize) -> Option<Option<&[u8]>> {
        let present = *self.present.get(index)?;
        if present {
            Some(Some(
                self.values
                    .get(index)
                    .expect("storage value batch invariant violated"),
            ))
        } else {
            Some(None)
        }
    }

    pub(crate) fn values_iter(&self) -> impl Iterator<Item = Option<&[u8]>> {
        (0..self.len()).filter_map(|index| self.value(index))
    }

    pub(crate) fn single_value_owned(&self) -> Option<Vec<u8>> {
        if self.len() != 1 {
            return None;
        }
        self.value(0).flatten().map(<[u8]>::to_vec)
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
    pub(crate) keys: BytePage,
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
    pub(crate) values: BytePage,
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
    pub(crate) keys: BytePage,
    pub(crate) values: BytePage,
    pub(crate) resume_after: Option<Vec<u8>>,
}

impl From<backend::BackendKvEntryPage> for KvEntryPage {
    fn from(result: backend::BackendKvEntryPage) -> Self {
        Self {
            keys: result.keys,
            values: result.values,
            resume_after: result.resume_after,
        }
    }
}

impl KvEntryPage {
    pub(crate) fn len(&self) -> usize {
        self.keys.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub(crate) fn key(&self, index: usize) -> Option<&[u8]> {
        self.keys.get(index)
    }

    pub(crate) fn value(&self, index: usize) -> Option<&[u8]> {
        self.values.get(index)
    }
}

#[derive(Debug, Default)]
pub(crate) struct StorageWriteSet {
    batch: KvWriteBatch,
}

impl StorageWriteSet {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn put(&mut self, namespace: &'static str, key: Vec<u8>, value: Vec<u8>) {
        self.batch.put(namespace, key, value);
    }

    pub(crate) fn delete(&mut self, namespace: &'static str, key: Vec<u8>) {
        self.batch.delete(namespace, key);
    }

    pub(crate) fn delete_range(&mut self, namespace: &'static str, range: KvScanRange) {
        self.batch.delete_range(namespace, range);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }

    pub(crate) async fn apply(
        self,
        writer: &mut (impl StorageWriter + ?Sized),
    ) -> Result<KvWriteStats, LixError> {
        writer.write_kv_batch(self.batch).await
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
        group.put(key.into(), value.into());
    }

    pub(crate) fn delete(&mut self, namespace: impl Into<String>, key: impl Into<Vec<u8>>) {
        let namespace = namespace.into();
        let group = self.group_mut(namespace);
        group.delete(key.into());
    }

    pub(crate) fn delete_range(&mut self, namespace: impl Into<String>, range: KvScanRange) {
        let namespace = namespace.into();
        let group = self.group_mut(namespace);
        group.delete_range(range);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.groups.iter().all(KvWriteGroup::is_empty)
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
            ops: Vec::new(),
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
    namespace: String,
    ops: Vec<KvWriteOp>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KvWriteOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    DeleteRange { range: KvScanRange },
}

impl From<KvWriteGroup> for backend::BackendKvWriteGroup {
    fn from(group: KvWriteGroup) -> Self {
        let mut backend_group = Self::new(group.namespace);
        for op in group.ops {
            backend_group.push(op.into());
        }
        backend_group
    }
}

impl From<KvWriteOp> for backend::BackendKvWriteOp {
    fn from(op: KvWriteOp) -> Self {
        match op {
            KvWriteOp::Put { key, value } => Self::Put { key, value },
            KvWriteOp::Delete { key } => Self::Delete { key },
            KvWriteOp::DeleteRange { range } => Self::DeleteRange {
                range: range.into(),
            },
        }
    }
}

impl KvWriteGroup {
    pub(crate) fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            ops: Vec::new(),
        }
    }

    pub(crate) fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.ops.push(KvWriteOp::Put {
            key: key.into(),
            value: value.into(),
        });
    }

    pub(crate) fn delete(&mut self, key: impl Into<Vec<u8>>) {
        self.ops.push(KvWriteOp::Delete { key: key.into() });
    }

    pub(crate) fn delete_range(&mut self, range: KvScanRange) {
        self.ops.push(KvWriteOp::DeleteRange { range });
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub(crate) fn ops(&self) -> &[KvWriteOp] {
        &self.ops
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct KvWriteStats {
    pub(crate) puts: usize,
    pub(crate) deletes: usize,
    pub(crate) delete_ranges: usize,
    pub(crate) bytes_written: usize,
}

impl From<backend::BackendKvWriteStats> for KvWriteStats {
    fn from(stats: backend::BackendKvWriteStats) -> Self {
        Self {
            puts: stats.puts,
            deletes: stats.deletes,
            delete_ranges: stats.delete_ranges,
            bytes_written: stats.bytes_written,
        }
    }
}
