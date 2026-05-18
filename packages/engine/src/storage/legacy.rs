use std::ops::Bound;

use async_trait::async_trait;
use bytes::Bytes;

use crate::LixError;
use crate::backend::{CoreProjection, Key, KeyRange, Prefix, ProjectedValue, ScanOptions, SpaceId};
use crate::storage::{
    PointReadPlan, ScanPlan, StorageGetOptions, StorageRead, StorageSpace, StorageWriteSet,
    StorageWriteSetStats,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KvScanRange {
    Prefix(Vec<u8>),
    Range { start: Vec<u8>, end: Vec<u8> },
}

impl KvScanRange {
    pub(crate) fn prefix(prefix: impl Into<Vec<u8>>) -> Self {
        Self::Prefix(prefix.into())
    }

    #[allow(dead_code)]
    pub(crate) fn range(start: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) -> Self {
        Self::Range {
            start: start.into(),
            end: end.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvGetRequest {
    pub(crate) groups: Vec<KvGetGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvGetGroup {
    pub(crate) namespace: String,
    pub(crate) keys: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvValueBatch {
    pub(crate) groups: Vec<KvValueGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvValueGroup {
    namespace: String,
    values: Vec<Option<Vec<u8>>>,
}

impl KvValueGroup {
    pub(crate) fn len(&self) -> usize {
        self.values.len()
    }

    pub(crate) fn value(&self, index: usize) -> Option<Option<&[u8]>> {
        self.values
            .get(index)
            .map(|value| value.as_ref().map(Vec::as_slice))
    }

    pub(crate) fn values_iter(&self) -> impl Iterator<Item = Option<&[u8]>> {
        self.values
            .iter()
            .map(|value| value.as_ref().map(Vec::as_slice))
    }

    pub(crate) fn single_value_owned(&self) -> Option<Vec<u8>> {
        if self.len() != 1 {
            return None;
        }
        self.values.first().cloned().flatten()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvExistsBatch {
    pub(crate) groups: Vec<KvExistsGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvExistsGroup {
    pub(crate) namespace: String,
    pub(crate) exists: Vec<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvScanRequest {
    pub(crate) namespace: String,
    pub(crate) range: KvScanRange,
    pub(crate) after: Option<Vec<u8>>,
    pub(crate) limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvKeyPage {
    pub(crate) keys: Vec<Vec<u8>>,
    pub(crate) resume_after: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvValuePage {
    pub(crate) values: Vec<Vec<u8>>,
    pub(crate) resume_after: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvEntryPage {
    pub(crate) keys: Vec<Vec<u8>>,
    pub(crate) values: Vec<Vec<u8>>,
    pub(crate) resume_after: Option<Vec<u8>>,
}

impl KvEntryPage {
    pub(crate) fn len(&self) -> usize {
        self.keys.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub(crate) fn key(&self, index: usize) -> Option<&[u8]> {
        self.keys.get(index).map(Vec::as_slice)
    }

    pub(crate) fn value(&self, index: usize) -> Option<&[u8]> {
        self.values.get(index).map(Vec::as_slice)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct KvWriteStats {
    pub(crate) puts: usize,
    pub(crate) deletes: usize,
    pub(crate) bytes_written: usize,
}

impl From<StorageWriteSetStats> for KvWriteStats {
    fn from(stats: StorageWriteSetStats) -> Self {
        Self {
            puts: stats.staged_puts as usize,
            deletes: stats.staged_deletes as usize,
            bytes_written: stats.written_bytes as usize,
        }
    }
}

#[async_trait(?Send)]
pub(crate) trait StorageReader {
    async fn get_values(&mut self, request: KvGetRequest) -> Result<KvValueBatch, LixError>;

    async fn exists_many(&mut self, request: KvGetRequest) -> Result<KvExistsBatch, LixError>;

    async fn scan_keys(&mut self, request: KvScanRequest) -> Result<KvKeyPage, LixError>;

    async fn scan_values(&mut self, request: KvScanRequest) -> Result<KvValuePage, LixError>;

    async fn scan_entries(&mut self, request: KvScanRequest) -> Result<KvEntryPage, LixError>;
}

#[async_trait(?Send)]
pub(crate) trait StorageWriter: StorageReader {
    async fn write_storage_set(
        &mut self,
        write_set: StorageWriteSet,
    ) -> Result<KvWriteStats, LixError>;
}

#[async_trait(?Send)]
impl<T> StorageReader for T
where
    T: StorageRead,
{
    async fn get_values(&mut self, request: KvGetRequest) -> Result<KvValueBatch, LixError> {
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            let space = legacy_space_from_namespace(group.namespace.as_str());
            let keys = group.keys.into_iter().map(key).collect::<Vec<_>>();
            let result = PointReadPlan::new(space, &keys)
                .materialize(self, StorageGetOptions::default())?
                .value;
            groups.push(KvValueGroup {
                namespace: group.namespace,
                values: result.into_iter().map(projected_value_bytes).collect(),
            });
        }
        Ok(KvValueBatch { groups })
    }

    async fn exists_many(&mut self, request: KvGetRequest) -> Result<KvExistsBatch, LixError> {
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            let space = legacy_space_from_namespace(group.namespace.as_str());
            let keys = group.keys.into_iter().map(key).collect::<Vec<_>>();
            let result = PointReadPlan::new(space, &keys)
                .materialize(
                    self,
                    StorageGetOptions {
                        projection: CoreProjection::KeyOnly,
                        ..StorageGetOptions::default()
                    },
                )?
                .value;
            groups.push(KvExistsGroup {
                namespace: group.namespace,
                exists: result.into_iter().map(|value| value.is_some()).collect(),
            });
        }
        Ok(KvExistsBatch { groups })
    }

    async fn scan_keys(&mut self, request: KvScanRequest) -> Result<KvKeyPage, LixError> {
        let page = scan(self, request, CoreProjection::KeyOnly)?;
        Ok(KvKeyPage {
            keys: page.keys,
            resume_after: page.resume_after,
        })
    }

    async fn scan_values(&mut self, request: KvScanRequest) -> Result<KvValuePage, LixError> {
        let page = scan(self, request, CoreProjection::FullValue)?;
        Ok(KvValuePage {
            values: page.values,
            resume_after: page.resume_after,
        })
    }

    async fn scan_entries(&mut self, request: KvScanRequest) -> Result<KvEntryPage, LixError> {
        scan(self, request, CoreProjection::FullValue)
    }
}

fn scan<R>(
    read: &R,
    request: KvScanRequest,
    projection: CoreProjection,
) -> Result<KvEntryPage, LixError>
where
    R: StorageRead + ?Sized,
{
    let space = legacy_space_from_namespace(request.namespace.as_str());
    let resume_after = request.after.as_ref().map(|after| key(after.clone()));
    let opts = ScanOptions {
        projection,
        limit_rows: request.limit,
        resume_after: resume_after.as_ref(),
    };
    let chunk = match request.range {
        KvScanRange::Prefix(prefix) => {
            ScanPlan::prefix(
                space,
                Prefix {
                    bytes: Bytes::from(prefix),
                },
            )
            .collect(read, opts)?
            .value
        }
        KvScanRange::Range { start, end } => {
            ScanPlan::range(
                space,
                KeyRange {
                    lower: Bound::Included(key(start)),
                    upper: Bound::Excluded(key(end)),
                },
            )
            .collect(read, opts)?
            .value
        }
    };

    let mut keys = Vec::with_capacity(chunk.entries.len());
    let mut values = Vec::with_capacity(chunk.entries.len());
    for entry in chunk.entries {
        keys.push(entry.key.0.to_vec());
        if let Some(bytes) = projected_value_bytes(Some(entry.value)) {
            values.push(bytes);
        }
    }
    let resume_after = chunk.has_more.then(|| keys.last().cloned()).flatten();
    Ok(KvEntryPage {
        keys,
        values,
        resume_after,
    })
}

pub(crate) fn legacy_space_from_namespace(namespace: &str) -> StorageSpace {
    match namespace {
        "json_store.json" => StorageSpace::new(SpaceId(0x0002_0001), "json_store.json"),
        "json_store.pack" => StorageSpace::new(SpaceId(0x0002_0002), "json_store.pack"),
        "changelog.segment" => StorageSpace::new(SpaceId(0x0004_0001), "changelog.segment"),
        "changelog.commit_visibility" => {
            StorageSpace::new(SpaceId(0x0004_0002), "changelog.commit_visibility")
        }
        "changelog.index.by_commit" => {
            StorageSpace::new(SpaceId(0x0004_0003), "changelog.index.by_commit")
        }
        "changelog.index.by_change" => {
            StorageSpace::new(SpaceId(0x0004_0004), "changelog.index.by_change")
        }
        "changelog.index.by_change_membership" => {
            StorageSpace::new(SpaceId(0x0004_0005), "changelog.index.by_change_membership")
        }
        _ => StorageSpace::new(SpaceId(fnv1a_32(namespace)), "legacy.dynamic"),
    }
}

pub(crate) fn key(bytes: impl Into<Vec<u8>>) -> Key {
    Key(Bytes::from(bytes.into()))
}

pub(crate) fn stored_value(bytes: impl Into<Vec<u8>>) -> crate::backend::StoredValue {
    crate::backend::StoredValue {
        bytes: Bytes::from(bytes.into()),
    }
}

fn projected_value_bytes(value: Option<ProjectedValue>) -> Option<Vec<u8>> {
    match value {
        Some(ProjectedValue::FullValue(bytes)) => Some(bytes.to_vec()),
        Some(ProjectedValue::KeyOnly) => Some(Vec::new()),
        None => None,
    }
}

fn fnv1a_32(value: &str) -> u32 {
    let mut hash = 0x811c9dc5u32;
    for byte in value.as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}
