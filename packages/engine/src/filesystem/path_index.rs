use std::collections::BTreeMap;
use std::mem::size_of;
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use serde::Deserialize;

use crate::LixError;
use crate::changelog::{ChangeId, CommitId};
use crate::common::compose_file_path;
use crate::entity_pk::EntityPk;
use crate::live_state::{
    LiveStateFilter, LiveStateReader, LiveStateScanRequest, MaterializedLiveStateRow,
};
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageCoreProjection, StorageGetOptions, StorageKey,
    StorageProjectedValue, StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet,
};

use super::descriptor_path::{DirectoryPathRecord, derive_directory_paths};
use super::keys::{DIRECTORY_DESCRIPTOR_SCHEMA_KEY, FILE_DESCRIPTOR_SCHEMA_KEY};
use super::planner::FilesystemDescriptorKey;

const FILESYSTEM_PATH_REVISION_SPACE: StorageSpace = StorageSpace::new(
    StorageSpaceId(0x0007_0002),
    "filesystem.path_index_revision",
);
const FILESYSTEM_PATH_REVISION_KEY: &[u8] = b"global";
const MAX_CACHE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum FilesystemPathKind {
    File,
    Directory,
}

#[derive(Debug, Clone)]
pub(crate) struct FilesystemPathEntry {
    pub(crate) path: String,
    pub(crate) kind: FilesystemPathKind,
    pub(crate) parent_id: Option<String>,
    pub(crate) name: String,
    pub(crate) key: FilesystemDescriptorKey,
    metadata: Option<String>,
    created_at: String,
    updated_at: String,
    change_id: Option<ChangeId>,
    commit_id: Option<CommitId>,
}

impl FilesystemPathEntry {
    pub(crate) fn id(&self) -> &str {
        self.key.descriptor_id()
    }

    pub(crate) fn live_row(&self) -> MaterializedLiveStateRow {
        let snapshot_content = match self.kind {
            FilesystemPathKind::File => serde_json::json!({
                "id": self.id(),
                "directory_id": &self.parent_id,
                "name": &self.name,
            }),
            FilesystemPathKind::Directory => serde_json::json!({
                "id": self.id(),
                "parent_id": &self.parent_id,
                "name": &self.name,
            }),
        };
        MaterializedLiveStateRow {
            entity_pk: EntityPk::single(self.id()),
            schema_key: match self.kind {
                FilesystemPathKind::File => FILE_DESCRIPTOR_SCHEMA_KEY,
                FilesystemPathKind::Directory => DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
            }
            .to_string(),
            file_id: self.key.file_id().map(str::to_string),
            snapshot_content: Some(snapshot_content.to_string()),
            metadata: self.metadata.clone(),
            deleted: false,
            created_at: self.created_at.clone(),
            updated_at: self.updated_at.clone(),
            global: self.key.global(),
            change_id: self.change_id,
            commit_id: self.commit_id,
            untracked: self.key.is_untracked(),
            branch_id: self.key.branch_id().to_string(),
        }
    }

    pub(crate) fn metadata(&self) -> Option<&str> {
        self.metadata.as_deref()
    }

    pub(crate) fn created_at(&self) -> &str {
        &self.created_at
    }

    pub(crate) fn updated_at(&self) -> &str {
        &self.updated_at
    }

    pub(crate) fn change_id(&self) -> Option<ChangeId> {
        self.change_id
    }

    pub(crate) fn commit_id(&self) -> Option<CommitId> {
        self.commit_id
    }

    fn estimated_heap_bytes(&self) -> usize {
        self.path.capacity()
            + self.parent_id.as_ref().map_or(0, String::capacity)
            + self.name.capacity()
            + self.key.estimated_heap_bytes()
            + self.metadata.as_ref().map_or(0, String::capacity)
            + self.created_at.capacity()
            + self.updated_at.capacity()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FilesystemPathSelection {
    index: Arc<FilesystemPathIndex>,
    indices: Arc<[usize]>,
}

impl FilesystemPathSelection {
    pub(crate) fn new(index: Arc<FilesystemPathIndex>, indices: Vec<usize>) -> Self {
        Self {
            index,
            indices: indices.into(),
        }
    }

    pub(crate) fn entries(&self) -> impl Iterator<Item = &FilesystemPathEntry> {
        self.indices.iter().map(|index| &self.index.entries[*index])
    }

    pub(crate) fn entries_of_kind_with_limit(
        &self,
        kind: FilesystemPathKind,
        limit: Option<usize>,
    ) -> impl Iterator<Item = &FilesystemPathEntry> {
        self.entries()
            .filter(move |entry| entry.kind == kind)
            .take(limit.unwrap_or(usize::MAX))
    }

    pub(crate) fn len(&self) -> usize {
        self.indices.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    pub(crate) fn index(&self) -> &FilesystemPathIndex {
        &self.index
    }
}

/// Ordered, descriptor-only index over one effective live filesystem view.
///
/// Duplicate paths remain adjacent because Lix path uniqueness is storage-scope
/// local. Distinct branch/global lanes can legally render the same path and SQL
/// predicates must retain every visible candidate.
#[derive(Debug, Clone, Default)]
pub(crate) struct FilesystemPathIndex {
    entries: Vec<FilesystemPathEntry>,
    file_count: usize,
    directory_count: usize,
    estimated_heap_bytes: usize,
}

impl FilesystemPathIndex {
    pub(crate) fn from_live_rows(rows: Vec<MaterializedLiveStateRow>) -> Result<Self, LixError> {
        let mut directory_rows = BTreeMap::<FilesystemDescriptorKey, DirectoryRecord>::new();
        let mut file_rows = Vec::<(FilesystemDescriptorKey, FileRecord)>::new();

        for row in rows {
            let Some(snapshot_content) = row.snapshot_content.as_deref() else {
                continue;
            };
            match row.schema_key.as_str() {
                DIRECTORY_DESCRIPTOR_SCHEMA_KEY => {
                    let snapshot: DirectorySnapshot = serde_json::from_str(snapshot_content)
                        .map_err(|error| {
                            LixError::unknown(format!(
                                "invalid lix_directory_descriptor snapshot JSON: {error}"
                            ))
                        })?;
                    let key = FilesystemDescriptorKey::from_live_row(&row, snapshot.id.clone());
                    directory_rows.insert(
                        key.clone(),
                        DirectoryRecord {
                            key,
                            id: snapshot.id,
                            parent_id: snapshot.parent_id,
                            name: snapshot.name,
                            metadata: row.metadata,
                            created_at: row.created_at,
                            updated_at: row.updated_at,
                            change_id: row.change_id,
                            commit_id: row.commit_id,
                        },
                    );
                }
                FILE_DESCRIPTOR_SCHEMA_KEY => {
                    let snapshot: FileSnapshot =
                        serde_json::from_str(snapshot_content).map_err(|error| {
                            LixError::unknown(format!(
                                "invalid lix_file_descriptor snapshot JSON: {error}"
                            ))
                        })?;
                    let key = FilesystemDescriptorKey::from_live_row(&row, snapshot.id.clone());
                    file_rows.push((
                        key,
                        FileRecord {
                            id: snapshot.id,
                            directory_id: snapshot.directory_id,
                            name: snapshot.name,
                            metadata: row.metadata,
                            created_at: row.created_at,
                            updated_at: row.updated_at,
                            change_id: row.change_id,
                            commit_id: row.commit_id,
                        },
                    ));
                }
                _ => {}
            }
        }

        let directory_paths = derive_directory_paths(
            directory_rows
                .iter()
                .map(|(key, record)| (key.clone(), record)),
        )?;
        let mut entries = Vec::<FilesystemPathEntry>::with_capacity(
            directory_rows.len().saturating_add(file_rows.len()),
        );

        for (key, record) in &directory_rows {
            let path = directory_paths.get(key).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    format!("directory {:?} is not reachable from root", record.id),
                )
            })?;
            entries.push(FilesystemPathEntry {
                path: path.clone(),
                kind: FilesystemPathKind::Directory,
                parent_id: record.parent_id.clone(),
                name: record.name.clone(),
                key: record.key.clone(),
                metadata: record.metadata.clone(),
                created_at: record.created_at.clone(),
                updated_at: record.updated_at.clone(),
                change_id: record.change_id,
                commit_id: record.commit_id,
            });
        }

        for (key, record) in file_rows {
            let directory_path = match record.directory_id.as_deref() {
                Some(directory_id) => {
                    let parent_key = file_directory_parent_keys(&key, directory_id)
                        .into_iter()
                        .find(|candidate| directory_paths.contains_key(candidate));
                    let Some(path) = parent_key
                        .as_ref()
                        .and_then(|candidate| directory_paths.get(candidate))
                    else {
                        return Err(LixError::new(
                            LixError::CODE_FOREIGN_KEY,
                            format!(
                                "lix_file_descriptor '{}' references missing directory_id '{}' in branch '{}'",
                                record.id,
                                directory_id,
                                key.branch_id()
                            ),
                        ));
                    };
                    Some(path.as_str())
                }
                None => None,
            };
            let path = compose_file_path(directory_path, &record.name)?;
            entries.push(FilesystemPathEntry {
                path,
                kind: FilesystemPathKind::File,
                parent_id: record.directory_id,
                name: record.name,
                key,
                metadata: record.metadata,
                created_at: record.created_at,
                updated_at: record.updated_at,
                change_id: record.change_id,
                commit_id: record.commit_id,
            });
        }

        entries.sort_unstable_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.key.cmp(&right.key))
                .then_with(|| left.kind.cmp(&right.kind))
        });
        let file_count = entries
            .iter()
            .filter(|entry| entry.kind == FilesystemPathKind::File)
            .count();
        let directory_count = entries.len().saturating_sub(file_count);
        let estimated_heap_bytes = size_of::<Self>()
            + entries.capacity() * size_of::<FilesystemPathEntry>()
            + entries
                .iter()
                .map(FilesystemPathEntry::estimated_heap_bytes)
                .sum::<usize>();
        Ok(Self {
            entries,
            file_count,
            directory_count,
            estimated_heap_bytes,
        })
    }

    pub(crate) fn exact_indices(&self, path: &str) -> std::ops::Range<usize> {
        let start = self
            .entries
            .partition_point(|entry| entry.path.as_str() < path);
        let end = self
            .entries
            .partition_point(|entry| entry.path.as_str() <= path);
        start..end
    }

    pub(crate) fn range_indices(
        &self,
        lower: Bound<&str>,
        upper: Bound<&str>,
    ) -> std::ops::Range<usize> {
        let start = match lower {
            Bound::Unbounded => 0,
            Bound::Included(path) => self
                .entries
                .partition_point(|entry| entry.path.as_str() < path),
            Bound::Excluded(path) => self
                .entries
                .partition_point(|entry| entry.path.as_str() <= path),
        };
        let end = match upper {
            Bound::Unbounded => self.entries.len(),
            Bound::Included(path) => self
                .entries
                .partition_point(|entry| entry.path.as_str() <= path),
            Bound::Excluded(path) => self
                .entries
                .partition_point(|entry| entry.path.as_str() < path),
        };
        start..end
    }

    pub(crate) fn all_indices(&self) -> std::ops::Range<usize> {
        0..self.entries.len()
    }

    pub(crate) fn entry(&self, index: usize) -> &FilesystemPathEntry {
        &self.entries[index]
    }

    pub(crate) fn entries(&self) -> impl Iterator<Item = &FilesystemPathEntry> {
        self.entries.iter()
    }

    pub(crate) fn kind_count(&self, kind: FilesystemPathKind) -> usize {
        match kind {
            FilesystemPathKind::File => self.file_count,
            FilesystemPathKind::Directory => self.directory_count,
        }
    }

    pub(crate) fn estimated_heap_bytes(&self) -> usize {
        self.estimated_heap_bytes
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FilesystemPathIndexRequest {
    pub(crate) branch_ids: Vec<String>,
}

impl FilesystemPathIndexRequest {
    pub(crate) fn new(mut branch_ids: Vec<String>) -> Self {
        branch_ids.sort();
        branch_ids.dedup();
        Self { branch_ids }
    }

    pub(crate) fn live_state_request(&self) -> LiveStateScanRequest {
        LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec![
                    DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
                    FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                ],
                branch_ids: self.branch_ids.clone(),
                ..LiveStateFilter::default()
            },
            ..LiveStateScanRequest::default()
        }
    }
}

#[async_trait]
pub(crate) trait FilesystemPathIndexReader: Send + Sync {
    async fn path_index(
        &self,
        request: &FilesystemPathIndexRequest,
    ) -> Result<Arc<FilesystemPathIndex>, LixError>;
}

pub(crate) struct UncachedFilesystemPathIndexReader {
    live_state: Arc<dyn LiveStateReader>,
}

impl UncachedFilesystemPathIndexReader {
    pub(crate) fn new(live_state: Arc<dyn LiveStateReader>) -> Self {
        Self { live_state }
    }
}

#[async_trait]
impl FilesystemPathIndexReader for UncachedFilesystemPathIndexReader {
    async fn path_index(
        &self,
        request: &FilesystemPathIndexRequest,
    ) -> Result<Arc<FilesystemPathIndex>, LixError> {
        build_path_index(self.live_state.as_ref(), request).await
    }
}

pub(crate) async fn build_path_index(
    live_state: &dyn LiveStateReader,
    request: &FilesystemPathIndexRequest,
) -> Result<Arc<FilesystemPathIndex>, LixError> {
    let rows = live_state.scan_rows(&request.live_state_request()).await?;
    Ok(Arc::new(FilesystemPathIndex::from_live_rows(rows)?))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CacheKey {
    branch_ids: Vec<String>,
    revision: Option<Vec<u8>>,
}

impl CacheKey {
    fn estimated_heap_bytes(&self) -> usize {
        self.branch_ids.capacity() * size_of::<String>()
            + self.branch_ids.iter().map(String::capacity).sum::<usize>()
            + self.revision.as_ref().map_or(0, Vec::capacity)
    }
}

#[derive(Debug)]
struct CachedIndex {
    key: CacheKey,
    index: Arc<FilesystemPathIndex>,
    bytes: usize,
}

#[derive(Debug, Default)]
pub(crate) struct FilesystemPathIndexCache {
    entries: Mutex<Vec<CachedIndex>>,
}

impl FilesystemPathIndexCache {
    pub(crate) fn get(
        &self,
        request: &FilesystemPathIndexRequest,
        revision: Option<&[u8]>,
    ) -> Option<Arc<FilesystemPathIndex>> {
        let key = CacheKey {
            branch_ids: request.branch_ids.clone(),
            revision: revision.map(<[u8]>::to_vec),
        };
        let mut entries = self
            .entries
            .lock()
            .expect("filesystem path cache lock poisoned");
        let index = entries.iter().position(|candidate| candidate.key == key)?;
        let entry = entries.remove(index);
        let result = Arc::clone(&entry.index);
        entries.push(entry);
        Some(result)
    }

    pub(crate) fn insert(
        &self,
        request: &FilesystemPathIndexRequest,
        revision: Option<&[u8]>,
        index: Arc<FilesystemPathIndex>,
    ) -> Arc<FilesystemPathIndex> {
        let key = CacheKey {
            branch_ids: request.branch_ids.clone(),
            revision: revision.map(<[u8]>::to_vec),
        };
        let mut entries = self
            .entries
            .lock()
            .expect("filesystem path cache lock poisoned");
        if let Some(position) = entries.iter().position(|candidate| candidate.key == key) {
            return Arc::clone(&entries[position].index);
        }
        entries.retain(|candidate| candidate.key.branch_ids != key.branch_ids);
        let bytes =
            size_of::<CachedIndex>() + key.estimated_heap_bytes() + index.estimated_heap_bytes();
        entries.push(CachedIndex {
            key,
            index: Arc::clone(&index),
            bytes,
        });
        while entries.len() > 1
            && entries
                .iter()
                .map(|candidate| candidate.bytes)
                .sum::<usize>()
                > MAX_CACHE_BYTES
        {
            entries.remove(0);
        }
        index
    }
}

pub(crate) async fn load_path_index_revision(
    store: &(impl StorageAdapterRead + ?Sized),
) -> Result<Option<Vec<u8>>, LixError> {
    let result = PointReadPlan::new(
        FILESYSTEM_PATH_REVISION_SPACE,
        &[StorageKey(Bytes::from_static(FILESYSTEM_PATH_REVISION_KEY))],
    )
    .materialize(
        store,
        StorageGetOptions {
            projection: StorageCoreProjection::FullValue,
        },
    )
    .await?;
    Ok(result
        .value
        .into_iter()
        .next()
        .flatten()
        .and_then(|value| match value {
            StorageProjectedValue::FullValue(bytes) => Some(bytes.to_vec()),
            StorageProjectedValue::KeyOnly => None,
        }))
}

pub(crate) fn stage_path_index_revision(writes: &mut StorageWriteSet) {
    writes.put(
        FILESYSTEM_PATH_REVISION_SPACE,
        StorageKey(Bytes::from_static(FILESYSTEM_PATH_REVISION_KEY)),
        StorageValue {
            bytes: Bytes::copy_from_slice(uuid::Uuid::now_v7().as_bytes()),
        },
    );
}

fn file_directory_parent_keys(
    file_key: &FilesystemDescriptorKey,
    directory_id: &str,
) -> Vec<FilesystemDescriptorKey> {
    let mut keys = vec![file_key.in_same_scope(directory_id)];
    if file_key.is_untracked() {
        keys.push(file_key.in_tracked_scope(directory_id));
    }
    keys
}

#[derive(Debug, Deserialize)]
struct DirectorySnapshot {
    id: String,
    parent_id: Option<String>,
    name: String,
}

#[derive(Debug, Deserialize)]
struct FileSnapshot {
    id: String,
    directory_id: Option<String>,
    name: String,
}

#[derive(Debug)]
struct DirectoryRecord {
    key: FilesystemDescriptorKey,
    id: String,
    parent_id: Option<String>,
    name: String,
    metadata: Option<String>,
    created_at: String,
    updated_at: String,
    change_id: Option<ChangeId>,
    commit_id: Option<CommitId>,
}

impl DirectoryPathRecord for DirectoryRecord {
    type Key = FilesystemDescriptorKey;

    fn parent_key(&self, key: &Self::Key) -> Option<Self::Key> {
        self.parent_id
            .as_deref()
            .map(|parent_id| key.in_same_scope(parent_id))
    }

    fn parent_keys(&self, key: &Self::Key) -> Vec<Self::Key> {
        let Some(parent_id) = self.parent_id.as_deref() else {
            return Vec::new();
        };
        let mut keys = vec![key.in_same_scope(parent_id)];
        if key.is_untracked() {
            keys.push(key.in_tracked_scope(parent_id));
        }
        keys
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug)]
struct FileRecord {
    id: String,
    directory_id: Option<String>,
    name: String,
    metadata: Option<String>,
    created_at: String,
    updated_at: String,
    change_id: Option<ChangeId>,
    commit_id: Option<CommitId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::{ChangeId, CommitId};
    use crate::entity_pk::EntityPk;

    #[test]
    fn exact_range_and_order_preserve_path_buckets() {
        let index = FilesystemPathIndex::from_live_rows(vec![
            directory_row("dir-docs", None, "docs", "branch-a", false),
            file_row("file-a", Some("dir-docs"), "a.md", "branch-a", false),
            file_row("file-b", Some("dir-docs"), "b.md", "branch-a", false),
            file_row(
                "file-a-global",
                Some("dir-docs-global"),
                "a.md",
                "branch-a",
                true,
            ),
            directory_row("dir-docs-global", None, "docs", "branch-a", true),
        ])
        .expect("path index should build");

        assert_eq!(index.exact_indices("/docs/a.md").len(), 2);
        assert!(index.exact_indices("/docs/missing.md").is_empty());
        let range =
            index.range_indices(Bound::Included("/docs/a.md"), Bound::Excluded("/docs/c.md"));
        assert_eq!(
            range
                .map(|entry| index.entry(entry).path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/a.md", "/docs/a.md", "/docs/b.md"]
        );
        assert_eq!(
            index
                .all_indices()
                .map(|entry| index.entry(entry).path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/", "/docs/", "/docs/a.md", "/docs/a.md", "/docs/b.md"]
        );
        assert!(index.estimated_heap_bytes() > 0);
    }

    #[test]
    fn cache_supersedes_older_revision_for_the_same_scope() {
        let cache = FilesystemPathIndexCache::default();
        let request = FilesystemPathIndexRequest::new(vec!["branch-a".to_string()]);
        let first = Arc::new(FilesystemPathIndex::default());
        let second = Arc::new(FilesystemPathIndex::default());

        cache.insert(&request, Some(&[1]), Arc::clone(&first));
        cache.insert(&request, Some(&[2]), Arc::clone(&second));

        assert!(cache.get(&request, Some(&[1])).is_none());
        assert!(Arc::ptr_eq(
            &cache.get(&request, Some(&[2])).expect("new revision"),
            &second
        ));
    }

    fn directory_row(
        id: &str,
        parent_id: Option<&str>,
        name: &str,
        branch_id: &str,
        global: bool,
    ) -> MaterializedLiveStateRow {
        live_row(
            id,
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
            serde_json::json!({"id": id, "parent_id": parent_id, "name": name}).to_string(),
            branch_id,
            global,
        )
    }

    fn file_row(
        id: &str,
        directory_id: Option<&str>,
        name: &str,
        branch_id: &str,
        global: bool,
    ) -> MaterializedLiveStateRow {
        live_row(
            id,
            FILE_DESCRIPTOR_SCHEMA_KEY,
            serde_json::json!({"id": id, "directory_id": directory_id, "name": name}).to_string(),
            branch_id,
            global,
        )
    }

    fn live_row(
        id: &str,
        schema_key: &str,
        snapshot_content: String,
        branch_id: &str,
        global: bool,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: EntityPk::single(id),
            schema_key: schema_key.to_string(),
            file_id: None,
            snapshot_content: Some(snapshot_content),
            metadata: None,
            deleted: false,
            created_at: "2026-01-01T00:00:00.000Z".to_string(),
            updated_at: "2026-01-01T00:00:00.000Z".to_string(),
            global,
            change_id: Some(ChangeId::for_test_label(id)),
            commit_id: Some(CommitId::for_test_label(id)),
            untracked: false,
            branch_id: branch_id.to_string(),
        }
    }
}
