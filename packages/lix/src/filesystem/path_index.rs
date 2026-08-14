use std::collections::{BTreeMap, VecDeque};
use std::mem::size_of;
use std::ops::Bound;
use std::sync::{Arc, Mutex};

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use serde::Deserialize;

use crate::LixError;
use crate::changelog::{ChangeId, CommitId};
use crate::common::{LixTimestamp, compose_directory_path, compose_file_path};
use crate::row_pk::RowPk;
use crate::forktree::ForkTreeReadFacade;
use crate::state::{ForkTreeStateView, TransactionStateView};
use crate::storage_adapter::StorageAdapterRead;

use super::descriptor_path::{DirectoryPathRecord, derive_directory_paths};
use super::keys::{
    BLOB_REF_SCHEMA_KEY, DIRECTORY_DESCRIPTOR_SCHEMA_KEY, FILE_DESCRIPTOR_SCHEMA_KEY,
};
use super::persistent_map::PersistentMap;
use super::planner::{FilesystemBlobRefKey, FilesystemDescriptorKey};
use super::{FilesystemStateRow, FilesystemStateRows, merge_filesystem_state_rows};

const MAX_CACHE_BYTES: usize = 64 * 1024 * 1024;

#[cfg(test)]
static FULL_REBUILD_BUILDS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static FULL_REBUILD_DESCRIPTOR_ROWS: AtomicUsize = AtomicUsize::new(0);

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
    parent_identity: Option<FilesystemPathEntryIdentity>,
    metadata: Option<String>,
    created_at: String,
    updated_at: String,
    change_id: Option<ChangeId>,
    commit_id: Option<CommitId>,
    blob_ref: Option<FilesystemStateRow>,
}

impl FilesystemPathEntry {
    pub(crate) fn id(&self) -> &str {
        self.key.descriptor_id()
    }

    pub(crate) fn state_row(&self) -> FilesystemStateRow {
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
        FilesystemStateRow {
            row_pk: RowPk::uuid_from_canonical(self.id())
                .expect("filesystem descriptor IDs are validated canonical UUIDs"),
            schema_key: match self.kind {
                FilesystemPathKind::File => FILE_DESCRIPTOR_SCHEMA_KEY,
                FilesystemPathKind::Directory => DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
            }
            .to_string(),
            file_id: self.key.file_id().map(str::to_string),
            snapshot_content: Some(snapshot_content.to_string().into()),
            metadata: self.metadata.clone().map(Into::into),
            deleted: false,
            created_at: LixTimestamp::expect_parse(
                "filesystem path entry created_at",
                &self.created_at,
            ),
            updated_at: LixTimestamp::expect_parse(
                "filesystem path entry updated_at",
                &self.updated_at,
            ),
            global: self.key.global(),
            change_id: self.change_id,
            commit_id: self.commit_id,
            untracked: false,
            branch_id: self.key.branch_id().into(),
        }
    }

    pub(crate) fn metadata(&self) -> Option<&str> {
        self.metadata.as_deref()
    }

    pub(crate) fn created_at(&self) -> &str {
        &self.created_at
    }

    pub(crate) fn updated_at(&self) -> String {
        self.blob_ref
            .as_ref()
            .filter(|_| self.kind == FilesystemPathKind::File)
            .map_or_else(
                || self.updated_at.clone(),
                |blob_ref| blob_ref.updated_at.to_string(),
            )
    }

    pub(crate) fn change_id(&self) -> Option<ChangeId> {
        self.change_id
    }

    pub(crate) fn commit_id(&self) -> Option<CommitId> {
        self.commit_id
    }

    pub(crate) fn blob_ref_state_row(&self) -> Option<&FilesystemStateRow> {
        self.blob_ref.as_ref()
    }

    fn estimated_heap_bytes(&self) -> usize {
        self.path.capacity()
            + self.parent_id.as_ref().map_or(0, String::capacity)
            + self.name.capacity()
            + self.key.estimated_heap_bytes()
            + self.metadata.as_ref().map_or(0, String::capacity)
            + self.created_at.capacity()
            + self.updated_at.capacity()
            + self.blob_ref.as_ref().map_or(0, |row| {
                row.schema_key.capacity()
                    + row.row_pk.estimated_heap_bytes()
                    + row.file_id.as_ref().map_or(0, String::capacity)
                    + row
                        .snapshot_content
                        .as_ref()
                        .map_or(0, crate::common::SharedStr::retained_buffer_len)
                    + row
                        .metadata
                        .as_ref()
                        .map_or(0, crate::common::SharedStr::retained_buffer_len)
                    + row.branch_id.len()
            })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FilesystemPathSelection {
    index: Arc<FilesystemPathIndex>,
    entries: Arc<[Arc<FilesystemPathEntry>]>,
}

impl FilesystemPathSelection {
    pub(crate) fn new(
        index: Arc<FilesystemPathIndex>,
        entries: Vec<Arc<FilesystemPathEntry>>,
    ) -> Self {
        Self {
            index,
            entries: entries.into(),
        }
    }

    pub(crate) fn entries(&self) -> impl Iterator<Item = &FilesystemPathEntry> {
        self.entries.iter().map(AsRef::as_ref)
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
        self.entries.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn index(&self) -> &FilesystemPathIndex {
        &self.index
    }
}

/// Ordered file-materialization index over one effective live filesystem view.
///
/// Duplicate paths remain adjacent because Lix path uniqueness is storage-scope
/// local. Distinct branch/global lanes can legally render the same path and SQL
/// predicates must retain every visible candidate. File entries retain their
/// visible blob-ref row so exact reads do not rediscover it from live state.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct FilesystemPathEntryIdentity {
    kind: FilesystemPathKind,
    key: FilesystemDescriptorKey,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct FilesystemPathSortKey {
    path: String,
    identity: FilesystemPathEntryIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct FilesystemFileIdSortKey {
    id: String,
    path: FilesystemPathSortKey,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct FilesystemDescriptorIdSortKey {
    id: String,
    kind: FilesystemPathKind,
    identity: FilesystemPathEntryIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct FilesystemChildSortKey {
    parent: FilesystemPathEntryIdentity,
    child: FilesystemPathEntryIdentity,
}

#[derive(Debug, Clone)]
pub(crate) struct FilesystemPathIndex {
    entries_by_path: PersistentMap<FilesystemPathSortKey, Arc<FilesystemPathEntry>>,
    entries_by_identity: PersistentMap<FilesystemPathEntryIdentity, Arc<FilesystemPathEntry>>,
    files_by_id: PersistentMap<FilesystemFileIdSortKey, Arc<FilesystemPathEntry>>,
    entries_by_descriptor_id:
        PersistentMap<FilesystemDescriptorIdSortKey, Arc<FilesystemPathEntry>>,
    children_by_parent: PersistentMap<FilesystemChildSortKey, Arc<FilesystemPathEntry>>,
    file_count: usize,
    directory_count: usize,
    estimated_heap_bytes: usize,
    generation: Option<Vec<u8>>,
}

impl Default for FilesystemPathIndex {
    fn default() -> Self {
        Self {
            entries_by_path: PersistentMap::default(),
            entries_by_identity: PersistentMap::default(),
            files_by_id: PersistentMap::default(),
            entries_by_descriptor_id: PersistentMap::default(),
            children_by_parent: PersistentMap::default(),
            file_count: 0,
            directory_count: 0,
            estimated_heap_bytes: size_of::<Self>(),
            generation: None,
        }
    }
}

impl FilesystemPathIndex {
    pub(crate) fn from_state_rows(rows: &FilesystemStateRows) -> Result<Self, LixError> {
        let mut directory_rows = BTreeMap::<FilesystemDescriptorKey, DirectoryRecord>::new();
        let mut file_rows = Vec::<(FilesystemDescriptorKey, FileRecord)>::new();
        let mut blob_rows = BTreeMap::<FilesystemBlobRefKey, FilesystemStateRow>::new();

        for row in rows.iter() {
            if row.schema_key() != BLOB_REF_SCHEMA_KEY || row.deleted() {
                continue;
            }
            let Some(snapshot_content) = row.snapshot_content().map(|content| content.as_str())
            else {
                continue;
            };
            let snapshot: serde_json::Value =
                serde_json::from_str(snapshot_content).map_err(|error| {
                    LixError::unknown(format!(
                        "invalid lix_binary_blob_ref snapshot JSON: {error}"
                    ))
                })?;
            let id = snapshot
                .get("id")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    LixError::unknown("lix_binary_blob_ref snapshot is missing string id")
                })?;
            blob_rows.insert(
                FilesystemBlobRefKey::from_state_row_ref(row, id.to_string()),
                row.to_owned(),
            );
        }

        for row in rows.iter() {
            let Some(snapshot_content) = row.snapshot_content().map(|content| content.as_str())
            else {
                continue;
            };
            match row.schema_key() {
                DIRECTORY_DESCRIPTOR_SCHEMA_KEY => {
                    let snapshot: DirectorySnapshot = serde_json::from_str(snapshot_content)
                        .map_err(|error| {
                            LixError::unknown(format!(
                                "invalid lix_directory_descriptor snapshot JSON: {error}"
                            ))
                        })?;
                    let key = FilesystemDescriptorKey::from_state_row_ref(row, snapshot.id.clone());
                    directory_rows.insert(
                        key.clone(),
                        DirectoryRecord {
                            key,
                            id: snapshot.id,
                            parent_id: snapshot.parent_id,
                            name: snapshot.name,
                            metadata: row.metadata().map(ToString::to_string),
                            created_at: row.created_at().to_string(),
                            updated_at: row.updated_at().to_string(),
                            change_id: row.change_id(),
                            commit_id: row.commit_id(),
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
                    let key = FilesystemDescriptorKey::from_file_descriptor_state_row_ref(
                        row,
                        snapshot.id.clone(),
                    );
                    file_rows.push((
                        key,
                        FileRecord {
                            id: snapshot.id,
                            directory_id: snapshot.directory_id,
                            name: snapshot.name,
                            metadata: row.metadata().map(ToString::to_string),
                            created_at: row.created_at().to_string(),
                            updated_at: row.updated_at().to_string(),
                            change_id: row.change_id(),
                            commit_id: row.commit_id(),
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
        let file_count = file_rows.len();

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
                parent_identity: record.parent_id.as_deref().and_then(|parent_id| {
                    file_directory_parent_keys(key, parent_id)
                        .into_iter()
                        .find(|candidate| directory_paths.contains_key(candidate))
                        .map(|key| FilesystemPathEntryIdentity {
                            kind: FilesystemPathKind::Directory,
                            key,
                        })
                }),
                metadata: record.metadata.clone(),
                created_at: record.created_at.clone(),
                updated_at: record.updated_at.clone(),
                change_id: record.change_id,
                commit_id: record.commit_id,
                blob_ref: None,
            });
        }

        for (key, record) in file_rows {
            let blob_ref = blob_rows.remove(&key.blob_ref_key());
            let mut parent_identity = None;
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
                    parent_identity = parent_key.clone().map(|key| FilesystemPathEntryIdentity {
                        kind: FilesystemPathKind::Directory,
                        key,
                    });
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
                parent_identity,
                metadata: record.metadata,
                created_at: record.created_at,
                updated_at: record.updated_at,
                change_id: record.change_id,
                commit_id: record.commit_id,
                blob_ref,
            });
        }

        let entries = entries.into_iter().map(Arc::new).collect::<Vec<_>>();
        let mut entries_by_path = entries
            .iter()
            .map(|entry| (path_sort_key(entry), Arc::clone(entry)))
            .collect::<Vec<_>>();
        entries_by_path.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        let mut entries_by_identity = entries
            .iter()
            .map(|entry| (entry_identity(entry), Arc::clone(entry)))
            .collect::<Vec<_>>();
        entries_by_identity.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        let mut files_by_id = entries
            .iter()
            .filter(|entry| entry.kind == FilesystemPathKind::File)
            .map(|entry| (file_id_sort_key(entry), Arc::clone(entry)))
            .collect::<Vec<_>>();
        files_by_id.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        let mut entries_by_descriptor_id = entries
            .iter()
            .map(|entry| (descriptor_id_sort_key(entry), Arc::clone(entry)))
            .collect::<Vec<_>>();
        entries_by_descriptor_id.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        let mut children_by_parent = entries
            .iter()
            .filter_map(|entry| {
                entry.parent_identity.clone().map(|parent| {
                    (
                        FilesystemChildSortKey {
                            parent,
                            child: entry_identity(entry),
                        },
                        Arc::clone(entry),
                    )
                })
            })
            .collect::<Vec<_>>();
        children_by_parent.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        let directory_count = entries.len().saturating_sub(file_count);
        let estimated_heap_bytes = estimated_index_heap_bytes(&entries);
        Ok(Self {
            entries_by_path: PersistentMap::from_sorted(entries_by_path),
            entries_by_identity: PersistentMap::from_sorted(entries_by_identity),
            files_by_id: PersistentMap::from_sorted(files_by_id),
            entries_by_descriptor_id: PersistentMap::from_sorted(entries_by_descriptor_id),
            children_by_parent: PersistentMap::from_sorted(children_by_parent),
            file_count,
            directory_count,
            estimated_heap_bytes,
            generation: None,
        })
    }

    pub(crate) fn exact_entries(&self, path: &str) -> Vec<Arc<FilesystemPathEntry>> {
        self.entries_by_path
            .values_equal_by(path, |key| key.path.as_str())
    }

    pub(crate) fn exact_file_id_entries(&self, id: &str) -> Vec<Arc<FilesystemPathEntry>> {
        self.files_by_id.values_equal_by(id, |key| key.id.as_str())
    }

    pub(crate) fn range_entries(
        &self,
        lower: Bound<&str>,
        upper: Bound<&str>,
    ) -> Vec<Arc<FilesystemPathEntry>> {
        self.entries_by_path
            .values_range_by(lower, upper, |key| key.path.as_str())
    }

    pub(crate) fn entries(&self) -> Vec<Arc<FilesystemPathEntry>> {
        self.entries_by_path.values()
    }

    pub(crate) fn with_generation(mut self, generation: Option<&[u8]>) -> Self {
        self.generation = generation.map(<[u8]>::to_vec);
        self
    }

    pub(crate) fn generation(&self) -> Option<&[u8]> {
        self.generation.as_deref()
    }

    /// Applies committed descriptor rows by path-copying only the affected AVL
    /// search paths. Directory moves additionally rewrite their descendants.
    pub(crate) fn apply_committed_rows(
        &self,
        request: &FilesystemPathIndexRequest,
        rows: &[FilesystemStateRow],
        generation: Option<&[u8]>,
    ) -> Result<Self, LixError> {
        let mut next = self.clone();
        next.generation = generation.map(<[u8]>::to_vec);
        for row in rows.iter().filter(|row| {
            matches!(
                row.schema_key.as_str(),
                FILE_DESCRIPTOR_SCHEMA_KEY | DIRECTORY_DESCRIPTOR_SCHEMA_KEY
            )
        }) {
            for_each_committed_row_projection(request, row, |projected| {
                next.apply_committed_row(projected)
            })?;
        }
        for row in rows
            .iter()
            .filter(|row| row.schema_key == BLOB_REF_SCHEMA_KEY)
        {
            for_each_committed_row_projection(request, row, |projected| {
                next.apply_committed_blob_ref_row(projected)
            })?;
        }
        Ok(next)
    }

    fn apply_committed_blob_ref_row(&mut self, row: &FilesystemStateRow) -> Result<(), LixError> {
        let descriptor_id = row
            .snapshot_content
            .as_deref()
            .and_then(|snapshot| serde_json::from_str::<serde_json::Value>(snapshot).ok())
            .and_then(|snapshot| {
                snapshot
                    .get("id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            })
            .unwrap_or(row.row_pk.as_single_string_owned()?);
        let blob_ref_key = FilesystemBlobRefKey::from_state_row(row, descriptor_id.clone());
        let entry = self
            .entries_by_descriptor_id
            .values_equal_by(descriptor_id.as_str(), |key| key.id.as_str())
            .into_iter()
            .find(|entry| {
                entry.kind == FilesystemPathKind::File && entry.key.blob_ref_key() == blob_ref_key
            });
        let Some(entry) = entry else {
            return Ok(());
        };
        let mut next = (*entry).clone();
        if row.deleted || row.snapshot_content.is_none() {
            next.blob_ref = None;
        } else {
            next.blob_ref = Some(row.clone());
        }
        self.insert_entry(Arc::new(next));
        Ok(())
    }

    fn apply_committed_row(&mut self, row: &FilesystemStateRow) -> Result<(), LixError> {
        let kind = match row.schema_key.as_str() {
            FILE_DESCRIPTOR_SCHEMA_KEY => FilesystemPathKind::File,
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY => FilesystemPathKind::Directory,
            _ => return Ok(()),
        };
        let descriptor_id = row
            .snapshot_content
            .as_deref()
            .and_then(|snapshot| serde_json::from_str::<serde_json::Value>(snapshot).ok())
            .and_then(|snapshot| {
                snapshot
                    .get("id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            })
            .unwrap_or(row.row_pk.as_single_string_owned()?);
        let same_id = self
            .entries_by_descriptor_id
            .values_equal_by(descriptor_id.as_str(), |key| key.id.as_str())
            .into_iter()
            .filter(|entry| entry.kind == kind)
            .collect::<Vec<_>>();
        if row.global
            && same_id.iter().any(|entry| {
                !entry.key.global()
                    && entry.key.branch_id() == row.branch_id.as_str()
                    && entry.key.file_id()
                        == if kind == FilesystemPathKind::File {
                            None
                        } else {
                            row.file_id.as_deref()
                        }
            })
        {
            return Ok(());
        }

        let key = if kind == FilesystemPathKind::File {
            FilesystemDescriptorKey::from_file_descriptor_state_row(row, descriptor_id)
        } else {
            FilesystemDescriptorKey::from_state_row(row, descriptor_id)
        };
        let identity = FilesystemPathEntryIdentity {
            kind,
            key: key.clone(),
        };
        let prior = same_id
            .iter()
            .find(|entry| entry_identity(entry) == identity)
            .cloned();
        let prior_blob_ref = prior
            .as_ref()
            .and_then(|entry| entry.blob_ref.as_ref())
            .cloned();
        let mut descendants = if kind == FilesystemPathKind::Directory {
            prior
                .as_ref()
                .map(|entry| self.descendants(entry))
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        for entry in same_id {
            if entry.key == key || (!row.global && entry.key.global()) {
                self.remove_entry(&entry);
            }
        }
        if row.deleted || row.snapshot_content.is_none() {
            for descendant in descendants {
                self.remove_entry(&descendant);
            }
            return Ok(());
        }

        let mut entry = self.entry_from_row(row, key, kind)?;
        if let Some(prior) = prior.as_ref() {
            // Descriptor writes in the transaction overlay carry the current
            // change timestamp. Replacing the same identity must retain the
            // row's first visible timestamp.
            entry.created_at.clone_from(&prior.created_at);
        }
        entry.blob_ref = prior_blob_ref;
        self.insert_entry(Arc::new(entry));
        for descendant in &mut descendants {
            let path = self.path_for_entry(descendant)?;
            let mut rewritten = (**descendant).clone();
            rewritten.path = path;
            self.insert_entry(Arc::new(rewritten));
        }
        Ok(())
    }

    fn entry_from_row(
        &self,
        row: &FilesystemStateRow,
        key: FilesystemDescriptorKey,
        kind: FilesystemPathKind,
    ) -> Result<FilesystemPathEntry, LixError> {
        let snapshot = row.snapshot_content.as_deref().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "descriptor delta has no snapshot",
            )
        })?;
        let (parent_id, name) = match kind {
            FilesystemPathKind::File => {
                let snapshot: FileSnapshot = serde_json::from_str(snapshot).map_err(|error| {
                    LixError::unknown(format!(
                        "invalid lix_file_descriptor snapshot JSON: {error}"
                    ))
                })?;
                (snapshot.directory_id, snapshot.name)
            }
            FilesystemPathKind::Directory => {
                let snapshot: DirectorySnapshot =
                    serde_json::from_str(snapshot).map_err(|error| {
                        LixError::unknown(format!(
                            "invalid lix_directory_descriptor snapshot JSON: {error}"
                        ))
                    })?;
                (snapshot.parent_id, snapshot.name)
            }
        };
        let mut entry = FilesystemPathEntry {
            path: String::new(),
            kind,
            parent_id,
            name,
            key,
            parent_identity: None,
            metadata: row.metadata.as_ref().map(ToString::to_string),
            created_at: row.created_at.to_string(),
            updated_at: row.updated_at.to_string(),
            change_id: row.change_id,
            commit_id: row.commit_id,
            blob_ref: None,
        };
        entry.parent_identity = self.resolve_parent_identity(&entry);
        entry.path = self.path_for_entry(&entry)?;
        Ok(entry)
    }

    fn path_for_entry(&self, entry: &FilesystemPathEntry) -> Result<String, LixError> {
        let parent_path = match entry.parent_id.as_deref() {
            None => None,
            Some(parent_id) => {
                let parent = entry
                    .parent_identity
                    .as_ref()
                    .and_then(|identity| self.entries_by_identity.get(identity))
                    .cloned()
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_FOREIGN_KEY,
                            format!(
                                "filesystem descriptor '{}' references missing parent '{}'",
                                entry.id(),
                                parent_id
                            ),
                        )
                    })?;
                Some(parent.path.clone())
            }
        };
        match entry.kind {
            FilesystemPathKind::File => compose_file_path(parent_path.as_deref(), &entry.name),
            FilesystemPathKind::Directory => {
                compose_directory_path(parent_path.as_deref(), &entry.name)
            }
        }
    }

    fn resolve_parent_identity(
        &self,
        entry: &FilesystemPathEntry,
    ) -> Option<FilesystemPathEntryIdentity> {
        let parent_id = entry.parent_id.as_deref()?;
        file_directory_parent_keys(&entry.key, parent_id)
            .into_iter()
            .map(|key| FilesystemPathEntryIdentity {
                kind: FilesystemPathKind::Directory,
                key,
            })
            .find(|identity| self.entries_by_identity.get(identity).is_some())
    }

    fn descendants(&self, root: &FilesystemPathEntry) -> Vec<Arc<FilesystemPathEntry>> {
        let mut descendants = Vec::new();
        let mut queue = VecDeque::from([entry_identity(root)]);
        while let Some(parent) = queue.pop_front() {
            for child in self
                .children_by_parent
                .values_equal_by(&parent, |key| &key.parent)
            {
                if child.kind == FilesystemPathKind::Directory {
                    queue.push_back(entry_identity(&child));
                }
                descendants.push(child);
            }
        }
        descendants
    }

    fn remove_entry(&mut self, entry: &FilesystemPathEntry) {
        let identity = entry_identity(entry);
        self.entries_by_path = self.entries_by_path.remove(&path_sort_key(entry));
        self.entries_by_identity = self.entries_by_identity.remove(&identity);
        self.entries_by_descriptor_id = self
            .entries_by_descriptor_id
            .remove(&descriptor_id_sort_key(entry));
        if entry.kind == FilesystemPathKind::File {
            self.files_by_id = self.files_by_id.remove(&file_id_sort_key(entry));
            self.file_count = self.file_count.saturating_sub(1);
        } else {
            self.directory_count = self.directory_count.saturating_sub(1);
        }
        if let Some(parent) = entry.parent_identity.clone() {
            self.children_by_parent = self.children_by_parent.remove(&FilesystemChildSortKey {
                parent,
                child: identity,
            });
        }
        self.estimated_heap_bytes = self
            .estimated_heap_bytes
            .saturating_sub(estimated_entry_index_bytes(entry));
    }

    fn insert_entry(&mut self, entry: Arc<FilesystemPathEntry>) {
        let identity = entry_identity(&entry);
        if let Some(previous) = self.entries_by_identity.get(&identity).cloned() {
            self.remove_entry(&previous);
        }
        self.entries_by_path = self
            .entries_by_path
            .insert(path_sort_key(&entry), Arc::clone(&entry));
        self.entries_by_identity = self
            .entries_by_identity
            .insert(identity.clone(), Arc::clone(&entry));
        self.entries_by_descriptor_id = self
            .entries_by_descriptor_id
            .insert(descriptor_id_sort_key(&entry), Arc::clone(&entry));
        if entry.kind == FilesystemPathKind::File {
            self.files_by_id = self
                .files_by_id
                .insert(file_id_sort_key(&entry), Arc::clone(&entry));
            self.file_count += 1;
        } else {
            self.directory_count += 1;
        }
        if let Some(parent) = entry.parent_identity.clone() {
            self.children_by_parent = self.children_by_parent.insert(
                FilesystemChildSortKey {
                    parent,
                    child: identity,
                },
                Arc::clone(&entry),
            );
        }
        self.estimated_heap_bytes = self
            .estimated_heap_bytes
            .saturating_add(estimated_entry_index_bytes(&entry));
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

fn for_each_committed_row_projection(
    request: &FilesystemPathIndexRequest,
    row: &FilesystemStateRow,
    mut apply: impl FnMut(&FilesystemStateRow) -> Result<(), LixError>,
) -> Result<(), LixError> {
    if row.global && !request.branch_ids.is_empty() {
        for branch_id in &request.branch_ids {
            let mut projected = row.clone();
            projected.branch_id = branch_id.clone().into();
            apply(&projected)?;
        }
    } else if row.global
        || request
            .branch_ids
            .iter()
            .any(|branch_id| branch_id.as_str() == row.branch_id.as_str())
    {
        apply(row)?;
    }
    Ok(())
}

fn entry_identity(entry: &FilesystemPathEntry) -> FilesystemPathEntryIdentity {
    FilesystemPathEntryIdentity {
        kind: entry.kind,
        key: entry.key.clone(),
    }
}

fn path_sort_key(entry: &FilesystemPathEntry) -> FilesystemPathSortKey {
    FilesystemPathSortKey {
        path: entry.path.clone(),
        identity: entry_identity(entry),
    }
}

fn file_id_sort_key(entry: &FilesystemPathEntry) -> FilesystemFileIdSortKey {
    FilesystemFileIdSortKey {
        id: entry.id().to_string(),
        path: path_sort_key(entry),
    }
}

fn descriptor_id_sort_key(entry: &FilesystemPathEntry) -> FilesystemDescriptorIdSortKey {
    FilesystemDescriptorIdSortKey {
        id: entry.id().to_string(),
        kind: entry.kind,
        identity: entry_identity(entry),
    }
}

fn estimated_index_heap_bytes(entries: &[Arc<FilesystemPathEntry>]) -> usize {
    size_of::<FilesystemPathIndex>()
        + entries
            .iter()
            .map(|entry| estimated_entry_index_bytes(entry))
            .sum::<usize>()
}

fn estimated_entry_index_bytes(entry: &FilesystemPathEntry) -> usize {
    size_of::<FilesystemPathEntry>()
        + size_of::<FilesystemPathSortKey>()
        + size_of::<FilesystemPathEntryIdentity>()
        + size_of::<FilesystemDescriptorIdSortKey>()
        + entry.estimated_heap_bytes()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FilesystemPathIndexRequest {
    pub(crate) branch_ids: Vec<String>,
    pub(crate) include_blob_refs: bool,
}

impl FilesystemPathIndexRequest {
    pub(crate) fn new(mut branch_ids: Vec<String>) -> Self {
        branch_ids.sort();
        branch_ids.dedup();
        Self {
            branch_ids,
            include_blob_refs: false,
        }
    }

    pub(crate) fn with_blob_refs(mut self, enabled: bool) -> Self {
        self.include_blob_refs = enabled;
        self
    }
}

#[async_trait]
pub(crate) trait FilesystemPathIndexReader: Send + Sync {
    async fn path_index(
        &self,
        request: &FilesystemPathIndexRequest,
    ) -> Result<Arc<FilesystemPathIndex>, LixError>;
}

/// Operation-owned filesystem index reader. The index is derived from the
/// caller's retained ForkTree capability; it never opens a second facade or
/// reaches the underlying storage read directly.
pub(crate) struct ForkTreeFilesystemPathIndexReader<R> {
    facade: ForkTreeReadFacade<R>,
}

impl<R> ForkTreeFilesystemPathIndexReader<R> {
    pub(crate) fn new(facade: ForkTreeReadFacade<R>) -> Self {
        Self { facade }
    }
}

#[async_trait]
impl<R> FilesystemPathIndexReader for ForkTreeFilesystemPathIndexReader<R>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    async fn path_index(
        &self,
        request: &FilesystemPathIndexRequest,
    ) -> Result<Arc<FilesystemPathIndex>, LixError> {
        build_path_index(&self.facade, request).await
    }
}

#[async_trait]
pub(crate) trait FilesystemPathIndexSource {
    async fn build_path_index(
        &self,
        request: &FilesystemPathIndexRequest,
    ) -> Result<Arc<FilesystemPathIndex>, LixError>;
}

#[async_trait]
impl<R> FilesystemPathIndexSource for &ForkTreeStateView<R>
where
    R: StorageAdapterRead + Send + Sync + 'static,
{
    async fn build_path_index(
        &self,
        request: &FilesystemPathIndexRequest,
    ) -> Result<Arc<FilesystemPathIndex>, LixError> {
        build_path_index_from_state(self, request).await
    }
}

#[async_trait]
impl<R> FilesystemPathIndexSource for &TransactionStateView<R>
where
    R: StorageAdapterRead + Send + Sync + 'static,
{
    async fn build_path_index(
        &self,
        request: &FilesystemPathIndexRequest,
    ) -> Result<Arc<FilesystemPathIndex>, LixError> {
        if request.branch_ids.is_empty() {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "filesystem path index requires one operation-owned branch state view",
            ));
        }

        // A transaction view's active branch is only the default selector. A
        // public *_by_branch request may name several branches, and each must
        // be resolved through the same retained read with its staged overlay
        // applied only to the transaction's branch. Keep the physical lanes
        // separate until the canonical filesystem-key merge below so an
        // untracked tombstone can shadow a tracked row without resurrection.
        let mut rows = Vec::new();
        let empty_pk = crate::row_pk::RowPk {
            components: crate::row_pk::RowPkComponents::Empty,
        };
        for branch_id in &request.branch_ids {
            let mut tracked_rows = Vec::new();
            for schema_key in crate::filesystem::filesystem_schema_keys() {
                let lower = crate::forktree::encode_state_row_prefix(&schema_key, &empty_pk);
                let upper = crate::forktree::exclusive_prefix_upper_bound(&lower);
                tracked_rows.extend(
                    self.branch_range(
                        branch_id,
                        Some(&lower),
                        upper.as_deref(),
                        None,
                        true,
                    )
                    .await
                    .map_err(|error| {
                        LixError::new(LixError::CODE_STORAGE_ERROR, error.to_string())
                    })?,
                );
            }
            rows.extend(FilesystemStateRows::from_view_rows(
                tracked_rows,
                branch_id,
                false,
            )?);
        }

        let rows = merge_filesystem_state_rows(rows, true);
        #[cfg(test)]
        {
            FULL_REBUILD_BUILDS.fetch_add(1, Ordering::SeqCst);
            FULL_REBUILD_DESCRIPTOR_ROWS.fetch_add(rows.len(), Ordering::SeqCst);
        }
        Ok(Arc::new(FilesystemPathIndex::from_state_rows(&rows)?))
    }
}

#[async_trait]
impl<R> FilesystemPathIndexSource for &ForkTreeReadFacade<R>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    async fn build_path_index(
        &self,
        request: &FilesystemPathIndexRequest,
    ) -> Result<Arc<FilesystemPathIndex>, LixError> {
        let branch_id = request.branch_ids.first().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                "filesystem path index requires one operation-owned branch state view",
            )
        })?;
        let state = ForkTreeStateView::from_facade((*self).clone(), branch_id).await?;
        build_path_index_from_state(&state, request).await
    }
}

pub(crate) async fn build_path_index<S>(
    source: S,
    request: &FilesystemPathIndexRequest,
) -> Result<Arc<FilesystemPathIndex>, LixError>
where
    S: FilesystemPathIndexSource + Send + Sync,
{
    source.build_path_index(request).await
}

async fn build_path_index_from_state<R>(
    state: &ForkTreeStateView<R>,
    request: &FilesystemPathIndexRequest,
) -> Result<Arc<FilesystemPathIndex>, LixError>
where
    R: StorageAdapterRead,
{
    if request.branch_ids.is_empty() {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "filesystem path index requires one operation-owned branch state view",
        ));
    }
    let mut rows = Vec::new();
    for branch_id in &request.branch_ids {
        let tracked_rows =
            crate::filesystem::filesystem_state_rows_for_branch(state, branch_id, true).await?;
        rows.extend(FilesystemStateRows::from_view_rows(
            tracked_rows,
            branch_id,
            false,
        )?);
    }
    let rows = merge_filesystem_state_rows(rows, true);
    #[cfg(test)]
    {
        FULL_REBUILD_BUILDS.fetch_add(1, Ordering::SeqCst);
        FULL_REBUILD_DESCRIPTOR_ROWS.fetch_add(rows.len(), Ordering::SeqCst);
    }
    Ok(Arc::new(FilesystemPathIndex::from_state_rows(&rows)?))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CacheKey {
    branch_ids: Vec<String>,
    revision: Option<Vec<u8>>,
    include_blob_refs: bool,
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
    /// Evicts transaction-local cached views after restoring an earlier staged
    /// write checkpoint. Rebuilding from the restored overlay is cheaper and
    /// safer than cloning potentially large path indexes for an error path.
    pub(crate) fn clear(&self) {
        self.entries
            .lock()
            .expect("filesystem path cache lock poisoned")
            .clear();
    }

    pub(crate) fn get(
        &self,
        request: &FilesystemPathIndexRequest,
        revision: Option<&[u8]>,
    ) -> Option<Arc<FilesystemPathIndex>> {
        let key = CacheKey {
            branch_ids: request.branch_ids.clone(),
            revision: revision.map(<[u8]>::to_vec),
            include_blob_refs: request.include_blob_refs,
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
            include_blob_refs: request.include_blob_refs,
        };
        let mut entries = self
            .entries
            .lock()
            .expect("filesystem path cache lock poisoned");
        if let Some(position) = entries.iter().position(|candidate| candidate.key == key) {
            return Arc::clone(&entries[position].index);
        }
        let index = if index.generation() == revision {
            index
        } else {
            Arc::new((*index).clone().with_generation(revision))
        };
        entries.retain(|candidate| {
            candidate.key.branch_ids != key.branch_ids
                || candidate.key.include_blob_refs != key.include_blob_refs
        });
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

    /// Advances cached views whose revision can be mapped to a successor.
    ///
    /// Transaction-local path indexes use a composite revision that changes
    /// after every staged descriptor batch. Applying that batch directly
    /// avoids rebuilding the complete visible filesystem for the next
    /// statement in the same transaction.
    pub(crate) fn advance_revisions(
        &self,
        rows: &[FilesystemStateRow],
        next_revision_for: impl Fn(&[u8]) -> Option<Vec<u8>>,
    ) {
        let invalidates_delta = rows.iter().any(|row| row.global);
        let mut entries = self
            .entries
            .lock()
            .expect("filesystem path cache lock poisoned");
        let mut advanced = Vec::new();
        entries.retain(|candidate| {
            let Some(previous_revision) = candidate.key.revision.as_deref() else {
                return true;
            };
            let Some(next_revision) = next_revision_for(previous_revision) else {
                return true;
            };
            if invalidates_delta || candidate.key.branch_ids.len() != 1 {
                return false;
            }
            let request = FilesystemPathIndexRequest::new(candidate.key.branch_ids.clone())
                .with_blob_refs(candidate.key.include_blob_refs);
            if let Ok(index) =
                candidate
                    .index
                    .apply_committed_rows(&request, rows, Some(next_revision.as_slice()))
            {
                advanced.push((request, next_revision, Arc::new(index)));
            }
            false
        });
        for (request, revision, index) in advanced {
            let key = CacheKey {
                branch_ids: request.branch_ids,
                revision: Some(revision),
                include_blob_refs: request.include_blob_refs,
            };
            let bytes = size_of::<CachedIndex>()
                + key.estimated_heap_bytes()
                + index.estimated_heap_bytes();
            entries.push(CachedIndex { key, index, bytes });
        }
        while entries.len() > 1
            && entries
                .iter()
                .map(|candidate| candidate.bytes)
                .sum::<usize>()
                > MAX_CACHE_BYTES
        {
            entries.remove(0);
        }
    }
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
    use crate::row_pk::RowPk;

    fn path_index_from_rows(
        rows: Vec<FilesystemStateRow>,
    ) -> Result<FilesystemPathIndex, LixError> {
        FilesystemPathIndex::from_state_rows(&FilesystemStateRows::from_rows(rows))
    }

    #[test]
    fn exact_range_and_order_preserve_path_buckets() {
        let index = path_index_from_rows(vec![
            directory_row(
                "01920000-0000-7000-8000-0000000000d3",
                None,
                "docs",
                "01920000-0000-7000-8000-0000000000a1",
                false,
            ),
            file_row(
                "01920000-0000-7000-8000-0000000000a2",
                Some("01920000-0000-7000-8000-0000000000d3"),
                "a.md",
                "01920000-0000-7000-8000-0000000000a1",
                false,
            ),
            file_row(
                "01920000-0000-7000-8000-0000000000b2",
                Some("01920000-0000-7000-8000-0000000000d3"),
                "b.md",
                "01920000-0000-7000-8000-0000000000a1",
                false,
            ),
            file_row(
                "01920000-0000-7000-8000-0000000000a2-global",
                Some("01920000-0000-7000-8000-0000000000d3-global"),
                "a.md",
                "01920000-0000-7000-8000-0000000000a1",
                true,
            ),
            directory_row(
                "01920000-0000-7000-8000-0000000000d3-global",
                None,
                "docs",
                "01920000-0000-7000-8000-0000000000a1",
                true,
            ),
        ])
        .expect("path index should build");

        assert_eq!(index.exact_entries("/docs/a.md").len(), 2);
        assert!(index.exact_entries("/docs/missing.md").is_empty());
        let range =
            index.range_entries(Bound::Included("/docs/a.md"), Bound::Excluded("/docs/c.md"));
        assert_eq!(
            range
                .iter()
                .map(|entry| entry.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/a.md", "/docs/a.md", "/docs/b.md"]
        );
        assert_eq!(
            index
                .entries()
                .iter()
                .map(|entry| entry.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs", "/docs", "/docs/a.md", "/docs/a.md", "/docs/b.md"]
        );
        assert!(index.estimated_heap_bytes() > 0);
    }

    #[test]
    fn exact_file_id_entries_keep_every_file_lane_and_exclude_directories() {
        let index = path_index_from_rows(vec![
            directory_row(
                "shared",
                None,
                "directory",
                "01920000-0000-7000-8000-0000000000a1",
                false,
            ),
            file_row(
                "shared",
                None,
                "z-tracked.md",
                "01920000-0000-7000-8000-0000000000a1",
                false,
            ),
            file_row(
                "shared",
                None,
                "a-global.md",
                "01920000-0000-7000-8000-0000000000a1",
                true,
            ),
            file_row(
                "other",
                None,
                "other.md",
                "01920000-0000-7000-8000-0000000000a1",
                false,
            ),
        ])
        .expect("path index should build");

        let matches = index.exact_file_id_entries("shared");
        assert_eq!(
            matches
                .iter()
                .map(|entry| entry.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/a-global.md", "/z-tracked.md"]
        );
        assert!(
            matches
                .iter()
                .all(|entry| entry.kind == FilesystemPathKind::File)
        );
        assert!(index.exact_file_id_entries("missing").is_empty());
        assert!(index.estimated_heap_bytes() > size_of::<FilesystemPathIndex>());
    }

    #[test]
    fn cache_supersedes_older_revision_for_the_same_scope() {
        let cache = FilesystemPathIndexCache::default();
        let request = FilesystemPathIndexRequest::new(vec![
            "01920000-0000-7000-8000-0000000000a1".to_string(),
        ]);
        let first = Arc::new(FilesystemPathIndex::default());
        let second = Arc::new(FilesystemPathIndex::default());

        cache.insert(&request, Some(&[1]), first);
        let second = cache.insert(&request, Some(&[2]), second);

        assert!(cache.get(&request, Some(&[1])).is_none());
        assert!(Arc::ptr_eq(
            &cache.get(&request, Some(&[2])).expect("new revision"),
            &second
        ));
    }

    #[test]
    fn blob_refs_build_and_advance_with_their_file_entry() {
        let request = FilesystemPathIndexRequest::new(vec!["branch-a".to_string()]);
        let prior = path_index_from_rows(vec![
            file_row("file-a", None, "a.md", "branch-a", false),
            blob_row("file-a", "hash-before", "branch-a"),
        ])
        .expect("path index should build");
        let prior_entry = &prior.exact_entries("/a.md")[0];
        assert_eq!(
            prior_entry
                .blob_ref_state_row()
                .and_then(|row| row.snapshot_content.as_deref()),
            Some(r#"{"blob_hash":"hash-before","id":"file-a","size_bytes":7}"#)
        );

        let next = prior
            .apply_committed_rows(
                &request,
                &[blob_row("file-a", "hash-after", "branch-a")],
                Some(&[2]),
            )
            .expect("blob-ref delta should apply");
        assert_eq!(
            next.exact_entries("/a.md")[0]
                .blob_ref_state_row()
                .and_then(|row| row.snapshot_content.as_deref()),
            Some(r#"{"blob_hash":"hash-after","id":"file-a","size_bytes":7}"#)
        );
        assert_eq!(
            prior_entry
                .blob_ref_state_row()
                .and_then(|row| row.snapshot_content.as_deref()),
            Some(r#"{"blob_hash":"hash-before","id":"file-a","size_bytes":7}"#)
        );

        let mut tombstone = blob_row("file-a", "unused", "branch-a");
        tombstone.snapshot_content = None;
        tombstone.deleted = true;
        let deleted = next
            .apply_committed_rows(&request, &[tombstone], Some(&[3]))
            .expect("blob-ref tombstone should apply");
        assert!(
            deleted.exact_entries("/a.md")[0]
                .blob_ref_state_row()
                .is_none()
        );
    }

    #[test]
    fn global_blob_delta_updates_its_projected_branch_entry() {
        let request = FilesystemPathIndexRequest::new(vec!["branch-a".to_string()]);
        let mut projected_blob = blob_row("global-file", "hash-before", "branch-a");
        projected_blob.global = true;
        let prior = path_index_from_rows(vec![
            file_row("global-file", None, "global.md", "branch-a", true),
            projected_blob,
        ])
        .expect("projected global path index should build");

        let mut committed_blob = blob_row("global-file", "hash-after", crate::GLOBAL_BRANCH_ID);
        committed_blob.global = true;
        let next = prior
            .apply_committed_rows(&request, &[committed_blob], Some(&[2]))
            .expect("global blob delta should project into the cached branch");

        let entries = next.exact_entries("/global.md");
        assert_eq!(entries.len(), 1);
        let blob_ref = entries[0]
            .blob_ref_state_row()
            .expect("projected file should retain its updated blob ref");
        assert_eq!(blob_ref.branch_id.as_str(), "branch-a");
        assert_eq!(
            blob_ref.snapshot_content.as_deref(),
            Some(r#"{"blob_hash":"hash-after","id":"global-file","size_bytes":7}"#)
        );
    }

    #[test]
    fn global_descriptor_delta_updates_only_unshadowed_branch_entries() {
        let request =
            FilesystemPathIndexRequest::new(vec!["branch-a".to_string(), "branch-b".to_string()]);
        let prior = path_index_from_rows(vec![
            file_row("global-file", None, "local.md", "branch-a", false),
            file_row("global-file", None, "global.md", "branch-b", true),
        ])
        .expect("mixed local and projected global path index should build");

        let committed = file_row(
            "global-file",
            None,
            "updated-global.md",
            crate::GLOBAL_BRANCH_ID,
            true,
        );
        let next = prior
            .apply_committed_rows(&request, &[committed], Some(&[2]))
            .expect("global descriptor delta should update each unshadowed projection");

        assert_eq!(next.exact_entries("/local.md").len(), 1);
        assert_eq!(next.exact_entries("/updated-global.md").len(), 1);
        assert!(next.exact_entries("/global.md").is_empty());
        assert_eq!(next.entries().len(), 2);
    }

    #[test]
    fn committed_file_delta_advances_generation_without_mutating_prior_snapshot() {
        let request = FilesystemPathIndexRequest::new(vec![
            "01920000-0000-7000-8000-0000000000a1".to_string(),
        ]);
        let prior = path_index_from_rows(vec![file_row(
            "01920000-0000-7000-8000-0000000000a2",
            None,
            "before.md",
            "01920000-0000-7000-8000-0000000000a1",
            false,
        )])
        .expect("prior index should build")
        .with_generation(Some(&[1]));

        let next = prior
            .apply_committed_rows(
                &request,
                &[file_row(
                    "01920000-0000-7000-8000-0000000000a2",
                    None,
                    "after.md",
                    "01920000-0000-7000-8000-0000000000a1",
                    false,
                )],
                Some(&[2]),
            )
            .expect("descriptor delta should apply");

        assert_eq!(prior.generation(), Some([1].as_slice()));
        assert_eq!(next.generation(), Some([2].as_slice()));
        assert_eq!(prior.exact_entries("/before.md").len(), 1);
        assert!(prior.exact_entries("/after.md").is_empty());
        assert!(next.exact_entries("/before.md").is_empty());
        assert_eq!(next.exact_entries("/after.md").len(), 1);
        assert_eq!(
            next.exact_file_id_entries("01920000-0000-7000-8000-0000000000a2")[0].path,
            "/after.md"
        );
    }

    #[test]
    fn committed_branch_delta_preserves_another_branch_lane() {
        let request = FilesystemPathIndexRequest::new(vec![
            "01920000-0000-7000-8000-0000000000a1".to_string(),
            "01920000-0000-7000-8000-0000000000b1".to_string(),
        ]);
        let prior = path_index_from_rows(vec![
            file_row(
                "shared",
                None,
                "a.md",
                "01920000-0000-7000-8000-0000000000a1",
                false,
            ),
            file_row(
                "shared",
                None,
                "b.md",
                "01920000-0000-7000-8000-0000000000b1",
                false,
            ),
        ])
        .expect("prior index should build");

        let next = prior
            .apply_committed_rows(
                &request,
                &[file_row(
                    "shared",
                    None,
                    "a2.md",
                    "01920000-0000-7000-8000-0000000000a1",
                    false,
                )],
                Some(&[2]),
            )
            .expect("descriptor delta should apply");

        assert!(next.exact_entries("/a.md").is_empty());
        assert_eq!(next.exact_entries("/a2.md").len(), 1);
        assert_eq!(next.exact_entries("/b.md").len(), 1);
    }

    #[test]
    fn committed_directory_delta_rewrites_only_its_descendants() {
        let request = FilesystemPathIndexRequest::new(vec![
            "01920000-0000-7000-8000-0000000000a1".to_string(),
        ]);
        let prior = path_index_from_rows(vec![
            directory_row(
                "docs",
                None,
                "docs",
                "01920000-0000-7000-8000-0000000000a1",
                false,
            ),
            directory_row(
                "nested",
                Some("docs"),
                "nested",
                "01920000-0000-7000-8000-0000000000a1",
                false,
            ),
            file_row(
                "inside",
                Some("nested"),
                "inside.md",
                "01920000-0000-7000-8000-0000000000a1",
                false,
            ),
            file_row(
                "outside",
                None,
                "outside.md",
                "01920000-0000-7000-8000-0000000000a1",
                false,
            ),
        ])
        .expect("prior index should build");

        let next = prior
            .apply_committed_rows(
                &request,
                &[directory_row(
                    "docs",
                    None,
                    "archive",
                    "01920000-0000-7000-8000-0000000000a1",
                    false,
                )],
                Some(&[2]),
            )
            .expect("subtree delta should apply");

        assert_eq!(next.exact_entries("/archive").len(), 1);
        assert_eq!(next.exact_entries("/archive/nested").len(), 1);
        assert_eq!(next.exact_entries("/archive/nested/inside.md").len(), 1);
        assert_eq!(next.exact_entries("/outside.md").len(), 1);
        assert!(next.exact_entries("/docs/nested/inside.md").is_empty());
    }

    #[test]
    fn committed_directory_tombstone_does_not_resurrect_removed_children() {
        let request = FilesystemPathIndexRequest::new(vec![
            "01920000-0000-7000-8000-0000000000a1".to_string(),
        ]);
        let prior = path_index_from_rows(vec![
            directory_row(
                "docs",
                None,
                "docs",
                "01920000-0000-7000-8000-0000000000a1",
                false,
            ),
            directory_row(
                "nested",
                Some("docs"),
                "nested",
                "01920000-0000-7000-8000-0000000000a1",
                false,
            ),
            file_row(
                "inside",
                Some("nested"),
                "inside.md",
                "01920000-0000-7000-8000-0000000000a1",
                false,
            ),
        ])
        .expect("prior index should build");
        let mut tombstone = directory_row(
            "docs",
            None,
            "docs",
            "01920000-0000-7000-8000-0000000000a1",
            false,
        );
        tombstone.snapshot_content = None;
        tombstone.deleted = true;
        let deleted = prior
            .apply_committed_rows(&request, &[tombstone], Some(&[2]))
            .expect("subtree tombstone should apply");
        assert!(deleted.entries().is_empty());

        let recreated = deleted
            .apply_committed_rows(
                &request,
                &[directory_row(
                    "docs",
                    None,
                    "recreated",
                    "01920000-0000-7000-8000-0000000000a1",
                    false,
                )],
                Some(&[3]),
            )
            .expect("root recreation should apply");
        assert_eq!(recreated.exact_entries("/recreated").len(), 1);
        assert_eq!(recreated.entries().len(), 1);
    }

    fn directory_row(
        id: &str,
        parent_id: Option<&str>,
        name: &str,
        branch_id: &str,
        global: bool,
    ) -> FilesystemStateRow {
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
    ) -> FilesystemStateRow {
        live_row(
            id,
            FILE_DESCRIPTOR_SCHEMA_KEY,
            serde_json::json!({"id": id, "directory_id": directory_id, "name": name}).to_string(),
            branch_id,
            global,
        )
    }

    fn blob_row(id: &str, blob_hash: &str, branch_id: &str) -> FilesystemStateRow {
        let mut row = live_row(
            id,
            BLOB_REF_SCHEMA_KEY,
            serde_json::json!({
                "id": id,
                "blob_hash": blob_hash,
                "size_bytes": 7,
            })
            .to_string(),
            branch_id,
            false,
        );
        row.file_id = Some(id.to_string());
        row
    }

    fn live_row(
        id: &str,
        schema_key: &str,
        snapshot_content: String,
        branch_id: &str,
        global: bool,
    ) -> FilesystemStateRow {
        FilesystemStateRow {
            row_pk: RowPk::single(id),
            schema_key: schema_key.to_string(),
            file_id: None,
            snapshot_content: Some(snapshot_content.into()),
            metadata: None,
            deleted: false,
            created_at: LixTimestamp::expect_parse(
                "filesystem path index test created_at",
                "2026-01-01T00:00:00.000Z",
            ),
            updated_at: LixTimestamp::expect_parse(
                "filesystem path index test updated_at",
                "2026-01-01T00:00:00.000Z",
            ),
            global,
            change_id: Some(ChangeId::for_test_label(id)),
            commit_id: Some(CommitId::for_test_label(id)),
            untracked: false,
            branch_id: branch_id.into(),
        }
    }
}
