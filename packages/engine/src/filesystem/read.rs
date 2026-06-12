use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;

use crate::LixError;
use crate::binary_cas::{BlobDataReader, BlobHash};
use crate::common::{LixPath, compose_file_path};
use crate::live_state::{LiveStateFilter, LiveStateReader, LiveStateScanRequest};
use crate::transaction::types::TransactionJson;

use super::keys::{
    BLOB_REF_SCHEMA_KEY, DIRECTORY_DESCRIPTOR_SCHEMA_KEY, FILE_DESCRIPTOR_SCHEMA_KEY,
};
use super::planner::{FilesystemBlobRefKey, FilesystemDescriptorKey, FilesystemRowContext};
use super::visibility::VisibleFilesystem;
use super::{DirectoryPathRecord, derive_directory_paths};

#[derive(Debug, Clone)]
pub(crate) struct FilesystemIndex {
    entries_by_path: BTreeMap<String, FilesystemEntry>,
    directories_by_key: BTreeMap<FilesystemDescriptorKey, FilesystemDirectoryEntry>,
    files_by_key: BTreeMap<FilesystemDescriptorKey, FilesystemFileEntry>,
    directory_paths_by_key: BTreeMap<FilesystemDescriptorKey, String>,
    file_paths_by_key: BTreeMap<FilesystemDescriptorKey, String>,
    directories_by_parent_id:
        BTreeMap<Option<FilesystemDescriptorKey>, BTreeSet<FilesystemDescriptorKey>>,
    files_by_directory_id:
        BTreeMap<Option<FilesystemDescriptorKey>, BTreeSet<FilesystemDescriptorKey>>,
    directory_keys_by_parent_and_name:
        BTreeMap<(Option<FilesystemDescriptorKey>, String), BTreeSet<FilesystemDescriptorKey>>,
    file_keys_by_directory_and_name:
        BTreeMap<(Option<FilesystemDescriptorKey>, String), BTreeSet<FilesystemDescriptorKey>>,
}

impl FilesystemIndex {
    pub(crate) fn from_live_rows(
        rows: Vec<crate::live_state::MaterializedLiveStateRow>,
    ) -> Result<Self, LixError> {
        let mut directory_rows = BTreeMap::<FilesystemDescriptorKey, DirectorySnapshot>::new();
        let mut file_rows = Vec::<(FileSnapshot, RowScope)>::new();
        let mut blob_hashes_by_key = BTreeMap::<FilesystemBlobRefKey, String>::new();

        for row in rows {
            let scope = RowScope {
                branch_id: row.branch_id.clone(),
                global: row.global,
                untracked: row.untracked,
                file_id: row.file_id.clone(),
            };
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
                    directory_rows.insert(
                        FilesystemDescriptorKey::from_live_row(&row, snapshot.id.clone()),
                        snapshot.with_scope(scope),
                    );
                }
                FILE_DESCRIPTOR_SCHEMA_KEY => {
                    let snapshot: FileSnapshot =
                        serde_json::from_str(snapshot_content).map_err(|error| {
                            LixError::unknown(format!(
                                "invalid lix_file_descriptor snapshot JSON: {error}"
                            ))
                        })?;
                    file_rows.push((snapshot, scope));
                }
                BLOB_REF_SCHEMA_KEY => {
                    let snapshot: BlobRefSnapshot = serde_json::from_str(snapshot_content)
                        .map_err(|error| {
                            LixError::unknown(format!(
                                "invalid lix_binary_blob_ref snapshot JSON: {error}"
                            ))
                        })?;
                    blob_hashes_by_key.insert(
                        FilesystemBlobRefKey::from_live_row(&row, snapshot.id),
                        snapshot.blob_hash,
                    );
                }
                _ => {}
            }
        }

        let directory_paths_by_id = derive_directory_paths(
            directory_rows
                .iter()
                .map(|(directory_id, row)| (directory_id.clone(), row)),
        )?;

        let mut entries_by_path = BTreeMap::new();
        let mut directories_by_key = BTreeMap::new();
        let mut files_by_key = BTreeMap::new();
        let mut directory_paths_by_key = BTreeMap::new();
        let mut file_paths_by_key = BTreeMap::new();
        let mut directories_by_parent_id =
            BTreeMap::<Option<FilesystemDescriptorKey>, BTreeSet<FilesystemDescriptorKey>>::new();
        let mut files_by_directory_id =
            BTreeMap::<Option<FilesystemDescriptorKey>, BTreeSet<FilesystemDescriptorKey>>::new();
        let mut directory_keys_by_parent_and_name = BTreeMap::<
            (Option<FilesystemDescriptorKey>, String),
            BTreeSet<FilesystemDescriptorKey>,
        >::new();
        let mut file_keys_by_directory_and_name = BTreeMap::<
            (Option<FilesystemDescriptorKey>, String),
            BTreeSet<FilesystemDescriptorKey>,
        >::new();

        for (directory_id, snapshot) in &directory_rows {
            let path = directory_paths_by_id.get(directory_id).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    format!("directory {:?} is not reachable from root", snapshot.id),
                )
            })?;
            let directory = FilesystemDirectoryEntry {
                id: snapshot.id.clone(),
                name: snapshot.name.clone(),
                scope: snapshot.scope()?,
            };
            insert_entry(
                &mut entries_by_path,
                path.clone(),
                FilesystemEntry::Directory(directory.clone()),
            )?;
            directories_by_key.insert(directory_id.clone(), directory);
            directory_paths_by_key.insert(directory_id.clone(), path.clone());
            let parent_key = snapshot.parent_key(directory_id);
            directories_by_parent_id
                .entry(parent_key.clone())
                .or_default()
                .insert(directory_id.clone());
            directory_keys_by_parent_and_name
                .entry((parent_key, snapshot.name.clone()))
                .or_default()
                .insert(directory_id.clone());
        }

        for (snapshot, scope) in file_rows {
            let file_key =
                FilesystemDescriptorKey::from_context(&scope.context(None, None), &snapshot.id);
            let path = match snapshot.directory_id.as_ref() {
                Some(directory_id) => {
                    let directory_key = file_key.in_same_scope(directory_id);
                    let directory_path =
                        directory_paths_by_id.get(&directory_key).ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_CONSTRAINT_VIOLATION,
                                format!(
                                    "file {:?} references missing directory {directory_id:?}",
                                    snapshot.id
                                ),
                            )
                        })?;
                    compose_file_path(Some(directory_path), &snapshot.name)?
                }
                None => compose_file_path(None, &snapshot.name)?,
            };
            let directory_key = snapshot
                .directory_id
                .as_ref()
                .map(|id| file_key.in_same_scope(id));
            let file = FilesystemFileEntry {
                id: snapshot.id.clone(),
                directory_id: snapshot.directory_id,
                name: snapshot.name,
                blob_hash: blob_hashes_by_key
                    .get(&FilesystemBlobRefKey::from_context(
                        &scope.context(Some(snapshot.id.clone()), None),
                        &snapshot.id,
                    ))
                    .cloned(),
                scope,
            };
            insert_entry(
                &mut entries_by_path,
                path.clone(),
                FilesystemEntry::File(file.clone()),
            )?;
            let file_name = file.name.clone();
            files_by_key.insert(file_key.clone(), file);
            file_paths_by_key.insert(file_key.clone(), path);
            files_by_directory_id
                .entry(directory_key.clone())
                .or_default()
                .insert(file_key.clone());
            file_keys_by_directory_and_name
                .entry((directory_key, file_name))
                .or_default()
                .insert(file_key);
        }

        Ok(Self {
            entries_by_path,
            directories_by_key,
            files_by_key,
            directory_paths_by_key,
            file_paths_by_key,
            directories_by_parent_id,
            files_by_directory_id,
            directory_keys_by_parent_and_name,
            file_keys_by_directory_and_name,
        })
    }

    pub(crate) fn entry(&self, path: &str) -> Option<&FilesystemEntry> {
        self.entries_by_path.get(path)
    }

    pub(crate) fn file_entries(&self) -> impl Iterator<Item = (&str, &FilesystemFileEntry)> {
        self.entries_by_path
            .iter()
            .filter_map(|(path, entry)| match entry {
                FilesystemEntry::File(file) => Some((path.as_str(), file)),
                FilesystemEntry::Directory(_) => None,
            })
    }

    pub(crate) async fn read_file_bytes(
        &self,
        path: &str,
        blob_reader: &dyn BlobDataReader,
    ) -> Result<Option<Vec<u8>>, LixError> {
        let Some(entry) = self.entry(path) else {
            let directory_path = LixPath::try_from_file_path(path)
                .expect("file path should parse before converting to a directory path")
                .to_file_path()
                + "/";
            if matches!(
                self.entry(&directory_path),
                Some(FilesystemEntry::Directory(_))
            ) {
                return Err(wrong_kind_error(path, "file", "directory"));
            }
            return Ok(None);
        };
        let file = match entry {
            FilesystemEntry::File(file) => file,
            FilesystemEntry::Directory(_) => {
                return Err(wrong_kind_error(path, "file", "directory"));
            }
        };
        let Some(blob_hash) = file.blob_hash.as_deref() else {
            return Ok(Some(Vec::new()));
        };
        let hash = BlobHash::from_hex(blob_hash)?;
        let mut bytes = blob_reader.load_bytes_many(&[hash]).await?.into_vec();
        let Some(bytes) = bytes.pop().flatten() else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("fs.read_file could not load binary data for {path:?}"),
            ));
        };
        Ok(Some(bytes))
    }

    pub(crate) fn readdir(&self, path: &str) -> Result<Option<Vec<FilesystemDirEntry>>, LixError> {
        let parent_key = if path == "/" {
            None
        } else {
            match self.entry(path) {
                Some(FilesystemEntry::Directory(directory)) => Some(directory.descriptor_key()),
                Some(FilesystemEntry::File(_)) => {
                    return Err(wrong_kind_error(path, "directory", "file"));
                }
                None => {
                    if let Some(file_path) = path.strip_suffix('/')
                        && matches!(self.entry(file_path), Some(FilesystemEntry::File(_)))
                    {
                        return Err(wrong_kind_error(file_path, "directory", "file"));
                    }
                    return Ok(None);
                }
            }
        };
        let mut entries = Vec::new();
        if let Some(directory_keys) = self.directories_by_parent_id.get(&parent_key) {
            for directory_key in directory_keys {
                if let Some(child_path) = self.directory_paths_by_key.get(directory_key)
                    && let Some(FilesystemEntry::Directory(directory)) =
                        self.entries_by_path.get(child_path)
                {
                    entries.push(FilesystemDirEntry {
                        name: directory.name.clone(),
                        path: child_path.clone(),
                        kind: FilesystemDirEntryKind::Directory,
                    });
                }
            }
        }
        if let Some(file_keys) = self.files_by_directory_id.get(&parent_key) {
            for file_key in file_keys {
                if let Some(child_path) = self.file_paths_by_key.get(file_key)
                    && let Some(FilesystemEntry::File(file)) = self.entries_by_path.get(child_path)
                {
                    entries.push(FilesystemDirEntry {
                        name: file.name.clone(),
                        path: child_path.clone(),
                        kind: FilesystemDirEntryKind::File,
                    });
                }
            }
        }
        entries.sort_by(|left, right| {
            left.name
                .cmp(&right.name)
                .then_with(|| entry_kind_order(left.kind).cmp(&entry_kind_order(right.kind)))
        });
        Ok(Some(entries))
    }

    pub(crate) fn has_children(&self, directory: &FilesystemDirectoryEntry) -> bool {
        let key = Some(directory.descriptor_key());
        self.directories_by_parent_id
            .get(&key)
            .is_some_and(|children| !children.is_empty())
            || self
                .files_by_directory_id
                .get(&key)
                .is_some_and(|children| !children.is_empty())
    }

    pub(crate) fn visible_filesystem(&self) -> VisibleFilesystem {
        VisibleFilesystem {
            directory_children_by_parent_id: descriptor_id_sets_by_parent(
                &self.directories_by_parent_id,
            ),
            files_by_directory_id: descriptor_id_sets_by_parent(&self.files_by_directory_id),
            blob_refs_by_key: self
                .entries_by_path
                .values()
                .filter_map(|entry| match entry {
                    FilesystemEntry::File(file) if file.blob_hash.is_some() => Some(
                        FilesystemBlobRefKey::from_context(&file.context(), &file.id),
                    ),
                    _ => None,
                })
                .collect(),
        }
    }

    pub(crate) fn reject_tracked_path_collision(
        &self,
        path: &str,
        operation: &str,
    ) -> Result<(), LixError> {
        if let Some(entry) = self.entries_by_path.get(path) {
            if !entry.untracked() {
                return Err(filesystem_conflict_error(format!(
                    "{operation} cannot create untracked path {path:?} over tracked filesystem entry"
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn reject_cross_lane_segments_collision(
        &self,
        segments: &[String],
        untracked: bool,
        operation: &str,
    ) -> Result<(), LixError> {
        let mut parent_key = None::<FilesystemDescriptorKey>;
        let mut namespace_segments = Vec::new();

        for segment in segments {
            namespace_segments.push(segment.clone());
            let lookup_parent = parent_key.clone();
            let mut same_lane_directory = None::<FilesystemDescriptorKey>;

            if let Some(directory_keys) = self
                .directory_keys_by_parent_and_name
                .get(&(lookup_parent.clone(), segment.clone()))
            {
                for directory_key in directory_keys {
                    let Some(directory) = self.directories_by_key.get(directory_key) else {
                        continue;
                    };
                    if directory.scope.untracked != untracked {
                        return Err(cross_lane_namespace_collision_error(
                            operation,
                            untracked,
                            &namespace_segments,
                            directory.scope.untracked,
                            "directory",
                        ));
                    }
                    same_lane_directory.get_or_insert_with(|| directory_key.clone());
                }
            }

            if let Some(file_keys) = self
                .file_keys_by_directory_and_name
                .get(&(lookup_parent, segment.clone()))
            {
                for file_key in file_keys {
                    let Some(file) = self.files_by_key.get(file_key) else {
                        continue;
                    };
                    if file.scope.untracked != untracked {
                        return Err(cross_lane_namespace_collision_error(
                            operation,
                            untracked,
                            &namespace_segments,
                            file.scope.untracked,
                            "file",
                        ));
                    }
                }
            }

            let Some(directory_key) = same_lane_directory else {
                break;
            };
            parent_key = Some(directory_key);
        }

        Ok(())
    }
}

fn cross_lane_namespace_collision_error(
    operation: &str,
    untracked: bool,
    namespace_segments: &[String],
    existing_untracked: bool,
    existing_label: &str,
) -> LixError {
    filesystem_conflict_error(format!(
        "{operation} cannot create {} path in namespace segments {namespace_segments:?} occupied by existing {} {existing_label}",
        lane_name(untracked),
        lane_name(existing_untracked),
    ))
}

fn descriptor_id_sets_by_parent(
    entries: &BTreeMap<Option<FilesystemDescriptorKey>, BTreeSet<FilesystemDescriptorKey>>,
) -> BTreeMap<Option<FilesystemDescriptorKey>, BTreeSet<String>> {
    entries
        .iter()
        .map(|(parent_key, child_keys)| {
            (
                parent_key.clone(),
                child_keys
                    .iter()
                    .map(|key| key.descriptor_id().to_string())
                    .collect(),
            )
        })
        .collect()
}

pub(crate) async fn load_filesystem_index(
    live_state: &dyn LiveStateReader,
    branch_id: &str,
) -> Result<FilesystemIndex, LixError> {
    let rows = live_state
        .scan_rows(&LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: filesystem_schema_keys(),
                branch_ids: vec![branch_id.to_string()],
                ..Default::default()
            },
            ..Default::default()
        })
        .await?;
    FilesystemIndex::from_live_rows(rows)
}

pub(crate) fn filesystem_schema_keys() -> Vec<String> {
    vec![
        DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
        FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
        BLOB_REF_SCHEMA_KEY.to_string(),
    ]
}

#[derive(Debug, Clone)]
pub(crate) enum FilesystemEntry {
    Directory(FilesystemDirectoryEntry),
    File(FilesystemFileEntry),
}

impl FilesystemEntry {
    fn untracked(&self) -> bool {
        match self {
            Self::Directory(entry) => entry.scope.untracked,
            Self::File(entry) => entry.scope.untracked,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FilesystemDirectoryEntry {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) scope: RowScope,
}

impl FilesystemDirectoryEntry {
    pub(crate) fn context(&self) -> FilesystemRowContext {
        self.scope.context(None, None)
    }

    pub(crate) fn descriptor_key(&self) -> FilesystemDescriptorKey {
        FilesystemDescriptorKey::from_context(&self.context(), &self.id)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FilesystemFileEntry {
    pub(crate) id: String,
    pub(crate) directory_id: Option<String>,
    pub(crate) name: String,
    pub(crate) blob_hash: Option<String>,
    pub(crate) scope: RowScope,
}

impl FilesystemFileEntry {
    pub(crate) fn context(&self) -> FilesystemRowContext {
        self.scope.context(None, None)
    }

    pub(crate) fn context_with_metadata(
        &self,
        metadata: Option<TransactionJson>,
    ) -> FilesystemRowContext {
        self.scope.context(None, metadata)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RowScope {
    pub(crate) branch_id: String,
    pub(crate) global: bool,
    pub(crate) untracked: bool,
    pub(crate) file_id: Option<String>,
}

impl RowScope {
    fn context(
        &self,
        file_id: Option<String>,
        metadata: Option<TransactionJson>,
    ) -> FilesystemRowContext {
        FilesystemRowContext {
            branch_id: self.branch_id.clone(),
            global: self.global,
            untracked: self.untracked,
            file_id: file_id.or_else(|| self.file_id.clone()),
            metadata,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FilesystemDirEntry {
    pub(crate) name: String,
    pub(crate) path: String,
    pub(crate) kind: FilesystemDirEntryKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FilesystemDirEntryKind {
    File,
    Directory,
}

#[derive(Debug, Clone, Deserialize)]
struct DirectorySnapshot {
    id: String,
    parent_id: Option<String>,
    name: String,
    #[serde(skip)]
    scope: Option<RowScope>,
}

impl DirectorySnapshot {
    fn with_scope(mut self, scope: RowScope) -> Self {
        self.scope = Some(scope);
        self
    }

    fn scope(&self) -> Result<RowScope, LixError> {
        self.scope.clone().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("directory {:?} is missing live-state scope", self.id),
            )
        })
    }

    fn parent_key(&self, key: &FilesystemDescriptorKey) -> Option<FilesystemDescriptorKey> {
        self.parent_id
            .as_deref()
            .map(|parent_id| key.in_same_scope(parent_id))
    }
}

impl DirectoryPathRecord for DirectorySnapshot {
    type Key = FilesystemDescriptorKey;

    fn parent_key(&self, key: &Self::Key) -> Option<Self::Key> {
        Self::parent_key(self, key)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Deserialize)]
struct FileSnapshot {
    id: String,
    directory_id: Option<String>,
    name: String,
}

#[derive(Debug, Deserialize)]
struct BlobRefSnapshot {
    id: String,
    blob_hash: String,
}

fn insert_entry(
    entries: &mut BTreeMap<String, FilesystemEntry>,
    path: String,
    entry: FilesystemEntry,
) -> Result<(), LixError> {
    if let Some(existing) = entries.get(&path) {
        return Err(filesystem_conflict_error(format!(
            "filesystem path {path:?} is claimed by both {} and {}",
            entry_label(existing),
            entry_label(&entry)
        )));
    }
    let namespace_conflict_path = namespace_conflict_path(&path, &entry);
    if let Some(existing) = entries.get(&namespace_conflict_path) {
        return Err(filesystem_conflict_error(format!(
            "filesystem namespace conflict at {path:?}: {} conflicts with existing {} at {namespace_conflict_path:?}",
            entry_label(&entry),
            entry_label(existing)
        )));
    }
    entries.insert(path, entry);
    Ok(())
}

fn namespace_conflict_path(path: &str, entry: &FilesystemEntry) -> String {
    match entry {
        FilesystemEntry::Directory(_) => path
            .strip_suffix('/')
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| path.to_string()),
        FilesystemEntry::File(_) => {
            LixPath::try_from_file_path(path)
                .expect("file path should parse before converting to a directory path")
                .to_file_path()
                + "/"
        }
    }
}

fn entry_label(entry: &FilesystemEntry) -> &'static str {
    match entry {
        FilesystemEntry::Directory(_) => "directory",
        FilesystemEntry::File(_) => "file",
    }
}

fn entry_kind_order(kind: FilesystemDirEntryKind) -> u8 {
    match kind {
        FilesystemDirEntryKind::Directory => 0,
        FilesystemDirEntryKind::File => 1,
    }
}

pub(crate) fn wrong_kind_error(path: &str, expected: &str, actual: &str) -> LixError {
    LixError::new(
        LixError::CODE_CONSTRAINT_VIOLATION,
        format!("fs path {path:?} expected {expected}, found {actual}"),
    )
}

pub(crate) fn filesystem_conflict_error(message: String) -> LixError {
    LixError::new(LixError::CODE_CONSTRAINT_VIOLATION, message)
}

fn lane_name(untracked: bool) -> &'static str {
    if untracked { "untracked" } else { "tracked" }
}

#[cfg(test)]
mod tests {
    use crate::changelog::{ChangeId, CommitId};
    use crate::entity_pk::EntityPk;
    use crate::live_state::MaterializedLiveStateRow;

    use super::{
        BLOB_REF_SCHEMA_KEY, DIRECTORY_DESCRIPTOR_SCHEMA_KEY, FILE_DESCRIPTOR_SCHEMA_KEY,
        FilesystemIndex, insert_entry,
    };
    use super::{FilesystemDirectoryEntry, FilesystemEntry, FilesystemFileEntry, RowScope};

    #[test]
    fn from_live_rows_rejects_file_directory_namespace_conflicts() {
        let error = FilesystemIndex::from_live_rows(vec![
            directory_row(
                "dir-foo",
                r#"{"id":"dir-foo","parent_id":null,"name":"foo"}"#,
            ),
            file_row(
                "file-foo",
                r#"{"id":"file-foo","directory_id":null,"name":"foo"}"#,
            ),
        ])
        .expect_err("file and directory with same parent/name should conflict");

        assert_eq!(error.code, crate::LixError::CODE_CONSTRAINT_VIOLATION);
        assert!(
            error.message.contains("filesystem namespace conflict"),
            "expected namespace conflict error: {error}"
        );
    }

    #[test]
    fn insert_entry_rejects_file_directory_namespace_conflicts_in_both_orders() {
        let mut entries = std::collections::BTreeMap::new();
        insert_entry(
            &mut entries,
            "/foo".to_string(),
            FilesystemEntry::File(file_entry("file-foo")),
        )
        .expect("initial file entry should insert");
        insert_entry(
            &mut entries,
            "/foo/".to_string(),
            FilesystemEntry::Directory(directory_entry("dir-foo")),
        )
        .expect_err("directory should conflict with file namespace");

        let mut entries = std::collections::BTreeMap::new();
        insert_entry(
            &mut entries,
            "/foo/".to_string(),
            FilesystemEntry::Directory(directory_entry("dir-foo")),
        )
        .expect("initial directory entry should insert");
        insert_entry(
            &mut entries,
            "/foo".to_string(),
            FilesystemEntry::File(file_entry("file-foo")),
        )
        .expect_err("file should conflict with directory namespace");
    }

    #[test]
    fn from_live_rows_attaches_blob_refs_by_storage_scope() {
        let index = FilesystemIndex::from_live_rows(vec![
            file_row(
                "file-readme",
                r#"{"id":"file-readme","directory_id":null,"name":"readme.md"}"#,
            ),
            live_row_with_scope(
                "file-readme",
                BLOB_REF_SCHEMA_KEY,
                r#"{"id":"file-readme","blob_hash":"abc123","size_bytes":5}"#,
                "branch-b",
                false,
                Some("file-readme".to_string()),
            ),
        ])
        .expect("filesystem index should load");

        let Some(FilesystemEntry::File(file)) = index.entry("/readme.md") else {
            panic!("readme file should be indexed");
        };
        assert_eq!(file.blob_hash, None);
    }

    #[test]
    fn from_live_rows_resolves_directories_by_storage_scope() {
        let index = FilesystemIndex::from_live_rows(vec![
            directory_row(
                "dir-shared",
                r#"{"id":"dir-shared","parent_id":null,"name":"docs"}"#,
            ),
            live_row_with_scope(
                "dir-shared",
                DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
                r#"{"id":"dir-shared","parent_id":null,"name":"scoped"}"#,
                "branch-a",
                false,
                Some("owner-file".to_string()),
            ),
            file_row(
                "file-root",
                r#"{"id":"file-root","directory_id":"dir-shared","name":"root.txt"}"#,
            ),
            live_row_with_scope(
                "file-scoped",
                FILE_DESCRIPTOR_SCHEMA_KEY,
                r#"{"id":"file-scoped","directory_id":"dir-shared","name":"scoped.txt"}"#,
                "branch-a",
                false,
                Some("owner-file".to_string()),
            ),
        ])
        .expect("filesystem index should keep scoped directories distinct");

        assert!(matches!(
            index.entry("/docs/root.txt"),
            Some(FilesystemEntry::File(_))
        ));
        assert!(matches!(
            index.entry("/scoped/scoped.txt"),
            Some(FilesystemEntry::File(_))
        ));
    }

    fn directory_row(entity_pk: &str, snapshot_content: &str) -> MaterializedLiveStateRow {
        live_row(entity_pk, DIRECTORY_DESCRIPTOR_SCHEMA_KEY, snapshot_content)
    }

    fn file_row(entity_pk: &str, snapshot_content: &str) -> MaterializedLiveStateRow {
        live_row(entity_pk, FILE_DESCRIPTOR_SCHEMA_KEY, snapshot_content)
    }

    fn live_row(
        entity_pk: &str,
        schema_key: &str,
        snapshot_content: &str,
    ) -> MaterializedLiveStateRow {
        live_row_with_scope(
            entity_pk,
            schema_key,
            snapshot_content,
            "branch-a",
            false,
            None,
        )
    }

    fn live_row_with_scope(
        entity_pk: &str,
        schema_key: &str,
        snapshot_content: &str,
        branch_id: &str,
        untracked: bool,
        file_id: Option<String>,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: EntityPk::single(entity_pk),
            schema_key: schema_key.to_string(),
            file_id,
            snapshot_content: Some(snapshot_content.to_string()),
            metadata: None,
            deleted: false,
            branch_id: branch_id.to_string(),
            change_id: Some(ChangeId::for_test_label(&format!("change-{entity_pk}"))),
            commit_id: Some(CommitId::for_test_label(&format!("commit-{entity_pk}"))),
            global: false,
            untracked,
            created_at: "2026-04-23T00:00:00Z".to_string(),
            updated_at: "2026-04-23T01:00:00Z".to_string(),
        }
    }

    fn file_entry(id: &str) -> FilesystemFileEntry {
        FilesystemFileEntry {
            id: id.to_string(),
            directory_id: None,
            name: "foo".to_string(),
            blob_hash: None,
            scope: row_scope(),
        }
    }

    fn directory_entry(id: &str) -> FilesystemDirectoryEntry {
        FilesystemDirectoryEntry {
            id: id.to_string(),
            name: "foo".to_string(),
            scope: row_scope(),
        }
    }

    fn row_scope() -> RowScope {
        RowScope {
            branch_id: "branch-a".to_string(),
            global: false,
            untracked: false,
            file_id: None,
        }
    }
}
