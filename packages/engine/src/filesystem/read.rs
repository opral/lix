use std::collections::BTreeMap;

use serde::Deserialize;

use crate::LixError;
use crate::common::compose_file_path;

use super::keys::{
    BLOB_REF_SCHEMA_KEY, DIRECTORY_DESCRIPTOR_SCHEMA_KEY, FILE_DESCRIPTOR_SCHEMA_KEY,
};
use super::planner::{FilesystemBlobRefKey, FilesystemDescriptorKey, FilesystemRowContext};
use super::{DirectoryPathRecord, derive_directory_paths};

#[derive(Debug, Clone)]
pub(crate) struct FilesystemIndex {
    entries_by_path: BTreeMap<String, FilesystemEntry>,
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
                        snapshot,
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

        for (directory_id, snapshot) in &directory_rows {
            let path = directory_paths_by_id.get(directory_id).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    format!("directory {:?} is not reachable from root", snapshot.id),
                )
            })?;
            insert_entry(
                &mut entries_by_path,
                path.clone(),
                FilesystemEntry::Directory,
            )?;
        }

        for (snapshot, scope) in file_rows {
            let file_key =
                FilesystemDescriptorKey::from_context(&scope.context(None), &snapshot.id);
            let path = match snapshot.directory_id.as_ref() {
                Some(directory_id) => {
                    let directory_key = file_directory_parent_keys(&file_key, directory_id)
                        .into_iter()
                        .find(|key| directory_paths_by_id.contains_key(key));
                    let directory_path = directory_key
                        .as_ref()
                        .and_then(|key| directory_paths_by_id.get(key))
                        .ok_or_else(|| {
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
            let file = FilesystemFileEntry {
                id: snapshot.id.clone(),
                directory_id: snapshot.directory_id,
                name: snapshot.name,
                blob_hash: blob_hashes_by_key
                    .get(&FilesystemBlobRefKey::from_context(
                        &scope.context(Some(snapshot.id.clone())),
                        &snapshot.id,
                    ))
                    .cloned(),
                scope,
            };
            insert_entry(
                &mut entries_by_path,
                path.clone(),
                FilesystemEntry::File(file),
            )?;
        }

        Ok(Self { entries_by_path })
    }

    pub(crate) fn file_entries(&self) -> impl Iterator<Item = (&str, &FilesystemFileEntry)> {
        self.entries_by_path
            .iter()
            .filter_map(|(path, entry)| match entry {
                FilesystemEntry::File(file) => Some((path.as_str(), file)),
                FilesystemEntry::Directory => None,
            })
    }

    pub(crate) fn file_entry(&self, path: &str) -> Option<&FilesystemFileEntry> {
        match self.entries_by_path.get(path) {
            Some(FilesystemEntry::File(file)) => Some(file),
            _ => None,
        }
    }
}

pub(crate) fn filesystem_schema_keys() -> Vec<String> {
    vec![
        DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
        FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
        BLOB_REF_SCHEMA_KEY.to_string(),
    ]
}

#[derive(Debug, Clone)]
enum FilesystemEntry {
    Directory,
    File(FilesystemFileEntry),
}

#[derive(Debug, Clone)]
pub(crate) struct FilesystemFileEntry {
    pub(crate) id: String,
    pub(crate) directory_id: Option<String>,
    pub(crate) name: String,
    pub(crate) blob_hash: Option<String>,
    pub(crate) scope: RowScope,
}

#[derive(Debug, Clone)]
pub(crate) struct RowScope {
    pub(crate) branch_id: String,
    pub(crate) global: bool,
    pub(crate) untracked: bool,
    pub(crate) file_id: Option<String>,
}

impl RowScope {
    pub(crate) fn context(&self, file_id: Option<String>) -> FilesystemRowContext {
        FilesystemRowContext {
            branch_id: self.branch_id.clone(),
            global: self.global,
            untracked: self.untracked,
            file_id: file_id.or_else(|| self.file_id.clone()),
            metadata: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct DirectorySnapshot {
    id: String,
    parent_id: Option<String>,
    name: String,
}

impl DirectorySnapshot {
    fn parent_key(&self, key: &FilesystemDescriptorKey) -> Option<FilesystemDescriptorKey> {
        self.parent_id
            .as_deref()
            .map(|parent_id| key.in_same_scope(parent_id))
    }

    fn parent_keys(&self, key: &FilesystemDescriptorKey) -> Vec<FilesystemDescriptorKey> {
        let Some(parent_id) = self.parent_id.as_deref() else {
            return Vec::new();
        };
        let mut keys = vec![key.in_same_scope(parent_id)];
        if key.is_untracked() {
            keys.push(key.in_tracked_scope(parent_id));
        }
        keys
    }
}

impl DirectoryPathRecord for DirectorySnapshot {
    type Key = FilesystemDescriptorKey;

    fn parent_key(&self, key: &Self::Key) -> Option<Self::Key> {
        Self::parent_key(self, key)
    }

    fn parent_keys(&self, key: &Self::Key) -> Vec<Self::Key> {
        Self::parent_keys(self, key)
    }

    fn name(&self) -> &str {
        &self.name
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
        FilesystemEntry::Directory => path
            .strip_suffix('/')
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| path.to_string()),
        FilesystemEntry::File(_) => format!("{path}/"),
    }
}

fn entry_label(entry: &FilesystemEntry) -> &'static str {
    match entry {
        FilesystemEntry::Directory => "directory",
        FilesystemEntry::File(_) => "file",
    }
}

fn filesystem_conflict_error(message: String) -> LixError {
    LixError::new(LixError::CODE_CONSTRAINT_VIOLATION, message)
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
    use super::{FilesystemEntry, FilesystemFileEntry, RowScope};

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
            FilesystemEntry::Directory,
        )
        .expect_err("directory should conflict with file namespace");

        let mut entries = std::collections::BTreeMap::new();
        insert_entry(
            &mut entries,
            "/foo/".to_string(),
            FilesystemEntry::Directory,
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

        let Some(file) = file_entry_at(&index, "/readme.md") else {
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

        assert!(file_entry_at(&index, "/docs/root.txt").is_some());
        assert!(file_entry_at(&index, "/scoped/scoped.txt").is_some());
    }

    fn file_entry_at<'a>(
        index: &'a FilesystemIndex,
        path: &str,
    ) -> Option<&'a FilesystemFileEntry> {
        index
            .file_entries()
            .find_map(|(entry_path, file)| (entry_path == path).then_some(file))
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

    fn row_scope() -> RowScope {
        RowScope {
            branch_id: "branch-a".to_string(),
            global: false,
            untracked: false,
            file_id: None,
        }
    }
}
