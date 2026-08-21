use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;

use crate::LixError;
use crate::common::compose_file_path;

use super::keys::{
    BLOB_REF_SCHEMA_KEY, DIRECTORY_DESCRIPTOR_SCHEMA_KEY, FILE_DESCRIPTOR_SCHEMA_KEY,
};
use super::planner::{FilesystemBlobRefKey, FilesystemDescriptorKey, FilesystemRowContext};
use super::{DirectoryPathRecord, derive_directory_paths};

/// Collects every file payload root selected by the authenticated serving
/// controls and retained commit/checkpoint roots. Tracked history is read from
/// commit state; current-only untracked rows are read from each control's
/// untracked selector through the live-state owner.
pub(crate) async fn collect_gc_binary_blob_roots<S>(
    store: &S,
    controls: &[(String, crate::branch::BranchHeadControl)],
    retained_commits: &BTreeSet<crate::changelog::CommitId>,
) -> Result<BTreeSet<crate::binary_cas::BlobId>, LixError>
where
    S: crate::storage_adapter::StorageAdapterRead,
{
    let request = crate::tracked_state::TrackedStateScanRequest {
        filter: crate::tracked_state::TrackedStateFilter {
            schema_keys: vec![BLOB_REF_SCHEMA_KEY.to_owned()],
            ..crate::tracked_state::TrackedStateFilter::default()
        },
        read_columns: crate::tracked_state::TrackedStateReadColumns {
            columns: vec!["snapshot_content".to_owned()],
        },
        limit: None,
    };
    let mut roots = BTreeSet::new();
    let current = crate::hot_state::TrackedHeadContext::new()
        .reader(store)
        .scan_live_batches_for_controls(controls, &request, Some(true))
        .await
        .map_err(|error| {
            LixError::new(
                error.code,
                format!("collect current binary blob roots: {}", error.message),
            )
        })?;
    for (_, rows) in current {
        for row in rows.iter() {
            if row.deleted() {
                continue;
            }
            if let Some(lix_schema::Value::Text(blob_hash)) = row
                .decoded_snapshot()
                .and_then(|typed| typed.row.get("blob_hash"))
            {
                roots.insert(crate::binary_cas::BlobId::from_hex(blob_hash)?);
            } else if let Some(snapshot) = row.snapshot_content() {
                roots.insert(blob_id_from_snapshot(snapshot.as_str())?);
            } else {
                return Err(LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "current binary blob reference has no payload",
                ));
            }
        }
    }

    let retained_schema_keys = [BLOB_REF_SCHEMA_KEY.to_owned()];
    for commit_id in retained_commits {
        for row in crate::tracked_state::load_retained_commit_snapshots_for_schemas(
            store,
            *commit_id,
            &retained_schema_keys,
        )
        .await?
        {
            if row.deleted {
                continue;
            }
            let typed = row.decoded_snapshot.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    format!("live binary blob reference in commit '{commit_id}' has no native payload"),
                )
            })?;
            let blob_hash = match typed.row.get("blob_hash") {
                Some(lix_schema::Value::Text(value)) => value,
                _ => {
                    return Err(LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        format!("live binary blob reference in commit '{commit_id}' has no blob_hash"),
                    ));
                }
            };
            roots.insert(crate::binary_cas::BlobId::from_hex(blob_hash)?);
        }
    }
    Ok(roots)
}

fn blob_id_from_snapshot(snapshot: &str) -> Result<crate::binary_cas::BlobId, LixError> {
    let snapshot: BlobRefSnapshot = serde_json::from_str(snapshot).map_err(|error| {
        LixError::new(
            LixError::CODE_STORAGE_ERROR,
            format!("invalid live binary blob reference snapshot: {error}"),
        )
    })?;
    crate::binary_cas::BlobId::from_hex(&snapshot.blob_hash)
}

#[derive(Debug, Clone)]
pub(crate) struct FilesystemIndex {
    entries_by_path: BTreeMap<String, FilesystemEntry>,
}

impl FilesystemIndex {
    pub(crate) fn from_live_batch(
        rows: &crate::hot_state::MaterializedHotStateBatch,
    ) -> Result<Self, LixError> {
        let mut directory_rows = BTreeMap::<FilesystemDescriptorKey, DirectorySnapshot>::new();
        let mut file_rows = Vec::<(FileSnapshot, RowScope)>::new();
        let mut blob_hashes_by_key = BTreeMap::<FilesystemBlobRefKey, String>::new();

        for row in rows.iter() {
            let scope = RowScope {
                branch_id: row.branch_id().to_string(),
                global: row.global(),
                untracked: row.untracked(),
                file_id: row.file_id().map(str::to_owned),
            };
            if row.deleted() {
                continue;
            }
            let snapshot = row.snapshot_json_value()?.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    format!("live filesystem row '{}' has no payload", row.schema_key()),
                )
            })?;
            match row.schema_key() {
                DIRECTORY_DESCRIPTOR_SCHEMA_KEY => {
                    let snapshot: DirectorySnapshot = serde_json::from_value(snapshot)
                        .map_err(|error| {
                            LixError::unknown(format!(
                                "invalid lix_directory_descriptor snapshot JSON: {error}"
                            ))
                        })?;
                    directory_rows.insert(
                        FilesystemDescriptorKey::from_live_row_ref(row, snapshot.id.clone()),
                        snapshot,
                    );
                }
                FILE_DESCRIPTOR_SCHEMA_KEY => {
                    let snapshot: FileSnapshot =
                        serde_json::from_value(snapshot).map_err(|error| {
                            LixError::unknown(format!(
                                "invalid lix_file_descriptor snapshot JSON: {error}"
                            ))
                        })?;
                    file_rows.push((
                        snapshot,
                        RowScope {
                            file_id: None,
                            ..scope
                        },
                    ));
                }
                BLOB_REF_SCHEMA_KEY => {
                    let snapshot: BlobRefSnapshot = serde_json::from_value(snapshot)
                        .map_err(|error| {
                            LixError::unknown(format!(
                                "invalid lix_binary_blob_ref snapshot JSON: {error}"
                            ))
                        })?;
                    blob_hashes_by_key.insert(
                        FilesystemBlobRefKey::from_live_row_ref(row, snapshot.id),
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
            let materialization_key = FilesystemBlobRefKey::from_context(
                &scope.context(Some(snapshot.id.clone())),
                &snapshot.id,
            );
            let file = FilesystemFileEntry {
                id: snapshot.id.clone(),
                directory_id: snapshot.directory_id,
                name: snapshot.name,
                blob_hash: blob_hashes_by_key.get(&materialization_key).cloned(),
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

    #[cfg(test)]
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
    entries.insert(path, entry);
    Ok(())
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
    use crate::common::LixTimestamp;
    use crate::row_pk::RowPk;
    use crate::hot_state::{MaterializedHotStateBatch, MaterializedHotStateRow};

    use super::{
        BLOB_REF_SCHEMA_KEY, DIRECTORY_DESCRIPTOR_SCHEMA_KEY, FILE_DESCRIPTOR_SCHEMA_KEY,
        FilesystemIndex, insert_entry,
    };
    use super::{FilesystemEntry, FilesystemFileEntry, RowScope};

    #[test]
    fn from_live_rows_rejects_file_directory_namespace_conflicts() {
        let error = filesystem_index_from_rows(vec![
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
            error.message.contains("claimed by both directory and file"),
            "expected exact path conflict error: {error}"
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
        insert_entry(&mut entries, "/foo".to_string(), FilesystemEntry::Directory)
            .expect_err("directory should conflict with file namespace");

        let mut entries = std::collections::BTreeMap::new();
        insert_entry(&mut entries, "/foo".to_string(), FilesystemEntry::Directory)
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
        let index = filesystem_index_from_rows(vec![
            file_row(
                "01920000-0000-7000-8000-0000000000d2",
                r#"{"id":"01920000-0000-7000-8000-0000000000d2","directory_id":null,"name":"readme.md"}"#,
            ),
            live_row_with_scope(
                "01920000-0000-7000-8000-0000000000d2",
                BLOB_REF_SCHEMA_KEY,
                r#"{"id":"01920000-0000-7000-8000-0000000000d2","blob_hash":"abc123","size_bytes":5}"#,
                "01920000-0000-7000-8000-0000000000b1",
                false,
                Some("01920000-0000-7000-8000-0000000000d2".to_string()),
            ),
        ])
        .expect("filesystem index should load");

        let Some(file) = file_entry_at(&index, "/readme.md") else {
            panic!("readme file should be indexed");
        };
        assert_eq!(file.blob_hash, None);
    }

    #[test]
    fn from_live_rows_resolves_directories_by_branch_scope() {
        let index = filesystem_index_from_rows(vec![
            directory_row(
                "dir-shared",
                r#"{"id":"dir-shared","parent_id":null,"name":"docs"}"#,
            ),
            live_row_with_scope(
                "dir-shared",
                DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
                r#"{"id":"dir-shared","parent_id":null,"name":"scoped"}"#,
                "01920000-0000-7000-8000-0000000000b1",
                false,
                None,
            ),
            file_row(
                "01920000-0000-7000-8000-000000000142",
                r#"{"id":"01920000-0000-7000-8000-000000000142","directory_id":"dir-shared","name":"root.txt"}"#,
            ),
            live_row_with_scope(
                "01920000-0000-7000-8000-000000000342",
                FILE_DESCRIPTOR_SCHEMA_KEY,
                r#"{"id":"01920000-0000-7000-8000-000000000342","directory_id":"dir-shared","name":"scoped.txt"}"#,
                "01920000-0000-7000-8000-0000000000b1",
                false,
                Some("01920000-0000-7000-8000-000000000342".to_string()),
            ),
        ])
        .expect("filesystem index should keep scoped directories distinct");

        assert!(file_entry_at(&index, "/docs/root.txt").is_some());
        assert!(file_entry_at(&index, "/scoped/scoped.txt").is_some());
    }

    fn filesystem_index_from_rows(
        rows: Vec<MaterializedHotStateRow>,
    ) -> Result<FilesystemIndex, crate::LixError> {
        let rows = MaterializedHotStateBatch::from_rows(rows);
        FilesystemIndex::from_live_batch(&rows)
    }

    fn file_entry_at<'a>(
        index: &'a FilesystemIndex,
        path: &str,
    ) -> Option<&'a FilesystemFileEntry> {
        index
            .file_entries()
            .find_map(|(entry_path, file)| (entry_path == path).then_some(file))
    }

    fn directory_row(row_pk: &str, snapshot_content: &str) -> MaterializedHotStateRow {
        live_row(row_pk, DIRECTORY_DESCRIPTOR_SCHEMA_KEY, snapshot_content)
    }

    fn file_row(row_pk: &str, snapshot_content: &str) -> MaterializedHotStateRow {
        live_row_with_scope(
            row_pk,
            FILE_DESCRIPTOR_SCHEMA_KEY,
            snapshot_content,
            "01920000-0000-7000-8000-0000000000a1",
            false,
            Some(row_pk.to_string()),
        )
    }

    fn live_row(
        row_pk: &str,
        schema_key: &str,
        snapshot_content: &str,
    ) -> MaterializedHotStateRow {
        live_row_with_scope(
            row_pk,
            schema_key,
            snapshot_content,
            "01920000-0000-7000-8000-0000000000a1",
            false,
            None,
        )
    }

    fn live_row_with_scope(
        row_pk: &str,
        schema_key: &str,
        snapshot_content: &str,
        branch_id: &str,
        untracked: bool,
        file_id: Option<String>,
    ) -> MaterializedHotStateRow {
        MaterializedHotStateRow {
            row_pk: RowPk::single(row_pk),
            schema_key: schema_key.to_string(),
            file_id,
            snapshot_content: Some(snapshot_content.into()),
            metadata: None,
            deleted: false,
            branch_id: branch_id.into(),
            change_id: Some(ChangeId::for_test_label(&format!("change-{row_pk}"))),
            commit_id: Some(CommitId::for_test_label(&format!("commit-{row_pk}"))),
            global: false,
            untracked,
            created_at: LixTimestamp::expect_parse(
                "filesystem read test created_at",
                "2026-04-23T00:00:00Z",
            ),
            updated_at: LixTimestamp::expect_parse(
                "filesystem read test updated_at",
                "2026-04-23T01:00:00Z",
            ),
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
            branch_id: "01920000-0000-7000-8000-0000000000a1".to_string(),
            global: false,
            untracked: false,
            file_id: None,
        }
    }
}
