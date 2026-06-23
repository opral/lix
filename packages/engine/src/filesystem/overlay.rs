use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::binary_cas::BlobHash;
use crate::entity_pk::EntityPk;
use crate::live_state::MaterializedLiveStateRow;
use crate::storage::{MountedFilesystem, MountedFilesystemListing};
use crate::{GLOBAL_BRANCH_ID, LixError};

use super::FilesystemIndex;
use super::keys::{DIRECTORY_DESCRIPTOR_SCHEMA_KEY, FILE_DESCRIPTOR_SCHEMA_KEY};
use super::planner::{FilesystemDescriptorKey, FilesystemRowContext};

const MOUNTED_FILE_ID_PREFIX: &str = "mounted:file:";
const MOUNTED_DIRECTORY_ID_PREFIX: &str = "mounted:dir:";
const MOUNTED_ROW_TIMESTAMP: &str = "1970-01-01T00:00:00Z";

#[derive(Debug, Clone, Default)]
pub(crate) struct MountedWorkspaceRows {
    pub(crate) rows: Vec<MaterializedLiveStateRow>,
    pub(crate) file_paths_by_key: BTreeMap<FilesystemDescriptorKey, String>,
}

pub(crate) async fn mounted_workspace_rows(
    active_branch_id: &str,
    mounted_filesystem: Option<Arc<dyn MountedFilesystem>>,
    owned_rows: &[MaterializedLiveStateRow],
) -> Result<MountedWorkspaceRows, LixError> {
    let Some(mounted_filesystem) = mounted_filesystem else {
        return Ok(MountedWorkspaceRows::default());
    };
    let listing = mounted_filesystem.list().await.map_err(|error| {
        LixError::new(
            LixError::CODE_STORAGE_ERROR,
            format!("mounted filesystem listing failed: {error}"),
        )
    })?;
    let owned = FilesystemIndex::from_live_rows(owned_rows.to_vec())?;
    rows_from_listing(active_branch_id, listing, &owned, owned_rows)
}

pub(crate) async fn mounted_workspace_rows_by_branch(
    mounted_filesystem: Option<Arc<dyn MountedFilesystem>>,
    branch_ids: &[String],
    rows: &[MaterializedLiveStateRow],
) -> Result<MountedWorkspaceRows, LixError> {
    let Some(mounted_filesystem) = mounted_filesystem else {
        return Ok(MountedWorkspaceRows::default());
    };
    let mut combined = MountedWorkspaceRows::default();
    for branch_id in branch_ids
        .iter()
        .filter(|branch_id| branch_id.as_str() != GLOBAL_BRANCH_ID)
    {
        let owned_rows_for_branch = rows
            .iter()
            .filter(|row| row.branch_id == *branch_id || row.global)
            .cloned()
            .collect::<Vec<_>>();
        let mounted_rows = mounted_workspace_rows(
            branch_id,
            Some(Arc::clone(&mounted_filesystem)),
            &owned_rows_for_branch,
        )
        .await?;
        combined.rows.extend(mounted_rows.rows);
        combined
            .file_paths_by_key
            .extend(mounted_rows.file_paths_by_key);
    }
    Ok(combined)
}

pub(crate) fn is_mounted_directory_id(id: &str) -> bool {
    id.starts_with(MOUNTED_DIRECTORY_ID_PREFIX)
}

pub(crate) fn is_mounted_file_id(id: &str) -> bool {
    id.starts_with(MOUNTED_FILE_ID_PREFIX)
}

fn rows_from_listing(
    active_branch_id: &str,
    listing: MountedFilesystemListing,
    owned: &FilesystemIndex,
    owned_rows: &[MaterializedLiveStateRow],
) -> Result<MountedWorkspaceRows, LixError> {
    let mut rows = Vec::new();
    let mut file_paths_by_key = BTreeMap::new();
    let owned_descriptor_keys = owned_descriptor_keys(owned_rows)?;
    let mounted_context = mounted_row_context(active_branch_id);
    let mut directories = listing
        .directories
        .into_iter()
        .filter(|path| path != "/")
        .collect::<BTreeSet<_>>();
    for file_path in &listing.files {
        for directory in ancestor_directories(file_path) {
            directories.insert(directory);
        }
    }

    let mut mounted_directories_by_path = BTreeMap::<String, MountedDirectory>::new();
    for directory_path in directories {
        if owned.contains_path(&directory_path)
            || owned.contains_path(directory_path.trim_end_matches('/'))
            || owned.is_shadowed_by_file_ancestor(&directory_path)
        {
            continue;
        }
        let id = mounted_directory_id(&directory_path);
        let mut context = mounted_context.clone();
        let parent_id = match parent_directory_path(&directory_path).filter(|path| path != "/") {
            Some(parent_path) => {
                if let Some(id) = owned.directory_id_for_path(&parent_path) {
                    context = owned
                        .directory_context_for_path(&parent_path)
                        .unwrap_or_else(|| mounted_context.clone());
                    Some(id.to_string())
                } else if let Some(parent) = mounted_directories_by_path.get(&parent_path) {
                    context = parent.context.clone();
                    Some(parent.id.clone())
                } else {
                    continue;
                }
            }
            None => None,
        };
        if owned_descriptor_keys.contains(&FilesystemDescriptorKey::from_context(&context, &id)) {
            continue;
        }
        let name = path_leaf_name(&directory_path);
        mounted_directories_by_path.insert(
            directory_path.clone(),
            MountedDirectory {
                id: id.clone(),
                context: context.clone(),
            },
        );
        rows.push(mounted_row(
            &context,
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
            &id,
            serde_json::json!({
                "id": id,
                "parent_id": parent_id,
                "name": name,
            })
            .to_string(),
        ));
    }

    for file_path in listing.files {
        if owned.contains_path(&file_path)
            || owned.contains_path(&format!("{file_path}/"))
            || owned.is_shadowed_by_file_ancestor(&file_path)
        {
            continue;
        }
        let id = mounted_file_id(&file_path);
        let mut context = mounted_context.clone();
        let directory_id = match parent_directory_path(&file_path).filter(|path| path != "/") {
            Some(parent_path) => {
                if let Some(id) = owned.directory_id_for_path(&parent_path) {
                    context = owned
                        .directory_context_for_path(&parent_path)
                        .unwrap_or_else(|| mounted_context.clone());
                    Some(id.to_string())
                } else if let Some(parent) = mounted_directories_by_path.get(&parent_path) {
                    context = parent.context.clone();
                    Some(parent.id.clone())
                } else {
                    continue;
                }
            }
            None => None,
        };
        let file_key = FilesystemDescriptorKey::from_context(&context, &id);
        if owned_descriptor_keys.contains(&file_key) {
            continue;
        }
        let name = path_leaf_name(&file_path);
        file_paths_by_key.insert(file_key, file_path);
        rows.push(mounted_row(
            &context,
            FILE_DESCRIPTOR_SCHEMA_KEY,
            &id,
            serde_json::json!({
                "id": id,
                "directory_id": directory_id,
                "name": name,
            })
            .to_string(),
        ));
    }

    Ok(MountedWorkspaceRows {
        rows,
        file_paths_by_key,
    })
}

#[derive(Debug, Clone)]
struct MountedDirectory {
    id: String,
    context: FilesystemRowContext,
}

fn owned_descriptor_keys(
    owned_rows: &[MaterializedLiveStateRow],
) -> Result<BTreeSet<FilesystemDescriptorKey>, LixError> {
    let mut keys = BTreeSet::new();
    for row in owned_rows {
        match row.schema_key.as_str() {
            FILE_DESCRIPTOR_SCHEMA_KEY | DIRECTORY_DESCRIPTOR_SCHEMA_KEY => {
                if row.deleted {
                    keys.insert(FilesystemDescriptorKey::from_live_row(
                        row,
                        row.entity_pk.as_single_string_owned()?,
                    ));
                    continue;
                }
                let Some(snapshot_content) = row.snapshot_content.as_deref() else {
                    continue;
                };
                let snapshot: DescriptorSnapshot =
                    serde_json::from_str(snapshot_content).map_err(|error| {
                        LixError::unknown(format!(
                            "invalid filesystem descriptor snapshot JSON: {error}"
                        ))
                    })?;
                keys.insert(FilesystemDescriptorKey::from_live_row(row, snapshot.id));
            }
            _ => {}
        }
    }
    Ok(keys)
}

#[derive(serde::Deserialize)]
struct DescriptorSnapshot {
    id: String,
}

fn mounted_row_context(active_branch_id: &str) -> FilesystemRowContext {
    FilesystemRowContext {
        branch_id: active_branch_id.to_string(),
        global: false,
        untracked: false,
        file_id: None,
        metadata: None,
    }
}

fn mounted_row(
    context: &FilesystemRowContext,
    schema_key: &str,
    id: &str,
    snapshot_content: String,
) -> MaterializedLiveStateRow {
    MaterializedLiveStateRow {
        entity_pk: EntityPk::single(id.to_string()),
        schema_key: schema_key.to_string(),
        file_id: context.file_id.clone(),
        snapshot_content: Some(snapshot_content),
        metadata: None,
        deleted: false,
        branch_id: context.branch_id.clone(),
        change_id: None,
        commit_id: None,
        global: context.global,
        untracked: context.untracked,
        created_at: MOUNTED_ROW_TIMESTAMP.to_string(),
        updated_at: MOUNTED_ROW_TIMESTAMP.to_string(),
    }
}

fn mounted_file_id(path: &str) -> String {
    format!("{MOUNTED_FILE_ID_PREFIX}{}", mounted_path_hash(path))
}

fn mounted_directory_id(path: &str) -> String {
    format!("{MOUNTED_DIRECTORY_ID_PREFIX}{}", mounted_path_hash(path))
}

fn mounted_path_hash(path: &str) -> String {
    BlobHash::from_content(path.as_bytes()).to_hex()
}

fn ancestor_directories(file_path: &str) -> Vec<String> {
    let mut directories = Vec::new();
    let mut current = parent_directory_path(file_path);
    while let Some(path) = current {
        if path == "/" {
            break;
        }
        current = parent_directory_path(&path);
        directories.push(path);
    }
    directories
}

fn parent_directory_path(path: &str) -> Option<String> {
    let trimmed = path.trim_end_matches('/');
    if trimmed == "/" || trimmed.is_empty() {
        return None;
    }
    let slash = trimmed.rfind('/')?;
    if slash == 0 {
        Some("/".to_string())
    } else {
        Some(format!("{}/", &trimmed[..slash]))
    }
}

fn path_leaf_name(path: &str) -> String {
    path.trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or("")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mounted_file_under_owned_directory_uses_owned_parent_id() {
        let owned_rows = vec![descriptor_row(
            "branch-a",
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
            "dir-src",
            r#"{"id":"dir-src","parent_id":null,"name":"src"}"#,
        )];
        let owned = FilesystemIndex::from_live_rows(owned_rows.clone()).unwrap();
        let mounted = rows_from_listing(
            "branch-a",
            MountedFilesystemListing {
                directories: BTreeSet::new(),
                files: BTreeSet::from(["/src/local.txt".to_string()]),
                unmanaged_paths: BTreeSet::new(),
            },
            &owned,
            &owned_rows,
        )
        .unwrap();

        let file_row = mounted
            .rows
            .iter()
            .find(|row| row.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY)
            .expect("mounted child file should be visible");
        let snapshot: serde_json::Value =
            serde_json::from_str(file_row.snapshot_content.as_deref().unwrap()).unwrap();
        assert_eq!(snapshot["directory_id"], "dir-src");
    }

    #[test]
    fn mounted_file_under_global_owned_directory_inherits_parent_scope() {
        let owned_rows = vec![descriptor_row_with_scope(
            "branch-a",
            true,
            false,
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
            "dir-src",
            r#"{"id":"dir-src","parent_id":null,"name":"src"}"#,
        )];
        let owned = FilesystemIndex::from_live_rows(owned_rows.clone()).unwrap();
        let mounted = rows_from_listing(
            "branch-a",
            MountedFilesystemListing {
                directories: BTreeSet::new(),
                files: BTreeSet::from(["/src/local.txt".to_string()]),
                unmanaged_paths: BTreeSet::new(),
            },
            &owned,
            &owned_rows,
        )
        .unwrap();

        let file_row = mounted
            .rows
            .iter()
            .find(|row| row.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY)
            .expect("mounted child file should be visible");
        let snapshot: serde_json::Value =
            serde_json::from_str(file_row.snapshot_content.as_deref().unwrap()).unwrap();
        assert_eq!(snapshot["directory_id"], "dir-src");
        assert!(file_row.global);
    }

    #[test]
    fn owned_file_shadows_mounted_descendants() {
        let owned_rows = vec![descriptor_row(
            "branch-a",
            FILE_DESCRIPTOR_SCHEMA_KEY,
            "file-foo",
            r#"{"id":"file-foo","directory_id":null,"name":"foo"}"#,
        )];
        let owned = FilesystemIndex::from_live_rows(owned_rows.clone()).unwrap();
        let mounted = rows_from_listing(
            "branch-a",
            MountedFilesystemListing {
                directories: BTreeSet::from(["/foo/".to_string()]),
                files: BTreeSet::from(["/foo/local.txt".to_string()]),
                unmanaged_paths: BTreeSet::new(),
            },
            &owned,
            &owned_rows,
        )
        .unwrap();

        assert!(
            mounted.rows.is_empty(),
            "owned file path should shadow lower-layer directory subtree"
        );
    }

    #[test]
    fn mounted_file_provenance_is_keyed_by_descriptor_scope() {
        let owned_rows = Vec::new();
        let owned = FilesystemIndex::from_live_rows(owned_rows.clone()).unwrap();
        let mounted = rows_from_listing(
            "branch-a",
            MountedFilesystemListing {
                directories: BTreeSet::new(),
                files: BTreeSet::from(["/note.md".to_string()]),
                unmanaged_paths: BTreeSet::new(),
            },
            &owned,
            &owned_rows,
        )
        .unwrap();

        let file_row = mounted
            .rows
            .iter()
            .find(|row| row.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY)
            .expect("mounted file should be visible");
        let snapshot: DescriptorSnapshot =
            serde_json::from_str(file_row.snapshot_content.as_deref().unwrap()).unwrap();
        let key =
            FilesystemDescriptorKey::from_context(&mounted_row_context("branch-a"), &snapshot.id);
        assert_eq!(
            mounted.file_paths_by_key.get(&key).map(String::as_str),
            Some("/note.md")
        );
    }

    fn descriptor_row(
        branch_id: &str,
        schema_key: &str,
        descriptor_id: &str,
        snapshot_content: &str,
    ) -> MaterializedLiveStateRow {
        descriptor_row_with_scope(
            branch_id,
            false,
            false,
            schema_key,
            descriptor_id,
            snapshot_content,
        )
    }

    fn descriptor_row_with_scope(
        branch_id: &str,
        global: bool,
        untracked: bool,
        schema_key: &str,
        descriptor_id: &str,
        snapshot_content: &str,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: EntityPk::single(descriptor_id.to_string()),
            schema_key: schema_key.to_string(),
            file_id: None,
            snapshot_content: Some(snapshot_content.to_string()),
            metadata: None,
            deleted: false,
            branch_id: branch_id.to_string(),
            change_id: None,
            commit_id: None,
            global,
            untracked,
            created_at: MOUNTED_ROW_TIMESTAMP.to_string(),
            updated_at: MOUNTED_ROW_TIMESTAMP.to_string(),
        }
    }
}
