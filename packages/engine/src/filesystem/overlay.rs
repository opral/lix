use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::backend::BackendMountedFilesystem;
use crate::binary_cas::BlobHash;
use crate::entity_pk::EntityPk;
use crate::live_state::MaterializedLiveStateRow;
use crate::{LixError, MountedFilesystemInventory};

use super::FilesystemIndex;
use super::keys::{DIRECTORY_DESCRIPTOR_SCHEMA_KEY, FILE_DESCRIPTOR_SCHEMA_KEY};

const MOUNTED_FILE_ID_PREFIX: &str = "mounted:file:";
const MOUNTED_DIRECTORY_ID_PREFIX: &str = "mounted:dir:";
const MOUNTED_ROW_TIMESTAMP: &str = "1970-01-01T00:00:00Z";

#[derive(Debug, Clone, Default)]
pub(crate) struct MountedWorkspaceRows {
    pub(crate) rows: Vec<MaterializedLiveStateRow>,
    pub(crate) file_paths_by_id: BTreeMap<String, String>,
}

pub(crate) async fn mounted_workspace_rows(
    active_branch_id: &str,
    mounted_filesystem: Option<Arc<dyn BackendMountedFilesystem>>,
    owned_rows: &[MaterializedLiveStateRow],
) -> Result<MountedWorkspaceRows, LixError> {
    let Some(mounted_filesystem) = mounted_filesystem else {
        return Ok(MountedWorkspaceRows::default());
    };
    let inventory = mounted_filesystem.inventory().await.map_err(|error| {
        LixError::new(
            LixError::CODE_STORAGE_ERROR,
            format!("mounted filesystem inventory failed: {error}"),
        )
    })?;
    let owned = FilesystemIndex::from_live_rows(owned_rows.to_vec())?;
    Ok(rows_from_inventory(active_branch_id, inventory, &owned))
}

fn rows_from_inventory(
    active_branch_id: &str,
    inventory: MountedFilesystemInventory,
    owned: &FilesystemIndex,
) -> MountedWorkspaceRows {
    let mut rows = Vec::new();
    let mut file_paths_by_id = BTreeMap::new();
    let mut directories = inventory
        .directories
        .into_iter()
        .filter(|path| path != "/")
        .collect::<BTreeSet<_>>();
    for file_path in &inventory.files {
        for directory in ancestor_directories(file_path) {
            directories.insert(directory);
        }
    }

    for directory_path in directories {
        if owned.contains_path(&directory_path)
            || owned.contains_path(directory_path.trim_end_matches('/'))
        {
            continue;
        }
        let id = mounted_directory_id(&directory_path);
        let parent_id = parent_directory_path(&directory_path)
            .filter(|path| path != "/")
            .map(|path| mounted_directory_id(&path));
        let name = path_leaf_name(&directory_path);
        rows.push(mounted_row(
            active_branch_id,
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

    for file_path in inventory.files {
        if owned.contains_path(&file_path) || owned.contains_path(&format!("{file_path}/")) {
            continue;
        }
        let id = mounted_file_id(&file_path);
        let directory_id = parent_directory_path(&file_path)
            .filter(|path| path != "/")
            .map(|path| mounted_directory_id(&path));
        let name = path_leaf_name(&file_path);
        file_paths_by_id.insert(id.clone(), file_path);
        rows.push(mounted_row(
            active_branch_id,
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

    MountedWorkspaceRows {
        rows,
        file_paths_by_id,
    }
}

fn mounted_row(
    branch_id: &str,
    schema_key: &str,
    id: &str,
    snapshot_content: String,
) -> MaterializedLiveStateRow {
    MaterializedLiveStateRow {
        entity_pk: EntityPk::single(id.to_string()),
        schema_key: schema_key.to_string(),
        file_id: None,
        snapshot_content: Some(snapshot_content),
        metadata: None,
        deleted: false,
        branch_id: branch_id.to_string(),
        change_id: None,
        commit_id: None,
        global: false,
        untracked: false,
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
