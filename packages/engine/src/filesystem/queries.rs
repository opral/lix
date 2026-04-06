use crate::contracts::traits::{
    PendingFilesystemFileView, PendingSemanticRow, PendingSemanticStorage, PendingView,
};
use crate::filesystem::live_projection::{
    build_filesystem_directory_projection_sql, build_filesystem_file_projection_sql,
    FilesystemProjectionScope,
};
use crate::filesystem::path::{compose_directory_path, NormalizedDirectoryPath, ParsedFilePath};
use crate::live_schema_access::tracked_relation_name;
use crate::text::escape_sql_string;
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackend, SqlDialect, Value};
use serde_json::Value as JsonValue;
use std::collections::BTreeSet;

const FILESYSTEM_DESCRIPTOR_FILE_ID: &str = "lix";
const FILESYSTEM_FILE_SCHEMA_KEY: &str = "lix_file_descriptor";
const FILESYSTEM_DIRECTORY_SCHEMA_KEY: &str = "lix_directory_descriptor";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FilesystemQueryError {
    pub(crate) message: String,
}

#[derive(Debug, Clone)]
pub(crate) struct DirectoryFilesystemRow {
    pub(crate) id: String,
    pub(crate) parent_id: Option<String>,
    pub(crate) name: String,
    pub(crate) path: String,
    pub(crate) hidden: bool,
    pub(crate) version_id: String,
    pub(crate) untracked: bool,
    pub(crate) metadata: Option<String>,
    pub(crate) change_id: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct FileFilesystemRow {
    pub(crate) id: String,
    pub(crate) directory_id: Option<String>,
    pub(crate) name: String,
    pub(crate) extension: Option<String>,
    pub(crate) path: String,
    pub(crate) hidden: bool,
    pub(crate) untracked: bool,
    pub(crate) metadata: Option<String>,
    pub(crate) change_id: Option<String>,
}

pub(crate) async fn lookup_directory_id_by_path(
    backend: &dyn LixBackend,
    version_id: &str,
    path: &NormalizedDirectoryPath,
    scope: FilesystemProjectionScope,
) -> Result<Option<String>, FilesystemQueryError> {
    Ok(load_directory_row_by_path(backend, version_id, path, scope)
        .await?
        .map(|row| row.id))
}

pub(crate) async fn lookup_file_id_by_path(
    backend: &dyn LixBackend,
    version_id: &str,
    path: &ParsedFilePath,
    scope: FilesystemProjectionScope,
) -> Result<Option<String>, FilesystemQueryError> {
    Ok(load_file_row_by_path(backend, version_id, path, scope)
        .await?
        .map(|row| row.id))
}

pub(crate) async fn lookup_directory_path_by_id(
    backend: &dyn LixBackend,
    version_id: &str,
    directory_id: &str,
    scope: FilesystemProjectionScope,
) -> Result<Option<String>, FilesystemQueryError> {
    Ok(
        load_directory_row_by_id(backend, version_id, directory_id, scope)
            .await?
            .map(|row| row.path),
    )
}

pub(crate) async fn ensure_no_file_at_directory_path(
    backend: &dyn LixBackend,
    version_id: &str,
    directory_path: &NormalizedDirectoryPath,
    lookup_scope: FilesystemProjectionScope,
) -> Result<(), FilesystemQueryError> {
    let file_path = ParsedFilePath::from_normalized_path(
        directory_path.as_str().trim_end_matches('/').to_string(),
    )
    .map_err(filesystem_query_backend_error)?;
    if lookup_file_id_by_path(backend, version_id, &file_path, lookup_scope)
        .await?
        .is_none()
    {
        return Ok(());
    }
    Err(FilesystemQueryError {
        message: format!(
            "Directory path collides with existing file path: {}",
            file_path.normalized_path.as_str()
        ),
    })
}

pub(crate) async fn ensure_no_directory_at_file_path(
    backend: &dyn LixBackend,
    version_id: &str,
    file_path: &ParsedFilePath,
    lookup_scope: FilesystemProjectionScope,
) -> Result<(), FilesystemQueryError> {
    let directory_path = NormalizedDirectoryPath::from_normalized(format!(
        "{}/",
        file_path.normalized_path.as_str().trim_end_matches('/')
    ));
    if lookup_directory_id_by_path(backend, version_id, &directory_path, lookup_scope)
        .await?
        .is_none()
    {
        return Ok(());
    }
    Err(FilesystemQueryError {
        message: format!("File path collides with existing directory path: {directory_path}"),
    })
}

pub(crate) async fn load_directory_row_by_id(
    backend: &dyn LixBackend,
    version_id: &str,
    directory_id: &str,
    scope: FilesystemProjectionScope,
) -> Result<Option<DirectoryFilesystemRow>, FilesystemQueryError> {
    let Some(descriptor) =
        load_directory_descriptor_by_id(backend, version_id, directory_id, scope).await?
    else {
        return Ok(None);
    };
    let path = build_directory_path_from_descriptor(
        backend,
        version_id,
        &descriptor.id,
        descriptor.parent_id.as_deref(),
        &descriptor.name,
        scope,
    )
    .await?;
    Ok(Some(DirectoryFilesystemRow {
        id: descriptor.id,
        parent_id: descriptor.parent_id,
        name: descriptor.name,
        path,
        hidden: descriptor.hidden,
        version_id: version_id.to_string(),
        untracked: descriptor.untracked,
        metadata: descriptor.metadata,
        change_id: descriptor.change_id,
    }))
}

pub(crate) async fn load_directory_row_by_path(
    backend: &dyn LixBackend,
    version_id: &str,
    path: &NormalizedDirectoryPath,
    scope: FilesystemProjectionScope,
) -> Result<Option<DirectoryFilesystemRow>, FilesystemQueryError> {
    let mut current_parent_id = None;
    let mut current_path = "/".to_string();
    let mut current_row = None;

    for segment in path
        .as_str()
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
    {
        let Some(descriptor) = load_directory_descriptor_by_parent_and_name(
            backend,
            version_id,
            current_parent_id.as_deref(),
            segment,
            scope,
        )
        .await?
        else {
            return Ok(None);
        };
        current_path = compose_directory_path(&current_path, &descriptor.name)
            .map_err(filesystem_query_backend_error)?;
        current_parent_id = Some(descriptor.id.clone());
        current_row = Some(DirectoryFilesystemRow {
            id: descriptor.id,
            parent_id: descriptor.parent_id,
            name: descriptor.name,
            path: current_path.clone(),
            hidden: descriptor.hidden,
            version_id: version_id.to_string(),
            untracked: descriptor.untracked,
            metadata: descriptor.metadata,
            change_id: descriptor.change_id,
        });
    }

    Ok(current_row)
}

pub(crate) async fn load_file_row_by_path(
    backend: &dyn LixBackend,
    version_id: &str,
    path: &ParsedFilePath,
    scope: FilesystemProjectionScope,
) -> Result<Option<FileFilesystemRow>, FilesystemQueryError> {
    let directory_id = match path.directory_path.as_ref() {
        Some(directory_path) => {
            match lookup_directory_id_by_path(backend, version_id, directory_path, scope).await? {
                Some(directory_id) => Some(directory_id),
                None => return Ok(None),
            }
        }
        None => None,
    };
    let Some(descriptor) = load_file_descriptor_by_path_components(
        backend,
        version_id,
        directory_id.as_deref(),
        &path.name,
        path.extension.as_deref(),
        scope,
    )
    .await?
    else {
        return Ok(None);
    };

    Ok(Some(FileFilesystemRow {
        id: descriptor.id,
        directory_id: descriptor.directory_id,
        name: descriptor.name,
        extension: descriptor.extension,
        path: path.normalized_path.as_str().to_string(),
        hidden: descriptor.hidden,
        untracked: descriptor.untracked,
        metadata: descriptor.metadata,
        change_id: descriptor.change_id,
    }))
}

pub(crate) async fn load_file_row_by_id(
    backend: &dyn LixBackend,
    version_id: &str,
    file_id: &str,
    scope: FilesystemProjectionScope,
) -> Result<Option<FileFilesystemRow>, FilesystemQueryError> {
    let Some(descriptor) = load_file_descriptor_by_id(backend, version_id, file_id, scope).await?
    else {
        return Ok(None);
    };
    let path = match descriptor.directory_id.as_deref() {
        Some(directory_id) => {
            let Some(directory_path) =
                lookup_directory_path_by_id(backend, version_id, directory_id, scope).await?
            else {
                return Ok(None);
            };
            compose_file_path(
                &directory_path,
                &descriptor.name,
                descriptor.extension.as_deref(),
            )
        }
        None => compose_file_path("/", &descriptor.name, descriptor.extension.as_deref()),
    };

    Ok(Some(FileFilesystemRow {
        id: descriptor.id,
        directory_id: descriptor.directory_id,
        name: descriptor.name,
        extension: descriptor.extension,
        path,
        hidden: descriptor.hidden,
        untracked: descriptor.untracked,
        metadata: descriptor.metadata,
        change_id: descriptor.change_id,
    }))
}

pub(crate) async fn load_file_row_by_id_without_path(
    backend: &dyn LixBackend,
    version_id: &str,
    file_id: &str,
    scope: FilesystemProjectionScope,
) -> Result<Option<FileFilesystemRow>, FilesystemQueryError> {
    let Some(descriptor) = load_file_descriptor_by_id(backend, version_id, file_id, scope).await?
    else {
        return Ok(None);
    };

    Ok(Some(FileFilesystemRow {
        id: descriptor.id,
        directory_id: descriptor.directory_id,
        name: descriptor.name,
        extension: descriptor.extension,
        path: String::new(),
        hidden: descriptor.hidden,
        untracked: descriptor.untracked,
        metadata: descriptor.metadata,
        change_id: descriptor.change_id,
    }))
}

pub(crate) async fn load_directory_row_by_id_with_pending_transaction_view(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    version_id: &str,
    directory_id: &str,
    scope: FilesystemProjectionScope,
) -> Result<Option<DirectoryFilesystemRow>, FilesystemQueryError> {
    if let Some(row) = pending_directory_row_by_id(
        backend,
        pending_transaction_view,
        version_id,
        directory_id,
        scope,
    )
    .await?
    {
        return Ok(Some(row));
    }

    let Some(base_row) = load_directory_row_by_id(backend, version_id, directory_id, scope).await?
    else {
        return Ok(None);
    };

    if pending_directory_row_is_hidden(pending_transaction_view, version_id, &base_row.id) {
        return Ok(None);
    }

    Ok(Some(base_row))
}

pub(crate) async fn load_directory_row_by_path_with_pending_transaction_view(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    version_id: &str,
    path: &NormalizedDirectoryPath,
    scope: FilesystemProjectionScope,
) -> Result<Option<DirectoryFilesystemRow>, FilesystemQueryError> {
    if let Some(row) =
        pending_directory_row_by_path(backend, pending_transaction_view, version_id, path, scope)
            .await?
    {
        return Ok(Some(row));
    }

    let Some(base_row) = load_directory_row_by_path(backend, version_id, path, scope).await? else {
        return Ok(None);
    };

    if pending_directory_row_is_hidden(pending_transaction_view, version_id, &base_row.id) {
        return Ok(None);
    }

    Ok(Some(base_row))
}

pub(crate) async fn load_file_row_by_path_with_pending_transaction_view(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    version_id: &str,
    path: &ParsedFilePath,
    scope: FilesystemProjectionScope,
) -> Result<Option<FileFilesystemRow>, FilesystemQueryError> {
    if let Some(row) =
        pending_file_row_by_path(backend, pending_transaction_view, version_id, path, scope).await?
    {
        return Ok(Some(row));
    }

    let Some(base_row) = load_file_row_by_path(backend, version_id, path, scope).await? else {
        return Ok(None);
    };

    if pending_file_row_is_hidden(pending_transaction_view, version_id, &base_row.id) {
        return Ok(None);
    }

    Ok(Some(base_row))
}

pub(crate) async fn load_file_row_by_id_with_pending_transaction_view(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    version_id: &str,
    file_id: &str,
    scope: FilesystemProjectionScope,
) -> Result<Option<FileFilesystemRow>, FilesystemQueryError> {
    if let Some(row) = pending_file_row_by_id(
        backend,
        pending_transaction_view,
        version_id,
        file_id,
        scope,
    )
    .await?
    {
        return Ok(Some(row));
    }

    let Some(mut base_row) = load_file_row_by_id(backend, version_id, file_id, scope).await? else {
        return Ok(None);
    };

    let Some(pending) = pending_transaction_view
        .into_iter()
        .flat_map(PendingView::visible_files)
        .find(|pending| pending.version_id == version_id && pending.file_id == file_id)
    else {
        return Ok(Some(base_row));
    };
    if pending.deleted {
        return Ok(None);
    }
    if pending.descriptor.is_none() {
        base_row.metadata = pending.metadata_patch.apply(base_row.metadata.take());
    }
    Ok(Some(base_row))
}

pub(crate) async fn load_file_row_by_id_without_path_with_pending_transaction_view(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    version_id: &str,
    file_id: &str,
    scope: FilesystemProjectionScope,
) -> Result<Option<FileFilesystemRow>, FilesystemQueryError> {
    if let Some(mut row) = load_file_row_by_id_with_pending_transaction_view(
        backend,
        pending_transaction_view,
        version_id,
        file_id,
        scope,
    )
    .await?
    {
        row.path.clear();
        return Ok(Some(row));
    }
    Ok(None)
}

pub(crate) async fn lookup_directory_id_by_path_with_pending_transaction_view(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    version_id: &str,
    path: &NormalizedDirectoryPath,
    scope: FilesystemProjectionScope,
) -> Result<Option<String>, FilesystemQueryError> {
    Ok(load_directory_row_by_path_with_pending_transaction_view(
        backend,
        pending_transaction_view,
        version_id,
        path,
        scope,
    )
    .await?
    .map(|row| row.id))
}

pub(crate) async fn lookup_file_id_by_path_with_pending_transaction_view(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    version_id: &str,
    path: &ParsedFilePath,
    scope: FilesystemProjectionScope,
) -> Result<Option<String>, FilesystemQueryError> {
    Ok(load_file_row_by_path_with_pending_transaction_view(
        backend,
        pending_transaction_view,
        version_id,
        path,
        scope,
    )
    .await?
    .map(|row| row.id))
}

pub(crate) async fn lookup_directory_path_by_id_with_pending_transaction_view(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    version_id: &str,
    directory_id: &str,
    scope: FilesystemProjectionScope,
) -> Result<Option<String>, FilesystemQueryError> {
    Ok(load_directory_row_by_id_with_pending_transaction_view(
        backend,
        pending_transaction_view,
        version_id,
        directory_id,
        scope,
    )
    .await?
    .map(|row| row.path))
}

pub(crate) async fn load_directory_rows_under_path(
    backend: &dyn LixBackend,
    version_id: &str,
    root_path: &str,
) -> Result<Vec<DirectoryFilesystemRow>, FilesystemQueryError> {
    let prefix_length = root_path.chars().count();
    let sql = format!(
        "SELECT id, parent_id, name, path, hidden, lixcol_version_id, lixcol_untracked, lixcol_metadata, lixcol_change_id \
         FROM ({projection_sql}) directories \
         WHERE lixcol_version_id = '{version_id}' \
           AND substr(path, 1, {prefix_length}) = '{root_path}' \
         ORDER BY path ASC, id ASC",
        projection_sql = build_filesystem_directory_projection_sql(
            FilesystemProjectionScope::ExplicitVersion,
            None,
            backend.dialect(),
        )
        .map_err(filesystem_query_backend_error)?,
        version_id = escape_sql_string(version_id),
        prefix_length = prefix_length,
        root_path = escape_sql_string(root_path),
    );
    load_directory_rows_from_sql(backend, &sql).await
}

pub(crate) async fn load_file_rows_under_path(
    backend: &dyn LixBackend,
    version_id: &str,
    root_path: &str,
) -> Result<Vec<FileFilesystemRow>, FilesystemQueryError> {
    let prefix_length = root_path.chars().count();
    let sql = format!(
        "SELECT id, directory_id, name, extension, path, hidden, lixcol_version_id, lixcol_untracked, metadata, lixcol_change_id \
         FROM ({projection_sql}) files \
         WHERE lixcol_version_id = '{version_id}' \
           AND substr(path, 1, {prefix_length}) = '{root_path}' \
         ORDER BY path ASC, id ASC",
        projection_sql = build_filesystem_file_projection_sql(
            FilesystemProjectionScope::ExplicitVersion,
            None,
            false,
            backend.dialect(),
        )
        .map_err(filesystem_query_backend_error)?,
        version_id = escape_sql_string(version_id),
        prefix_length = prefix_length,
        root_path = escape_sql_string(root_path),
    );
    load_file_rows_from_sql(backend, &sql).await
}

async fn pending_directory_row_by_id(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    version_id: &str,
    directory_id: &str,
    scope: FilesystemProjectionScope,
) -> Result<Option<DirectoryFilesystemRow>, FilesystemQueryError> {
    let Some(pending) = pending_transaction_view
        .into_iter()
        .flat_map(|view| {
            view.visible_directory_rows(
                PendingSemanticStorage::Tracked,
                FILESYSTEM_DIRECTORY_SCHEMA_KEY,
            )
        })
        .find(|row| row.version_id == version_id && row.entity_id == directory_id)
    else {
        return Ok(None);
    };
    if pending.tombstone {
        return Ok(None);
    }

    build_pending_directory_row(backend, pending_transaction_view, &pending, scope).await
}

async fn pending_directory_row_by_path(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    version_id: &str,
    path: &NormalizedDirectoryPath,
    scope: FilesystemProjectionScope,
) -> Result<Option<DirectoryFilesystemRow>, FilesystemQueryError> {
    for pending in pending_transaction_view.into_iter().flat_map(|view| {
        view.visible_directory_rows(
            PendingSemanticStorage::Tracked,
            FILESYSTEM_DIRECTORY_SCHEMA_KEY,
        )
    }) {
        if pending.version_id != version_id || pending.tombstone {
            continue;
        }
        let Some(row) =
            build_pending_directory_row(backend, pending_transaction_view, &pending, scope).await?
        else {
            continue;
        };
        if row.path == path.as_str() {
            return Ok(Some(row));
        }
    }
    Ok(None)
}

async fn pending_file_row_by_id(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    version_id: &str,
    file_id: &str,
    scope: FilesystemProjectionScope,
) -> Result<Option<FileFilesystemRow>, FilesystemQueryError> {
    let Some(pending) = pending_transaction_view
        .into_iter()
        .flat_map(PendingView::visible_files)
        .find(|pending| pending.version_id == version_id && pending.file_id == file_id)
    else {
        return Ok(None);
    };
    if pending.deleted {
        return Ok(None);
    }
    build_pending_file_row(backend, pending_transaction_view, &pending, scope).await
}

async fn pending_file_row_by_path(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    version_id: &str,
    path: &ParsedFilePath,
    scope: FilesystemProjectionScope,
) -> Result<Option<FileFilesystemRow>, FilesystemQueryError> {
    for pending in pending_transaction_view
        .into_iter()
        .flat_map(PendingView::visible_files)
    {
        if pending.version_id != version_id || pending.deleted {
            continue;
        }
        let Some(row) =
            build_pending_file_row(backend, pending_transaction_view, &pending, scope).await?
        else {
            continue;
        };
        if row.path == path.normalized_path.as_str() {
            return Ok(Some(row));
        }
    }
    Ok(None)
}

fn pending_directory_row_is_hidden(
    pending_transaction_view: Option<&dyn PendingView>,
    version_id: &str,
    directory_id: &str,
) -> bool {
    pending_transaction_view
        .into_iter()
        .flat_map(|view| {
            view.visible_directory_rows(
                PendingSemanticStorage::Tracked,
                FILESYSTEM_DIRECTORY_SCHEMA_KEY,
            )
        })
        .any(|row| row.version_id == version_id && row.entity_id == directory_id && row.tombstone)
}

fn pending_file_row_is_hidden(
    pending_transaction_view: Option<&dyn PendingView>,
    version_id: &str,
    file_id: &str,
) -> bool {
    pending_transaction_view
        .into_iter()
        .flat_map(PendingView::visible_files)
        .any(|pending| {
            pending.version_id == version_id
                && pending.file_id == file_id
                && (pending.deleted || pending.descriptor.is_some())
        })
}

async fn build_pending_directory_row(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    row: &PendingSemanticRow,
    scope: FilesystemProjectionScope,
) -> Result<Option<DirectoryFilesystemRow>, FilesystemQueryError> {
    let Some(snapshot_content) = row.snapshot_content.as_deref() else {
        return Ok(None);
    };
    let snapshot: JsonValue =
        serde_json::from_str(snapshot_content).map_err(|error| FilesystemQueryError {
            message: format!("filesystem pending directory snapshot invalid JSON: {error}"),
        })?;
    let parent_id = snapshot
        .get("parent_id")
        .and_then(|value| value.as_str())
        .map(str::to_string);
    let name = snapshot
        .get("name")
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_string();
    let hidden = snapshot
        .get("hidden")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let path = match parent_id.as_deref() {
        Some(parent_id) => {
            let Some(parent_path) =
                Box::pin(lookup_directory_path_by_id_with_pending_transaction_view(
                    backend,
                    pending_transaction_view,
                    &row.version_id,
                    parent_id,
                    scope,
                ))
                .await?
            else {
                return Ok(None);
            };
            compose_directory_path(&parent_path, &name).map_err(filesystem_query_backend_error)?
        }
        None => compose_directory_path("/", &name).map_err(filesystem_query_backend_error)?,
    };

    Ok(Some(DirectoryFilesystemRow {
        id: row.entity_id.clone(),
        parent_id,
        name,
        path,
        hidden,
        version_id: row.version_id.clone(),
        untracked: false,
        metadata: row.metadata.clone(),
        change_id: None,
    }))
}

async fn build_pending_file_row(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    row: &PendingFilesystemFileView,
    scope: FilesystemProjectionScope,
) -> Result<Option<FileFilesystemRow>, FilesystemQueryError> {
    let Some(descriptor) = row.descriptor.as_ref() else {
        return Ok(None);
    };
    let path = match descriptor.directory_id.as_str() {
        "" => compose_file_path("/", &descriptor.name, descriptor.extension.as_deref()),
        directory_id => {
            let Some(parent_path) = lookup_directory_path_by_id_with_pending_transaction_view(
                backend,
                pending_transaction_view,
                &row.version_id,
                directory_id,
                scope,
            )
            .await?
            else {
                return Ok(None);
            };
            compose_file_path(
                &parent_path,
                &descriptor.name,
                descriptor.extension.as_deref(),
            )
        }
    };

    Ok(Some(FileFilesystemRow {
        id: row.file_id.clone(),
        directory_id: (!descriptor.directory_id.is_empty())
            .then(|| descriptor.directory_id.clone()),
        name: descriptor.name.clone(),
        extension: descriptor.extension.clone(),
        path,
        hidden: descriptor.hidden,
        untracked: row.untracked,
        metadata: descriptor.metadata.clone(),
        change_id: None,
    }))
}

async fn load_directory_rows_from_sql(
    backend: &dyn LixBackend,
    sql: &str,
) -> Result<Vec<DirectoryFilesystemRow>, FilesystemQueryError> {
    let lowered_sql = lower_internal_sql_for_backend(backend, sql)?;
    let result = backend
        .execute(&lowered_sql, &[])
        .await
        .map_err(filesystem_query_backend_error)?;
    result
        .rows
        .iter()
        .map(|row| {
            Ok(DirectoryFilesystemRow {
                id: required_text_value(row, "id")?,
                parent_id: optional_text_value(row.get(1)),
                name: required_text_value_index(row, 2, "name")?,
                path: required_text_value_index(row, 3, "path")?,
                hidden: row.get(4).and_then(value_as_bool).unwrap_or(false),
                version_id: required_text_value_index(row, 5, "lixcol_version_id")?,
                untracked: row.get(6).and_then(value_as_bool).unwrap_or(false),
                metadata: row.get(7).and_then(text_from_value),
                change_id: row.get(8).and_then(text_from_value),
            })
        })
        .collect()
}

async fn load_file_rows_from_sql(
    backend: &dyn LixBackend,
    sql: &str,
) -> Result<Vec<FileFilesystemRow>, FilesystemQueryError> {
    let lowered_sql = lower_internal_sql_for_backend(backend, sql)?;
    let result = backend
        .execute(&lowered_sql, &[])
        .await
        .map_err(filesystem_query_backend_error)?;
    result
        .rows
        .iter()
        .map(|row| {
            Ok(FileFilesystemRow {
                id: required_text_value(row, "id")?,
                directory_id: optional_text_value(row.get(1)),
                name: required_text_value_index(row, 2, "name")?,
                extension: optional_text_value(row.get(3)),
                path: required_text_value_index(row, 4, "path")?,
                hidden: row.get(5).and_then(value_as_bool).unwrap_or(false),
                untracked: row.get(7).and_then(value_as_bool).unwrap_or(false),
                metadata: row.get(8).and_then(text_from_value),
                change_id: row.get(9).and_then(text_from_value),
            })
        })
        .collect()
}

fn lower_internal_sql_for_backend(
    _backend: &dyn LixBackend,
    sql: &str,
) -> Result<String, FilesystemQueryError> {
    Ok(sql.to_string())
}

#[derive(Debug, Clone)]
pub(crate) struct EffectiveDescriptorRow {
    pub(crate) id: String,
    pub(crate) parent_id: Option<String>,
    pub(crate) directory_id: Option<String>,
    pub(crate) name: String,
    pub(crate) extension: Option<String>,
    pub(crate) hidden: bool,
    pub(crate) untracked: bool,
    pub(crate) metadata: Option<String>,
    pub(crate) change_id: Option<String>,
}

pub(crate) async fn load_directory_descriptors_by_parent_name_pairs(
    backend: &dyn LixBackend,
    version_id: &str,
    pairs: &BTreeSet<(Option<String>, String)>,
    scope: FilesystemProjectionScope,
) -> Result<Vec<EffectiveDescriptorRow>, FilesystemQueryError> {
    if pairs.is_empty() {
        return Ok(Vec::new());
    }
    let mut rows = Vec::with_capacity(pairs.len());
    for (parent_id, name) in pairs {
        if let Some(row) = load_directory_descriptor_by_parent_and_name(
            backend,
            version_id,
            parent_id.as_deref(),
            name,
            scope,
        )
        .await?
        {
            rows.push(row);
        }
    }
    Ok(rows)
}

pub(crate) async fn load_file_descriptors_by_directory_name_extension_triplets(
    backend: &dyn LixBackend,
    version_id: &str,
    triplets: &BTreeSet<(Option<String>, String, Option<String>)>,
    scope: FilesystemProjectionScope,
) -> Result<Vec<EffectiveDescriptorRow>, FilesystemQueryError> {
    if triplets.is_empty() {
        return Ok(Vec::new());
    }
    let mut rows = Vec::with_capacity(triplets.len());
    for (directory_id, name, extension) in triplets {
        if let Some(row) = load_file_descriptor_by_path_components(
            backend,
            version_id,
            directory_id.as_deref(),
            name,
            extension.as_deref(),
            scope,
        )
        .await?
        {
            rows.push(row);
        }
    }
    Ok(rows)
}

async fn load_directory_descriptor_by_id(
    backend: &dyn LixBackend,
    version_id: &str,
    directory_id: &str,
    scope: FilesystemProjectionScope,
) -> Result<Option<EffectiveDescriptorRow>, FilesystemQueryError> {
    load_scoped_descriptor_row(
        backend,
        &tracked_relation_name(FILESYSTEM_DIRECTORY_SCHEMA_KEY),
        FILESYSTEM_DIRECTORY_SCHEMA_KEY,
        &format!("entity_id = '{}'", escape_sql_string(directory_id)),
        &format!("entity_id = '{}'", escape_sql_string(directory_id)),
        version_id,
        scope,
    )
    .await
}

async fn load_directory_descriptor_by_parent_and_name(
    backend: &dyn LixBackend,
    version_id: &str,
    parent_id: Option<&str>,
    name: &str,
    scope: FilesystemProjectionScope,
) -> Result<Option<EffectiveDescriptorRow>, FilesystemQueryError> {
    let parent_predicate_tracked = match parent_id {
        Some(parent_id) => format!("parent_id = '{}'", escape_sql_string(parent_id)),
        None => "parent_id IS NULL".to_string(),
    };
    let parent_predicate_untracked = match parent_id {
        Some(parent_id) => format!("parent_id = '{}'", escape_sql_string(parent_id)),
        None => "parent_id IS NULL".to_string(),
    };
    let name_predicate_tracked = format!("name = '{}'", escape_sql_string(name));
    let name_predicate_untracked = format!("name = '{}'", escape_sql_string(name));
    load_scoped_descriptor_row(
        backend,
        &tracked_relation_name(FILESYSTEM_DIRECTORY_SCHEMA_KEY),
        FILESYSTEM_DIRECTORY_SCHEMA_KEY,
        &format!("{parent_predicate_tracked} AND {name_predicate_tracked}"),
        &format!("{parent_predicate_untracked} AND {name_predicate_untracked}"),
        version_id,
        scope,
    )
    .await
}

async fn load_file_descriptor_by_id(
    backend: &dyn LixBackend,
    version_id: &str,
    file_id: &str,
    scope: FilesystemProjectionScope,
) -> Result<Option<EffectiveDescriptorRow>, FilesystemQueryError> {
    load_scoped_descriptor_row(
        backend,
        &tracked_relation_name(FILESYSTEM_FILE_SCHEMA_KEY),
        FILESYSTEM_FILE_SCHEMA_KEY,
        &format!("entity_id = '{}'", escape_sql_string(file_id)),
        &format!("entity_id = '{}'", escape_sql_string(file_id)),
        version_id,
        scope,
    )
    .await
}

async fn load_file_descriptor_by_path_components(
    backend: &dyn LixBackend,
    version_id: &str,
    directory_id: Option<&str>,
    name: &str,
    extension: Option<&str>,
    scope: FilesystemProjectionScope,
) -> Result<Option<EffectiveDescriptorRow>, FilesystemQueryError> {
    let directory_predicate_tracked = match directory_id {
        Some(directory_id) => format!("directory_id = '{}'", escape_sql_string(directory_id)),
        None => "directory_id IS NULL".to_string(),
    };
    let directory_predicate_untracked = match directory_id {
        Some(directory_id) => format!("directory_id = '{}'", escape_sql_string(directory_id)),
        None => "directory_id IS NULL".to_string(),
    };
    let name_predicate_tracked = format!("name = '{}'", escape_sql_string(name));
    let name_predicate_untracked = format!("name = '{}'", escape_sql_string(name));
    let extension_predicate_tracked = match extension {
        Some(extension) => format!("extension = '{}'", escape_sql_string(extension)),
        None => "(extension IS NULL OR extension = '')".to_string(),
    };
    let extension_predicate_untracked = match extension {
        Some(extension) => format!("extension = '{}'", escape_sql_string(extension)),
        None => "(extension IS NULL OR extension = '')".to_string(),
    };
    load_scoped_descriptor_row(
        backend,
        &tracked_relation_name(FILESYSTEM_FILE_SCHEMA_KEY),
        FILESYSTEM_FILE_SCHEMA_KEY,
        &format!(
            "{directory_predicate_tracked} AND {name_predicate_tracked} AND {extension_predicate_tracked}"
        ),
        &format!(
            "{directory_predicate_untracked} AND {name_predicate_untracked} AND {extension_predicate_untracked}"
        ),
        version_id,
        scope,
    )
    .await
}

async fn load_scoped_descriptor_row(
    backend: &dyn LixBackend,
    tracked_table: &str,
    schema_key: &str,
    tracked_base_predicate: &str,
    untracked_base_predicate: &str,
    version_id: &str,
    _scope: FilesystemProjectionScope,
) -> Result<Option<EffectiveDescriptorRow>, FilesystemQueryError> {
    if version_id == GLOBAL_VERSION_ID {
        return load_visible_descriptor_row_for_version(
            backend,
            tracked_table,
            schema_key,
            tracked_base_predicate,
            untracked_base_predicate,
            version_id,
        )
        .await;
    }

    if let Some(local_row) = load_visible_descriptor_row_for_version(
        backend,
        tracked_table,
        schema_key,
        tracked_base_predicate,
        untracked_base_predicate,
        version_id,
    )
    .await?
    {
        return Ok(Some(local_row));
    }

    let Some(global_row) = load_visible_descriptor_row_for_version(
        backend,
        tracked_table,
        schema_key,
        tracked_base_predicate,
        untracked_base_predicate,
        GLOBAL_VERSION_ID,
    )
    .await?
    else {
        return Ok(None);
    };

    if version_has_tombstone_for_entity(
        backend,
        tracked_table,
        schema_key,
        version_id,
        &global_row.id,
    )
    .await?
    {
        return Ok(None);
    }

    Ok(Some(global_row))
}

async fn load_visible_descriptor_row_for_version(
    backend: &dyn LixBackend,
    tracked_table: &str,
    schema_key: &str,
    tracked_base_predicate: &str,
    untracked_base_predicate: &str,
    version_id: &str,
) -> Result<Option<EffectiveDescriptorRow>, FilesystemQueryError> {
    let sql = visible_descriptor_sql(
        backend.dialect(),
        tracked_table,
        schema_key,
        tracked_base_predicate,
        untracked_base_predicate,
        version_id,
    );
    load_effective_descriptor_row(backend, &sql).await
}

async fn version_has_tombstone_for_entity(
    backend: &dyn LixBackend,
    tracked_table: &str,
    schema_key: &str,
    version_id: &str,
    entity_id: &str,
) -> Result<bool, FilesystemQueryError> {
    let sql = version_shadow_sql(tracked_table, schema_key, version_id, entity_id);
    let result = backend
        .execute(&sql, &[])
        .await
        .map_err(filesystem_query_backend_error)?;
    let Some(row) = result.rows.first() else {
        return Ok(false);
    };
    Ok(row.get(4).and_then(value_as_bool).unwrap_or(false))
}

fn visible_descriptor_sql(
    _dialect: SqlDialect,
    tracked_table: &str,
    schema_key: &str,
    tracked_base_predicate: &str,
    untracked_base_predicate: &str,
    version_id: &str,
) -> String {
    let tracked_base = format!(
        "file_id = '{file_id}' AND {tracked_base_predicate}",
        file_id = escape_sql_string(FILESYSTEM_DESCRIPTOR_FILE_ID),
        tracked_base_predicate = tracked_base_predicate,
    );
    let untracked_table = quote_ident(&tracked_relation_name(schema_key));
    let tracked_parent_expr = normalized_descriptor_select_expr(schema_key, "parent_id");
    let tracked_directory_expr = normalized_descriptor_select_expr(schema_key, "directory_id");
    let tracked_name_expr = normalized_descriptor_select_expr(schema_key, "name");
    let tracked_extension_expr = normalized_descriptor_select_expr(schema_key, "extension");
    let tracked_hidden_expr = normalized_hidden_select_expr(schema_key);
    let untracked_parent_expr = normalized_descriptor_select_expr(schema_key, "parent_id");
    let untracked_directory_expr = normalized_descriptor_select_expr(schema_key, "directory_id");
    let untracked_name_expr = normalized_descriptor_select_expr(schema_key, "name");
    let untracked_extension_expr = normalized_descriptor_select_expr(schema_key, "extension");
    let untracked_hidden_expr = normalized_hidden_select_expr(schema_key);
    format!(
        "SELECT entity_id, \
                {untracked_parent_expr} AS parent_id, \
                {untracked_directory_expr} AS directory_id, \
                {untracked_name_expr} AS name, \
                {untracked_extension_expr} AS extension, \
                {untracked_hidden_expr} AS hidden, \
                metadata, NULL AS change_id, 0 AS is_tombstone, \
                1 AS precedence, 1 AS untracked \
         FROM {untracked_table} \
         WHERE version_id = '{version_id}' \
           AND file_id = '{file_id}' \
           AND untracked = true \
           AND {untracked_base_predicate} \
         UNION ALL \
         SELECT entity_id, \
                {tracked_parent_expr} AS parent_id, \
                {tracked_directory_expr} AS directory_id, \
                {tracked_name_expr} AS name, \
                {tracked_extension_expr} AS extension, \
                {tracked_hidden_expr} AS hidden, \
                metadata, change_id, is_tombstone, 2 AS precedence, 0 AS untracked \
         FROM {tracked_table} \
         WHERE version_id = '{version_id}' \
           AND untracked = false \
           AND {tracked_base} \
         ORDER BY precedence ASC \
         LIMIT 1",
        untracked_table = untracked_table,
        tracked_table = tracked_table,
        version_id = escape_sql_string(version_id),
        file_id = escape_sql_string(FILESYSTEM_DESCRIPTOR_FILE_ID),
        tracked_base = tracked_base,
        untracked_base_predicate = untracked_base_predicate,
        untracked_parent_expr = untracked_parent_expr,
        untracked_directory_expr = untracked_directory_expr,
        untracked_name_expr = untracked_name_expr,
        untracked_extension_expr = untracked_extension_expr,
        untracked_hidden_expr = untracked_hidden_expr,
        tracked_parent_expr = tracked_parent_expr,
        tracked_directory_expr = tracked_directory_expr,
        tracked_name_expr = tracked_name_expr,
        tracked_extension_expr = tracked_extension_expr,
        tracked_hidden_expr = tracked_hidden_expr,
    )
}

fn normalized_descriptor_select_expr(schema_key: &str, column: &str) -> &'static str {
    match (schema_key, column) {
        (FILESYSTEM_DIRECTORY_SCHEMA_KEY, "parent_id") => "parent_id",
        (FILESYSTEM_DIRECTORY_SCHEMA_KEY, "name") => "name",
        (FILESYSTEM_DIRECTORY_SCHEMA_KEY, "directory_id") => "NULL",
        (FILESYSTEM_DIRECTORY_SCHEMA_KEY, "extension") => "NULL",
        (FILESYSTEM_FILE_SCHEMA_KEY, "parent_id") => "NULL",
        (FILESYSTEM_FILE_SCHEMA_KEY, "directory_id") => "directory_id",
        (FILESYSTEM_FILE_SCHEMA_KEY, "name") => "name",
        (FILESYSTEM_FILE_SCHEMA_KEY, "extension") => "extension",
        _ => "NULL",
    }
}

fn normalized_hidden_select_expr(schema_key: &str) -> &'static str {
    match schema_key {
        FILESYSTEM_DIRECTORY_SCHEMA_KEY | FILESYSTEM_FILE_SCHEMA_KEY => "COALESCE(hidden, false)",
        _ => "false",
    }
}

fn version_shadow_sql(
    tracked_table: &str,
    schema_key: &str,
    version_id: &str,
    entity_id: &str,
) -> String {
    let tracked_base = format!(
        "file_id = '{file_id}' AND entity_id = '{entity_id}'",
        file_id = escape_sql_string(FILESYSTEM_DESCRIPTOR_FILE_ID),
        entity_id = escape_sql_string(entity_id),
    );
    let untracked_base = format!(
        "file_id = '{file_id}' AND entity_id = '{entity_id}'",
        file_id = escape_sql_string(FILESYSTEM_DESCRIPTOR_FILE_ID),
        entity_id = escape_sql_string(entity_id),
    );
    format!(
        "SELECT entity_id, NULL AS snapshot_content, metadata, NULL AS change_id, \
                0 AS is_tombstone, \
                1 AS precedence, 1 AS untracked \
         FROM {untracked_table} \
         WHERE version_id = '{version_id}' \
           AND untracked = true \
           AND {untracked_base} \
         UNION ALL \
         SELECT entity_id, NULL AS snapshot_content, metadata, change_id, is_tombstone, 2 AS precedence, 0 AS untracked \
         FROM {tracked_table} \
         WHERE version_id = '{version_id}' \
           AND untracked = false \
           AND {tracked_base} \
         ORDER BY precedence ASC \
         LIMIT 1",
        untracked_table = quote_ident(&tracked_relation_name(schema_key)),
        tracked_table = tracked_table,
        version_id = escape_sql_string(version_id),
        tracked_base = tracked_base,
        untracked_base = untracked_base,
    )
}

async fn load_effective_descriptor_row(
    backend: &dyn LixBackend,
    sql: &str,
) -> Result<Option<EffectiveDescriptorRow>, FilesystemQueryError> {
    Ok(load_effective_descriptor_rows(backend, sql)
        .await?
        .into_iter()
        .next())
}

async fn load_effective_descriptor_rows(
    backend: &dyn LixBackend,
    sql: &str,
) -> Result<Vec<EffectiveDescriptorRow>, FilesystemQueryError> {
    let result = backend
        .execute(sql, &[])
        .await
        .map_err(filesystem_query_backend_error)?;
    let mut rows = Vec::new();
    let mut seen_ids = BTreeSet::new();
    for row in &result.rows {
        let id = required_text_value(row, "entity_id")?;
        if !seen_ids.insert(id.clone()) {
            continue;
        }
        if row.get(8).and_then(value_as_bool).unwrap_or(false) {
            continue;
        }
        let untracked = row.get(10).and_then(value_as_bool).unwrap_or(false);
        let name = row.get(3).and_then(text_from_value);
        if name.is_none() && untracked {
            continue;
        }
        rows.push(EffectiveDescriptorRow {
            id,
            parent_id: row.get(1).and_then(text_from_value),
            directory_id: row.get(2).and_then(text_from_value),
            name: name.ok_or_else(|| FilesystemQueryError {
                message: "filesystem descriptor row missing name".to_string(),
            })?,
            extension: row.get(4).and_then(text_from_value),
            hidden: row.get(5).and_then(value_as_bool).unwrap_or(false),
            untracked,
            metadata: row.get(6).and_then(text_from_value),
            change_id: row.get(7).and_then(text_from_value),
        });
    }
    Ok(rows)
}

async fn build_directory_path_from_descriptor(
    backend: &dyn LixBackend,
    version_id: &str,
    directory_id: &str,
    parent_id: Option<&str>,
    name: &str,
    scope: FilesystemProjectionScope,
) -> Result<String, FilesystemQueryError> {
    let mut segments = vec![name.to_string()];
    let mut current_parent_id = parent_id.map(ToOwned::to_owned);
    let mut safety = 0usize;
    while let Some(parent_id) = current_parent_id {
        let Some(parent) =
            load_directory_descriptor_by_id(backend, version_id, &parent_id, scope).await?
        else {
            return Err(FilesystemQueryError {
                message: format!(
                    "filesystem directory '{}' references missing parent '{}'",
                    directory_id, parent_id
                ),
            });
        };
        segments.push(parent.name.clone());
        current_parent_id = parent.parent_id;
        safety += 1;
        if safety > 1024 {
            return Err(FilesystemQueryError {
                message: "filesystem directory parent chain appears cyclic".to_string(),
            });
        }
    }

    segments.reverse();
    Ok(format!("/{}/", segments.join("/")))
}

fn compose_file_path(directory_path: &str, name: &str, extension: Option<&str>) -> String {
    match extension {
        Some(extension) if !extension.is_empty() => format!("{directory_path}{name}.{extension}"),
        _ => format!("{directory_path}{name}"),
    }
}

fn quote_ident(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}

fn required_text_value(row: &[Value], label: &str) -> Result<String, FilesystemQueryError> {
    required_text_value_index(row, 0, label)
}

fn required_text_value_index(
    row: &[Value],
    index: usize,
    label: &str,
) -> Result<String, FilesystemQueryError> {
    row.get(index)
        .and_then(text_from_value)
        .ok_or_else(|| FilesystemQueryError {
            message: format!("public filesystem resolver expected text {}", label),
        })
}

fn optional_text_value(value: Option<&Value>) -> Option<String> {
    value.and_then(text_from_value)
}

fn text_from_value(value: &Value) -> Option<String> {
    match value {
        Value::Text(value) => Some(value.clone()),
        Value::Integer(value) => Some(value.to_string()),
        Value::Boolean(value) => Some(value.to_string()),
        Value::Real(value) => Some(value.to_string()),
        _ => None,
    }
}

fn value_as_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Boolean(value) => Some(*value),
        Value::Integer(value) => Some(*value != 0),
        Value::Text(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" => Some(true),
            "0" | "false" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn filesystem_query_backend_error(error: crate::LixError) -> FilesystemQueryError {
    FilesystemQueryError {
        message: error.description,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{LixBackendTransaction, SqlDialect};
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    struct DirectFilesystemLookupBackend {
        projection_seen: Arc<AtomicBool>,
    }

    struct UnusedTransaction;

    #[async_trait(?Send)]
    impl LixBackend for DirectFilesystemLookupBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(
            &self,
            sql: &str,
            _params: &[Value],
        ) -> Result<crate::QueryResult, crate::LixError> {
            if sql.contains("WITH RECURSIVE target_versions")
                || sql.contains("FROM (WITH RECURSIVE")
            {
                self.projection_seen.store(true, Ordering::SeqCst);
            }
            if sql.contains(&tracked_relation_name(FILESYSTEM_FILE_SCHEMA_KEY))
                && sql.contains("entity_id = 'file-1'")
            {
                return Ok(crate::QueryResult {
                    rows: vec![vec![
                        Value::Text("file-1".to_string()),
                        Value::Null,
                        Value::Text("dir-nested".to_string()),
                        Value::Text("file".to_string()),
                        Value::Text("json".to_string()),
                        Value::Boolean(false),
                        Value::Null,
                        Value::Text("change-file".to_string()),
                        Value::Boolean(false),
                        Value::Integer(2),
                        Value::Boolean(false),
                    ]],
                    columns: Vec::new(),
                });
            }
            if sql.contains(&tracked_relation_name(FILESYSTEM_DIRECTORY_SCHEMA_KEY))
                && sql.contains("entity_id = 'dir-nested'")
            {
                return Ok(crate::QueryResult {
                    rows: vec![vec![
                        Value::Text("dir-nested".to_string()),
                        Value::Text("dir-bench".to_string()),
                        Value::Null,
                        Value::Text("nested".to_string()),
                        Value::Null,
                        Value::Boolean(false),
                        Value::Null,
                        Value::Text("change-dir-2".to_string()),
                        Value::Boolean(false),
                        Value::Integer(2),
                        Value::Boolean(false),
                    ]],
                    columns: Vec::new(),
                });
            }
            if sql.contains(&tracked_relation_name(FILESYSTEM_DIRECTORY_SCHEMA_KEY))
                && sql.contains("entity_id = 'dir-bench'")
            {
                return Ok(crate::QueryResult {
                    rows: vec![vec![
                        Value::Text("dir-bench".to_string()),
                        Value::Null,
                        Value::Null,
                        Value::Text("bench".to_string()),
                        Value::Null,
                        Value::Boolean(false),
                        Value::Null,
                        Value::Text("change-dir-1".to_string()),
                        Value::Boolean(false),
                        Value::Integer(2),
                        Value::Boolean(false),
                    ]],
                    columns: Vec::new(),
                });
            }
            Ok(crate::QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn begin_transaction(
            &self,
            _mode: crate::TransactionMode,
        ) -> Result<Box<dyn LixBackendTransaction + '_>, crate::LixError> {
            Ok(Box::new(UnusedTransaction))
        }

        async fn begin_savepoint(
            &self,
            _name: &str,
        ) -> Result<Box<dyn LixBackendTransaction + '_>, crate::LixError> {
            self.begin_transaction(crate::TransactionMode::Write).await
        }
    }

    #[async_trait(?Send)]
    impl LixBackendTransaction for UnusedTransaction {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        fn mode(&self) -> crate::TransactionMode {
            crate::TransactionMode::Write
        }

        async fn execute(
            &mut self,
            _sql: &str,
            _params: &[Value],
        ) -> Result<crate::QueryResult, crate::LixError> {
            Ok(crate::QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn commit(self: Box<Self>) -> Result<(), crate::LixError> {
            Ok(())
        }

        async fn rollback(self: Box<Self>) -> Result<(), crate::LixError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn load_file_row_by_id_reads_live_tables_without_projection() {
        let projection_seen = Arc::new(AtomicBool::new(false));
        let backend = DirectFilesystemLookupBackend {
            projection_seen: Arc::clone(&projection_seen),
        };

        let row = load_file_row_by_id(
            &backend,
            "v1",
            "file-1",
            FilesystemProjectionScope::ExplicitVersion,
        )
        .await
        .expect("live filesystem row lookup should succeed")
        .expect("live filesystem row should exist");

        assert!(
            !projection_seen.load(Ordering::SeqCst),
            "exact file lookup should not fall back to projection SQL"
        );
        assert_eq!(row.id, "file-1");
        assert_eq!(row.directory_id.as_deref(), Some("dir-nested"));
        assert_eq!(row.path, "/bench/nested/file.json");
        assert_eq!(row.change_id.as_deref(), Some("change-file"));
    }
}
