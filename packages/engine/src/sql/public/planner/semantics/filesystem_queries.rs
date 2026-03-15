use crate::filesystem::live_projection::{
    build_filesystem_directory_projection_sql, build_filesystem_file_projection_sql,
    FilesystemProjectionScope,
};
use crate::filesystem::path::{compose_directory_path, NormalizedDirectoryPath, ParsedFilePath};
use crate::sql::common::ast::{lower_statement, parse_sql_statements};
use crate::sql::storage::sql_text::escape_sql_string;
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackend, SqlDialect, Value};
use std::collections::{BTreeMap, BTreeSet};

const LIVE_FILE_DESCRIPTOR_TABLE: &str = "lix_internal_live_v1_lix_file_descriptor";
const LIVE_DIRECTORY_DESCRIPTOR_TABLE: &str = "lix_internal_live_v1_lix_directory_descriptor";
const LIVE_UNTRACKED_TABLE: &str = "lix_internal_live_untracked_v1";
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
    pub(crate) version_id: String,
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
        version_id: version_id.to_string(),
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
        version_id: version_id.to_string(),
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
        version_id: version_id.to_string(),
        untracked: descriptor.untracked,
        metadata: descriptor.metadata,
        change_id: descriptor.change_id,
    }))
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
        projection_sql =
            build_filesystem_directory_projection_sql(FilesystemProjectionScope::ExplicitVersion),
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
        projection_sql =
            build_filesystem_file_projection_sql(FilesystemProjectionScope::ExplicitVersion, false),
        version_id = escape_sql_string(version_id),
        prefix_length = prefix_length,
        root_path = escape_sql_string(root_path),
    );
    load_file_rows_from_sql(backend, &sql).await
}

pub(crate) async fn load_directory_rows_by_paths(
    backend: &dyn LixBackend,
    version_id: &str,
    paths: &BTreeSet<String>,
    scope: FilesystemProjectionScope,
) -> Result<Vec<DirectoryFilesystemRow>, FilesystemQueryError> {
    if paths.is_empty() {
        return Ok(Vec::new());
    }
    let path_list = sql_string_list(paths.iter().map(String::as_str));
    let sql = format!(
        "SELECT id, parent_id, name, path, hidden, lixcol_version_id, lixcol_untracked, lixcol_metadata, lixcol_change_id \
         FROM ({projection_sql}) directories \
         WHERE lixcol_version_id = '{version_id}' \
           AND path IN ({path_list}) \
         ORDER BY path ASC, id ASC",
        projection_sql = build_filesystem_directory_projection_sql(scope),
        version_id = escape_sql_string(version_id),
        path_list = path_list,
    );
    load_directory_rows_from_sql(backend, &sql).await
}

pub(crate) async fn load_file_rows_by_paths(
    backend: &dyn LixBackend,
    version_id: &str,
    paths: &BTreeSet<String>,
    scope: FilesystemProjectionScope,
) -> Result<Vec<FileFilesystemRow>, FilesystemQueryError> {
    if paths.is_empty() {
        return Ok(Vec::new());
    }
    let path_list = sql_string_list(paths.iter().map(String::as_str));
    let sql = format!(
        "SELECT id, directory_id, name, extension, path, hidden, lixcol_version_id, lixcol_untracked, metadata, lixcol_change_id \
         FROM ({projection_sql}) files \
         WHERE lixcol_version_id = '{version_id}' \
           AND path IN ({path_list}) \
         ORDER BY path ASC, id ASC",
        projection_sql = build_filesystem_file_projection_sql(scope, false),
        version_id = escape_sql_string(version_id),
        path_list = path_list,
    );
    load_file_rows_from_sql(backend, &sql).await
}

async fn load_directory_row_from_sql(
    backend: &dyn LixBackend,
    sql: &str,
) -> Result<Option<DirectoryFilesystemRow>, FilesystemQueryError> {
    Ok(load_directory_rows_from_sql(backend, sql)
        .await?
        .into_iter()
        .next())
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

async fn load_file_row_from_sql(
    backend: &dyn LixBackend,
    sql: &str,
) -> Result<Option<FileFilesystemRow>, FilesystemQueryError> {
    Ok(load_file_rows_from_sql(backend, sql)
        .await?
        .into_iter()
        .next())
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
                version_id: required_text_value_index(row, 6, "lixcol_version_id")?,
                untracked: row.get(7).and_then(value_as_bool).unwrap_or(false),
                metadata: row.get(8).and_then(text_from_value),
                change_id: row.get(9).and_then(text_from_value),
            })
        })
        .collect()
}

fn lower_internal_sql_for_backend(
    backend: &dyn LixBackend,
    sql: &str,
) -> Result<String, FilesystemQueryError> {
    let mut statements = parse_sql_statements(sql).map_err(filesystem_query_backend_error)?;
    if statements.len() != 1 {
        return Err(FilesystemQueryError {
            message: "public filesystem resolver expected a single helper statement".to_string(),
        });
    }
    let statement = statements.remove(0);
    let lowered =
        lower_statement(statement, backend.dialect()).map_err(filesystem_query_backend_error)?;
    Ok(lowered.to_string())
}

fn sql_string_list<'a>(values: impl IntoIterator<Item = &'a str>) -> String {
    values
        .into_iter()
        .map(|value| format!("'{}'", escape_sql_string(value)))
        .collect::<Vec<_>>()
        .join(", ")
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
    _scope: FilesystemProjectionScope,
) -> Result<Vec<EffectiveDescriptorRow>, FilesystemQueryError> {
    if pairs.is_empty() {
        return Ok(Vec::new());
    }

    let local_rows = load_visible_descriptor_rows_for_version(
        backend,
        LIVE_DIRECTORY_DESCRIPTOR_TABLE,
        FILESYSTEM_DIRECTORY_SCHEMA_KEY,
        &directory_pair_predicate(backend.dialect(), pairs),
        version_id,
    )
    .await?;
    let mut rows_by_key = BTreeMap::new();
    for row in local_rows {
        rows_by_key
            .entry((row.parent_id.clone(), row.name.clone()))
            .or_insert(row);
    }

    if version_id == GLOBAL_VERSION_ID || rows_by_key.len() == pairs.len() {
        return Ok(rows_by_key.into_values().collect());
    }

    let unresolved = pairs
        .iter()
        .filter(|pair| !rows_by_key.contains_key(*pair))
        .cloned()
        .collect::<BTreeSet<_>>();
    let global_rows = load_visible_descriptor_rows_for_version(
        backend,
        LIVE_DIRECTORY_DESCRIPTOR_TABLE,
        FILESYSTEM_DIRECTORY_SCHEMA_KEY,
        &directory_pair_predicate(backend.dialect(), &unresolved),
        GLOBAL_VERSION_ID,
    )
    .await?;
    let shadowed = entity_ids_with_version_tombstones(
        backend,
        LIVE_DIRECTORY_DESCRIPTOR_TABLE,
        FILESYSTEM_DIRECTORY_SCHEMA_KEY,
        version_id,
        &global_rows
            .iter()
            .map(|row| row.id.clone())
            .collect::<BTreeSet<_>>(),
    )
    .await?;
    for row in global_rows {
        if shadowed.contains(&row.id) {
            continue;
        }
        rows_by_key
            .entry((row.parent_id.clone(), row.name.clone()))
            .or_insert(row);
    }

    Ok(rows_by_key.into_values().collect())
}

pub(crate) async fn load_file_descriptors_by_directory_name_extension_triplets(
    backend: &dyn LixBackend,
    version_id: &str,
    triplets: &BTreeSet<(Option<String>, String, Option<String>)>,
    _scope: FilesystemProjectionScope,
) -> Result<Vec<EffectiveDescriptorRow>, FilesystemQueryError> {
    if triplets.is_empty() {
        return Ok(Vec::new());
    }

    let local_rows = load_visible_descriptor_rows_for_version(
        backend,
        LIVE_FILE_DESCRIPTOR_TABLE,
        FILESYSTEM_FILE_SCHEMA_KEY,
        &file_triplet_predicate(backend.dialect(), triplets),
        version_id,
    )
    .await?;
    let mut rows_by_key = BTreeMap::new();
    for row in local_rows {
        rows_by_key
            .entry((
                row.directory_id.clone(),
                row.name.clone(),
                row.extension.clone().filter(|value| !value.is_empty()),
            ))
            .or_insert(row);
    }

    if version_id == GLOBAL_VERSION_ID || rows_by_key.len() == triplets.len() {
        return Ok(rows_by_key.into_values().collect());
    }

    let unresolved = triplets
        .iter()
        .filter(|triplet| !rows_by_key.contains_key(*triplet))
        .cloned()
        .collect::<BTreeSet<_>>();
    let global_rows = load_visible_descriptor_rows_for_version(
        backend,
        LIVE_FILE_DESCRIPTOR_TABLE,
        FILESYSTEM_FILE_SCHEMA_KEY,
        &file_triplet_predicate(backend.dialect(), &unresolved),
        GLOBAL_VERSION_ID,
    )
    .await?;
    let shadowed = entity_ids_with_version_tombstones(
        backend,
        LIVE_FILE_DESCRIPTOR_TABLE,
        FILESYSTEM_FILE_SCHEMA_KEY,
        version_id,
        &global_rows
            .iter()
            .map(|row| row.id.clone())
            .collect::<BTreeSet<_>>(),
    )
    .await?;
    for row in global_rows {
        if shadowed.contains(&row.id) {
            continue;
        }
        rows_by_key
            .entry((
                row.directory_id.clone(),
                row.name.clone(),
                row.extension.clone().filter(|value| !value.is_empty()),
            ))
            .or_insert(row);
    }

    Ok(rows_by_key.into_values().collect())
}

async fn load_directory_descriptor_by_id(
    backend: &dyn LixBackend,
    version_id: &str,
    directory_id: &str,
    scope: FilesystemProjectionScope,
) -> Result<Option<EffectiveDescriptorRow>, FilesystemQueryError> {
    let sql = effective_directory_descriptor_sql(
        backend.dialect(),
        &format!("entity_id = '{}'", escape_sql_string(directory_id)),
        version_id,
        scope,
    );
    load_effective_descriptor_row(backend, &sql).await
}

async fn load_directory_descriptor_by_parent_and_name(
    backend: &dyn LixBackend,
    version_id: &str,
    parent_id: Option<&str>,
    name: &str,
    scope: FilesystemProjectionScope,
) -> Result<Option<EffectiveDescriptorRow>, FilesystemQueryError> {
    let parent_predicate = match parent_id {
        Some(parent_id) => format!(
            "{} = '{}'",
            json_text_extract_expr(backend.dialect(), "snapshot_content", "parent_id"),
            escape_sql_string(parent_id)
        ),
        None => format!(
            "{} IS NULL",
            json_text_extract_expr(backend.dialect(), "snapshot_content", "parent_id")
        ),
    };
    let name_predicate = format!(
        "{} = '{}'",
        json_text_extract_expr(backend.dialect(), "snapshot_content", "name"),
        escape_sql_string(name)
    );
    load_scoped_descriptor_row(
        backend,
        LIVE_DIRECTORY_DESCRIPTOR_TABLE,
        FILESYSTEM_DIRECTORY_SCHEMA_KEY,
        &format!("{parent_predicate} AND {name_predicate}"),
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
    let sql = effective_file_descriptor_sql(
        backend.dialect(),
        &format!("entity_id = '{}'", escape_sql_string(file_id)),
        version_id,
        scope,
    );
    load_effective_descriptor_row(backend, &sql).await
}

async fn load_file_descriptor_by_path_components(
    backend: &dyn LixBackend,
    version_id: &str,
    directory_id: Option<&str>,
    name: &str,
    extension: Option<&str>,
    scope: FilesystemProjectionScope,
) -> Result<Option<EffectiveDescriptorRow>, FilesystemQueryError> {
    let directory_predicate = match directory_id {
        Some(directory_id) => format!(
            "{} = '{}'",
            json_text_extract_expr(backend.dialect(), "snapshot_content", "directory_id"),
            escape_sql_string(directory_id)
        ),
        None => format!(
            "{} IS NULL",
            json_text_extract_expr(backend.dialect(), "snapshot_content", "directory_id")
        ),
    };
    let name_predicate = format!(
        "{} = '{}'",
        json_text_extract_expr(backend.dialect(), "snapshot_content", "name"),
        escape_sql_string(name)
    );
    let extension_predicate = match extension {
        Some(extension) => format!(
            "{} = '{}'",
            json_text_extract_expr(backend.dialect(), "snapshot_content", "extension"),
            escape_sql_string(extension)
        ),
        None => format!(
            "({expr} IS NULL OR {expr} = '')",
            expr = json_text_extract_expr(backend.dialect(), "snapshot_content", "extension")
        ),
    };
    load_scoped_descriptor_row(
        backend,
        LIVE_FILE_DESCRIPTOR_TABLE,
        FILESYSTEM_FILE_SCHEMA_KEY,
        &format!("{directory_predicate} AND {name_predicate} AND {extension_predicate}"),
        version_id,
        scope,
    )
    .await
}

fn effective_directory_descriptor_sql(
    dialect: SqlDialect,
    base_predicate: &str,
    version_id: &str,
    scope: FilesystemProjectionScope,
) -> String {
    effective_descriptor_sql(
        dialect,
        LIVE_DIRECTORY_DESCRIPTOR_TABLE,
        FILESYSTEM_DIRECTORY_SCHEMA_KEY,
        base_predicate,
        version_id,
        scope,
    )
}

fn effective_file_descriptor_sql(
    dialect: SqlDialect,
    base_predicate: &str,
    version_id: &str,
    scope: FilesystemProjectionScope,
) -> String {
    effective_descriptor_sql(
        dialect,
        LIVE_FILE_DESCRIPTOR_TABLE,
        FILESYSTEM_FILE_SCHEMA_KEY,
        base_predicate,
        version_id,
        scope,
    )
}

fn effective_descriptor_sql(
    _dialect: SqlDialect,
    tracked_table: &str,
    schema_key: &str,
    base_predicate: &str,
    version_id: &str,
    _scope: FilesystemProjectionScope,
) -> String {
    let tracked_base = format!(
        "file_id = '{file_id}' AND {base_predicate}",
        file_id = escape_sql_string(FILESYSTEM_DESCRIPTOR_FILE_ID),
        base_predicate = base_predicate,
    );
    let untracked_base = format!(
        "schema_key = '{schema_key}' AND file_id = '{file_id}' AND {base_predicate}",
        schema_key = escape_sql_string(schema_key),
        file_id = escape_sql_string(FILESYSTEM_DESCRIPTOR_FILE_ID),
        base_predicate = base_predicate,
    );
    format!(
        "SELECT entity_id, snapshot_content, metadata, NULL AS change_id, \
                CASE WHEN snapshot_content IS NULL THEN 1 ELSE 0 END AS is_tombstone, \
                1 AS precedence, 1 AS untracked \
         FROM {untracked_table} \
         WHERE version_id = '{version_id}' \
           AND {untracked_base} \
         UNION ALL \
         SELECT entity_id, snapshot_content, metadata, change_id, is_tombstone, 2 AS precedence, 0 AS untracked \
         FROM {tracked_table} \
         WHERE version_id = '{version_id}' \
           AND {tracked_base} \
         UNION ALL \
         SELECT entity_id, snapshot_content, metadata, NULL AS change_id, \
                CASE WHEN snapshot_content IS NULL THEN 1 ELSE 0 END AS is_tombstone, \
                3 AS precedence, 1 AS untracked \
         FROM {untracked_table} \
         WHERE version_id = '{global_version_id}' \
           AND {untracked_base} \
         UNION ALL \
         SELECT entity_id, snapshot_content, metadata, change_id, is_tombstone, 4 AS precedence, 0 AS untracked \
         FROM {tracked_table} \
         WHERE version_id = '{global_version_id}' \
           AND {tracked_base} \
         ORDER BY precedence ASC \
         LIMIT 1",
        untracked_table = LIVE_UNTRACKED_TABLE,
        tracked_table = tracked_table,
        version_id = escape_sql_string(version_id),
        global_version_id = escape_sql_string(GLOBAL_VERSION_ID),
        tracked_base = tracked_base,
        untracked_base = untracked_base,
    )
}

async fn load_scoped_descriptor_row(
    backend: &dyn LixBackend,
    tracked_table: &str,
    schema_key: &str,
    base_predicate: &str,
    version_id: &str,
    _scope: FilesystemProjectionScope,
) -> Result<Option<EffectiveDescriptorRow>, FilesystemQueryError> {
    if let Some(local_row) = load_visible_descriptor_row_for_version(
        backend,
        tracked_table,
        schema_key,
        base_predicate,
        version_id,
    )
    .await?
    {
        return Ok(Some(local_row));
    }

    if version_id == GLOBAL_VERSION_ID {
        return Ok(None);
    }

    let Some(global_row) = load_visible_descriptor_row_for_version(
        backend,
        tracked_table,
        schema_key,
        base_predicate,
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
    base_predicate: &str,
    version_id: &str,
) -> Result<Option<EffectiveDescriptorRow>, FilesystemQueryError> {
    let sql = visible_descriptor_sql(tracked_table, schema_key, base_predicate, version_id);
    load_effective_descriptor_row(backend, &sql).await
}

async fn load_visible_descriptor_rows_for_version(
    backend: &dyn LixBackend,
    tracked_table: &str,
    schema_key: &str,
    base_predicate: &str,
    version_id: &str,
) -> Result<Vec<EffectiveDescriptorRow>, FilesystemQueryError> {
    let sql = visible_descriptor_rows_sql(tracked_table, schema_key, base_predicate, version_id);
    load_effective_descriptor_rows(backend, &sql).await
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

async fn entity_ids_with_version_tombstones(
    backend: &dyn LixBackend,
    tracked_table: &str,
    schema_key: &str,
    version_id: &str,
    entity_ids: &BTreeSet<String>,
) -> Result<BTreeSet<String>, FilesystemQueryError> {
    if entity_ids.is_empty() {
        return Ok(BTreeSet::new());
    }
    let sql = version_shadow_rows_sql(tracked_table, schema_key, version_id, entity_ids);
    let result = backend
        .execute(&sql, &[])
        .await
        .map_err(filesystem_query_backend_error)?;
    let mut shadowed = BTreeSet::new();
    let mut seen = BTreeSet::new();
    for row in &result.rows {
        let Some(entity_id) = row.first().and_then(text_from_value) else {
            continue;
        };
        if !seen.insert(entity_id.clone()) {
            continue;
        }
        if row.get(4).and_then(value_as_bool).unwrap_or(false) {
            shadowed.insert(entity_id);
        }
    }
    Ok(shadowed)
}

fn visible_descriptor_sql(
    tracked_table: &str,
    schema_key: &str,
    base_predicate: &str,
    version_id: &str,
) -> String {
    let tracked_base = format!(
        "file_id = '{file_id}' AND {base_predicate}",
        file_id = escape_sql_string(FILESYSTEM_DESCRIPTOR_FILE_ID),
        base_predicate = base_predicate,
    );
    let untracked_base = format!(
        "schema_key = '{schema_key}' AND file_id = '{file_id}' AND {base_predicate}",
        schema_key = escape_sql_string(schema_key),
        file_id = escape_sql_string(FILESYSTEM_DESCRIPTOR_FILE_ID),
        base_predicate = base_predicate,
    );
    format!(
        "SELECT entity_id, snapshot_content, metadata, NULL AS change_id, \
                CASE WHEN snapshot_content IS NULL THEN 1 ELSE 0 END AS is_tombstone, \
                1 AS precedence, 1 AS untracked \
         FROM {untracked_table} \
         WHERE version_id = '{version_id}' \
           AND {untracked_base} \
         UNION ALL \
         SELECT entity_id, snapshot_content, metadata, change_id, is_tombstone, 2 AS precedence, 0 AS untracked \
         FROM {tracked_table} \
         WHERE version_id = '{version_id}' \
           AND {tracked_base} \
         ORDER BY precedence ASC \
         LIMIT 1",
        untracked_table = LIVE_UNTRACKED_TABLE,
        tracked_table = tracked_table,
        version_id = escape_sql_string(version_id),
        tracked_base = tracked_base,
        untracked_base = untracked_base,
    )
}

fn visible_descriptor_rows_sql(
    tracked_table: &str,
    schema_key: &str,
    base_predicate: &str,
    version_id: &str,
) -> String {
    let tracked_base = format!(
        "file_id = '{file_id}' AND ({base_predicate})",
        file_id = escape_sql_string(FILESYSTEM_DESCRIPTOR_FILE_ID),
        base_predicate = base_predicate,
    );
    let untracked_base = format!(
        "schema_key = '{schema_key}' AND file_id = '{file_id}' AND ({base_predicate})",
        schema_key = escape_sql_string(schema_key),
        file_id = escape_sql_string(FILESYSTEM_DESCRIPTOR_FILE_ID),
        base_predicate = base_predicate,
    );
    format!(
        "SELECT entity_id, snapshot_content, metadata, NULL AS change_id, \
                CASE WHEN snapshot_content IS NULL THEN 1 ELSE 0 END AS is_tombstone, \
                1 AS precedence, 1 AS untracked \
         FROM {untracked_table} \
         WHERE version_id = '{version_id}' \
           AND {untracked_base} \
         UNION ALL \
         SELECT entity_id, snapshot_content, metadata, change_id, is_tombstone, 2 AS precedence, 0 AS untracked \
         FROM {tracked_table} \
         WHERE version_id = '{version_id}' \
           AND {tracked_base} \
         ORDER BY precedence ASC",
        untracked_table = LIVE_UNTRACKED_TABLE,
        tracked_table = tracked_table,
        version_id = escape_sql_string(version_id),
        tracked_base = tracked_base,
        untracked_base = untracked_base,
    )
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
        "schema_key = '{schema_key}' AND file_id = '{file_id}' AND entity_id = '{entity_id}'",
        schema_key = escape_sql_string(schema_key),
        file_id = escape_sql_string(FILESYSTEM_DESCRIPTOR_FILE_ID),
        entity_id = escape_sql_string(entity_id),
    );
    format!(
        "SELECT entity_id, snapshot_content, metadata, NULL AS change_id, \
                CASE WHEN snapshot_content IS NULL THEN 1 ELSE 0 END AS is_tombstone, \
                1 AS precedence, 1 AS untracked \
         FROM {untracked_table} \
         WHERE version_id = '{version_id}' \
           AND {untracked_base} \
         UNION ALL \
         SELECT entity_id, snapshot_content, metadata, change_id, is_tombstone, 2 AS precedence, 0 AS untracked \
         FROM {tracked_table} \
         WHERE version_id = '{version_id}' \
           AND {tracked_base} \
         ORDER BY precedence ASC \
         LIMIT 1",
        untracked_table = LIVE_UNTRACKED_TABLE,
        tracked_table = tracked_table,
        version_id = escape_sql_string(version_id),
        tracked_base = tracked_base,
        untracked_base = untracked_base,
    )
}

fn version_shadow_rows_sql(
    tracked_table: &str,
    schema_key: &str,
    version_id: &str,
    entity_ids: &BTreeSet<String>,
) -> String {
    let entity_predicate = format!(
        "entity_id IN ({})",
        sql_string_list(entity_ids.iter().map(String::as_str))
    );
    let tracked_base = format!(
        "file_id = '{file_id}' AND {entity_predicate}",
        file_id = escape_sql_string(FILESYSTEM_DESCRIPTOR_FILE_ID),
        entity_predicate = entity_predicate,
    );
    let untracked_base = format!(
        "schema_key = '{schema_key}' AND file_id = '{file_id}' AND {entity_predicate}",
        schema_key = escape_sql_string(schema_key),
        file_id = escape_sql_string(FILESYSTEM_DESCRIPTOR_FILE_ID),
        entity_predicate = entity_predicate,
    );
    format!(
        "SELECT entity_id, snapshot_content, metadata, NULL AS change_id, \
                CASE WHEN snapshot_content IS NULL THEN 1 ELSE 0 END AS is_tombstone, \
                1 AS precedence, 1 AS untracked \
         FROM {untracked_table} \
         WHERE version_id = '{version_id}' \
           AND {untracked_base} \
         UNION ALL \
         SELECT entity_id, snapshot_content, metadata, change_id, is_tombstone, 2 AS precedence, 0 AS untracked \
         FROM {tracked_table} \
         WHERE version_id = '{version_id}' \
           AND {tracked_base} \
         ORDER BY entity_id ASC, precedence ASC",
        untracked_table = LIVE_UNTRACKED_TABLE,
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
    for row in &result.rows {
        if row.get(4).and_then(value_as_bool).unwrap_or(false) || row.get(1).is_none() {
            continue;
        }
        let Some(snapshot_content) = row.get(1).and_then(text_from_value) else {
            continue;
        };
        let id = required_text_value(row, "entity_id")?;
        rows.push(EffectiveDescriptorRow {
            id,
            parent_id: extract_json_text(&snapshot_content, "parent_id")
                .map_err(filesystem_query_backend_error)?,
            directory_id: extract_json_text(&snapshot_content, "directory_id")
                .map_err(filesystem_query_backend_error)?,
            name: extract_json_text(&snapshot_content, "name")
                .map_err(filesystem_query_backend_error)?
                .ok_or_else(|| FilesystemQueryError {
                    message: "filesystem descriptor snapshot missing name".to_string(),
                })?,
            extension: extract_json_text(&snapshot_content, "extension")
                .map_err(filesystem_query_backend_error)?,
            hidden: extract_json_bool(&snapshot_content, "hidden")
                .map_err(filesystem_query_backend_error)?
                .unwrap_or(false),
            untracked: row.get(6).and_then(value_as_bool).unwrap_or(false),
            metadata: row.get(2).and_then(text_from_value),
            change_id: row.get(3).and_then(text_from_value),
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

fn json_text_extract_expr(dialect: SqlDialect, column: &str, key: &str) -> String {
    match dialect {
        SqlDialect::Sqlite => format!("json_extract({column}, '$.{key}')"),
        SqlDialect::Postgres => {
            format!("jsonb_extract_path_text(CAST({column} AS JSONB), '{key}')")
        }
    }
}

fn directory_pair_predicate(
    dialect: SqlDialect,
    pairs: &BTreeSet<(Option<String>, String)>,
) -> String {
    let parent_expr = json_text_extract_expr(dialect, "snapshot_content", "parent_id");
    let name_expr = json_text_extract_expr(dialect, "snapshot_content", "name");
    pairs
        .iter()
        .map(|(parent_id, name)| {
            let parent_predicate = match parent_id {
                Some(parent_id) => format!("{parent_expr} = '{}'", escape_sql_string(parent_id)),
                None => format!("{parent_expr} IS NULL"),
            };
            format!(
                "({parent_predicate} AND {name_expr} = '{}')",
                escape_sql_string(name)
            )
        })
        .collect::<Vec<_>>()
        .join(" OR ")
}

fn file_triplet_predicate(
    dialect: SqlDialect,
    triplets: &BTreeSet<(Option<String>, String, Option<String>)>,
) -> String {
    let directory_expr = json_text_extract_expr(dialect, "snapshot_content", "directory_id");
    let name_expr = json_text_extract_expr(dialect, "snapshot_content", "name");
    let extension_expr = json_text_extract_expr(dialect, "snapshot_content", "extension");
    triplets
        .iter()
        .map(|(directory_id, name, extension)| {
            let directory_predicate = match directory_id {
                Some(directory_id) => {
                    format!("{directory_expr} = '{}'", escape_sql_string(directory_id))
                }
                None => format!("{directory_expr} IS NULL"),
            };
            let extension_predicate = match extension {
                Some(extension) => format!("{extension_expr} = '{}'", escape_sql_string(extension)),
                None => format!("({extension_expr} IS NULL OR {extension_expr} = '')"),
            };
            format!(
                "({directory_predicate} AND {name_expr} = '{}' AND {extension_predicate})",
                escape_sql_string(name)
            )
        })
        .collect::<Vec<_>>()
        .join(" OR ")
}

fn extract_json_text(snapshot_content: &str, key: &str) -> Result<Option<String>, crate::LixError> {
    let parsed = serde_json::from_str::<serde_json::Value>(snapshot_content).map_err(|error| {
        crate::LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("filesystem descriptor snapshot invalid JSON: {error}"),
        }
    })?;
    Ok(parsed
        .get(key)
        .and_then(|value| value.as_str().map(ToOwned::to_owned)))
}

fn extract_json_bool(snapshot_content: &str, key: &str) -> Result<Option<bool>, crate::LixError> {
    let parsed = serde_json::from_str::<serde_json::Value>(snapshot_content).map_err(|error| {
        crate::LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("filesystem descriptor snapshot invalid JSON: {error}"),
        }
    })?;
    Ok(parsed.get(key).and_then(|value| value.as_bool()))
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
    use crate::backend::{LixTransaction, SqlDialect};
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
            if sql.contains(LIVE_FILE_DESCRIPTOR_TABLE) && sql.contains("entity_id = 'file-1'") {
                return Ok(crate::QueryResult {
                    rows: vec![vec![
                        Value::Text("file-1".to_string()),
                        Value::Text(
                            "{\"id\":\"file-1\",\"directory_id\":\"dir-nested\",\"name\":\"file\",\"extension\":\"json\",\"hidden\":false}".to_string(),
                        ),
                        Value::Null,
                        Value::Text("change-file".to_string()),
                        Value::Integer(0),
                        Value::Integer(2),
                        Value::Integer(0),
                    ]],
                    columns: Vec::new(),
                });
            }
            if sql.contains(LIVE_DIRECTORY_DESCRIPTOR_TABLE)
                && sql.contains("entity_id = 'dir-nested'")
            {
                return Ok(crate::QueryResult {
                    rows: vec![vec![
                        Value::Text("dir-nested".to_string()),
                        Value::Text(
                            "{\"id\":\"dir-nested\",\"parent_id\":\"dir-bench\",\"name\":\"nested\",\"hidden\":false}".to_string(),
                        ),
                        Value::Null,
                        Value::Text("change-dir-2".to_string()),
                        Value::Integer(0),
                        Value::Integer(2),
                        Value::Integer(0),
                    ]],
                    columns: Vec::new(),
                });
            }
            if sql.contains(LIVE_DIRECTORY_DESCRIPTOR_TABLE)
                && sql.contains("entity_id = 'dir-bench'")
            {
                return Ok(crate::QueryResult {
                    rows: vec![vec![
                        Value::Text("dir-bench".to_string()),
                        Value::Text(
                            "{\"id\":\"dir-bench\",\"parent_id\":null,\"name\":\"bench\",\"hidden\":false}".to_string(),
                        ),
                        Value::Null,
                        Value::Text("change-dir-1".to_string()),
                        Value::Integer(0),
                        Value::Integer(2),
                        Value::Integer(0),
                    ]],
                    columns: Vec::new(),
                });
            }
            Ok(crate::QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, crate::LixError> {
            Ok(Box::new(UnusedTransaction))
        }
    }

    #[async_trait(?Send)]
    impl LixTransaction for UnusedTransaction {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
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
