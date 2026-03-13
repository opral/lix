use crate::filesystem::live_projection::{
    build_filesystem_directory_projection_sql, build_filesystem_file_projection_sql,
    FilesystemProjectionScope,
};
use crate::sql::common::ast::{lower_statement, parse_sql_statements};
use crate::sql::storage::sql_text::escape_sql_string;
use crate::{LixBackend, Value};

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
    path: &str,
    scope: FilesystemProjectionScope,
) -> Result<Option<String>, FilesystemQueryError> {
    Ok(load_directory_row_by_path(backend, version_id, path, scope)
        .await?
        .map(|row| row.id))
}

pub(crate) async fn lookup_file_id_by_path(
    backend: &dyn LixBackend,
    version_id: &str,
    path: &str,
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
    directory_path: &str,
    lookup_scope: FilesystemProjectionScope,
) -> Result<(), FilesystemQueryError> {
    let file_path = directory_path.trim_end_matches('/').to_string();
    if lookup_file_id_by_path(backend, version_id, &file_path, lookup_scope)
        .await?
        .is_none()
    {
        return Ok(());
    }
    Err(FilesystemQueryError {
        message: format!("Directory path collides with existing file path: {file_path}"),
    })
}

pub(crate) async fn ensure_no_directory_at_file_path(
    backend: &dyn LixBackend,
    version_id: &str,
    file_path: &str,
    lookup_scope: FilesystemProjectionScope,
) -> Result<(), FilesystemQueryError> {
    let directory_path = format!("{}/", file_path.trim_end_matches('/'));
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
    let sql = format!(
        "SELECT id, parent_id, name, path, hidden, lixcol_version_id, lixcol_untracked, lixcol_metadata, lixcol_change_id \
         FROM ({projection_sql}) directories \
         WHERE lixcol_version_id = '{version_id}' \
           AND id = '{directory_id}' \
         LIMIT 1",
        projection_sql = build_filesystem_directory_projection_sql(scope),
        version_id = escape_sql_string(version_id),
        directory_id = escape_sql_string(directory_id),
    );
    load_directory_row_from_sql(backend, &sql).await
}

pub(crate) async fn load_directory_row_by_path(
    backend: &dyn LixBackend,
    version_id: &str,
    path: &str,
    scope: FilesystemProjectionScope,
) -> Result<Option<DirectoryFilesystemRow>, FilesystemQueryError> {
    let sql = format!(
        "SELECT id, parent_id, name, path, hidden, lixcol_version_id, lixcol_untracked, lixcol_metadata, lixcol_change_id \
         FROM ({projection_sql}) directories \
         WHERE lixcol_version_id = '{version_id}' \
           AND path = '{path}' \
         LIMIT 1",
        projection_sql = build_filesystem_directory_projection_sql(scope),
        version_id = escape_sql_string(version_id),
        path = escape_sql_string(path),
    );
    load_directory_row_from_sql(backend, &sql).await
}

pub(crate) async fn load_file_row_by_path(
    backend: &dyn LixBackend,
    version_id: &str,
    path: &str,
    scope: FilesystemProjectionScope,
) -> Result<Option<FileFilesystemRow>, FilesystemQueryError> {
    let sql = format!(
        "SELECT id, directory_id, name, extension, path, hidden, lixcol_version_id, lixcol_untracked, metadata, lixcol_change_id \
         FROM ({projection_sql}) files \
         WHERE lixcol_version_id = '{version_id}' \
           AND path = '{path}' \
         LIMIT 1",
        projection_sql = build_filesystem_file_projection_sql(scope, false),
        version_id = escape_sql_string(version_id),
        path = escape_sql_string(path),
    );
    load_file_row_from_sql(backend, &sql).await
}

pub(crate) async fn load_file_row_by_id(
    backend: &dyn LixBackend,
    version_id: &str,
    file_id: &str,
    scope: FilesystemProjectionScope,
) -> Result<Option<FileFilesystemRow>, FilesystemQueryError> {
    let sql = format!(
        "SELECT id, directory_id, name, extension, path, hidden, lixcol_version_id, lixcol_untracked, metadata, lixcol_change_id \
         FROM ({projection_sql}) files \
         WHERE lixcol_version_id = '{version_id}' \
           AND id = '{file_id}' \
         LIMIT 1",
        projection_sql = build_filesystem_file_projection_sql(scope, false),
        version_id = escape_sql_string(version_id),
        file_id = escape_sql_string(file_id),
    );
    load_file_row_from_sql(backend, &sql).await
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
