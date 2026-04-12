use crate::binary_cas::binary_file_version_ref_relation_name;
use crate::{LixBackend, LixError, Value};

use super::FILE_PATH_CACHE_TABLE;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PluginArchiveRef {
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) path: String,
    pub(crate) blob_hash: String,
}

pub(crate) async fn list_installed_plugin_archive_refs(
    backend: &dyn LixBackend,
) -> Result<Vec<PluginArchiveRef>, LixError> {
    let rows = backend
        .execute(
            &format!(
                "SELECT binary_ref.file_id, binary_ref.version_id, path_cache.path, binary_ref.blob_hash \
                 FROM {binary_file_version_ref} AS binary_ref \
                 INNER JOIN {file_path_cache} AS path_cache \
                     ON path_cache.file_id = binary_ref.file_id \
                    AND path_cache.version_id = binary_ref.version_id \
                 WHERE binary_ref.version_id = 'global' \
                   AND path_cache.path LIKE '/.lix/plugins/%.lixplugin' \
                   AND path_cache.path NOT LIKE '/.lix/plugins/%/%' \
                 ORDER BY path_cache.path",
                binary_file_version_ref = binary_file_version_ref_relation_name(),
                file_path_cache = FILE_PATH_CACHE_TABLE,
            ),
            &[],
        )
        .await?;

    rows.rows
        .into_iter()
        .map(|row| plugin_archive_ref_from_row(&row))
        .collect()
}

fn plugin_archive_ref_from_row(row: &[Value]) -> Result<PluginArchiveRef, LixError> {
    Ok(PluginArchiveRef {
        file_id: text_required(row, 0, "file_id")?,
        version_id: text_required(row, 1, "version_id")?,
        path: text_required(row, 2, "path")?,
        blob_hash: text_required(row, 3, "blob_hash")?,
    })
}

fn text_required(row: &[Value], index: usize, column: &str) -> Result<String, LixError> {
    let Some(value) = row.get(index) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin archive lookup: row missing column '{column}' at index {index}"
            ),
        });
    };

    match value {
        Value::Text(text) => Ok(text.clone()),
        other => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin archive lookup: expected text in column '{column}', got {other:?}"
            ),
        }),
    }
}
