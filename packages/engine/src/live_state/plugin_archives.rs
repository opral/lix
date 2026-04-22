use crate::live_state::store::LiveStateBackendRef;
use crate::{LixError, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PluginArchiveRef {
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) path: String,
    pub(crate) blob_hash: String,
}

pub(crate) async fn list_installed_plugin_archive_refs(
    backend: LiveStateBackendRef<'_>,
) -> Result<Vec<PluginArchiveRef>, LixError> {
    crate::live_state::storage::load_plugin_archive_ref_rows(backend)
        .await?
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
            hint: None,
        });
    };

    match value {
        Value::Text(text) => Ok(text.clone()),
        other => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin archive lookup: expected text in column '{column}', got {other:?}"
            ),
            hint: None,
        }),
    }
}
