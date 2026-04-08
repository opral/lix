use crate::{LixBackend, LixError, Value};

pub(crate) async fn load_file_payload_cache_data(
    backend: &dyn LixBackend,
    file_id: &str,
    version_id: &str,
) -> Result<Vec<u8>, LixError> {
    let result = backend
        .execute(
            "SELECT data \
             FROM lix_internal_file_data_cache \
             WHERE file_id = $1 AND version_id = $2 \
             LIMIT 1",
            &[
                Value::Text(file_id.to_string()),
                Value::Text(version_id.to_string()),
            ],
        )
        .await?;

    let Some(row) = result.rows.first() else {
        return Ok(Vec::new());
    };
    blob_required(row, 0, "data")
}

pub(crate) async fn upsert_file_payload_cache_data(
    backend: &dyn LixBackend,
    file_id: &str,
    version_id: &str,
    data: &[u8],
) -> Result<(), LixError> {
    backend
        .execute(
            "INSERT INTO lix_internal_file_data_cache (file_id, version_id, data) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (file_id, version_id) DO UPDATE SET \
             data = EXCLUDED.data",
            &[
                Value::Text(file_id.to_string()),
                Value::Text(version_id.to_string()),
                Value::Blob(data.to_vec()),
            ],
        )
        .await?;
    Ok(())
}

pub(crate) async fn delete_file_payload_cache_data(
    backend: &dyn LixBackend,
    file_id: &str,
    version_id: &str,
) -> Result<(), LixError> {
    backend
        .execute(
            "DELETE FROM lix_internal_file_data_cache \
             WHERE file_id = $1 AND version_id = $2",
            &[
                Value::Text(file_id.to_string()),
                Value::Text(version_id.to_string()),
            ],
        )
        .await?;
    Ok(())
}

fn blob_required(row: &[Value], index: usize, column: &str) -> Result<Vec<u8>, LixError> {
    let Some(value) = row.get(index) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: row missing column '{column}' at index {index}"
            ),
        });
    };
    match value {
        Value::Blob(bytes) => Ok(bytes.clone()),
        other => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: expected blob column '{column}' at index {index}, got {other:?}"
            ),
        }),
    }
}
