use crate::binary_cas::codec::decode_binary_chunk_payload;
use crate::binary_cas::schema::{
    INTERNAL_BINARY_BLOB_MANIFEST, INTERNAL_BINARY_BLOB_MANIFEST_CHUNK, INTERNAL_BINARY_BLOB_STORE,
    INTERNAL_BINARY_CHUNK_STORE,
};
use crate::{LixBackend, LixError, Value};

pub(crate) async fn blob_exists(
    backend: &dyn LixBackend,
    blob_hash: &str,
) -> Result<bool, LixError> {
    let result = backend
        .execute(
            &format!(
                "SELECT 1 \
                 FROM {blob_store} bs \
                 JOIN {blob_manifest} bm ON bm.blob_hash = bs.blob_hash \
                 WHERE bs.blob_hash = $1 \
                 LIMIT 1",
                blob_store = INTERNAL_BINARY_BLOB_STORE,
                blob_manifest = INTERNAL_BINARY_BLOB_MANIFEST,
            ),
            &[Value::Text(blob_hash.to_string())],
        )
        .await?;
    Ok(!result.rows.is_empty())
}

pub(crate) async fn load_binary_blob_data_by_hash(
    backend: &dyn LixBackend,
    blob_hash: &str,
) -> Result<Option<Vec<u8>>, LixError> {
    let inline_result = backend
        .execute(
            &format!(
                "SELECT data \
                 FROM {blob_store} \
                 WHERE blob_hash = $1 \
                 LIMIT 1",
                blob_store = INTERNAL_BINARY_BLOB_STORE,
            ),
            &[Value::Text(blob_hash.to_string())],
        )
        .await?;

    if let Some(row) = inline_result.rows.first() {
        return Ok(Some(blob_required(
            row,
            0,
            "data",
            "binary CAS read inline blob",
        )?));
    }

    let manifest_rows = backend
        .execute(
            &format!(
                "SELECT size_bytes, chunk_count \
                 FROM {blob_manifest} \
                 WHERE blob_hash = $1 \
                 LIMIT 1",
                blob_manifest = INTERNAL_BINARY_BLOB_MANIFEST,
            ),
            &[Value::Text(blob_hash.to_string())],
        )
        .await?;
    let Some(manifest_row) = manifest_rows.rows.first() else {
        return Ok(None);
    };
    let manifest_size_bytes =
        i64_required(manifest_row, 0, "size_bytes", "binary CAS read manifest")?;
    let manifest_chunk_count =
        i64_required(manifest_row, 1, "chunk_count", "binary CAS read manifest")?;
    if manifest_size_bytes < 0 || manifest_chunk_count < 0 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "binary CAS read: invalid negative manifest values for blob hash '{}'",
                blob_hash
            ),
        });
    }

    let chunk_rows = backend
        .execute(
            &format!(
                "SELECT mc.chunk_index, mc.chunk_hash, mc.chunk_size, cs.data, cs.codec \
                 FROM {manifest_chunk} mc \
                 LEFT JOIN {chunk_store} cs ON cs.chunk_hash = mc.chunk_hash \
                 WHERE mc.blob_hash = $1 \
                 ORDER BY mc.chunk_index ASC",
                manifest_chunk = INTERNAL_BINARY_BLOB_MANIFEST_CHUNK,
                chunk_store = INTERNAL_BINARY_CHUNK_STORE,
            ),
            &[Value::Text(blob_hash.to_string())],
        )
        .await?;

    let expected_chunk_count = usize::try_from(manifest_chunk_count).map_err(|_| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "binary CAS read: chunk count out of range for blob hash '{}'",
            blob_hash
        ),
    })?;
    if chunk_rows.rows.len() != expected_chunk_count {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "binary CAS read: chunk manifest mismatch for blob hash '{}': expected {} chunks, got {}",
                blob_hash,
                expected_chunk_count,
                chunk_rows.rows.len()
            ),
        });
    }

    let mut reconstructed = Vec::with_capacity(usize::try_from(manifest_size_bytes).unwrap_or(0));
    for (expected_index, row) in chunk_rows.rows.iter().enumerate() {
        let chunk_index = i64_required(row, 0, "chunk_index", "binary CAS read chunk row")?;
        let chunk_hash = text_required(row, 1, "chunk_hash", "binary CAS read chunk row")?;
        let chunk_size = i64_required(row, 2, "chunk_size", "binary CAS read chunk row")?;
        if chunk_index != expected_index as i64 {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "binary CAS read: unexpected chunk order for blob hash '{}': expected index {}, got {}",
                    blob_hash, expected_index, chunk_index
                ),
            });
        }
        if chunk_size < 0 {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "binary CAS read: invalid negative chunk size for blob hash '{}' chunk '{}'",
                    blob_hash, chunk_hash
                ),
            });
        }
        let chunk_data =
            blob_required(row, 3, "data", "binary CAS read chunk row").map_err(|_| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "binary CAS read: missing chunk payload for blob hash '{}' chunk '{}'",
                    blob_hash, chunk_hash
                ),
            })?;
        let codec = nullable_text(row, 4, "codec", "binary CAS read chunk row")?;
        let expected_chunk_size = usize::try_from(chunk_size).map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "binary CAS read: chunk size out of range for blob hash '{}' chunk '{}': {}",
                blob_hash, chunk_hash, chunk_size
            ),
        })?;
        let decoded_chunk_data = decode_binary_chunk_payload(
            &chunk_data,
            codec.as_deref(),
            expected_chunk_size,
            blob_hash,
            &chunk_hash,
            "binary CAS read",
        )?;
        if decoded_chunk_data.len() as i64 != chunk_size {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "binary CAS read: chunk size mismatch for blob hash '{}' chunk '{}': expected {}, got {}",
                    blob_hash,
                    chunk_hash,
                    chunk_size,
                    decoded_chunk_data.len()
                ),
            });
        }
        reconstructed.extend_from_slice(&decoded_chunk_data);
    }

    if reconstructed.len() as i64 != manifest_size_bytes {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "binary CAS read: reconstructed size mismatch for blob hash '{}': expected {}, got {}",
                blob_hash,
                manifest_size_bytes,
                reconstructed.len()
            ),
        });
    }

    Ok(Some(reconstructed))
}

fn text_required(
    row: &[Value],
    index: usize,
    column: &str,
    context: &str,
) -> Result<String, LixError> {
    let Some(value) = row.get(index) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("{context}: row missing column '{column}' at index {index}"),
        });
    };
    match value {
        Value::Text(text) => Ok(text.clone()),
        other => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "{context}: expected text column '{column}' at index {index}, got {other:?}"
            ),
        }),
    }
}

fn nullable_text(
    row: &[Value],
    index: usize,
    column: &str,
    context: &str,
) -> Result<Option<String>, LixError> {
    let Some(value) = row.get(index) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("{context}: row missing column '{column}' at index {index}"),
        });
    };
    match value {
        Value::Null => Ok(None),
        Value::Text(text) => Ok(Some(text.clone())),
        other => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "{context}: expected nullable text column '{column}' at index {index}, got {other:?}"
            ),
        }),
    }
}

fn i64_required(row: &[Value], index: usize, column: &str, context: &str) -> Result<i64, LixError> {
    let Some(value) = row.get(index) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("{context}: row missing column '{column}' at index {index}"),
        });
    };
    match value {
        Value::Integer(number) => Ok(*number),
        other => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "{context}: expected integer column '{column}' at index {index}, got {other:?}"
            ),
        }),
    }
}

fn blob_required(
    row: &[Value],
    index: usize,
    column: &str,
    context: &str,
) -> Result<Vec<u8>, LixError> {
    let Some(value) = row.get(index) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("{context}: row missing column '{column}' at index {index}"),
        });
    };
    match value {
        Value::Blob(bytes) => Ok(bytes.clone()),
        other => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "{context}: expected blob column '{column}' at index {index}, got {other:?}"
            ),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::blob_required;
    use crate::Value;

    #[test]
    fn blob_required_rejects_text_values() {
        let err = blob_required(
            &[Value::Text("hello".to_string())],
            0,
            "data",
            "binary CAS read test",
        )
        .expect_err("text should not be accepted as blob data");

        assert!(
            err.description
                .contains("expected blob column 'data' at index 0"),
            "unexpected error: {}",
            err.description
        );
    }
}
