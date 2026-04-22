#![allow(dead_code)]

//! SQL-backed lower seam for binary CAS persistence.
//!
//! This module owns the raw backend / transaction SQL lowering for binary CAS
//! so the rest of `binary_cas/*` can compile against owner-local storage
//! operations instead of the shared `persistence/*` root.

use async_trait::async_trait;

use crate::binary_cas::store::{
    BinaryCasBackendRef, BinaryCasReadStore, BinaryCasTransactionRef, BinaryCasWriteStore,
};
use crate::binary_cas::BinaryBlobWrite;
use crate::{LixError, Value};

use crate::binary_cas::schema::{
    INTERNAL_BINARY_BLOB_MANIFEST, INTERNAL_BINARY_BLOB_MANIFEST_CHUNK, INTERNAL_BINARY_BLOB_STORE,
    INTERNAL_BINARY_CHUNK_STORE, INTERNAL_BINARY_FILE_VERSION_REF,
};
use crate::binary_cas::{schema, write};
use crate::catalog::state_by_version_relation_name;
use crate::SqlDialect;

pub(crate) struct SqlBinaryCasReadStore<'a> {
    backend: BinaryCasBackendRef<'a>,
}

impl<'a> SqlBinaryCasReadStore<'a> {
    pub(crate) fn new(backend: BinaryCasBackendRef<'a>) -> Self {
        Self { backend }
    }
}

pub(crate) struct SqlBinaryCasWriteStore<'a> {
    transaction: BinaryCasTransactionRef<'a>,
}

impl<'a> SqlBinaryCasWriteStore<'a> {
    pub(crate) fn new(transaction: BinaryCasTransactionRef<'a>) -> Self {
        Self { transaction }
    }
}

async fn execute_ddl_batch_with_backend(
    backend: BinaryCasBackendRef<'_>,
    batch_name: &str,
    statements: &[&str],
) -> Result<(), LixError> {
    crate::backend::execute_ddl_batch(backend, batch_name, statements).await
}

async fn add_column_if_missing_with_backend(
    backend: BinaryCasBackendRef<'_>,
    table_name: &str,
    column_name: &str,
    column_sql: &str,
) -> Result<(), LixError> {
    crate::backend::add_column_if_missing(backend, table_name, column_name, column_sql).await
}

pub(crate) async fn init_storage(backend: BinaryCasBackendRef<'_>) -> Result<(), LixError> {
    const BINARY_CAS_INIT_STATEMENTS: &[&str] = &[
        "CREATE TABLE IF NOT EXISTS lix_internal_binary_blob_store (\
         blob_hash TEXT PRIMARY KEY,\
         data BYTEA NOT NULL,\
         size_bytes BIGINT NOT NULL,\
         created_at TEXT NOT NULL\
         )",
        "CREATE TABLE IF NOT EXISTS lix_internal_binary_blob_manifest (\
         blob_hash TEXT PRIMARY KEY,\
         size_bytes BIGINT NOT NULL,\
         chunk_count BIGINT NOT NULL,\
         created_at TEXT NOT NULL\
         )",
        "CREATE TABLE IF NOT EXISTS lix_internal_binary_chunk_store (\
         chunk_hash TEXT PRIMARY KEY,\
         data BYTEA NOT NULL,\
         size_bytes BIGINT NOT NULL,\
         codec TEXT NOT NULL DEFAULT 'raw',\
         codec_dict_id TEXT,\
         created_at TEXT NOT NULL\
         )",
        "CREATE TABLE IF NOT EXISTS lix_internal_binary_blob_manifest_chunk (\
         blob_hash TEXT NOT NULL,\
         chunk_index BIGINT NOT NULL,\
         chunk_hash TEXT NOT NULL,\
         chunk_size BIGINT NOT NULL,\
         PRIMARY KEY (blob_hash, chunk_index),\
         FOREIGN KEY (blob_hash) REFERENCES lix_internal_binary_blob_manifest (blob_hash) ON DELETE RESTRICT,\
         FOREIGN KEY (chunk_hash) REFERENCES lix_internal_binary_chunk_store (chunk_hash) ON DELETE RESTRICT\
         )",
        "CREATE INDEX IF NOT EXISTS idx_lix_internal_binary_blob_manifest_chunk_hash \
         ON lix_internal_binary_blob_manifest_chunk (chunk_hash)",
        "CREATE INDEX IF NOT EXISTS idx_lix_internal_binary_blob_manifest_chunk_blob_hash \
         ON lix_internal_binary_blob_manifest_chunk (blob_hash)",
        "CREATE TABLE IF NOT EXISTS lix_internal_binary_file_version_ref (\
         file_id TEXT NOT NULL,\
         version_id TEXT NOT NULL,\
         blob_hash TEXT NOT NULL,\
         size_bytes BIGINT NOT NULL,\
         updated_at TEXT NOT NULL,\
         PRIMARY KEY (file_id, version_id),\
         FOREIGN KEY (blob_hash) REFERENCES lix_internal_binary_blob_manifest (blob_hash) ON DELETE RESTRICT\
         )",
        "CREATE INDEX IF NOT EXISTS idx_lix_internal_binary_file_version_ref_blob_hash \
         ON lix_internal_binary_file_version_ref (blob_hash)",
        "CREATE INDEX IF NOT EXISTS idx_lix_internal_binary_file_version_ref_version_id \
         ON lix_internal_binary_file_version_ref (version_id)",
    ];

    execute_ddl_batch_with_backend(backend, "binary_cas", BINARY_CAS_INIT_STATEMENTS).await?;
    add_column_if_missing_with_backend(
        backend,
        INTERNAL_BINARY_CHUNK_STORE,
        "codec",
        "TEXT NOT NULL DEFAULT 'raw'",
    )
    .await?;
    add_column_if_missing_with_backend(
        backend,
        INTERNAL_BINARY_CHUNK_STORE,
        "codec_dict_id",
        "TEXT",
    )
    .await?;
    Ok(())
}

pub(crate) fn chunk_store_relation_name() -> &'static str {
    schema::INTERNAL_BINARY_CHUNK_STORE
}

pub(crate) async fn blob_exists(
    backend: BinaryCasBackendRef<'_>,
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

pub(crate) async fn load_blob_data_by_hash(
    backend: BinaryCasBackendRef<'_>,
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
            hint: None,
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
        hint: None,
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
            hint: None,
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
                hint: None,
            });
        }
        if chunk_size < 0 {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "binary CAS read: invalid negative chunk size for blob hash '{}' chunk '{}'",
                    blob_hash, chunk_hash
                ),
                hint: None,
            });
        }
        let chunk_data =
            blob_required(row, 3, "data", "binary CAS read chunk row").map_err(|_| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "binary CAS read: missing chunk payload for blob hash '{}' chunk '{}'",
                    blob_hash, chunk_hash
                ),
                hint: None,
            })?;
        let codec = nullable_text(row, 4, "codec", "binary CAS read chunk row")?;
        let expected_chunk_size = usize::try_from(chunk_size).map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "binary CAS read: chunk size out of range for blob hash '{}' chunk '{}': {}",
                blob_hash, chunk_hash, chunk_size
            ),
            hint: None,
        })?;
        let decoded_chunk_data = crate::binary_cas::decode_binary_chunk_payload(
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
                hint: None,
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
            hint: None,
        });
    }

    Ok(Some(reconstructed))
}

pub(crate) async fn garbage_collect_unreachable_in_transaction(
    transaction: BinaryCasTransactionRef<'_>,
) -> Result<(), LixError> {
    if !state_by_version_relation_exists_in_transaction(transaction).await? {
        return Ok(());
    }

    let state_blob_hash_expr = state_blob_hash_extract_expr_sql(transaction.dialect());
    let delete_unreferenced_file_ref_sql =
        delete_unreferenced_binary_file_version_ref_sql(state_blob_hash_expr);
    let delete_unreferenced_manifest_chunk_sql =
        delete_unreferenced_binary_blob_manifest_chunk_sql(state_blob_hash_expr);
    let delete_unreferenced_chunk_store_sql = delete_unreferenced_binary_chunk_store_sql();
    let delete_unreferenced_manifest_sql =
        delete_unreferenced_binary_blob_manifest_sql(state_blob_hash_expr);
    let delete_unreferenced_blob_store_sql = delete_unreferenced_binary_blob_store_sql();

    transaction
        .execute(&delete_unreferenced_file_ref_sql, &[])
        .await?;
    transaction
        .execute(&delete_unreferenced_manifest_chunk_sql, &[])
        .await?;
    transaction
        .execute(&delete_unreferenced_chunk_store_sql, &[])
        .await?;
    transaction
        .execute(&delete_unreferenced_manifest_sql, &[])
        .await?;
    transaction
        .execute(&delete_unreferenced_blob_store_sql, &[])
        .await?;

    Ok(())
}

#[async_trait(?Send)]
impl BinaryCasReadStore for SqlBinaryCasReadStore<'_> {
    async fn blob_exists(&self, blob_hash: &str) -> Result<bool, LixError> {
        blob_exists(self.backend, blob_hash).await
    }

    async fn load_blob_data_by_hash(&self, blob_hash: &str) -> Result<Option<Vec<u8>>, LixError> {
        load_blob_data_by_hash(self.backend, blob_hash).await
    }
}

#[async_trait(?Send)]
impl BinaryCasWriteStore for SqlBinaryCasWriteStore<'_> {
    async fn persist_blob_writes(
        &mut self,
        writes: &[BinaryBlobWrite<'_>],
    ) -> Result<(), LixError> {
        write::persist_blob_writes_in_transaction(self.transaction, writes).await
    }

    async fn garbage_collect_unreachable(&mut self) -> Result<(), LixError> {
        garbage_collect_unreachable_in_transaction(self.transaction).await
    }
}

async fn state_by_version_relation_exists_in_transaction(
    transaction: BinaryCasTransactionRef<'_>,
) -> Result<bool, LixError> {
    match transaction.dialect() {
        SqlDialect::Sqlite => {
            let result = transaction
                .execute(
                    "SELECT 1 \
                     FROM sqlite_master \
                     WHERE name = $1 \
                       AND type IN ('table', 'view') \
                     LIMIT 1",
                    &[Value::Text(state_by_version_relation_name().to_string())],
                )
                .await?;
            Ok(!result.rows.is_empty())
        }
        SqlDialect::Postgres => {
            let result = transaction
                .execute(
                    "SELECT 1 \
                     FROM pg_catalog.pg_class c \
                     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                     WHERE n.nspname = current_schema() \
                       AND c.relname = $1 \
                     LIMIT 1",
                    &[Value::Text(state_by_version_relation_name().to_string())],
                )
                .await?;
            Ok(!result.rows.is_empty())
        }
    }
}

fn state_blob_hash_extract_expr_sql(dialect: SqlDialect) -> &'static str {
    match dialect {
        SqlDialect::Sqlite => "json_extract(snapshot_content, '$.blob_hash')",
        SqlDialect::Postgres => "(snapshot_content::jsonb ->> 'blob_hash')",
    }
}

fn delete_unreferenced_binary_file_version_ref_sql(state_blob_hash_expr: &str) -> String {
    format!(
        "WITH referenced AS (\
             SELECT file_id, version_id, {state_blob_hash_expr} AS blob_hash \
             FROM {} \
             WHERE schema_key = 'lix_binary_blob_ref' \
               AND snapshot_content IS NOT NULL \
               AND {state_blob_hash_expr} IS NOT NULL\
         ) \
         DELETE FROM {} \
         WHERE NOT EXISTS (\
             SELECT 1 \
             FROM referenced r \
             WHERE r.file_id = {}.file_id \
               AND r.version_id = {}.version_id \
               AND r.blob_hash = {}.blob_hash\
        )",
        state_by_version_relation_name(),
        INTERNAL_BINARY_FILE_VERSION_REF,
        INTERNAL_BINARY_FILE_VERSION_REF,
        INTERNAL_BINARY_FILE_VERSION_REF,
        INTERNAL_BINARY_FILE_VERSION_REF,
    )
}

fn delete_unreferenced_binary_blob_manifest_chunk_sql(state_blob_hash_expr: &str) -> String {
    format!(
        "WITH referenced AS (\
             SELECT DISTINCT {state_blob_hash_expr} AS blob_hash \
             FROM {} \
             WHERE schema_key = 'lix_binary_blob_ref' \
               AND snapshot_content IS NOT NULL \
               AND {state_blob_hash_expr} IS NOT NULL\
         ) \
         DELETE FROM {} \
         WHERE NOT EXISTS (\
             SELECT 1 \
             FROM referenced r \
             WHERE r.blob_hash = {}.blob_hash\
        )",
        state_by_version_relation_name(),
        INTERNAL_BINARY_BLOB_MANIFEST_CHUNK,
        INTERNAL_BINARY_BLOB_MANIFEST_CHUNK,
    )
}

fn delete_unreferenced_binary_chunk_store_sql() -> String {
    format!(
        "DELETE FROM {} \
         WHERE NOT EXISTS (\
             SELECT 1 \
             FROM {} mc \
             WHERE mc.chunk_hash = {}.chunk_hash\
         )",
        INTERNAL_BINARY_CHUNK_STORE,
        INTERNAL_BINARY_BLOB_MANIFEST_CHUNK,
        INTERNAL_BINARY_CHUNK_STORE,
    )
}

fn delete_unreferenced_binary_blob_manifest_sql(state_blob_hash_expr: &str) -> String {
    format!(
        "WITH referenced AS (\
             SELECT DISTINCT {state_blob_hash_expr} AS blob_hash \
             FROM {} \
             WHERE schema_key = 'lix_binary_blob_ref' \
               AND snapshot_content IS NOT NULL \
               AND {state_blob_hash_expr} IS NOT NULL\
         ) \
         DELETE FROM {} \
         WHERE NOT EXISTS (\
             SELECT 1 \
             FROM referenced r \
             WHERE r.blob_hash = {}.blob_hash\
         ) \
         AND NOT EXISTS (\
             SELECT 1 \
             FROM {} mc \
             WHERE mc.blob_hash = {}.blob_hash\
        )",
        state_by_version_relation_name(),
        INTERNAL_BINARY_BLOB_MANIFEST,
        INTERNAL_BINARY_BLOB_MANIFEST,
        INTERNAL_BINARY_BLOB_MANIFEST_CHUNK,
        INTERNAL_BINARY_BLOB_MANIFEST,
    )
}

fn delete_unreferenced_binary_blob_store_sql() -> String {
    format!(
        "DELETE FROM {} \
         WHERE NOT EXISTS (\
             SELECT 1 \
             FROM {} r \
             WHERE r.blob_hash = {}.blob_hash\
         )",
        INTERNAL_BINARY_BLOB_STORE, INTERNAL_BINARY_FILE_VERSION_REF, INTERNAL_BINARY_BLOB_STORE,
    )
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
            hint: None,
        });
    };
    match value {
        Value::Text(text) => Ok(text.clone()),
        other => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "{context}: expected text column '{column}' at index {index}, got {other:?}"
            ),
            hint: None,
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
            hint: None,
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
            hint: None,
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
            hint: None,
        });
    };
    match value {
        Value::Blob(bytes) => Ok(bytes.clone()),
        other => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "{context}: expected blob column '{column}' at index {index}, got {other:?}"
            ),
            hint: None,
        }),
    }
}

fn i64_required(row: &[Value], index: usize, column: &str, context: &str) -> Result<i64, LixError> {
    let Some(value) = row.get(index) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("{context}: row missing column '{column}' at index {index}"),
            hint: None,
        });
    };
    match value {
        Value::Integer(number) => Ok(*number),
        other => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "{context}: expected integer column '{column}' at index {index}, got {other:?}"
            ),
            hint: None,
        }),
    }
}
