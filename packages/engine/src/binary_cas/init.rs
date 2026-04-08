use crate::backend::ddl::{add_column_if_missing, execute_ddl_batch};
use crate::binary_cas::schema::INTERNAL_BINARY_CHUNK_STORE;
use crate::{LixBackend, LixError};

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

pub(crate) async fn init(backend: &dyn LixBackend) -> Result<(), LixError> {
    execute_ddl_batch(backend, "binary_cas", BINARY_CAS_INIT_STATEMENTS).await?;
    add_column_if_missing(
        backend,
        INTERNAL_BINARY_CHUNK_STORE,
        "codec",
        "TEXT NOT NULL DEFAULT 'raw'",
    )
    .await?;
    add_column_if_missing(
        backend,
        INTERNAL_BINARY_CHUNK_STORE,
        "codec_dict_id",
        "TEXT",
    )
    .await?;
    Ok(())
}
