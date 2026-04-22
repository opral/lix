use crate::common::{storage_scope_key_for_file_id, STORAGE_SCOPE_KEY_COLUMN};
use crate::live_state::lifecycle;
use crate::live_state::register_schema;
use crate::live_state::store::LiveStateBackendRef;
use crate::live_state::store_sql::{
    add_column_if_missing_with_backend, execute_ddl_batch_with_backend, SqlLiveStateStore,
};
use crate::LixError;

const LIVE_STATE_CREATE_TABLE_STATEMENTS: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS lix_internal_registered_schema_bootstrap (\
     entity_id TEXT NOT NULL,\
     schema_key TEXT NOT NULL,\
     schema_version TEXT NOT NULL,\
     file_id TEXT,\
     storage_scope_key TEXT NOT NULL,\
     version_id TEXT NOT NULL,\
     global BOOLEAN NOT NULL DEFAULT false,\
     plugin_key TEXT,\
     snapshot_content TEXT,\
     change_id TEXT NOT NULL,\
     metadata TEXT,\
     is_tombstone INTEGER NOT NULL DEFAULT 0,\
     untracked BOOLEAN NOT NULL DEFAULT false,\
     created_at TEXT NOT NULL,\
     updated_at TEXT NOT NULL,\
     PRIMARY KEY (entity_id, storage_scope_key, version_id, untracked)\
     )",
    "CREATE TABLE IF NOT EXISTS lix_internal_file_data_cache (\
     file_id TEXT NOT NULL,\
     version_id TEXT NOT NULL,\
     data BYTEA NOT NULL,\
     PRIMARY KEY (file_id, version_id)\
     )",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_file_data_cache_version_id \
     ON lix_internal_file_data_cache (version_id)",
    "CREATE TABLE IF NOT EXISTS lix_internal_file_path_cache (\
     file_id TEXT NOT NULL,\
     version_id TEXT NOT NULL,\
     directory_id TEXT,\
     name TEXT NOT NULL,\
     extension TEXT,\
     path TEXT NOT NULL,\
     PRIMARY KEY (file_id, version_id)\
     )",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_file_path_cache_version_path \
     ON lix_internal_file_path_cache (version_id, path, file_id)",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_file_path_cache_version_directory \
     ON lix_internal_file_path_cache (version_id, directory_id)",
    "CREATE TABLE IF NOT EXISTS lix_internal_file_lixcol_cache (\
     file_id TEXT NOT NULL,\
     version_id TEXT NOT NULL,\
     latest_change_id TEXT,\
     latest_commit_id TEXT,\
     created_at TEXT,\
     updated_at TEXT,\
     PRIMARY KEY (file_id, version_id)\
     )",
];

const LIVE_STATE_INDEX_STATEMENTS: &[&str] = &[
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_lix_internal_registered_schema_bootstrap_scope_identity \
     ON lix_internal_registered_schema_bootstrap (entity_id, storage_scope_key, version_id, untracked)",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_registered_schema_bootstrap_version_id \
     ON lix_internal_registered_schema_bootstrap (version_id)",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_registered_schema_bootstrap_global_version \
     ON lix_internal_registered_schema_bootstrap (global, version_id)",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_registered_schema_bootstrap_vfe \
     ON lix_internal_registered_schema_bootstrap (version_id, file_id, entity_id)",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_registered_schema_bootstrap_vse \
     ON lix_internal_registered_schema_bootstrap (version_id, storage_scope_key, entity_id)",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_registered_schema_bootstrap_live_vfe \
     ON lix_internal_registered_schema_bootstrap (version_id, file_id, entity_id) \
     WHERE is_tombstone = 0 AND snapshot_content IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_registered_schema_bootstrap_live_vse \
     ON lix_internal_registered_schema_bootstrap (version_id, storage_scope_key, entity_id) \
     WHERE is_tombstone = 0 AND snapshot_content IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_file_lixcol_cache_lookup \
     ON lix_internal_file_lixcol_cache (file_id, version_id)",
];

pub async fn init(backend: LiveStateBackendRef<'_>) -> Result<(), LixError> {
    lifecycle::init(&SqlLiveStateStore::from_backend(backend)).await?;
    execute_ddl_batch_with_backend(
        backend,
        "live_state.tables",
        LIVE_STATE_CREATE_TABLE_STATEMENTS,
    )
    .await?;
    ensure_internal_storage_scope_keys(backend).await?;
    execute_ddl_batch_with_backend(backend, "live_state.indexes", LIVE_STATE_INDEX_STATEMENTS)
        .await?;
    register_schema(backend, "lix_registered_schema").await?;
    Ok(())
}

async fn ensure_internal_storage_scope_keys(
    backend: LiveStateBackendRef<'_>,
) -> Result<(), LixError> {
    add_column_if_missing_with_backend(
        backend,
        "lix_internal_registered_schema_bootstrap",
        STORAGE_SCOPE_KEY_COLUMN,
        &format!(
            "TEXT NOT NULL DEFAULT '{}'",
            storage_scope_key_for_file_id(None)
        ),
    )
    .await?;
    crate::live_state::store_sql::execute_query_with_backend(
        backend,
        &format!(
            "UPDATE lix_internal_registered_schema_bootstrap \
             SET {storage_scope_key} = CASE \
               WHEN file_id IS NULL THEN '{engine_scope}' \
               ELSE 'file:' || file_id \
             END",
            storage_scope_key = STORAGE_SCOPE_KEY_COLUMN,
            engine_scope = storage_scope_key_for_file_id(None),
        ),
        &[],
    )
    .await?;

    Ok(())
}
