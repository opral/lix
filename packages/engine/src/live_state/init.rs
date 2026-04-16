use crate::backend::{add_column_if_missing, execute_ddl_batch};
use crate::common::{storage_scope_key_for_file_id, STORAGE_SCOPE_KEY_COLUMN};
use crate::live_state::lifecycle;
use crate::live_state::register_schema;
use crate::live_state::schema_access::{snapshot_select_expr_for_schema, tracked_relation_name};
use crate::{LixBackend, LixError};

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

pub async fn init(backend: &dyn LixBackend) -> Result<(), LixError> {
    lifecycle::init(backend).await?;
    execute_ddl_batch(
        backend,
        "live_state.tables",
        LIVE_STATE_CREATE_TABLE_STATEMENTS,
    )
    .await?;
    ensure_internal_storage_scope_keys(backend).await?;
    execute_ddl_batch(backend, "live_state.indexes", LIVE_STATE_INDEX_STATEMENTS).await?;
    register_schema(backend, "lix_registered_schema").await?;
    seed_registered_schema_bootstrap_rows(backend).await?;
    Ok(())
}

async fn seed_registered_schema_bootstrap_rows(backend: &dyn LixBackend) -> Result<(), LixError> {
    let registered_schema_table = tracked_relation_name("lix_registered_schema");
    let snapshot_expr = snapshot_select_expr_for_schema(
        "lix_registered_schema",
        None,
        backend.dialect(),
        Some("m"),
    )?;
    backend
        .execute(
            &format!(
                "INSERT INTO lix_internal_registered_schema_bootstrap (\
                 entity_id, schema_key, schema_version, file_id, storage_scope_key, version_id, global, plugin_key, snapshot_content, change_id, metadata, is_tombstone, untracked, created_at, updated_at\
                 ) \
                 SELECT m.entity_id, m.schema_key, m.schema_version, m.file_id, m.{storage_scope_key}, m.version_id, m.global, m.plugin_key, {snapshot_expr}, m.change_id, m.metadata, m.is_tombstone, m.untracked, m.created_at, m.updated_at \
                 FROM {registered_schema_table} m \
                 WHERE NOT EXISTS (\
                   SELECT 1 \
                   FROM lix_internal_registered_schema_bootstrap b \
                   WHERE b.entity_id = m.entity_id \
                     AND b.storage_scope_key = m.{storage_scope_key} \
                     AND b.version_id = m.version_id \
                     AND b.untracked = m.untracked\
                 )",
                storage_scope_key = STORAGE_SCOPE_KEY_COLUMN,
            ),
            &[],
        )
        .await?;
    Ok(())
}

async fn ensure_internal_storage_scope_keys(backend: &dyn LixBackend) -> Result<(), LixError> {
    add_column_if_missing(
        backend,
        "lix_internal_registered_schema_bootstrap",
        STORAGE_SCOPE_KEY_COLUMN,
        &format!(
            "TEXT NOT NULL DEFAULT '{}'",
            storage_scope_key_for_file_id(None)
        ),
    )
    .await?;
    backend
        .execute(
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
