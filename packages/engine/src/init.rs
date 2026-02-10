use crate::LixBackend;
use crate::LixError;

const INIT_STATEMENTS: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS lix_internal_snapshot (\
     id TEXT PRIMARY KEY,\
     content TEXT\
     )",
    "INSERT INTO lix_internal_snapshot (id, content) VALUES ('no-content', NULL) \
     ON CONFLICT (id) DO NOTHING",
    "CREATE TABLE IF NOT EXISTS lix_internal_change (\
     id TEXT PRIMARY KEY,\
     entity_id TEXT NOT NULL,\
     schema_key TEXT NOT NULL,\
     schema_version TEXT NOT NULL,\
     file_id TEXT NOT NULL,\
     plugin_key TEXT NOT NULL,\
     snapshot_id TEXT NOT NULL,\
     metadata TEXT,\
     created_at TEXT NOT NULL\
     )",
    "CREATE TABLE IF NOT EXISTS lix_internal_state_materialized_v1_lix_stored_schema (\
     entity_id TEXT NOT NULL,\
     schema_key TEXT NOT NULL,\
     schema_version TEXT NOT NULL,\
     file_id TEXT NOT NULL,\
     version_id TEXT NOT NULL,\
     plugin_key TEXT NOT NULL,\
     snapshot_content TEXT,\
     inherited_from_version_id TEXT,\
     change_id TEXT NOT NULL,\
     metadata TEXT,\
     is_tombstone INTEGER NOT NULL DEFAULT 0,\
     created_at TEXT NOT NULL,\
     updated_at TEXT NOT NULL,\
     PRIMARY KEY (entity_id, file_id, version_id)\
     )",
    "CREATE TABLE IF NOT EXISTS lix_internal_state_untracked (\
     entity_id TEXT NOT NULL,\
     schema_key TEXT NOT NULL,\
     file_id TEXT NOT NULL,\
     version_id TEXT NOT NULL,\
     plugin_key TEXT NOT NULL,\
     snapshot_content TEXT,\
     metadata TEXT,\
     schema_version TEXT NOT NULL,\
     created_at TEXT NOT NULL,\
     updated_at TEXT NOT NULL,\
     PRIMARY KEY (entity_id, schema_key, file_id, version_id)\
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
     writer_key TEXT,\
     PRIMARY KEY (file_id, version_id)\
     )",
    "CREATE INDEX IF NOT EXISTS idx_file_lixcol_cache_lookup \
     ON lix_internal_file_lixcol_cache (file_id, version_id)",
    "CREATE TABLE IF NOT EXISTS lix_internal_plugin (\
     key TEXT PRIMARY KEY,\
     runtime TEXT NOT NULL,\
     api_version TEXT NOT NULL,\
     detect_changes_glob TEXT NOT NULL,\
     entry TEXT NOT NULL,\
     manifest_json TEXT NOT NULL,\
     wasm BYTEA NOT NULL,\
     created_at TEXT NOT NULL,\
     updated_at TEXT NOT NULL\
     )",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_plugin_runtime \
     ON lix_internal_plugin (runtime)",
];

pub async fn init_backend(backend: &dyn LixBackend) -> Result<(), LixError> {
    for statement in INIT_STATEMENTS {
        backend.execute(statement, &[]).await?;
    }
    Ok(())
}
