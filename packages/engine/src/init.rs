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
     file_id TEXT NOT NULL,\
     version_id TEXT NOT NULL,\
     plugin_key TEXT NOT NULL,\
     snapshot_content TEXT,\
     change_id TEXT NOT NULL,\
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
     snapshot_content TEXT,\
     PRIMARY KEY (entity_id, schema_key, file_id, version_id)\
     )",
];

pub async fn init_backend(backend: &dyn LixBackend) -> Result<(), LixError> {
    for statement in INIT_STATEMENTS {
        backend.execute(statement, &[]).await?;
    }
    Ok(())
}
