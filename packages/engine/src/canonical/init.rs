use crate::backend::{add_column_if_missing, execute_ddl_batch};
use crate::common::{storage_scope_key_for_file_id, STORAGE_SCOPE_KEY_COLUMN};
use crate::{LixBackend, LixError};

const CANONICAL_CREATE_TABLE_STATEMENTS: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS lix_internal_snapshot (\
     id TEXT PRIMARY KEY,\
     content TEXT\
     )",
    "INSERT INTO lix_internal_snapshot (id, content) \
     SELECT 'no-content', NULL \
     WHERE NOT EXISTS ( \
       SELECT 1 FROM lix_internal_snapshot WHERE id = 'no-content' \
     )",
    "CREATE TABLE IF NOT EXISTS lix_internal_change (\
     id TEXT PRIMARY KEY,\
     entity_id TEXT NOT NULL,\
     schema_key TEXT NOT NULL,\
     schema_version TEXT NOT NULL,\
     file_id TEXT,\
     plugin_key TEXT,\
     snapshot_id TEXT NOT NULL,\
     metadata TEXT,\
     untracked BOOLEAN NOT NULL DEFAULT false,\
     created_at TEXT NOT NULL\
     )",
    "CREATE TABLE IF NOT EXISTS lix_internal_commit_graph_node (\
     commit_id TEXT PRIMARY KEY,\
     generation BIGINT NOT NULL\
     )",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_commit_graph_node_generation \
     ON lix_internal_commit_graph_node (generation)",
    "CREATE TABLE IF NOT EXISTS lix_internal_entity_state_timeline_breakpoint (\
     root_commit_id TEXT NOT NULL,\
     entity_id TEXT NOT NULL,\
     schema_key TEXT NOT NULL,\
     file_id TEXT,\
     storage_scope_key TEXT NOT NULL,\
     from_depth BIGINT NOT NULL,\
     plugin_key TEXT,\
     schema_version TEXT NOT NULL,\
     metadata TEXT,\
     snapshot_id TEXT NOT NULL,\
     change_id TEXT NOT NULL,\
     PRIMARY KEY (root_commit_id, entity_id, schema_key, storage_scope_key, from_depth)\
     )",
    "CREATE TABLE IF NOT EXISTS lix_internal_timeline_status (\
     root_commit_id TEXT PRIMARY KEY,\
     built_max_depth BIGINT NOT NULL,\
     built_at TEXT NOT NULL\
     )",
];

const CANONICAL_INDEX_STATEMENTS: &[&str] = &[
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_lix_internal_entity_state_timeline_breakpoint_scope_identity \
     ON lix_internal_entity_state_timeline_breakpoint (root_commit_id, entity_id, schema_key, storage_scope_key, from_depth)",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_entity_state_timeline_breakpoint_root_depth \
     ON lix_internal_entity_state_timeline_breakpoint (root_commit_id, from_depth)",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_entity_state_timeline_breakpoint_lookup \
     ON lix_internal_entity_state_timeline_breakpoint (root_commit_id, entity_id, storage_scope_key, schema_key, from_depth)",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_entity_state_timeline_breakpoint_filters \
     ON lix_internal_entity_state_timeline_breakpoint (root_commit_id, file_id, plugin_key, schema_key, entity_id, from_depth)",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_entity_state_timeline_breakpoint_scope_filters \
     ON lix_internal_entity_state_timeline_breakpoint (root_commit_id, storage_scope_key, plugin_key, schema_key, entity_id, from_depth)",
];

const CHANGE_UNTRACKED_INDEX_STATEMENTS: &[&str] = &[
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_change_untracked_created_at \
     ON lix_internal_change (untracked, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_change_untracked_identity \
     ON lix_internal_change (untracked, entity_id, schema_key, file_id)",
];

pub(crate) async fn init(backend: &dyn LixBackend) -> Result<(), LixError> {
    execute_ddl_batch(
        backend,
        "canonical.tables",
        CANONICAL_CREATE_TABLE_STATEMENTS,
    )
    .await?;
    ensure_breakpoint_storage_scope_keys(backend).await?;
    execute_ddl_batch(backend, "canonical.indexes", CANONICAL_INDEX_STATEMENTS).await?;
    add_column_if_missing(
        backend,
        "lix_internal_change",
        "untracked",
        "BOOLEAN NOT NULL DEFAULT false",
    )
    .await?;
    execute_ddl_batch(
        backend,
        "canonical.change_untracked_indexes",
        CHANGE_UNTRACKED_INDEX_STATEMENTS,
    )
    .await
}

async fn ensure_breakpoint_storage_scope_keys(backend: &dyn LixBackend) -> Result<(), LixError> {
    add_column_if_missing(
        backend,
        "lix_internal_entity_state_timeline_breakpoint",
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
                "UPDATE lix_internal_entity_state_timeline_breakpoint \
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
