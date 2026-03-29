use crate::init::tables::execute_init_statements;
use crate::live_state::lifecycle;
use crate::live_state::register_schema;
use crate::live_state::schema_access::{snapshot_select_expr_for_schema, tracked_relation_name};
use crate::{LixBackend, LixError};

const LIVE_STATE_INIT_STATEMENTS: &[&str] = &[
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
     change_ordinal BIGINT NOT NULL,\
     entity_id TEXT NOT NULL,\
     schema_key TEXT NOT NULL,\
     schema_version TEXT NOT NULL,\
     file_id TEXT NOT NULL,\
     plugin_key TEXT NOT NULL,\
     snapshot_id TEXT NOT NULL,\
     metadata TEXT,\
     created_at TEXT NOT NULL\
     )",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_change_ordinal \
     ON lix_internal_change (change_ordinal)",
    "CREATE TABLE IF NOT EXISTS lix_internal_commit_idempotency (\
     write_lane TEXT NOT NULL,\
     idempotency_key TEXT NOT NULL,\
     idempotency_kind TEXT NOT NULL,\
     idempotency_value TEXT NOT NULL,\
     parent_head_snapshot_content TEXT NOT NULL,\
     commit_id TEXT NOT NULL,\
     created_at TEXT NOT NULL,\
     PRIMARY KEY (write_lane, idempotency_kind, idempotency_value, parent_head_snapshot_content)\
     )",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_commit_idempotency_commit_id \
     ON lix_internal_commit_idempotency (commit_id)",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_commit_idempotency_legacy \
     ON lix_internal_commit_idempotency (write_lane, idempotency_key)",
    "CREATE TABLE IF NOT EXISTS lix_internal_registered_schema_bootstrap (\
     entity_id TEXT NOT NULL,\
     schema_key TEXT NOT NULL,\
     schema_version TEXT NOT NULL,\
     file_id TEXT NOT NULL,\
     version_id TEXT NOT NULL,\
     global BOOLEAN NOT NULL DEFAULT false,\
     plugin_key TEXT NOT NULL,\
     snapshot_content TEXT,\
     change_id TEXT NOT NULL,\
     metadata TEXT,\
     writer_key TEXT,\
     is_tombstone INTEGER NOT NULL DEFAULT 0,\
     untracked BOOLEAN NOT NULL DEFAULT false,\
     created_at TEXT NOT NULL,\
     updated_at TEXT NOT NULL,\
     PRIMARY KEY (entity_id, file_id, version_id, untracked)\
     )",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_registered_schema_bootstrap_version_id \
     ON lix_internal_registered_schema_bootstrap (version_id)",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_registered_schema_bootstrap_global_version \
     ON lix_internal_registered_schema_bootstrap (global, version_id)",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_registered_schema_bootstrap_vfe \
     ON lix_internal_registered_schema_bootstrap (version_id, file_id, entity_id)",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_registered_schema_bootstrap_live_vfe \
     ON lix_internal_registered_schema_bootstrap (version_id, file_id, entity_id) \
     WHERE is_tombstone = 0 AND snapshot_content IS NOT NULL",
];

pub async fn init(backend: &dyn LixBackend) -> Result<(), LixError> {
    lifecycle::init(backend).await?;
    execute_init_statements(backend, "live_state", LIVE_STATE_INIT_STATEMENTS).await?;
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
                 entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, snapshot_content, change_id, metadata, writer_key, is_tombstone, untracked, created_at, updated_at\
                 ) \
                 SELECT m.entity_id, m.schema_key, m.schema_version, m.file_id, m.version_id, m.global, m.plugin_key, {snapshot_expr}, COALESCE(m.change_id, ''), m.metadata, m.writer_key, m.is_tombstone, m.untracked, m.created_at, m.updated_at \
                 FROM {registered_schema_table} m \
                 WHERE NOT EXISTS (\
                   SELECT 1 \
                   FROM lix_internal_registered_schema_bootstrap b \
                   WHERE b.entity_id = m.entity_id \
                     AND b.file_id = m.file_id \
                     AND b.version_id = m.version_id \
                     AND b.untracked = m.untracked\
                 )",
            ),
            &[],
        )
        .await?;
    Ok(())
}
