use crate::backend::execute_ddl_batch;
use crate::{LixBackend, LixError};

const CANONICAL_INIT_STATEMENTS: &[&str] = &[
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
     file_id TEXT NOT NULL,\
     plugin_key TEXT NOT NULL,\
     snapshot_id TEXT NOT NULL,\
     metadata TEXT,\
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
     file_id TEXT NOT NULL,\
     from_depth BIGINT NOT NULL,\
     plugin_key TEXT NOT NULL,\
     schema_version TEXT NOT NULL,\
     metadata TEXT,\
     snapshot_id TEXT NOT NULL,\
     change_id TEXT NOT NULL,\
     PRIMARY KEY (root_commit_id, entity_id, schema_key, file_id, from_depth)\
     )",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_entity_state_timeline_breakpoint_root_depth \
     ON lix_internal_entity_state_timeline_breakpoint (root_commit_id, from_depth)",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_entity_state_timeline_breakpoint_lookup \
     ON lix_internal_entity_state_timeline_breakpoint (root_commit_id, entity_id, file_id, schema_key, from_depth)",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_entity_state_timeline_breakpoint_filters \
     ON lix_internal_entity_state_timeline_breakpoint (root_commit_id, file_id, plugin_key, schema_key, entity_id, from_depth)",
    "CREATE TABLE IF NOT EXISTS lix_internal_timeline_status (\
     root_commit_id TEXT PRIMARY KEY,\
     built_max_depth BIGINT NOT NULL,\
     built_at TEXT NOT NULL\
     )",
];

pub(crate) async fn init(backend: &dyn LixBackend) -> Result<(), LixError> {
    execute_ddl_batch(backend, "canonical", CANONICAL_INIT_STATEMENTS).await
}
