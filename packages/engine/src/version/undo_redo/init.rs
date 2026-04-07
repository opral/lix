use crate::ddl::execute_ddl_batch;
use crate::{LixBackend, LixError};

const UNDO_REDO_INIT_STATEMENTS: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS lix_internal_undo_redo_operation (\
     version_id TEXT NOT NULL,\
     operation_commit_id TEXT PRIMARY KEY,\
     operation_kind TEXT NOT NULL,\
     target_commit_id TEXT NOT NULL,\
     created_at TEXT NOT NULL\
     )",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_undo_redo_operation_version_created \
     ON lix_internal_undo_redo_operation (version_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_undo_redo_operation_target \
     ON lix_internal_undo_redo_operation (target_commit_id)",
];

pub(crate) async fn init(backend: &dyn LixBackend) -> Result<(), LixError> {
    execute_ddl_batch(backend, "version.undo_redo", UNDO_REDO_INIT_STATEMENTS).await
}
