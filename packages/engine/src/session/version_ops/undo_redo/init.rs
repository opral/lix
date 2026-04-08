use crate::backend::ddl::execute_ddl_batch;
use crate::{LixBackend, LixError};

pub(crate) async fn init(backend: &dyn LixBackend) -> Result<(), LixError> {
    let statements = [
        format!(
            "CREATE TABLE IF NOT EXISTS {} (\
             version_id TEXT NOT NULL,\
             operation_commit_id TEXT PRIMARY KEY,\
             operation_kind TEXT NOT NULL,\
             target_commit_id TEXT NOT NULL,\
             created_at TEXT NOT NULL\
             )",
            super::UNDO_REDO_OPERATION_TABLE
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS idx_lix_internal_undo_redo_operation_version_created \
             ON {} (version_id, created_at)",
            super::UNDO_REDO_OPERATION_TABLE
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS idx_lix_internal_undo_redo_operation_target \
             ON {} (target_commit_id)",
            super::UNDO_REDO_OPERATION_TABLE
        ),
    ];
    let statement_refs = statements.iter().map(String::as_str).collect::<Vec<_>>();
    execute_ddl_batch(backend, "version.undo_redo", &statement_refs).await
}
