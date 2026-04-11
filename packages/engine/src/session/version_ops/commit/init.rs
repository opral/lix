use crate::backend::execute_ddl_batch;
use crate::{LixBackend, LixError};

pub(crate) async fn init(backend: &dyn LixBackend) -> Result<(), LixError> {
    let statements = [
        format!(
            "CREATE TABLE IF NOT EXISTS {} (\
             write_lane TEXT NOT NULL,\
             idempotency_key TEXT NOT NULL,\
             idempotency_kind TEXT NOT NULL,\
             idempotency_value TEXT NOT NULL,\
             parent_head_snapshot_content TEXT NOT NULL,\
             commit_id TEXT NOT NULL,\
             created_at TEXT NOT NULL,\
             PRIMARY KEY (write_lane, idempotency_kind, idempotency_value, parent_head_snapshot_content)\
             )",
            super::COMMIT_IDEMPOTENCY_TABLE
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS idx_lix_internal_commit_idempotency_commit_id \
             ON {} (commit_id)",
            super::COMMIT_IDEMPOTENCY_TABLE
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS idx_lix_internal_commit_idempotency_legacy \
             ON {} (write_lane, idempotency_key)",
            super::COMMIT_IDEMPOTENCY_TABLE
        ),
    ];
    let statement_refs = statements.iter().map(String::as_str).collect::<Vec<_>>();
    execute_ddl_batch(backend, "commit", &statement_refs).await
}
