use crate::init::tables::execute_init_statements;
use crate::{LixBackend, LixError};

const COMMIT_INIT_STATEMENTS: &[&str] = &[
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
];

pub(crate) async fn init(backend: &dyn LixBackend) -> Result<(), LixError> {
    execute_init_statements(backend, "commit", COMMIT_INIT_STATEMENTS).await
}
