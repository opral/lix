use crate::LixError;
use crate::LixBackend;

const INIT_STATEMENTS: &[&str] = &[
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
