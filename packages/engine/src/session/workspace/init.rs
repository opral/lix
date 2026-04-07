use crate::annotations::writer_key::WORKSPACE_WRITER_KEY_TABLE;
use crate::ddl::execute_ddl_batch;
use crate::{LixBackend, LixError};

pub(crate) async fn init(backend: &dyn LixBackend) -> Result<(), LixError> {
    let statements = [
        format!(
            "CREATE TABLE {} (\
             key TEXT PRIMARY KEY, \
             value TEXT NOT NULL\
             )",
            super::WORKSPACE_METADATA_TABLE
        ),
        format!(
            "CREATE TABLE {} (\
             version_id TEXT NOT NULL, \
             schema_key TEXT NOT NULL, \
             entity_id TEXT NOT NULL, \
             file_id TEXT NOT NULL, \
             writer_key TEXT NOT NULL, \
             PRIMARY KEY (version_id, schema_key, entity_id, file_id)\
             )",
            WORKSPACE_WRITER_KEY_TABLE
        ),
    ];
    let statement_refs = statements.iter().map(String::as_str).collect::<Vec<_>>();
    execute_ddl_batch(backend, "workspace", &statement_refs).await
}
