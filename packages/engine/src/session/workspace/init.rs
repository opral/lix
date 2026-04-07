use crate::ddl::execute_ddl_batch;
use crate::{LixBackend, LixError};

const WORKSPACE_INIT_STATEMENTS: &[&str] = &[
    "CREATE TABLE lix_internal_workspace_metadata (\
     key TEXT PRIMARY KEY, \
     value TEXT NOT NULL\
     )",
    "CREATE TABLE lix_internal_workspace_writer_key (\
     version_id TEXT NOT NULL, \
     schema_key TEXT NOT NULL, \
     entity_id TEXT NOT NULL, \
     file_id TEXT NOT NULL, \
     writer_key TEXT NOT NULL, \
     PRIMARY KEY (version_id, schema_key, entity_id, file_id)\
     )",
];

pub(crate) async fn init(backend: &dyn LixBackend) -> Result<(), LixError> {
    execute_ddl_batch(backend, "workspace", WORKSPACE_INIT_STATEMENTS).await
}
