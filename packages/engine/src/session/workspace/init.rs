use crate::backend::ddl::execute_ddl_batch;
use crate::{LixBackend, LixError};

pub(crate) async fn init(backend: &dyn LixBackend) -> Result<(), LixError> {
    let statements = [format!(
        "CREATE TABLE {} (\
         key TEXT PRIMARY KEY, \
         value TEXT NOT NULL\
         )",
        super::WORKSPACE_METADATA_TABLE
    )];
    let statement_refs = statements.iter().map(String::as_str).collect::<Vec<_>>();
    execute_ddl_batch(backend, "workspace", &statement_refs).await
}
