use crate::{LixBackend, LixError};

pub async fn register_schema(backend: &dyn LixBackend, schema_key: &str) -> Result<(), LixError> {
    let table_name = format!("lix_internal_state_materialized_v1_{}", schema_key);
    let table_ident = quote_ident(&table_name);

    let create_sql = format!(
        "CREATE TABLE IF NOT EXISTS {table} (\
         entity_id TEXT NOT NULL,\
         schema_key TEXT NOT NULL,\
         schema_version TEXT NOT NULL,\
         file_id TEXT NOT NULL,\
         version_id TEXT NOT NULL,\
         plugin_key TEXT NOT NULL,\
         snapshot_content TEXT,\
         change_id TEXT NOT NULL,\
         is_tombstone INTEGER NOT NULL DEFAULT 0,\
         created_at TEXT NOT NULL,\
         updated_at TEXT NOT NULL,\
         PRIMARY KEY (entity_id, file_id, version_id)\
         )",
        table = table_ident
    );

    backend.execute(&create_sql, &[]).await?;

    Ok(())
}

fn quote_ident(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}
