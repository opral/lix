use crate::{LixBackend, LixError};

pub async fn register_schema(backend: &dyn LixBackend, schema_key: &str) -> Result<(), LixError> {
    for statement in register_schema_sql_statements(schema_key) {
        backend.execute(&statement, &[]).await?;
    }
    Ok(())
}

pub fn register_schema_sql(schema_key: &str) -> String {
    let table_name = format!("lix_internal_state_materialized_v1_{}", schema_key);
    let table_ident = quote_ident(&table_name);

    format!(
        "CREATE TABLE IF NOT EXISTS {table} (\
         entity_id TEXT NOT NULL,\
         schema_key TEXT NOT NULL,\
         schema_version TEXT NOT NULL,\
         file_id TEXT NOT NULL,\
         version_id TEXT NOT NULL,\
         plugin_key TEXT NOT NULL,\
         snapshot_content TEXT,\
         inherited_from_version_id TEXT,\
         change_id TEXT NOT NULL,\
         metadata TEXT,\
         writer_key TEXT,\
         is_tombstone INTEGER NOT NULL DEFAULT 0,\
         created_at TEXT NOT NULL,\
         updated_at TEXT NOT NULL,\
         PRIMARY KEY (entity_id, file_id, version_id)\
         )",
        table = table_ident
    )
}

pub fn register_schema_sql_statements(schema_key: &str) -> Vec<String> {
    let table_name = format!("lix_internal_state_materialized_v1_{}", schema_key);
    let table_ident = quote_ident(&table_name);

    let mut statements = vec![register_schema_sql(schema_key)];

    let index_statements = vec![
        format!(
            "CREATE INDEX IF NOT EXISTS {index} ON {table} (version_id)",
            index = quote_ident(&format!("idx_{}_version_id", table_name)),
            table = table_ident,
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {index} ON {table} (version_id, file_id, entity_id)",
            index = quote_ident(&format!("idx_{}_vfe", table_name)),
            table = table_ident,
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {index} ON {table} (version_id, entity_id)",
            index = quote_ident(&format!("idx_{}_ve", table_name)),
            table = table_ident,
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {index} ON {table} (file_id, version_id)",
            index = quote_ident(&format!("idx_{}_fv", table_name)),
            table = table_ident,
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {index} \
             ON {table} (version_id, file_id, entity_id) \
             WHERE is_tombstone = 0 AND snapshot_content IS NOT NULL",
            index = quote_ident(&format!("idx_{}_live_vfe", table_name)),
            table = table_ident,
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {index} \
             ON {table} (version_id, file_id, entity_id) \
             WHERE is_tombstone = 1 AND snapshot_content IS NULL",
            index = quote_ident(&format!("idx_{}_tomb_vfe", table_name)),
            table = table_ident,
        ),
    ];
    statements.extend(index_statements);

    if schema_key == "lix_version_descriptor" {
        statements.push(format!(
            "CREATE INDEX IF NOT EXISTS {index} \
             ON {table}(json_extract(snapshot_content, '$.inherits_from_version_id')) \
             WHERE json_extract(snapshot_content, '$.inherits_from_version_id') IS NOT NULL",
            index = quote_ident(&format!("idx_{}_inherits_from", table_name)),
            table = table_ident,
        ));
        statements.push(format!(
            "CREATE INDEX IF NOT EXISTS {index} \
             ON {table}(json_extract(snapshot_content, '$.id'), json_extract(snapshot_content, '$.inherits_from_version_id'))",
            index = quote_ident(&format!("idx_{}_id_parent", table_name)),
            table = table_ident,
        ));
    }

    statements
}

fn quote_ident(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}
