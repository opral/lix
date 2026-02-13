use crate::{LixBackend, LixError, SqlDialect};

pub async fn register_schema(backend: &dyn LixBackend, schema_key: &str) -> Result<(), LixError> {
    for statement in register_schema_sql_statements(schema_key, backend.dialect()) {
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

pub fn register_schema_sql_statements(schema_key: &str, dialect: SqlDialect) -> Vec<String> {
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
        let inherits_from_expr = json_text_extract_expr(dialect, "inherits_from_version_id");
        let id_expr = json_text_extract_expr(dialect, "id");
        statements.push(format!(
            "CREATE INDEX IF NOT EXISTS {index} \
             ON {table}({inherits_from_expr}) \
             WHERE {inherits_from_expr} IS NOT NULL",
            index = quote_ident(&format!("idx_{}_inherits_from", table_name)),
            table = table_ident,
            inherits_from_expr = inherits_from_expr,
        ));
        statements.push(format!(
            "CREATE INDEX IF NOT EXISTS {index} \
             ON {table}({id_expr}, {inherits_from_expr})",
            index = quote_ident(&format!("idx_{}_id_parent", table_name)),
            table = table_ident,
            id_expr = id_expr,
            inherits_from_expr = inherits_from_expr,
        ));
    }

    if schema_key == "lix_file_descriptor" {
        let directory_expr = json_text_extract_expr(dialect, "directory_id");
        let name_expr = json_text_extract_expr(dialect, "name");
        let extension_expr = json_text_extract_expr(dialect, "extension");
        statements.push(format!(
            "CREATE INDEX IF NOT EXISTS {index} \
             ON {table}(version_id, {directory_expr}, {name_expr}, {extension_expr}) \
             WHERE is_tombstone = 0 AND snapshot_content IS NOT NULL",
            index = quote_ident("idx_lix_file_desc_v_dne_live"),
            table = table_ident,
            directory_expr = directory_expr,
            name_expr = name_expr,
            extension_expr = extension_expr,
        ));
    }

    if schema_key == "lix_directory_descriptor" {
        let parent_expr = json_text_extract_expr(dialect, "parent_id");
        let name_expr = json_text_extract_expr(dialect, "name");
        statements.push(format!(
            "CREATE INDEX IF NOT EXISTS {index} \
             ON {table}(version_id, {parent_expr}, {name_expr}) \
             WHERE is_tombstone = 0 AND snapshot_content IS NOT NULL",
            index = quote_ident("idx_lix_dir_desc_v_pn_live"),
            table = table_ident,
            parent_expr = parent_expr,
            name_expr = name_expr,
        ));
    }

    statements
}

fn quote_ident(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}

fn json_text_extract_expr(dialect: SqlDialect, key: &str) -> String {
    match dialect {
        SqlDialect::Sqlite => format!("json_extract(snapshot_content, '$.{key}')"),
        SqlDialect::Postgres => {
            format!("jsonb_extract_path_text(CAST(snapshot_content AS JSONB), '{key}')")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::register_schema_sql_statements;
    use crate::SqlDialect;
    use std::collections::BTreeMap;

    #[test]
    fn version_descriptor_indexes_use_sqlite_json_extract_on_sqlite() {
        let statements =
            register_schema_sql_statements("lix_version_descriptor", SqlDialect::Sqlite).join("\n");
        assert!(statements.contains("json_extract(snapshot_content, '$.inherits_from_version_id')"));
        assert!(!statements.contains("jsonb_extract_path_text("));
    }

    #[test]
    fn version_descriptor_indexes_use_postgres_json_extract_on_postgres() {
        let statements =
            register_schema_sql_statements("lix_version_descriptor", SqlDialect::Postgres)
                .join("\n");
        assert!(statements.contains(
            "jsonb_extract_path_text(CAST(snapshot_content AS JSONB), 'inherits_from_version_id')"
        ));
        assert!(!statements.contains("json_extract(snapshot_content"));
    }

    #[test]
    fn postgres_file_descriptor_index_names_do_not_truncate_to_collisions() {
        let statements = register_schema_sql_statements("lix_file_descriptor", SqlDialect::Postgres);
        let mut by_truncated = BTreeMap::<String, Vec<String>>::new();
        for statement in statements {
            let Some(rest) = statement.strip_prefix("CREATE INDEX IF NOT EXISTS \"") else {
                continue;
            };
            let Some((name, _)) = rest.split_once('"') else {
                continue;
            };
            let truncated = name.chars().take(63).collect::<String>();
            by_truncated
                .entry(truncated)
                .or_default()
                .push(name.to_string());
        }

        let collisions = by_truncated
            .into_iter()
            .filter_map(|(truncated, originals)| {
                if originals.len() <= 1 {
                    return None;
                }
                Some((truncated, originals))
            })
            .collect::<Vec<_>>();

        assert!(
            collisions.is_empty(),
            "postgres-truncated index name collisions detected: {collisions:?}"
        );
    }
}
