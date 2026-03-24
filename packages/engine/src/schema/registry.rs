use crate::schema::live_layout::{
    builtin_live_table_layout, merge_live_table_layouts, tracked_live_table_name, LiveTableLayout,
};
use crate::schema::schema_from_registered_snapshot;
use crate::sql::execution::contracts::planned_statement::SchemaLiveTableRequirement;
use crate::{LixBackend, LixError, LixBackendTransaction, SqlDialect};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

pub async fn ensure_schema_live_table(
    backend: &dyn LixBackend,
    schema_key: &str,
) -> Result<(), LixError> {
    ensure_schema_live_table_with_requirement(
        backend,
        &SchemaLiveTableRequirement {
            schema_key: schema_key.to_string(),
            layout: None,
        },
    )
    .await
}

pub async fn ensure_schema_live_table_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    schema_key: &str,
) -> Result<(), LixError> {
    ensure_schema_live_table_with_requirement_in_transaction(
        transaction,
        &SchemaLiveTableRequirement {
            schema_key: schema_key.to_string(),
            layout: None,
        },
    )
    .await
}

pub async fn ensure_schema_live_table_with_requirement(
    backend: &dyn LixBackend,
    requirement: &SchemaLiveTableRequirement,
) -> Result<(), LixError> {
    let layout = match requirement.layout.as_ref() {
        Some(layout) => layout.clone(),
        None => load_live_table_layout_with_backend(backend, &requirement.schema_key).await?,
    };
    for statement in
        ensure_schema_live_table_sql_statements(&requirement.schema_key, backend.dialect(), &layout)
    {
        backend.execute(&statement, &[]).await?;
    }
    Ok(())
}

pub async fn ensure_schema_live_table_with_requirement_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    requirement: &SchemaLiveTableRequirement,
) -> Result<(), LixError> {
    let layout = match requirement.layout.as_ref() {
        Some(layout) => layout.clone(),
        None => load_live_table_layout_in_transaction(transaction, &requirement.schema_key).await?,
    };
    for statement in ensure_schema_live_table_sql_statements(
        &requirement.schema_key,
        transaction.dialect(),
        &layout,
    ) {
        transaction.execute(&statement, &[]).await?;
    }
    Ok(())
}

pub fn coalesce_live_table_requirements(
    requirements: &[SchemaLiveTableRequirement],
) -> Vec<SchemaLiveTableRequirement> {
    let mut by_schema = BTreeMap::<String, SchemaLiveTableRequirement>::new();
    for requirement in requirements {
        by_schema
            .entry(requirement.schema_key.clone())
            .and_modify(|existing| {
                if existing.layout.is_none() && requirement.layout.is_some() {
                    existing.layout = requirement.layout.clone();
                }
            })
            .or_insert_with(|| requirement.clone());
    }
    by_schema.into_values().collect()
}

pub fn ensure_schema_live_table_sql_statements(
    schema_key: &str,
    dialect: SqlDialect,
    layout: &LiveTableLayout,
) -> Vec<String> {
    let table_name = tracked_live_table_name(schema_key);
    let table_ident = quote_ident(&table_name);
    let mut statements = vec![format!(
        "CREATE TABLE IF NOT EXISTS {table} (\
         entity_id TEXT NOT NULL,\
         schema_key TEXT NOT NULL,\
         schema_version TEXT NOT NULL,\
         file_id TEXT NOT NULL,\
         version_id TEXT NOT NULL,\
         global BOOLEAN NOT NULL DEFAULT false,\
         plugin_key TEXT NOT NULL,\
         change_id TEXT,\
         metadata TEXT,\
         writer_key TEXT,\
         is_tombstone INTEGER NOT NULL DEFAULT 0,\
         untracked BOOLEAN NOT NULL DEFAULT false,\
         created_at TEXT NOT NULL,\
         updated_at TEXT NOT NULL{normalized_columns},\
         PRIMARY KEY (entity_id, file_id, version_id, untracked)\
         )",
        table = table_ident,
        normalized_columns = render_normalized_columns(Some(layout), dialect),
    )];

    statements.extend(common_live_indexes(&table_name, &table_ident));
    statements.extend(normalized_column_indexes(&table_name, &table_ident, layout));
    statements
}

fn render_normalized_columns(layout: Option<&LiveTableLayout>, dialect: SqlDialect) -> String {
    let Some(layout) = layout else {
        return String::new();
    };
    let mut out = String::new();
    for column in &layout.columns {
        out.push_str(",\n         ");
        out.push_str(&quote_ident(&column.column_name));
        out.push(' ');
        out.push_str(column.sql_type(dialect));
    }
    out
}

fn common_live_indexes(table_name: &str, table_ident: &str) -> Vec<String> {
    let mut statements = vec![
        format!(
            "CREATE INDEX IF NOT EXISTS {index} ON {table} (version_id)",
            index = quote_ident(&format!("idx_{}_version_id", table_name)),
            table = table_ident,
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {index} ON {table} (global, version_id)",
            index = quote_ident(&format!("idx_{}_global_version", table_name)),
            table = table_ident,
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {index} ON {table} (version_id, file_id, entity_id, untracked)",
            index = quote_ident(&format!("idx_{}_vfe", table_name)),
            table = table_ident,
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {index} ON {table} (version_id, entity_id, untracked)",
            index = quote_ident(&format!("idx_{}_ve", table_name)),
            table = table_ident,
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {index} ON {table} (file_id, version_id, untracked)",
            index = quote_ident(&format!("idx_{}_fv", table_name)),
            table = table_ident,
        ),
    ];
    statements.push(format!(
        "CREATE INDEX IF NOT EXISTS {index} \
         ON {table} (version_id, file_id, entity_id) \
         WHERE untracked = false AND is_tombstone = 0",
        index = quote_ident(&format!("idx_{}_live_vfe", table_name)),
        table = table_ident,
    ));
    statements.push(format!(
        "CREATE INDEX IF NOT EXISTS {index} \
         ON {table} (version_id, file_id, entity_id) \
         WHERE untracked = false AND is_tombstone = 1",
        index = quote_ident(&format!("idx_{}_tomb_vfe", table_name)),
        table = table_ident,
    ));
    statements.push(format!(
        "CREATE INDEX IF NOT EXISTS {index} \
         ON {table} (version_id, file_id, entity_id) \
         WHERE untracked = true",
        index = quote_ident(&format!("idx_{}_untracked_vfe", table_name)),
        table = table_ident,
    ));
    statements
}

fn normalized_column_indexes(
    _table_name: &str,
    table_ident: &str,
    layout: &LiveTableLayout,
) -> Vec<String> {
    let mut statements = Vec::new();
    match layout.schema_key.as_str() {
        "lix_file_descriptor" => {
            if has_columns(layout, &["directory_id", "name", "extension"]) {
                statements.push(format!(
                    "CREATE INDEX IF NOT EXISTS {index} \
                     ON {table}(version_id, {directory}, {name}, {extension}) \
                     WHERE untracked = false AND is_tombstone = 0",
                    index = quote_ident("idx_lix_file_desc_v_dne_live"),
                    table = table_ident,
                    directory = quote_ident("directory_id"),
                    name = quote_ident("name"),
                    extension = quote_ident("extension"),
                ));
                statements.push(format!(
                    "CREATE INDEX IF NOT EXISTS {index} \
                     ON {table}(version_id, {directory}, {name}, {extension}) \
                     WHERE untracked = true",
                    index = quote_ident("idx_lix_file_desc_v_dne_untracked"),
                    table = table_ident,
                    directory = quote_ident("directory_id"),
                    name = quote_ident("name"),
                    extension = quote_ident("extension"),
                ));
            }
        }
        "lix_directory_descriptor" => {
            if has_columns(layout, &["parent_id", "name"]) {
                statements.push(format!(
                    "CREATE INDEX IF NOT EXISTS {index} \
                     ON {table}(version_id, {parent_id}, {name}) \
                     WHERE untracked = false AND is_tombstone = 0",
                    index = quote_ident("idx_lix_dir_desc_v_pn_live"),
                    table = table_ident,
                    parent_id = quote_ident("parent_id"),
                    name = quote_ident("name"),
                ));
                statements.push(format!(
                    "CREATE INDEX IF NOT EXISTS {index} \
                     ON {table}(version_id, {parent_id}, {name}) \
                     WHERE untracked = true",
                    index = quote_ident("idx_lix_dir_desc_v_pn_untracked"),
                    table = table_ident,
                    parent_id = quote_ident("parent_id"),
                    name = quote_ident("name"),
                ));
            }
        }
        "lix_commit_edge" => {
            if has_columns(layout, &["child_id"]) {
                statements.push(format!(
                    "CREATE INDEX IF NOT EXISTS {index} \
                     ON {table}(version_id, {child_id}) \
                     WHERE untracked = false AND is_tombstone = 0",
                    index = quote_ident("idx_lix_commit_edge_v_child_live"),
                    table = table_ident,
                    child_id = quote_ident("child_id"),
                ));
                statements.push(format!(
                    "CREATE INDEX IF NOT EXISTS {index} \
                     ON {table}(version_id, {child_id}) \
                     WHERE untracked = true",
                    index = quote_ident("idx_lix_commit_edge_v_child_untracked"),
                    table = table_ident,
                    child_id = quote_ident("child_id"),
                ));
            }
            if has_columns(layout, &["parent_id"]) {
                statements.push(format!(
                    "CREATE INDEX IF NOT EXISTS {index} \
                     ON {table}(version_id, {parent_id}) \
                     WHERE untracked = false AND is_tombstone = 0",
                    index = quote_ident("idx_lix_commit_edge_v_parent_live"),
                    table = table_ident,
                    parent_id = quote_ident("parent_id"),
                ));
                statements.push(format!(
                    "CREATE INDEX IF NOT EXISTS {index} \
                     ON {table}(version_id, {parent_id}) \
                     WHERE untracked = true",
                    index = quote_ident("idx_lix_commit_edge_v_parent_untracked"),
                    table = table_ident,
                    parent_id = quote_ident("parent_id"),
                ));
            }
        }
        "lix_commit" => {
            if has_columns(layout, &["change_set_id"]) {
                statements.push(format!(
                    "CREATE INDEX IF NOT EXISTS {index} \
                     ON {table}(version_id, {change_set_id}) \
                     WHERE untracked = false AND is_tombstone = 0",
                    index = quote_ident("idx_lix_commit_v_change_set_live"),
                    table = table_ident,
                    change_set_id = quote_ident("change_set_id"),
                ));
                statements.push(format!(
                    "CREATE INDEX IF NOT EXISTS {index} \
                     ON {table}(version_id, {change_set_id}) \
                     WHERE untracked = true",
                    index = quote_ident("idx_lix_commit_v_change_set_untracked"),
                    table = table_ident,
                    change_set_id = quote_ident("change_set_id"),
                ));
            }
        }
        "lix_change_set_element" => {
            if has_columns(layout, &["change_set_id"]) {
                statements.push(format!(
                    "CREATE INDEX IF NOT EXISTS {index} \
                     ON {table}(version_id, {change_set_id}) \
                     WHERE untracked = false AND is_tombstone = 0",
                    index = quote_ident("idx_lix_cse_v_change_set_live"),
                    table = table_ident,
                    change_set_id = quote_ident("change_set_id"),
                ));
                statements.push(format!(
                    "CREATE INDEX IF NOT EXISTS {index} \
                     ON {table}(version_id, {change_set_id}) \
                     WHERE untracked = true",
                    index = quote_ident("idx_lix_cse_v_change_set_untracked"),
                    table = table_ident,
                    change_set_id = quote_ident("change_set_id"),
                ));
            }
            if has_columns(layout, &["change_id"]) {
                statements.push(format!(
                    "CREATE INDEX IF NOT EXISTS {index} \
                     ON {table}(version_id, {change_id}) \
                     WHERE untracked = false AND is_tombstone = 0",
                    index = quote_ident("idx_lix_cse_v_change_live"),
                    table = table_ident,
                    change_id = quote_ident("change_id"),
                ));
                statements.push(format!(
                    "CREATE INDEX IF NOT EXISTS {index} \
                     ON {table}(version_id, {change_id}) \
                     WHERE untracked = true",
                    index = quote_ident("idx_lix_cse_v_change_untracked"),
                    table = table_ident,
                    change_id = quote_ident("change_id"),
                ));
            }
        }
        _ => {}
    }
    statements
}

fn has_columns(layout: &LiveTableLayout, expected: &[&str]) -> bool {
    expected.iter().all(|column| {
        layout
            .columns
            .iter()
            .any(|candidate| candidate.column_name == *column)
    })
}

fn quote_ident(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}

pub(crate) async fn load_live_table_layout_with_backend(
    backend: &dyn LixBackend,
    schema_key: &str,
) -> Result<LiveTableLayout, LixError> {
    if let Some(layout) = builtin_live_table_layout(schema_key)? {
        return Ok(layout);
    }

    let sql = format!(
        "SELECT snapshot_content \
         FROM lix_internal_registered_schema_bootstrap \
         WHERE schema_key = 'lix_registered_schema' \
           AND version_id = 'global' \
           AND is_tombstone = 0 \
           AND snapshot_content IS NOT NULL",
    );
    let result = backend.execute(&sql, &[]).await?;
    compile_registered_live_layout(schema_key, result.rows.into_iter().collect())
}

pub(crate) async fn load_live_table_layout_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    schema_key: &str,
) -> Result<LiveTableLayout, LixError> {
    if let Some(layout) = builtin_live_table_layout(schema_key)? {
        return Ok(layout);
    }

    let sql = format!(
        "SELECT snapshot_content \
         FROM lix_internal_registered_schema_bootstrap \
         WHERE schema_key = 'lix_registered_schema' \
           AND version_id = 'global' \
           AND is_tombstone = 0 \
           AND snapshot_content IS NOT NULL",
    );
    let result = transaction.execute(&sql, &[]).await?;
    compile_registered_live_layout(schema_key, result.rows.into_iter().collect())
}

pub(crate) fn compile_registered_live_layout(
    schema_key: &str,
    rows: Vec<Vec<crate::Value>>,
) -> Result<LiveTableLayout, LixError> {
    let mut layouts = Vec::new();
    for row in rows {
        let Some(crate::Value::Text(snapshot_content)) = row.first() else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                &format!(
                    "registered schema bootstrap lookup for '{}' returned a non-text snapshot_content",
                    schema_key
                ),
            ));
        };
        let snapshot: JsonValue = serde_json::from_str(snapshot_content).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                &format!(
                    "registered schema bootstrap snapshot_content for '{}' is invalid JSON: {error}",
                    schema_key
                ),
            )
        })?;
        let (key, schema) = schema_from_registered_snapshot(&snapshot)?;
        if key.schema_key != schema_key {
            continue;
        }
        layouts.push(crate::schema::live_layout::live_table_layout_from_schema(
            &schema,
        )?);
    }

    if layouts.is_empty() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!("schema '{}' is not stored", schema_key),
        ));
    }

    merge_live_table_layouts(schema_key, layouts)
}

#[cfg(test)]
mod tests {
    use super::ensure_schema_live_table_sql_statements;
    use crate::schema::builtin::builtin_schema_definition;
    use crate::schema::live_layout::live_table_layout_from_schema;
    use crate::SqlDialect;
    use std::collections::BTreeMap;

    #[test]
    fn version_descriptor_indexes_do_not_reference_inheritance_state() {
        let layout = live_table_layout_from_schema(
            builtin_schema_definition("lix_version_descriptor")
                .expect("builtin schema should exist"),
        )
        .expect("layout should compile");
        let sqlite_statements = ensure_schema_live_table_sql_statements(
            "lix_version_descriptor",
            SqlDialect::Sqlite,
            &layout,
        )
        .join("\n");
        let postgres_statements = ensure_schema_live_table_sql_statements(
            "lix_version_descriptor",
            SqlDialect::Postgres,
            &layout,
        )
        .join("\n");
        assert!(!sqlite_statements.contains("inherits_from_version_id"));
        assert!(!postgres_statements.contains("inherits_from_version_id"));
    }

    #[test]
    fn postgres_file_descriptor_index_names_do_not_truncate_to_collisions() {
        let layout = live_table_layout_from_schema(
            builtin_schema_definition("lix_file_descriptor").expect("builtin schema should exist"),
        )
        .expect("layout should compile");
        let statements = ensure_schema_live_table_sql_statements(
            "lix_file_descriptor",
            SqlDialect::Postgres,
            &layout,
        );
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
