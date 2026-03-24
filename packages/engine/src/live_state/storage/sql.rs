#![allow(dead_code)]

use crate::live_state::constraints::{quote_ident as constraint_quote_ident, render_constraint_sql, sql_literal, ScanConstraint};
use crate::{LixError, SqlDialect, Value};

use super::layout::{LiveColumnSpec, LiveRowAccess, LiveTableLayout};

pub(crate) const TRACKED_LIVE_TABLE_PREFIX: &str = "lix_internal_live_v1_";

pub(crate) fn ensure_schema_live_table_sql_statements(
    schema_key: &str,
    dialect: SqlDialect,
    layout: &LiveTableLayout,
) -> Vec<String> {
    let table_name = live_table_name(schema_key);
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

pub(crate) fn selected_columns<'a>(
    access: &'a LiveRowAccess,
    required_columns: &[String],
    state_label: &str,
) -> Result<Vec<&'a LiveColumnSpec>, LixError> {
    if required_columns.is_empty() {
        return Ok(access.columns().iter().collect());
    }

    let mut selected = Vec::with_capacity(required_columns.len());
    for property_name in required_columns {
        let Some(column) = access
            .columns()
            .iter()
            .find(|column| column.property_name == *property_name)
        else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                &format!(
                    "{state_label} scan for schema '{}' requested unknown property '{}'",
                    access.layout().schema_key,
                    property_name
                ),
            ));
        };
        selected.push(column);
    }
    Ok(selected)
}

pub(crate) fn selected_projection_sql(selected_columns: &[&LiveColumnSpec]) -> String {
    if selected_columns.is_empty() {
        return String::new();
    }
    format!(
        ", {}",
        selected_columns
            .iter()
            .map(|column| quote_ident(&column.column_name))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

pub(crate) struct ScanSqlRequest<'a> {
    pub(crate) select_prefix: &'a str,
    pub(crate) schema_key: &'a str,
    pub(crate) version_id: &'a str,
    pub(crate) projection: &'a str,
    pub(crate) fixed_predicates: &'a [&'a str],
    pub(crate) constraints: &'a [ScanConstraint],
    pub(crate) order_by: &'a [&'a str],
    pub(crate) limit: Option<usize>,
}

pub(crate) fn build_partitioned_scan_sql(request: ScanSqlRequest<'_>) -> Result<String, LixError> {
    let mut sql = format!(
        "{}{} FROM {} WHERE schema_key = '{}' AND version_id = '{}'",
        request.select_prefix,
        request.projection,
        quoted_live_table_name(request.schema_key),
        crate::live_state::constraints::escape_sql_string(request.schema_key),
        crate::live_state::constraints::escape_sql_string(request.version_id),
    );

    for predicate in request.fixed_predicates {
        sql.push_str(" AND ");
        sql.push_str(predicate);
    }

    for constraint in request.constraints {
        let clause = render_constraint_sql(constraint)?;
        if !clause.is_empty() {
            sql.push_str(" AND ");
            sql.push_str(&clause);
        }
    }

    if !request.order_by.is_empty() {
        sql.push_str(" ORDER BY ");
        sql.push_str(&request.order_by.join(", "));
    }
    if let Some(limit) = request.limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }
    Ok(sql)
}

pub(crate) fn normalized_insert_columns_sql(values: &[(String, Value)]) -> String {
    if values.is_empty() {
        return String::new();
    }
    values
        .iter()
        .map(|(column, _)| format!(", {}", quote_ident(column)))
        .collect::<String>()
}

pub(crate) fn normalized_insert_values_sql(values: &[(String, Value)]) -> String {
    if values.is_empty() {
        return String::new();
    }
    values
        .iter()
        .map(|(_, value)| format!(", {}", sql_literal(value)))
        .collect::<String>()
}

pub(crate) fn normalized_update_assignments_sql(values: &[(String, Value)]) -> String {
    if values.is_empty() {
        return String::new();
    }
    values
        .iter()
        .map(|(column, _)| {
            format!(
                ", {} = excluded.{}",
                quote_ident(column),
                quote_ident(column)
            )
        })
        .collect::<String>()
}

pub(crate) fn required_text_cell(
    row: &[Value],
    index: usize,
    schema_key: &str,
    column_name: &str,
    state_label: &str,
) -> Result<String, LixError> {
    row.get(index).and_then(text_from_value).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!(
                "{state_label} row for schema '{}' is missing text column '{}'",
                schema_key, column_name
            ),
        )
    })
}

pub(crate) fn required_bool_cell(
    row: &[Value],
    index: usize,
    schema_key: &str,
    column_name: &str,
    state_label: &str,
) -> Result<bool, LixError> {
    row.get(index).and_then(bool_from_value).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!(
                "{state_label} row for schema '{}' is missing boolean column '{}'",
                schema_key, column_name
            ),
        )
    })
}

pub(crate) fn text_from_value(value: &Value) -> Option<String> {
    match value {
        Value::Text(value) => Some(value.clone()),
        Value::Integer(value) => Some(value.to_string()),
        _ => None,
    }
}

pub(crate) fn quote_ident(value: &str) -> String {
    constraint_quote_ident(value)
}

pub(crate) fn quoted_live_table_name(schema_key: &str) -> String {
    quote_ident(&live_table_name(schema_key))
}

pub(crate) fn is_untracked_live_table(_name: &str) -> bool {
    false
}

pub(crate) fn live_schema_key_for_table_name(table_name: &str) -> Option<&str> {
    table_name.strip_prefix(TRACKED_LIVE_TABLE_PREFIX)
}

fn live_table_name(schema_key: &str) -> String {
    format!("{TRACKED_LIVE_TABLE_PREFIX}{schema_key}")
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

fn bool_from_value(value: &Value) -> Option<bool> {
    match value {
        Value::Boolean(value) => Some(*value),
        Value::Integer(value) => Some(*value != 0),
        Value::Text(value) => match value.as_str() {
            "true" | "TRUE" | "1" => Some(true),
            "false" | "FALSE" | "0" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::ensure_schema_live_table_sql_statements;
    use crate::live_state::storage::layout::live_table_layout_from_schema;
    use crate::schema::builtin::builtin_schema_definition;
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
