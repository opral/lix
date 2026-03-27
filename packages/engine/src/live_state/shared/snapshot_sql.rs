use crate::backend::SqlDialect;
use crate::live_state::storage::{builtin_live_table_layout, LiveColumnKind, LiveTableLayout};
use crate::LixError;

#[allow(dead_code)]
pub(crate) fn live_snapshot_select_expr_for_schema(
    schema_key: &str,
    dialect: SqlDialect,
    table_alias: Option<&str>,
) -> Result<String, LixError> {
    let Some(layout) = builtin_live_table_layout(schema_key)? else {
        return Ok(qualified_column_ref(table_alias, "snapshot_content"));
    };
    Ok(live_snapshot_select_expr(&layout, dialect, table_alias))
}

pub(crate) fn live_snapshot_select_expr(
    layout: &LiveTableLayout,
    dialect: SqlDialect,
    table_alias: Option<&str>,
) -> String {
    if layout.columns.is_empty() {
        return "'{}'".to_string();
    }

    let pieces = layout
        .columns
        .iter()
        .map(|column| {
            let column_ref = qualified_column_ref(table_alias, &column.column_name);
            let value_expr = match (dialect, column.kind) {
                (_, LiveColumnKind::JsonText) => column_ref.clone(),
                (SqlDialect::Sqlite, LiveColumnKind::Boolean) => format!(
                    "CASE WHEN {column_ref} = 0 THEN 'false' ELSE 'true' END"
                ),
                (SqlDialect::Sqlite, LiveColumnKind::String) => {
                    format!("json_quote({column_ref})")
                }
                (SqlDialect::Sqlite, LiveColumnKind::Integer | LiveColumnKind::Number) => {
                    format!("CAST({column_ref} AS TEXT)")
                }
                (SqlDialect::Postgres, _) => format!("to_json({column_ref})::text"),
            };
            if column.preserve_null_in_logical_snapshot() {
                format!(
                    "CASE WHEN {column_ref} IS NULL THEN '\"{property_name}\":null' ELSE '\"{property_name}\":' || {value_expr} END",
                    property_name = column.property_name,
                )
            } else {
                format!(
                    "CASE WHEN {column_ref} IS NULL THEN NULL ELSE '\"{property_name}\":' || {value_expr} END",
                    property_name = column.property_name,
                )
            }
        })
        .collect::<Vec<_>>();

    match dialect {
        SqlDialect::Sqlite => {
            let body = format!(
                "rtrim({parts}, ',')",
                parts = pieces
                    .iter()
                    .map(|piece| format!("COALESCE(({piece}) || ',', '')"))
                    .collect::<Vec<_>>()
                    .join(" || "),
            );
            format!("'{{' || {body} || '}}'")
        }
        SqlDialect::Postgres => format!("'{{' || concat_ws(',', {}) || '}}'", pieces.join(", ")),
    }
}

fn qualified_column_ref(table_alias: Option<&str>, column_name: &str) -> String {
    match table_alias {
        Some(alias) => format!(
            "{}.{}",
            quote_ident_fragment(alias),
            quote_ident_fragment(column_name)
        ),
        None => quote_ident_fragment(column_name),
    }
}

fn quote_ident_fragment(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
