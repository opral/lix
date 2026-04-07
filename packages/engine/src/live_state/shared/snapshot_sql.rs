use crate::live_state::storage::{builtin_live_table_layout, LiveColumnKind, LiveTableLayout};
use crate::LixError;
use crate::SqlDialect;

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

    match dialect {
        SqlDialect::Sqlite => sqlite_live_snapshot_select_expr(layout, table_alias),
        SqlDialect::Postgres => postgres_live_snapshot_select_expr(layout, table_alias),
    }
}

fn sqlite_live_snapshot_select_expr(layout: &LiveTableLayout, table_alias: Option<&str>) -> String {
    layout
        .columns
        .iter()
        .fold("json('{}')".to_string(), |current, column| {
            sqlite_live_snapshot_step(&current, column, table_alias)
        })
}

fn sqlite_live_snapshot_step(
    current: &str,
    column: &crate::live_state::storage::LiveColumnSpec,
    table_alias: Option<&str>,
) -> String {
    let column_ref = qualified_column_ref(table_alias, &column.column_name);
    let json_path = escape_sql_string_literal(&format!("$.{}", column.property_name));
    let value_expr = match column.kind {
        LiveColumnKind::JsonText => format!(
            "CASE WHEN {column_ref} IS NULL THEN json('null') ELSE json({column_ref}) END",
            column_ref = column_ref,
        ),
        LiveColumnKind::Boolean => format!(
            "CASE \
                WHEN {column_ref} IS NULL THEN json('null') \
                WHEN {column_ref} = 0 THEN json('false') \
                ELSE json('true') \
             END",
            column_ref = column_ref,
        ),
        LiveColumnKind::String | LiveColumnKind::Integer | LiveColumnKind::Number => format!(
            "CASE WHEN {column_ref} IS NULL THEN json('null') ELSE {column_ref} END",
            column_ref = column_ref,
        ),
    };
    let set_expr = format!(
        "json_set({current}, '{json_path}', {value_expr})",
        current = current,
        json_path = json_path,
        value_expr = value_expr,
    );
    if column.preserve_null_in_logical_snapshot() {
        set_expr
    } else {
        format!(
            "CASE WHEN {column_ref} IS NULL THEN {current} ELSE {set_expr} END",
            column_ref = column_ref,
            current = current,
            set_expr = set_expr,
        )
    }
}

fn postgres_live_snapshot_select_expr(
    layout: &LiveTableLayout,
    table_alias: Option<&str>,
) -> String {
    let pieces = layout
        .columns
        .iter()
        .map(|column| {
            let column_ref = qualified_column_ref(table_alias, &column.column_name);
            let value_expr = match column.kind {
                LiveColumnKind::JsonText => column_ref.clone(),
                LiveColumnKind::Boolean
                | LiveColumnKind::String
                | LiveColumnKind::Integer
                | LiveColumnKind::Number => format!("to_json({column_ref})::text"),
            };
            if column.preserve_null_in_logical_snapshot() {
                format!(
                    "CASE WHEN {column_ref} IS NULL THEN '\"{property_name}\":null' ELSE '\"{property_name}\":' || {value_expr} END",
                    property_name = column.property_name,
                    column_ref = column_ref,
                    value_expr = value_expr,
                )
            } else {
                format!(
                    "CASE WHEN {column_ref} IS NULL THEN NULL ELSE '\"{property_name}\":' || {value_expr} END",
                    property_name = column.property_name,
                    column_ref = column_ref,
                    value_expr = value_expr,
                )
            }
        })
        .collect::<Vec<_>>();
    format!("'{{' || concat_ws(',', {}) || '}}'", pieces.join(", "))
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

fn escape_sql_string_literal(value: &str) -> String {
    value.replace('\'', "''")
}
