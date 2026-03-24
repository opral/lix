use crate::live_state::constraints::{render_constraint_sql, ScanConstraint};
use crate::schema::live_layout::{LiveColumnSpec, LiveRowAccess};
use crate::{LixError, Value};

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
            .map(|column| crate::live_state::constraints::quote_ident(&column.column_name))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

pub(crate) struct ScanSqlRequest<'a> {
    pub select_prefix: &'a str,
    pub table_name: &'a str,
    pub schema_key: &'a str,
    pub version_id: &'a str,
    pub projection: &'a str,
    pub fixed_predicates: &'a [&'a str],
    pub constraints: &'a [ScanConstraint],
    pub order_by: &'a [&'a str],
    pub limit: Option<usize>,
}

pub(crate) fn build_partitioned_scan_sql(request: ScanSqlRequest<'_>) -> Result<String, LixError> {
    let mut sql = format!(
        "{}{} FROM {} WHERE schema_key = '{}' AND version_id = '{}'",
        request.select_prefix,
        request.projection,
        crate::live_state::constraints::quote_ident(request.table_name),
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
