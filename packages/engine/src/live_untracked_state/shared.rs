use crate::constraints::{Bound, ScanConstraint, ScanField, ScanOperator};
use crate::schema::live_layout::{LiveColumnSpec, LiveRowAccess};
use crate::{LixError, Value};

use super::contracts::{ExactUntrackedRowRequest, UntrackedRow};

pub(super) fn exact_row_constraints(request: &ExactUntrackedRowRequest) -> Vec<ScanConstraint> {
    let mut constraints = vec![ScanConstraint {
        field: ScanField::EntityId,
        operator: ScanOperator::Eq(Value::Text(request.entity_id.clone())),
    }];
    if let Some(file_id) = &request.file_id {
        constraints.push(ScanConstraint {
            field: ScanField::FileId,
            operator: ScanOperator::Eq(Value::Text(file_id.clone())),
        });
    }
    constraints
}

pub(super) fn selected_columns<'a>(
    access: &'a LiveRowAccess,
    required_columns: &[String],
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
                    "untracked scan for schema '{}' requested unknown property '{}'",
                    access.layout().schema_key,
                    property_name
                ),
            ));
        };
        selected.push(column);
    }
    Ok(selected)
}

pub(super) fn selected_projection_sql(selected_columns: &[&LiveColumnSpec]) -> String {
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

pub(super) fn render_constraint_sql(constraint: &ScanConstraint) -> Result<String, LixError> {
    let column_name = quote_ident(match constraint.field {
        ScanField::EntityId => "entity_id",
        ScanField::FileId => "file_id",
        ScanField::PluginKey => "plugin_key",
        ScanField::SchemaVersion => "schema_version",
    });

    match &constraint.operator {
        ScanOperator::Eq(Value::Null) => Ok(format!("{column_name} IS NULL")),
        ScanOperator::Eq(value) => Ok(format!("{column_name} = {}", sql_literal(value))),
        ScanOperator::In(values) => render_in_constraint_sql(&column_name, values),
        ScanOperator::Range { lower, upper } => {
            render_range_constraint_sql(&column_name, lower, upper)
        }
    }
}

pub(super) fn decode_untracked_row(
    row: &[Value],
    selected_columns: &[&LiveColumnSpec],
    schema_key: &str,
) -> Result<UntrackedRow, LixError> {
    let entity_id = required_text_cell(row, 0, schema_key, "entity_id")?;
    let schema_key_value = required_text_cell(row, 1, schema_key, "schema_key")?;
    let schema_version = required_text_cell(row, 2, schema_key, "schema_version")?;
    let file_id = required_text_cell(row, 3, schema_key, "file_id")?;
    let version_id = required_text_cell(row, 4, schema_key, "version_id")?;
    let global = required_bool_cell(row, 5, schema_key, "global")?;
    let plugin_key = required_text_cell(row, 6, schema_key, "plugin_key")?;
    let metadata = row.get(7).and_then(text_from_value);
    let writer_key = row.get(8).and_then(text_from_value);
    let created_at = required_text_cell(row, 9, schema_key, "created_at")?;
    let updated_at = required_text_cell(row, 10, schema_key, "updated_at")?;

    let mut values = std::collections::BTreeMap::new();
    for (offset, column) in selected_columns.iter().enumerate() {
        let value = row.get(11 + offset).ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                &format!(
                    "untracked row for schema '{}' is missing property '{}'",
                    schema_key, column.property_name
                ),
            )
        })?;
        values.insert(column.property_name.clone(), value.clone());
    }

    Ok(UntrackedRow {
        entity_id,
        schema_key: schema_key_value,
        schema_version,
        file_id,
        version_id,
        global,
        plugin_key,
        metadata,
        writer_key,
        created_at,
        updated_at,
        values,
    })
}

pub(super) fn normalized_insert_columns_sql(values: &[(String, Value)]) -> String {
    if values.is_empty() {
        return String::new();
    }
    values
        .iter()
        .map(|(column, _)| format!(", {}", quote_ident(column)))
        .collect::<String>()
}

pub(super) fn normalized_insert_values_sql(values: &[(String, Value)]) -> String {
    if values.is_empty() {
        return String::new();
    }
    values
        .iter()
        .map(|(_, value)| format!(", {}", sql_literal(value)))
        .collect::<String>()
}

pub(super) fn normalized_update_assignments_sql(values: &[(String, Value)]) -> String {
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

pub(super) fn sql_literal_text(value: &str) -> String {
    format!("'{}'", escape_sql_string(value))
}

pub(super) fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

pub(super) fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

pub(super) fn text_from_value(value: &Value) -> Option<String> {
    match value {
        Value::Text(value) => Some(value.clone()),
        Value::Integer(value) => Some(value.to_string()),
        _ => None,
    }
}

fn render_in_constraint_sql(column_name: &str, values: &[Value]) -> Result<String, LixError> {
    if values.is_empty() {
        return Ok("1 = 0".to_string());
    }

    let mut has_null = false;
    let mut non_null_literals = Vec::new();
    for value in values {
        if matches!(value, Value::Null) {
            has_null = true;
        } else {
            non_null_literals.push(sql_literal(value));
        }
    }

    match (non_null_literals.is_empty(), has_null) {
        (true, true) => Ok(format!("{column_name} IS NULL")),
        (false, false) => Ok(format!(
            "{column_name} IN ({})",
            non_null_literals.join(", ")
        )),
        (false, true) => Ok(format!(
            "({column_name} IN ({}) OR {column_name} IS NULL)",
            non_null_literals.join(", ")
        )),
        (true, false) => Ok("1 = 0".to_string()),
    }
}

fn render_range_constraint_sql(
    column_name: &str,
    lower: &Option<Bound>,
    upper: &Option<Bound>,
) -> Result<String, LixError> {
    let mut clauses = Vec::new();
    if let Some(lower) = lower {
        if matches!(lower.value, Value::Null) {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "range lower bounds do not support NULL",
            ));
        }
        clauses.push(format!(
            "{column_name} {} {}",
            if lower.inclusive { ">=" } else { ">" },
            sql_literal(&lower.value)
        ));
    }
    if let Some(upper) = upper {
        if matches!(upper.value, Value::Null) {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "range upper bounds do not support NULL",
            ));
        }
        clauses.push(format!(
            "{column_name} {} {}",
            if upper.inclusive { "<=" } else { "<" },
            sql_literal(&upper.value)
        ));
    }
    Ok(clauses.join(" AND "))
}

fn required_text_cell(
    row: &[Value],
    index: usize,
    schema_key: &str,
    column_name: &str,
) -> Result<String, LixError> {
    row.get(index).and_then(text_from_value).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!(
                "untracked row for schema '{}' is missing text column '{}'",
                schema_key, column_name
            ),
        )
    })
}

fn required_bool_cell(
    row: &[Value],
    index: usize,
    schema_key: &str,
    column_name: &str,
) -> Result<bool, LixError> {
    row.get(index).and_then(bool_from_value).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!(
                "untracked row for schema '{}' is missing boolean column '{}'",
                schema_key, column_name
            ),
        )
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

fn sql_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Boolean(value) => {
            if *value {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        Value::Integer(value) => value.to_string(),
        Value::Real(value) => value.to_string(),
        Value::Text(value) => sql_literal_text(value),
        Value::Json(value) => sql_literal_text(&value.to_string()),
        Value::Blob(_) => "NULL".to_string(),
    }
}
