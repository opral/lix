use crate::Value;

use super::{Bound, ScanConstraint, ScanField, ScanOperator};
use crate::LixError;

pub(crate) fn render_constraint_sql(constraint: &ScanConstraint) -> Result<String, LixError> {
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

pub(crate) fn sql_literal_text(value: &str) -> String {
    format!("'{}'", escape_sql_string(value))
}

pub(crate) fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

pub(crate) fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

pub(crate) fn sql_literal(value: &Value) -> String {
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
