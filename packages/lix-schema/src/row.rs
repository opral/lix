use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::{DataType, Error, ErrorKind, Schema};

#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Null,
    Text(String),
    Uuid(uuid::Uuid),
    Int8(i64),
    Float8(f64),
    Boolean(bool),
    /// JSON null is represented as `Jsonb(Value::Null)`, not `SqlValue::Null`.
    Jsonb(Value),
    /// Signed UTC microseconds since the Unix epoch.
    Timestamptz(i64),
}

pub type SqlRow = BTreeMap<String, SqlValue>;

#[derive(Debug, Clone)]
pub struct CompiledSchema {
    columns: BTreeMap<String, CompiledColumn>,
}

#[derive(Debug, Clone)]
struct CompiledColumn {
    data_type: DataType,
    nullable: bool,
    has_default: bool,
}

impl CompiledSchema {
    pub fn compile(schema: &Schema) -> Result<Self, Error> {
        schema.validate()?;
        Ok(Self {
            columns: schema
                .columns
                .iter()
                .map(|column| {
                    (
                        column.name.clone(),
                        CompiledColumn {
                            data_type: column.data_type,
                            nullable: column.nullable,
                            has_default: column.default_value.is_some()
                                || column.default_expression.is_some(),
                        },
                    )
                })
                .collect(),
        })
    }

    pub fn validate_row(&self, row: &SqlRow) -> Result<(), Error> {
        let supplied = row.keys().map(String::as_str).collect::<BTreeSet<_>>();
        for name in supplied {
            if !self.columns.contains_key(name) {
                return row_error(format!("/{name}"), "unknown column");
            }
        }
        for (name, column) in &self.columns {
            let Some(value) = row.get(name) else {
                if column.nullable || column.has_default {
                    continue;
                }
                return row_error(
                    format!("/{name}"),
                    "missing NOT NULL column without a default",
                );
            };
            if matches!(value, SqlValue::Null) {
                if column.nullable {
                    continue;
                }
                return row_error(format!("/{name}"), "must not be SQL NULL");
            }
            if !value_matches(column.data_type, value) {
                return row_error(
                    format!("/{name}"),
                    format!(
                        "expected PostgreSQL type '{}'",
                        column.data_type.postgres_name()
                    ),
                );
            }
        }
        Ok(())
    }

    /// Validate the canonical JSON object used for a row snapshot.
    ///
    /// A JSON `null` in a JSONB column is a JSONB value. In every other column
    /// it represents SQL `NULL` and therefore requires a nullable column.
    pub fn validate(&self, value: &Value) -> Result<(), Error> {
        let object = value
            .as_object()
            .ok_or_else(|| Error::new(ErrorKind::Row, "/", "row snapshot must be an object"))?;
        for name in object.keys() {
            if !self.columns.contains_key(name) {
                return row_error(format!("/{name}"), "unknown column");
            }
        }
        for (name, column) in &self.columns {
            let Some(value) = object.get(name) else {
                if column.nullable || column.has_default {
                    continue;
                }
                return row_error(
                    format!("/{name}"),
                    "missing NOT NULL column without a default",
                );
            };
            if !json_value_matches(column.data_type, column.nullable, value) {
                return row_error(
                    format!("/{name}"),
                    format!(
                        "expected PostgreSQL type '{}'{}",
                        column.data_type.postgres_name(),
                        if column.nullable { " or SQL NULL" } else { "" }
                    ),
                );
            }
        }
        Ok(())
    }

    pub fn is_valid(&self, value: &Value) -> bool {
        self.validate(value).is_ok()
    }
}

fn value_matches(data_type: DataType, value: &SqlValue) -> bool {
    match (data_type, value) {
        (DataType::Text, SqlValue::Text(_))
        | (DataType::Uuid, SqlValue::Uuid(_))
        | (DataType::Int8, SqlValue::Int8(_))
        | (DataType::Boolean, SqlValue::Boolean(_))
        | (DataType::Jsonb, SqlValue::Jsonb(_))
        | (DataType::Timestamptz, SqlValue::Timestamptz(_)) => true,
        (DataType::Float8, SqlValue::Float8(value)) => value.is_finite(),
        _ => false,
    }
}

fn json_value_matches(data_type: DataType, nullable: bool, value: &Value) -> bool {
    if value.is_null() {
        return data_type == DataType::Jsonb || nullable;
    }
    match data_type {
        DataType::Text => value.is_string(),
        DataType::Uuid => value
            .as_str()
            .is_some_and(|value| uuid::Uuid::parse_str(value).is_ok()),
        DataType::Int8 => value.as_i64().is_some(),
        DataType::Float8 => value.as_f64().is_some_and(f64::is_finite),
        DataType::Boolean => value.is_boolean(),
        DataType::Jsonb => true,
        DataType::Timestamptz => value.as_str().is_some_and(is_rfc3339_timestamp),
    }
}

pub(crate) fn is_rfc3339_timestamp(value: &str) -> bool {
    chrono::DateTime::parse_from_rfc3339(value).is_ok()
}

fn row_error<T>(path: impl Into<String>, message: impl Into<String>) -> Result<T, Error> {
    Err(Error::new(ErrorKind::Row, path, message))
}
