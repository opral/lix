use std::collections::BTreeSet;

use serde_json::Value;

use crate::{Column, DataType, Error, ErrorKind, SCHEMA_V1_URI, Schema};

pub(crate) fn validate_schema(schema: &Schema) -> Result<(), Error> {
    if schema.schema != SCHEMA_V1_URI {
        return definition("/$schema", format!("must equal '{SCHEMA_V1_URI}'"));
    }
    validate_identifier(&schema.key, "/key")?;
    if schema.columns.is_empty() {
        return definition("/columns", "must contain at least one column");
    }

    let mut names = BTreeSet::new();
    for (index, column) in schema.columns.iter().enumerate() {
        let path = format!("/columns/{index}");
        validate_identifier(&column.name, &format!("{path}/name"))?;
        if !names.insert(column.name.as_str()) {
            return definition(format!("{path}/name"), "duplicates an earlier column");
        }
        validate_column(column, &path)?;
    }

    validate_column_group(&schema.primary_key, &names, "/primary_key")?;
    for column in &schema.primary_key {
        let value = schema
            .columns
            .iter()
            .find(|candidate| &candidate.name == column)
            .unwrap();
        if !matches!(
            value.data_type,
            DataType::Text | DataType::Uuid | DataType::Int8
        ) {
            return definition(
                "/primary_key",
                format!("primary-key column '{column}' must use text, uuid, or int8"),
            );
        }
        if value.nullable {
            return definition(
                "/primary_key",
                format!("primary-key column '{column}' must set nullable to false"),
            );
        }
    }
    let mut groups = BTreeSet::new();
    for (index, group) in schema.unique.iter().enumerate() {
        validate_column_group(group, &names, &format!("/unique/{index}"))?;
        if !groups.insert(group.clone()) {
            return definition(
                format!("/unique/{index}"),
                "duplicates an earlier unique constraint",
            );
        }
    }
    for (index, foreign_key) in schema.foreign_keys.iter().enumerate() {
        let path = format!("/foreign_keys/{index}");
        validate_column_group(&foreign_key.columns, &names, &format!("{path}/columns"))?;
        validate_identifier(
            &foreign_key.references.schema_key,
            &format!("{path}/references/schema_key"),
        )?;
        if foreign_key.references.columns.is_empty() {
            return definition(format!("{path}/references/columns"), "must not be empty");
        }
        let mut referenced = BTreeSet::new();
        for (column_index, column) in foreign_key.references.columns.iter().enumerate() {
            validate_identifier(column, &format!("{path}/references/columns/{column_index}"))?;
            if !referenced.insert(column) {
                return definition(
                    format!("{path}/references/columns/{column_index}"),
                    "duplicates an earlier column",
                );
            }
        }
        if foreign_key.columns.len() != foreign_key.references.columns.len() {
            return definition(path, "local and referenced column counts must match");
        }
    }
    Ok(())
}

fn validate_column(column: &Column, path: &str) -> Result<(), Error> {
    if column.default_value.is_some() && column.default_expression.is_some() {
        return definition(
            path,
            "default_value and default_expression are mutually exclusive",
        );
    }
    if let Some(expression) = &column.default_expression {
        let valid = matches!(
            (column.data_type, expression.as_str()),
            (DataType::Uuid, "uuidv7()") | (DataType::Timestamptz, "CURRENT_TIMESTAMP")
        );
        if !valid {
            return definition(
                format!("{path}/default_expression"),
                "Schema v1 supports uuidv7() on uuid columns and CURRENT_TIMESTAMP on timestamptz columns",
            );
        }
    }
    if let Some(value) = &column.default_value {
        validate_default(column.data_type, value, &format!("{path}/default_value"))?;
        if value.is_null() && !column.nullable {
            return definition(
                format!("{path}/default_value"),
                "NULL default requires nullable true",
            );
        }
    }
    Ok(())
}

fn validate_default(data_type: DataType, value: &Value, path: &str) -> Result<(), Error> {
    if value.is_null() {
        return Ok(());
    }
    let valid = match data_type {
        DataType::Text => value.is_string(),
        DataType::Uuid => value
            .as_str()
            .is_some_and(|value| uuid::Uuid::parse_str(value).is_ok()),
        DataType::Int8 => value.as_i64().is_some(),
        DataType::Float8 => value.as_f64().is_some_and(f64::is_finite),
        DataType::Boolean => value.is_boolean(),
        DataType::Jsonb => true,
        DataType::Timestamptz => value.as_str().is_some_and(crate::row::is_rfc3339_timestamp),
    };
    if valid {
        Ok(())
    } else {
        definition(
            path,
            format!(
                "does not match PostgreSQL type '{}'",
                data_type.postgres_name()
            ),
        )
    }
}

fn validate_column_group(
    group: &[String],
    names: &BTreeSet<&str>,
    path: &str,
) -> Result<(), Error> {
    if group.is_empty() {
        return definition(path, "must not be empty");
    }
    let mut seen = BTreeSet::new();
    for (index, column) in group.iter().enumerate() {
        if !names.contains(column.as_str()) {
            return definition(
                format!("{path}/{index}"),
                format!("unknown column '{column}'"),
            );
        }
        if !seen.insert(column) {
            return definition(format!("{path}/{index}"), "duplicates an earlier column");
        }
    }
    Ok(())
}

fn validate_identifier(value: &str, path: &str) -> Result<(), Error> {
    if value.len() > 63 {
        return definition(
            path,
            "must be at most 63 UTF-8 bytes for PostgreSQL compatibility",
        );
    }
    let mut chars = value.chars();
    if !chars.next().is_some_and(|ch| ch.is_ascii_lowercase())
        || !chars.all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
    {
        return definition(path, "must be a snake_case PostgreSQL identifier");
    }
    Ok(())
}

fn definition<T>(path: impl Into<String>, message: impl Into<String>) -> Result<T, Error> {
    Err(Error::new(ErrorKind::Definition, path, message))
}
