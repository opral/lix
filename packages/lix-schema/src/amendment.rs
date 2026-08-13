use crate::{Error, ErrorKind, Schema};

/// Validate the deliberately conservative Schema v1 amendment policy.
pub fn validate_amendment(previous: &Schema, next: &Schema) -> Result<(), Error> {
    previous.validate()?;
    next.validate()?;
    if previous.key != next.key {
        return amendment("/key", "schema identity cannot change");
    }
    if previous.schema != next.schema {
        return amendment("/$schema", "schema language cannot change in an amendment");
    }
    if previous.primary_key != next.primary_key
        || previous.unique != next.unique
        || previous.foreign_keys != next.foreign_keys
    {
        return amendment(
            "/",
            "primary-key, unique, and foreign-key constraints cannot change",
        );
    }
    if next.columns.len() < previous.columns.len() {
        return amendment("/columns", "columns cannot be removed");
    }
    for (index, old) in previous.columns.iter().enumerate() {
        let new = &next.columns[index];
        if old.name != new.name {
            return amendment(
                format!("/columns/{index}/name"),
                "existing columns cannot be renamed or reordered",
            );
        }
        if old.data_type != new.data_type
            || old.nullable != new.nullable
            || old.default_value != new.default_value
            || old.default_expression != new.default_expression
        {
            return amendment(
                format!("/columns/{index}"),
                "existing column semantics cannot change",
            );
        }
    }
    for (index, column) in next.columns.iter().enumerate().skip(previous.columns.len()) {
        if !column.nullable && column.default_value.is_none() && column.default_expression.is_none()
        {
            return amendment(
                format!("/columns/{index}"),
                "new columns must be nullable or provide a default",
            );
        }
    }
    Ok(())
}

fn amendment<T>(path: impl Into<String>, message: impl Into<String>) -> Result<T, Error> {
    Err(Error::new(ErrorKind::Amendment, path, message))
}
