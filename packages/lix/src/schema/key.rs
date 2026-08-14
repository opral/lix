use serde_json::Value as JsonValue;

use crate::LixError;
use crate::row_pk::RowPk;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SchemaKey {
    pub schema_key: String,
}

impl SchemaKey {
    pub fn new(schema_key: impl Into<String>) -> Self {
        Self {
            schema_key: schema_key.into(),
        }
    }
}

pub fn schema_key_from_definition(schema: &JsonValue) -> Result<SchemaKey, LixError> {
    let schema = super::definition::parse_lix_schema(schema)?;
    Ok(SchemaKey::new(schema.key))
}

pub fn schema_from_registered_snapshot(
    snapshot: &JsonValue,
) -> Result<(SchemaKey, JsonValue), LixError> {
    let schema_key = snapshot
        .get("schema_key")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                "registered schema snapshot missing string schema_key",
            )
        })?;
    let value = snapshot.get("value").cloned().ok_or_else(|| {
        LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            "registered schema snapshot missing value",
        )
    })?;
    let schema = super::definition::parse_lix_schema(&value)?;
    if schema.key != schema_key {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!(
                "registered schema schema_key '{schema_key}' does not match value.key '{}'",
                schema.key
            ),
        ));
    }
    Ok((SchemaKey::new(schema_key), value))
}

pub(crate) fn registered_schema_row_pk(schema_key: &str) -> Result<RowPk, LixError> {
    RowPk::from_primary_key_paths(
        &serde_json::json!({ "schema_key": schema_key }),
        &[vec!["schema_key".to_string()]],
    )
    .map_err(|error| {
        LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!("registered schema identity is invalid for '{schema_key}': {error}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn schema(key: &str) -> JsonValue {
        json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": key,
            "columns": [{
                "name": "id",
                "type": "text",
                "nullable": false
            }],
            "primary_key": ["id"]
        })
    }

    #[test]
    fn registered_snapshot_requires_explicit_schema_key() {
        let error = schema_from_registered_snapshot(&json!({
            "value": schema("acme_note")
        }))
        .expect_err("schema_key must not be inferred from value.key");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(error.message.contains("missing string schema_key"));
    }

    #[test]
    fn registered_snapshot_rejects_schema_key_value_key_mismatch() {
        let error = schema_from_registered_snapshot(&json!({
            "schema_key": "acme_task",
            "value": schema("acme_note")
        }))
        .expect_err("the relational key and document key must agree");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(error.message.contains("does not match value.key 'acme_note'"));
    }

    #[test]
    fn registered_snapshot_accepts_matching_schema_key_and_value_key() {
        let (key, value) = schema_from_registered_snapshot(&json!({
            "schema_key": "acme_note",
            "value": schema("acme_note")
        }))
        .expect("matching registry identity must be valid");

        assert_eq!(key.schema_key, "acme_note");
        assert_eq!(value["key"], "acme_note");
    }
}
