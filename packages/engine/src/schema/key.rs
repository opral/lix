use serde_json::Value as JsonValue;

use crate::entity_identity::EntityIdentity;
use crate::LixError;

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
    let object = schema.as_object().ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        message: "schema definition must be a JSON object".to_string(),
        hint: None,
        details: None,
    })?;
    let schema_key = object
        .get("x-lix-key")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: "schema definition must include string x-lix-key".to_string(),
            hint: None,
            details: None,
        })?;

    Ok(SchemaKey::new(schema_key.to_string()))
}

pub fn schema_from_registered_snapshot(
    snapshot: &JsonValue,
) -> Result<(SchemaKey, JsonValue), LixError> {
    let value = snapshot.get("value").ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        message: "registered schema snapshot_content missing value".to_string(),
        hint: None,
        details: None,
    })?;
    let value = value.as_object().ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        message: "registered schema snapshot_content value must be an object".to_string(),
        hint: None,
        details: None,
    })?;

    let schema_key = value
        .get("x-lix-key")
        .and_then(|value| value.as_str())
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: "registered schema value.x-lix-key must be string".to_string(),
            hint: None,
            details: None,
        })?;

    Ok((
        SchemaKey::new(schema_key.to_string()),
        JsonValue::Object(value.clone()),
    ))
}

pub(crate) fn registered_schema_entity_id(schema_key: &str) -> Result<EntityIdentity, LixError> {
    EntityIdentity::from_primary_key_paths(
        &serde_json::json!({
            "value": {
                "x-lix-key": schema_key,
            }
        }),
        &[vec!["value".to_string(), "x-lix-key".to_string()]],
    )
    .map_err(|error| {
        LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!("registered schema identity could not be derived for schema '{schema_key}': {error}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{schema_from_registered_snapshot, schema_key_from_definition, SchemaKey};

    #[test]
    fn schema_from_registered_snapshot_extracts_key_and_schema() {
        let snapshot = json!({
            "value": {
                "x-lix-key": "profile",
                "type": "object"
            }
        });

        let (key, schema) = schema_from_registered_snapshot(&snapshot).expect("schema is valid");
        assert_eq!(key, SchemaKey::new("profile"));
        assert_eq!(schema["type"], json!("object"));
    }

    #[test]
    fn schema_from_registered_snapshot_requires_value_object() {
        let snapshot = json!({});

        let err = schema_from_registered_snapshot(&snapshot).expect_err("should fail");
        assert!(err.message.contains("missing value"), "{err:?}");
    }

    #[test]
    fn schema_from_registered_snapshot_requires_string_key() {
        let snapshot = json!({
            "value": {
                "x-lix-key": 1,
            }
        });

        let err = schema_from_registered_snapshot(&snapshot).expect_err("should fail");
        assert!(err.message.contains("x-lix-key"), "{err:?}");
    }

    #[test]
    fn schema_key_from_definition_extracts_key() {
        let schema = json!({
            "x-lix-key": "users",
            "type": "object"
        });

        let key = schema_key_from_definition(&schema).expect("schema key");
        assert_eq!(key, SchemaKey::new("users"));
    }
}
