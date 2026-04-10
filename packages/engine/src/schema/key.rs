use serde_json::Value as JsonValue;

pub use crate::contracts::SchemaKey;
use crate::LixError;

pub fn schema_key_from_definition(schema: &JsonValue) -> Result<SchemaKey, LixError> {
    let object = schema.as_object().ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: "schema definition must be a JSON object".to_string(),
    })?;
    let schema_key = object
        .get("x-lix-key")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "schema definition must include string x-lix-key".to_string(),
        })?;
    let schema_version = object
        .get("x-lix-version")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "schema definition must include string x-lix-version".to_string(),
        })?;

    Ok(SchemaKey::new(
        schema_key.to_string(),
        schema_version.to_string(),
    ))
}

pub fn schema_from_registered_snapshot(
    snapshot: &JsonValue,
) -> Result<(SchemaKey, JsonValue), LixError> {
    let value = snapshot.get("value").ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: "registered schema snapshot_content missing value".to_string(),
    })?;
    let value = value.as_object().ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: "registered schema snapshot_content value must be an object".to_string(),
    })?;

    let schema_key = value
        .get("x-lix-key")
        .and_then(|value| value.as_str())
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "registered schema value.x-lix-key must be string".to_string(),
        })?;
    let schema_version = value
        .get("x-lix-version")
        .and_then(|value| value.as_str())
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "registered schema value.x-lix-version must be string".to_string(),
        })?;

    Ok((
        SchemaKey::new(schema_key.to_string(), schema_version.to_string()),
        JsonValue::Object(value.clone()),
    ))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{schema_from_registered_snapshot, schema_key_from_definition, SchemaKey};

    #[test]
    fn schema_key_entity_id_and_numeric_version() {
        let key = SchemaKey::new("users", "42");

        assert_eq!(key.entity_id(), "users~42");
        assert_eq!(key.version_number(), Some(42));
    }

    #[test]
    fn schema_key_non_numeric_version_returns_none() {
        let key = SchemaKey::new("users", "v2");

        assert_eq!(key.version_number(), None);
    }

    #[test]
    fn schema_from_registered_snapshot_extracts_key_and_schema() {
        let snapshot = json!({
            "value": {
                "x-lix-key": "profile",
                "x-lix-version": "1",
                "type": "object"
            }
        });

        let (key, schema) = schema_from_registered_snapshot(&snapshot).expect("schema is valid");
        assert_eq!(key, SchemaKey::new("profile", "1"));
        assert_eq!(schema["type"], json!("object"));
    }

    #[test]
    fn schema_from_registered_snapshot_requires_value_object() {
        let snapshot = json!({});

        let err = schema_from_registered_snapshot(&snapshot).expect_err("should fail");
        assert!(err.description.contains("missing value"), "{err:?}");
    }

    #[test]
    fn schema_from_registered_snapshot_requires_string_key() {
        let snapshot = json!({
            "value": {
                "x-lix-key": 1,
                "x-lix-version": "1"
            }
        });

        let err = schema_from_registered_snapshot(&snapshot).expect_err("should fail");
        assert!(err.description.contains("x-lix-key"), "{err:?}");
    }

    #[test]
    fn schema_key_from_definition_extracts_key_and_version() {
        let schema = json!({
            "x-lix-key": "users",
            "x-lix-version": "2",
            "type": "object"
        });

        let key = schema_key_from_definition(&schema).expect("schema key");
        assert_eq!(key, SchemaKey::new("users", "2"));
    }
}
