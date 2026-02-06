use serde_json::Value as JsonValue;

use crate::LixError;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SchemaKey {
    pub schema_key: String,
    pub schema_version: String,
}

impl SchemaKey {
    pub fn new(schema_key: impl Into<String>, schema_version: impl Into<String>) -> Self {
        Self {
            schema_key: schema_key.into(),
            schema_version: schema_version.into(),
        }
    }

    pub fn entity_id(&self) -> String {
        format!("{}~{}", self.schema_key, self.schema_version)
    }

    pub fn version_number(&self) -> Option<u64> {
        self.schema_version.parse::<u64>().ok()
    }
}

pub fn schema_from_stored_snapshot(
    snapshot: &JsonValue,
) -> Result<(SchemaKey, JsonValue), LixError> {
    let value = snapshot.get("value").ok_or_else(|| LixError {
        message: "stored schema snapshot_content missing value".to_string(),
    })?;
    let value = value.as_object().ok_or_else(|| LixError {
        message: "stored schema snapshot_content value must be an object".to_string(),
    })?;

    let schema_key = value
        .get("x-lix-key")
        .and_then(|value| value.as_str())
        .ok_or_else(|| LixError {
            message: "stored schema value.x-lix-key must be string".to_string(),
        })?;
    let schema_version = value
        .get("x-lix-version")
        .and_then(|value| value.as_str())
        .ok_or_else(|| LixError {
            message: "stored schema value.x-lix-version must be string".to_string(),
        })?;

    Ok((
        SchemaKey::new(schema_key.to_string(), schema_version.to_string()),
        JsonValue::Object(value.clone()),
    ))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{schema_from_stored_snapshot, SchemaKey};

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
    fn schema_from_stored_snapshot_extracts_key_and_schema() {
        let snapshot = json!({
            "value": {
                "x-lix-key": "profile",
                "x-lix-version": "1",
                "type": "object"
            }
        });

        let (key, schema) = schema_from_stored_snapshot(&snapshot).expect("schema is valid");
        assert_eq!(key, SchemaKey::new("profile", "1"));
        assert_eq!(schema["type"], json!("object"));
    }

    #[test]
    fn schema_from_stored_snapshot_requires_value_object() {
        let snapshot = json!({});

        let err = schema_from_stored_snapshot(&snapshot).expect_err("should fail");
        assert!(err.message.contains("missing value"), "{err:?}");
    }

    #[test]
    fn schema_from_stored_snapshot_requires_string_key() {
        let snapshot = json!({
            "value": {
                "x-lix-key": 1,
                "x-lix-version": "1"
            }
        });

        let err = schema_from_stored_snapshot(&snapshot).expect_err("should fail");
        assert!(err.message.contains("x-lix-key"), "{err:?}");
    }
}
