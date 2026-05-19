use serde_json::Value as JsonValue;

use crate::common::json_pointer_get;
use crate::LixError;

/// Logical entity identity derived from a schema primary key.
///
/// Keep this as typed tuple data inside engine. SQL `entity_id` surfaces
/// should use the JSON-array projection.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub(crate) struct EntityIdentity {
    pub(crate) parts: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EntityIdentityError {
    EmptyPrimaryKey,
    EmptyPrimaryKeyPath { index: usize },
    EmptyPrimaryKeyValue { index: usize },
    MissingPrimaryKeyValue { index: usize },
    UnsupportedPrimaryKeyValue { index: usize },
    InvalidEncodedEntityIdentity,
}

impl std::fmt::Display for EntityIdentityError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyPrimaryKey => {
                write!(formatter, "primary key must contain at least one path")
            }
            Self::EmptyPrimaryKeyPath { index } => {
                write!(
                    formatter,
                    "primary-key path at index {index} must not be empty"
                )
            }
            Self::EmptyPrimaryKeyValue { index } => {
                write!(
                    formatter,
                    "primary-key value at index {index} must not be empty"
                )
            }
            Self::MissingPrimaryKeyValue { index } => {
                write!(formatter, "primary-key value at index {index} is missing")
            }
            Self::UnsupportedPrimaryKeyValue { index } => write!(
                formatter,
                "primary-key value at index {index} must be a JSON string"
            ),
            Self::InvalidEncodedEntityIdentity => {
                write!(
                    formatter,
                    "encoded entity identity must be a non-empty JSON array of strings"
                )
            }
        }
    }
}

impl EntityIdentity {
    pub(crate) fn single(value: impl Into<String>) -> Self {
        Self {
            parts: vec![value.into()],
        }
    }

    pub(crate) fn from_parts(parts: Vec<String>) -> Result<Self, EntityIdentityError> {
        validate_parts(&parts)?;
        Ok(Self { parts })
    }

    #[cfg(test)]
    pub(crate) fn tuple(parts: Vec<String>) -> Result<Self, EntityIdentityError> {
        Self::from_parts(parts)
    }

    pub(crate) fn from_primary_key_paths(
        snapshot: &JsonValue,
        primary_key_paths: &[Vec<String>],
    ) -> Result<Self, EntityIdentityError> {
        if primary_key_paths.is_empty() {
            return Err(EntityIdentityError::EmptyPrimaryKey);
        }

        let mut parts = Vec::with_capacity(primary_key_paths.len());
        for (index, path) in primary_key_paths.iter().enumerate() {
            if path.is_empty() {
                return Err(EntityIdentityError::EmptyPrimaryKeyPath { index });
            }
            let Some(value) = json_pointer_get(snapshot, path) else {
                return Err(EntityIdentityError::MissingPrimaryKeyValue { index });
            };
            parts.push(string_part_from_json_value(value, index)?);
        }

        Ok(Self { parts })
    }

    pub(crate) fn as_json_array_value(&self) -> Result<JsonValue, LixError> {
        if self.parts.is_empty() {
            return Err(LixError::unknown(
                "entity identity must contain at least one primary-key part",
            ));
        }

        Ok(JsonValue::Array(
            self.parts
                .iter()
                .map(|part| JsonValue::String(part.clone()))
                .collect(),
        ))
    }

    pub(crate) fn as_json_array_text(&self) -> Result<String, LixError> {
        serde_json::to_string(&self.as_json_array_value()?).map_err(|error| {
            LixError::unknown(format!("failed to encode entity id as JSON: {error}"))
        })
    }

    pub(crate) fn as_single_string(&self) -> Result<&str, LixError> {
        if self.parts.is_empty() {
            return Err(LixError::unknown(
                "entity identity must contain at least one primary-key part",
            ));
        }

        if let [value] = self.parts.as_slice() {
            return Ok(value.as_str());
        }

        Err(LixError::unknown(
            "entity identity is not a single string primary-key tuple",
        ))
    }

    pub(crate) fn as_single_string_owned(&self) -> Result<String, LixError> {
        Ok(self.as_single_string()?.to_owned())
    }

    pub(crate) fn from_json_array_text(entity_id: &str) -> Result<Self, EntityIdentityError> {
        let value = serde_json::from_str::<JsonValue>(entity_id)
            .map_err(|_| EntityIdentityError::InvalidEncodedEntityIdentity)?;
        Self::from_json_array_value(&value)
    }

    pub(crate) fn from_json_array_value(
        entity_id: &JsonValue,
    ) -> Result<Self, EntityIdentityError> {
        let JsonValue::Array(values) = entity_id else {
            return Err(EntityIdentityError::InvalidEncodedEntityIdentity);
        };
        if values.is_empty() {
            return Err(EntityIdentityError::EmptyPrimaryKey);
        }

        let mut parts = Vec::with_capacity(values.len());
        for (index, value) in values.iter().enumerate() {
            parts.push(string_part_from_json_value(value, index)?);
        }
        Ok(Self { parts })
    }
}

fn validate_parts(parts: &[String]) -> Result<(), EntityIdentityError> {
    if parts.is_empty() {
        return Err(EntityIdentityError::EmptyPrimaryKey);
    }
    if let Some((index, _)) = parts.iter().enumerate().find(|(_, part)| part.is_empty()) {
        return Err(EntityIdentityError::EmptyPrimaryKeyValue { index });
    }
    Ok(())
}

fn string_part_from_json_value(
    value: &JsonValue,
    index: usize,
) -> Result<String, EntityIdentityError> {
    match value {
        JsonValue::String(value) if value.is_empty() => {
            Err(EntityIdentityError::EmptyPrimaryKeyValue { index })
        }
        JsonValue::String(value) => Ok(value.clone()),
        _ => Err(EntityIdentityError::UnsupportedPrimaryKeyValue { index }),
    }
}

pub(crate) fn canonical_json_text(value: &JsonValue) -> serde_json::Result<String> {
    serde_json::to_string(&canonical_json_value(value))
}

fn canonical_json_value(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Array(values) => {
            JsonValue::Array(values.iter().map(canonical_json_value).collect())
        }
        JsonValue::Object(object) => {
            let mut entries = object.iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));

            let mut canonical = serde_json::Map::new();
            for (key, value) in entries {
                canonical.insert(key.clone(), canonical_json_value(value));
            }
            JsonValue::Object(canonical)
        }
        _ => value.clone(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn single_string_identity_projects_to_single_string() {
        let identity = EntityIdentity::single("plain-id");

        assert_eq!(
            identity.as_single_string().expect("projection should work"),
            "plain-id"
        );
    }

    #[test]
    fn single_identity_projects_to_json_array_entity_id() {
        let identity = EntityIdentity::single("plain-id");

        assert_eq!(
            identity
                .as_json_array_text()
                .expect("projection should work"),
            "[\"plain-id\"]"
        );
    }

    #[test]
    fn composite_identity_projects_to_json_array_entity_id() {
        let identity = EntityIdentity::tuple(vec!["namespace".to_string(), "42".to_string()])
            .expect("tuple identity");

        assert_eq!(
            identity
                .as_json_array_text()
                .expect("projection should work"),
            "[\"namespace\",\"42\"]"
        );
    }

    #[test]
    fn entity_id_json_array_roundtrips() {
        let identity = EntityIdentity::tuple(vec!["namespace".to_string(), "42".to_string()])
            .expect("tuple identity");
        let encoded = identity
            .as_json_array_text()
            .expect("projection should work");

        assert_eq!(
            EntityIdentity::from_json_array_text(&encoded).expect("decode should work"),
            identity
        );
    }

    #[test]
    fn entity_id_json_array_rejects_empty_string_part() {
        assert_eq!(
            EntityIdentity::from_json_array_text("[\"\"]"),
            Err(EntityIdentityError::EmptyPrimaryKeyValue { index: 0 })
        );
    }

    #[test]
    fn tuple_rejects_empty_string_part() {
        assert_eq!(
            EntityIdentity::tuple(vec!["namespace".to_string(), "".to_string()]),
            Err(EntityIdentityError::EmptyPrimaryKeyValue { index: 1 })
        );
    }

    #[test]
    fn entity_id_json_array_does_not_collide_on_delimiter_like_values() {
        let left = EntityIdentity::tuple(vec!["a~b".to_string(), "c".to_string()])
            .expect("left tuple identity");
        let right = EntityIdentity::tuple(vec!["a".to_string(), "b~c".to_string()])
            .expect("right tuple identity");

        assert_ne!(
            left.as_json_array_text().expect("left should encode"),
            right.as_json_array_text().expect("right should encode")
        );
    }

    #[test]
    fn composite_identity_rejects_single_string_projection() {
        let identity = EntityIdentity::tuple(vec!["namespace".to_string(), "42".to_string()])
            .expect("tuple identity");

        assert!(identity.as_single_string().is_err());
    }

    #[test]
    fn composite_identity_does_not_collide_on_delimiter_like_values() {
        let left = EntityIdentity::tuple(vec!["a~b".to_string(), "1".to_string()])
            .expect("left tuple identity");
        let right = EntityIdentity::tuple(vec!["a".to_string(), "b~1".to_string()])
            .expect("right tuple identity");

        assert_ne!(
            left.as_json_array_text().expect("left should encode"),
            right.as_json_array_text().expect("right should encode")
        );
    }

    #[test]
    fn from_primary_key_paths_derives_ordered_parts() {
        let snapshot = json!({
            "namespace": "messages",
            "locale": "en"
        });

        let identity = EntityIdentity::from_primary_key_paths(
            &snapshot,
            &[vec!["namespace".to_string()], vec!["locale".to_string()]],
        )
        .expect("primary key should derive");

        assert_eq!(
            identity,
            EntityIdentity {
                parts: vec!["messages".to_string(), "en".to_string()],
            }
        );
    }

    #[test]
    fn entity_id_json_array_rejects_non_string_parts() {
        assert_eq!(
            EntityIdentity::from_json_array_text("[\"namespace\",42]"),
            Err(EntityIdentityError::UnsupportedPrimaryKeyValue { index: 1 })
        );
        assert_eq!(
            EntityIdentity::from_json_array_text("[\"namespace\",null]"),
            Err(EntityIdentityError::UnsupportedPrimaryKeyValue { index: 1 })
        );
        assert_eq!(
            EntityIdentity::from_json_array_text("[[\"nested\"]]"),
            Err(EntityIdentityError::UnsupportedPrimaryKeyValue { index: 0 })
        );
    }

    #[test]
    fn from_primary_key_paths_rejects_non_string_parts() {
        let snapshot = json!({
            "namespace": "messages",
            "index": 7
        });

        assert_eq!(
            EntityIdentity::from_primary_key_paths(
                &snapshot,
                &[vec!["namespace".to_string()], vec!["index".to_string()],],
            ),
            Err(EntityIdentityError::UnsupportedPrimaryKeyValue { index: 1 })
        );
    }

    #[test]
    fn from_primary_key_paths_rejects_empty_string_parts() {
        let snapshot = json!({
            "namespace": "messages",
            "id": ""
        });

        assert_eq!(
            EntityIdentity::from_primary_key_paths(
                &snapshot,
                &[vec!["namespace".to_string()], vec!["id".to_string()],],
            ),
            Err(EntityIdentityError::EmptyPrimaryKeyValue { index: 1 })
        );
    }

    #[test]
    fn from_primary_key_paths_rejects_nested_json_parts() {
        let snapshot = json!({
            "entity_id": ["welcome.title", "en"],
            "schema_key": "message"
        });

        assert_eq!(
            EntityIdentity::from_primary_key_paths(
                &snapshot,
                &[
                    vec!["entity_id".to_string()],
                    vec!["schema_key".to_string()],
                ],
            ),
            Err(EntityIdentityError::UnsupportedPrimaryKeyValue { index: 0 })
        );
    }

    #[test]
    fn from_primary_key_paths_rejects_missing_parts() {
        let snapshot = json!({ "id": "a" });

        assert_eq!(
            EntityIdentity::from_primary_key_paths(&snapshot, &[vec!["missing".to_string()]]),
            Err(EntityIdentityError::MissingPrimaryKeyValue { index: 0 })
        );
    }
}
