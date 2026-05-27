use serde_json::Value as JsonValue;

use crate::common::json_pointer_get;
use crate::LixError;
use musli::{Allocator, Context, Decode, Decoder, Encode, Encoder};

/// Logical entity primary key derived from a schema primary key.
///
/// Keep this as typed tuple data inside engine. SQL `entity_pk` surfaces
/// should use the JSON-array projection.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub(crate) struct EntityPk {
    pub(crate) parts: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EntityPkError {
    EmptyPrimaryKey,
    EmptyPrimaryKeyPath { index: usize },
    MissingPrimaryKeyValue { index: usize },
    UnsupportedPrimaryKeyValue { index: usize },
    InvalidEncodedEntityPk,
}

impl std::fmt::Display for EntityPkError {
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
            Self::MissingPrimaryKeyValue { index } => {
                write!(formatter, "primary-key value at index {index} is missing")
            }
            Self::UnsupportedPrimaryKeyValue { index } => write!(
                formatter,
                "primary-key value at index {index} must be a JSON string"
            ),
            Self::InvalidEncodedEntityPk => {
                write!(
                    formatter,
                    "encoded entity primary key must be a non-empty JSON array of strings"
                )
            }
        }
    }
}

impl EntityPk {
    pub(crate) fn single(value: impl Into<String>) -> Self {
        Self {
            parts: vec![value.into()],
        }
    }

    pub(crate) fn from_parts(parts: Vec<String>) -> Result<Self, EntityPkError> {
        validate_parts(&parts)?;
        Ok(Self { parts })
    }

    #[cfg(test)]
    pub(crate) fn tuple(parts: Vec<String>) -> Result<Self, EntityPkError> {
        Self::from_parts(parts)
    }

    pub(crate) fn from_primary_key_paths(
        snapshot: &JsonValue,
        primary_key_paths: &[Vec<String>],
    ) -> Result<Self, EntityPkError> {
        if primary_key_paths.is_empty() {
            return Err(EntityPkError::EmptyPrimaryKey);
        }

        let mut parts = Vec::with_capacity(primary_key_paths.len());
        for (index, path) in primary_key_paths.iter().enumerate() {
            if path.is_empty() {
                return Err(EntityPkError::EmptyPrimaryKeyPath { index });
            }
            let Some(value) = json_pointer_get(snapshot, path) else {
                return Err(EntityPkError::MissingPrimaryKeyValue { index });
            };
            parts.push(string_part_from_json_value(value, index)?);
        }

        Ok(Self { parts })
    }

    pub(crate) fn as_json_array_value(&self) -> Result<JsonValue, LixError> {
        if self.parts.is_empty() {
            return Err(LixError::unknown(
                "entity primary key must contain at least one primary-key part",
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
            LixError::unknown(format!("failed to encode entity pk as JSON: {error}"))
        })
    }

    pub(crate) fn as_single_string(&self) -> Result<&str, LixError> {
        if self.parts.is_empty() {
            return Err(LixError::unknown(
                "entity primary key must contain at least one primary-key part",
            ));
        }

        if let [value] = self.parts.as_slice() {
            return Ok(value.as_str());
        }

        Err(LixError::unknown(
            "entity primary key is not a single string primary-key tuple",
        ))
    }

    pub(crate) fn as_single_string_owned(&self) -> Result<String, LixError> {
        Ok(self.as_single_string()?.to_owned())
    }

    pub(crate) fn from_json_array_text(entity_pk: &str) -> Result<Self, EntityPkError> {
        let value = serde_json::from_str::<JsonValue>(entity_pk)
            .map_err(|_| EntityPkError::InvalidEncodedEntityPk)?;
        Self::from_json_array_value(&value)
    }

    pub(crate) fn from_json_array_value(entity_pk: &JsonValue) -> Result<Self, EntityPkError> {
        let JsonValue::Array(values) = entity_pk else {
            return Err(EntityPkError::InvalidEncodedEntityPk);
        };
        if values.is_empty() {
            return Err(EntityPkError::EmptyPrimaryKey);
        }

        let mut parts = Vec::with_capacity(values.len());
        for (index, value) in values.iter().enumerate() {
            parts.push(string_part_from_json_value(value, index)?);
        }
        Ok(Self { parts })
    }
}

impl<M> Encode<M> for EntityPk {
    type Encode = [String];

    fn encode<E>(&self, encoder: E) -> Result<(), E::Error>
    where
        E: Encoder<Mode = M>,
    {
        encoder.encode(self.parts.as_slice())
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.parts.len())
    }

    fn as_encode(&self) -> &Self::Encode {
        self.parts.as_slice()
    }
}

impl<'de, M, A> Decode<'de, M, A> for EntityPk
where
    A: Allocator,
{
    fn decode<D>(decoder: D) -> Result<Self, D::Error>
    where
        D: Decoder<'de, Mode = M, Allocator = A>,
    {
        let cx = decoder.cx();
        let parts = Vec::<String>::decode(decoder)?;
        Self::from_parts(parts).map_err(|error| {
            cx.message(format_args!(
                "entity primary key decoded from storage is invalid: {error}"
            ))
        })
    }
}

fn validate_parts(parts: &[String]) -> Result<(), EntityPkError> {
    if parts.is_empty() {
        return Err(EntityPkError::EmptyPrimaryKey);
    }
    Ok(())
}

fn string_part_from_json_value(value: &JsonValue, index: usize) -> Result<String, EntityPkError> {
    match value {
        JsonValue::String(value) => Ok(value.clone()),
        _ => Err(EntityPkError::UnsupportedPrimaryKeyValue { index }),
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
        let identity = EntityPk::single("plain-id");

        assert_eq!(
            identity.as_single_string().expect("projection should work"),
            "plain-id"
        );
    }

    #[test]
    fn single_identity_projects_to_json_array_entity_pk() {
        let identity = EntityPk::single("plain-id");

        assert_eq!(
            identity
                .as_json_array_text()
                .expect("projection should work"),
            "[\"plain-id\"]"
        );
    }

    #[test]
    fn composite_identity_projects_to_json_array_entity_pk() {
        let identity = EntityPk::tuple(vec!["namespace".to_string(), "42".to_string()])
            .expect("tuple identity");

        assert_eq!(
            identity
                .as_json_array_text()
                .expect("projection should work"),
            "[\"namespace\",\"42\"]"
        );
    }

    #[test]
    fn entity_pk_json_array_roundtrips() {
        let identity = EntityPk::tuple(vec!["namespace".to_string(), "42".to_string()])
            .expect("tuple identity");
        let encoded = identity
            .as_json_array_text()
            .expect("projection should work");

        assert_eq!(
            EntityPk::from_json_array_text(&encoded).expect("decode should work"),
            identity
        );
    }

    #[test]
    fn entity_pk_json_array_allows_empty_string_part() {
        assert_eq!(
            EntityPk::from_json_array_text("[\"\"]").expect("empty string is a valid part"),
            EntityPk::single("")
        );
    }

    #[test]
    fn tuple_allows_empty_string_part() {
        assert_eq!(
            EntityPk::tuple(vec!["namespace".to_string(), "".to_string()])
                .expect("empty string is a valid part"),
            EntityPk {
                parts: vec!["namespace".to_string(), "".to_string()],
            }
        );
    }

    #[test]
    fn entity_pk_json_array_does_not_collide_on_delimiter_like_values() {
        let left =
            EntityPk::tuple(vec!["a~b".to_string(), "c".to_string()]).expect("left tuple identity");
        let right = EntityPk::tuple(vec!["a".to_string(), "b~c".to_string()])
            .expect("right tuple identity");

        assert_ne!(
            left.as_json_array_text().expect("left should encode"),
            right.as_json_array_text().expect("right should encode")
        );
    }

    #[test]
    fn composite_identity_rejects_single_string_projection() {
        let identity = EntityPk::tuple(vec!["namespace".to_string(), "42".to_string()])
            .expect("tuple identity");

        assert!(identity.as_single_string().is_err());
    }

    #[test]
    fn composite_identity_does_not_collide_on_delimiter_like_values() {
        let left =
            EntityPk::tuple(vec!["a~b".to_string(), "1".to_string()]).expect("left tuple identity");
        let right = EntityPk::tuple(vec!["a".to_string(), "b~1".to_string()])
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

        let identity = EntityPk::from_primary_key_paths(
            &snapshot,
            &[vec!["namespace".to_string()], vec!["locale".to_string()]],
        )
        .expect("primary key should derive");

        assert_eq!(
            identity,
            EntityPk {
                parts: vec!["messages".to_string(), "en".to_string()],
            }
        );
    }

    #[test]
    fn entity_pk_json_array_rejects_non_string_parts() {
        assert_eq!(
            EntityPk::from_json_array_text("[\"namespace\",42]"),
            Err(EntityPkError::UnsupportedPrimaryKeyValue { index: 1 })
        );
        assert_eq!(
            EntityPk::from_json_array_text("[\"namespace\",null]"),
            Err(EntityPkError::UnsupportedPrimaryKeyValue { index: 1 })
        );
        assert_eq!(
            EntityPk::from_json_array_text("[[\"nested\"]]"),
            Err(EntityPkError::UnsupportedPrimaryKeyValue { index: 0 })
        );
    }

    #[test]
    fn from_primary_key_paths_rejects_non_string_parts() {
        let snapshot = json!({
            "namespace": "messages",
            "index": 7
        });

        assert_eq!(
            EntityPk::from_primary_key_paths(
                &snapshot,
                &[vec!["namespace".to_string()], vec!["index".to_string()],],
            ),
            Err(EntityPkError::UnsupportedPrimaryKeyValue { index: 1 })
        );
    }

    #[test]
    fn from_primary_key_paths_allows_empty_string_parts() {
        let snapshot = json!({
            "namespace": "messages",
            "id": ""
        });

        assert_eq!(
            EntityPk::from_primary_key_paths(
                &snapshot,
                &[vec!["namespace".to_string()], vec!["id".to_string()],],
            )
            .expect("empty string is a valid primary-key value"),
            EntityPk {
                parts: vec!["messages".to_string(), "".to_string()],
            }
        );
    }

    #[test]
    fn from_primary_key_paths_rejects_nested_json_parts() {
        let snapshot = json!({
            "entity_pk": ["welcome.title", "en"],
            "schema_key": "message"
        });

        assert_eq!(
            EntityPk::from_primary_key_paths(
                &snapshot,
                &[
                    vec!["entity_pk".to_string()],
                    vec!["schema_key".to_string()],
                ],
            ),
            Err(EntityPkError::UnsupportedPrimaryKeyValue { index: 0 })
        );
    }

    #[test]
    fn from_primary_key_paths_rejects_missing_parts() {
        let snapshot = json!({ "id": "a" });

        assert_eq!(
            EntityPk::from_primary_key_paths(&snapshot, &[vec!["missing".to_string()]]),
            Err(EntityPkError::MissingPrimaryKeyValue { index: 0 })
        );
    }

    #[test]
    fn storage_codec_roundtrips_entity_pk() {
        let identity =
            EntityPk::tuple(vec!["namespace".to_string(), "id".to_string()]).expect("entity pk");
        let bytes = crate::storage_codec::encode("entity primary key", &identity)
            .expect("entity pk should encode");

        let decoded: EntityPk = crate::storage_codec::decode("entity primary key", &bytes)
            .expect("entity pk should decode");

        assert_eq!(decoded, identity);
    }

    #[test]
    fn storage_codec_rejects_empty_entity_pk() {
        let empty_parts: &[String] = &[];
        let bytes = crate::storage_codec::encode("entity primary key parts", empty_parts)
            .expect("empty parts should encode");

        let error = crate::storage_codec::decode::<EntityPk>("entity primary key", &bytes)
            .expect_err("empty entity primary key should reject");

        assert!(error
            .message
            .contains("entity primary key decoded from storage is invalid"));
    }
}
