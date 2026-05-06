use base64::Engine as _;
use serde_json::Value as JsonValue;

use crate::common::json_pointer_get;
use crate::LixError;

const COMPOSITE_ENTITY_ID_PREFIX: &str = "pk:v1:";

/// Logical entity identity derived from a schema primary key.
///
/// Keep this as typed tuple data inside engine. The string projection exists
/// only for SQL/canonical boundaries that still expose a single `entity_id`.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub(crate) struct EntityIdentity {
    pub(crate) parts: Vec<EntityIdentityPart>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(tag = "type", content = "value")]
pub(crate) enum EntityIdentityPart {
    String(String),
    Bool(bool),
    Number(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EntityIdentityError {
    EmptyPrimaryKey,
    EmptyPrimaryKeyPath { index: usize },
    MissingPrimaryKeyValue { index: usize },
    NullPrimaryKeyValue { index: usize },
    EmptyPrimaryKeyValue { index: usize },
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
            Self::MissingPrimaryKeyValue { index } => {
                write!(formatter, "primary-key value at index {index} is missing")
            }
            Self::NullPrimaryKeyValue { index } => {
                write!(
                    formatter,
                    "primary-key value at index {index} must not be null"
                )
            }
            Self::EmptyPrimaryKeyValue { index } => {
                write!(
                    formatter,
                    "primary-key string value at index {index} must not be empty"
                )
            }
            Self::UnsupportedPrimaryKeyValue { index } => write!(
                formatter,
                "primary-key value at index {index} must be a string, number, or boolean"
            ),
            Self::InvalidEncodedEntityIdentity => {
                write!(formatter, "encoded entity identity is invalid")
            }
        }
    }
}

impl EntityIdentity {
    pub(crate) fn single(value: impl Into<String>) -> Self {
        Self {
            parts: vec![EntityIdentityPart::String(value.into())],
        }
    }

    #[cfg(test)]
    pub(crate) fn tuple(parts: Vec<EntityIdentityPart>) -> Result<Self, EntityIdentityError> {
        if parts.is_empty() {
            return Err(EntityIdentityError::EmptyPrimaryKey);
        }
        Ok(Self { parts })
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
            parts.push(EntityIdentityPart::from_json_value(value, index)?);
        }

        Ok(Self { parts })
    }

    pub(crate) fn as_string(&self) -> Result<String, LixError> {
        if self.parts.is_empty() {
            return Err(LixError::unknown(
                "entity identity must contain at least one primary-key part",
            ));
        }

        if let [EntityIdentityPart::String(value)] = self.parts.as_slice() {
            return Ok(value.clone());
        }

        let payload = serde_json::to_vec(self).map_err(|error| {
            LixError::unknown(format!(
                "failed to encode composite entity identity: {error}"
            ))
        })?;
        Ok(format!(
            "{COMPOSITE_ENTITY_ID_PREFIX}{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload)
        ))
    }

    pub(crate) fn from_string(entity_id: &str) -> Result<Self, EntityIdentityError> {
        if let Some(encoded) = entity_id.strip_prefix(COMPOSITE_ENTITY_ID_PREFIX) {
            let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(encoded)
                .map_err(|_| EntityIdentityError::InvalidEncodedEntityIdentity)?;
            let identity = serde_json::from_slice::<Self>(&payload)
                .map_err(|_| EntityIdentityError::InvalidEncodedEntityIdentity)?;
            if identity.parts.is_empty() {
                return Err(EntityIdentityError::InvalidEncodedEntityIdentity);
            }
            return Ok(identity);
        }

        Ok(Self::single(entity_id))
    }
}

impl EntityIdentityPart {
    fn from_json_value(value: &JsonValue, index: usize) -> Result<Self, EntityIdentityError> {
        match value {
            JsonValue::Null => Err(EntityIdentityError::NullPrimaryKeyValue { index }),
            JsonValue::String(value) if value.is_empty() => {
                Err(EntityIdentityError::EmptyPrimaryKeyValue { index })
            }
            JsonValue::String(value) => Ok(Self::String(value.clone())),
            JsonValue::Bool(value) => Ok(Self::Bool(*value)),
            JsonValue::Number(value) => Ok(Self::Number(value.to_string())),
            JsonValue::Array(_) | JsonValue::Object(_) => {
                Err(EntityIdentityError::UnsupportedPrimaryKeyValue { index })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn single_string_identity_projects_to_plain_entity_id() {
        let identity = EntityIdentity::single("plain-id");

        assert_eq!(
            identity.as_string().expect("projection should work"),
            "plain-id"
        );
    }

    #[test]
    fn composite_identity_projects_to_versioned_opaque_entity_id() {
        let identity = EntityIdentity::tuple(vec![
            EntityIdentityPart::String("namespace".to_string()),
            EntityIdentityPart::String("key".to_string()),
        ])
        .expect("tuple identity");

        let encoded = identity.as_string().expect("projection should work");

        assert!(encoded.starts_with(COMPOSITE_ENTITY_ID_PREFIX));
        assert!(!encoded.contains("namespace~key"));
    }

    #[test]
    fn composite_identity_roundtrips_from_string() {
        let identity = EntityIdentity::tuple(vec![
            EntityIdentityPart::String("namespace".to_string()),
            EntityIdentityPart::Number("42".to_string()),
            EntityIdentityPart::Bool(true),
        ])
        .expect("tuple identity");

        let encoded = identity.as_string().expect("projection should work");

        assert_eq!(
            EntityIdentity::from_string(&encoded).expect("decode should work"),
            identity
        );
    }

    #[test]
    fn composite_identity_does_not_collide_on_delimiter_like_values() {
        let left = EntityIdentity::tuple(vec![
            EntityIdentityPart::String("a~b".to_string()),
            EntityIdentityPart::String("1".to_string()),
        ])
        .expect("left tuple identity");
        let right = EntityIdentity::tuple(vec![
            EntityIdentityPart::String("a".to_string()),
            EntityIdentityPart::String("b~1".to_string()),
        ])
        .expect("right tuple identity");

        assert_ne!(
            left.as_string().expect("left should encode"),
            right.as_string().expect("right should encode")
        );
    }

    #[test]
    fn from_string_treats_plain_string_as_single_string_identity() {
        assert_eq!(
            EntityIdentity::single("plain-id"),
            EntityIdentity::single("plain-id")
        );
    }

    #[test]
    fn from_primary_key_paths_derives_ordered_parts() {
        let snapshot = json!({
            "namespace": "messages",
            "index": 7,
            "active": true
        });

        let identity = EntityIdentity::from_primary_key_paths(
            &snapshot,
            &[
                vec!["namespace".to_string()],
                vec!["index".to_string()],
                vec!["active".to_string()],
            ],
        )
        .expect("primary key should derive");

        assert_eq!(
            identity,
            EntityIdentity {
                parts: vec![
                    EntityIdentityPart::String("messages".to_string()),
                    EntityIdentityPart::Number("7".to_string()),
                    EntityIdentityPart::Bool(true),
                ],
            }
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
