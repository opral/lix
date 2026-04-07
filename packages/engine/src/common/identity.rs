use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value as JsonValue;

use crate::LixError;

macro_rules! canonical_identity_type {
    ($name:ident, $label:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, LixError> {
                let value = value.into();
                if value.is_empty() {
                    return Err(LixError::unknown(format!("{} must be non-empty", $label)));
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = LixError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = LixError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl Deref for $name {
            type Target = str;

            fn deref(&self) -> &Self::Target {
                self.0.as_str()
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.0.as_str()
            }
        }

        impl Borrow<str> for $name {
            fn borrow(&self) -> &str {
                self.0.as_str()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }

        impl PartialEq<&str> for $name {
            fn eq(&self, other: &&str) -> bool {
                self.0 == *other
            }
        }

        impl PartialEq<$name> for &str {
            fn eq(&self, other: &$name) -> bool {
                *self == other.0
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(serde::de::Error::custom)
            }
        }
    };
}

canonical_identity_type!(EntityId, "entity_id");
canonical_identity_type!(FileId, "file_id");
canonical_identity_type!(VersionId, "version_id");
canonical_identity_type!(CanonicalSchemaKey, "schema_key");
canonical_identity_type!(CanonicalSchemaVersion, "schema_version");
canonical_identity_type!(CanonicalPluginKey, "plugin_key");

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EntityIdDerivationError {
    EmptyPrimaryKeyPath { index: usize },
    MissingPrimaryKeyValue { index: usize },
    NullPrimaryKeyValue { index: usize },
    EmptyPrimaryKeyValue { index: usize },
}

pub(crate) fn derive_entity_id_from_json_paths(
    snapshot: &JsonValue,
    primary_key_paths: &[Vec<String>],
) -> Result<EntityId, EntityIdDerivationError> {
    let mut parts = Vec::with_capacity(primary_key_paths.len());
    for (index, path) in primary_key_paths.iter().enumerate() {
        if path.is_empty() {
            return Err(EntityIdDerivationError::EmptyPrimaryKeyPath { index });
        }
        let Some(value) = json_pointer_get(snapshot, path) else {
            return Err(EntityIdDerivationError::MissingPrimaryKeyValue { index });
        };
        parts.push(entity_id_component_from_json_value(value, index)?);
    }

    if parts.len() == 1 {
        EntityId::new(parts.pop().expect("single primary-key part"))
            .map_err(|_| EntityIdDerivationError::EmptyPrimaryKeyValue { index: 0 })
    } else {
        EntityId::new(parts.join("~"))
            .map_err(|_| EntityIdDerivationError::EmptyPrimaryKeyValue { index: 0 })
    }
}

fn entity_id_component_from_json_value(
    value: &JsonValue,
    index: usize,
) -> Result<String, EntityIdDerivationError> {
    match value {
        JsonValue::Null => Err(EntityIdDerivationError::NullPrimaryKeyValue { index }),
        JsonValue::String(text) if text.is_empty() => {
            Err(EntityIdDerivationError::EmptyPrimaryKeyValue { index })
        }
        JsonValue::String(text) => Ok(text.clone()),
        JsonValue::Bool(flag) => Ok(flag.to_string()),
        JsonValue::Number(number) => Ok(number.to_string()),
        JsonValue::Array(_) | JsonValue::Object(_) => Ok(value.to_string()),
    }
}

pub(crate) fn json_pointer_get<'a>(
    value: &'a JsonValue,
    pointer: &[String],
) -> Option<&'a JsonValue> {
    let mut current = value;
    for segment in pointer {
        match current {
            JsonValue::Object(object) => current = object.get(segment)?,
            JsonValue::Array(array) => {
                let index = segment.parse::<usize>().ok()?;
                current = array.get(index)?;
            }
            _ => return None,
        }
    }
    Some(current)
}
