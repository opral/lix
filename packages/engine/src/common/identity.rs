use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;

use crate::LixError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

macro_rules! canonical_identity_type {
    ($name:ident, $label:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, LixError> {
                let value = value.into();
                validate_non_empty_identity_value($label, value).map(Self)
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

canonical_identity_type!(EntityPk, "entity_pk");
canonical_identity_type!(FileId, "file_id");
canonical_identity_type!(BranchId, "branch_id");
canonical_identity_type!(CanonicalSchemaKey, "schema_key");
canonical_identity_type!(CanonicalPluginKey, "plugin_key");

pub(crate) fn validate_non_empty_identity_value(
    label: &str,
    value: impl Into<String>,
) -> Result<String, LixError> {
    let value = value.into();
    if value.is_empty() {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("{label} must be non-empty"),
        ));
    }
    Ok(value)
}

pub(crate) fn json_pointer_get<'a>(
    value: &'a serde_json::Value,
    pointer: &[String],
) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for segment in pointer {
        match current {
            serde_json::Value::Object(object) => current = object.get(segment)?,
            serde_json::Value::Array(array) => {
                let index = segment.parse::<usize>().ok()?;
                current = array.get(index)?;
            }
            _ => return None,
        }
    }
    Some(current)
}
