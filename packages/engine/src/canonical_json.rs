use std::fmt;
use std::ops::Deref;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::LixError;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CanonicalJson {
    text: String,
}

impl CanonicalJson {
    pub fn from_text(text: impl AsRef<str>) -> Result<Self, LixError> {
        let value = serde_json::from_str::<serde_json::Value>(text.as_ref()).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("invalid JSON payload: {error}"),
            )
        })?;
        Self::from_value(value)
    }

    pub fn from_value(value: serde_json::Value) -> Result<Self, LixError> {
        let normalized = canonicalize_value(&value);
        let text = serde_json::to_string(&normalized).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("failed to serialize canonical JSON payload: {error}"),
            )
        })?;
        Ok(Self { text })
    }

    pub fn from_serializable<T: Serialize>(value: &T) -> Result<Self, LixError> {
        let value = serde_json::to_value(value).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("failed to convert payload into JSON value: {error}"),
            )
        })?;
        Self::from_value(value)
    }

    pub fn as_str(&self) -> &str {
        &self.text
    }

    pub fn into_string(self) -> String {
        self.text
    }

    pub fn to_value(&self) -> Result<serde_json::Value, LixError> {
        serde_json::from_str(&self.text).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("stored canonical JSON payload is invalid: {error}"),
            )
        })
    }

    pub fn parse<T: DeserializeOwned>(&self) -> Result<T, LixError> {
        serde_json::from_str(&self.text).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("stored canonical JSON payload could not be decoded: {error}"),
            )
        })
    }
}

impl fmt::Display for CanonicalJson {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}

impl Deref for CanonicalJson {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl AsRef<str> for CanonicalJson {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl From<CanonicalJson> for String {
    fn from(value: CanonicalJson) -> Self {
        value.into_string()
    }
}

impl Serialize for CanonicalJson {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.text)
    }
}

impl<'de> Deserialize<'de> for CanonicalJson {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let text = String::deserialize(deserializer)?;
        Self::from_text(text).map_err(serde::de::Error::custom)
    }
}

impl TryFrom<String> for CanonicalJson {
    type Error = LixError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_text(value)
    }
}

impl TryFrom<&str> for CanonicalJson {
    type Error = LixError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::from_text(value)
    }
}

fn canonicalize_value(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(canonicalize_value).collect())
        }
        serde_json::Value::Object(map) => {
            let mut keys = map.keys().cloned().collect::<Vec<_>>();
            keys.sort();
            let mut out = serde_json::Map::new();
            for key in keys {
                if let Some(entry) = map.get(&key) {
                    out.insert(key, canonicalize_value(entry));
                }
            }
            serde_json::Value::Object(out)
        }
        _ => value.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::CanonicalJson;
    use serde_json::json;

    #[test]
    fn canonicalizes_object_keys_deterministically() {
        let canonical =
            CanonicalJson::from_text(r#"{"b":2,"a":1}"#).expect("canonical json should parse");
        assert_eq!(canonical.as_str(), r#"{"a":1,"b":2}"#);
    }

    #[test]
    fn canonicalizes_nested_objects() {
        let canonical = CanonicalJson::from_text(r#"{"z":{"b":2,"a":1},"a":[{"d":4,"c":3}]}"#)
            .expect("canonical json should parse");
        assert_eq!(
            canonical.as_str(),
            r#"{"a":[{"c":3,"d":4}],"z":{"a":1,"b":2}}"#
        );
    }

    #[test]
    fn preserves_scalar_json_values() {
        let canonical =
            CanonicalJson::from_value(json!("hello")).expect("canonical json should encode");
        assert_eq!(canonical.as_str(), r#""hello""#);
    }
}
