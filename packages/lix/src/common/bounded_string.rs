use std::fmt;

use serde::Deserialize;
use serde::de::Visitor;

/// An owned string whose retained bytes are capped during deserialization.
/// The deserializer may use parser scratch for escaped strings; callers must
/// bound the source body to bound that transient storage as well.
pub(crate) struct BoundedString<const MAX_BYTES: usize>(pub(crate) String);

impl<'de, const MAX_BYTES: usize> Deserialize<'de> for BoundedString<MAX_BYTES> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct StringVisitor<const MAX_BYTES: usize>;

        impl<'de, const MAX_BYTES: usize> Visitor<'de> for StringVisitor<MAX_BYTES> {
            type Value = BoundedString<MAX_BYTES>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "a string of at most {MAX_BYTES} bytes")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if value.len() > MAX_BYTES {
                    return Err(E::custom(format!("string exceeds {MAX_BYTES} byte limit")));
                }
                Ok(BoundedString(value.to_owned()))
            }

            fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_str(value)
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if value.len() > MAX_BYTES {
                    return Err(E::custom(format!("string exceeds {MAX_BYTES} byte limit")));
                }
                Ok(BoundedString(value))
            }
        }

        deserializer.deserialize_str(StringVisitor::<MAX_BYTES>)
    }
}

pub(crate) fn deserialize_bounded_string<'de, D, const MAX_BYTES: usize>(
    deserializer: D,
) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    BoundedString::<MAX_BYTES>::deserialize(deserializer).map(|value| value.0)
}

pub(crate) fn deserialize_optional_bounded_string<'de, D, const MAX_BYTES: usize>(
    deserializer: D,
) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<BoundedString<MAX_BYTES>>::deserialize(deserializer)
        .map(|value| value.map(|value| value.0))
}
