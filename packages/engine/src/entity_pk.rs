use base64::Engine as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value as JsonValue;
use smallvec::{SmallVec, smallvec};

use crate::LixError;
use crate::common::json_pointer_get;
use musli::{Allocator, Context, Decode, Decoder, Encode, Encoder};

/// Logical entity primary key derived from a schema primary key.
///
/// Components stay typed inside the engine and are projected to standard JSON
/// scalar representations only at API and SQL boundaries.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct EntityPk {
    pub(crate) components: SmallVec<[EntityPkComponent; 1]>,
}

/// Cross-type order is part of the physical key contract.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum EntityPkComponent {
    Uuid([u8; 16]),
    Integer(i64),
    String(Box<str>),
    Bytes(Box<[u8]>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EntityPkComponentType {
    Uuid,
    Integer,
    String,
    Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EntityPkError {
    EmptyPrimaryKey,
    EmptyPrimaryKeyPath {
        index: usize,
    },
    MissingPrimaryKeyValue {
        index: usize,
    },
    UnsupportedPrimaryKeyValue {
        index: usize,
    },
    InvalidPrimaryKeyValue {
        index: usize,
        expected: &'static str,
    },
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
            Self::UnsupportedPrimaryKeyValue { index } => {
                write!(
                    formatter,
                    "primary-key value at index {index} has unsupported JSON type"
                )
            }
            Self::InvalidPrimaryKeyValue { index, expected } => write!(
                formatter,
                "primary-key value at index {index} is not a valid {expected}"
            ),
            Self::InvalidEncodedEntityPk => {
                write!(
                    formatter,
                    "encoded entity primary key must be a non-empty JSON array of scalar values"
                )
            }
        }
    }
}

impl EntityPk {
    pub(crate) fn single(value: impl Into<String>) -> Self {
        Self {
            components: smallvec![EntityPkComponent::String(value.into().into_boxed_str())],
        }
    }

    pub(crate) fn from_parts(parts: Vec<String>) -> Result<Self, EntityPkError> {
        Self::from_components(
            parts
                .into_iter()
                .map(|part| EntityPkComponent::String(part.into_boxed_str()))
                .collect(),
        )
    }

    pub(crate) fn into_parts(self) -> Vec<String> {
        self.components
            .into_iter()
            .map(|component| component.external_string())
            .collect()
    }

    pub(crate) fn from_components(
        components: SmallVec<[EntityPkComponent; 1]>,
    ) -> Result<Self, EntityPkError> {
        if components.is_empty() {
            return Err(EntityPkError::EmptyPrimaryKey);
        }
        Ok(Self { components })
    }

    pub(crate) fn uuid_from_canonical(value: &str) -> Result<Self, EntityPkError> {
        let bytes = crate::storage_codec::id_string::uuid_bytes_from_canonical(value).ok_or(
            EntityPkError::InvalidPrimaryKeyValue {
                index: 0,
                expected: "canonical UUID string",
            },
        )?;
        Ok(Self {
            components: smallvec![EntityPkComponent::Uuid(bytes)],
        })
    }

    pub(crate) fn from_external_parts(
        parts: Vec<String>,
        component_types: &[EntityPkComponentType],
    ) -> Result<Self, EntityPkError> {
        if parts.is_empty() {
            return Err(EntityPkError::EmptyPrimaryKey);
        }
        if parts.len() != component_types.len() {
            return Err(EntityPkError::InvalidEncodedEntityPk);
        }
        let mut components = SmallVec::with_capacity(parts.len());
        for (index, (part, component_type)) in parts.into_iter().zip(component_types).enumerate() {
            let component = match component_type {
                EntityPkComponentType::Uuid => {
                    let bytes = crate::storage_codec::id_string::uuid_bytes_from_canonical(&part)
                        .ok_or(EntityPkError::InvalidPrimaryKeyValue {
                        index,
                        expected: "canonical UUID string",
                    })?;
                    EntityPkComponent::Uuid(bytes)
                }
                EntityPkComponentType::Integer => {
                    EntityPkComponent::Integer(part.parse().map_err(|_| {
                        EntityPkError::InvalidPrimaryKeyValue {
                            index,
                            expected: "integer",
                        }
                    })?)
                }
                EntityPkComponentType::String => EntityPkComponent::String(part.into_boxed_str()),
                EntityPkComponentType::Bytes => {
                    let value = base64::engine::general_purpose::STANDARD
                        .decode(part)
                        .map_err(|_| EntityPkError::InvalidPrimaryKeyValue {
                            index,
                            expected: "base64 string",
                        })?;
                    EntityPkComponent::Bytes(value.into_boxed_slice())
                }
            };
            components.push(component);
        }
        Self::from_components(components)
    }

    #[cfg(test)]
    pub(crate) fn tuple(parts: Vec<String>) -> Result<Self, EntityPkError> {
        Self::from_parts(parts)
    }

    #[cfg(test)]
    pub(crate) fn from_parts_unchecked(parts: Vec<String>) -> Self {
        Self {
            components: parts
                .into_iter()
                .map(|part| EntityPkComponent::String(part.into_boxed_str()))
                .collect(),
        }
    }

    pub(crate) fn from_primary_key_paths(
        snapshot: &JsonValue,
        primary_key_paths: &[Vec<String>],
    ) -> Result<Self, EntityPkError> {
        if primary_key_paths.is_empty() {
            return Err(EntityPkError::EmptyPrimaryKey);
        }

        let component_types = vec![EntityPkComponentType::String; primary_key_paths.len()];
        Self::from_primary_key_plan(snapshot, primary_key_paths, &component_types)
    }

    pub(crate) fn from_primary_key_plan(
        snapshot: &JsonValue,
        primary_key_paths: &[Vec<String>],
        component_types: &[EntityPkComponentType],
    ) -> Result<Self, EntityPkError> {
        if primary_key_paths.len() != component_types.len() {
            return Err(EntityPkError::InvalidEncodedEntityPk);
        }
        let mut components = SmallVec::with_capacity(primary_key_paths.len());
        for (index, path) in primary_key_paths.iter().enumerate() {
            if path.is_empty() {
                return Err(EntityPkError::EmptyPrimaryKeyPath { index });
            }
            let Some(value) = json_pointer_get(snapshot, path) else {
                return Err(EntityPkError::MissingPrimaryKeyValue { index });
            };
            components.push(component_from_json_value(
                value,
                component_types[index],
                index,
            )?);
        }

        Self::from_components(components)
    }

    pub(crate) fn from_json_values(
        values: &[JsonValue],
        component_types: &[EntityPkComponentType],
    ) -> Result<Self, EntityPkError> {
        if values.len() != component_types.len() {
            return Err(EntityPkError::InvalidEncodedEntityPk);
        }
        let components = values
            .iter()
            .zip(component_types)
            .enumerate()
            .map(|(index, (value, component_type))| {
                component_from_json_value(value, *component_type, index)
            })
            .collect::<Result<SmallVec<_>, _>>()?;
        Self::from_components(components)
    }

    pub(crate) fn as_json_array_value(&self) -> Result<JsonValue, LixError> {
        if self.components.is_empty() {
            return Err(LixError::unknown(
                "entity primary key must contain at least one primary-key part",
            ));
        }

        Ok(JsonValue::Array(
            self.components
                .iter()
                .map(EntityPkComponent::external_json)
                .collect(),
        ))
    }

    pub(crate) fn as_json_array_text(&self) -> Result<String, LixError> {
        serde_json::to_string(&self.as_json_array_value()?).map_err(|error| {
            LixError::unknown(format!("failed to encode entity pk as JSON: {error}"))
        })
    }

    pub(crate) fn as_single_string(&self) -> Result<&str, LixError> {
        if self.components.is_empty() {
            return Err(LixError::unknown(
                "entity primary key must contain at least one primary-key part",
            ));
        }

        if let [EntityPkComponent::String(value)] = self.components.as_slice() {
            return Ok(value);
        }

        Err(LixError::unknown(
            "entity primary key is not a single string primary-key tuple",
        ))
    }

    pub(crate) fn as_single_string_owned(&self) -> Result<String, LixError> {
        let [component] = self.components.as_slice() else {
            return Err(LixError::unknown(
                "entity primary key is not a single-component tuple",
            ));
        };
        Ok(component.external_string())
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

        let mut components = SmallVec::with_capacity(values.len());
        for (index, value) in values.iter().enumerate() {
            components.push(untyped_component_from_json_value(value, index)?);
        }
        Self::from_components(components)
    }
}

impl EntityPkComponent {
    pub(crate) fn external_string(&self) -> String {
        match self {
            Self::Uuid(bytes) => crate::storage_codec::id_string::uuid_string_from_bytes(*bytes),
            Self::Integer(value) => value.to_string(),
            Self::String(value) => value.to_string(),
            Self::Bytes(value) => base64::engine::general_purpose::STANDARD.encode(value),
        }
    }

    pub(crate) fn external_json(&self) -> JsonValue {
        match self {
            Self::Integer(value) => JsonValue::from(*value),
            _ => JsonValue::String(self.external_string()),
        }
    }
}

impl Serialize for EntityPk {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.as_json_array_value()
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for EntityPk {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = JsonValue::deserialize(deserializer)?;
        Self::from_json_array_value(&value).map_err(serde::de::Error::custom)
    }
}

const ENTITY_PK_VALUE_CODEC_V1: u8 = 1;

#[derive(musli::Encode, musli::Decode)]
#[musli(packed)]
struct EntityPkWire {
    version: u8,
    components: Vec<EntityPkComponentWire>,
}

#[derive(musli::Encode, musli::Decode)]
#[musli(packed)]
struct EntityPkComponentWire {
    tag: u8,
    #[musli(bytes)]
    value: Vec<u8>,
}

#[derive(musli::Encode)]
#[musli(packed)]
struct EntityPkWireRef<'a> {
    version: u8,
    components: &'a [EntityPkComponentWireRef<'a>],
}

#[derive(musli::Encode)]
#[musli(packed)]
struct EntityPkComponentWireRef<'a> {
    tag: u8,
    #[musli(bytes)]
    value: &'a [u8],
}

impl<M> Encode<M> for EntityPk
where
    for<'a> EntityPkWireRef<'a>: Encode<M>,
    EntityPkWire: Encode<M>,
{
    type Encode = Self;

    fn encode<E>(&self, encoder: E) -> Result<(), E::Error>
    where
        E: Encoder<Mode = M>,
    {
        if let [EntityPkComponent::Uuid(bytes)] = self.components.as_slice() {
            let component = [EntityPkComponentWireRef {
                tag: 0,
                value: bytes,
            }];
            return encoder.encode(EntityPkWireRef {
                version: ENTITY_PK_VALUE_CODEC_V1,
                components: &component,
            });
        }

        let components = self
            .components
            .iter()
            .map(|component| match component {
                EntityPkComponent::Uuid(bytes) => EntityPkComponentWire {
                    tag: 0,
                    value: bytes.to_vec(),
                },
                EntityPkComponent::Integer(value) => EntityPkComponentWire {
                    tag: 1,
                    value: value.to_be_bytes().to_vec(),
                },
                EntityPkComponent::String(value) => EntityPkComponentWire {
                    tag: 2,
                    value: value.as_bytes().to_vec(),
                },
                EntityPkComponent::Bytes(value) => EntityPkComponentWire {
                    tag: 3,
                    value: value.to_vec(),
                },
            })
            .collect::<Vec<_>>();
        encoder.encode(EntityPkWire {
            version: ENTITY_PK_VALUE_CODEC_V1,
            components,
        })
    }

    fn size_hint(&self) -> Option<usize> {
        Some(
            2 + self
                .components
                .iter()
                .map(|component| {
                    2 + match component {
                        EntityPkComponent::Uuid(_) => 16,
                        EntityPkComponent::Integer(_) => 8,
                        EntityPkComponent::String(value) => value.len(),
                        EntityPkComponent::Bytes(value) => value.len(),
                    }
                })
                .sum::<usize>(),
        )
    }

    fn as_encode(&self) -> &Self::Encode {
        self
    }
}

impl<'de, M, A> Decode<'de, M, A> for EntityPk
where
    A: Allocator,
    EntityPkWire: Decode<'de, M, A>,
{
    fn decode<D>(decoder: D) -> Result<Self, D::Error>
    where
        D: Decoder<'de, Mode = M, Allocator = A>,
    {
        let cx = decoder.cx();
        let wire = EntityPkWire::decode(decoder)?;
        if wire.version != ENTITY_PK_VALUE_CODEC_V1 {
            return Err(cx.message(format_args!(
                "unsupported entity primary-key value codec version {}",
                wire.version
            )));
        }
        let mut components = SmallVec::with_capacity(wire.components.len());
        for (index, component) in wire.components.into_iter().enumerate() {
            let component = match component.tag {
                0 => EntityPkComponent::Uuid(component.value.try_into().map_err(|_| {
                    cx.message(format_args!(
                        "UUID entity primary-key component {index} must contain 16 bytes"
                    ))
                })?),
                1 => EntityPkComponent::Integer(i64::from_be_bytes(
                    component.value.try_into().map_err(|_| {
                        cx.message(format_args!(
                            "integer entity primary-key component {index} must contain 8 bytes"
                        ))
                    })?,
                )),
                2 => EntityPkComponent::String(
                    String::from_utf8(component.value)
                        .map_err(|error| {
                            cx.message(format_args!(
                                "string entity primary-key component {index} is not UTF-8: {error}"
                            ))
                        })?
                        .into_boxed_str(),
                ),
                3 => EntityPkComponent::Bytes(component.value.into_boxed_slice()),
                tag => {
                    return Err(cx.message(format_args!(
                        "unknown entity primary-key component tag {tag}"
                    )));
                }
            };
            components.push(component);
        }
        Self::from_components(components).map_err(|error| {
            cx.message(format_args!(
                "entity primary key decoded from storage is invalid: {error}"
            ))
        })
    }
}

fn untyped_component_from_json_value(
    value: &JsonValue,
    index: usize,
) -> Result<EntityPkComponent, EntityPkError> {
    match value {
        JsonValue::String(value) => Ok(EntityPkComponent::String(value.clone().into_boxed_str())),
        JsonValue::Number(value) => value
            .as_i64()
            .map(EntityPkComponent::Integer)
            .ok_or(EntityPkError::UnsupportedPrimaryKeyValue { index }),
        _ => Err(EntityPkError::UnsupportedPrimaryKeyValue { index }),
    }
}

fn component_from_json_value(
    value: &JsonValue,
    component_type: EntityPkComponentType,
    index: usize,
) -> Result<EntityPkComponent, EntityPkError> {
    match component_type {
        EntityPkComponentType::Uuid => {
            let value = value
                .as_str()
                .ok_or(EntityPkError::InvalidPrimaryKeyValue {
                    index,
                    expected: "UUID string",
                })?;
            let bytes = crate::storage_codec::id_string::uuid_bytes_from_canonical(value).ok_or(
                EntityPkError::InvalidPrimaryKeyValue {
                    index,
                    expected: "canonical UUID string",
                },
            )?;
            Ok(EntityPkComponent::Uuid(bytes))
        }
        EntityPkComponentType::Integer => value.as_i64().map(EntityPkComponent::Integer).ok_or(
            EntityPkError::InvalidPrimaryKeyValue {
                index,
                expected: "signed 64-bit integer",
            },
        ),
        EntityPkComponentType::String => value
            .as_str()
            .map(|value| EntityPkComponent::String(value.into()))
            .ok_or(EntityPkError::InvalidPrimaryKeyValue {
                index,
                expected: "string",
            }),
        EntityPkComponentType::Bytes => {
            let value = value
                .as_str()
                .ok_or(EntityPkError::InvalidPrimaryKeyValue {
                    index,
                    expected: "base64 string",
                })?;
            base64::engine::general_purpose::STANDARD
                .decode(value)
                .map(|value| EntityPkComponent::Bytes(value.into_boxed_slice()))
                .map_err(|_| EntityPkError::InvalidPrimaryKeyValue {
                    index,
                    expected: "base64 string",
                })
        }
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
            entries.sort_by_key(|(left, _)| *left);

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
            EntityPk::from_parts_unchecked(vec!["namespace".to_string(), "".to_string()])
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
            EntityPk::from_parts_unchecked(vec!["messages".to_string(), "en".to_string()])
        );
    }

    #[test]
    fn entity_pk_json_array_accepts_integer_and_rejects_non_scalar_parts() {
        assert_eq!(
            EntityPk::from_json_array_text("[\"namespace\",42]")
                .expect("integer component should decode")
                .components
                .as_slice(),
            &[
                EntityPkComponent::String("namespace".into()),
                EntityPkComponent::Integer(42),
            ]
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
            Err(EntityPkError::InvalidPrimaryKeyValue {
                index: 1,
                expected: "string",
            })
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
            EntityPk::from_parts_unchecked(vec!["messages".to_string(), "".to_string()])
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
            Err(EntityPkError::InvalidPrimaryKeyValue {
                index: 0,
                expected: "string",
            })
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
        let bytes = crate::storage_codec::encode(
            "entity primary key parts",
            &EntityPkWire {
                version: ENTITY_PK_VALUE_CODEC_V1,
                components: Vec::new(),
            },
        )
        .expect("empty parts should encode");

        let error = crate::storage_codec::decode::<EntityPk>("entity primary key", &bytes)
            .expect_err("empty entity primary key should reject");

        assert!(
            error
                .message
                .contains("entity primary key decoded from storage is invalid")
        );
    }
}
