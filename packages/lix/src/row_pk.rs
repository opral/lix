use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;

use base64::Engine as _;
use bytes::Bytes;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value as JsonValue;
use smallvec::SmallVec;

use crate::LixError;
use crate::common::{SharedStr, json_pointer_get};
use musli::{Allocator, Context, Decode, Decoder, Encode, Encoder};

type RowPkComponentBuffer = SmallVec<[RowPkComponent; 2]>;

/// Logical row primary key derived from a schema primary key.
///
/// Components stay typed inside the engine and are projected to standard JSON
/// scalar representations only at API and SQL boundaries.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct RowPk {
    pub(crate) components: RowPkComponents,
}

impl RowPk {
    pub(crate) fn update_owner_digest(&self, hash: &mut blake3::Hasher) {
        hash.update(&(self.components.len() as u64).to_be_bytes());
        for component in &self.components {
            match component {
                RowPkComponent::Uuid(value) => {
                    hash.update(&[0]);
                    hash.update(value);
                }
                RowPkComponent::Integer(value) => {
                    hash.update(&[1]);
                    hash.update(&value.to_be_bytes());
                }
                RowPkComponent::String(value) => {
                    hash.update(&[2]);
                    hash.update(&(value.len() as u64).to_be_bytes());
                    hash.update(value.as_bytes());
                }
                RowPkComponent::Bytes(value) => {
                    hash.update(&[3]);
                    hash.update(&(value.len() as u64).to_be_bytes());
                    hash.update(value);
                }
            }
        }
    }
    /// How many refcounted buffer handles one `clone` of this key duplicates.
    ///
    /// A composite key shares one `Arc` over its component slice, so it costs
    /// exactly one handle no matter how many components it has; a single
    /// string or bytes component costs one; a UUID or integer costs none.
    #[cfg(feature = "storage-benches")]
    pub(crate) fn shared_handle_count(&self) -> usize {
        match &self.components {
            RowPkComponents::Empty => 0,
            RowPkComponents::Single(component) => match component {
                RowPkComponent::String(_) | RowPkComponent::Bytes(_) => 1,
                RowPkComponent::Uuid(_) | RowPkComponent::Integer(_) => 0,
            },
            RowPkComponents::Shared(_) => 1,
        }
    }
}

pub(crate) fn state_identity_digest(
    global: bool,
    schema_key: &str,
    row_pk: &RowPk,
    file_id: Option<&str>,
) -> [u8; 32] {
    let mut owner = blake3::Hasher::new();
    owner.update(b"lix.forktree.schema-v1.native-row-owner.v2\0");
    owner.update(&[u8::from(global)]);
    owner.update(&(schema_key.len() as u64).to_be_bytes());
    owner.update(schema_key.as_bytes());
    match file_id {
        Some(file_id) => {
            owner.update(&[1]);
            owner.update(&(file_id.len() as u64).to_be_bytes());
            owner.update(file_id.as_bytes());
        }
        None => {
            owner.update(&[0]);
        }
    }
    row_pk.update_owner_digest(&mut owner);
    *owner.finalize().as_bytes()
}

/// A single primary-key component stays inline; composite tuples share one
/// immutable component slice across every identity clone.
///
/// This mirrors a small-vector's one-component happy path without making a
/// composite clone allocate a fresh vector. Component payloads are themselves
/// shared, so cloning either representation only increments existing owners.
#[derive(Debug, Clone)]
pub(crate) enum RowPkComponents {
    Empty,
    Single(RowPkComponent),
    Shared(Arc<[RowPkComponent]>),
}

impl RowPkComponents {
    fn from_smallvec(mut components: RowPkComponentBuffer) -> Self {
        match components.len() {
            0 => Self::Empty,
            1 => Self::Single(
                components
                    .pop()
                    .expect("one primary-key component was just counted"),
            ),
            2 => {
                let second = components
                    .pop()
                    .expect("two primary-key components were just counted");
                let first = components
                    .pop()
                    .expect("two primary-key components were just counted");
                let owner: Arc<[RowPkComponent; 2]> = Arc::new([first, second]);
                Self::Shared(owner)
            }
            _ => Self::Shared(Arc::from(components.into_vec())),
        }
    }

    pub(crate) fn as_slice(&self) -> &[RowPkComponent] {
        match self {
            Self::Empty => &[],
            Self::Single(component) => std::slice::from_ref(component),
            Self::Shared(components) => components,
        }
    }
}

impl Deref for RowPkComponents {
    type Target = [RowPkComponent];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<'a> IntoIterator for &'a RowPkComponents {
    type Item = &'a RowPkComponent;
    type IntoIter = std::slice::Iter<'a, RowPkComponent>;

    fn into_iter(self) -> Self::IntoIter {
        self.as_slice().iter()
    }
}

impl PartialEq for RowPkComponents {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for RowPkComponents {}

impl PartialOrd for RowPkComponents {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RowPkComponents {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_slice().cmp(other.as_slice())
    }
}

impl Hash for RowPkComponents {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state);
    }
}

/// Cross-type order is part of the physical key contract.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum RowPkComponent {
    Uuid([u8; 16]),
    Integer(i64),
    String(SharedStr),
    Bytes(Bytes),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowPkComponentType {
    Uuid,
    Integer,
    String,
    // Never constructed since the cut: its only constructor was the JSON
    // Schema primary-key type mapper, which minted it for
    // `"contentEncoding": "base64"`. Schema v1 has no binary type and no
    // contentEncoding, so no schema can produce one. Kept rather than
    // deleted because five live match arms in the shipping library still
    // handle it, one of them feeding the row-surface fingerprint
    // (sql2/catalog/row_surface.rs), so removing the variant is a
    // design decision about binary primary keys, not dead-code hygiene.
    #[allow(dead_code, reason = "unconstructible after the Schema v1 cut; see comment")]
    Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RowPkError {
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
    InvalidEncodedRowPk,
}

impl std::fmt::Display for RowPkError {
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
            Self::InvalidEncodedRowPk => {
                write!(
                    formatter,
                    "encoded row primary key must be a non-empty JSON array of scalar values"
                )
            }
        }
    }
}

impl RowPk {
    pub(crate) fn empty() -> Self {
        Self {
            components: RowPkComponents::Empty,
        }
    }

    pub(crate) fn single(value: impl Into<String>) -> Self {
        Self {
            components: RowPkComponents::Single(RowPkComponent::String(value.into().into())),
        }
    }

    pub(crate) fn from_shared_parts(
        parts: impl IntoIterator<Item = SharedStr>,
    ) -> Result<Self, RowPkError> {
        Self::from_components(
            parts
                .into_iter()
                .map(RowPkComponent::String)
                .collect::<RowPkComponentBuffer>(),
        )
    }

    /// Builds an all-string identity after a typed producer has validated the
    /// component count and scalar types against the active schema plan.
    pub(crate) fn from_validated_shared_string_parts(
        parts: impl IntoIterator<Item = SharedStr>,
    ) -> Self {
        let components = parts
            .into_iter()
            .map(RowPkComponent::String)
            .collect::<RowPkComponentBuffer>();
        debug_assert!(!components.is_empty());
        Self {
            components: RowPkComponents::from_smallvec(components),
        }
    }

    /// Builds the dominant one-column string identity without assembling an
    /// intermediate component buffer after typed ingress validation.
    pub(crate) fn from_validated_shared_string(value: SharedStr) -> Self {
        Self {
            components: RowPkComponents::Single(RowPkComponent::String(value)),
        }
    }

    pub(crate) fn into_parts(self) -> Vec<String> {
        self.components
            .iter()
            .map(|component| component.external_string())
            .collect()
    }

    pub(crate) fn estimated_heap_bytes(&self) -> usize {
        let tuple_storage = match &self.components {
            RowPkComponents::Shared(components) => {
                components.len() * size_of::<RowPkComponent>()
            }
            RowPkComponents::Empty | RowPkComponents::Single(_) => 0,
        };
        tuple_storage
            + self
                .components
                .iter()
                .map(|component| match component {
                    RowPkComponent::Uuid(_) | RowPkComponent::Integer(_) => 0,
                    RowPkComponent::String(value) => value.len(),
                    RowPkComponent::Bytes(value) => value.len(),
                })
                .sum::<usize>()
    }

    pub(crate) fn from_components(
        components: RowPkComponentBuffer,
    ) -> Result<Self, RowPkError> {
        if components.is_empty() {
            return Err(RowPkError::EmptyPrimaryKey);
        }
        for (index, component) in components.iter().enumerate() {
            if matches!(component, RowPkComponent::String(value) if value.contains('\0')) {
                return Err(RowPkError::InvalidPrimaryKeyValue {
                    index,
                    expected: "text without Unicode NUL",
                });
            }
        }
        Ok(Self {
            components: RowPkComponents::from_smallvec(components),
        })
    }

    pub(crate) fn uuid_from_canonical(value: &str) -> Result<Self, RowPkError> {
        let bytes = crate::storage_codec::id_string::uuid_bytes_from_canonical(value).ok_or(
            RowPkError::InvalidPrimaryKeyValue {
                index: 0,
                expected: "canonical UUID string",
            },
        )?;
        Ok(Self {
            components: RowPkComponents::Single(RowPkComponent::Uuid(bytes)),
        })
    }

    pub(crate) fn from_external_parts(
        parts: Vec<String>,
        component_types: &[RowPkComponentType],
    ) -> Result<Self, RowPkError> {
        Self::from_shared_external_parts(parts.into_iter().map(SharedStr::from), component_types)
    }

    pub(crate) fn from_shared_external_parts(
        parts: impl IntoIterator<Item = SharedStr>,
        component_types: &[RowPkComponentType],
    ) -> Result<Self, RowPkError> {
        let mut parts = parts.into_iter().peekable();
        if parts.peek().is_none() {
            return Err(RowPkError::EmptyPrimaryKey);
        }
        let mut components = RowPkComponentBuffer::new();
        let mut conversion_error = None;
        let mut missing_part = false;
        for (index, component_type) in component_types.iter().copied().enumerate() {
            let Some(part) = parts.next() else {
                missing_part = true;
                break;
            };
            match component_from_external_shared_part(part, component_type, index) {
                Ok(component) => components.push(component),
                Err(error) if conversion_error.is_none() => conversion_error = Some(error),
                Err(_) => {}
            }
        }
        if missing_part || parts.next().is_some() {
            return Err(RowPkError::InvalidEncodedRowPk);
        }
        if let Some(error) = conversion_error {
            return Err(error);
        }
        Self::from_components(components)
    }

    pub(crate) fn validate_external_parts(
        parts: &[&str],
        component_types: &[RowPkComponentType],
    ) -> Result<(), RowPkError> {
        if parts.is_empty() {
            return Err(RowPkError::EmptyPrimaryKey);
        }
        if parts.len() != component_types.len() {
            return Err(RowPkError::InvalidEncodedRowPk);
        }
        for (index, (part, component_type)) in parts.iter().zip(component_types).enumerate() {
            match component_type {
                RowPkComponentType::Uuid => {
                    if crate::storage_codec::id_string::uuid_bytes_from_canonical(part).is_none() {
                        return Err(RowPkError::InvalidPrimaryKeyValue {
                            index,
                            expected: "canonical UUID string",
                        });
                    }
                }
                RowPkComponentType::Integer => {
                    part.parse::<i64>()
                        .map_err(|_| RowPkError::InvalidPrimaryKeyValue {
                            index,
                            expected: "integer",
                        })?;
                }
                RowPkComponentType::String => {
                    if part.contains('\0') {
                        return Err(RowPkError::InvalidPrimaryKeyValue {
                            index,
                            expected: "text without Unicode NUL",
                        });
                    }
                }
                RowPkComponentType::Bytes => {
                    base64::engine::general_purpose::STANDARD
                        .decode(part.as_bytes())
                        .map_err(|_| RowPkError::InvalidPrimaryKeyValue {
                            index,
                            expected: "base64 string",
                        })?;
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn from_parts(parts: Vec<String>) -> Result<Self, RowPkError> {
        Self::from_shared_parts(parts.into_iter().map(SharedStr::from))
    }

    #[cfg(test)]
    pub(crate) fn tuple(parts: Vec<String>) -> Result<Self, RowPkError> {
        Self::from_parts(parts)
    }

    #[cfg(test)]
    pub(crate) fn from_parts_unchecked(parts: Vec<String>) -> Self {
        Self {
            components: RowPkComponents::from_smallvec(
                parts
                    .into_iter()
                    .map(|part| RowPkComponent::String(part.into()))
                    .collect(),
            ),
        }
    }

    pub(crate) fn from_primary_key_paths(
        snapshot: &JsonValue,
        primary_key_paths: &[Vec<String>],
    ) -> Result<Self, RowPkError> {
        if primary_key_paths.is_empty() {
            return Err(RowPkError::EmptyPrimaryKey);
        }

        let component_types = vec![RowPkComponentType::String; primary_key_paths.len()];
        Self::from_primary_key_plan(snapshot, primary_key_paths, &component_types)
    }

    pub(crate) fn from_primary_key_plan(
        snapshot: &JsonValue,
        primary_key_paths: &[Vec<String>],
        component_types: &[RowPkComponentType],
    ) -> Result<Self, RowPkError> {
        if primary_key_paths.len() != component_types.len() {
            return Err(RowPkError::InvalidEncodedRowPk);
        }
        let mut components = SmallVec::with_capacity(primary_key_paths.len());
        for (index, path) in primary_key_paths.iter().enumerate() {
            if path.is_empty() {
                return Err(RowPkError::EmptyPrimaryKeyPath { index });
            }
            let Some(value) = json_pointer_get(snapshot, path) else {
                return Err(RowPkError::MissingPrimaryKeyValue { index });
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
        component_types: &[RowPkComponentType],
    ) -> Result<Self, RowPkError> {
        if values.len() != component_types.len() {
            return Err(RowPkError::InvalidEncodedRowPk);
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
                "row primary key must contain at least one primary-key part",
            ));
        }

        Ok(JsonValue::Array(
            self.components
                .iter()
                .map(RowPkComponent::external_json)
                .collect(),
        ))
    }

    pub(crate) fn as_json_array_text(&self) -> Result<String, LixError> {
        serde_json::to_string(&self.as_json_array_value()?).map_err(|error| {
            LixError::unknown(format!("failed to encode row pk as JSON: {error}"))
        })
    }

    pub(crate) fn as_single_string(&self) -> Result<&str, LixError> {
        if self.components.is_empty() {
            return Err(LixError::unknown(
                "row primary key must contain at least one primary-key part",
            ));
        }

        if let [RowPkComponent::String(value)] = self.components.as_slice() {
            return Ok(value);
        }

        Err(LixError::unknown(
            "row primary key is not a single string primary-key tuple",
        ))
    }

    pub(crate) fn as_single_string_owned(&self) -> Result<String, LixError> {
        let [component] = self.components.as_slice() else {
            return Err(LixError::unknown(
                "row primary key is not a single-component tuple",
            ));
        };
        Ok(component.external_string())
    }

    pub(crate) fn from_json_array_text(row_pk: &str) -> Result<Self, RowPkError> {
        let value = serde_json::from_str::<JsonValue>(row_pk)
            .map_err(|_| RowPkError::InvalidEncodedRowPk)?;
        Self::from_json_array_value(&value)
    }

    pub(crate) fn from_json_array_value(row_pk: &JsonValue) -> Result<Self, RowPkError> {
        let JsonValue::Array(values) = row_pk else {
            return Err(RowPkError::InvalidEncodedRowPk);
        };
        if values.is_empty() {
            return Err(RowPkError::EmptyPrimaryKey);
        }

        let mut components = SmallVec::with_capacity(values.len());
        for (index, value) in values.iter().enumerate() {
            components.push(untyped_component_from_json_value(value, index)?);
        }
        Self::from_components(components)
    }
}

impl RowPkComponent {
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

impl Serialize for RowPk {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.as_json_array_value()
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RowPk {
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
struct RowPkWire {
    version: u8,
    components: Vec<RowPkComponentWire>,
}

#[derive(musli::Encode, musli::Decode)]
#[musli(packed)]
struct RowPkComponentWire {
    tag: u8,
    #[musli(bytes)]
    value: Vec<u8>,
}

#[derive(musli::Encode)]
#[musli(packed)]
struct RowPkWireRef<'a> {
    version: u8,
    components: &'a [RowPkComponentWireRef<'a>],
}

#[derive(musli::Encode)]
#[musli(packed)]
struct RowPkComponentWireRef<'a> {
    tag: u8,
    #[musli(bytes)]
    value: &'a [u8],
}

impl<M> Encode<M> for RowPk
where
    for<'a> RowPkWireRef<'a>: Encode<M>,
    RowPkWire: Encode<M>,
{
    type Encode = Self;

    fn encode<E>(&self, encoder: E) -> Result<(), E::Error>
    where
        E: Encoder<Mode = M>,
    {
        if let [component] = self.components.as_slice() {
            let integer;
            let (tag, value) = match component {
                RowPkComponent::Uuid(bytes) => (0, bytes.as_slice()),
                RowPkComponent::Integer(value) => {
                    integer = value.to_be_bytes();
                    (1, integer.as_slice())
                }
                RowPkComponent::String(value) => (2, value.as_bytes()),
                RowPkComponent::Bytes(value) => (3, value.as_ref()),
            };
            let component = [RowPkComponentWireRef { tag, value }];
            return encoder.encode(RowPkWireRef {
                version: ENTITY_PK_VALUE_CODEC_V1,
                components: &component,
            });
        }

        let components = self
            .components
            .iter()
            .map(|component| match component {
                RowPkComponent::Uuid(bytes) => RowPkComponentWire {
                    tag: 0,
                    value: bytes.to_vec(),
                },
                RowPkComponent::Integer(value) => RowPkComponentWire {
                    tag: 1,
                    value: value.to_be_bytes().to_vec(),
                },
                RowPkComponent::String(value) => RowPkComponentWire {
                    tag: 2,
                    value: value.as_bytes().to_vec(),
                },
                RowPkComponent::Bytes(value) => RowPkComponentWire {
                    tag: 3,
                    value: value.to_vec(),
                },
            })
            .collect::<Vec<_>>();
        encoder.encode(RowPkWire {
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
                        RowPkComponent::Uuid(_) => 16,
                        RowPkComponent::Integer(_) => 8,
                        RowPkComponent::String(value) => value.len(),
                        RowPkComponent::Bytes(value) => value.len(),
                    }
                })
                .sum::<usize>(),
        )
    }

    fn as_encode(&self) -> &Self::Encode {
        self
    }
}

impl<'de, M, A> Decode<'de, M, A> for RowPk
where
    A: Allocator,
    RowPkWire: Decode<'de, M, A>,
{
    fn decode<D>(decoder: D) -> Result<Self, D::Error>
    where
        D: Decoder<'de, Mode = M, Allocator = A>,
    {
        let cx = decoder.cx();
        let wire = RowPkWire::decode(decoder)?;
        if wire.version != ENTITY_PK_VALUE_CODEC_V1 {
            return Err(cx.message(format_args!(
                "unsupported row primary-key value codec version {}",
                wire.version
            )));
        }
        let mut components = SmallVec::with_capacity(wire.components.len());
        for (index, component) in wire.components.into_iter().enumerate() {
            let component = match component.tag {
                0 => RowPkComponent::Uuid(component.value.try_into().map_err(|_| {
                    cx.message(format_args!(
                        "UUID row primary-key component {index} must contain 16 bytes"
                    ))
                })?),
                1 => RowPkComponent::Integer(i64::from_be_bytes(
                    component.value.try_into().map_err(|_| {
                        cx.message(format_args!(
                            "integer row primary-key component {index} must contain 8 bytes"
                        ))
                    })?,
                )),
                2 => RowPkComponent::String(
                    String::from_utf8(component.value)
                        .map_err(|error| {
                            cx.message(format_args!(
                                "string row primary-key component {index} is not UTF-8: {error}"
                            ))
                        })?
                        .into(),
                ),
                3 => RowPkComponent::Bytes(component.value.into()),
                tag => {
                    return Err(cx.message(format_args!(
                        "unknown row primary-key component tag {tag}"
                    )));
                }
            };
            components.push(component);
        }
        Self::from_components(components).map_err(|error| {
            cx.message(format_args!(
                "row primary key decoded from storage is invalid: {error}"
            ))
        })
    }
}

fn untyped_component_from_json_value(
    value: &JsonValue,
    index: usize,
) -> Result<RowPkComponent, RowPkError> {
    match value {
        JsonValue::String(value) => Ok(RowPkComponent::String(value.as_str().into())),
        JsonValue::Number(value) => value
            .as_i64()
            .map(RowPkComponent::Integer)
            .ok_or(RowPkError::UnsupportedPrimaryKeyValue { index }),
        _ => Err(RowPkError::UnsupportedPrimaryKeyValue { index }),
    }
}

fn component_from_external_shared_part(
    part: SharedStr,
    component_type: RowPkComponentType,
    index: usize,
) -> Result<RowPkComponent, RowPkError> {
    match component_type {
        RowPkComponentType::Uuid => {
            let bytes = crate::storage_codec::id_string::uuid_bytes_from_canonical(&part).ok_or(
                RowPkError::InvalidPrimaryKeyValue {
                    index,
                    expected: "canonical UUID string",
                },
            )?;
            Ok(RowPkComponent::Uuid(bytes))
        }
        RowPkComponentType::Integer => {
            Ok(RowPkComponent::Integer(part.as_str().parse().map_err(
                |_| RowPkError::InvalidPrimaryKeyValue {
                    index,
                    expected: "integer",
                },
            )?))
        }
        RowPkComponentType::String => Ok(RowPkComponent::String(part)),
        RowPkComponentType::Bytes => base64::engine::general_purpose::STANDARD
            .decode(part.as_bytes())
            .map(|value| RowPkComponent::Bytes(value.into()))
            .map_err(|_| RowPkError::InvalidPrimaryKeyValue {
                index,
                expected: "base64 string",
            }),
    }
}

fn component_from_json_value(
    value: &JsonValue,
    component_type: RowPkComponentType,
    index: usize,
) -> Result<RowPkComponent, RowPkError> {
    match component_type {
        RowPkComponentType::Uuid => {
            let value = value
                .as_str()
                .ok_or(RowPkError::InvalidPrimaryKeyValue {
                    index,
                    expected: "UUID string",
                })?;
            let bytes = crate::storage_codec::id_string::uuid_bytes_from_canonical(value).ok_or(
                RowPkError::InvalidPrimaryKeyValue {
                    index,
                    expected: "canonical UUID string",
                },
            )?;
            Ok(RowPkComponent::Uuid(bytes))
        }
        RowPkComponentType::Integer => value.as_i64().map(RowPkComponent::Integer).ok_or(
            RowPkError::InvalidPrimaryKeyValue {
                index,
                expected: "signed 64-bit integer",
            },
        ),
        RowPkComponentType::String => value
            .as_str()
            .map(|value| RowPkComponent::String(value.into()))
            .ok_or(RowPkError::InvalidPrimaryKeyValue {
                index,
                expected: "string",
            }),
        RowPkComponentType::Bytes => {
            let value = value
                .as_str()
                .ok_or(RowPkError::InvalidPrimaryKeyValue {
                    index,
                    expected: "base64 string",
                })?;
            base64::engine::general_purpose::STANDARD
                .decode(value)
                .map(|value| RowPkComponent::Bytes(value.into()))
                .map_err(|_| RowPkError::InvalidPrimaryKeyValue {
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
        let identity = RowPk::single("plain-id");

        assert_eq!(
            identity.as_single_string().expect("projection should work"),
            "plain-id"
        );
    }

    #[test]
    fn single_identity_projects_to_json_array_row_pk() {
        let identity = RowPk::single("plain-id");

        assert_eq!(
            identity
                .as_json_array_text()
                .expect("projection should work"),
            "[\"plain-id\"]"
        );
    }

    #[test]
    fn composite_identity_projects_to_json_array_row_pk() {
        let identity = RowPk::tuple(vec!["namespace".to_string(), "42".to_string()])
            .expect("tuple identity");

        assert_eq!(
            identity
                .as_json_array_text()
                .expect("projection should work"),
            "[\"namespace\",\"42\"]"
        );
    }

    #[test]
    fn ten_thousand_composite_clones_share_component_and_payload_owners() {
        let identity = RowPk::tuple(vec![
            "namespace-with-a-non-inline-payload".to_string(),
            "row-with-a-non-inline-payload".to_string(),
        ])
        .expect("composite identity");
        let RowPkComponents::Shared(owner) = &identity.components else {
            panic!("a composite identity must use shared tuple storage")
        };

        let clones = std::iter::repeat_n(identity.clone(), 10_000).collect::<Vec<_>>();
        for cloned in &clones {
            let RowPkComponents::Shared(cloned_owner) = &cloned.components else {
                panic!("a composite clone must retain shared tuple storage")
            };
            assert!(
                Arc::ptr_eq(owner, cloned_owner),
                "composite clones must retain the original component owner"
            );
            for (original, cloned) in owner.iter().zip(cloned_owner.iter()) {
                let (RowPkComponent::String(original), RowPkComponent::String(cloned)) =
                    (original, cloned)
                else {
                    panic!("test identity contains only string components")
                };
                assert!(
                    original.shares_buffer_with(cloned),
                    "component payload clones must retain the original byte owner"
                );
            }
        }
    }

    #[test]
    fn ten_thousand_single_clones_share_the_inline_component_payload() {
        let identity = RowPk::single("row-with-a-non-inline-payload");
        let [RowPkComponent::String(owner)] = identity.components.as_slice() else {
            panic!("single string identity must stay inline")
        };

        let clones = std::iter::repeat_n(identity.clone(), 10_000).collect::<Vec<_>>();
        for cloned in &clones {
            let [RowPkComponent::String(cloned_owner)] = cloned.components.as_slice() else {
                panic!("single string clone must stay inline")
            };
            assert!(
                owner.shares_buffer_with(cloned_owner),
                "single-component clones must retain the original byte owner"
            );
        }
    }

    #[test]
    fn shared_external_common_path_streams_without_heap_spilling_scratch() {
        struct NoSizeHint<I>(I);

        impl<I: Iterator> Iterator for NoSizeHint<I> {
            type Item = I::Item;

            fn next(&mut self) -> Option<Self::Item> {
                self.0.next()
            }

            fn size_hint(&self) -> (usize, Option<usize>) {
                panic!("the direct lockstep path must not collect external parts first")
            }
        }

        let scratch = RowPkComponentBuffer::new();
        assert_eq!(scratch.capacity(), 2);
        assert!(!scratch.spilled());

        let namespace = SharedStr::from_static("namespace");
        let row = SharedStr::from_static("row");
        let identity = RowPk::from_shared_external_parts(
            NoSizeHint([namespace.clone(), row.clone()].into_iter()),
            &[RowPkComponentType::String, RowPkComponentType::String],
        )
        .expect("two-component external identity");
        let RowPkComponents::Shared(owner) = &identity.components else {
            panic!("two components retain one final shared owner")
        };
        let [
            RowPkComponent::String(actual_namespace),
            RowPkComponent::String(actual_entity),
        ] = owner.as_ref()
        else {
            panic!("test identity contains two string components")
        };

        assert_eq!(Arc::strong_count(owner), 1);
        assert!(actual_namespace.shares_buffer_with(&namespace));
        assert!(actual_entity.shares_buffer_with(&row));

        let identitys = (0..10_000)
            .map(|_| {
                RowPk::from_shared_external_parts(
                    [namespace.clone(), row.clone()],
                    &[RowPkComponentType::String, RowPkComponentType::String],
                )
                .expect("two-component external identity")
            })
            .collect::<Vec<_>>();
        for identity in &identitys {
            let RowPkComponents::Shared(owner) = &identity.components else {
                panic!("two components retain one final shared owner")
            };
            let [
                RowPkComponent::String(actual_namespace),
                RowPkComponent::String(actual_entity),
            ] = owner.as_ref()
            else {
                panic!("test identity contains two string components")
            };
            assert_eq!(Arc::strong_count(owner), 1);
            assert!(actual_namespace.shares_buffer_with(&namespace));
            assert!(actual_entity.shares_buffer_with(&row));
        }

        let single = RowPk::from_shared_external_parts(
            NoSizeHint([row.clone()].into_iter()),
            &[RowPkComponentType::String],
        )
        .expect("single-component external identity");
        let [RowPkComponent::String(actual)] = single.components.as_slice() else {
            panic!("one component must stay inline")
        };
        assert!(actual.shares_buffer_with(&row));
    }

    #[test]
    fn shared_external_cardinality_errors_precede_component_errors() {
        assert_eq!(
            RowPk::from_shared_external_parts(
                [
                    SharedStr::from_static("not-an-integer"),
                    SharedStr::from_static("extra"),
                ],
                &[RowPkComponentType::Integer],
            ),
            Err(RowPkError::InvalidEncodedRowPk)
        );
        assert_eq!(
            RowPk::from_shared_external_parts(
                [SharedStr::from_static("not-an-integer")],
                &[
                    RowPkComponentType::Integer,
                    RowPkComponentType::String,
                ],
            ),
            Err(RowPkError::InvalidEncodedRowPk)
        );
    }

    #[test]
    fn row_pk_json_array_roundtrips() {
        let identity = RowPk::tuple(vec!["namespace".to_string(), "42".to_string()])
            .expect("tuple identity");
        let encoded = identity
            .as_json_array_text()
            .expect("projection should work");

        assert_eq!(
            RowPk::from_json_array_text(&encoded).expect("decode should work"),
            identity
        );
    }

    #[test]
    fn row_pk_json_array_allows_empty_string_part() {
        assert_eq!(
            RowPk::from_json_array_text("[\"\"]").expect("empty string is a valid part"),
            RowPk::single("")
        );
    }

    #[test]
    fn tuple_allows_empty_string_part() {
        assert_eq!(
            RowPk::tuple(vec!["namespace".to_string(), "".to_string()])
                .expect("empty string is a valid part"),
            RowPk::from_parts_unchecked(vec!["namespace".to_string(), "".to_string()])
        );
    }

    #[test]
    fn row_pk_json_array_does_not_collide_on_delimiter_like_values() {
        let left =
            RowPk::tuple(vec!["a~b".to_string(), "c".to_string()]).expect("left tuple identity");
        let right = RowPk::tuple(vec!["a".to_string(), "b~c".to_string()])
            .expect("right tuple identity");

        assert_ne!(
            left.as_json_array_text().expect("left should encode"),
            right.as_json_array_text().expect("right should encode")
        );
    }

    #[test]
    fn composite_identity_rejects_single_string_projection() {
        let identity = RowPk::tuple(vec!["namespace".to_string(), "42".to_string()])
            .expect("tuple identity");

        assert!(identity.as_single_string().is_err());
    }

    #[test]
    fn composite_identity_does_not_collide_on_delimiter_like_values() {
        let left =
            RowPk::tuple(vec!["a~b".to_string(), "1".to_string()]).expect("left tuple identity");
        let right = RowPk::tuple(vec!["a".to_string(), "b~1".to_string()])
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

        let identity = RowPk::from_primary_key_paths(
            &snapshot,
            &[vec!["namespace".to_string()], vec!["locale".to_string()]],
        )
        .expect("primary key should derive");

        assert_eq!(
            identity,
            RowPk::from_parts_unchecked(vec!["messages".to_string(), "en".to_string()])
        );
    }

    #[test]
    fn row_pk_json_array_accepts_integer_and_rejects_non_scalar_parts() {
        assert_eq!(
            RowPk::from_json_array_text("[\"namespace\",42]")
                .expect("integer component should decode")
                .components
                .as_slice(),
            &[
                RowPkComponent::String("namespace".into()),
                RowPkComponent::Integer(42),
            ]
        );
        assert_eq!(
            RowPk::from_json_array_text("[\"namespace\",null]"),
            Err(RowPkError::UnsupportedPrimaryKeyValue { index: 1 })
        );
        assert_eq!(
            RowPk::from_json_array_text("[[\"nested\"]]"),
            Err(RowPkError::UnsupportedPrimaryKeyValue { index: 0 })
        );
    }

    #[test]
    fn from_primary_key_paths_rejects_non_string_parts() {
        let snapshot = json!({
            "namespace": "messages",
            "index": 7
        });

        assert_eq!(
            RowPk::from_primary_key_paths(
                &snapshot,
                &[vec!["namespace".to_string()], vec!["index".to_string()],],
            ),
            Err(RowPkError::InvalidPrimaryKeyValue {
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
            RowPk::from_primary_key_paths(
                &snapshot,
                &[vec!["namespace".to_string()], vec!["id".to_string()],],
            )
            .expect("empty string is a valid primary-key value"),
            RowPk::from_parts_unchecked(vec!["messages".to_string(), "".to_string()])
        );
    }

    #[test]
    fn from_primary_key_paths_rejects_nested_json_parts() {
        let snapshot = json!({
            "row_pk": ["welcome.title", "en"],
            "schema_key": "message"
        });

        assert_eq!(
            RowPk::from_primary_key_paths(
                &snapshot,
                &[
                    vec!["row_pk".to_string()],
                    vec!["schema_key".to_string()],
                ],
            ),
            Err(RowPkError::InvalidPrimaryKeyValue {
                index: 0,
                expected: "string",
            })
        );
    }

    #[test]
    fn from_primary_key_paths_rejects_missing_parts() {
        let snapshot = json!({ "id": "a" });

        assert_eq!(
            RowPk::from_primary_key_paths(&snapshot, &[vec!["missing".to_string()]]),
            Err(RowPkError::MissingPrimaryKeyValue { index: 0 })
        );
    }

    #[test]
    fn storage_codec_roundtrips_row_pk() {
        let identity =
            RowPk::tuple(vec!["namespace".to_string(), "id".to_string()]).expect("row pk");
        let bytes = crate::storage_codec::encode("row primary key", &identity)
            .expect("row pk should encode");

        let decoded: RowPk = crate::storage_codec::decode("row primary key", &bytes)
            .expect("row pk should decode");

        assert_eq!(decoded, identity);
    }

    #[test]
    fn storage_codec_preserves_typed_components_and_semantic_traits() {
        let identity = RowPk::from_components(SmallVec::from_vec(vec![
            RowPkComponent::Uuid([0x2a; 16]),
            RowPkComponent::Integer(-42),
            RowPkComponent::Bytes(Bytes::from_static(&[0, 1, 0xff])),
        ]))
        .expect("typed row pk");
        let cloned = identity.clone();

        assert_eq!(identity.cmp(&cloned), Ordering::Equal);
        let mut original_hash = std::collections::hash_map::DefaultHasher::new();
        identity.hash(&mut original_hash);
        let mut cloned_hash = std::collections::hash_map::DefaultHasher::new();
        cloned.hash(&mut cloned_hash);
        assert_eq!(original_hash.finish(), cloned_hash.finish());
        assert_eq!(
            serde_json::to_value(&identity).expect("typed primary key should serialize"),
            json!(["2a2a2a2a-2a2a-2a2a-2a2a-2a2a2a2a2a2a", -42, "AAH/"])
        );

        let encoded = crate::storage_codec::encode("typed row primary key", &identity)
            .expect("typed row pk should encode");
        let decoded: RowPk = crate::storage_codec::decode("typed row primary key", &encoded)
            .expect("typed row pk should decode");
        assert_eq!(decoded, identity);
    }

    #[test]
    fn storage_codec_rejects_empty_row_pk() {
        let bytes = crate::storage_codec::encode(
            "row primary key parts",
            &RowPkWire {
                version: ENTITY_PK_VALUE_CODEC_V1,
                components: Vec::new(),
            },
        )
        .expect("empty parts should encode");

        let error = crate::storage_codec::decode::<RowPk>("row primary key", &bytes)
            .expect_err("empty row primary key should reject");

        assert!(
            error
                .message
                .contains("row primary key decoded from storage is invalid")
        );
    }
}
