//! Canonical opaque encoding for public relation-qualified row addresses.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bytes::Bytes;
use smallvec::SmallVec;

use crate::row_pk::{RowPk, RowPkComponent};
use crate::row_pk::RowPkComponentType;
use crate::sql2::{PublicCatalog, PublicSurfaceKind};
use crate::{LixError, RowRef};

const PREFIX: &str = "lix_row_ref:v1:";
const UUID_TAG: u8 = 1;
const INTEGER_TAG: u8 = 2;
const TEXT_TAG: u8 = 3;
const BYTES_TAG: u8 = 4;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolvedRowRef {
    pub(crate) relation: String,
    pub(crate) row_pk: RowPk,
}

pub(crate) fn primary_key_component_types(
    catalog: &PublicCatalog,
    relation: &str,
) -> Result<Vec<RowPkComponentType>, LixError> {
    let surface = catalog.surface(relation).ok_or_else(|| {
        invalid(format!("lix_row_ref relation '{relation}' does not exist"))
    })?;
    match &surface.kind {
        PublicSurfaceKind::File | PublicSurfaceKind::Directory => {
            Ok(vec![RowPkComponentType::Uuid])
        }
        PublicSurfaceKind::SchemaBase { schema_key } => catalog
            .schema_spec(schema_key)
            .map(|spec| spec.primary_key_component_types.clone())
            .ok_or_else(|| invalid(format!("relation '{relation}' has no primary-key schema"))),
        _ => Err(invalid(format!(
            "lix_row_ref does not support relation '{relation}'"
        ))),
    }
}

pub(crate) fn encode(relation: &str, row_pk: &RowPk) -> Result<RowRef, LixError> {
    if relation.is_empty() || relation.contains('\0') {
        return Err(invalid("row reference relation must be non-empty text without Unicode NUL"));
    }
    let relation_len = u32::try_from(relation.len())
        .map_err(|_| invalid("row reference relation is too long"))?;
    let component_count = u16::try_from(row_pk.components.len())
        .map_err(|_| invalid("row reference has too many primary-key components"))?;
    let mut bytes = Vec::with_capacity(relation.len() + 32);
    bytes.extend_from_slice(&relation_len.to_be_bytes());
    bytes.extend_from_slice(relation.as_bytes());
    bytes.extend_from_slice(&component_count.to_be_bytes());
    for component in &row_pk.components {
        match component {
            RowPkComponent::Uuid(value) => {
                bytes.push(UUID_TAG);
                bytes.extend_from_slice(value);
            }
            RowPkComponent::Integer(value) => {
                bytes.push(INTEGER_TAG);
                bytes.extend_from_slice(&value.to_be_bytes());
            }
            RowPkComponent::String(value) => {
                bytes.push(TEXT_TAG);
                write_sized(&mut bytes, value.as_bytes())?;
            }
            RowPkComponent::Bytes(value) => {
                bytes.push(BYTES_TAG);
                write_sized(&mut bytes, value)?;
            }
        }
    }
    Ok(RowRef(format!("{PREFIX}{}", URL_SAFE_NO_PAD.encode(bytes))))
}

/// Encodes a durable schema identity for public diagnostics.
///
/// Filesystem descriptor schema keys are implementation details of the two
/// logical filesystem relations. Registered relation schema keys already are
/// their public relation names. Other engine-only identities retain their
/// schema key as the relation qualifier so details remain lossless without
/// exposing the old JSON row-key representation.
pub(crate) fn encode_schema_identity(
    schema_key: &str,
    row_pk: &RowPk,
) -> Result<RowRef, LixError> {
    let relation = match schema_key {
        "lix_file_descriptor" => "lix_file",
        "lix_directory_descriptor" => "lix_directory",
        relation => relation,
    };
    encode(relation, row_pk)
}

pub(crate) fn schema_identity_detail(schema_key: &str, row_pk: &RowPk) -> serde_json::Value {
    match encode_schema_identity(schema_key, row_pk) {
        Ok(row_ref) => serde_json::Value::String(row_ref.as_str().to_owned()),
        Err(_) => serde_json::Value::Null,
    }
}

pub(crate) fn decode(row_ref: &RowRef) -> Result<ResolvedRowRef, LixError> {
    decode_str(row_ref.as_str())
}

pub(crate) fn decode_str(encoded: &str) -> Result<ResolvedRowRef, LixError> {
    let payload = encoded
        .strip_prefix(PREFIX)
        .ok_or_else(|| invalid("value is not a canonical lix_row_ref"))?;
    let bytes = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| invalid("lix_row_ref payload is not canonical base64url"))?;
    let mut cursor = Cursor::new(&bytes);
    let relation_len = cursor.read_u32()? as usize;
    let relation = std::str::from_utf8(cursor.read(relation_len)?)
        .map_err(|_| invalid("lix_row_ref relation is not UTF-8"))?
        .to_owned();
    if relation.is_empty() || relation.contains('\0') {
        return Err(invalid("lix_row_ref relation is invalid"));
    }
    let component_count = cursor.read_u16()? as usize;
    if component_count == 0 {
        return Err(invalid("lix_row_ref primary key is empty"));
    }
    let mut components = SmallVec::<[RowPkComponent; 2]>::new();
    for _ in 0..component_count {
        components.push(match cursor.read_u8()? {
            UUID_TAG => {
                let mut value = [0_u8; 16];
                value.copy_from_slice(cursor.read(16)?);
                RowPkComponent::Uuid(value)
            }
            INTEGER_TAG => RowPkComponent::Integer(i64::from_be_bytes(
                cursor
                    .read(8)?
                    .try_into()
                    .expect("eight bytes were requested"),
            )),
            TEXT_TAG => {
                let value = std::str::from_utf8(cursor.read_sized()?)
                    .map_err(|_| invalid("lix_row_ref text key is not UTF-8"))?;
                RowPkComponent::String(value.to_owned().into())
            }
            BYTES_TAG => RowPkComponent::Bytes(Bytes::copy_from_slice(cursor.read_sized()?)),
            _ => return Err(invalid("lix_row_ref contains an unknown key component type")),
        });
    }
    if !cursor.is_finished() {
        return Err(invalid("lix_row_ref contains trailing bytes"));
    }
    let row_pk = RowPk::from_components(components)
        .map_err(|error| invalid(format!("lix_row_ref primary key is invalid: {error}")))?;
    // Reject alternate encodings so equality is byte-canonical.
    let decoded = ResolvedRowRef { relation, row_pk };
    if encode(&decoded.relation, &decoded.row_pk)?.as_str() != encoded {
        return Err(invalid("lix_row_ref is not canonically encoded"));
    }
    Ok(decoded)
}

fn write_sized(out: &mut Vec<u8>, value: &[u8]) -> Result<(), LixError> {
    let len = u32::try_from(value.len()).map_err(|_| invalid("row reference key is too long"))?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value);
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    fn read(&mut self, len: usize) -> Result<&'a [u8], LixError> {
        let end = self.offset.checked_add(len).ok_or_else(|| invalid("lix_row_ref is truncated"))?;
        let value = self.bytes.get(self.offset..end).ok_or_else(|| invalid("lix_row_ref is truncated"))?;
        self.offset = end;
        Ok(value)
    }
    fn read_u8(&mut self) -> Result<u8, LixError> { Ok(self.read(1)?[0]) }
    fn read_u16(&mut self) -> Result<u16, LixError> {
        Ok(u16::from_be_bytes(self.read(2)?.try_into().expect("two bytes")))
    }
    fn read_u32(&mut self) -> Result<u32, LixError> {
        Ok(u32::from_be_bytes(self.read(4)?.try_into().expect("four bytes")))
    }
    fn read_sized(&mut self) -> Result<&'a [u8], LixError> {
        let len = self.read_u32()? as usize;
        self.read(len)
    }
    fn is_finished(&self) -> bool { self.offset == self.bytes.len() }
}

fn invalid(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_TYPE_MISMATCH, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row_pk::RowPkComponent;

    #[test]
    fn round_trips_composite_typed_identity_without_json() {
        let row_pk = RowPk::from_components(smallvec::smallvec![
            RowPkComponent::String("parent".into()),
            RowPkComponent::Integer(7),
        ]).unwrap();
        let encoded = encode("json_object_member", &row_pk).unwrap();
        assert!(!encoded.as_str().contains('['));
        assert!(!encoded.as_str().contains("parent"));
        assert_eq!(decode(&encoded).unwrap(), ResolvedRowRef {
            relation: "json_object_member".to_owned(), row_pk,
        });
    }

    #[test]
    fn round_trips_every_supported_primary_key_component_type() {
        let row_pk = RowPk::from_components(smallvec::smallvec![
            RowPkComponent::Uuid([7; 16]),
            RowPkComponent::Integer(-42),
            RowPkComponent::String("member".into()),
            RowPkComponent::Bytes(Bytes::from_static(b"\0binary\xff")),
        ])
        .unwrap();
        let encoded = encode("typed_identity", &row_pk).unwrap();
        assert_eq!(decode(&encoded).unwrap().row_pk, row_pk);
        assert_eq!(serde_json::from_value::<RowRef>(serde_json::json!(encoded.as_str())).unwrap(), encoded);
    }

    #[test]
    fn rejects_malformed_or_noncanonical_values() {
        assert!(decode_str("[\"row\"]").is_err());
        assert!(decode_str("lix_row_ref:v1:not-base64!").is_err());
        assert!(serde_json::from_str::<RowRef>(r#""[\"row\"]""#).is_err());
        assert!(serde_json::from_str::<RowRef>(r#""lix_row_ref:v1:not-base64!""#).is_err());
    }
}
