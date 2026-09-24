//! Canonical opaque encoding for public relation- and file-qualified row
//! addresses.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bytes::Bytes;
use smallvec::SmallVec;

use crate::catalog::CatalogSnapshot;
use crate::domain::{Domain, DomainRowIdentity};
use crate::row_pk::RowPkComponentType;
use crate::row_pk::{RowPk, RowPkComponent};
use crate::sql2::{PublicCatalog, PublicSurfaceKind};
use crate::{LixError, RowRef};

const PREFIX: &str = "lix_row_ref:v2:";
const FILE_NULL_TAG: u8 = 0;
const FILE_PRESENT_TAG: u8 = 1;
const UUID_TAG: u8 = 1;
const INTEGER_TAG: u8 = 2;
const TEXT_TAG: u8 = 3;
const BYTES_TAG: u8 = 4;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolvedRowRef {
    pub(crate) relation: String,
    pub(crate) file_id: Option<String>,
    pub(crate) row_pk: RowPk,
}

pub(crate) fn primary_key_component_types(
    catalog: &PublicCatalog,
    relation: &str,
) -> Result<Vec<RowPkComponentType>, LixError> {
    let surface = catalog
        .surface(relation)
        .ok_or_else(|| invalid(format!("lix_row_ref relation '{relation}' does not exist")))?;
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

pub(crate) fn encode(
    relation: &str,
    file_id: Option<&str>,
    row_pk: &RowPk,
) -> Result<RowRef, LixError> {
    validate_relation(relation)?;
    validate_file_scope(relation, file_id)?;
    let relation_len =
        u32::try_from(relation.len()).map_err(|_| invalid("row reference relation is too long"))?;
    let file_len = file_id
        .map(str::len)
        .map(u32::try_from)
        .transpose()
        .map_err(|_| invalid("row reference file id is too long"))?;
    let component_count = u16::try_from(row_pk.components.len())
        .map_err(|_| invalid("row reference has too many primary-key components"))?;
    let mut bytes = Vec::with_capacity(relation.len() + file_id.map_or(0, str::len) + 35);
    bytes.extend_from_slice(&relation_len.to_be_bytes());
    bytes.extend_from_slice(relation.as_bytes());
    match file_id {
        None => bytes.push(FILE_NULL_TAG),
        Some(file_id) => {
            bytes.push(FILE_PRESENT_TAG);
            bytes.extend_from_slice(&file_len.expect("file id length was computed").to_be_bytes());
            bytes.extend_from_slice(file_id.as_bytes());
        }
    }
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
    file_id: Option<&str>,
    row_pk: &RowPk,
) -> Result<RowRef, LixError> {
    let (relation, file_id) = match schema_key {
        // The public filesystem views are repository-wide logical relations.
        // Their physical descriptor rows carry the file id as an internal
        // scope, which must not become part of a public row address.
        "lix_file_descriptor" => ("lix_file", None),
        "lix_directory_descriptor" => ("lix_directory", None),
        "lix_file" | "lix_directory" => (schema_key, None),
        relation => (relation, file_id),
    };
    encode(relation, file_id, row_pk)
}

pub(crate) fn schema_identity_detail(
    schema_key: &str,
    file_id: Option<&str>,
    row_pk: &RowPk,
) -> serde_json::Value {
    match encode_schema_identity(schema_key, file_id, row_pk) {
        Ok(row_ref) => serde_json::Value::String(row_ref.as_str().to_owned()),
        Err(_) => serde_json::Value::Null,
    }
}

pub(crate) fn decode(row_ref: &RowRef) -> Result<ResolvedRowRef, LixError> {
    decode_str(row_ref.as_str())
}

/// Lossless JSONB read shape for a canonical row reference.
pub(crate) fn parts_json(resolved: &ResolvedRowRef) -> serde_json::Value {
    let primary_key = resolved
        .row_pk
        .components
        .iter()
        .map(|component| {
            let kind = match component {
                RowPkComponent::Uuid(_) => "uuid",
                RowPkComponent::Integer(_) => "integer",
                RowPkComponent::String(_) => "string",
                RowPkComponent::Bytes(_) => "bytes",
            };
            serde_json::json!({ "type": kind, "value": component.external_string() })
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "relation": resolved.relation,
        "file_id": resolved.file_id,
        "primary_key": primary_key,
    })
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
    validate_relation(&relation)?;
    let file_id = match cursor.read_u8()? {
        FILE_NULL_TAG => None,
        FILE_PRESENT_TAG => {
            let value = std::str::from_utf8(cursor.read_sized()?)
                .map_err(|_| invalid("lix_row_ref file id is not UTF-8"))?;
            validate_file_id(value)?;
            Some(value.to_owned())
        }
        _ => return Err(invalid("lix_row_ref contains an unknown file scope tag")),
    };
    validate_file_scope(&relation, file_id.as_deref())?;
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
            _ => {
                return Err(invalid(
                    "lix_row_ref contains an unknown key component type",
                ));
            }
        });
    }
    if !cursor.is_finished() {
        return Err(invalid("lix_row_ref contains trailing bytes"));
    }
    let row_pk = RowPk::from_components(components)
        .map_err(|error| invalid(format!("lix_row_ref primary key is invalid: {error}")))?;
    // Reject alternate encodings so equality is byte-canonical.
    let decoded = ResolvedRowRef {
        relation,
        file_id,
        row_pk,
    };
    if encode(
        &decoded.relation,
        decoded.file_id.as_deref(),
        &decoded.row_pk,
    )?
    .as_str()
        != encoded
    {
        return Err(invalid("lix_row_ref is not canonically encoded"));
    }
    Ok(decoded)
}

/// Resolves a decoded public row reference to the physical identity used by
/// validation and storage. The branch and durability come from the source
/// row's domain; only the file scope is carried by the reference itself.
///
/// `lix_file` and `lix_directory` are public logical relations. Their
/// descriptor rows are physical engine identities: a file descriptor is
/// scoped to its own id, while a directory descriptor is fileless.
pub(crate) fn resolve_catalog_target(
    catalog: &CatalogSnapshot,
    source_domain: &Domain,
    resolved: &ResolvedRowRef,
) -> Result<DomainRowIdentity, LixError> {
    let (schema_key, file_id) = match resolved.relation.as_str() {
        "lix_file" => {
            if resolved.file_id.is_some() {
                return Err(invalid(
                    "lix_file row references must use a null file scope",
                ));
            }
            let [RowPkComponent::Uuid(bytes)] = resolved.row_pk.components.as_slice() else {
                return Err(invalid(
                    "lix_file row references require one UUID primary-key component",
                ));
            };
            (
                "lix_file_descriptor",
                Some(crate::storage_codec::id_string::uuid_string_from_bytes(
                    *bytes,
                )),
            )
        }
        "lix_directory" => {
            if resolved.file_id.is_some() {
                return Err(invalid(
                    "lix_directory row references must use a null file scope",
                ));
            }
            if !matches!(
                resolved.row_pk.components.as_slice(),
                [RowPkComponent::Uuid(_)]
            ) {
                return Err(invalid(
                    "lix_directory row references require one UUID primary-key component",
                ));
            }
            ("lix_directory_descriptor", None)
        }
        relation => {
            if relation == "lix_commit_edge"
                || !crate::sql2::schema_exposed_as_schema_surface(relation)
            {
                return Err(invalid(format!(
                    "lix_row_ref relation '{relation}' is not a public schema relation"
                )));
            }
            (relation, resolved.file_id.clone())
        }
    };

    let (_, plan) = catalog.plan_for_key(schema_key).ok_or_else(|| {
        invalid(format!(
            "lix_row_ref relation '{}' does not exist in the schema catalog",
            resolved.relation
        ))
    })?;
    let expected = plan
        .primary_key_component_types
        .as_deref()
        .ok_or_else(|| invalid(format!("relation '{schema_key}' has no primary key")))?;
    if resolved.row_pk.components.len() != expected.len()
        || !resolved
            .row_pk
            .components
            .iter()
            .zip(expected)
            .all(|(component, expected)| component_matches_type(component, *expected))
    {
        return Err(invalid(format!(
            "lix_row_ref primary key for relation '{}' has the wrong arity or component types",
            resolved.relation
        )));
    }

    Ok(DomainRowIdentity::new(
        Domain::exact_file(
            source_domain.branch_id().to_owned(),
            source_domain.untracked(),
            file_id,
        ),
        schema_key,
        resolved.row_pk.clone(),
    ))
}

fn component_matches_type(component: &RowPkComponent, expected: RowPkComponentType) -> bool {
    matches!(
        (component, expected),
        (RowPkComponent::Uuid(_), RowPkComponentType::Uuid)
            | (RowPkComponent::Integer(_), RowPkComponentType::Integer)
            | (RowPkComponent::String(_), RowPkComponentType::String)
            | (RowPkComponent::Bytes(_), RowPkComponentType::Bytes)
    )
}

fn validate_relation(relation: &str) -> Result<(), LixError> {
    if relation.is_empty() || relation.contains('\0') {
        return Err(invalid(
            "row reference relation must be non-empty text without Unicode NUL",
        ));
    }
    if matches!(relation, "lix_file_descriptor" | "lix_directory_descriptor") {
        return Err(invalid(
            "row reference relation must use the public filesystem relation name",
        ));
    }
    Ok(())
}

fn validate_file_id(file_id: &str) -> Result<(), LixError> {
    if file_id.is_empty() || file_id.contains('\0') {
        return Err(invalid(
            "row reference file id must be non-empty text without Unicode NUL",
        ));
    }
    Ok(())
}

fn validate_file_scope(relation: &str, file_id: Option<&str>) -> Result<(), LixError> {
    if let Some(file_id) = file_id {
        validate_file_id(file_id)?;
        if matches!(relation, "lix_file" | "lix_directory") {
            return Err(invalid(format!(
                "row reference relation '{relation}' only permits a null file scope",
            )));
        }
    }
    Ok(())
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
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| invalid("lix_row_ref is truncated"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| invalid("lix_row_ref is truncated"))?;
        self.offset = end;
        Ok(value)
    }
    fn read_u8(&mut self) -> Result<u8, LixError> {
        Ok(self.read(1)?[0])
    }
    fn read_u16(&mut self) -> Result<u16, LixError> {
        Ok(u16::from_be_bytes(
            self.read(2)?.try_into().expect("two bytes"),
        ))
    }
    fn read_u32(&mut self) -> Result<u32, LixError> {
        Ok(u32::from_be_bytes(
            self.read(4)?.try_into().expect("four bytes"),
        ))
    }
    fn read_sized(&mut self) -> Result<&'a [u8], LixError> {
        let len = self.read_u32()? as usize;
        self.read(len)
    }
    fn is_finished(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

fn invalid(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_TYPE_MISMATCH, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row_pk::RowPkComponent;

    #[test]
    fn parts_preserve_composite_types_and_large_integer_values() {
        let row_pk = RowPk::from_components(smallvec::smallvec![
            RowPkComponent::Integer(i64::MIN),
            RowPkComponent::String("héllo".into()),
            RowPkComponent::Bytes(Bytes::from_static(b"\0binary\xff")),
        ])
        .unwrap();
        let encoded = encode("mixed_relation", Some("file-a"), &row_pk).unwrap();
        let parts = parts_json(&decode(&encoded).unwrap());
        assert_eq!(parts, serde_json::json!({
            "relation": "mixed_relation",
            "file_id": "file-a",
            "primary_key": [
                {"type": "integer", "value": i64::MIN.to_string()},
                {"type": "string", "value": "héllo"},
                {"type": "bytes", "value": "AGJpbmFyef8="},
            ],
        }));
    }

    #[test]
    fn round_trips_composite_typed_identity_without_json() {
        let row_pk = RowPk::from_components(smallvec::smallvec![
            RowPkComponent::String("parent".into()),
            RowPkComponent::Integer(7),
        ])
        .unwrap();
        let encoded = encode("json_object_member", None, &row_pk).unwrap();
        assert!(!encoded.as_str().contains('['));
        assert!(!encoded.as_str().contains("parent"));
        assert_eq!(
            decode(&encoded).unwrap(),
            ResolvedRowRef {
                relation: "json_object_member".to_owned(),
                file_id: None,
                row_pk,
            }
        );
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
        let encoded = encode("typed_identity", None, &row_pk).unwrap();
        assert_eq!(decode(&encoded).unwrap().row_pk, row_pk);
        assert_eq!(
            serde_json::from_value::<RowRef>(serde_json::json!(encoded.as_str())).unwrap(),
            encoded
        );
    }

    #[test]
    fn rejects_malformed_or_noncanonical_values() {
        assert!(decode_str("[\"row\"]").is_err());
        assert!(decode_str("lix_row_ref:v1:not-base64!").is_err());
        assert!(decode_str("lix_row_ref:v1:AAAADWxpeF9rZXlfdmFsdWUAAQMAAAAFaGVsbG8").is_err());
        assert!(serde_json::from_str::<RowRef>(r#""[\"row\"]""#).is_err());
        assert!(serde_json::from_str::<RowRef>(r#""lix_row_ref:v1:not-base64!""#).is_err());
    }

    #[test]
    fn file_scope_is_part_of_the_canonical_address() {
        let row_pk = RowPk::single("row");
        let unscoped = encode("state", None, &row_pk).unwrap();
        let first_file = encode("state", Some("file-a"), &row_pk).unwrap();
        let second_file = encode("state", Some("file-b"), &row_pk).unwrap();

        assert_ne!(unscoped, first_file);
        assert_ne!(first_file, second_file);
        assert_eq!(decode(&unscoped).unwrap().file_id, None);
        assert_eq!(
            decode(&first_file).unwrap().file_id.as_deref(),
            Some("file-a")
        );
        assert_eq!(
            decode(&second_file).unwrap().file_id.as_deref(),
            Some("file-b")
        );
    }

    #[test]
    fn rejects_trailing_bytes_and_private_or_scoped_filesystem_addresses() {
        let row_pk = RowPk::single("row");
        assert!(encode("lix_file", Some("file-a"), &row_pk).is_err());
        assert!(encode("lix_file_descriptor", None, &row_pk).is_err());

        let encoded = encode("state", None, &row_pk).unwrap();
        let mut bytes = URL_SAFE_NO_PAD
            .decode(encoded.as_str().strip_prefix(PREFIX).unwrap())
            .unwrap();
        bytes.push(0);
        let trailing = format!("{PREFIX}{}", URL_SAFE_NO_PAD.encode(bytes));
        assert!(decode_str(&trailing).is_err());
    }

    #[test]
    fn canonicalizes_physical_filesystem_identity_to_public_unscoped_alias() {
        let row_pk = RowPk::uuid_from_canonical("01950000-0000-7000-8000-000000000001").unwrap();
        let encoded =
            encode_schema_identity("lix_file_descriptor", Some("physical-file"), &row_pk).unwrap();
        assert_eq!(
            decode(&encoded).unwrap(),
            ResolvedRowRef {
                relation: "lix_file".to_owned(),
                file_id: None,
                row_pk,
            }
        );
    }

    #[test]
    fn resolves_public_filesystem_aliases_to_exact_physical_scopes() {
        let source = Domain::exact_file("branch-a", false, Some("source-file".to_owned()));
        let file_id = RowPk::uuid_from_canonical("01950000-0000-7000-8000-000000000002").unwrap();
        let file_target = resolve_catalog_target(
            CatalogSnapshot::builtin(),
            &source,
            &ResolvedRowRef {
                relation: "lix_file".to_owned(),
                file_id: None,
                row_pk: file_id.clone(),
            },
        )
        .unwrap();
        assert_eq!(file_target.schema_key(), "lix_file_descriptor");
        assert_eq!(file_target.domain().branch_id(), "branch-a");
        assert!(!file_target.domain().untracked());
        assert_eq!(
            file_target.domain().file_filters(),
            vec![crate::NullableKeyFilter::Value(
                "01950000-0000-7000-8000-000000000002".to_owned()
            )]
        );
        assert_eq!(file_target.row_pk(), &file_id);

        let directory_id =
            RowPk::uuid_from_canonical("01950000-0000-7000-8000-000000000003").unwrap();
        let directory_target = resolve_catalog_target(
            CatalogSnapshot::builtin(),
            &source,
            &ResolvedRowRef {
                relation: "lix_directory".to_owned(),
                file_id: None,
                row_pk: directory_id.clone(),
            },
        )
        .unwrap();
        assert_eq!(directory_target.schema_key(), "lix_directory_descriptor");
        assert_eq!(
            directory_target.domain().file_filters(),
            vec![crate::NullableKeyFilter::Null]
        );
        assert_eq!(directory_target.row_pk(), &directory_id);
    }

    #[test]
    fn rejects_filesystem_alias_file_scope_and_wrong_primary_key_shape() {
        let source = Domain::exact_file("branch-a", false, None);
        let file_id = RowPk::uuid_from_canonical("01950000-0000-7000-8000-000000000004").unwrap();
        let scoped_file_alias = ResolvedRowRef {
            relation: "lix_file".to_owned(),
            file_id: Some("file-a".to_owned()),
            row_pk: file_id,
        };
        assert!(
            resolve_catalog_target(CatalogSnapshot::builtin(), &source, &scoped_file_alias)
                .is_err()
        );

        let catalog = CatalogSnapshot::from_visible_schemas(&[serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "typed_target",
            "columns": [
                { "name": "id", "type": "uuid", "nullable": false },
                { "name": "value", "type": "text", "nullable": true }
            ],
            "primary_key": ["id"]
        })])
        .unwrap();
        let wrong_type = ResolvedRowRef {
            relation: "typed_target".to_owned(),
            file_id: None,
            row_pk: RowPk::single("not-a-uuid"),
        };
        assert!(resolve_catalog_target(&catalog, &source, &wrong_type).is_err());

        let wrong_arity = ResolvedRowRef {
            relation: "typed_target".to_owned(),
            file_id: None,
            row_pk: RowPk::from_components(smallvec::smallvec![
                RowPkComponent::Uuid([7; 16]),
                RowPkComponent::Uuid([8; 16]),
            ])
            .unwrap(),
        };
        assert!(resolve_catalog_target(&catalog, &source, &wrong_arity).is_err());
    }

    #[test]
    fn rejects_hidden_catalog_schema_targets() {
        let source = Domain::exact_file("branch-a", false, None);
        let hidden = ResolvedRowRef {
            relation: "lix_branch_descriptor".to_owned(),
            file_id: None,
            row_pk: RowPk::uuid_from_canonical("01950000-0000-7000-8000-000000000005").unwrap(),
        };
        assert!(resolve_catalog_target(CatalogSnapshot::builtin(), &source, &hidden).is_err());
    }
}
