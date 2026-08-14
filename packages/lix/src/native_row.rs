use serde_json::Value as JsonValue;

use crate::LixError;
use crate::entity_pk::EntityPk;
use crate::forktree::NativeRowCell;

pub(crate) fn encode(
    schema: &lix_schema::Schema,
    entity_pk: &EntityPk,
    branch_id: &str,
    file_id: Option<&str>,
    snapshot: &JsonValue,
) -> Result<NativeRowCell, LixError> {
    let object = snapshot.as_object().ok_or_else(|| {
        LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!("Schema v1 row '{}' must be an object", schema.key),
        )
    })?;
    let mut values = Vec::with_capacity(schema.columns.len() - schema.primary_key.len());
    for column in schema
        .columns
        .iter()
        .filter(|column| !schema.primary_key.contains(&column.name))
    {
        let value = object.get(&column.name).unwrap_or(&JsonValue::Null);
        let value = match (column.data_type, value) {
            (_, JsonValue::Null) => lix_schema::value_layout::BodyValue::Null,
            (lix_schema::DataType::Text, JsonValue::String(value)) => {
                lix_schema::value_layout::BodyValue::Text(value.clone())
            }
            (lix_schema::DataType::Uuid, JsonValue::String(value)) => {
                lix_schema::value_layout::BodyValue::Uuid(uuid::Uuid::parse_str(value).map_err(
                    |error| {
                        LixError::new(
                            LixError::CODE_SCHEMA_VALIDATION,
                            format!("{}.{} contains invalid uuid: {error}", schema.key, column.name),
                        )
                    },
                )?)
            }
            (lix_schema::DataType::Int8, JsonValue::Number(value)) => {
                lix_schema::value_layout::BodyValue::Int8(value.as_i64().ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_SCHEMA_VALIDATION,
                        format!("{}.{} is not int8", schema.key, column.name),
                    )
                })?)
            }
            (lix_schema::DataType::Float8, JsonValue::Number(value)) => {
                lix_schema::value_layout::BodyValue::Float8(value.as_f64().ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_SCHEMA_VALIDATION,
                        format!("{}.{} is not float8", schema.key, column.name),
                    )
                })?)
            }
            (lix_schema::DataType::Boolean, JsonValue::Bool(value)) => {
                lix_schema::value_layout::BodyValue::Boolean(*value)
            }
            (lix_schema::DataType::Jsonb, value) => {
                lix_schema::value_layout::BodyValue::Jsonb(value.clone())
            }
            (lix_schema::DataType::Timestamptz, JsonValue::String(value)) => {
                lix_schema::value_layout::BodyValue::Timestamptz(
                    chrono::DateTime::parse_from_rfc3339(value)
                        .map_err(|error| {
                            LixError::new(
                                LixError::CODE_SCHEMA_VALIDATION,
                                format!(
                                    "{}.{} contains invalid timestamptz: {error}",
                                    schema.key, column.name
                                ),
                            )
                        })?
                        .timestamp_micros(),
                )
            }
            _ => {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!(
                        "{}.{} does not match declared type {}",
                        schema.key,
                        column.name,
                        column.data_type.postgres_name()
                    ),
                ));
            }
        };
        values.push(value);
    }
    let layout_id = layout_id(schema);
    let mut body = Vec::new();
    lix_schema::value_layout::encode_body(
        &body_plan(schema),
        &values,
        &mut body,
    )
    .map_err(|error| LixError::new(LixError::CODE_SCHEMA_VALIDATION, error.to_string()))?;
    Ok(NativeRowCell {
        layout_id,
        owner_branch_id: *uuid::Uuid::parse_str(branch_id)
            .map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("native row owner branch is not a UUID: {error}"),
                )
            })?
            .as_bytes(),
        owner_digest: crate::entity_pk::native_row_owner_digest(
            branch_id,
            &schema.key,
            entity_pk,
            file_id,
        ),
        semantic_digest: semantic_digest(snapshot),
        body: bytes::Bytes::from(body),
    })
}

pub(crate) fn semantic_digest(value: &JsonValue) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"lix.forktree.schema-v1.logical-row.v1\0");
    hash.update(
        &serde_json::to_vec(value)
            .expect("serde_json::Value always has a canonical serializable representation"),
    );
    *hash.finalize().as_bytes()
}

pub(crate) fn semantic_digest_text(value: &str) -> Result<[u8; 32], LixError> {
    let value: JsonValue = serde_json::from_str(value).map_err(|error| {
        LixError::new(
            LixError::CODE_STORAGE_ERROR,
            format!("authenticated history row is malformed JSON: {error}"),
        )
    })?;
    Ok(semantic_digest(&value))
}

pub(crate) fn decode(
    schema: &lix_schema::Schema,
    entity_pk: &EntityPk,
    branch_id: &str,
    file_id: Option<&str>,
    native: &NativeRowCell,
) -> Result<Vec<lix_schema::value_layout::BodyValue>, LixError> {
    let expected_layout = layout_id(schema);
    if native.layout_id != expected_layout {
        return Err(storage_error(schema, "has a mismatched native layout"));
    }
    let expected_branch = uuid::Uuid::parse_str(branch_id).map_err(|error| {
        LixError::new(
            LixError::CODE_STORAGE_ERROR,
            format!("authenticated native row branch is not a UUID: {error}"),
        )
    })?;
    if native.owner_branch_id != *expected_branch.as_bytes() {
        return Err(storage_error(
            schema,
            &format!(
                "branch owner does not match its authenticated source (expected={expected_branch}, actual={})",
                uuid::Uuid::from_bytes(native.owner_branch_id),
            ),
        ));
    }
    let expected_owner = crate::entity_pk::native_row_owner_digest(
        branch_id,
        &schema.key,
        entity_pk,
        file_id,
    );
    if native.owner_digest != expected_owner {
        return Err(storage_error(
            schema,
            &format!(
                "owner does not match its authenticated StateKey (branch={branch_id}, file_id={file_id:?}, entity_pk={entity_pk:?}, expected={expected_owner:02x?}, actual={:02x?})",
                native.owner_digest,
            ),
        ));
    }
    lix_schema::value_layout::decode_body(
        &body_plan(schema),
        &native.body,
    )
    .map_err(|error| storage_error(schema, &format!("is malformed: {error}")))
}

fn body_plan(schema: &lix_schema::Schema) -> Vec<lix_schema::value_layout::BodyColumn> {
    use lix_schema::value_layout::{BodyColumn, BodyKind};
    schema
        .columns
        .iter()
        .filter(|column| !schema.primary_key.contains(&column.name))
        .map(|column| BodyColumn {
            kind: match column.data_type {
                lix_schema::DataType::Text => BodyKind::Text,
                lix_schema::DataType::Uuid => BodyKind::Uuid,
                lix_schema::DataType::Int8 => BodyKind::Int8,
                lix_schema::DataType::Float8 => BodyKind::Float8,
                lix_schema::DataType::Boolean => BodyKind::Boolean,
                lix_schema::DataType::Jsonb => BodyKind::Jsonb,
                lix_schema::DataType::Timestamptz => BodyKind::Timestamptz,
            },
            nullable: column.nullable,
        })
        .collect()
}

/// Physical tuple identity. Logical constraints/defaults/examples do not
/// alter bytes already stored; ordered names, scalar kinds, nullability, PK
/// identity, and non-PK body order do.
fn layout_id(schema: &lix_schema::Schema) -> [u8; 32] {
    fn field(hash: &mut blake3::Hasher, value: &str) {
        hash.update(&(value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    let mut hash = blake3::Hasher::new();
    hash.update(b"lix.forktree.schema-v1.native-row-layout.v2\0");
    field(&mut hash, &schema.key);
    hash.update(&(schema.columns.len() as u64).to_be_bytes());
    for column in &schema.columns {
        field(&mut hash, &column.name);
        field(&mut hash, column.data_type.postgres_name());
        hash.update(&[u8::from(column.nullable)]);
    }
    hash.update(&(schema.primary_key.len() as u64).to_be_bytes());
    for column in &schema.primary_key {
        field(&mut hash, column);
    }
    for column in schema
        .columns
        .iter()
        .filter(|column| !schema.primary_key.contains(&column.name))
    {
        field(&mut hash, &column.name);
    }
    *hash.finalize().as_bytes()
}

pub(crate) fn logical_value(
    schema: &lix_schema::Schema,
    entity_pk: &EntityPk,
    branch_id: &str,
    file_id: Option<&str>,
    native: &NativeRowCell,
) -> Result<JsonValue, LixError> {
    use lix_schema::value_layout::BodyValue;
    let body = decode(schema, entity_pk, branch_id, file_id, native)?;
    let JsonValue::Array(pk) = entity_pk.as_json_array_value()? else {
        unreachable!("typed entity primary key always encodes as an array")
    };
    if pk.len() != schema.primary_key.len() {
        return Err(storage_error(schema, "primary key arity is invalid"));
    }
    let value_columns = schema
        .columns
        .iter()
        .filter(|column| !schema.primary_key.contains(&column.name))
        .collect::<Vec<_>>();
    if body.len() != value_columns.len() {
        return Err(storage_error(schema, "body arity is invalid"));
    }
    let mut object = serde_json::Map::with_capacity(schema.columns.len());
    for (name, value) in schema.primary_key.iter().zip(pk) {
        object.insert(name.clone(), value);
    }
    for (column, value) in value_columns.into_iter().zip(body) {
        let value = match value {
            BodyValue::Null => JsonValue::Null,
            BodyValue::Text(value) => JsonValue::String(value),
            BodyValue::Uuid(value) => JsonValue::String(value.to_string()),
            BodyValue::Int8(value) => JsonValue::from(value),
            BodyValue::Float8(value) => serde_json::Number::from_f64(value)
                .map(JsonValue::Number)
                .ok_or_else(|| storage_error(schema, "contains non-finite float8"))?,
            BodyValue::Boolean(value) => JsonValue::Bool(value),
            BodyValue::Jsonb(value) => value,
            BodyValue::Timestamptz(value) => chrono::DateTime::from_timestamp_micros(value)
                .map(|value| {
                    JsonValue::String(
                        value.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                    )
                })
                .ok_or_else(|| storage_error(schema, "contains invalid timestamptz"))?,
        };
        object.insert(column.name.clone(), value);
    }
    Ok(JsonValue::Object(object))
}

pub(crate) fn logical_text(
    schema: &lix_schema::Schema,
    entity_pk: &EntityPk,
    branch_id: &str,
    file_id: Option<&str>,
    native: &NativeRowCell,
) -> Result<crate::common::SharedStr, LixError> {
    serde_json::to_string(&logical_value(
        schema, entity_pk, branch_id, file_id, native,
    )?)
    .map(crate::common::SharedStr::from)
    .map_err(|error| storage_error(schema, &format!("cannot materialize logical row: {error}")))
}

pub(crate) fn seed_schema(schema_key: &str) -> Result<lix_schema::Schema, LixError> {
    if schema_key == crate::checkpoint::CHECKPOINT_MARKER_SCHEMA_KEY {
        return lix_schema::from_value(serde_json::json!({
            "$schema": lix_schema::SCHEMA_V1_URI,
            "key": crate::checkpoint::CHECKPOINT_MARKER_SCHEMA_KEY,
            "columns": [
                {"name": "branch_id", "type": "uuid", "nullable": false}
            ],
            "primary_key": ["branch_id"]
        }))
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("trusted checkpoint-marker tuple plan is invalid: {error}"),
            )
        });
    }
    let definition = crate::schema::seed_schema_definition(schema_key).ok_or_else(|| {
        LixError::new(
            LixError::CODE_STORAGE_ERROR,
            format!("native current-state row has no trusted Schema v1 plan for '{schema_key}'"),
        )
    })?;
    lix_schema::from_value(definition.clone()).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("trusted Schema v1 definition '{schema_key}' is invalid: {error}"),
        )
    })
}

pub(crate) fn logical_text_for_seed(
    key: &crate::forktree::StateKey,
    branch_id: &str,
    native: &NativeRowCell,
) -> Result<crate::common::SharedStr, LixError> {
    logical_text(
        &seed_schema(&key.schema_key)?,
        &key.entity_pk,
        branch_id,
        key.file_id.as_deref(),
        native,
    )
}

fn storage_error(schema: &lix_schema::Schema, detail: &str) -> LixError {
    LixError::new(
        LixError::CODE_STORAGE_ERROR,
        format!("Schema v1 current-state row '{}' {detail}", schema.key),
    )
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn schema(description: Option<&str>, nullable: bool) -> lix_schema::Schema {
        lix_schema::from_value(json!({
            "$schema": lix_schema::SCHEMA_V1_URI,
            "key": "native_probe",
            "description": description,
            "columns": [
                {"name": "id", "type": "text", "nullable": false},
                {"name": "payload", "type": "text", "nullable": nullable}
            ],
            "primary_key": ["id"]
        }))
        .expect("probe schema")
    }

    #[test]
    fn tuple_omits_pk_and_authenticates_layout_owner_and_semantics() {
        let branch = "01920000-0000-7000-8000-000000000001";
        let key = EntityPk::single("pk-must-not-appear-in-body");
        let row = json!({"id":"pk-must-not-appear-in-body","payload":"body-only"});
        let encoded = encode(&schema(Some("first"), false), &key, branch, None, &row)
            .expect("native tuple encodes");
        assert!(!encoded
            .body
            .windows(b"pk-must-not-appear-in-body".len())
            .any(|window| window == b"pk-must-not-appear-in-body"));
        decode(&schema(Some("changed metadata"), false), &key, branch, None, &encoded)
            .expect("layout-neutral description amendment remains readable");
        assert!(decode(
            &schema(Some("first"), true),
            &key,
            branch,
            None,
            &encoded
        )
        .is_err());
        assert!(decode(
            &schema(Some("first"), false),
            &key,
            "01920000-0000-7000-8000-000000000002",
            None,
            &encoded
        )
        .is_err());
        assert_ne!(encoded.semantic_digest, semantic_digest(&json!({
            "id":"pk-must-not-appear-in-body",
            "payload":"substituted"
        })));
    }
}
