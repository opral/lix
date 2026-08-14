use serde_json::Value as JsonValue;

use crate::LixError;
use crate::entity_pk::EntityPk;
use crate::hot_state::NativeRowSnapshot;
use crate::transaction_types::NativeRowPayload;

/// Encodes the sole current-state payload for one Schema-v1 row.
///
/// The primary key is authenticated by the state key and owner digest and is
/// therefore intentionally absent from the tuple body. Only non-PK columns
/// are encoded here.
pub(crate) fn encode(
    schema: &lix_schema::Schema,
    entity_pk: &EntityPk,
    branch_id: &str,
    file_id: Option<&str>,
    untracked: bool,
    snapshot: &JsonValue,
) -> Result<NativeRowPayload, LixError> {
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
                let micros = chrono::DateTime::parse_from_rfc3339(value)
                    .map_err(|error| {
                        LixError::new(
                            LixError::CODE_SCHEMA_VALIDATION,
                            format!(
                                "{}.{} contains invalid timestamptz: {error}",
                                schema.key, column.name
                            ),
                        )
                    })?
                    .timestamp_micros();
                lix_schema::value_layout::BodyValue::Timestamptz(micros)
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
    let layout_id = lix_schema::value_layout::layout_id(schema)
        .map_err(|error| LixError::new(LixError::CODE_INTERNAL_ERROR, error.to_string()))?;
    let plan = lix_schema::value_layout::body_plan(schema);
    let mut body = Vec::new();
    lix_schema::value_layout::encode_body(&plan, &values, &mut body)
        .map_err(|error| LixError::new(LixError::CODE_SCHEMA_VALIDATION, error.to_string()))?;
    Ok(NativeRowPayload {
        layout_id,
        owner_digest: crate::entity_pk::native_row_owner_digest(
            branch_id,
            None,
            &schema.key,
            entity_pk,
            file_id,
            untracked,
        ),
        body: bytes::Bytes::from(body),
    })
}

/// Decodes a retained, owner-authenticated Schema-v1 tuple.
///
/// State-key/branch/generation authentication happens before a
/// [`NativeRowSnapshot`] is materialized. This boundary additionally binds the
/// tuple to the complete expected schema layout before exposing any cell.
pub(crate) fn decode(
    schema: &lix_schema::Schema,
    native: &NativeRowSnapshot,
) -> Result<Vec<lix_schema::value_layout::BodyValue>, LixError> {
    let expected_layout = lix_schema::value_layout::layout_id(schema)
        .map_err(|error| LixError::new(LixError::CODE_INTERNAL_ERROR, error.to_string()))?;
    if native.layout_id != expected_layout {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            format!(
                "Schema v1 current-state row '{}' has a mismatched native layout",
                schema.key
            ),
        ));
    }
    lix_schema::value_layout::decode_body(
        &lix_schema::value_layout::body_plan(schema),
        &native.body,
    )
    .map_err(|error| {
        LixError::new(
            LixError::CODE_STORAGE_ERROR,
            format!("Schema v1 current-state row '{}' is malformed: {error}", schema.key),
        )
    })
}

pub(crate) fn seed_schema(schema_key: &str) -> Result<&'static lix_schema::Schema, LixError> {
    static SCHEMAS: std::sync::OnceLock<std::collections::BTreeMap<String, lix_schema::Schema>> =
        std::sync::OnceLock::new();
    let schemas = SCHEMAS.get_or_init(|| {
        crate::schema::seed_schema_definitions()
            .into_iter()
            .map(|definition| {
                let schema = lix_schema::from_value(definition.clone())
                    .expect("compile-time Schema v1 definition must be valid");
                (schema.key.clone(), schema)
            })
            .collect()
    });
    schemas.get(schema_key).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("compile-time Schema v1 definition '{schema_key}' is missing"),
        )
    })
}
