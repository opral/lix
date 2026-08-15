//! Self-contained native bodies for immutable LXCD17 authored members.
//!
//! History cannot depend on a mutable/current-state serving root: GC is free to
//! retire those roots while the immutable commit remains reachable.  The
//! member therefore carries the Schema-v1 value-layout plan needed to decode
//! its non-PK body.  PK values remain in the authenticated [`RowPk`] embedded
//! in the member key.  Declared `jsonb` is represented by `BodyKind::Jsonb`;
//! ordinary scalar columns never pass through a JSON row envelope.

use serde_json::Value as JsonValue;

use crate::row_pk::RowPk;
use crate::{LixError, storage_codec};

const FORMAT_VERSION: u8 = 2;
const SEMANTIC_DOMAIN: &str = "lix tracked history native row semantic v2";
const LAYOUT_DOMAIN: &str = "lix tracked history native row layout v2";
const TEXT_ESCAPE: char = '\u{1}';
const TEXT_ESCAPED_NUL: char = '\u{2}';

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
struct StoredColumn {
    name: String,
    kind: u8,
    nullable: bool,
    projected: bool,
    #[musli(with = storage_codec::option)]
    pk_ordinal: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
struct StoredBody {
    version: u8,
    layout_digest: [u8; 32],
    semantic_digest: [u8; 32],
    columns: Vec<StoredColumn>,
    #[musli(bytes)]
    body: Vec<u8>,
}

pub(super) fn encode(
    schema_json: &JsonValue,
    row_pk: &RowPk,
    snapshot: &str,
) -> Result<Vec<u8>, LixError> {
    let schema = lix_schema::from_value(schema_json.clone()).map_err(|error| {
        body_error(format!("Schema-v1 definition is invalid: {error}"))
    })?;
    let snapshot: JsonValue = serde_json::from_str(snapshot)
        .map_err(|error| body_error(format!("authored row is malformed JSON: {error}")))?;
    encode_value(&schema, row_pk, &snapshot)
}

fn encode_value(
    schema: &lix_schema::Schema,
    row_pk: &RowPk,
    snapshot: &JsonValue,
) -> Result<Vec<u8>, LixError> {
    let object = snapshot.as_object().ok_or_else(|| {
        body_error(format!("Schema-v1 row '{}' is not an object", schema.key))
    })?;
    let pk = row_pk.as_json_array_value()?;
    let JsonValue::Array(pk) = pk else {
        unreachable!("RowPk JSON representation is always an array")
    };
    if pk.len() != schema.primary_key.len() {
        return Err(body_error("member RowPk arity disagrees with its schema"));
    }

    let mut columns = Vec::with_capacity(schema.columns.len());
    let mut plan = Vec::with_capacity(schema.columns.len() - schema.primary_key.len());
    let mut values = Vec::with_capacity(plan.capacity());
    for column in &schema.columns {
        let pk_ordinal = schema
            .primary_key
            .iter()
            .position(|name| name == &column.name)
            .map(|ordinal| u16::try_from(ordinal).expect("Schema-v1 PK arity is bounded"));
        let kind = body_kind(column.data_type);
        columns.push(StoredColumn {
            name: column.name.clone(),
            kind: kind_tag(kind),
            nullable: column.nullable,
            projected: object.contains_key(&column.name),
            pk_ordinal,
        });
        if let Some(ordinal) = pk_ordinal {
            if object
                .get(&column.name)
                .is_some_and(|actual| actual != &pk[usize::from(ordinal)])
            {
                return Err(body_error(format!(
                    "member RowPk component for '{}' disagrees with its authored body",
                    column.name
                )));
            }
            continue;
        }
        plan.push(lix_schema::value_layout::BodyColumn {
            kind,
            nullable: column.nullable,
        });
        values.push(body_value(
            &schema.key,
            &column.name,
            column.data_type,
            object.get(&column.name).unwrap_or(&JsonValue::Null),
        )?);
    }
    validate_columns(&columns)?;
    let mut body = Vec::new();
    lix_schema::value_layout::encode_body(&plan, &values, &mut body)
        .map_err(|error| body_error(error.to_string()))?;
    let canonical = logical_value(&columns, row_pk, &body)?;
    let stored = StoredBody {
        version: FORMAT_VERSION,
        layout_digest: layout_digest(&columns),
        semantic_digest: semantic_digest(&canonical),
        columns,
        body,
    };
    storage_codec::encode("LXCD17 native history body", &stored)
}

pub(super) fn decode(row_pk: &RowPk, encoded: &[u8]) -> Result<String, LixError> {
    let stored: StoredBody = storage_codec::decode("LXCD17 native history body", encoded)?;
    if stored.version != FORMAT_VERSION {
        return Err(body_error("unsupported native history body version"));
    }
    validate_columns(&stored.columns)?;
    if stored.layout_digest != layout_digest(&stored.columns) {
        return Err(body_error("native history layout digest is invalid"));
    }
    let value = logical_value(&stored.columns, row_pk, &stored.body)?;
    if stored.semantic_digest != semantic_digest(&value) {
        return Err(body_error("native history semantic digest is invalid"));
    }
    // Re-encoding proves the value-layout body itself is canonical, including
    // JSONB number normalization and the unique narrow/wide offset choice.
    let (plan, values) = decoded_plan_and_values(&stored.columns, &stored.body)?;
    let mut canonical_body = Vec::new();
    lix_schema::value_layout::encode_body(&plan, &values, &mut canonical_body)
        .map_err(|error| body_error(error.to_string()))?;
    if canonical_body != stored.body {
        return Err(body_error("native history body is not canonical"));
    }
    serde_json::to_string(&value)
        .map_err(|error| body_error(format!("cannot project native history row: {error}")))
}

pub(super) fn encode_metadata(metadata: &str) -> Result<Vec<u8>, LixError> {
    let value: JsonValue = serde_json::from_str(metadata)
        .map_err(|error| body_error(format!("history metadata is malformed JSON: {error}")))?;
    let plan = [lix_schema::value_layout::BodyColumn {
        kind: lix_schema::value_layout::BodyKind::Jsonb,
        nullable: false,
    }];
    let mut body = Vec::new();
    lix_schema::value_layout::encode_body(
        &plan,
        &[lix_schema::value_layout::BodyValue::Jsonb(value)],
        &mut body,
    )
    .map_err(|error| body_error(error.to_string()))?;
    Ok(body)
}

pub(super) fn decode_metadata(encoded: &[u8]) -> Result<String, LixError> {
    let plan = [lix_schema::value_layout::BodyColumn {
        kind: lix_schema::value_layout::BodyKind::Jsonb,
        nullable: false,
    }];
    let mut values = lix_schema::value_layout::decode_body(&plan, encoded)
        .map_err(|error| body_error(error.to_string()))?;
    let lix_schema::value_layout::BodyValue::Jsonb(value) = values.remove(0) else {
        return Err(body_error("native history metadata has the wrong type"));
    };
    let mut canonical = Vec::new();
    lix_schema::value_layout::encode_body(
        &plan,
        &[lix_schema::value_layout::BodyValue::Jsonb(value.clone())],
        &mut canonical,
    )
    .map_err(|error| body_error(error.to_string()))?;
    if canonical != encoded {
        return Err(body_error("native history metadata is not canonical"));
    }
    serde_json::to_string(&value)
        .map_err(|error| body_error(format!("cannot project history metadata: {error}")))
}

fn decoded_plan_and_values(
    columns: &[StoredColumn],
    body: &[u8],
) -> Result<(
    Vec<lix_schema::value_layout::BodyColumn>,
    Vec<lix_schema::value_layout::BodyValue>,
), LixError> {
    let plan = columns
        .iter()
        .filter(|column| column.pk_ordinal.is_none())
        .map(|column| {
            Ok(lix_schema::value_layout::BodyColumn {
                kind: tag_kind(column.kind)?,
                nullable: column.nullable,
            })
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let values = lix_schema::value_layout::decode_body(&plan, body)
        .map_err(|error| body_error(error.to_string()))?;
    Ok((plan, values))
}

fn logical_value(
    columns: &[StoredColumn],
    row_pk: &RowPk,
    body: &[u8],
) -> Result<JsonValue, LixError> {
    let JsonValue::Array(pk) = row_pk.as_json_array_value()? else {
        unreachable!("RowPk JSON representation is always an array")
    };
    let (_, values) = decoded_plan_and_values(columns, body)?;
    let mut values = values.into_iter();
    let mut object = serde_json::Map::with_capacity(columns.len());
    for column in columns {
        if column.pk_ordinal.is_some() && !column.projected {
            continue;
        }
        let value = if let Some(ordinal) = column.pk_ordinal {
            pk.get(usize::from(ordinal))
                .cloned()
                .ok_or_else(|| body_error("native history PK ordinal is out of bounds"))?
        } else {
            logical_body_value(values.next().ok_or_else(|| {
                body_error("native history body omitted a declared value")
            })?)?
        };
        if column.projected {
            object.insert(column.name.clone(), value);
        }
    }
    if values.next().is_some() {
        return Err(body_error("native history body has excess values"));
    }
    Ok(JsonValue::Object(object))
}

fn validate_columns(columns: &[StoredColumn]) -> Result<(), LixError> {
    if columns.is_empty() || columns.len() > usize::from(u16::MAX) {
        return Err(body_error("native history column inventory is empty or oversized"));
    }
    let mut names = std::collections::BTreeSet::new();
    let mut ordinals = std::collections::BTreeSet::new();
    for column in columns {
        tag_kind(column.kind)?;
        if column.name.is_empty() || !names.insert(column.name.as_str()) {
            return Err(body_error("native history columns are empty or duplicated"));
        }
        if let Some(ordinal) = column.pk_ordinal
            && !ordinals.insert(ordinal)
        {
            return Err(body_error("native history PK ordinal is duplicated"));
        }
    }
    if ordinals.iter().copied().ne(0..u16::try_from(ordinals.len()).unwrap_or(u16::MAX)) {
        return Err(body_error("native history PK ordinals are not contiguous"));
    }
    Ok(())
}

fn body_value(
    schema_key: &str,
    name: &str,
    kind: lix_schema::DataType,
    value: &JsonValue,
) -> Result<lix_schema::value_layout::BodyValue, LixError> {
    use lix_schema::value_layout::BodyValue;
    let invalid = || body_error(format!("{schema_key}.{name} disagrees with its declared type"));
    Ok(match (kind, value) {
        (_, JsonValue::Null) => BodyValue::Null,
        (lix_schema::DataType::Text, JsonValue::String(value)) => {
            BodyValue::Text(escape_history_text(value))
        }
        (lix_schema::DataType::Uuid, JsonValue::String(value)) => {
            BodyValue::Uuid(uuid::Uuid::parse_str(value).map_err(|_| invalid())?)
        }
        (lix_schema::DataType::Int8, JsonValue::Number(value)) => {
            BodyValue::Int8(value.as_i64().ok_or_else(invalid)?)
        }
        (lix_schema::DataType::Float8, JsonValue::Number(value)) => {
            BodyValue::Float8(value.as_f64().ok_or_else(invalid)?)
        }
        (lix_schema::DataType::Boolean, JsonValue::Bool(value)) => BodyValue::Boolean(*value),
        (lix_schema::DataType::Jsonb, value) => BodyValue::Jsonb(value.clone()),
        (lix_schema::DataType::Timestamptz, JsonValue::String(value)) => {
            BodyValue::Timestamptz(
                chrono::DateTime::parse_from_rfc3339(value)
                    .map_err(|_| invalid())?
                    .timestamp_micros(),
            )
        }
        _ => return Err(invalid()),
    })
}

fn logical_body_value(
    value: lix_schema::value_layout::BodyValue,
) -> Result<JsonValue, LixError> {
    use lix_schema::value_layout::BodyValue;
    Ok(match value {
        BodyValue::Null => JsonValue::Null,
        BodyValue::Text(value) => JsonValue::String(unescape_history_text(&value)?),
        BodyValue::Uuid(value) => JsonValue::String(value.to_string()),
        BodyValue::Int8(value) => JsonValue::from(value),
        BodyValue::Float8(value) => serde_json::Number::from_f64(value)
            .map(JsonValue::Number)
            .ok_or_else(|| body_error("native history contains non-finite float8"))?,
        BodyValue::Boolean(value) => JsonValue::Bool(value),
        BodyValue::Jsonb(value) => value,
        BodyValue::Timestamptz(value) => JsonValue::String(
            chrono::DateTime::from_timestamp_micros(value)
                .ok_or_else(|| body_error("native history contains invalid timestamptz"))?
                .to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
        ),
    })
}

/// History preserves text values that predate Schema-v1's public NUL policy.
///
/// The shared typed-row codec deliberately rejects NUL in newly-authored SQL
/// `text`, while plugin/file history already contains length-delimited text
/// with embedded NUL bytes. LXCD17 therefore uses one injective, canonical
/// history-only transform before invoking that codec. This remains a typed
/// text slot; it is not JSON, a fallback representation, or a second reader.
fn escape_history_text(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\0' => {
                escaped.push(TEXT_ESCAPE);
                escaped.push(TEXT_ESCAPED_NUL);
            }
            TEXT_ESCAPE => {
                escaped.push(TEXT_ESCAPE);
                escaped.push(TEXT_ESCAPE);
            }
            character => escaped.push(character),
        }
    }
    escaped
}

fn unescape_history_text(value: &str) -> Result<String, LixError> {
    let mut decoded = String::with_capacity(value.len());
    let mut characters = value.chars();
    while let Some(character) = characters.next() {
        if character != TEXT_ESCAPE {
            decoded.push(character);
            continue;
        }
        match characters.next() {
            Some(TEXT_ESCAPE) => decoded.push(TEXT_ESCAPE),
            Some(TEXT_ESCAPED_NUL) => decoded.push('\0'),
            _ => return Err(body_error("contains a non-canonical text escape")),
        }
    }
    Ok(decoded)
}

fn body_kind(kind: lix_schema::DataType) -> lix_schema::value_layout::BodyKind {
    use lix_schema::value_layout::BodyKind;
    match kind {
        lix_schema::DataType::Text => BodyKind::Text,
        lix_schema::DataType::Uuid => BodyKind::Uuid,
        lix_schema::DataType::Int8 => BodyKind::Int8,
        lix_schema::DataType::Float8 => BodyKind::Float8,
        lix_schema::DataType::Boolean => BodyKind::Boolean,
        lix_schema::DataType::Jsonb => BodyKind::Jsonb,
        lix_schema::DataType::Timestamptz => BodyKind::Timestamptz,
    }
}

fn kind_tag(kind: lix_schema::value_layout::BodyKind) -> u8 {
    use lix_schema::value_layout::BodyKind;
    match kind {
        BodyKind::Boolean => 1,
        BodyKind::Int8 => 2,
        BodyKind::Float8 => 3,
        BodyKind::Uuid => 4,
        BodyKind::Text => 5,
        BodyKind::Jsonb => 6,
        BodyKind::Timestamptz => 7,
    }
}

fn tag_kind(tag: u8) -> Result<lix_schema::value_layout::BodyKind, LixError> {
    use lix_schema::value_layout::BodyKind;
    match tag {
        1 => Ok(BodyKind::Boolean),
        2 => Ok(BodyKind::Int8),
        3 => Ok(BodyKind::Float8),
        4 => Ok(BodyKind::Uuid),
        5 => Ok(BodyKind::Text),
        6 => Ok(BodyKind::Jsonb),
        7 => Ok(BodyKind::Timestamptz),
        _ => Err(body_error("native history column has an unknown type")),
    }
}

fn layout_digest(columns: &[StoredColumn]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new_derive_key(LAYOUT_DOMAIN);
    hash.update(&(columns.len() as u64).to_be_bytes());
    for column in columns {
        hash.update(&(column.name.len() as u64).to_be_bytes());
        hash.update(column.name.as_bytes());
        hash.update(&[
            column.kind,
            u8::from(column.nullable),
            u8::from(column.projected),
        ]);
        match column.pk_ordinal {
            Some(ordinal) => {
                hash.update(&[1]);
                hash.update(&ordinal.to_be_bytes());
            }
            None => {
                hash.update(&[0]);
            }
        };
    }
    *hash.finalize().as_bytes()
}

fn semantic_digest(value: &JsonValue) -> [u8; 32] {
    let bytes = serde_json::to_vec(value).expect("serde_json::Value always serializes");
    *blake3::Hasher::new_derive_key(SEMANTIC_DOMAIN)
        .update(&bytes)
        .finalize()
        .as_bytes()
}

fn body_error(message: impl std::fmt::Display) -> LixError {
    LixError::new(
        LixError::CODE_STORAGE_ERROR,
        format!("LXCD17 native history body {message}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_v1_body_is_self_contained_and_distinguishes_jsonb() {
        let schema = serde_json::json!({
            "$schema": lix_schema::SCHEMA_V1_URI,
            "key": "native_history",
            "columns": [
                {"name":"id", "type":"text", "nullable":false},
                {"name":"count", "type":"int8", "nullable":false},
                {"name":"payload", "type":"jsonb", "nullable":true}
            ],
            "primary_key": ["id"]
        });
        let row_pk = RowPk::from_validated_shared_string("row".into());
        let text = r#"{"id":"row","count":7,"payload":{"z":2,"a":1}}"#;
        let encoded = encode(&schema, &row_pk, text).expect("encode");
        assert_eq!(
            decode(&row_pk, &encoded).expect("decode"),
            r#"{"count":7,"id":"row","payload":{"a":1,"z":2}}"#
        );
        for end in 0..encoded.len() {
            assert!(decode(&row_pk, &encoded[..end]).is_err());
        }
    }

    #[test]
    fn native_history_text_losslessly_carries_nul_and_escape_code_points() {
        let schema = serde_json::json!({
            "$schema": lix_schema::SCHEMA_V1_URI,
            "key": "native_history_text",
            "columns": [
                {"name":"id", "type":"text", "nullable":false},
                {"name":"payload", "type":"text", "nullable":false}
            ],
            "primary_key": ["id"]
        });
        let row_pk = RowPk::from_validated_shared_string("row".into());
        let text = "{\"id\":\"row\",\"payload\":\"left\\u0000middle\\u0001right\"}";
        let encoded = encode(&schema, &row_pk, text).expect("encode history text");
        assert_eq!(
            decode(&row_pk, &encoded).expect("decode history text"),
            text
        );
    }
}
