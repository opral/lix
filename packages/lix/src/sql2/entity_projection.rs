//! Projection-aware decoding for entity snapshot JSON.
//!
//! Entity SQL reads need only their selected fields. This module is the
//! private boundary from raw snapshot bytes to Arrow arrays. The current
//! caller adapts materialized rows; a later tracked-head reader can hand its
//! v5 JSON bytes to the same boundary directly.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use datafusion::common::DataFusionError;
use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde_json::Value as JsonValue;
use serde_json::value::RawValue;

use crate::LixError;
use crate::sql2::catalog::{EntityColumnType, EntitySurfaceSpec};
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::sql2::value_contract::{json_bigint_value, json_double_value};

/// A projection decoder for the general entity provider.
pub(crate) struct EntityProjectionDecoder {
    schema_key: String,
    fields: Vec<EntityProjectionField>,
    slots_by_name: HashMap<String, Vec<usize>>,
}

/// Keep malformed snapshots and provider-shape failures on the same
/// DataFusion `Execution` error path as the established entity projection.
/// Typed value failures already carry a Lix error code and retain that SQL
/// error contract.
pub(crate) fn entity_projection_error_to_datafusion_error(error: LixError) -> DataFusionError {
    if error.code == LixError::CODE_INTERNAL_ERROR {
        DataFusionError::Execution(error.message)
    } else {
        lix_error_to_datafusion_error(error)
    }
}

#[derive(Clone)]
struct EntityProjectionField {
    name: String,
    column_type: EntityColumnType,
}

impl EntityProjectionDecoder {
    /// Builds a decoder for visible entity columns in output order.
    pub(crate) fn new<'a>(
        spec: &EntitySurfaceSpec,
        columns: impl IntoIterator<Item = &'a str>,
    ) -> Result<Self, LixError> {
        let mut fields = Vec::new();
        let mut slots_by_name = HashMap::<String, Vec<usize>>::new();
        for column_name in columns {
            let column = spec.visible_column(column_name).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "sql2 entity provider '{}' does not expose column '{}'",
                        spec.schema_key, column_name
                    ),
                )
            })?;
            let index = fields.len();
            fields.push(EntityProjectionField {
                name: column.name.clone(),
                column_type: column.column_type,
            });
            slots_by_name
                .entry(column.name.clone())
                .or_default()
                .push(index);
        }
        Ok(Self {
            schema_key: spec.schema_key.clone(),
            fields,
            slots_by_name,
        })
    }

    /// Decodes a batch directly into Arrow arrays in constructor field order.
    pub(crate) fn decode_arrow_columns<'a>(
        &self,
        snapshots: impl IntoIterator<Item = Option<&'a [u8]>>,
    ) -> Result<Vec<ArrayRef>, LixError> {
        let snapshots = snapshots.into_iter();
        let (capacity, _) = snapshots.size_hint();
        let mut sink = ArrowProjectionSink {
            columns: self
                .fields
                .iter()
                .map(|field| EntityProjectionColumn::new(field.column_type, capacity))
                .collect(),
        };
        for snapshot in snapshots {
            self.decode_into(snapshot, &mut sink)?;
        }
        Ok(sink
            .columns
            .into_iter()
            .map(EntityProjectionColumn::into_array)
            .collect())
    }

    fn decode_into<S>(&self, snapshot: Option<&[u8]>, sink: &mut S) -> Result<(), LixError>
    where
        S: EntityProjectionSink,
    {
        let Some(snapshot) = snapshot else {
            sink.begin_row(self.fields.len());
            return Ok(());
        };
        let mut deserializer = serde_json::Deserializer::from_slice(snapshot);
        let semantic_error = RawProjectionSeed {
            decoder: self,
            sink,
        }
        .deserialize(&mut deserializer)
        .map_err(snapshot_decode_error)?;
        deserializer.end().map_err(snapshot_decode_error)?;
        semantic_error.map_or(Ok(()), Err)
    }
}

trait EntityProjectionSink {
    fn begin_row(&mut self, field_count: usize);

    fn project_raw(
        &mut self,
        decoder: &EntityProjectionDecoder,
        indices: &[usize],
        raw: &RawValue,
    ) -> Result<(), LixError>;
}

/// Deserializes only selected top-level object fields. The selected values are
/// borrowed from the source bytes and consumed immediately by the sink, so a
/// normal tracked Arrow scan has neither a snapshot JSON DOM nor per-field
/// raw-value boxes.
struct RawProjectionSeed<'decoder, 'sink, S> {
    decoder: &'decoder EntityProjectionDecoder,
    sink: &'sink mut S,
}

impl<'de, S> DeserializeSeed<'de> for RawProjectionSeed<'_, '_, S>
where
    S: EntityProjectionSink,
{
    type Value = Option<LixError>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(RawProjectionVisitor {
            decoder: self.decoder,
            sink: self.sink,
        })
    }
}

struct RawProjectionVisitor<'decoder, 'sink, S> {
    decoder: &'decoder EntityProjectionDecoder,
    sink: &'sink mut S,
}

impl<'de, S> Visitor<'de> for RawProjectionVisitor<'_, '_, S>
where
    S: EntityProjectionSink,
{
    type Value = Option<LixError>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON entity snapshot")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let RawProjectionVisitor { decoder, sink } = self;
        sink.begin_row(decoder.fields.len());
        let mut semantic_error = None;
        while let Some(key) = map.next_key::<Cow<'de, str>>()? {
            let Some(indices) = decoder.slots_by_name.get(key.as_ref()) else {
                map.next_value::<IgnoredAny>()?;
                continue;
            };
            let raw = map.next_value::<&RawValue>()?;
            if semantic_error.is_none() {
                if let Err(error) = sink.project_raw(decoder, indices, raw) {
                    semantic_error = Some(error);
                }
            }
        }
        Ok(semantic_error)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        self.sink.begin_row(self.decoder.fields.len());
        while seq.next_element::<IgnoredAny>()?.is_some() {}
        Ok(None)
    }

    fn visit_bool<E>(self, _value: bool) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.sink.begin_row(self.decoder.fields.len());
        Ok(None)
    }

    fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.sink.begin_row(self.decoder.fields.len());
        Ok(None)
    }

    fn visit_u64<E>(self, _value: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.sink.begin_row(self.decoder.fields.len());
        Ok(None)
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.sink.begin_row(self.decoder.fields.len());
        Ok(None)
    }

    fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.sink.begin_row(self.decoder.fields.len());
        Ok(None)
    }

    fn visit_borrowed_str<E>(self, _value: &'de str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.sink.begin_row(self.decoder.fields.len());
        Ok(None)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.sink.begin_row(self.decoder.fields.len());
        Ok(None)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.sink.begin_row(self.decoder.fields.len());
        Ok(None)
    }
}

struct ArrowProjectionSink {
    columns: Vec<EntityProjectionColumn>,
}

impl EntityProjectionSink for ArrowProjectionSink {
    fn begin_row(&mut self, _field_count: usize) {
        for column in &mut self.columns {
            column.push_null();
        }
    }

    fn project_raw(
        &mut self,
        decoder: &EntityProjectionDecoder,
        indices: &[usize],
        raw: &RawValue,
    ) -> Result<(), LixError> {
        for index in indices {
            self.columns[*index].replace_last_from_raw(
                raw,
                &decoder.fields[*index],
                &decoder.schema_key,
            )?;
        }
        Ok(())
    }
}

fn parse_json_value(raw: &RawValue) -> Result<JsonValue, LixError> {
    serde_json::from_str(raw.get()).map_err(snapshot_decode_error)
}

fn raw_string_text(raw: &RawValue) -> Result<Option<String>, LixError> {
    // String-valued entity fields dominate broad public reads. Deserializing
    // through `serde_json::Value` first allocates the string and then clones
    // it again in `json_value_to_string`. Decode the JSON string directly;
    // all non-string coercions retain the established general path below.
    if raw.get().trim_start().starts_with('"') {
        return serde_json::from_str(raw.get())
            .map(Some)
            .map_err(snapshot_decode_error);
    }
    crate::common::json_value_to_string(&parse_json_value(raw)?)
}

fn raw_bool(raw: &RawValue) -> Option<bool> {
    match raw.get().trim() {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn raw_json_text(raw: &RawValue) -> Option<String> {
    let json = raw.get();
    if json.trim() == "null" {
        return None;
    }
    Some(json.to_string())
}

fn snapshot_decode_error(error: serde_json::Error) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("sql2 entity provider expected valid snapshot_content JSON: {error}"),
    )
}

enum EntityProjectionColumn {
    String(Vec<Option<String>>),
    Json(Vec<Option<String>>),
    Integer(Vec<Option<i64>>),
    Number(Vec<Option<f64>>),
    Boolean(Vec<Option<bool>>),
}

impl EntityProjectionColumn {
    fn new(column_type: EntityColumnType, capacity: usize) -> Self {
        match column_type {
            EntityColumnType::String => Self::String(Vec::with_capacity(capacity)),
            EntityColumnType::Json => Self::Json(Vec::with_capacity(capacity)),
            EntityColumnType::Integer => Self::Integer(Vec::with_capacity(capacity)),
            EntityColumnType::Number => Self::Number(Vec::with_capacity(capacity)),
            EntityColumnType::Boolean => Self::Boolean(Vec::with_capacity(capacity)),
        }
    }

    fn push_null(&mut self) {
        match self {
            Self::String(values) | Self::Json(values) => values.push(None),
            Self::Integer(values) => values.push(None),
            Self::Number(values) => values.push(None),
            Self::Boolean(values) => values.push(None),
        }
    }

    fn replace_last_from_raw(
        &mut self,
        raw: &RawValue,
        field: &EntityProjectionField,
        schema_key: &str,
    ) -> Result<(), LixError> {
        match self {
            Self::String(values) if field.column_type == EntityColumnType::String => {
                *values
                    .last_mut()
                    .expect("projection sink must start the row first") = raw_string_text(raw)?;
            }
            Self::Json(values) if field.column_type == EntityColumnType::Json => {
                *values
                    .last_mut()
                    .expect("projection sink must start the row first") = raw_json_text(raw);
            }
            Self::Integer(values) if field.column_type == EntityColumnType::Integer => {
                let value = parse_json_value(raw)?;
                *values
                    .last_mut()
                    .expect("projection sink must start the row first") =
                    json_bigint_value(Some(&value), schema_key, &field.name)?;
            }
            Self::Number(values) if field.column_type == EntityColumnType::Number => {
                let value = parse_json_value(raw)?;
                *values
                    .last_mut()
                    .expect("projection sink must start the row first") =
                    json_double_value(Some(&value), schema_key, &field.name)?;
            }
            Self::Boolean(values) if field.column_type == EntityColumnType::Boolean => {
                *values
                    .last_mut()
                    .expect("projection sink must start the row first") = raw_bool(raw);
            }
            _ => {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "entity snapshot projection produced a value with the wrong SQL type",
                ));
            }
        }
        Ok(())
    }

    fn into_array(self) -> ArrayRef {
        match self {
            Self::String(values) | Self::Json(values) => Arc::new(StringArray::from(values)),
            Self::Integer(values) => Arc::new(Int64Array::from(values)),
            Self::Number(values) => Arc::new(Float64Array::from(values)),
            Self::Boolean(values) => Arc::new(BooleanArray::from(values)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use serde_json::json;

    use super::EntityProjectionDecoder;
    use crate::sql2::catalog::derive_entity_surface_spec_from_schema;
    use crate::sql2::exec::datafusion::query_result_from_batches;
    use crate::sql2::result_metadata::mark_json_field;
    use crate::transaction_types::TransactionJson;
    use crate::{Json, LixError, Value};

    fn canonical_json(canonical: &str) -> Json {
        Json::from_canonical_text(canonical)
    }

    fn spec() -> crate::sql2::catalog::EntitySurfaceSpec {
        derive_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "projection_test",
            "x-lix-primary-key": ["/text"],
            "type": "object",
            "properties": {
                "text": { "type": "string" },
                "json": { "type": "object" },
                "integer": { "type": "integer" },
                "number": { "type": "number" },
                "boolean": { "type": "boolean" },
                "coerce_bool": { "type": "string" },
                "coerce_object": { "type": "string" },
                "null_text": { "type": "string" },
                "missing": { "type": "string" }
            }
        }))
        .expect("test schema should derive")
    }

    #[test]
    #[expect(clippy::float_cmp)]
    fn decodes_selected_fields_from_canonical_tracked_arrow_projection() {
        let spec = spec();
        let decoder = EntityProjectionDecoder::new(
            &spec,
            [
                "text",
                "json",
                "integer",
                "number",
                "boolean",
                "coerce_bool",
                "coerce_object",
                "null_text",
                "missing",
            ],
        )
        .expect("decoder should build");
        let snapshot = TransactionJson::from_value(
            json!({
                "text": "line\nquote: \"",
                "json": {"z": [true, null], "a": "value"},
                "integer": 7.0,
                "number": 4.5,
                "boolean": true,
                "coerce_bool": false,
                "coerce_object": {"z": 2, "a": 1},
                "null_text": null,
                "ignored": {"nested": [1, 2, 3]}
            }),
            "canonical tracked projection test",
        )
        .expect("transaction JSON should normalize");

        let arrays = decoder
            .decode_arrow_columns([Some(snapshot.normalized().as_bytes())])
            .expect("snapshot should decode");
        let text = arrays[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("text array");
        assert_eq!(text.value(0), "line\nquote: \"");
        let json = arrays[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("JSON array");
        assert_eq!(json.value(0), r#"{"a":"value","z":[true,null]}"#);
        let integer = arrays[2]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("integer array");
        assert_eq!(integer.value(0), 7);
        let number = arrays[3]
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("number array");
        assert_eq!(number.value(0), 4.5);
        let boolean = arrays[4]
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean array");
        assert!(boolean.value(0));
        let coerce_bool = arrays[5]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("coerced bool array");
        assert_eq!(coerce_bool.value(0), "false");
        let coerce_object = arrays[6]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("coerced object array");
        assert_eq!(coerce_object.value(0), r#"{"a":1,"z":2}"#);
        assert!(arrays[7].is_null(0));
        assert!(arrays[8].is_null(0));
    }

    #[test]
    fn arrow_projection_preserves_public_result_contract() {
        let spec = spec();
        let decoder = EntityProjectionDecoder::new(
            &spec,
            [
                "text",
                "json",
                "integer",
                "number",
                "boolean",
                "coerce_bool",
                "coerce_object",
                "null_text",
                "missing",
                "text",
            ],
        )
        .expect("decoder should build");
        // Raw snapshots are allowed to contain duplicate JSON member names.
        // The visitor must retain the final member in both result paths.
        let duplicate_source_snapshot: &[u8] = br#"{
            "text":"old",
            "text":"line\nquote: \"",
            "json":{"old":true},
            "json":{"z":[true,null],"a":"value"},
            "integer":7.0,
            "number":4.5,
            "boolean":true,
            "coerce_bool":false,
            "coerce_object":{"z":2,"a":1},
            "null_text":null
        }"#;
        let snapshots = [
            Some(duplicate_source_snapshot),
            None,
            Some(br"[]".as_slice()),
        ];

        let arrays = decoder
            .decode_arrow_columns(snapshots)
            .expect("Arrow values should decode");
        let fields = vec![
            Field::new("text", DataType::Utf8, true),
            mark_json_field(Field::new("json", DataType::Utf8, true)),
            Field::new("integer", DataType::Int64, true),
            Field::new("number", DataType::Float64, true),
            Field::new("boolean", DataType::Boolean, true),
            Field::new("coerce_bool", DataType::Utf8, true),
            Field::new("coerce_object", DataType::Utf8, true),
            Field::new("null_text", DataType::Utf8, true),
            Field::new("missing", DataType::Utf8, true),
            Field::new("text", DataType::Utf8, true),
        ];
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields.clone())), arrays)
            .expect("decoded arrays should form a batch");
        let arrow_rows = query_result_from_batches(&fields, &[batch])
            .expect("Arrow result values should decode")
            .rows;

        // JSON results carry the stored bytes verbatim. Canonical member order
        // is owned by `canonicalize_transaction_json_batch` at the write
        // boundary, so this hand-built raw snapshot keeps its source order
        // rather than being re-sorted by a decode-side DOM round trip.
        assert_eq!(
            arrow_rows[0],
            vec![
                Value::Text("line\nquote: \"".to_string()),
                Value::Json(canonical_json(r#"{"z":[true,null],"a":"value"}"#)),
                Value::Integer(7),
                Value::Real(4.5),
                Value::Boolean(true),
                Value::Text("false".to_string()),
                Value::Text(r#"{"a":1,"z":2}"#.to_string()),
                Value::Null,
                Value::Null,
                Value::Text("line\nquote: \"".to_string()),
            ]
        );
        assert_eq!(arrow_rows[1], vec![Value::Null; 10]);
        assert_eq!(arrow_rows[2], vec![Value::Null; 10]);
    }

    #[test]
    fn reports_the_existing_typed_number_contract_error() {
        let spec = spec();
        let decoder = EntityProjectionDecoder::new(&spec, ["integer", "number"])
            .expect("decoder should build");
        let snapshot = TransactionJson::from_value(
            json!({"integer": "7", "number": 4.5}),
            "typed number projection test",
        )
        .expect("transaction JSON should normalize");
        let error = decoder
            .decode_arrow_columns([Some(snapshot.normalized().as_bytes())])
            .expect_err("string must not become a BIGINT");
        assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);
        assert!(error.message.contains("projection_test"));
        assert!(error.message.contains("integer"));
    }
}
