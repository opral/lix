//! Projection-aware decoding for row snapshot JSON.
//!
//! Row SQL reads need only their selected fields. This module is the
//! private boundary from raw snapshot bytes to Arrow arrays. The current
//! caller adapts materialized rows; a later tracked-head reader can hand its
//! v5 JSON bytes to the same boundary directly.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use datafusion::arrow::buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use datafusion::common::DataFusionError;
use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde_json::Value as JsonValue;
use serde_json::value::RawValue;
use smallvec::SmallVec;

use crate::LixError;
use crate::plugin::wire::typed::{
    BorrowedNativeValue, CertifiedNativeProjectionSegment, CertifiedNativeScalarKind,
    ValidatedNativePayload,
};
use crate::row_pk::{RowPk, RowPkComponent};
use crate::sql2::catalog::{SchemaColumnType, SchemaSurfaceSpec};
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::sql2::value_contract::{json_bigint_value, json_double_value};

/// A projection decoder for the general row provider.
pub(crate) struct RowProjectionDecoder {
    schema_key: String,
    schema_fingerprint: [u8; 32],
    fields: Vec<RowProjectionField>,
    slots_by_name: HashMap<String, Vec<usize>>,
    expected_fields: SmallVec<[ExpectedNativeField; 8]>,
    expected_field_slot_ranges: SmallVec<[Range<usize>; 8]>,
    expected_field_slots: SmallVec<[usize; 8]>,
    primary_key_field_ordinals: SmallVec<[usize; 4]>,
    primary_key_kinds: SmallVec<[CertifiedNativeScalarKind; 4]>,
}

/// Keep malformed snapshots and provider-shape failures on the same
/// DataFusion `Execution` error path as the established row projection.
/// Typed value failures already carry a Lix error code and retain that SQL
/// error contract.
pub(crate) fn row_projection_error_to_datafusion_error(error: LixError) -> DataFusionError {
    if error.code == LixError::CODE_INTERNAL_ERROR {
        DataFusionError::Execution(error.message)
    } else {
        lix_error_to_datafusion_error(error)
    }
}

#[derive(Clone)]
struct RowProjectionField {
    name: String,
    column_type: SchemaColumnType,
}

#[derive(Clone)]
struct ExpectedNativeField {
    name: String,
    data_type: lix_schema::DataType,
    nullable: bool,
}

enum NativeProjectionPayload<'a> {
    Raw(&'a [u8]),
    Validated(&'a ValidatedNativePayload),
}

fn visit_projection_native_payload<'a>(
    payload: NativeProjectionPayload<'a>,
    visit_key: impl FnMut(usize, BorrowedNativeValue<'a>),
    visit_field: impl FnMut(&'a str, BorrowedNativeValue<'a>),
) -> Result<[u8; 32], crate::plugin::wire::typed::Error> {
    match payload {
        NativeProjectionPayload::Raw(payload) => {
            crate::plugin::wire::typed::visit_native_row_payload(payload, visit_key, visit_field)
        }
        NativeProjectionPayload::Validated(payload) => {
            crate::plugin::wire::typed::visit_validated_native_row_payload(
                payload,
                visit_key,
                visit_field,
            )
        }
    }
}

impl RowProjectionDecoder {
    /// Builds a decoder for visible row columns in output order.
    pub(crate) fn new<'a>(
        spec: &SchemaSurfaceSpec,
        columns: impl IntoIterator<Item = &'a str>,
    ) -> Result<Self, LixError> {
        let mut fields = Vec::new();
        let mut slots_by_name = HashMap::<String, Vec<usize>>::new();
        for column_name in columns {
            let column = spec.visible_column(column_name).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "sql2 row provider '{}' does not expose column '{}'",
                        spec.schema_key, column_name
                    ),
                )
            })?;
            let index = fields.len();
            fields.push(RowProjectionField {
                name: column.name.clone(),
                column_type: column.column_type,
            });
            slots_by_name
                .entry(column.name.clone())
                .or_default()
                .push(index);
        }
        let mut expected_fields = spec
            .columns
            .iter()
            .map(|column| ExpectedNativeField {
                name: column.name.clone(),
                data_type: column.native_type,
                nullable: column.read_nullable,
            })
            .collect::<SmallVec<[_; 8]>>();
        expected_fields.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        let mut expected_field_slots = SmallVec::<[usize; 8]>::with_capacity(fields.len());
        let expected_field_slot_ranges = expected_fields
            .iter()
            .map(|expected| {
                let start = expected_field_slots.len();
                if let Some(slots) = slots_by_name.get(&expected.name) {
                    expected_field_slots.extend_from_slice(slots);
                }
                start..expected_field_slots.len()
            })
            .collect::<SmallVec<[_; 8]>>();
        let primary_key_field_ordinals = spec
            .primary_key_paths
            .iter()
            .map(|path| {
                let [name] = path.as_slice() else {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "Schema v1 primary key is not a top-level column",
                    ));
                };
                expected_fields
                    .binary_search_by(|field| field.name.as_str().cmp(name))
                    .map_err(|_| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "Schema v1 primary-key column is absent from its SQL surface",
                        )
                    })
            })
            .collect::<Result<SmallVec<[_; 4]>, _>>()?;
        let primary_key_kinds = spec
            .primary_key_component_types
            .iter()
            .map(|kind| match kind {
                crate::row_pk::RowPkComponentType::String => CertifiedNativeScalarKind::Text,
                crate::row_pk::RowPkComponentType::Uuid => CertifiedNativeScalarKind::Uuid,
                crate::row_pk::RowPkComponentType::Integer => CertifiedNativeScalarKind::Int8,
                crate::row_pk::RowPkComponentType::Bytes => {
                    unreachable!("Schema v1 primary keys never contain raw byte components")
                }
            })
            .collect::<SmallVec<[_; 4]>>();
        Ok(Self {
            schema_key: spec.schema_key.clone(),
            schema_fingerprint: spec.schema_fingerprint,
            fields,
            slots_by_name,
            expected_fields,
            expected_field_slot_ranges,
            expected_field_slots,
            primary_key_field_ordinals,
            primary_key_kinds,
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
                .map(|field| RowProjectionColumn::new(field.column_type, capacity))
                .collect(),
        };
        for snapshot in snapshots {
            self.decode_into(snapshot, &mut sink)?;
        }
        Ok(sink
            .columns
            .into_iter()
            .map(RowProjectionColumn::into_array)
            .collect())
    }

    /// Decodes a mixed live-state batch. Native Schema v1 rows are projected
    /// directly; internal engine records use their own raw JSON projection.
    pub(crate) fn decode_mixed_arrow_columns<'a>(
        &self,
        rows: impl IntoIterator<Item = (Option<&'a [u8]>, Option<&'a lix_schema::Row>)>,
    ) -> Result<Vec<ArrayRef>, LixError> {
        let rows = rows.into_iter();
        let (capacity, _) = rows.size_hint();
        let mut sink = ArrowProjectionSink {
            columns: self
                .fields
                .iter()
                .map(|field| RowProjectionColumn::new(field.column_type, capacity))
                .collect(),
        };
        for (snapshot, typed_row) in rows {
            if let Some(typed_row) = typed_row {
                sink.begin_row(self.fields.len());
                for (index, field) in self.fields.iter().enumerate() {
                    sink.columns[index].replace_last_from_typed(
                        typed_row.get(&field.name),
                        field,
                        &self.schema_key,
                    )?;
                }
            } else {
                self.decode_into(snapshot, &mut sink)?;
            }
        }
        Ok(sink
            .columns
            .into_iter()
            .map(RowProjectionColumn::into_array)
            .collect())
    }

    /// Projects durable payloads directly into Arrow builders. Native v2 and
    /// compact built-in v3 rows stay borrowed; compressed v4 engine rows pay
    /// only their bounded decompression allocation. Every encoded value is
    /// validated, including unselected ones.
    pub(crate) fn decode_durable_payload_arrow_columns<'a>(
        &self,
        rows: impl IntoIterator<Item = (&'a [u8], &'a RowPk)>,
    ) -> Result<Vec<ArrayRef>, LixError> {
        let rows = rows.into_iter();
        let (capacity, _) = rows.size_hint();
        let mut sink = ArrowProjectionSink {
            columns: self
                .fields
                .iter()
                .map(|field| RowProjectionColumn::new(field.column_type, capacity))
                .collect(),
        };
        for (payload, row_pk) in rows {
            sink.begin_row(self.fields.len());
            self.decode_durable_payload_into(payload, row_pk, &mut sink)?;
        }
        Ok(sink
            .columns
            .into_iter()
            .map(RowProjectionColumn::into_array)
            .collect())
    }

    /// Projects a visibility batch that spans transient JSON, decoded native
    /// rows, and durable native bytes. This is the branch/merge fallback where
    /// choosing one physical source for the whole batch would silently turn
    /// rows from the other sources into SQL NULLs.
    pub(crate) fn decode_mixed_durable_arrow_columns<'a>(
        &self,
        rows: impl IntoIterator<
            Item = (
                Option<&'a [u8]>,
                Option<&'a [u8]>,
                Option<&'a lix_schema::Row>,
                &'a RowPk,
            ),
        >,
    ) -> Result<Vec<ArrayRef>, LixError> {
        let rows = rows.into_iter();
        let (capacity, _) = rows.size_hint();
        let mut sink = ArrowProjectionSink {
            columns: self
                .fields
                .iter()
                .map(|field| RowProjectionColumn::new(field.column_type, capacity))
                .collect(),
        };
        for (raw, snapshot, typed, row_pk) in rows {
            if let Some(raw) = raw {
                sink.begin_row(self.fields.len());
                self.decode_durable_payload_into(raw, row_pk, &mut sink)?;
            } else if let Some(typed) = typed {
                sink.begin_row(self.fields.len());
                for (index, field) in self.fields.iter().enumerate() {
                    sink.columns[index].replace_last_from_typed(
                        typed.get(&field.name),
                        field,
                        &self.schema_key,
                    )?;
                }
            } else {
                self.decode_into(snapshot, &mut sink)?;
            }
        }
        Ok(sink
            .columns
            .into_iter()
            .map(RowProjectionColumn::into_array)
            .collect())
    }

    fn decode_durable_payload_into(
        &self,
        payload: &[u8],
        row_pk: &RowPk,
        sink: &mut ArrowProjectionSink,
    ) -> Result<(), LixError> {
        let decoded;
        let payload = if payload.first().copied()
            == Some(crate::plugin::runtime::COMPRESSED_ENGINE_ROW_PAYLOAD_VERSION)
        {
            decoded = crate::plugin::runtime::decompress_engine_row_payload(payload)?;
            decoded.as_ref()
        } else {
            payload
        };
        match payload.first().copied() {
            Some(
                crate::plugin::wire::typed::NATIVE_ROW_PAYLOAD_VERSION
                | crate::plugin::wire::typed::STORAGE_ROW_PAYLOAD_VERSION,
            ) => self.decode_native_payload_into(
                NativeProjectionPayload::Raw(payload),
                row_pk,
                sink,
            ),
            Some(crate::plugin::wire::typed::ENGINE_ROW_PAYLOAD_VERSION) => {
                self.decode_engine_payload_into(payload, row_pk, sink)
            }
            version => Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "cannot project durable typed payload for schema '{}': unsupported payload version {version:?}",
                    self.schema_key
                ),
            )),
        }
    }

    /// Projects native payloads whose complete wire and canonical scalars
    /// were validated unchanged at an immutable storage boundary. Resolved
    /// schema and envelope identity checks remain identical to the raw path.
    pub(crate) fn decode_validated_native_payload_arrow_columns<'a>(
        &self,
        rows: impl IntoIterator<Item = (&'a ValidatedNativePayload, &'a RowPk)>,
    ) -> Result<Vec<ArrayRef>, LixError> {
        let rows = rows.into_iter();
        let (capacity, _) = rows.size_hint();
        let mut sink = ArrowProjectionSink {
            columns: self
                .fields
                .iter()
                .map(|field| RowProjectionColumn::new(field.column_type, capacity))
                .collect(),
        };
        for (payload, row_pk) in rows {
            sink.begin_row(self.fields.len());
            self.decode_native_payload_into(
                NativeProjectionPayload::Validated(payload),
                row_pk,
                &mut sink,
            )?;
        }
        Ok(sink
            .columns
            .into_iter()
            .map(RowProjectionColumn::into_array)
            .collect())
    }

    pub(crate) fn decode_owned_validated_native_payload_arrow_columns(
        &self,
        rows: impl IntoIterator<Item = Result<(ValidatedNativePayload, RowPk), LixError>>,
    ) -> Result<Vec<ArrayRef>, LixError> {
        let rows = rows.into_iter();
        let (capacity, _) = rows.size_hint();
        let mut sink = ArrowProjectionSink {
            columns: self
                .fields
                .iter()
                .map(|field| RowProjectionColumn::new(field.column_type, capacity))
                .collect(),
        };
        for row in rows {
            let (payload, row_pk) = row?;
            sink.begin_row(self.fields.len());
            self.decode_native_payload_into(
                NativeProjectionPayload::Validated(&payload),
                &row_pk,
                &mut sink,
            )?;
        }
        Ok(sink
            .columns
            .into_iter()
            .map(RowProjectionColumn::into_array)
            .collect())
    }

    /// Projects one envelope-bound immutable batch into one set of Arrow
    /// arrays. Storage proves every part has the same canonical layout, so the
    /// resolved Schema v1 comparison runs once before any row is appended.
    pub(crate) fn decode_certified_native_projection_batch(
        &self,
        batch: &crate::tracked_state::EnvelopeCertifiedNativeProjectionBatch,
    ) -> Result<Vec<ArrayRef>, LixError> {
        let mut segments = batch.segments();
        let first = segments.next().ok_or_else(|| {
            native_shape_error(&self.schema_key, "certified projection batch is empty")
        })?;
        self.validate_certified_native_layout(first.projection(), first.has_outer_row_keys())?;
        if batch.reuse_eligible()
            && self.fields.len() == self.expected_fields.len()
            && self
                .expected_field_slot_ranges
                .iter()
                .all(|slots| slots.len() == 1)
        {
            if let Some(cached) = batch.combined_arrow_columns() {
                return self.reorder_certified_full_columns(cached);
            }
            let mut cached_parts = SmallVec::<[&[ArrayRef]; 8]>::new();
            for envelope in std::iter::once(first).chain(segments) {
                let cached = if let Some(columns) = envelope.arrow_columns() {
                    columns
                } else {
                    self.validate_certified_native_outer_keys(envelope)?;
                    let mut sink = ArrowProjectionSink {
                        columns: self
                            .expected_field_slot_ranges
                            .iter()
                            .map(|slots| {
                                RowProjectionColumn::new(
                                    self.fields[self.expected_field_slots[slots.start]].column_type,
                                    envelope.projection().row_count(),
                                )
                            })
                            .collect(),
                    };
                    for row_ordinal in 0..envelope.projection().row_count() {
                        for (field_ordinal, column) in sink.columns.iter_mut().enumerate() {
                            let value = envelope
                                .projection()
                                .field_value(row_ordinal, field_ordinal)
                                .ok_or_else(|| native_shape_error(&self.schema_key, "certified projection omitted a field value"))?;
                            let slot = self.expected_field_slots
                                [self.expected_field_slot_ranges[field_ordinal].start];
                            column.push_from_native(value, &self.fields[slot], &self.schema_key)?;
                        }
                    }
                    envelope.cache_arrow_columns(
                        sink.columns.into_iter().map(RowProjectionColumn::into_array).collect(),
                    )
                };
                cached_parts.push(cached);
            }
            let mut combined = Vec::with_capacity(self.expected_fields.len());
            for expected_ordinal in 0..self.expected_fields.len() {
                if cached_parts.len() == 1 {
                    combined.push(Arc::clone(&cached_parts[0][expected_ordinal]));
                } else {
                    let arrays = cached_parts.iter().map(|columns| columns[expected_ordinal].as_ref()).collect::<SmallVec<[&dyn Array; 8]>>();
                    combined.push(datafusion::arrow::compute::concat(&arrays).map_err(|_| native_shape_error(&self.schema_key, "certified Arrow segments could not be concatenated"))?);
                }
            }
            let cached = batch.cache_combined_arrow_columns(combined);
            return self.reorder_certified_full_columns(cached);
        }
        let mut sink = ArrowProjectionSink {
            columns: self.fields.iter().map(|field| RowProjectionColumn::new(field.column_type, batch.len())).collect(),
        };
        for envelope in std::iter::once(first).chain(segments) {
            let segment = envelope.projection();
            self.validate_certified_native_outer_keys(envelope)?;
            for row_ordinal in 0..segment.row_count() {
                for (field_ordinal, slots) in self.expected_field_slot_ranges.iter().enumerate() {
                    if slots.is_empty() { continue; }
                    let value = segment.field_value(row_ordinal, field_ordinal).ok_or_else(|| native_shape_error(&self.schema_key, "certified projection omitted a field value"))?;
                    for &slot in &self.expected_field_slots[slots.clone()] {
                        sink.columns[slot].push_from_native(value, &self.fields[slot], &self.schema_key)?;
                    }
                }
            }
        }
        Ok(sink.columns.into_iter().map(RowProjectionColumn::into_array).collect())
    }

    fn reorder_certified_full_columns(
        &self,
        cached: &[ArrayRef],
    ) -> Result<Vec<ArrayRef>, LixError> {
        self.fields
            .iter()
            .map(|field| {
                let expected_ordinal = self
                    .expected_fields
                    .binary_search_by(|expected| expected.name.cmp(&field.name))
                    .map_err(|_| {
                        native_shape_error(
                            &self.schema_key,
                            "certified output field is absent from its schema",
                        )
                    })?;
                Ok(Arc::clone(&cached[expected_ordinal]))
            })
            .collect()
    }

    fn validate_certified_native_layout(
        &self,
        segment: &CertifiedNativeProjectionSegment,
        has_outer_row_keys: bool,
    ) -> Result<(), LixError> {
        if segment.schema_fingerprint() != self.schema_fingerprint
            || segment.fields().len() != self.expected_fields.len()
            || (!has_outer_row_keys
                && segment.key_kinds() != self.primary_key_kinds.as_slice())
            || (has_outer_row_keys && !segment.key_kinds().is_empty())
        {
            return Err(native_shape_error(
                &self.schema_key,
                "certified layout does not match its resolved schema",
            ));
        }
        for (field, expected) in segment.fields().iter().zip(&self.expected_fields) {
            if field.name() != expected.name
                || field
                    .observed_non_null_kind()
                    .is_some_and(|kind| !certified_kind_matches_data_type(kind, expected.data_type))
                || (field.saw_null() && !expected.nullable)
                || (field.observed_non_null_kind().is_none() && !expected.nullable)
            {
                return Err(native_shape_error(
                    &self.schema_key,
                    "certified layout does not satisfy its resolved schema",
                ));
            }
        }
        if !has_outer_row_keys {
            for (key_ordinal, &field_ordinal) in self.primary_key_field_ordinals.iter().enumerate() {
                if !segment.key_equals_field(key_ordinal, field_ordinal) {
                    return Err(native_shape_error(
                        &self.schema_key,
                        "certified primary key does not match its row field",
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_certified_native_outer_keys(
        &self,
        envelope: &crate::tracked_state::EnvelopeCertifiedNativeProjectionSegment,
    ) -> Result<(), LixError> {
        let Some(row_key_count) = envelope.outer_row_key_count() else {
            return Ok(());
        };
        let segment = envelope.projection();
        if row_key_count != segment.row_count() {
            return Err(native_shape_error(
                &self.schema_key,
                "certified outer primary-key count does not match its rows",
            ));
        }
        for row_ordinal in 0..row_key_count {
            let Some(row_key) = envelope.outer_row_key(row_ordinal) else {
                return Err(native_shape_error(
                    &self.schema_key,
                    "certified outer primary-key range is invalid",
                ));
            };
            let values = self
                .primary_key_field_ordinals
                .iter()
                .map(|&field_ordinal| segment.field_value(row_ordinal, field_ordinal));
            if !encoded_row_key_matches(row_key, values) {
                return Err(native_shape_error(
                    &self.schema_key,
                    "certified row field disagrees with its outer primary key",
                ));
            }
        }
        Ok(())
    }

    fn decode_native_payload_into(
        &self,
        payload: NativeProjectionPayload<'_>,
        row_pk: &RowPk,
        sink: &mut ArrowProjectionSink,
    ) -> Result<(), LixError> {
        let mut key_matches = true;
        let mut key_components = 0usize;
        let mut field_key_matches = true;
        let mut field_key_components = 0usize;
        let mut fields_seen = 0usize;
        let mut projection_error = None;
        let fingerprint = visit_projection_native_payload(
            payload,
            |index, value| {
                key_components = key_components.saturating_add(1);
                key_matches &= row_pk
                    .components
                    .as_slice()
                    .get(index)
                    .is_some_and(|component| borrowed_key_matches(value, component));
            },
            |name, value| {
                if projection_error.is_some() {
                    return;
                }
                let expected_index = fields_seen;
                let Some(expected) = self.expected_fields.get(expected_index) else {
                    projection_error = Some(native_shape_error(
                        &self.schema_key,
                        "contains an undeclared column",
                    ));
                    return;
                };
                fields_seen += 1;
                if expected.name != name
                    || !borrowed_value_matches_schema(value, expected.data_type, expected.nullable)
                {
                    projection_error = Some(native_shape_error(
                        &self.schema_key,
                        "does not satisfy its resolved schema",
                    ));
                    return;
                }
                if let Some(key_ordinal) = self
                    .primary_key_field_ordinals
                    .iter()
                    .position(|&field_ordinal| field_ordinal == expected_index)
                {
                    field_key_components = field_key_components.saturating_add(1);
                    field_key_matches &= row_pk
                        .components
                        .as_slice()
                        .get(key_ordinal)
                        .is_some_and(|component| borrowed_key_matches(value, component));
                }
                for &index in &self.expected_field_slots
                    [self.expected_field_slot_ranges[expected_index].clone()]
                {
                    if let Err(error) = sink.columns[index].replace_last_from_native(
                        value,
                        &self.fields[index],
                        &self.schema_key,
                    ) {
                        projection_error = Some(error);
                        break;
                    }
                }
            },
        )
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "cannot project durable typed payload for schema '{}': {error:?}",
                    self.schema_key
                ),
            )
        })?;
        if fingerprint != self.schema_fingerprint {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "durable typed row fingerprint for schema '{}' does not match the resolved schema",
                    self.schema_key
                ),
            ));
        }
        if fields_seen != self.expected_fields.len() {
            return Err(native_shape_error(
                &self.schema_key,
                "is missing a declared column",
            ));
        }
        let envelope_matches = if key_components == 0 {
            field_key_matches && field_key_components == row_pk.components.len()
        } else {
            key_matches && key_components == row_pk.components.len()
        };
        if !envelope_matches {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "durable typed payload identity does not match the stored envelope for schema '{}'",
                    self.schema_key
                ),
            ));
        }
        if let Some(error) = projection_error {
            return Err(error);
        }
        Ok(())
    }

    fn decode_engine_payload_into(
        &self,
        payload: &[u8],
        row_pk: &RowPk,
        sink: &mut ArrowProjectionSink,
    ) -> Result<(), LixError> {
        let (_, plan) = crate::catalog::CatalogSnapshot::builtin()
            .plan_for_key(&self.schema_key)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "compact engine typed payload references non-built-in schema '{}'",
                        self.schema_key
                    ),
                )
            })?;
        if plan.fingerprint().bytes() != self.schema_fingerprint {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "durable typed row fingerprint for schema '{}' does not match the resolved schema",
                    self.schema_key
                ),
            ));
        }
        let primary_key = plan.compiled_schema.primary_key();
        let mut fields_seen = 0usize;
        let mut key_matches = true;
        let mut key_components = 0usize;
        let mut projection_error = None;
        crate::plugin::wire::typed::visit_engine_row_payload(
            payload,
            &plan.compiled_schema,
            |name, value| {
                fields_seen = fields_seen.saturating_add(1);
                let Ok(expected_index) = self
                    .expected_fields
                    .binary_search_by(|expected| expected.name.as_str().cmp(name))
                else {
                    projection_error = Some(native_shape_error(
                        &self.schema_key,
                        "contains an undeclared column",
                    ));
                    return;
                };
                let expected = &self.expected_fields[expected_index];
                if !borrowed_value_matches_schema(value, expected.data_type, expected.nullable) {
                    projection_error = Some(native_shape_error(
                        &self.schema_key,
                        "does not satisfy its resolved schema",
                    ));
                    return;
                }
                if let Some(key_index) = primary_key.iter().position(|column| column == name) {
                    key_components = key_components.saturating_add(1);
                    key_matches &= row_pk
                        .components
                        .as_slice()
                        .get(key_index)
                        .is_some_and(|component| borrowed_key_matches(value, component));
                }
                if let Some(indices) = self.slots_by_name.get(name) {
                    for &index in indices {
                        if let Err(error) = sink.columns[index].replace_last_from_native(
                            value,
                            &self.fields[index],
                            &self.schema_key,
                        ) {
                            projection_error = Some(error);
                            break;
                        }
                    }
                }
            },
        )
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "cannot project durable typed payload for schema '{}': {error:?}",
                    self.schema_key
                ),
            )
        })?;
        if fields_seen != self.expected_fields.len() {
            return Err(native_shape_error(
                &self.schema_key,
                "is missing a declared column",
            ));
        }
        if !key_matches || key_components != row_pk.components.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "durable typed payload identity does not match the stored envelope for schema '{}'",
                    self.schema_key
                ),
            ));
        }
        if let Some(error) = projection_error {
            return Err(error);
        }
        Ok(())
    }

    /// Decodes one exact point result directly into the public scalar row.
    ///
    /// This is deliberately narrower than the Arrow path: callers must have
    /// already proved a unique registered-schema point query. Keeping the
    /// same decoder and raw-value coercions makes the native result an output
    /// boundary only, not a second row interpretation authority.
    pub(crate) fn decode_public_values(
        &self,
        snapshot: Option<&[u8]>,
    ) -> Result<Vec<crate::Value>, LixError> {
        let mut sink = PublicProjectionSink { values: Vec::new() };
        self.decode_into(snapshot, &mut sink)?;
        Ok(sink.values)
    }

    /// Decodes one typed plugin row at the public scalar boundary.
    pub(crate) fn decode_typed_public_values(
        &self,
        row: &lix_schema::Row,
    ) -> Result<Vec<crate::Value>, LixError> {
        self.fields
            .iter()
            .map(|field| typed_public_value(row.get(&field.name), field, &self.schema_key))
            .collect()
    }

    fn decode_into<S>(&self, snapshot: Option<&[u8]>, sink: &mut S) -> Result<(), LixError>
    where
        S: RowProjectionSink,
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

trait RowProjectionSink {
    fn begin_row(&mut self, field_count: usize);

    fn project_raw(
        &mut self,
        decoder: &RowProjectionDecoder,
        indices: &[usize],
        raw: &RawValue,
    ) -> Result<(), LixError>;
}

/// Deserializes only selected top-level object fields. The selected values are
/// borrowed from the source bytes and consumed immediately by the sink, so a
/// normal tracked Arrow scan has neither a snapshot JSON DOM nor per-field
/// raw-value boxes.
struct RawProjectionSeed<'decoder, 'sink, S> {
    decoder: &'decoder RowProjectionDecoder,
    sink: &'sink mut S,
}

impl<'de, S> DeserializeSeed<'de> for RawProjectionSeed<'_, '_, S>
where
    S: RowProjectionSink,
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
    decoder: &'decoder RowProjectionDecoder,
    sink: &'sink mut S,
}

impl<'de, S> Visitor<'de> for RawProjectionVisitor<'_, '_, S>
where
    S: RowProjectionSink,
{
    type Value = Option<LixError>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON row snapshot")
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
    columns: Vec<RowProjectionColumn>,
}

struct PublicProjectionSink {
    values: Vec<crate::Value>,
}

impl RowProjectionSink for PublicProjectionSink {
    fn begin_row(&mut self, field_count: usize) {
        self.values = vec![crate::Value::Null; field_count];
    }

    fn project_raw(
        &mut self,
        decoder: &RowProjectionDecoder,
        indices: &[usize],
        raw: &RawValue,
    ) -> Result<(), LixError> {
        for index in indices {
            let field = &decoder.fields[*index];
            let value = match field.column_type {
                SchemaColumnType::String => raw_string_text(raw)?
                    .map(crate::Value::Text)
                    .unwrap_or(crate::Value::Null),
                SchemaColumnType::Jsonb => raw_json_text(raw)
                    .map(|json| crate::Value::Jsonb(crate::Json::from_canonical_text(json)))
                    .unwrap_or(crate::Value::Null),
                SchemaColumnType::Integer => {
                    let value = parse_json_value(raw)?;
                    json_bigint_value(Some(&value), &decoder.schema_key, &field.name)?
                        .map(crate::Value::Integer)
                        .unwrap_or(crate::Value::Null)
                }
                SchemaColumnType::Number => {
                    let value = parse_json_value(raw)?;
                    json_double_value(Some(&value), &decoder.schema_key, &field.name)?
                        .map(crate::Value::Real)
                        .unwrap_or(crate::Value::Null)
                }
                SchemaColumnType::Boolean => raw_bool(raw)
                    .map(crate::Value::Boolean)
                    .unwrap_or(crate::Value::Null),
                SchemaColumnType::Timestamptz => raw_string_text(raw)?
                    .map(|value| {
                        chrono::DateTime::parse_from_rfc3339(&value)
                            .map(|timestamp| {
                                crate::Value::Timestamptz(timestamp.timestamp_micros())
                            })
                            .map_err(|error| {
                                LixError::new(
                                    LixError::CODE_TYPE_MISMATCH,
                                    format!(
                                        "invalid timestamptz value for {}.{}: {error}",
                                        decoder.schema_key, field.name
                                    ),
                                )
                            })
                    })
                    .transpose()?
                    .unwrap_or(crate::Value::Null),
            };
            self.values[*index] = value;
        }
        Ok(())
    }
}

impl RowProjectionSink for ArrowProjectionSink {
    fn begin_row(&mut self, _field_count: usize) {
        for column in &mut self.columns {
            column.push_null();
        }
    }

    fn project_raw(
        &mut self,
        decoder: &RowProjectionDecoder,
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
    // String-valued row fields dominate broad public reads. Deserializing
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
        format!("sql2 row provider expected valid snapshot_content JSON: {error}"),
    )
}

enum RowProjectionColumn {
    String(VariableWidthProjection),
    Jsonb(VariableWidthProjection),
    Integer(Vec<Option<i64>>),
    Number(Vec<Option<f64>>),
    Boolean(Vec<Option<bool>>),
    Timestamptz(Vec<Option<i64>>),
}

/// Mutable UTF-8 Arrow storage whose current row can be filled after the
/// projection decoder has first reserved it as null. Keeping one values arena
/// and one offset vector avoids allocating a `String` owner for every typed
/// value before Arrow immediately copies it again.
struct VariableWidthProjection {
    offsets: Vec<i32>,
    values: Vec<u8>,
    valid: Vec<bool>,
}

impl VariableWidthProjection {
    fn with_capacity(capacity: usize) -> Self {
        let mut offsets = Vec::with_capacity(capacity.saturating_add(1));
        offsets.push(0);
        Self {
            offsets,
            values: Vec::new(),
            valid: Vec::with_capacity(capacity),
        }
    }

    fn push_null(&mut self) {
        self.offsets
            .push(i32::try_from(self.values.len()).expect("Arrow UTF-8 values fit i32"));
        self.valid.push(false);
    }

    fn push(&mut self, value: Option<&str>) -> Result<(), LixError> {
        if let Some(value) = value {
            self.values.extend_from_slice(value.as_bytes());
        }
        let end = i32::try_from(self.values.len()).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "projected UTF-8 values exceed Arrow's i32 offset range",
            )
        })?;
        self.offsets.push(end);
        self.valid.push(value.is_some());
        Ok(())
    }

    fn replace_last(&mut self, value: Option<&str>) -> Result<(), LixError> {
        let row_start = usize::try_from(
            *self
                .offsets
                .get(self.offsets.len().saturating_sub(2))
                .expect("projection sink must start the row first"),
        )
        .expect("Arrow UTF-8 offset is nonnegative");
        self.values.truncate(row_start);
        if let Some(value) = value {
            self.values.extend_from_slice(value.as_bytes());
        }
        let end = i32::try_from(self.values.len()).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "projected UTF-8 values exceed Arrow's i32 offset range",
            )
        })?;
        *self
            .offsets
            .last_mut()
            .expect("projection sink must start the row first") = end;
        *self
            .valid
            .last_mut()
            .expect("projection sink must start the row first") = value.is_some();
        Ok(())
    }

    fn into_array(self) -> StringArray {
        StringArray::new(
            OffsetBuffer::new(ScalarBuffer::from(self.offsets)),
            Buffer::from(self.values),
            Some(NullBuffer::from(self.valid)),
        )
    }
}

impl RowProjectionColumn {
    fn new(column_type: SchemaColumnType, capacity: usize) -> Self {
        match column_type {
            SchemaColumnType::String => {
                Self::String(VariableWidthProjection::with_capacity(capacity))
            }
            SchemaColumnType::Jsonb => {
                Self::Jsonb(VariableWidthProjection::with_capacity(capacity))
            }
            SchemaColumnType::Integer => Self::Integer(Vec::with_capacity(capacity)),
            SchemaColumnType::Number => Self::Number(Vec::with_capacity(capacity)),
            SchemaColumnType::Boolean => Self::Boolean(Vec::with_capacity(capacity)),
            SchemaColumnType::Timestamptz => Self::Timestamptz(Vec::with_capacity(capacity)),
        }
    }

    fn push_null(&mut self) {
        match self {
            Self::String(values) | Self::Jsonb(values) => values.push_null(),
            Self::Integer(values) => values.push(None),
            Self::Number(values) => values.push(None),
            Self::Boolean(values) => values.push(None),
            Self::Timestamptz(values) => values.push(None),
        }
    }

    fn replace_last_from_raw(
        &mut self,
        raw: &RawValue,
        field: &RowProjectionField,
        schema_key: &str,
    ) -> Result<(), LixError> {
        match self {
            Self::String(values) if field.column_type == SchemaColumnType::String => {
                let value = raw_string_text(raw)?;
                values.replace_last(value.as_deref())?;
            }
            Self::Jsonb(values) if field.column_type == SchemaColumnType::Jsonb => {
                let value = raw_json_text(raw);
                values.replace_last(value.as_deref())?;
            }
            Self::Integer(values) if field.column_type == SchemaColumnType::Integer => {
                let value = parse_json_value(raw)?;
                *values
                    .last_mut()
                    .expect("projection sink must start the row first") =
                    json_bigint_value(Some(&value), schema_key, &field.name)?;
            }
            Self::Number(values) if field.column_type == SchemaColumnType::Number => {
                let value = parse_json_value(raw)?;
                *values
                    .last_mut()
                    .expect("projection sink must start the row first") =
                    json_double_value(Some(&value), schema_key, &field.name)?;
            }
            Self::Boolean(values) if field.column_type == SchemaColumnType::Boolean => {
                *values
                    .last_mut()
                    .expect("projection sink must start the row first") = raw_bool(raw);
            }
            Self::Timestamptz(values) if field.column_type == SchemaColumnType::Timestamptz => {
                let value = raw_string_text(raw)?;
                *values
                    .last_mut()
                    .expect("projection sink must start the row first") = value
                    .map(|value| {
                        chrono::DateTime::parse_from_rfc3339(&value)
                            .map(|timestamp| timestamp.timestamp_micros())
                            .map_err(|error| {
                                LixError::new(
                                    LixError::CODE_TYPE_MISMATCH,
                                    format!(
                                        "invalid timestamptz value for {}.{}: {error}",
                                        schema_key, field.name
                                    ),
                                )
                            })
                    })
                    .transpose()?;
            }
            _ => {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "row snapshot projection produced a value with the wrong SQL type",
                ));
            }
        }
        Ok(())
    }

    fn replace_last_from_typed(
        &mut self,
        value: Option<&lix_schema::Value>,
        field: &RowProjectionField,
        schema_key: &str,
    ) -> Result<(), LixError> {
        let value = typed_public_value(value, field, schema_key)?;
        match (self, value) {
            (Self::String(values), crate::Value::Null)
            | (Self::Jsonb(values), crate::Value::Null) => {
                values.replace_last(None)?;
            }
            (Self::String(values), crate::Value::Text(value)) => {
                values.replace_last(Some(&value))?;
            }
            (Self::Jsonb(values), crate::Value::Jsonb(value)) => {
                let value = value.to_string();
                values.replace_last(Some(&value))?;
            }
            (Self::Integer(values), crate::Value::Integer(value)) => {
                *values
                    .last_mut()
                    .expect("projection sink must start the row first") = Some(value);
            }
            (Self::Number(values), crate::Value::Real(value)) => {
                *values
                    .last_mut()
                    .expect("projection sink must start the row first") = Some(value);
            }
            (Self::Boolean(values), crate::Value::Boolean(value)) => {
                *values
                    .last_mut()
                    .expect("projection sink must start the row first") = Some(value);
            }
            (Self::Timestamptz(values), crate::Value::Timestamptz(value)) => {
                *values
                    .last_mut()
                    .expect("projection sink must start the row first") = Some(value);
            }
            _ => {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "typed row projection produced a value with the wrong SQL type",
                ));
            }
        }
        Ok(())
    }

    fn replace_last_from_native(
        &mut self,
        value: BorrowedNativeValue<'_>,
        field: &RowProjectionField,
        schema_key: &str,
    ) -> Result<(), LixError> {
        let mismatch = || {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "typed row projection value does not match {schema_key}.{}",
                    field.name
                ),
            )
        };
        match (self, value) {
            (Self::String(values) | Self::Jsonb(values), BorrowedNativeValue::Null) => {
                values.replace_last(None)?;
            }
            (Self::Integer(values), BorrowedNativeValue::Null) => {
                *values.last_mut().expect("projection row was started") = None;
            }
            (Self::Number(values), BorrowedNativeValue::Null) => {
                *values.last_mut().expect("projection row was started") = None;
            }
            (Self::Boolean(values), BorrowedNativeValue::Null) => {
                *values.last_mut().expect("projection row was started") = None;
            }
            (Self::Timestamptz(values), BorrowedNativeValue::Null) => {
                *values.last_mut().expect("projection row was started") = None;
            }
            (Self::String(values), BorrowedNativeValue::Text(value)) => {
                values.replace_last(Some(value))?;
            }
            (Self::String(values), BorrowedNativeValue::Uuid(value)) => {
                let value = value.to_string();
                values.replace_last(Some(&value))?;
            }
            (Self::Jsonb(values), BorrowedNativeValue::Jsonb(value)) => {
                // The typed-wire reader has already validated canonical JSON
                // text and UTF-8, so Arrow can copy the borrowed string directly.
                values.replace_last(Some(value))?;
            }
            (Self::Integer(values), BorrowedNativeValue::Int8(value)) => {
                *values.last_mut().expect("projection row was started") = Some(value);
            }
            (Self::Number(values), BorrowedNativeValue::Float8(value)) => {
                *values.last_mut().expect("projection row was started") = Some(value);
            }
            (Self::Boolean(values), BorrowedNativeValue::Boolean(value)) => {
                *values.last_mut().expect("projection row was started") = Some(value);
            }
            (Self::Timestamptz(values), BorrowedNativeValue::Timestamptz(value)) => {
                *values.last_mut().expect("projection row was started") = Some(value);
            }
            _ => return Err(mismatch()),
        }
        Ok(())
    }

    fn push_from_native(
        &mut self,
        value: BorrowedNativeValue<'_>,
        field: &RowProjectionField,
        schema_key: &str,
    ) -> Result<(), LixError> {
        let mismatch = || {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "typed row projection value does not match {schema_key}.{}",
                    field.name
                ),
            )
        };
        match (self, value) {
            (Self::String(values) | Self::Jsonb(values), BorrowedNativeValue::Null) => {
                values.push(None)?;
            }
            (Self::Integer(values), BorrowedNativeValue::Null) => values.push(None),
            (Self::Number(values), BorrowedNativeValue::Null) => values.push(None),
            (Self::Boolean(values), BorrowedNativeValue::Null) => values.push(None),
            (Self::Timestamptz(values), BorrowedNativeValue::Null) => values.push(None),
            (Self::String(values), BorrowedNativeValue::Text(value)) => {
                values.push(Some(value))?;
            }
            (Self::String(values), BorrowedNativeValue::Uuid(value)) => {
                let value = value.to_string();
                values.push(Some(&value))?;
            }
            (Self::Jsonb(values), BorrowedNativeValue::Jsonb(value)) => {
                values.push(Some(value))?;
            }
            (Self::Integer(values), BorrowedNativeValue::Int8(value)) => {
                values.push(Some(value));
            }
            (Self::Number(values), BorrowedNativeValue::Float8(value)) => {
                values.push(Some(value));
            }
            (Self::Boolean(values), BorrowedNativeValue::Boolean(value)) => {
                values.push(Some(value));
            }
            (Self::Timestamptz(values), BorrowedNativeValue::Timestamptz(value)) => {
                values.push(Some(value));
            }
            _ => return Err(mismatch()),
        }
        Ok(())
    }

    fn into_array(self) -> ArrayRef {
        match self {
            Self::String(values) | Self::Jsonb(values) => Arc::new(values.into_array()),
            Self::Integer(values) => Arc::new(Int64Array::from(values)),
            Self::Number(values) => Arc::new(Float64Array::from(values)),
            Self::Boolean(values) => Arc::new(BooleanArray::from(values)),
            Self::Timestamptz(values) => {
                Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC"))
            }
        }
    }
}

fn borrowed_key_matches(value: BorrowedNativeValue<'_>, component: &RowPkComponent) -> bool {
    match (value, component) {
        (BorrowedNativeValue::Text(left), RowPkComponent::String(right)) => left == right.as_str(),
        (BorrowedNativeValue::Uuid(left), RowPkComponent::Uuid(right)) => left.as_bytes() == right,
        (BorrowedNativeValue::Int8(left), RowPkComponent::Integer(right)) => left == *right,
        _ => false,
    }
}

/// Compares the row-primary-key suffix retained from an authenticated leaf
/// directly with certified typed fields. The order-preserving key scanner
/// borrows the common no-escape string case and allocates only for the rare
/// escaped-NUL representation; no owned `RowPk` or per-row string handle is
/// constructed.
fn encoded_row_key_matches<'a>(
    encoded: &[u8],
    values: impl ExactSizeIterator<Item = Option<BorrowedNativeValue<'a>>>,
) -> bool {
    use crate::order_preserving_key::{
        KEY_PART_FINAL, KEY_PART_MORE, ROW_PK_CODEC_V1, ROW_PK_INTEGER, ROW_PK_INTEGER_BYTES,
        ROW_PK_STRING, ROW_PK_UUID, ROW_PK_UUID_BYTES, ScannedKeyValue, i64_from_ordered_integer,
        scan_key_part,
    };

    let component_count = values.len();
    if component_count == 0 || encoded.first().copied() != Some(ROW_PK_CODEC_V1) {
        return false;
    }
    let mut offset = 1usize;
    for (ordinal, value) in values.enumerate() {
        let Some(value) = value else {
            return false;
        };
        let Some(&tag) = encoded.get(offset) else {
            return false;
        };
        offset += 1;
        let expected_terminator = if ordinal + 1 == component_count {
            KEY_PART_FINAL
        } else {
            KEY_PART_MORE
        };
        let matches = match (tag, value) {
            (ROW_PK_STRING, BorrowedNativeValue::Text(expected)) => {
                let Ok(scanned) = scan_key_part(encoded, offset) else {
                    return false;
                };
                if scanned.terminator != expected_terminator {
                    return false;
                }
                offset = scanned.end;
                match scanned.value {
                    ScannedKeyValue::Verbatim(range) => {
                        encoded.get(range) == Some(expected.as_bytes())
                    }
                    ScannedKeyValue::Unescaped(value) => value == expected.as_bytes(),
                }
            }
            (ROW_PK_UUID, BorrowedNativeValue::Uuid(expected)) => {
                let end = offset.saturating_add(ROW_PK_UUID_BYTES);
                let Some(bytes) = encoded.get(offset..end) else {
                    return false;
                };
                if encoded.get(end).copied() != Some(expected_terminator) {
                    return false;
                }
                offset = end + 1;
                bytes == expected.as_bytes()
            }
            (ROW_PK_INTEGER, BorrowedNativeValue::Int8(expected)) => {
                let end = offset.saturating_add(ROW_PK_INTEGER_BYTES);
                let Some(bytes) = encoded.get(offset..end) else {
                    return false;
                };
                if encoded.get(end).copied() != Some(expected_terminator) {
                    return false;
                }
                offset = end + 1;
                let ordered = u64::from_be_bytes(bytes.try_into().expect("fixed integer width"));
                i64_from_ordered_integer(ordered) == expected
            }
            _ => false,
        };
        if !matches {
            return false;
        }
    }
    offset == encoded.len()
}

fn certified_kind_matches_data_type(
    kind: CertifiedNativeScalarKind,
    data_type: lix_schema::DataType,
) -> bool {
    matches!(
        (kind, data_type),
        (CertifiedNativeScalarKind::Text, lix_schema::DataType::Text)
            | (CertifiedNativeScalarKind::Uuid, lix_schema::DataType::Uuid)
            | (CertifiedNativeScalarKind::Int8, lix_schema::DataType::Int8)
            | (
                CertifiedNativeScalarKind::Float8,
                lix_schema::DataType::Float8
            )
            | (
                CertifiedNativeScalarKind::Boolean,
                lix_schema::DataType::Boolean
            )
            | (
                CertifiedNativeScalarKind::Jsonb,
                lix_schema::DataType::Jsonb
            )
            | (
                CertifiedNativeScalarKind::Timestamptz,
                lix_schema::DataType::Timestamptz
            )
    )
}

fn borrowed_value_matches_schema(
    value: BorrowedNativeValue<'_>,
    data_type: lix_schema::DataType,
    nullable: bool,
) -> bool {
    match value {
        BorrowedNativeValue::Null => nullable,
        BorrowedNativeValue::Text(_) => data_type == lix_schema::DataType::Text,
        BorrowedNativeValue::Uuid(_) => data_type == lix_schema::DataType::Uuid,
        BorrowedNativeValue::Int8(_) => data_type == lix_schema::DataType::Int8,
        BorrowedNativeValue::Float8(_) => data_type == lix_schema::DataType::Float8,
        BorrowedNativeValue::Boolean(_) => data_type == lix_schema::DataType::Boolean,
        BorrowedNativeValue::Jsonb(_) => data_type == lix_schema::DataType::Jsonb,
        BorrowedNativeValue::Timestamptz(_) => data_type == lix_schema::DataType::Timestamptz,
    }
}

fn native_shape_error(schema_key: &str, reason: &str) -> LixError {
    LixError::new(
        LixError::CODE_SCHEMA_VALIDATION,
        format!("durable typed row for schema '{schema_key}' {reason}"),
    )
}

fn typed_public_value(
    value: Option<&lix_schema::Value>,
    field: &RowProjectionField,
    schema_key: &str,
) -> Result<crate::Value, LixError> {
    let Some(value) = value else {
        return Ok(crate::Value::Null);
    };
    let value = match (field.column_type, value) {
        (_, lix_schema::Value::Null) => crate::Value::Null,
        (SchemaColumnType::String, lix_schema::Value::Text(value)) => {
            crate::Value::Text(value.clone())
        }
        (SchemaColumnType::String, lix_schema::Value::Uuid(value)) => {
            crate::Value::Text(value.to_string())
        }
        (SchemaColumnType::Jsonb, lix_schema::Value::Jsonb(value)) => {
            let json = value.to_json_string().map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "cannot render typed JSONB value for {schema_key}.{}: {error}",
                        field.name
                    ),
                )
            })?;
            crate::Value::Jsonb(crate::Json::from_canonical_text(json))
        }
        (SchemaColumnType::Integer, lix_schema::Value::Int8(value)) => {
            crate::Value::Integer(*value)
        }
        (SchemaColumnType::Number, lix_schema::Value::Float8(value)) => crate::Value::Real(*value),
        (SchemaColumnType::Boolean, lix_schema::Value::Boolean(value)) => {
            crate::Value::Boolean(*value)
        }
        (SchemaColumnType::Timestamptz, lix_schema::Value::Timestamptz(value)) => {
            crate::Value::Timestamptz(*value)
        }
        _ => {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "typed row projection value does not match {}.{}",
                    schema_key, field.name
                ),
            ));
        }
    };
    Ok(value)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use serde_json::json;

    use super::{
        RowProjectionColumn, RowProjectionDecoder, RowProjectionField, encoded_row_key_matches,
    };
    use crate::plugin::wire::typed::BorrowedNativeValue;
    use crate::sql2::catalog::derive_schema_surface_spec_from_schema;
    use crate::sql2::exec::datafusion::query_result_from_batches;
    use crate::sql2::result_metadata::mark_json_field;
    use crate::transaction_types::TransactionJson;
    use crate::{Json, LixError, Value};

    fn canonical_json(canonical: &str) -> Json {
        Json::from_canonical_text(canonical)
    }

    #[test]
    fn encoded_outer_row_key_matches_certified_composite_values() {
        let uuid = uuid::Uuid::from_bytes([7; 16]);
        let row_pk = crate::row_pk::RowPk::from_components(smallvec::smallvec![
            crate::row_pk::RowPkComponent::String("alpha".into()),
            crate::row_pk::RowPkComponent::Uuid(*uuid.as_bytes()),
            crate::row_pk::RowPkComponent::Integer(-17),
        ])
        .expect("composite row key is valid");
        let mut encoded = Vec::new();
        crate::order_preserving_key::write_row_pk(&mut encoded, &row_pk);

        assert!(encoded_row_key_matches(
            &encoded,
            [
                Some(BorrowedNativeValue::Text("alpha")),
                Some(BorrowedNativeValue::Uuid(uuid)),
                Some(BorrowedNativeValue::Int8(-17)),
            ]
            .into_iter(),
        ));
    }

    #[test]
    fn encoded_outer_row_key_rejects_corruption_and_boundary_mismatches() {
        let row_pk = crate::row_pk::RowPk::single("row-a");
        let mut encoded = Vec::new();
        crate::order_preserving_key::write_row_pk(&mut encoded, &row_pk);
        let expected = || [Some(BorrowedNativeValue::Text("row-a"))].into_iter();

        assert!(!encoded_row_key_matches(&encoded[..encoded.len() - 1], expected()));
        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(!encoded_row_key_matches(&trailing, expected()));
        assert!(!encoded_row_key_matches(
            &encoded,
            [Some(BorrowedNativeValue::Text("row-b"))].into_iter(),
        ));
        assert!(!encoded_row_key_matches(
            &encoded,
            [Some(BorrowedNativeValue::Null)].into_iter(),
        ));
        assert!(!encoded_row_key_matches(
            &encoded,
            [
                Some(BorrowedNativeValue::Text("row-a")),
                Some(BorrowedNativeValue::Int8(1)),
            ]
            .into_iter(),
        ));
    }

    #[test]
    fn certified_direct_append_matches_reserved_row_replacement() {
        fn assert_equivalent(column_type: SchemaColumnType, values: &[BorrowedNativeValue<'_>]) {
            let field = RowProjectionField {
                name: "value".to_owned(),
                column_type,
            };
            let mut reserved = RowProjectionColumn::new(column_type, values.len());
            let mut direct = RowProjectionColumn::new(column_type, values.len());
            for &value in values {
                reserved.push_null();
                reserved
                    .replace_last_from_native(value, &field, "projection_test")
                    .expect("reserved projection should accept the native value");
                direct
                    .push_from_native(value, &field, "projection_test")
                    .expect("direct projection should accept the native value");
            }
            assert_eq!(
                reserved.into_array().to_data(),
                direct.into_array().to_data()
            );
        }

        use crate::sql2::catalog::SchemaColumnType;
        assert_equivalent(
            SchemaColumnType::String,
            &[
                BorrowedNativeValue::Text("row-1"),
                BorrowedNativeValue::Null,
            ],
        );
        assert_equivalent(
            SchemaColumnType::Jsonb,
            &[
                BorrowedNativeValue::Jsonb(r#"{"a":[true,null]}"#),
                BorrowedNativeValue::Null,
            ],
        );
        assert_equivalent(
            SchemaColumnType::Integer,
            &[BorrowedNativeValue::Int8(7), BorrowedNativeValue::Null],
        );
        assert_equivalent(
            SchemaColumnType::Number,
            &[BorrowedNativeValue::Float8(4.5), BorrowedNativeValue::Null],
        );
        assert_equivalent(
            SchemaColumnType::Boolean,
            &[
                BorrowedNativeValue::Boolean(true),
                BorrowedNativeValue::Null,
            ],
        );
        assert_equivalent(
            SchemaColumnType::Timestamptz,
            &[
                BorrowedNativeValue::Timestamptz(1_735_689_600_123_456),
                BorrowedNativeValue::Null,
            ],
        );
    }

    #[test]
    fn direct_public_projection_preserves_json_null_and_timestamptz() {
        let mut spec = spec();
        spec.columns
            .push(crate::sql2::catalog::schema_surface::SchemaSurfaceColumn {
                name: "stamp".to_string(),
                native_type: lix_schema::DataType::Timestamptz,
                column_type: crate::sql2::catalog::SchemaColumnType::Timestamptz,
                read_nullable: true,
                insert_required: false,
                default_expression: None,
            });
        let decoder = RowProjectionDecoder::new(&spec, ["json", "null_text", "stamp"])
            .expect("direct decoder should build");
        let values = decoder
            .decode_public_values(Some(
                br#"{"json":{"a":[true,null]},"null_text":null,"stamp":"2025-01-01T00:00:00.123456Z"}"#,
            ))
            .expect("direct values should decode");
        assert_eq!(
            values,
            vec![
                Value::Jsonb(canonical_json(r#"{"a":[true,null]}"#)),
                Value::Null,
                Value::Timestamptz(1_735_689_600_123_456),
            ]
        );
    }

    fn spec() -> crate::sql2::catalog::SchemaSurfaceSpec {
        derive_schema_surface_spec_from_schema(&json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "projection_test",
            "columns": [
                { "name": "text", "type": "text", "nullable": false },
                { "name": "json", "type": "jsonb", "nullable": true },
                { "name": "integer", "type": "int8", "nullable": true },
                { "name": "number", "type": "float8", "nullable": true },
                { "name": "boolean", "type": "boolean", "nullable": true },
                { "name": "coerce_bool", "type": "text", "nullable": true },
                { "name": "coerce_object", "type": "text", "nullable": true },
                { "name": "null_text", "type": "text", "nullable": true },
                { "name": "missing", "type": "text", "nullable": true },
            ],
            "primary_key": ["text"],
        }))
        .expect("test schema should derive")
    }

    #[test]
    fn native_payload_projection_streams_selected_values_and_binds_identity() {
        let spec = spec();
        let mut row = lix_schema::Row::new();
        row.insert("boolean", lix_schema::Value::Boolean(true));
        row.insert("coerce_bool", lix_schema::Value::Text("false".to_owned()));
        row.insert(
            "coerce_object",
            lix_schema::Value::Text(r#"{"a":1}"#.to_owned()),
        );
        row.insert("integer", lix_schema::Value::Int8(7));
        row.insert(
            "json",
            lix_schema::Value::Jsonb(lix_schema::Jsonb::from_value(json!({
                "a": [true, null]
            }))),
        );
        row.insert("missing", lix_schema::Value::Null);
        row.insert("null_text", lix_schema::Value::Null);
        row.insert("number", lix_schema::Value::Float8(4.5));
        row.insert("text", lix_schema::Value::Text("row-1".to_owned()));
        let payload = crate::plugin::wire::typed::encode_native_row_payload(
            &spec.schema_fingerprint,
            &[lix_schema::Value::Text("row-1".to_owned())],
            &row,
        )
        .expect("native payload should encode");
        let decoder = RowProjectionDecoder::new(&spec, ["text", "json", "json", "text"])
            .expect("native projection should build");
        let row_pk = crate::row_pk::RowPk::single("row-1");

        let arrays = decoder
            .decode_durable_payload_arrow_columns([(payload.as_slice(), &row_pk)])
            .expect("native payload should project");
        let validated = crate::plugin::wire::typed::ValidatedNativePayload::try_new(
            bytes::Bytes::from(payload.clone()),
        )
        .expect("native payload validates once");
        let validated_arrays = decoder
            .decode_validated_native_payload_arrow_columns([(&validated, &row_pk)])
            .expect("validated native payload should project");
        assert_eq!(
            arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("text array")
                .value(0),
            "row-1"
        );
        assert_eq!(
            arrays[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("JSON array")
                .value(0),
            r#"{"a":[true,null]}"#
        );
        assert_eq!(
            arrays[2]
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("duplicate JSON array")
                .value(0),
            r#"{"a":[true,null]}"#
        );
        assert_eq!(
            arrays[3]
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("duplicate text array")
                .value(0),
            "row-1"
        );
        for (raw, validated) in arrays.iter().zip(&validated_arrays) {
            assert_eq!(raw.to_data(), validated.to_data());
        }

        let wrong_pk = crate::row_pk::RowPk::single("other");
        let error = decoder
            .decode_durable_payload_arrow_columns([(payload.as_slice(), &wrong_pk)])
            .expect_err("payload identity must match its storage envelope");
        assert!(error.message.contains("identity does not match"));

        let error = decoder
            .decode_validated_native_payload_arrow_columns([(&validated, &wrong_pk)])
            .expect_err("proof does not replace storage-envelope validation");
        assert!(error.message.contains("identity does not match"));

        let wrong_fingerprint_payload = crate::plugin::wire::typed::encode_native_row_payload(
            &[9; 32],
            &[lix_schema::Value::Text("row-1".to_owned())],
            &row,
        )
        .expect("native payload should encode");
        let wrong_fingerprint =
            crate::plugin::wire::typed::ValidatedNativePayload::try_new(bytes::Bytes::from(
                wrong_fingerprint_payload,
            ))
            .expect("wire validation does not bind a resolved schema");
        let error = decoder
            .decode_validated_native_payload_arrow_columns([(&wrong_fingerprint, &row_pk)])
            .expect_err("proof does not replace fingerprint validation");
        assert!(error.message.contains("fingerprint"));

        let mut incomplete_row = row.clone();
        incomplete_row.remove("text");
        let incomplete_payload = crate::plugin::wire::typed::encode_native_row_payload(
            &spec.schema_fingerprint,
            &[lix_schema::Value::Text("row-1".to_owned())],
            &incomplete_row,
        )
        .expect("wire encoder accepts rows before resolved-schema validation");
        let incomplete = crate::plugin::wire::typed::ValidatedNativePayload::try_new(
            bytes::Bytes::from(incomplete_payload),
        )
        .expect("incomplete row remains a valid native wire");
        let error = decoder
            .decode_validated_native_payload_arrow_columns([(&incomplete, &row_pk)])
            .expect_err("native projection must retain complete-row validation");
        assert!(error.message.contains("missing a declared column"));

        let mut wrong_type_row = row;
        wrong_type_row.insert("json", lix_schema::Value::Text("wrong".to_owned()));
        let wrong_type_payload = crate::plugin::wire::typed::encode_native_row_payload(
            &spec.schema_fingerprint,
            &[lix_schema::Value::Text("row-1".to_owned())],
            &wrong_type_row,
        )
        .expect("wrong schema type remains a valid native wire");
        let wrong_type = crate::plugin::wire::typed::ValidatedNativePayload::try_new(
            bytes::Bytes::from(wrong_type_payload),
        )
        .expect("wire validation is independent from resolved schema types");
        let error = decoder
            .decode_validated_native_payload_arrow_columns([(&wrong_type, &row_pk)])
            .expect_err("proof does not replace native type validation");
        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
    }

    #[test]
    #[expect(clippy::float_cmp)]
    fn decodes_selected_fields_from_canonical_tracked_arrow_projection() {
        let spec = spec();
        let decoder = RowProjectionDecoder::new(
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
        let decoder = RowProjectionDecoder::new(
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
        let arrows = query_result_from_batches(&fields, &[batch])
            .expect("Arrow result values should decode")
            .rows;

        // JSON results carry the stored bytes verbatim. Canonical member order
        // is owned by `canonicalize_transaction_json_batch` at the write
        // boundary, so this hand-built raw snapshot keeps its source order
        // rather than being re-sorted by a decode-side DOM round trip.
        assert_eq!(
            arrows[0],
            vec![
                Value::Text("line\nquote: \"".to_string()),
                Value::Jsonb(canonical_json(r#"{"z":[true,null],"a":"value"}"#)),
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
        assert_eq!(arrows[1], vec![Value::Null; 10]);
        assert_eq!(arrows[2], vec![Value::Null; 10]);
    }

    #[test]
    fn reports_the_existing_typed_number_contract_error() {
        let spec = spec();
        let decoder =
            RowProjectionDecoder::new(&spec, ["integer", "number"]).expect("decoder should build");
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
