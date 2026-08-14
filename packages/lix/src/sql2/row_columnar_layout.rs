//! Registered-schema-bound columnar sidecars for immutable row generations.
//!
//! The transaction boundary calls this adapter while validated canonical
//! snapshots and typed row identities are still available. SQL's existing
//! projection decoder remains the single value-conversion contract; this
//! module only chooses physical row groups and delegates their encoding.

use std::collections::{BTreeMap, HashMap};
use std::ops::Deref;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::LixError;
use crate::columnar_row_group::{
    EncodedRowGroupSet, ROW_GROUP_MAX_ROWS, RowGroupRowLocation,
    encode_row_group_set_preserving_batches,
};
use crate::row_pk::{RowPk, RowPkComponent, RowPkComponentType};
use crate::sql2::{RowProjectionDecoder, SchemaColumnType, SchemaSurfaceSpec, row_visible_fields};

pub(crate) const ROW_COLUMNAR_LAYOUT_FINGERPRINT_METADATA_KEY: &str =
    "lix.row_columnar.layout_fingerprint.v1";
pub(crate) const ROW_COLUMNAR_BASE_COORDINATES_METADATA_KEY: &str =
    "lix.row_columnar.base_coordinates.v1";
pub(crate) const ROW_COLUMNAR_AUTHORITATIVE_SINGLETON_METADATA_KEY: &str =
    "lix.row_columnar.authoritative_singleton_key_bound.v1";
const AUTHORITATIVE_SINGLETON_LAYOUT: &str = "authoritative-singleton-key-bound-v1";
pub(crate) const ROW_COLUMNAR_SCHEMA_V1_TYPE_METADATA_KEY: &str = "lix.schema_v1_type";
pub(crate) use crate::hot_state::{
    ROW_COLUMNAR_LOSSLESS_SNAPSHOT_METADATA_KEY, ROW_COLUMNAR_ROW_PK_FIELD,
};
pub(crate) const LOW_CARDINALITY_CLUSTER_MAX_VALUES: usize = 64;
const LOW_CARDINALITY_CLUSTER_MAX_BUCKETS: usize = 8;
const ROW_COLUMNAR_MAX_CLUSTER_PARTITIONS: usize = 64;

enum ClusterField<'a> {
    Boolean(&'a str),
    String(&'a str, BTreeMap<String, u8>),
}

#[derive(Clone, Copy)]
pub(crate) struct RowColumnarRowRef<'a> {
    pub(crate) row_pk: &'a RowPk,
    pub(crate) snapshot_bytes: &'a [u8],
    pub(crate) snapshot_value: &'a JsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct SingletonLayoutDescriptor {
    layout: String,
    schema_fingerprint: String,
    field_count: u32,
    primary_key: Vec<SingletonPrimaryKeyColumn>,
    absent_non_primary_key: Vec<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct SingletonPrimaryKeyColumn {
    column_index: u32,
    name: String,
    component_type: SingletonPrimaryKeyType,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum SingletonPrimaryKeyType {
    Uuid,
    Integer,
    String,
    Bytes,
}

impl From<RowPkComponentType> for SingletonPrimaryKeyType {
    fn from(value: RowPkComponentType) -> Self {
        match value {
            RowPkComponentType::Uuid => Self::Uuid,
            RowPkComponentType::Integer => Self::Integer,
            RowPkComponentType::String => Self::String,
            RowPkComponentType::Bytes => Self::Bytes,
        }
    }
}

#[derive(Debug, Clone)]
enum SingletonOrderedField {
    PrimaryKey {
        name: String,
        component_index: usize,
        component_type: SingletonPrimaryKeyType,
    },
    Physical {
        name: String,
        column_index: usize,
        absent: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AuthoritativeSingletonFieldSource {
    PrimaryKey { name: String, component_index: usize },
    Cell { name: String, column_index: usize },
}

/// Validated, catalog-independent interpretation of an authoritative
/// singleton manifest.
#[derive(Debug, Clone)]
pub(crate) struct AuthoritativeSingletonLayout {
    pub(crate) schema_fingerprint: String,
    physical_fields: Vec<Field>,
    ordered_fields: Vec<SingletonOrderedField>,
}

impl AuthoritativeSingletonLayout {
    pub(crate) fn native_field_sources(&self) -> Vec<AuthoritativeSingletonFieldSource> {
        self.ordered_fields
            .iter()
            .filter_map(|field| match field {
                SingletonOrderedField::PrimaryKey {
                    name,
                    component_index,
                    ..
                } => Some(AuthoritativeSingletonFieldSource::PrimaryKey {
                    name: name.clone(),
                    component_index: *component_index,
                }),
                SingletonOrderedField::Physical {
                    name,
                    column_index,
                    absent,
                } => (!*absent).then(|| AuthoritativeSingletonFieldSource::Cell {
                    name: name.clone(),
                    column_index: *column_index,
                }),
            })
            .collect()
    }

    /// Reconstructs declared fields in schema order. Missing nullable fields
    /// remain absent; explicit nulls are emitted as JSON null.
    pub(crate) fn reconstruct_full_ordered_field_map(
        &self,
        row_pk: &RowPk,
        batch: &RecordBatch,
    ) -> Result<Vec<(String, JsonValue)>, LixError> {
        if batch.num_rows() != 1 || batch.num_columns() != self.physical_fields.len() {
            return Err(row_columnar_error(
                "authoritative singleton reconstruction requires exactly one row and the declared physical columns",
            ));
        }
        for (actual, expected) in batch.schema().fields().iter().zip(&self.physical_fields) {
            if actual.as_ref() != expected {
                return Err(row_columnar_error(
                    "authoritative singleton Arrow fields do not match validated metadata",
                ));
            }
        }
        let components = row_pk.components.as_slice();
        let primary_key_count = self
            .ordered_fields
            .iter()
            .filter(|field| matches!(field, SingletonOrderedField::PrimaryKey { .. }))
            .count();
        if components.len() != primary_key_count {
            return Err(row_columnar_error(
                "authoritative singleton key component count does not match metadata",
            ));
        }

        let mut fields = Vec::with_capacity(self.ordered_fields.len());
        for field in &self.ordered_fields {
            match field {
                SingletonOrderedField::PrimaryKey {
                    name,
                    component_index,
                    component_type,
                } => {
                    let component = &components[*component_index];
                    if !singleton_key_component_matches(component, *component_type) {
                        return Err(row_columnar_error(format!(
                            "authoritative singleton key component {component_index} has the wrong type"
                        )));
                    }
                    fields.push((name.clone(), component.external_json()));
                }
                SingletonOrderedField::Physical {
                    name,
                    column_index,
                    absent,
                } => {
                    if !absent {
                        fields.push((
                            name.clone(),
                            singleton_arrow_value(
                                &self.physical_fields[*column_index],
                                batch.column(*column_index),
                            )?,
                        ));
                    }
                }
            }
        }
        Ok(fields)
    }
}

fn singleton_key_component_matches(
    component: &RowPkComponent,
    component_type: SingletonPrimaryKeyType,
) -> bool {
    matches!(
        (component, component_type),
        (RowPkComponent::Uuid(_), SingletonPrimaryKeyType::Uuid)
            | (RowPkComponent::Integer(_), SingletonPrimaryKeyType::Integer)
            | (RowPkComponent::String(_), SingletonPrimaryKeyType::String)
            | (RowPkComponent::Bytes(_), SingletonPrimaryKeyType::Bytes)
    )
}

fn singleton_arrow_value(field: &Field, array: &ArrayRef) -> Result<JsonValue, LixError> {
    if array.len() != 1 {
        return Err(row_columnar_error(
            "authoritative singleton column does not contain exactly one value",
        ));
    }
    if array.is_null(0) {
        return Ok(JsonValue::Null);
    }
    match field.data_type() {
        DataType::Utf8 => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| row_columnar_error("singleton UTF-8 field has a non-UTF-8 array"))?;
            if crate::sql2::result_metadata::field_is_json(field) {
                serde_json::from_str(values.value(0)).map_err(|error| {
                    row_columnar_error(format!(
                        "singleton JSON field '{}' is invalid: {error}",
                        field.name()
                    ))
                })
            } else {
                Ok(JsonValue::String(values.value(0).to_owned()))
            }
        }
        DataType::Int64 => {
            let values = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| row_columnar_error("singleton int64 field has the wrong array"))?;
            Ok(JsonValue::from(values.value(0)))
        }
        DataType::Float64 => {
            let values = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| row_columnar_error("singleton float64 field has the wrong array"))?;
            serde_json::Number::from_f64(values.value(0))
                .map(JsonValue::Number)
                .ok_or_else(|| row_columnar_error("singleton float is not representable in JSON"))
        }
        DataType::Boolean => {
            let values = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| row_columnar_error("singleton boolean field has the wrong array"))?;
            Ok(JsonValue::from(values.value(0)))
        }
        other => Err(row_columnar_error(format!(
            "unsupported authoritative singleton Arrow type {other}"
        ))),
    }
}

/// Identifies and validates the canonical singleton metadata carried by an
/// Arrow schema. Absence means this is another row-columnar layout; malformed
/// marked metadata is an error.
pub(crate) fn identify_authoritative_singleton_layout(
    schema: &Schema,
) -> Result<Option<AuthoritativeSingletonLayout>, LixError> {
    let Some(encoded) = schema
        .metadata()
        .get(ROW_COLUMNAR_AUTHORITATIVE_SINGLETON_METADATA_KEY)
    else {
        return Ok(None);
    };
    let descriptor: SingletonLayoutDescriptor = serde_json::from_str(encoded)
        .map_err(|error| row_columnar_error(format!("invalid singleton metadata: {error}")))?;
    let canonical = serde_json::to_string(&descriptor).map_err(|error| {
        row_columnar_error(format!("cannot canonicalize singleton metadata: {error}"))
    })?;
    if &canonical != encoded {
        return Err(row_columnar_error(
            "authoritative singleton metadata is not canonically encoded",
        ));
    }
    if descriptor.layout != AUTHORITATIVE_SINGLETON_LAYOUT {
        return Err(row_columnar_error(
            "authoritative singleton metadata has an unknown layout marker",
        ));
    }
    if !canonical_fingerprint(&descriptor.schema_fingerprint)
        || schema
            .metadata()
            .get(ROW_COLUMNAR_LAYOUT_FINGERPRINT_METADATA_KEY)
            != Some(&descriptor.schema_fingerprint)
    {
        return Err(row_columnar_error(
            "authoritative singleton metadata is not bound to its schema fingerprint",
        ));
    }
    if schema
        .metadata()
        .get(ROW_COLUMNAR_LOSSLESS_SNAPSHOT_METADATA_KEY)
        .map(String::as_str)
        != Some("true")
    {
        return Err(row_columnar_error(
            "authoritative singleton metadata is missing its lossless marker",
        ));
    }
    let field_count = usize::try_from(descriptor.field_count)
        .map_err(|_| row_columnar_error("singleton field count does not fit usize"))?;
    if field_count != schema.fields().len() + descriptor.primary_key.len() {
        return Err(row_columnar_error(
            "authoritative singleton field count does not match its schema",
        ));
    }

    let mut ordered_fields = vec![None; field_count];
    let mut names = std::collections::BTreeSet::new();
    for (component_index, primary_key) in descriptor.primary_key.iter().enumerate() {
        let column_index = usize::try_from(primary_key.column_index)
            .map_err(|_| row_columnar_error("singleton PK position does not fit usize"))?;
        if column_index >= field_count
            || ordered_fields[column_index].is_some()
            || !names.insert(primary_key.name.clone())
        {
            return Err(row_columnar_error(
                "authoritative singleton metadata has duplicate or invalid PK fields",
            ));
        }
        ordered_fields[column_index] = Some(SingletonOrderedField::PrimaryKey {
            name: primary_key.name.clone(),
            component_index,
            component_type: primary_key.component_type,
        });
    }
    let absent = descriptor
        .absent_non_primary_key
        .iter()
        .map(|index| usize::try_from(*index))
        .collect::<Result<std::collections::BTreeSet<_>, _>>()
        .map_err(|_| row_columnar_error("singleton absent position does not fit usize"))?;
    if absent.len() != descriptor.absent_non_primary_key.len()
        || absent.iter().any(|index| *index >= field_count)
    {
        return Err(row_columnar_error(
            "authoritative singleton metadata has duplicate or invalid absent fields",
        ));
    }
    let mut physical_index = 0;
    for (field_index, slot) in ordered_fields.iter_mut().enumerate() {
        if slot.is_some() {
            if absent.contains(&field_index) {
                return Err(row_columnar_error(
                    "authoritative singleton PK field cannot be absent",
                ));
            }
            continue;
        }
        let field = schema.fields().get(physical_index).ok_or_else(|| {
            row_columnar_error("authoritative singleton metadata omits a physical field")
        })?;
        if field.name() == ROW_COLUMNAR_ROW_PK_FIELD || !names.insert(field.name().clone()) {
            return Err(row_columnar_error(
                "authoritative singleton physical fields contain identity or duplicate names",
            ));
        }
        *slot = Some(SingletonOrderedField::Physical {
            name: field.name().clone(),
            column_index: physical_index,
            absent: absent.contains(&field_index),
        });
        physical_index += 1;
    }
    if physical_index != schema.fields().len() {
        return Err(row_columnar_error(
            "authoritative singleton metadata leaves extra physical fields",
        ));
    }

    Ok(Some(AuthoritativeSingletonLayout {
        schema_fingerprint: descriptor.schema_fingerprint,
        physical_fields: schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect(),
        ordered_fields: ordered_fields
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .expect("every singleton field position was filled"),
    }))
}

fn canonical_fingerprint(fingerprint: &str) -> bool {
    fingerprint.len() == 64
        && fingerprint
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[derive(Clone, Debug)]
pub(crate) struct EncodedRowGroups {
    encoded: EncodedRowGroupSet,
    pub(crate) input_locations: RowGroupLocations,
}

/// Input-row to physical-row mapping for one sealed row generation.
///
/// Identity-preserving batches use arithmetic coordinates and retain no
/// row-cardinal location column. Clustered layouts keep the explicit
/// permutation required to map their reordered rows back to statement order.
#[derive(Clone, Debug)]
pub(crate) enum RowGroupLocations {
    Dense { row_count: usize },
    Explicit(Vec<RowGroupRowLocation>),
}

impl RowGroupLocations {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Dense { row_count } => *row_count,
            Self::Explicit(locations) => locations.len(),
        }
    }

    pub(crate) fn location(&self, input_index: usize) -> Option<RowGroupRowLocation> {
        match self {
            Self::Dense { row_count } if input_index < *row_count => Some(RowGroupRowLocation {
                group_index: u32::try_from(input_index / ROW_GROUP_MAX_ROWS).ok()?,
                row_index: u32::try_from(input_index % ROW_GROUP_MAX_ROWS).ok()?,
            }),
            Self::Dense { .. } => None,
            Self::Explicit(locations) => locations.get(input_index).copied(),
        }
    }

    pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = RowGroupRowLocation> + '_ {
        (0..self.len()).map(|input_index| {
            self.location(input_index)
                .expect("row-group location covers every input row")
        })
    }
}

impl PartialEq for RowGroupLocations {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().eq(other.iter())
    }
}

impl Eq for RowGroupLocations {}

impl Deref for EncodedRowGroups {
    type Target = EncodedRowGroupSet;

    fn deref(&self) -> &Self::Target {
        &self.encoded
    }
}

impl EncodedRowGroups {
    pub(crate) fn into_parts(self) -> (EncodedRowGroupSet, RowGroupLocations) {
        (self.encoded, self.input_locations)
    }
}

/// Encodes one authoritative key-bound row group. Primary-key values remain
/// solely in the surrounding authenticated key; the payload contains only
/// non-PK Schema-v1 columns.
pub(crate) fn encode_authoritative_singleton_row_group(
    spec: &SchemaSurfaceSpec,
    row: RowColumnarRowRef<'_>,
) -> Result<EncodedRowGroups, LixError> {
    let (schema, batch) = authoritative_singleton_batch(spec, row)?;
    let encoded =
        encode_row_group_set_preserving_batches(&spec.schema_key, Arc::clone(&schema), &[batch])?;
    Ok(EncodedRowGroups {
        encoded,
        input_locations: RowGroupLocations::Dense { row_count: 1 },
    })
}

fn authoritative_singleton_batch(
    spec: &SchemaSurfaceSpec,
    row: RowColumnarRowRef<'_>,
) -> Result<(Arc<Schema>, RecordBatch), LixError> {
    let primary_key = spec.top_level_primary_key_columns().ok_or_else(|| {
        row_columnar_error("authoritative singleton requires a complete top-level primary key")
    })?;
    let snapshot_pk = RowPk::from_primary_key_plan(
        row.snapshot_value,
        &spec.primary_key_paths,
        &spec.primary_key_component_types,
    )
    .map_err(|error| row_columnar_error(format!("snapshot primary key is invalid: {error}")))?;
    if &snapshot_pk != row.row_pk {
        return Err(row_columnar_error(
            "supplied row key does not match the snapshot primary key",
        ));
    }
    let snapshot = row
        .snapshot_value
        .as_object()
        .ok_or_else(|| row_columnar_error("authoritative singleton snapshot is not an object"))?;
    let primary_key_indices = primary_key
        .iter()
        .map(|column| column.column_index)
        .collect::<std::collections::BTreeSet<_>>();
    let visible_fields = row_visible_fields(spec);
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    let mut absent_non_primary_key = Vec::new();
    for (column_index, (column, visible_field)) in
        spec.columns.iter().zip(visible_fields).enumerate()
    {
        if primary_key_indices.contains(&column_index) {
            continue;
        }
        let value = snapshot.get(&column.name);
        if value.is_none() {
            absent_non_primary_key.push(
                u32::try_from(column_index)
                    .map_err(|_| row_columnar_error("schema column index exceeds u32"))?,
            );
        }
        let field = if column.column_type == SchemaColumnType::Timestamptz {
            Field::new(&column.name, DataType::Utf8, column.read_nullable)
        } else {
            visible_field
        };
        let mut field_metadata = field.metadata().clone();
        field_metadata.insert(
            ROW_COLUMNAR_SCHEMA_V1_TYPE_METADATA_KEY.to_owned(),
            match column.column_type {
                SchemaColumnType::String => "text",
                SchemaColumnType::Json => "jsonb",
                SchemaColumnType::Integer => "int8",
                SchemaColumnType::Number => "float8",
                SchemaColumnType::Boolean => "boolean",
                SchemaColumnType::Timestamptz => "timestamptz",
            }
            .to_owned(),
        );
        let field = field.with_metadata(field_metadata);
        columns.push(authoritative_singleton_column(
            &column.name,
            column.column_type,
            value,
        )?);
        fields.push(field);
    }
    let descriptor = SingletonLayoutDescriptor {
        layout: AUTHORITATIVE_SINGLETON_LAYOUT.to_owned(),
        schema_fingerprint: spec.columnar_layout_fingerprint(),
        field_count: u32::try_from(spec.columns.len())
            .map_err(|_| row_columnar_error("schema field count exceeds u32"))?,
        primary_key: primary_key
            .into_iter()
            .map(|column| {
                Ok(SingletonPrimaryKeyColumn {
                    column_index: u32::try_from(column.column_index)
                        .map_err(|_| row_columnar_error("schema PK column index exceeds u32"))?,
                    name: column.name,
                    component_type: column.component_type.into(),
                })
            })
            .collect::<Result<Vec<_>, LixError>>()?,
        absent_non_primary_key,
    };
    let descriptor = serde_json::to_string(&descriptor).map_err(|error| {
        row_columnar_error(format!("cannot encode singleton layout metadata: {error}"))
    })?;
    let fingerprint = spec.columnar_layout_fingerprint();
    let metadata = HashMap::from([
        (
            ROW_COLUMNAR_AUTHORITATIVE_SINGLETON_METADATA_KEY.to_owned(),
            descriptor,
        ),
        (
            ROW_COLUMNAR_LAYOUT_FINGERPRINT_METADATA_KEY.to_owned(),
            fingerprint,
        ),
        (
            ROW_COLUMNAR_LOSSLESS_SNAPSHOT_METADATA_KEY.to_owned(),
            "true".to_owned(),
        ),
    ]);
    let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
    let options = RecordBatchOptions::new().with_row_count(Some(1));
    let batch = RecordBatch::try_new_with_options(Arc::clone(&schema), columns, &options)
        .map_err(|error| row_columnar_error(error.to_string()))?;
    Ok((schema, batch))
}

fn authoritative_singleton_column(
    column_name: &str,
    column_type: SchemaColumnType,
    value: Option<&JsonValue>,
) -> Result<ArrayRef, LixError> {
    let type_error = || {
        row_columnar_error(format!(
            "column '{}' does not match its declared Schema-v1 type",
            column_name
        ))
    };
    match column_type {
        SchemaColumnType::String | SchemaColumnType::Timestamptz => {
            let value = match value {
                None | Some(JsonValue::Null) => None,
                Some(JsonValue::String(value)) => Some(value.as_str()),
                Some(_) => return Err(type_error()),
            };
            Ok(Arc::new(StringArray::from(vec![value])))
        }
        SchemaColumnType::Json => {
            let value = value
                .map(serde_json::to_string)
                .transpose()
                .map_err(|error| row_columnar_error(error.to_string()))?;
            Ok(Arc::new(StringArray::from(vec![value])))
        }
        SchemaColumnType::Integer => {
            let value = match value {
                None | Some(JsonValue::Null) => None,
                Some(JsonValue::Number(value)) => {
                    value.as_i64().ok_or_else(type_error).map(Some)?
                }
                Some(_) => return Err(type_error()),
            };
            Ok(Arc::new(Int64Array::from(vec![value])))
        }
        SchemaColumnType::Number => {
            let value = match value {
                None | Some(JsonValue::Null) => None,
                Some(JsonValue::Number(value)) => {
                    value.as_f64().ok_or_else(type_error).map(Some)?
                }
                Some(_) => return Err(type_error()),
            };
            Ok(Arc::new(Float64Array::from(vec![value])))
        }
        SchemaColumnType::Boolean => {
            let value = match value {
                None | Some(JsonValue::Null) => None,
                Some(JsonValue::Bool(value)) => Some(*value),
                Some(_) => return Err(type_error()),
            };
            Ok(Arc::new(BooleanArray::from(vec![value])))
        }
    }
}

pub(crate) fn encode_registered_row_groups<'a, I>(
    spec: &SchemaSurfaceSpec,
    rows: I,
) -> Result<Option<EncodedRowGroups>, LixError>
where
    I: ExactSizeIterator<Item = RowColumnarRowRef<'a>>,
{
    if rows.len() == 0 {
        return Ok(None);
    }
    // This is a derived acceleration structure. Projection or physical-limit
    // failures must retain the authoritative row layout rather than reject an
    // otherwise-valid transaction.
    Ok(optional_derived_row_group_set(
        encode_registered_row_groups_impl(spec, rows),
    ))
}

/// Encodes frontend-owned Arrow columns without reconstructing them from
/// canonical snapshot JSON. The fast contract is deliberately limited to
/// layouts whose established encoder would not reorder rows for clustering;
/// clustered layouts retain the general encoder and identical physical
/// behavior.
pub(crate) fn encode_unclustered_registered_row_groups(
    spec: &SchemaSurfaceSpec,
    mut columns: Vec<ArrayRef>,
    row_pks: ArrayRef,
) -> Result<Option<EncodedRowGroups>, LixError> {
    if columns.len() != spec.columns.len() {
        return Err(row_columnar_error(
            "frontend column count does not match the registered schema",
        ));
    }
    let row_count = row_pks.len();
    if row_count == 0 || columns.iter().any(|column| column.len() != row_count) {
        return Err(row_columnar_error(
            "frontend columns are empty or have inconsistent row counts",
        ));
    }
    let primary_key_roots = spec
        .primary_key_paths
        .iter()
        .filter_map(|path| path.first().map(String::as_str))
        .collect::<std::collections::BTreeSet<_>>();
    for (spec_column, array) in spec.columns.iter().zip(&columns) {
        if spec_column.column_type == SchemaColumnType::Boolean {
            return Ok(None);
        }
        if spec_column.column_type != SchemaColumnType::String
            || primary_key_roots.contains(spec_column.name.as_str())
        {
            continue;
        }
        let Some(strings) = array.as_any().downcast_ref::<StringArray>() else {
            return Ok(None);
        };
        let mut values = std::collections::BTreeSet::new();
        for value in strings.iter().flatten() {
            values.insert(value);
            if values.len() > LOW_CARDINALITY_CLUSTER_MAX_VALUES {
                break;
            }
        }
        if (2..=LOW_CARDINALITY_CLUSTER_MAX_VALUES).contains(&values.len()) {
            return Ok(None);
        }
    }

    let mut fields = row_visible_fields(spec);
    fields.push(Field::new(ROW_COLUMNAR_ROW_PK_FIELD, DataType::Utf8, false));
    let metadata = row_columnar_metadata(spec);
    let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
    columns.push(row_pks);
    let mut batches = Vec::with_capacity(row_count.div_ceil(ROW_GROUP_MAX_ROWS));
    for offset in (0..row_count).step_by(ROW_GROUP_MAX_ROWS) {
        let len = (row_count - offset).min(ROW_GROUP_MAX_ROWS);
        batches.push(
            RecordBatch::try_new(
                Arc::clone(&schema),
                columns
                    .iter()
                    .map(|column| column.slice(offset, len))
                    .collect(),
            )
            .map_err(|error| row_columnar_error(error.to_string()))?,
        );
    }
    let encoded = encode_row_group_set_preserving_batches(&spec.schema_key, schema, &batches)?;
    Ok(Some(EncodedRowGroups {
        encoded,
        input_locations: RowGroupLocations::Dense { row_count },
    }))
}

fn encode_registered_row_groups_impl<'a, I>(
    spec: &SchemaSurfaceSpec,
    rows: I,
) -> Result<EncodedRowGroups, LixError>
where
    I: ExactSizeIterator<Item = RowColumnarRowRef<'a>>,
{
    let mut fields = row_visible_fields(spec);
    fields.push(Field::new(ROW_COLUMNAR_ROW_PK_FIELD, DataType::Utf8, false));
    let metadata = row_columnar_metadata(spec);
    let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
    let decoder =
        RowProjectionDecoder::new(spec, spec.columns.iter().map(|column| column.name.as_str()))?;

    let rows = rows.enumerate().collect::<Vec<_>>();
    let primary_key_roots = spec
        .primary_key_paths
        .iter()
        .filter_map(|path| path.first().map(String::as_str))
        .collect::<std::collections::BTreeSet<_>>();
    let mut cluster_fields = Vec::new();
    let mut partition_budget = 1_usize;
    for column in &spec.columns {
        if column.column_type == SchemaColumnType::Boolean
            && partition_budget.saturating_mul(3) <= ROW_COLUMNAR_MAX_CLUSTER_PARTITIONS
        {
            cluster_fields.push(ClusterField::Boolean(column.name.as_str()));
            partition_budget *= 3;
        }
    }
    for column in &spec.columns {
        if column.column_type != SchemaColumnType::String
            || primary_key_roots.contains(column.name.as_str())
        {
            continue;
        }
        let mut values = std::collections::BTreeSet::new();
        for (_, row) in &rows {
            if let Some(value) = row
                .snapshot_value
                .get(&column.name)
                .and_then(JsonValue::as_str)
            {
                values.insert(value.to_owned());
                if values.len() > LOW_CARDINALITY_CLUSTER_MAX_VALUES {
                    break;
                }
            }
        }
        if (2..=LOW_CARDINALITY_CLUSTER_MAX_VALUES).contains(&values.len()) {
            let value_count = values.len();
            let bucket_count = value_count.min(LOW_CARDINALITY_CLUSTER_MAX_BUCKETS);
            // Reserve one state for null, missing, or non-string values. Even
            // when none are present in this generation, charging the full
            // key domain keeps the global budget independent of row shape.
            let partition_count = bucket_count + 1;
            if partition_budget.saturating_mul(partition_count)
                > ROW_COLUMNAR_MAX_CLUSTER_PARTITIONS
            {
                continue;
            }
            partition_budget *= partition_count;
            cluster_fields.push(ClusterField::String(
                column.name.as_str(),
                values
                    .into_iter()
                    .enumerate()
                    .map(|(index, value)| {
                        let bucket = index.saturating_mul(bucket_count) / value_count;
                        (value, bucket as u8)
                    })
                    .collect(),
            ));
        }
    }
    let partitions = if cluster_fields.is_empty() {
        vec![rows]
    } else {
        let mut partitions = BTreeMap::<Vec<u8>, Vec<(usize, RowColumnarRowRef<'_>)>>::new();
        for (input_index, row) in rows {
            let key = cluster_fields
                .iter()
                .map(|field| match field {
                    ClusterField::Boolean(name) => {
                        match row.snapshot_value.get(*name).and_then(JsonValue::as_bool) {
                            Some(false) => 0,
                            Some(true) => 1,
                            None => 2,
                        }
                    }
                    ClusterField::String(name, dictionary) => row
                        .snapshot_value
                        .get(*name)
                        .and_then(JsonValue::as_str)
                        .and_then(|value| dictionary.get(value).copied())
                        .unwrap_or(u8::MAX),
                })
                .collect();
            partitions.entry(key).or_default().push((input_index, row));
        }
        partitions.into_values().collect()
    };

    let input_count = partitions.iter().map(Vec::len).sum();
    let mut input_locations = vec![None; input_count];
    let mut batches = Vec::new();
    for partition in partitions {
        for rows in partition.chunks(ROW_GROUP_MAX_ROWS) {
            let group_index = u32::try_from(batches.len())
                .map_err(|_| row_columnar_error("row-group index exceeds u32"))?;
            for (row_index, (input_index, _)) in rows.iter().enumerate() {
                input_locations[*input_index] = Some(RowGroupRowLocation {
                    group_index,
                    row_index: u32::try_from(row_index)
                        .map_err(|_| row_columnar_error("row index exceeds u32"))?,
                });
            }
            let mut columns = decoder
                .decode_arrow_columns(rows.iter().map(|(_, row)| Some(row.snapshot_bytes)))?;
            let row_pks = rows
                .iter()
                .map(|(_, row)| row.row_pk.as_json_array_text())
                .collect::<Result<Vec<_>, _>>()?;
            let row_pks: ArrayRef = Arc::new(StringArray::from(row_pks));
            columns.push(row_pks);
            batches.push(
                RecordBatch::try_new(Arc::clone(&schema), columns)
                    .map_err(|error| row_columnar_error(error.to_string()))?,
            );
        }
    }
    let encoded = encode_row_group_set_preserving_batches(&spec.schema_key, schema, &batches)?;
    Ok(EncodedRowGroups {
        encoded,
        input_locations: RowGroupLocations::Explicit(
            input_locations
                .into_iter()
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| row_columnar_error("row-group permutation omitted an input row"))?,
        ),
    })
}

fn optional_derived_row_group_set(
    encoded: Result<EncodedRowGroups, LixError>,
) -> Option<EncodedRowGroups> {
    encoded.ok()
}

fn row_columnar_metadata(spec: &SchemaSurfaceSpec) -> HashMap<String, String> {
    let mut metadata = HashMap::from([
        (
            ROW_COLUMNAR_LAYOUT_FINGERPRINT_METADATA_KEY.to_string(),
            spec.columnar_layout_fingerprint(),
        ),
        (
            ROW_COLUMNAR_BASE_COORDINATES_METADATA_KEY.to_string(),
            "true".to_owned(),
        ),
    ]);
    if spec.columnar_snapshot_bijective {
        metadata.insert(
            ROW_COLUMNAR_LOSSLESS_SNAPSHOT_METADATA_KEY.to_string(),
            "true".to_owned(),
        );
    }
    metadata
}

fn row_columnar_error(message: impl Into<String>) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("row columnar layout: {}", message.into()),
    )
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::columnar_row_group::RowGroupScalar;
    use crate::row_pk::RowPk;
    use crate::sql2::derive_schema_surface_spec_from_schema;

    fn authoritative_all_types_spec() -> SchemaSurfaceSpec {
        derive_schema_surface_spec_from_schema(&json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "authoritative_all_types",
            "columns": [
                { "name": "tenant", "type": "text", "nullable": false },
                { "name": "ordinal", "type": "int8", "nullable": false },
                { "name": "text_value", "type": "text", "nullable": false },
                { "name": "uuid_value", "type": "uuid", "nullable": false },
                { "name": "integer_value", "type": "int8", "nullable": false },
                { "name": "number_value", "type": "float8", "nullable": false },
                { "name": "boolean_value", "type": "boolean", "nullable": false },
                { "name": "json_value", "type": "jsonb", "nullable": false },
                { "name": "time_value", "type": "timestamptz", "nullable": false }
            ],
            "primary_key": ["tenant", "ordinal"]
        }))
        .expect("all Schema-v1 types should derive")
    }

    fn authoritative_all_types_snapshot() -> JsonValue {
        json!({
            "tenant": "acme",
            "ordinal": 7,
            "text_value": "{\"stays\":\"text\"}",
            "uuid_value": "018f3f7a-6c2d-7c21-8a42-4e16fd6f66a1",
            "integer_value": -9,
            "number_value": 3.5,
            "boolean_value": true,
            "json_value": {"nested": [1, true]},
            "time_value": "2026-08-14T12:34:56Z"
        })
    }

    #[test]
    fn authoritative_singleton_is_key_bound_pk_free_and_reconstructable() {
        let spec = authoritative_all_types_spec();
        let snapshot = authoritative_all_types_snapshot();
        let row_pk = RowPk::from_primary_key_plan(
            &snapshot,
            &spec.primary_key_paths,
            &spec.primary_key_component_types,
        )
        .expect("composite identity");
        let canonical = snapshot.to_string();
        let row = RowColumnarRowRef {
            row_pk: &row_pk,
            snapshot_bytes: canonical.as_bytes(),
            snapshot_value: &snapshot,
        };
        let encoded =
            encode_authoritative_singleton_row_group(&spec, row).expect("authoritative singleton");
        assert!(matches!(
            encoded.input_locations,
            RowGroupLocations::Dense { row_count: 1 }
        ));
        assert_eq!(
            encoded
                .manifest
                .fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "text_value",
                "uuid_value",
                "integer_value",
                "number_value",
                "boolean_value",
                "json_value",
                "time_value"
            ]
        );
        assert!(
            encoded
                .manifest
                .fields
                .iter()
                .all(|field| field.name != ROW_COLUMNAR_ROW_PK_FIELD)
        );
        assert_eq!(
            encoded
                .manifest
                .metadata
                .get(ROW_COLUMNAR_LAYOUT_FINGERPRINT_METADATA_KEY),
            Some(&spec.columnar_layout_fingerprint())
        );

        let (schema, batch) = authoritative_singleton_batch(&spec, row).expect("batch");
        let layout = identify_authoritative_singleton_layout(&schema)
            .expect("valid metadata")
            .expect("singleton marker");
        assert_eq!(
            layout.schema_fingerprint,
            spec.columnar_layout_fingerprint()
        );
        let reconstructed = layout
            .reconstruct_full_ordered_field_map(&row_pk, &batch)
            .expect("catalog-free reconstruction");
        assert_eq!(
            reconstructed,
            spec.columns
                .iter()
                .map(|column| {
                    (
                        column.name.clone(),
                        snapshot.get(&column.name).expect("declared value").clone(),
                    )
                })
                .collect::<Vec<_>>()
        );
        assert_eq!(reconstructed[2].1, json!("{\"stays\":\"text\"}"));
        assert_eq!(reconstructed[7].1, json!({"nested": [1, true]}));

        let mut wrong_metadata = schema.metadata().clone();
        wrong_metadata.insert(
            ROW_COLUMNAR_LAYOUT_FINGERPRINT_METADATA_KEY.to_owned(),
            "0".repeat(64),
        );
        let wrong_schema = Schema::new_with_metadata(
            schema
                .fields()
                .iter()
                .map(|field| field.as_ref().clone())
                .collect::<Vec<_>>(),
            wrong_metadata,
        );
        assert!(identify_authoritative_singleton_layout(&wrong_schema).is_err());
    }

    #[test]
    fn authoritative_singleton_rejects_wrong_supplied_pk() {
        let spec = authoritative_all_types_spec();
        let snapshot = authoritative_all_types_snapshot();
        let canonical = snapshot.to_string();
        let wrong_pk = RowPk::from_json_values(
            &[json!("other"), json!(7)],
            &spec.primary_key_component_types,
        )
        .expect("typed wrong key");
        let error = encode_authoritative_singleton_row_group(
            &spec,
            RowColumnarRowRef {
                row_pk: &wrong_pk,
                snapshot_bytes: canonical.as_bytes(),
                snapshot_value: &snapshot,
            },
        )
        .expect_err("wrong key must be rejected");
        assert!(error.message.contains("does not match"));
    }

    #[test]
    fn registered_types_and_hidden_identity_round_trip() {
        let spec = derive_schema_surface_spec_from_schema(&json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "typed_sidecar",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "ordinal", "type": "int8", "nullable": false },
                { "name": "score", "type": "float8", "nullable": false },
                { "name": "active", "type": "boolean", "nullable": false },
                { "name": "payload", "type": "jsonb", "nullable": true },
            ],
            "primary_key": ["id", "ordinal"],
        }))
        .expect("spec");
        let snapshots = [
            json!({"id":"a","ordinal":1,"score":1.5,"active":true,"payload":{"z":2,"a":1}}),
            json!({"id":"b","ordinal":2,"score":2,"active":false,"payload":null}),
        ];
        let identities = [
            RowPk::from_json_array_value(&json!(["a", 1])).expect("first identity"),
            RowPk::from_json_array_value(&json!(["b", 2])).expect("second identity"),
        ];
        let canonical = snapshots
            .iter()
            .map(JsonValue::to_string)
            .collect::<Vec<_>>();
        let encoded = encode_registered_row_groups(
            &spec,
            identities.iter().zip(&snapshots).zip(&canonical).map(
                |((row_pk, snapshot), canonical)| RowColumnarRowRef {
                    row_pk,
                    snapshot_bytes: canonical.as_bytes(),
                    snapshot_value: snapshot,
                },
            ),
        )
        .expect("encode")
        .expect("registered sidecar");
        assert_eq!(
            encoded
                .manifest
                .metadata
                .get(ROW_COLUMNAR_LAYOUT_FINGERPRINT_METADATA_KEY),
            Some(&spec.columnar_layout_fingerprint())
        );
        assert_eq!(
            encoded
                .manifest
                .metadata
                .get(ROW_COLUMNAR_BASE_COORDINATES_METADATA_KEY)
                .map(String::as_str),
            Some("true")
        );
        let identity_index = encoded
            .manifest
            .fields
            .iter()
            .position(|field| field.name == ROW_COLUMNAR_ROW_PK_FIELD)
            .expect("hidden identity field");
        assert_eq!(
            encoded.manifest.fields[identity_index].data_type.to_arrow(),
            DataType::Utf8
        );
        let identities = encoded
            .manifest
            .groups
            .iter()
            .filter_map(|group| group.columns[identity_index].min.as_ref())
            .map(|value| match value {
                RowGroupScalar::String(value) => value.as_str(),
                _ => panic!("identity must have string statistics"),
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            identities,
            std::collections::BTreeSet::from([r#"["a",1]"#, r#"["b",2]"#])
        );
    }

    #[test]
    fn frontend_columns_match_canonical_encoding_when_clustering_is_absent() {
        let spec = derive_schema_surface_spec_from_schema(&json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "direct_columns",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        }))
        .expect("schema should derive");
        let ids = (0..128)
            .map(|index| format!("id-{index:04}"))
            .collect::<Vec<_>>();
        let values = (0..128)
            .map(|index| format!("value-{index:04}"))
            .collect::<Vec<_>>();
        let snapshots = ids
            .iter()
            .zip(&values)
            .map(|(id, value)| json!({"id": id, "value": value}))
            .collect::<Vec<_>>();
        let canonical = snapshots
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()
            .expect("snapshots should encode");
        let identities = ids
            .iter()
            .map(|id| RowPk::from_validated_shared_string(id.as_str().into()))
            .collect::<Vec<_>>();
        let canonical_encoding = encode_registered_row_groups(
            &spec,
            identities.iter().zip(&snapshots).zip(&canonical).map(
                |((row_pk, snapshot), canonical)| RowColumnarRowRef {
                    row_pk,
                    snapshot_bytes: canonical.as_bytes(),
                    snapshot_value: snapshot,
                },
            ),
        )
        .expect("canonical encoding should succeed")
        .expect("canonical encoding should exist");
        let direct_encoding = encode_unclustered_registered_row_groups(
            &spec,
            vec![
                Arc::new(StringArray::from(ids.clone())),
                Arc::new(StringArray::from(values)),
            ],
            Arc::new(StringArray::from(
                identities
                    .iter()
                    .map(RowPk::as_json_array_text)
                    .collect::<Result<Vec<_>, _>>()
                    .expect("identities should encode"),
            )),
        )
        .expect("direct encoding should succeed")
        .expect("high-cardinality values should not cluster");
        assert!(matches!(
            &direct_encoding.input_locations,
            RowGroupLocations::Dense { row_count: 128 }
        ));
        assert_eq!(direct_encoding.manifest, canonical_encoding.manifest);
        assert_eq!(
            direct_encoding.input_locations,
            canonical_encoding.input_locations
        );
    }

    #[test]
    fn frontend_json_columns_match_canonical_path_value_encoding() {
        let spec = derive_schema_surface_spec_from_schema(&json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "path_value_columns",
            "columns": [
                { "name": "path", "type": "text", "nullable": false },
                { "name": "value", "type": "jsonb", "nullable": false },
            ],
            "primary_key": ["path"],
        }))
        .expect("schema should derive");
        let paths = (0..128)
            .map(|index| format!("/path/{index:04}"))
            .collect::<Vec<_>>();
        let json_values = (0..128)
            .map(|index| json!({"index": index, "nested": [index, index + 1]}))
            .collect::<Vec<_>>();
        let snapshots = paths
            .iter()
            .zip(&json_values)
            .map(|(path, value)| json!({"path": path, "value": value}))
            .collect::<Vec<_>>();
        let canonical = snapshots
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()
            .expect("snapshots should encode");
        let identities = paths
            .iter()
            .map(|path| RowPk::from_validated_shared_string(path.as_str().into()))
            .collect::<Vec<_>>();
        let canonical_encoding = encode_registered_row_groups(
            &spec,
            identities.iter().zip(&snapshots).zip(&canonical).map(
                |((row_pk, snapshot), canonical)| RowColumnarRowRef {
                    row_pk,
                    snapshot_bytes: canonical.as_bytes(),
                    snapshot_value: snapshot,
                },
            ),
        )
        .expect("canonical encoding should succeed")
        .expect("canonical encoding should exist");
        let direct_encoding = encode_unclustered_registered_row_groups(
            &spec,
            vec![
                Arc::new(StringArray::from(paths.clone())),
                Arc::new(StringArray::from(
                    json_values
                        .iter()
                        .map(serde_json::to_string)
                        .collect::<Result<Vec<_>, _>>()
                        .expect("JSON values should encode"),
                )),
            ],
            Arc::new(StringArray::from(
                identities
                    .iter()
                    .map(RowPk::as_json_array_text)
                    .collect::<Result<Vec<_>, _>>()
                    .expect("identities should encode"),
            )),
        )
        .expect("direct encoding should succeed")
        .expect("path/value columns should not cluster");
        assert_eq!(direct_encoding.manifest, canonical_encoding.manifest);
        assert_eq!(
            direct_encoding.input_locations,
            canonical_encoding.input_locations
        );
    }

    #[test]
    fn input_coordinates_follow_the_clustered_physical_permutation() {
        let spec = derive_schema_surface_spec_from_schema(&json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "coordinate_fixture",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "active", "type": "boolean", "nullable": false },
                { "name": "lane", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        }))
        .expect("spec");
        // Deliberately interleave clustering values so physical order differs
        // from authoritative input order.
        let snapshots = [
            json!({"id":"a","active":true,"lane":"z"}),
            json!({"id":"b","active":false,"lane":"a"}),
            json!({"id":"c","active":true,"lane":"y"}),
            json!({"id":"d","active":false,"lane":"b"}),
        ];
        let identities = ["a", "b", "c", "d"].map(RowPk::single);
        let canonical = snapshots
            .iter()
            .map(JsonValue::to_string)
            .collect::<Vec<_>>();
        let encoded = encode_registered_row_groups(
            &spec,
            identities.iter().zip(&snapshots).zip(&canonical).map(
                |((row_pk, snapshot), canonical)| RowColumnarRowRef {
                    row_pk,
                    snapshot_bytes: canonical.as_bytes(),
                    snapshot_value: snapshot,
                },
            ),
        )
        .expect("encode")
        .expect("registered sidecar");
        let identity_index = encoded
            .manifest
            .fields
            .iter()
            .position(|field| field.name == ROW_COLUMNAR_ROW_PK_FIELD)
            .expect("hidden identity field");

        assert_eq!(encoded.input_locations.len(), identities.len());
        assert_ne!(
            encoded
                .input_locations
                .location(0)
                .expect("first input coordinate")
                .group_index,
            encoded
                .input_locations
                .location(1)
                .expect("second input coordinate")
                .group_index
        );
        for (input_index, location) in encoded.input_locations.iter().enumerate() {
            let group = &encoded.manifest.groups[location.group_index as usize];
            let expected = identities[input_index]
                .as_json_array_text()
                .expect("identity text");
            assert_eq!(
                group.columns[identity_index].min,
                Some(RowGroupScalar::String(expected.clone()))
            );
            assert_eq!(
                group.columns[identity_index].max,
                Some(RowGroupScalar::String(expected))
            );
        }
    }

    #[test]
    fn derived_encoding_failure_falls_back_without_rejecting_authoritative_rows() {
        assert!(
            optional_derived_row_group_set(Err(row_columnar_error("physical limit"))).is_none()
        );
    }

    #[test]
    fn any_json_property_encodes_in_registered_layout() {
        let spec = derive_schema_surface_spec_from_schema(&json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "json_layout",
            "columns": [
                { "name": "path", "type": "text", "nullable": false },
                { "name": "value", "type": "jsonb", "nullable": false },
            ],
            "primary_key": ["path"],
        }))
        .expect("spec");
        let snapshot = json!({"path":"a","value":"value-a"});
        let canonical = snapshot.to_string();
        let identity = RowPk::single("a");
        assert!(
            encode_registered_row_groups(
                &spec,
                std::iter::once(RowColumnarRowRef {
                    row_pk: &identity,
                    snapshot_bytes: canonical.as_bytes(),
                    snapshot_value: &snapshot,
                }),
            )
            .expect("encode")
            .is_some()
        );
    }

    #[test]
    fn clustering_has_a_global_partition_budget_for_wide_low_cardinality_schemas() {
        let mut columns = vec![json!({ "name": "id", "type": "text", "nullable": false })];
        for index in 0..2 {
            columns.push(json!({
                "name": format!("flag_{index}"), "type": "boolean", "nullable": false
            }));
        }
        for index in 0..4 {
            columns.push(json!({
                "name": format!("lane_{index}"), "type": "text", "nullable": true
            }));
        }
        let spec = derive_schema_surface_spec_from_schema(&json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "wide_low_cardinality",
            "columns": columns,
            "primary_key": ["id"]
        }))
        .expect("spec");
        let snapshots = (0..1_024)
            .map(|row| {
                let mut snapshot = serde_json::Map::new();
                snapshot.insert("id".to_string(), json!(format!("row-{row}")));
                for index in 0..2 {
                    snapshot.insert(format!("flag_{index}"), json!(((row >> index) & 1) == 1));
                }
                for index in 0..4 {
                    let divisor = 4 * 3_usize.pow(index);
                    match (row / divisor) % 3 {
                        0 => {
                            snapshot.insert(format!("lane_{index}"), json!("lane-a"));
                        }
                        1 => {
                            snapshot.insert(format!("lane_{index}"), json!("lane-b"));
                        }
                        _ => {}
                    }
                }
                JsonValue::Object(snapshot)
            })
            .collect::<Vec<_>>();
        let identities = (0..snapshots.len())
            .map(|row| RowPk::from_json_array_value(&json!([format!("row-{row}")])).unwrap())
            .collect::<Vec<_>>();
        let canonical = snapshots
            .iter()
            .map(JsonValue::to_string)
            .collect::<Vec<_>>();

        let encoded = encode_registered_row_groups(
            &spec,
            identities.iter().zip(&snapshots).zip(&canonical).map(
                |((row_pk, snapshot), canonical)| RowColumnarRowRef {
                    row_pk,
                    snapshot_bytes: canonical.as_bytes(),
                    snapshot_value: snapshot,
                },
            ),
        )
        .expect("encode")
        .expect("registered sidecar");

        assert!(encoded.manifest.groups.len() > 1);
        assert!(
            encoded.manifest.groups.len() <= ROW_COLUMNAR_MAX_CLUSTER_PARTITIONS,
            "wide independent dimensions created {} groups despite a {}-partition budget",
            encoded.manifest.groups.len(),
            ROW_COLUMNAR_MAX_CLUSTER_PARTITIONS
        );
    }
}
