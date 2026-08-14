//! Registered-schema-bound columnar sidecars for immutable entity generations.
//!
//! The transaction boundary calls this adapter while validated canonical
//! snapshots and typed entity identities are still available. SQL's existing
//! projection decoder remains the single value-conversion contract; this
//! module only chooses physical row groups and delegates their encoding.

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;

use crate::LixError;
use crate::columnar_row_group::{
    EncodedRowGroupSet, ROW_GROUP_MAX_ROWS, RowGroupRowLocation,
    encode_row_group_set_preserving_batches,
};
use crate::entity_pk::EntityPk;
use crate::sql2::{
    EntityColumnType, EntityProjectionDecoder, EntitySurfaceSpec, entity_visible_fields,
};

pub(crate) const ENTITY_COLUMNAR_LAYOUT_FINGERPRINT_METADATA_KEY: &str =
    "lix.entity_columnar.layout_fingerprint.v1";
pub(crate) const ENTITY_COLUMNAR_BASE_COORDINATES_METADATA_KEY: &str =
    "lix.entity_columnar.base_coordinates.v1";
pub(crate) const ENTITY_COLUMNAR_PRIMARY_KEY_PATHS_METADATA_KEY: &str =
    "lix.entity_columnar.primary_key_paths.v1";
pub(crate) use crate::hot_state::{
    ENTITY_COLUMNAR_DELETED_FIELD, ENTITY_COLUMNAR_ENTITY_PK_FIELD,
    ENTITY_COLUMNAR_LOSSLESS_SNAPSHOT_METADATA_KEY,
    ENTITY_COLUMNAR_TYPED_HISTORY_METADATA_KEY,
};
pub(crate) const LOW_CARDINALITY_CLUSTER_MAX_VALUES: usize = 64;

#[derive(Clone, Copy)]
pub(crate) struct EntityColumnarRowRef<'a> {
    pub(crate) entity_pk: &'a EntityPk,
    pub(crate) snapshot_bytes: Option<&'a [u8]>,
}

#[derive(Clone, Debug)]
pub(crate) struct EncodedEntityRowGroups {
    encoded: EncodedRowGroupSet,
    pub(crate) input_locations: EntityRowGroupLocations,
}

/// Input-row to physical-row mapping for one sealed entity generation.
///
/// Identity-preserving batches use arithmetic coordinates and retain no
/// row-cardinal location column. Clustered layouts keep the explicit
/// permutation required to map their reordered rows back to statement order.
#[derive(Clone, Debug)]
pub(crate) enum EntityRowGroupLocations {
    Dense { row_count: usize },
    Explicit(Vec<RowGroupRowLocation>),
}

impl EntityRowGroupLocations {
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
                .expect("entity row-group location covers every input row")
        })
    }
}

impl PartialEq for EntityRowGroupLocations {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().eq(other.iter())
    }
}

impl Eq for EntityRowGroupLocations {}

impl Deref for EncodedEntityRowGroups {
    type Target = EncodedRowGroupSet;

    fn deref(&self) -> &Self::Target {
        &self.encoded
    }
}

impl EncodedEntityRowGroups {
    pub(crate) fn into_parts(self) -> (EncodedRowGroupSet, EntityRowGroupLocations) {
        (self.encoded, self.input_locations)
    }
}

pub(crate) fn encode_registered_entity_row_groups<'a, I>(
    spec: &EntitySurfaceSpec,
    rows: I,
) -> Result<Option<EncodedEntityRowGroups>, LixError>
where
    I: ExactSizeIterator<Item = EntityColumnarRowRef<'a>>,
{
    if rows.len() == 0 {
        return Ok(None);
    }
    // This is a derived acceleration structure. Projection or physical-limit
    // failures must retain the authoritative row layout rather than reject an
    // otherwise-valid transaction.
    Ok(optional_derived_row_group_set(
        encode_registered_entity_row_groups_impl(spec, rows),
    ))
}

/// Encodes the sole durable payload for qualifying registered entity history.
/// Unlike the optional current-state acceleration wrapper, this path must
/// propagate every encoding failure rather than silently selecting JSON.
pub(crate) fn encode_registered_entity_history_row_groups<'a, I>(
    spec: &EntitySurfaceSpec,
    rows: I,
) -> Result<EncodedEntityRowGroups, LixError>
where
    I: ExactSizeIterator<Item = EntityColumnarRowRef<'a>>,
{
    encode_registered_entity_row_groups_impl(spec, rows)
}

/// Encodes one schema-bound typed history tuple for a mixed commit whose
/// native system members prevent the entity rows from owning the commit's
/// sole columnar group. The canonical member key owns the primary key, so the
/// payload contains only non-PK scalar cells. This is the same durable D1
/// scalar contract as a columnar group, not a JSON compatibility snapshot.
pub(crate) fn encode_registered_entity_history_snapshot(
    spec: &EntitySurfaceSpec,
    snapshot_bytes: Option<&[u8]>,
) -> Result<crate::changelog::TypedHistorySnapshot, LixError> {
    let Some(snapshot_bytes) = snapshot_bytes else {
        return Ok(crate::changelog::TypedHistorySnapshot {
            schema_layout_fingerprint: spec.columnar_layout_fingerprint(),
            deleted: true,
            primary_key_paths: spec.primary_key_paths.clone(),
            fields: Vec::new(),
        });
    };
    let snapshot: serde_json::Value = serde_json::from_slice(snapshot_bytes)
        .map_err(|error| entity_columnar_error(format!("typed history snapshot is invalid JSON: {error}")))?;
    let object = snapshot.as_object().ok_or_else(|| {
        entity_columnar_error("typed history snapshot is not a JSON object")
    })?;
    let primary_key_roots = spec
        .primary_key_paths
        .iter()
        .filter_map(|path| path.first().map(String::as_str))
        .collect::<std::collections::BTreeSet<_>>();
    let fields = spec
        .columns
        .iter()
        .filter(|column| !primary_key_roots.contains(column.name.as_str()))
        .map(|column| {
            let value = object.get(&column.name).ok_or_else(|| {
                entity_columnar_error(format!(
                    "typed history snapshot omitted declared column '{}'",
                    column.name
                ))
            })?;
            let value = if value.is_null() {
                None
            } else {
                Some(match column.column_type {
                    EntityColumnType::String => crate::changelog::TypedHistoryScalar::String(
                        value.as_str().ok_or_else(|| {
                            entity_columnar_error(format!(
                                "typed history column '{}' is not text",
                                column.name
                            ))
                        })?.to_owned(),
                    ),
                    EntityColumnType::Json => crate::changelog::TypedHistoryScalar::Jsonb(
                        serde_json::to_string(value).map_err(|error| {
                            entity_columnar_error(format!(
                                "typed history jsonb column '{}' cannot encode: {error}",
                                column.name
                            ))
                        })?,
                    ),
                    EntityColumnType::Integer => crate::changelog::TypedHistoryScalar::Int64(
                        value.as_i64().ok_or_else(|| {
                            entity_columnar_error(format!(
                                "typed history column '{}' is not int8",
                                column.name
                            ))
                        })?,
                    ),
                    EntityColumnType::Number => {
                        crate::changelog::TypedHistoryScalar::Float64Bits(
                            value.as_f64().ok_or_else(|| {
                                entity_columnar_error(format!(
                                    "typed history column '{}' is not float8",
                                    column.name
                                ))
                            })?.to_bits(),
                        )
                    }
                    EntityColumnType::Boolean => crate::changelog::TypedHistoryScalar::Boolean(
                        value.as_bool().ok_or_else(|| {
                            entity_columnar_error(format!(
                                "typed history column '{}' is not boolean",
                                column.name
                            ))
                        })?,
                    ),
                    EntityColumnType::Timestamptz => {
                        let text = value.as_str().ok_or_else(|| {
                            entity_columnar_error(format!(
                                "typed history column '{}' is not timestamptz text",
                                column.name
                            ))
                        })?;
                        let micros = chrono::DateTime::parse_from_rfc3339(text)
                            .map_err(|error| {
                                entity_columnar_error(format!(
                                    "typed history column '{}' is not canonical timestamptz: {error}",
                                    column.name
                                ))
                            })?
                            .timestamp_micros();
                        crate::changelog::TypedHistoryScalar::TimestampMicros(micros)
                    }
                })
            };
            Ok(crate::changelog::TypedHistoryField {
                name: column.name.clone(),
                value,
            })
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    Ok(crate::changelog::TypedHistorySnapshot {
        schema_layout_fingerprint: spec.columnar_layout_fingerprint(),
        deleted: false,
        primary_key_paths: spec.primary_key_paths.clone(),
        fields,
    })
}

/// Encodes frontend-owned Arrow columns without reconstructing them from
/// canonical snapshot JSON. The fast contract is deliberately limited to
/// layouts whose established encoder would not reorder rows for clustering;
/// clustered layouts retain the general encoder and identical physical
/// behavior.
pub(crate) fn encode_unclustered_registered_entity_row_groups(
    spec: &EntitySurfaceSpec,
    mut columns: Vec<ArrayRef>,
    entity_pks: ArrayRef,
) -> Result<Option<EncodedEntityRowGroups>, LixError> {
    if columns.len() != spec.columns.len() {
        return Err(entity_columnar_error(
            "frontend column count does not match the registered schema",
        ));
    }
    let row_count = entity_pks.len();
    if row_count == 0 || columns.iter().any(|column| column.len() != row_count) {
        return Err(entity_columnar_error(
            "frontend columns are empty or have inconsistent row counts",
        ));
    }
    let primary_key_roots = spec
        .primary_key_paths
        .iter()
        .filter_map(|path| path.first().map(String::as_str))
        .collect::<std::collections::BTreeSet<_>>();
    for (spec_column, array) in spec.columns.iter().zip(&columns) {
        if spec_column.column_type == EntityColumnType::Boolean {
            return Ok(None);
        }
        if spec_column.column_type != EntityColumnType::String
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

    columns = spec
        .columns
        .iter()
        .zip(columns)
        .filter_map(|(column, values)| {
            (!primary_key_roots.contains(column.name.as_str())).then_some(values)
        })
        .collect();

    // Physical history groups must represent tombstones as NULL payload cells.
    // Logical nullability remains schema-authenticated and is enforced when a
    // non-deleted tuple is decoded/projected.
    let mut fields = entity_visible_fields(spec)
        .into_iter()
        .filter(|field| !primary_key_roots.contains(field.name().as_str()))
        .map(|field| field.with_nullable(true))
        .collect::<Vec<_>>();
    fields.push(Field::new(
        ENTITY_COLUMNAR_DELETED_FIELD,
        DataType::Boolean,
        false,
    ));
    fields.push(Field::new(
        ENTITY_COLUMNAR_ENTITY_PK_FIELD,
        DataType::Utf8,
        false,
    ));
    let metadata = entity_columnar_metadata(spec);
    let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
    columns.push(Arc::new(BooleanArray::from(vec![false; row_count])));
    columns.push(entity_pks);
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
            .map_err(|error| entity_columnar_error(error.to_string()))?,
        );
    }
    let encoded = encode_row_group_set_preserving_batches(&spec.schema_key, schema, &batches)?;
    Ok(Some(EncodedEntityRowGroups {
        encoded,
        input_locations: EntityRowGroupLocations::Dense { row_count },
    }))
}

fn encode_registered_entity_row_groups_impl<'a, I>(
    spec: &EntitySurfaceSpec,
    rows: I,
) -> Result<EncodedEntityRowGroups, LixError>
where
    I: ExactSizeIterator<Item = EntityColumnarRowRef<'a>>,
{
    let primary_key_roots = spec
        .primary_key_paths
        .iter()
        .filter_map(|path| path.first().map(String::as_str))
        .collect::<std::collections::BTreeSet<_>>();
    let payload_columns = spec
        .columns
        .iter()
        .filter(|column| !primary_key_roots.contains(column.name.as_str()))
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>();
    let mut fields = entity_visible_fields(spec)
        .into_iter()
        .filter(|field| !primary_key_roots.contains(field.name().as_str()))
        .map(|field| field.with_nullable(true))
        .collect::<Vec<_>>();
    fields.push(Field::new(
        ENTITY_COLUMNAR_DELETED_FIELD,
        DataType::Boolean,
        false,
    ));
    fields.push(Field::new(
        ENTITY_COLUMNAR_ENTITY_PK_FIELD,
        DataType::Utf8,
        false,
    ));
    let metadata = entity_columnar_metadata(spec);
    let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
    let decoder = EntityProjectionDecoder::new(spec, payload_columns)?;

    let mut rows = rows
        .enumerate()
        .map(|(input_index, row)| {
            let key = crate::tracked_state::encode_key_ref(
                crate::tracked_state::TrackedStateKeyRef {
                    schema_key: spec.schema_key.as_str(),
                    file_id: None,
                    entity_pk: row.entity_pk,
                },
            );
            (key, input_index, row)
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.cmp(&right.0));
    if rows.windows(2).any(|rows| rows[0].0 == rows[1].0) {
        return Err(entity_columnar_error(
            "typed history batch contains duplicate canonical StateKeys",
        ));
    }
    let partitions = vec![rows
        .into_iter()
        .map(|(_, input_index, row)| (input_index, row))
        .collect::<Vec<_>>()];

    let input_count = partitions.iter().map(Vec::len).sum();
    let mut input_locations = vec![None; input_count];
    let mut batches = Vec::new();
    for partition in partitions {
        for rows in partition.chunks(ROW_GROUP_MAX_ROWS) {
            let group_index = u32::try_from(batches.len())
                .map_err(|_| entity_columnar_error("row-group index exceeds u32"))?;
            for (row_index, (input_index, _)) in rows.iter().enumerate() {
                input_locations[*input_index] = Some(RowGroupRowLocation {
                    group_index,
                    row_index: u32::try_from(row_index)
                        .map_err(|_| entity_columnar_error("row index exceeds u32"))?,
                });
            }
            let mut columns = decoder
                .decode_arrow_columns(rows.iter().map(|(_, row)| row.snapshot_bytes))?;
            columns.push(Arc::new(BooleanArray::from(
                rows.iter()
                    .map(|(_, row)| row.snapshot_bytes.is_none())
                    .collect::<Vec<_>>(),
            )));
            let entity_pks = rows
                .iter()
                .map(|(_, row)| row.entity_pk.as_json_array_text())
                .collect::<Result<Vec<_>, _>>()?;
            let entity_pks: ArrayRef = Arc::new(StringArray::from(entity_pks));
            columns.push(entity_pks);
            batches.push(
                RecordBatch::try_new(Arc::clone(&schema), columns)
                    .map_err(|error| entity_columnar_error(error.to_string()))?,
            );
        }
    }
    let encoded = encode_row_group_set_preserving_batches(&spec.schema_key, schema, &batches)?;
    Ok(EncodedEntityRowGroups {
        encoded,
        input_locations: EntityRowGroupLocations::Explicit(
            input_locations
                .into_iter()
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| {
                    entity_columnar_error("row-group permutation omitted an input row")
                })?,
        ),
    })
}

fn optional_derived_row_group_set(
    encoded: Result<EncodedEntityRowGroups, LixError>,
) -> Option<EncodedEntityRowGroups> {
    encoded.ok()
}

fn entity_columnar_metadata(spec: &EntitySurfaceSpec) -> HashMap<String, String> {
    let mut metadata = HashMap::from([
        (
            ENTITY_COLUMNAR_LAYOUT_FINGERPRINT_METADATA_KEY.to_string(),
            spec.columnar_layout_fingerprint(),
        ),
        (
            ENTITY_COLUMNAR_BASE_COORDINATES_METADATA_KEY.to_string(),
            "true".to_owned(),
        ),
        (
            ENTITY_COLUMNAR_PRIMARY_KEY_PATHS_METADATA_KEY.to_string(),
            serde_json::to_string(&spec.primary_key_paths)
            .expect("entity primary-key field names serialize"),
        ),
        (
            ENTITY_COLUMNAR_TYPED_HISTORY_METADATA_KEY.to_string(),
            "true".to_owned(),
        ),
    ]);
    if spec.columnar_snapshot_bijective {
        metadata.insert(
            ENTITY_COLUMNAR_LOSSLESS_SNAPSHOT_METADATA_KEY.to_string(),
            "true".to_owned(),
        );
    }
    metadata
}

fn entity_columnar_error(message: impl Into<String>) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("entity columnar layout: {}", message.into()),
    )
}

#[cfg(test)]
mod tests {
    use serde_json::{Value as JsonValue, json};

    use super::*;
    use crate::columnar_row_group::RowGroupScalar;
    use crate::entity_pk::EntityPk;
    use crate::sql2::derive_entity_surface_spec_from_schema;

    #[test]
    fn registered_types_and_hidden_identity_round_trip() {
        let spec = derive_entity_surface_spec_from_schema(&json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "typed_sidecar",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "ordinal", "type": "int8", "nullable": false },
                { "name": "score", "type": "float8", "nullable": false },
                { "name": "active", "type": "boolean", "nullable": false },
                { "name": "payload", "type": "jsonb", "nullable": true }
            ],
            "primary_key": ["id", "ordinal"]
        }))
        .expect("spec");
        let snapshots = [
            json!({"id":"a","ordinal":1,"score":1.5,"active":true,"payload":{"z":2,"a":1}}),
            json!({"id":"b","ordinal":2,"score":2,"active":false,"payload":null}),
        ];
        let identities = [
            EntityPk::from_json_array_value(&json!(["a", 1])).expect("first identity"),
            EntityPk::from_json_array_value(&json!(["b", 2])).expect("second identity"),
        ];
        let canonical = snapshots
            .iter()
            .map(JsonValue::to_string)
            .collect::<Vec<_>>();
        let encoded = encode_registered_entity_row_groups(
            &spec,
            identities.iter().zip(&snapshots).zip(&canonical).map(
                |((entity_pk, _snapshot), canonical)| EntityColumnarRowRef {
                    entity_pk,
                    snapshot_bytes: Some(canonical.as_bytes()),
                },
            ),
        )
        .expect("encode")
        .expect("registered sidecar");
        assert_eq!(
            encoded
                .manifest
                .metadata
                .get(ENTITY_COLUMNAR_LAYOUT_FINGERPRINT_METADATA_KEY),
            Some(&spec.columnar_layout_fingerprint())
        );
        assert_eq!(
            encoded
                .manifest
                .metadata
                .get(ENTITY_COLUMNAR_BASE_COORDINATES_METADATA_KEY)
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            encoded
                .manifest
                .metadata
                .get(ENTITY_COLUMNAR_PRIMARY_KEY_PATHS_METADATA_KEY)
                .map(String::as_str),
            Some(r#"[["id"],["ordinal"]]"#)
        );
        assert_eq!(
            encoded
                .manifest
                .metadata
                .get(ENTITY_COLUMNAR_TYPED_HISTORY_METADATA_KEY)
                .map(String::as_str),
            Some("true")
        );
        let identity_index = encoded
            .manifest
            .fields
            .iter()
            .position(|field| field.name == ENTITY_COLUMNAR_ENTITY_PK_FIELD)
            .expect("hidden identity field");
        assert_eq!(
            encoded.manifest.fields[identity_index].data_type.to_arrow(),
            DataType::Utf8
        );
        assert_eq!(
            encoded.manifest.groups[0].columns[identity_index].min,
            Some(RowGroupScalar::String(r#"["a",1]"#.to_owned()))
        );
        assert_eq!(
            encoded.manifest.groups[0].columns[identity_index].max,
            Some(RowGroupScalar::String(r#"["b",2]"#.to_owned()))
        );
    }

    #[test]
    fn frontend_columns_match_canonical_encoding_when_clustering_is_absent() {
        let spec = derive_entity_surface_spec_from_schema(&json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "direct_columns",
            "columns": [
                {"name":"id", "type":"text", "nullable":false},
                {"name":"value", "type":"text", "nullable":false}
            ],
            "primary_key": ["id"]
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
            .map(|id| EntityPk::from_validated_shared_string(id.as_str().into()))
            .collect::<Vec<_>>();
        let canonical_encoding = encode_registered_entity_row_groups(
            &spec,
            identities.iter().zip(&snapshots).zip(&canonical).map(
                |((entity_pk, _snapshot), canonical)| EntityColumnarRowRef {
                    entity_pk,
                    snapshot_bytes: Some(canonical.as_bytes()),
                },
            ),
        )
        .expect("canonical encoding should succeed")
        .expect("canonical encoding should exist");
        let direct_encoding = encode_unclustered_registered_entity_row_groups(
            &spec,
            vec![
                Arc::new(StringArray::from(ids.clone())),
                Arc::new(StringArray::from(values)),
            ],
            Arc::new(StringArray::from(
                identities
                    .iter()
                    .map(EntityPk::as_json_array_text)
                    .collect::<Result<Vec<_>, _>>()
                    .expect("identities should encode"),
            )),
        )
        .expect("direct encoding should succeed")
        .expect("high-cardinality values should not cluster");
        assert!(matches!(
            &direct_encoding.input_locations,
            EntityRowGroupLocations::Dense { row_count: 128 }
        ));
        assert_eq!(direct_encoding.manifest, canonical_encoding.manifest);
        assert_eq!(
            direct_encoding.input_locations,
            canonical_encoding.input_locations
        );
    }

    #[test]
    fn frontend_json_columns_match_canonical_path_value_encoding() {
        let spec = derive_entity_surface_spec_from_schema(&json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "path_value_columns",
            "columns": [
                {"name":"path", "type":"text", "nullable":false},
                {"name":"value", "type":"jsonb", "nullable":false}
            ],
            "primary_key": ["path"]
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
            .map(|path| EntityPk::from_validated_shared_string(path.as_str().into()))
            .collect::<Vec<_>>();
        let canonical_encoding = encode_registered_entity_row_groups(
            &spec,
            identities.iter().zip(&snapshots).zip(&canonical).map(
                |((entity_pk, _snapshot), canonical)| EntityColumnarRowRef {
                    entity_pk,
                    snapshot_bytes: Some(canonical.as_bytes()),
                },
            ),
        )
        .expect("canonical encoding should succeed")
        .expect("canonical encoding should exist");
        let direct_encoding = encode_unclustered_registered_entity_row_groups(
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
                    .map(EntityPk::as_json_array_text)
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
    fn input_coordinates_follow_canonical_state_key_order() {
        let spec = derive_entity_surface_spec_from_schema(&json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "coordinate_fixture",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "active", "type": "boolean", "nullable": false },
                { "name": "lane", "type": "text", "nullable": false }
            ],
            "primary_key": ["id"]
        }))
        .expect("spec");
        // Deliberately reverse identities so physical order differs from
        // statement order while the input-location map preserves RETURNING.
        let snapshots = [
            json!({"id":"d","active":true,"lane":"z"}),
            json!({"id":"b","active":false,"lane":"a"}),
            json!({"id":"c","active":true,"lane":"y"}),
            json!({"id":"a","active":false,"lane":"b"}),
        ];
        let identities = ["d", "b", "c", "a"].map(EntityPk::single);
        let canonical = snapshots
            .iter()
            .map(JsonValue::to_string)
            .collect::<Vec<_>>();
        let encoded = encode_registered_entity_row_groups(
            &spec,
            identities.iter().zip(&snapshots).zip(&canonical).map(
                |((entity_pk, _snapshot), canonical)| EntityColumnarRowRef {
                    entity_pk,
                    snapshot_bytes: Some(canonical.as_bytes()),
                },
            ),
        )
        .expect("encode")
        .expect("registered sidecar");
        assert_eq!(encoded.input_locations.len(), identities.len());
        let physical_rows = encoded
            .input_locations
            .iter()
            .map(|location| (location.group_index, location.row_index))
            .collect::<Vec<_>>();
        assert_eq!(physical_rows, vec![(0, 3), (0, 1), (0, 2), (0, 0)]);
    }

    #[test]
    fn derived_encoding_failure_falls_back_without_rejecting_authoritative_rows() {
        assert!(
            optional_derived_row_group_set(Err(entity_columnar_error("physical limit"))).is_none()
        );
    }

    #[test]
    fn any_json_property_encodes_in_registered_layout() {
        let spec = derive_entity_surface_spec_from_schema(&json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "json_layout",
            "columns": [
                {"name":"path", "type":"text", "nullable":false},
                {"name":"value", "type":"jsonb", "nullable":false}
            ],
            "primary_key": ["path"]
        }))
        .expect("spec");
        let snapshot = json!({"path":"a","value":"value-a"});
        let canonical = snapshot.to_string();
        let identity = EntityPk::single("a");
        assert!(
            encode_registered_entity_row_groups(
                &spec,
                std::iter::once(EntityColumnarRowRef {
                    entity_pk: &identity,
                    snapshot_bytes: Some(canonical.as_bytes()),
                }),
            )
            .expect("encode")
            .is_some()
        );
    }

    #[test]
    fn canonical_layout_does_not_partition_on_low_cardinality_values() {
        let mut columns = vec![json!({"name":"id", "type":"text", "nullable":false})];
        for index in 0..2 {
            columns.push(json!({"name":format!("flag_{index}"), "type":"boolean", "nullable":true}));
        }
        for index in 0..4 {
            columns.push(json!({"name":format!("lane_{index}"), "type":"text", "nullable":true}));
        }
        let spec = derive_entity_surface_spec_from_schema(&json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "wide_low_cardinality",
            "columns": columns,
            "primary_key": ["id"]
        }))
        .expect("spec");
        let snapshots = (0..1_024)
            .map(|row| {
                let mut snapshot = serde_json::Map::new();
                snapshot.insert("id".to_string(), json!(format!("entity-{row}")));
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
            .map(|row| EntityPk::from_json_array_value(&json!([format!("entity-{row}")])).unwrap())
            .collect::<Vec<_>>();
        let canonical = snapshots
            .iter()
            .map(JsonValue::to_string)
            .collect::<Vec<_>>();

        let encoded = encode_registered_entity_row_groups(
            &spec,
            identities.iter().zip(&snapshots).zip(&canonical).map(
                |((entity_pk, _snapshot), canonical)| EntityColumnarRowRef {
                    entity_pk,
                    snapshot_bytes: Some(canonical.as_bytes()),
                },
            ),
        )
        .expect("encode")
        .expect("registered sidecar");

        assert_eq!(encoded.manifest.groups.len(), 1);
    }
}
