//! Registered-schema-bound columnar sidecars for immutable entity generations.
//!
//! The transaction boundary calls this adapter while validated canonical
//! snapshots and typed entity identities are still available. SQL's existing
//! projection decoder remains the single value-conversion contract; this
//! module only chooses physical row groups and delegates their encoding.

use std::collections::{BTreeMap, HashMap};
use std::ops::Deref;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use serde_json::Value as JsonValue;

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
pub(crate) use crate::hot_state::{
    ENTITY_COLUMNAR_ENTITY_PK_FIELD, ENTITY_COLUMNAR_LOSSLESS_SNAPSHOT_METADATA_KEY,
};
pub(crate) const LOW_CARDINALITY_CLUSTER_MAX_VALUES: usize = 64;
const LOW_CARDINALITY_CLUSTER_MAX_BUCKETS: usize = 8;
const ENTITY_COLUMNAR_MAX_CLUSTER_PARTITIONS: usize = 64;

enum ClusterField<'a> {
    Boolean(&'a str),
    String(&'a str, BTreeMap<String, u8>),
}

#[derive(Clone, Copy)]
pub(crate) struct EntityColumnarRowRef<'a> {
    pub(crate) entity_pk: &'a EntityPk,
    pub(crate) snapshot_bytes: &'a [u8],
    pub(crate) snapshot_value: &'a JsonValue,
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

    let mut fields = entity_visible_fields(spec);
    fields.push(Field::new(
        ENTITY_COLUMNAR_ENTITY_PK_FIELD,
        DataType::Utf8,
        false,
    ));
    let metadata = entity_columnar_metadata(spec);
    let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
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
    let mut fields = entity_visible_fields(spec);
    fields.push(Field::new(
        ENTITY_COLUMNAR_ENTITY_PK_FIELD,
        DataType::Utf8,
        false,
    ));
    let metadata = entity_columnar_metadata(spec);
    let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
    let decoder =
        EntityProjectionDecoder::new(spec, spec.columns.iter().map(|column| column.name.as_str()))?;

    let rows = rows.enumerate().collect::<Vec<_>>();
    let primary_key_roots = spec
        .primary_key_paths
        .iter()
        .filter_map(|path| path.first().map(String::as_str))
        .collect::<std::collections::BTreeSet<_>>();
    let mut cluster_fields = Vec::new();
    let mut partition_budget = 1_usize;
    for column in &spec.columns {
        if column.column_type == EntityColumnType::Boolean
            && partition_budget.saturating_mul(3) <= ENTITY_COLUMNAR_MAX_CLUSTER_PARTITIONS
        {
            cluster_fields.push(ClusterField::Boolean(column.name.as_str()));
            partition_budget *= 3;
        }
    }
    for column in &spec.columns {
        if column.column_type != EntityColumnType::String
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
                > ENTITY_COLUMNAR_MAX_CLUSTER_PARTITIONS
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
        let mut partitions = BTreeMap::<Vec<u8>, Vec<(usize, EntityColumnarRowRef<'_>)>>::new();
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
                .map_err(|_| entity_columnar_error("row-group index exceeds u32"))?;
            for (row_index, (input_index, _)) in rows.iter().enumerate() {
                input_locations[*input_index] = Some(RowGroupRowLocation {
                    group_index,
                    row_index: u32::try_from(row_index)
                        .map_err(|_| entity_columnar_error("row index exceeds u32"))?,
                });
            }
            let mut columns = decoder
                .decode_arrow_columns(rows.iter().map(|(_, row)| Some(row.snapshot_bytes)))?;
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
    use serde_json::json;

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
                |((entity_pk, snapshot), canonical)| EntityColumnarRowRef {
                    entity_pk,
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
        let spec = derive_entity_surface_spec_from_schema(&json!({
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
            .map(|id| EntityPk::from_validated_shared_string(id.as_str().into()))
            .collect::<Vec<_>>();
        let canonical_encoding = encode_registered_entity_row_groups(
            &spec,
            identities.iter().zip(&snapshots).zip(&canonical).map(
                |((entity_pk, snapshot), canonical)| EntityColumnarRowRef {
                    entity_pk,
                    snapshot_bytes: canonical.as_bytes(),
                    snapshot_value: snapshot,
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
            .map(|path| EntityPk::from_validated_shared_string(path.as_str().into()))
            .collect::<Vec<_>>();
        let canonical_encoding = encode_registered_entity_row_groups(
            &spec,
            identities.iter().zip(&snapshots).zip(&canonical).map(
                |((entity_pk, snapshot), canonical)| EntityColumnarRowRef {
                    entity_pk,
                    snapshot_bytes: canonical.as_bytes(),
                    snapshot_value: snapshot,
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
    fn input_coordinates_follow_the_clustered_physical_permutation() {
        let spec = derive_entity_surface_spec_from_schema(&json!({
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
        let identities = ["a", "b", "c", "d"].map(EntityPk::single);
        let canonical = snapshots
            .iter()
            .map(JsonValue::to_string)
            .collect::<Vec<_>>();
        let encoded = encode_registered_entity_row_groups(
            &spec,
            identities.iter().zip(&snapshots).zip(&canonical).map(
                |((entity_pk, snapshot), canonical)| EntityColumnarRowRef {
                    entity_pk,
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
            .position(|field| field.name == ENTITY_COLUMNAR_ENTITY_PK_FIELD)
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
            optional_derived_row_group_set(Err(entity_columnar_error("physical limit"))).is_none()
        );
    }

    #[test]
    fn any_json_property_encodes_in_registered_layout() {
        let spec = derive_entity_surface_spec_from_schema(&json!({
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
        let identity = EntityPk::single("a");
        assert!(
            encode_registered_entity_row_groups(
                &spec,
                std::iter::once(EntityColumnarRowRef {
                    entity_pk: &identity,
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
                |((entity_pk, snapshot), canonical)| EntityColumnarRowRef {
                    entity_pk,
                    snapshot_bytes: canonical.as_bytes(),
                    snapshot_value: snapshot,
                },
            ),
        )
        .expect("encode")
        .expect("registered sidecar");

        assert!(encoded.manifest.groups.len() > 1);
        assert!(
            encoded.manifest.groups.len() <= ENTITY_COLUMNAR_MAX_CLUSTER_PARTITIONS,
            "wide independent dimensions created {} groups despite a {}-partition budget",
            encoded.manifest.groups.len(),
            ENTITY_COLUMNAR_MAX_CLUSTER_PARTITIONS
        );
    }
}
