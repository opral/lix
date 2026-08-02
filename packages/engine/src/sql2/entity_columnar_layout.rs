//! Registered-schema-bound analytical sidecars for immutable entity generations.
//!
//! The transaction boundary calls this adapter while validated canonical
//! snapshots and typed entity identities are still available. SQL's existing
//! projection decoder remains the single value-conversion contract; this
//! module only chooses physical row groups and delegates their encoding.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use serde_json::Value as JsonValue;

use crate::LixError;
use crate::columnar_row_group::{
    EncodedRowGroupSet, ROW_GROUP_MAX_ROWS, encode_row_group_set_preserving_batches,
};
use crate::entity_pk::EntityPk;
use crate::sql2::{
    EntityColumnType, EntityProjectionDecoder, EntitySurfaceSpec, entity_visible_fields,
};

pub(crate) const ENTITY_COLUMNAR_LAYOUT_FINGERPRINT_METADATA_KEY: &str =
    "lix.entity_columnar.layout_fingerprint.v1";
pub(crate) const ENTITY_COLUMNAR_ENTITY_PK_FIELD: &str = "lixcol_entity_pk";
const LOW_CARDINALITY_CLUSTER_MAX_VALUES: usize = 64;
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

pub(crate) fn encode_registered_entity_row_groups<'a, I>(
    spec: &EntitySurfaceSpec,
    rows: I,
) -> Result<Option<EncodedRowGroupSet>, LixError>
where
    I: ExactSizeIterator<Item = EntityColumnarRowRef<'a>> + Clone,
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

fn encode_registered_entity_row_groups_impl<'a, I>(
    spec: &EntitySurfaceSpec,
    rows: I,
) -> Result<EncodedRowGroupSet, LixError>
where
    I: ExactSizeIterator<Item = EntityColumnarRowRef<'a>> + Clone,
{
    let mut fields = entity_visible_fields(spec);
    fields.push(Field::new(
        ENTITY_COLUMNAR_ENTITY_PK_FIELD,
        DataType::Utf8,
        false,
    ));
    let mut metadata = HashMap::new();
    metadata.insert(
        ENTITY_COLUMNAR_LAYOUT_FINGERPRINT_METADATA_KEY.to_string(),
        spec.columnar_layout_fingerprint(),
    );
    let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
    let decoder =
        EntityProjectionDecoder::new(spec, spec.columns.iter().map(|column| column.name.as_str()))?;

    let rows = rows.collect::<Vec<_>>();
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
        for row in &rows {
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
        let mut partitions = BTreeMap::<Vec<u8>, Vec<EntityColumnarRowRef<'_>>>::new();
        for row in rows {
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
            partitions.entry(key).or_default().push(row);
        }
        partitions.into_values().collect()
    };

    let mut batches = Vec::new();
    for partition in partitions {
        for rows in partition.chunks(ROW_GROUP_MAX_ROWS) {
            let mut columns =
                decoder.decode_arrow_columns(rows.iter().map(|row| Some(row.snapshot_bytes)))?;
            let entity_pks = rows
                .iter()
                .map(|row| row.entity_pk.as_json_array_text())
                .collect::<Result<Vec<_>, _>>()?;
            let entity_pks: ArrayRef = Arc::new(StringArray::from(entity_pks));
            columns.push(entity_pks);
            batches.push(
                RecordBatch::try_new(Arc::clone(&schema), columns)
                    .map_err(|error| entity_columnar_error(error.to_string()))?,
            );
        }
    }
    encode_row_group_set_preserving_batches(&spec.schema_key, schema, &batches)
}

fn optional_derived_row_group_set(
    encoded: Result<EncodedRowGroupSet, LixError>,
) -> Option<EncodedRowGroupSet> {
    encoded.ok()
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
            "x-lix-key": "typed_sidecar",
            "x-lix-primary-key": ["/id", "/ordinal"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "ordinal": { "type": "integer" },
                "score": { "type": "number" },
                "active": { "type": "boolean" },
                "payload": { "type": ["object", "null"] }
            },
            "required": ["id", "ordinal", "score", "active"]
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
    fn derived_encoding_failure_falls_back_without_rejecting_authoritative_rows() {
        assert!(
            optional_derived_row_group_set(Err(entity_columnar_error("physical limit"))).is_none()
        );
    }

    #[test]
    fn clustering_has_a_global_partition_budget_for_wide_low_cardinality_schemas() {
        let mut properties = serde_json::Map::new();
        properties.insert("id".to_string(), json!({ "type": "string" }));
        for index in 0..2 {
            properties.insert(format!("flag_{index}"), json!({ "type": "boolean" }));
        }
        for index in 0..4 {
            properties.insert(
                format!("lane_{index}"),
                json!({ "type": ["string", "null"] }),
            );
        }
        let spec = derive_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "wide_low_cardinality",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": properties,
            "required": ["id"]
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
