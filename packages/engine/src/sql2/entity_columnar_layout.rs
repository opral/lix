//! Registered-schema-bound Arrow state leaves for immutable entity generations.
//!
//! The transaction boundary calls this adapter while validated canonical
//! snapshots and typed entity identities are still available. SQL's existing
//! projection decoder remains the single value-conversion contract; this
//! module seals the canonical typed leaf layout consumed by storage and SQL.

use std::collections::{BTreeMap, HashMap};
use std::ops::Deref;
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Int64Array, StringArray, UInt64Array,
};
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
pub(crate) const ENTITY_COLUMNAR_ENTITY_PK_FIELD: &str = "lixcol_entity_pk";
pub(crate) use crate::tracked_state::{
    ENTITY_ARROW_STATE_COMMIT_ID_METADATA, ENTITY_ARROW_STATE_CREATED_AT_METADATA,
    ENTITY_ARROW_STATE_LAYOUT, ENTITY_ARROW_STATE_NAMESPACE,
    ENTITY_ARROW_STATE_SCHEMA_KEY_METADATA, ENTITY_ARROW_STATE_UPDATED_AT_METADATA,
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
    /// Already-decoded JSON is optional because it is used only for physical
    /// clustering. Certified journals can feed canonical bytes straight into
    /// the Arrow projection decoder without allocating a second JSON tree.
    pub(crate) snapshot_value: Option<&'a JsonValue>,
    pub(crate) authority: Option<EntityColumnarAuthorityRef<'a>>,
}

#[derive(Clone, Copy)]
pub(crate) struct EntityColumnarAuthorityRef<'a> {
    pub(crate) value: crate::tracked_state::TrackedStateIndexValueRef,
    pub(crate) snapshot: crate::json_store::JsonSlotRef<'a>,
    pub(crate) metadata: crate::json_store::JsonSlotRef<'a>,
}

#[derive(Clone, Debug)]
pub(crate) struct EncodedEntityRowGroups {
    encoded: EncodedRowGroupSet,
    pub(crate) input_locations: Vec<RowGroupRowLocation>,
}

impl Deref for EncodedEntityRowGroups {
    type Target = EncodedRowGroupSet;

    fn deref(&self) -> &Self::Target {
        &self.encoded
    }
}

impl EncodedEntityRowGroups {
    pub(crate) fn into_parts(self) -> (EncodedRowGroupSet, Vec<RowGroupRowLocation>) {
        (self.encoded, self.input_locations)
    }
}

#[cfg(test)]
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

/// Seals registered rows as canonical state authority. Unlike the rebuildable
/// derived-cache path, a physical encoding failure is fatal and cannot be
/// silently downgraded to a row-owned fallback.
pub(crate) fn encode_authoritative_registered_entity_row_groups<'a, I>(
    spec: &EntitySurfaceSpec,
    rows: I,
) -> Result<EncodedEntityRowGroups, LixError>
where
    I: ExactSizeIterator<Item = EntityColumnarRowRef<'a>>,
{
    if rows.len() == 0 {
        return Err(entity_columnar_error(
            "authoritative Arrow state set cannot be empty",
        ));
    }
    let encoded = encode_registered_entity_row_groups_impl(spec, rows)?;
    if encoded
        .manifest
        .metadata
        .get("lix.layout")
        .is_none_or(|layout| layout != ENTITY_ARROW_STATE_LAYOUT)
    {
        return Err(entity_columnar_error(
            "authoritative rows did not produce the canonical state layout",
        ));
    }
    Ok(encoded)
}

/// Encodes frontend-owned typed columns when the established clustering rules
/// preserve input order. This is transaction-local ingress data; commit-time
/// sealing still adds physical identity and lifecycle columns before the set
/// can become state authority.
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
    let metadata = HashMap::from([
        (
            ENTITY_COLUMNAR_LAYOUT_FINGERPRINT_METADATA_KEY.to_string(),
            spec.columnar_layout_fingerprint(),
        ),
        (
            ENTITY_COLUMNAR_BASE_COORDINATES_METADATA_KEY.to_string(),
            "true".to_owned(),
        ),
    ]);
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
    let input_locations = (0..row_count)
        .map(|index| RowGroupRowLocation {
            group_index: u32::try_from(index / ROW_GROUP_MAX_ROWS)
                .expect("row-group count fits u32"),
            row_index: u32::try_from(index % ROW_GROUP_MAX_ROWS).expect("row index fits u32"),
        })
        .collect();
    Ok(Some(EncodedEntityRowGroups {
        encoded,
        input_locations,
    }))
}

fn encode_registered_entity_row_groups_impl<'a, I>(
    spec: &EntitySurfaceSpec,
    rows: I,
) -> Result<EncodedEntityRowGroups, LixError>
where
    I: ExactSizeIterator<Item = EntityColumnarRowRef<'a>>,
{
    let rows = rows.enumerate().collect::<Vec<_>>();
    let authoritative = rows.iter().all(|(_, row)| row.authority.is_some());
    if !authoritative && rows.iter().any(|(_, row)| row.authority.is_some()) {
        return Err(entity_columnar_error(
            "a row group set cannot mix authoritative and derived rows",
        ));
    }
    let authoritative_lifecycle = authoritative.then(|| {
        let first = rows[0]
            .1
            .authority
            .expect("authoritative set was validated");
        let uniform_commit_id = rows.iter().all(|(_, row)| {
            row.authority
                .expect("authoritative set was validated")
                .value
                .commit_id
                == first.value.commit_id
        });
        let uniform_created_at = rows.iter().all(|(_, row)| {
            row.authority
                .expect("authoritative set was validated")
                .value
                .created_at
                == first.value.created_at
        });
        let uniform_updated_at = rows.iter().all(|(_, row)| {
            row.authority
                .expect("authoritative set was validated")
                .value
                .updated_at
                == first.value.updated_at
        });
        (
            first,
            uniform_commit_id,
            uniform_created_at,
            uniform_updated_at,
        )
    });
    let mut fields = Vec::new();
    if authoritative {
        fields.extend([
            Field::new("physical_key", DataType::Binary, false),
            Field::new("change_id", DataType::Binary, false),
            Field::new("deleted", DataType::Boolean, false),
            Field::new("snapshot_kind", DataType::Int64, false),
            Field::new("snapshot_payload", DataType::Binary, true),
            Field::new("metadata_kind", DataType::Int64, false),
            Field::new("metadata_payload", DataType::Binary, true),
        ]);
        let (_, uniform_commit_id, uniform_created_at, uniform_updated_at) =
            authoritative_lifecycle.expect("authoritative lifecycle was computed");
        if !uniform_commit_id {
            fields.push(Field::new("commit_id", DataType::Binary, false));
        }
        if !uniform_created_at {
            fields.push(Field::new("created_at", DataType::UInt64, false));
        }
        if !uniform_updated_at {
            fields.push(Field::new("updated_at", DataType::UInt64, false));
        }
    }
    fields.extend(entity_visible_fields(spec));
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
    metadata.insert(
        ENTITY_COLUMNAR_BASE_COORDINATES_METADATA_KEY.to_string(),
        "true".to_owned(),
    );
    if authoritative {
        let (first, uniform_commit_id, uniform_created_at, uniform_updated_at) =
            authoritative_lifecycle.expect("authoritative lifecycle was computed");
        if rows.iter().any(|(_, row)| {
            row.authority
                .expect("authoritative set was validated")
                .value
                .deleted
        }) {
            return Err(entity_columnar_error(
                "typed Arrow state leaves require live post-images",
            ));
        }
        metadata.insert(
            "lix.layout".to_owned(),
            ENTITY_ARROW_STATE_LAYOUT.to_owned(),
        );
        metadata.insert("lix.order".to_owned(), "physical_key-ascending".to_owned());
        metadata.insert(
            ENTITY_ARROW_STATE_SCHEMA_KEY_METADATA.to_owned(),
            spec.schema_key.clone(),
        );
        if uniform_commit_id {
            metadata.insert(
                ENTITY_ARROW_STATE_COMMIT_ID_METADATA.to_owned(),
                first.value.commit_id.to_string(),
            );
        }
        if uniform_created_at {
            metadata.insert(
                ENTITY_ARROW_STATE_CREATED_AT_METADATA.to_owned(),
                first.value.created_at.packed().to_string(),
            );
        }
        if uniform_updated_at {
            metadata.insert(
                ENTITY_ARROW_STATE_UPDATED_AT_METADATA.to_owned(),
                first.value.updated_at.packed().to_string(),
            );
        }
    }
    let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
    let decoder =
        EntityProjectionDecoder::new(spec, spec.columns.iter().map(|column| column.name.as_str()))?;

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
                .and_then(|snapshot| snapshot.get(&column.name))
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
    let partitions = if authoritative || cluster_fields.is_empty() {
        vec![rows]
    } else {
        let mut partitions = BTreeMap::<Vec<u8>, Vec<(usize, EntityColumnarRowRef<'_>)>>::new();
        for (input_index, row) in rows {
            let key = cluster_fields
                .iter()
                .map(|field| match field {
                    ClusterField::Boolean(name) => {
                        match row
                            .snapshot_value
                            .and_then(|snapshot| snapshot.get(*name))
                            .and_then(JsonValue::as_bool)
                        {
                            Some(false) => 0,
                            Some(true) => 1,
                            None => 2,
                        }
                    }
                    ClusterField::String(name, dictionary) => row
                        .snapshot_value
                        .and_then(|snapshot| snapshot.get(*name))
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
        let group_max_rows = if authoritative {
            512
        } else {
            ROW_GROUP_MAX_ROWS
        };
        for rows in partition.chunks(group_max_rows) {
            let group_index = u32::try_from(batches.len())
                .map_err(|_| entity_columnar_error("row-group index exceeds u32"))?;
            for (row_index, (input_index, _)) in rows.iter().enumerate() {
                input_locations[*input_index] = Some(RowGroupRowLocation {
                    group_index,
                    row_index: u32::try_from(row_index)
                        .map_err(|_| entity_columnar_error("row index exceeds u32"))?,
                });
            }
            let mut columns = Vec::new();
            if authoritative {
                fn slot<'a>(value: crate::json_store::JsonSlotRef<'a>) -> (i64, Option<&'a [u8]>) {
                    match value {
                        crate::json_store::JsonSlotRef::None => (0, None),
                        crate::json_store::JsonSlotRef::Inline(json) => (1, Some(json.as_bytes())),
                        crate::json_store::JsonSlotRef::Ref(reference) => {
                            (2, Some(reference.as_hash_bytes()))
                        }
                    }
                }
                let authority = rows
                    .iter()
                    .map(|(_, row)| row.authority.expect("authoritative set was validated"))
                    .collect::<Vec<_>>();
                let (snapshot_tags, snapshot_payloads): (Vec<_>, Vec<_>) =
                    authority.iter().map(|row| slot(row.snapshot)).unzip();
                let (metadata_tags, metadata_payloads): (Vec<_>, Vec<_>) =
                    authority.iter().map(|row| slot(row.metadata)).unzip();
                let physical_keys = rows
                    .iter()
                    .map(|(_, row)| {
                        crate::tracked_state::encode_key_ref(
                            crate::tracked_state::TrackedStateKeyRef {
                                schema_key: &spec.schema_key,
                                file_id: None,
                                entity_pk: row.entity_pk,
                            },
                        )
                    })
                    .collect::<Vec<_>>();
                columns.extend::<[ArrayRef; 7]>([
                    Arc::new(BinaryArray::from_iter_values(
                        physical_keys.iter().map(Vec::as_slice),
                    )),
                    Arc::new(BinaryArray::from_iter_values(
                        authority
                            .iter()
                            .map(|row| row.value.change_id.as_uuid().as_bytes().as_slice()),
                    )),
                    Arc::new(BooleanArray::from_iter(
                        authority.iter().map(|row| Some(row.value.deleted)),
                    )),
                    Arc::new(Int64Array::from_iter_values(snapshot_tags)),
                    Arc::new(BinaryArray::from_iter(snapshot_payloads)),
                    Arc::new(Int64Array::from_iter_values(metadata_tags)),
                    Arc::new(BinaryArray::from_iter(metadata_payloads)),
                ]);
                let (_, uniform_commit_id, uniform_created_at, uniform_updated_at) =
                    authoritative_lifecycle.expect("authoritative lifecycle was computed");
                if !uniform_commit_id {
                    columns.push(Arc::new(BinaryArray::from_iter_values(
                        authority
                            .iter()
                            .map(|row| row.value.commit_id.as_uuid().as_bytes().as_slice()),
                    )));
                }
                if !uniform_created_at {
                    columns.push(Arc::new(UInt64Array::from_iter_values(
                        authority.iter().map(|row| row.value.created_at.packed()),
                    )));
                }
                if !uniform_updated_at {
                    columns.push(Arc::new(UInt64Array::from_iter_values(
                        authority.iter().map(|row| row.value.updated_at.packed()),
                    )));
                }
            }
            columns.extend(
                decoder
                    .decode_arrow_columns(rows.iter().map(|(_, row)| Some(row.snapshot_bytes)))?,
            );
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
    let namespace = if authoritative {
        ENTITY_ARROW_STATE_NAMESPACE
    } else {
        &spec.schema_key
    };
    let encoded = encode_row_group_set_preserving_batches(namespace, schema, &batches)?;
    Ok(EncodedEntityRowGroups {
        encoded,
        input_locations: input_locations
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| entity_columnar_error("row-group permutation omitted an input row"))?,
    })
}

#[cfg(test)]
fn optional_derived_row_group_set(
    encoded: Result<EncodedEntityRowGroups, LixError>,
) -> Option<EncodedEntityRowGroups> {
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
                    snapshot_value: Some(snapshot),
                    authority: None,
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
    fn authoritative_layout_lifts_only_uniform_lifecycle_values() {
        let spec = derive_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "mixed_lifecycle",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "score": { "type": "integer" }
            },
            "required": ["id", "score"]
        }))
        .expect("spec");
        let snapshots = [json!({"id":"a","score":1}), json!({"id":"b","score":2})];
        let canonical = snapshots
            .iter()
            .map(JsonValue::to_string)
            .collect::<Vec<_>>();
        let identities = [EntityPk::single("a"), EntityPk::single("b")];
        let commit_id = crate::changelog::CommitId::for_test_label("mixed-lifecycle-commit");
        let updated_at = crate::common::LixTimestamp::from_unix_millis_utc_lossy(30);
        let encoded = encode_authoritative_registered_entity_row_groups(
            &spec,
            identities
                .iter()
                .zip(&snapshots)
                .zip(&canonical)
                .enumerate()
                .map(
                    |(index, ((entity_pk, snapshot), canonical))| EntityColumnarRowRef {
                        entity_pk,
                        snapshot_bytes: canonical.as_bytes(),
                        snapshot_value: Some(snapshot),
                        authority: Some(EntityColumnarAuthorityRef {
                            value: crate::tracked_state::TrackedStateIndexValueRef {
                                change_id: crate::changelog::ChangeId::for_test_label(&format!(
                                    "mixed-lifecycle-change-{index}"
                                )),
                                commit_id,
                                deleted: false,
                                created_at: crate::common::LixTimestamp::from_unix_millis_utc_lossy(
                                    10 + index as i64,
                                ),
                                updated_at,
                            },
                            snapshot: crate::json_store::JsonSlotRef::Inline(canonical),
                            metadata: crate::json_store::JsonSlotRef::None,
                        }),
                    },
                ),
        )
        .expect("authoritative layout");
        assert_eq!(
            encoded
                .manifest
                .metadata
                .get("lix.layout")
                .map(String::as_str),
            Some(ENTITY_ARROW_STATE_LAYOUT)
        );
        assert!(
            encoded
                .manifest
                .metadata
                .contains_key(ENTITY_ARROW_STATE_COMMIT_ID_METADATA)
        );
        assert!(
            encoded
                .manifest
                .metadata
                .contains_key(ENTITY_ARROW_STATE_UPDATED_AT_METADATA)
        );
        assert!(
            !encoded
                .manifest
                .metadata
                .contains_key(ENTITY_ARROW_STATE_CREATED_AT_METADATA)
        );
        assert!(
            encoded
                .manifest
                .fields
                .iter()
                .any(|field| field.name == "created_at")
        );
    }

    #[test]
    fn input_coordinates_follow_the_clustered_physical_permutation() {
        let spec = derive_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "coordinate_fixture",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "active": { "type": "boolean" },
                "lane": { "type": "string" }
            },
            "required": ["id", "active", "lane"]
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
                    snapshot_value: Some(snapshot),
                    authority: None,
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
            encoded.input_locations[0].group_index,
            encoded.input_locations[1].group_index
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
            "x-lix-key": "json_layout",
            "x-lix-primary-key": ["/path"],
            "type": "object",
            "properties": {
                "path": { "type": "string" },
                "value": {
                    "type": [
                        "object", "array", "string", "number", "integer", "boolean", "null"
                    ]
                }
            },
            "required": ["path", "value"]
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
                    snapshot_value: Some(&snapshot),
                    authority: None,
                }),
            )
            .expect("encode")
            .is_some()
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
                    snapshot_value: Some(snapshot),
                    authority: None,
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
