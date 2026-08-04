//! Arrow-native immutable leaves for the persistent current-state range tree.
//!
//! Every leaf owns ordered physical identity, provenance, lifecycle, and
//! payload slots in independently addressable Arrow columns. The leaf's
//! manifest digest is its physical identity; there is no row-format copy.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array,
    new_null_array,
};
use datafusion::arrow::compute::concat;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;

use crate::LixError;
use crate::columnar_row_group::{
    ArrowStateSetId, EncodedRowGroupSet, LoadedRowGroupSet, RowGroupRowLocation,
    decode_encoded_row_group_set, encode_row_group_set_preserving_batches, stage_row_group_set,
};
use crate::json_store::{JsonRef, JsonSlot, JsonSlotRef};
use crate::storage_adapter::{
    StorageKey, StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet,
};
use crate::tracked_state::types::{TrackedStateIndexValue, TrackedStateIndexValueRef};

pub(crate) const CURRENT_STATE_DATA_PART_REFS_SPACE: StorageSpace = StorageSpace::immutable(
    StorageSpaceId(0x0004_0030),
    "tracked_state.arrow_state_leaf_refs.v1",
);

pub(crate) const CURRENT_STATE_DATA_PART_MAX_ROWS: usize = 16 * 1024;
const CURRENT_STATE_CANONICAL_LEAF_ROWS: usize = 512;
const CURRENT_STATE_DATA_PART_MAX_BYTES: usize = 4 * 1024 * 1024;
const REFS_DIGEST_CONTEXT: &str = "lix arrow state leaf refs v1";
const LEAF_NAMESPACE: &str = "lix.tracked_state.arrow_leaf.v1";
pub(crate) const ENTITY_ARROW_STATE_NAMESPACE: &str = LEAF_NAMESPACE;
pub(crate) const ENTITY_ARROW_STATE_LAYOUT: &str = "arrow-native-state-leaf-v2";
pub(crate) const ENTITY_ARROW_STATE_SCHEMA_KEY_METADATA: &str = "lix.state.schema_key";
pub(crate) const ENTITY_ARROW_STATE_COMMIT_ID_METADATA: &str = "lix.state.commit_id";
pub(crate) const ENTITY_ARROW_STATE_CREATED_AT_METADATA: &str = "lix.state.created_at";
pub(crate) const ENTITY_ARROW_STATE_UPDATED_AT_METADATA: &str = "lix.state.updated_at";
pub(crate) const ENTITY_ARROW_STATE_FILE_ID_METADATA: &str = "lix.state.file_id";

pub(crate) fn current_state_data_projection(
    manifest: &crate::columnar_row_group::RowGroupManifest,
) -> Result<Vec<usize>, LixError> {
    if !manifest
        .metadata
        .get("lix.layout")
        .is_some_and(|layout| layout == ENTITY_ARROW_STATE_LAYOUT)
    {
        return Err(part_error(
            "unsupported Arrow state leaf layout; recreate the repository",
        ));
    }
    if compact_replacement(manifest) {
        // Certified path/value replacements have only one visible payload
        // column. Keep the point-read projection to the identity plus that
        // payload; lifecycle columns are uniform and authenticated by
        // manifest metadata. Bulk scans still request the full schema at the
        // entity-columnar layer.
        let compact_path_value = manifest
            .metadata
            .get(crate::sql2::ENTITY_COLUMNAR_COMPACT_PATH_VALUE_METADATA_KEY)
            .is_some_and(|value| value == "true");
        if compact_path_value {
            return ["physical_key"]
                .into_iter()
                .chain(manifest.fields.iter().filter_map(|field| {
                    (!matches!(
                        field.name.as_str(),
                        "physical_key" | "commit_id" | "created_at" | "updated_at"
                    ))
                    .then_some(field.name.as_str())
                }))
                .map(|name| {
                    manifest
                        .fields
                        .iter()
                        .position(|field| field.name == name)
                        .ok_or_else(|| part_error(format!("Arrow state leaf omitted '{name}'")))
                })
                .collect();
        }
        return Ok((0..manifest.fields.len()).collect());
    }
    let mut names = vec![
        "physical_key",
        "change_id",
        "deleted",
        "snapshot_kind",
        "snapshot_payload",
        "metadata_kind",
        "metadata_payload",
    ];
    for (metadata_key, column_name) in [
        (ENTITY_ARROW_STATE_COMMIT_ID_METADATA, "commit_id"),
        (ENTITY_ARROW_STATE_CREATED_AT_METADATA, "created_at"),
        (ENTITY_ARROW_STATE_UPDATED_AT_METADATA, "updated_at"),
    ] {
        if !manifest.metadata.contains_key(metadata_key) {
            names.push(column_name);
        }
    }
    names
        .into_iter()
        .map(|name| {
            manifest
                .fields
                .iter()
                .position(|field| field.name == name)
                .ok_or_else(|| part_error(format!("Arrow state leaf omitted '{name}'")))
        })
        .collect()
}

fn compact_replacement(manifest: &crate::columnar_row_group::RowGroupManifest) -> bool {
    manifest
        .metadata
        .get(crate::sql2::ENTITY_COLUMNAR_COMPACT_REPLACEMENT_METADATA_KEY)
        .is_some_and(|value| value == "true")
}

fn compact_snapshot(batch: &RecordBatch, row_index: usize) -> Result<JsonSlot, LixError> {
    let mut object = serde_json::Map::new();
    if batch
        .schema()
        .metadata()
        .get(crate::sql2::ENTITY_COLUMNAR_COMPACT_PATH_VALUE_METADATA_KEY)
        .is_some_and(|value| value == "true")
    {
        let key_index = batch
            .schema()
            .index_of("physical_key")
            .map_err(|_| part_error("compact path/value leaf omitted physical_key"))?;
        let encoded_key = binary_column(batch, key_index)?.value(row_index);
        let decoded =
            crate::tracked_state::codec::decode_key_shared(Bytes::copy_from_slice(encoded_key))?;
        let path = decoded
            .entity_pk
            .as_single_string()
            .map_err(|error| part_error(format!("compact path identity is invalid: {error}")))?;
        object.insert(
            "path".to_owned(),
            serde_json::Value::String(path.to_owned()),
        );
    }
    for (column_index, field) in batch.schema().fields().iter().enumerate() {
        if field.name() == "physical_key"
            || field.name() == crate::sql2::ENTITY_COLUMNAR_ENTITY_PK_FIELD
        {
            continue;
        }
        let array = batch.column(column_index);
        let value = if array.is_null(row_index) {
            serde_json::Value::Null
        } else {
            match field.data_type() {
                DataType::Utf8 => {
                    let value = array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| part_error("compact String column has the wrong type"))?
                        .value(row_index);
                    if crate::sql2::field_is_json(field) {
                        serde_json::from_str(value).map_err(|error| {
                            part_error(format!("compact JSON column is invalid: {error}"))
                        })?
                    } else {
                        serde_json::Value::String(value.to_owned())
                    }
                }
                DataType::Int64 => serde_json::Value::Number(
                    array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| part_error("compact Int64 column has the wrong type"))?
                        .value(row_index)
                        .into(),
                ),
                DataType::Float64 => {
                    let value = array
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or_else(|| part_error("compact Float64 column has the wrong type"))?
                        .value(row_index);
                    serde_json::Number::from_f64(value)
                        .map(serde_json::Value::Number)
                        .ok_or_else(|| part_error("compact Float64 column is not finite"))?
                }
                DataType::Boolean => serde_json::Value::Bool(
                    array
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| part_error("compact Boolean column has the wrong type"))?
                        .value(row_index),
                ),
                data_type => {
                    return Err(part_error(format!(
                        "compact replacement has unsupported Arrow type {data_type}"
                    )));
                }
            }
        };
        object.insert(field.name().to_owned(), value);
    }
    serde_json::to_string(&serde_json::Value::Object(object))
        .map(|json| JsonSlot::Inline(json.into()))
        .map_err(|error| part_error(format!("compact snapshot encoding failed: {error}")))
}

fn compact_uniform_value(
    manifest: &crate::columnar_row_group::RowGroupManifest,
    batch: &RecordBatch,
    group_index: u32,
    row_index: usize,
) -> Result<TrackedStateIndexValue, LixError> {
    let commit_id = manifest
        .metadata
        .get(ENTITY_ARROW_STATE_COMMIT_ID_METADATA)
        .ok_or_else(|| part_error("compact replacement omitted commit id"))?
        .parse::<crate::changelog::CommitId>()
        .map_err(|error| part_error(format!("compact commit id is invalid: {error}")))?;
    let timestamp =
        |key: &str, label: &str| {
            let packed =
                if let Some(value) = manifest.metadata.get(key) {
                    value.parse::<u64>().map_err(|error| {
                        part_error(format!("compact {label} is invalid: {error}"))
                    })?
                } else {
                    batch
                        .column(batch.schema().index_of(label).map_err(|_| {
                            part_error(format!("compact replacement omitted {label}"))
                        })?)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .ok_or_else(|| part_error(format!("compact {label} has the wrong type")))?
                        .value(row_index)
                };
            crate::common::LixTimestamp::from_packed(packed)
                .map_err(|error| part_error(format!("compact {label} is invalid: {error}")))
        };
    Ok(TrackedStateIndexValue {
        change_id: crate::tracked_state::addressable_change_id(
            commit_id,
            group_index as usize,
            row_index,
        )?,
        commit_id,
        deleted: false,
        created_at: timestamp(ENTITY_ARROW_STATE_CREATED_AT_METADATA, "created_at")?,
        updated_at: timestamp(ENTITY_ARROW_STATE_UPDATED_AT_METADATA, "updated_at")?,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CurrentStateDataRow {
    pub(crate) encoded_key: Vec<u8>,
    pub(crate) value: TrackedStateIndexValue,
    pub(crate) snapshot: JsonSlot,
    pub(crate) metadata: JsonSlot,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ArrowStateInputRowRef<'a> {
    pub(crate) encoded_key: &'a [u8],
    pub(crate) value: TrackedStateIndexValueRef,
    pub(crate) snapshot: JsonSlotRef<'a>,
    pub(crate) metadata: JsonSlotRef<'a>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ArrowStateRowSelection {
    pub(crate) encoded_key: Vec<u8>,
    pub(crate) value: TrackedStateIndexValue,
    pub(crate) source: Option<(usize, usize, usize)>,
    pub(crate) retain_payload: bool,
}

pub(crate) fn encode_authoritative_arrow_state_rows(
    scope: &crate::tracked_state::types::CommitDeltaReplacementScope,
    rows: &[ArrowStateInputRowRef<'_>],
) -> Result<(EncodedRowGroupSet, Vec<RowGroupRowLocation>), LixError> {
    if rows.is_empty()
        || rows
            .windows(2)
            .any(|pair| pair[0].encoded_key >= pair[1].encoded_key)
    {
        return Err(part_error(
            "authoritative Arrow input is empty or not strictly ordered",
        ));
    }
    let first = rows[0].value;
    let uniform_commit_id = rows
        .iter()
        .all(|row| row.value.commit_id == first.commit_id);
    let uniform_created_at = rows
        .iter()
        .all(|row| row.value.created_at == first.created_at);
    let uniform_updated_at = rows
        .iter()
        .all(|row| row.value.updated_at == first.updated_at);
    let mut fields = vec![
        Field::new("physical_key", DataType::Binary, false),
        Field::new("change_id", DataType::Binary, false),
        Field::new("deleted", DataType::Boolean, false),
        Field::new("snapshot_kind", DataType::Int64, false),
        Field::new("snapshot_payload", DataType::Binary, true),
        Field::new("metadata_kind", DataType::Int64, false),
        Field::new("metadata_payload", DataType::Binary, true),
    ];
    if !uniform_commit_id {
        fields.push(Field::new("commit_id", DataType::Binary, false));
    }
    if !uniform_created_at {
        fields.push(Field::new("created_at", DataType::UInt64, false));
    }
    if !uniform_updated_at {
        fields.push(Field::new("updated_at", DataType::UInt64, false));
    }
    let mut schema_metadata = HashMap::from([
        (
            "lix.layout".to_owned(),
            ENTITY_ARROW_STATE_LAYOUT.to_owned(),
        ),
        ("lix.order".to_owned(), "physical_key-ascending".to_owned()),
        (
            ENTITY_ARROW_STATE_SCHEMA_KEY_METADATA.to_owned(),
            scope.schema_key.clone(),
        ),
    ]);
    if let Some(file_id) = &scope.file_id {
        schema_metadata.insert(
            ENTITY_ARROW_STATE_FILE_ID_METADATA.to_owned(),
            file_id.clone(),
        );
    }
    if uniform_commit_id {
        schema_metadata.insert(
            ENTITY_ARROW_STATE_COMMIT_ID_METADATA.to_owned(),
            first.commit_id.to_string(),
        );
    }
    if uniform_created_at {
        schema_metadata.insert(
            ENTITY_ARROW_STATE_CREATED_AT_METADATA.to_owned(),
            first.created_at.packed().to_string(),
        );
    }
    if uniform_updated_at {
        schema_metadata.insert(
            ENTITY_ARROW_STATE_UPDATED_AT_METADATA.to_owned(),
            first.updated_at.packed().to_string(),
        );
    }
    let schema = Arc::new(Schema::new_with_metadata(fields, schema_metadata));
    let mut locations = Vec::with_capacity(rows.len());
    let mut batches = Vec::with_capacity(rows.len().div_ceil(CURRENT_STATE_DATA_PART_MAX_ROWS));
    for chunk in rows.chunks(CURRENT_STATE_DATA_PART_MAX_ROWS) {
        let group_index = u32::try_from(batches.len())
            .map_err(|_| part_error("Arrow input group index exceeds u32"))?;
        locations.extend((0..chunk.len()).map(|row_index| RowGroupRowLocation {
            group_index,
            row_index: u32::try_from(row_index).expect("bounded Arrow row index fits u32"),
        }));
        let (snapshot_kinds, snapshot_payloads): (Vec<_>, Vec<_>) = chunk
            .iter()
            .map(|row| encode_slot_ref(row.snapshot))
            .unzip();
        let (metadata_kinds, metadata_payloads): (Vec<_>, Vec<_>) = chunk
            .iter()
            .map(|row| encode_slot_ref(row.metadata))
            .unzip();
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(BinaryArray::from_iter_values(
                chunk.iter().map(|row| row.encoded_key),
            )),
            Arc::new(BinaryArray::from_iter_values(
                chunk
                    .iter()
                    .map(|row| row.value.change_id.as_uuid().as_bytes().as_slice()),
            )),
            Arc::new(BooleanArray::from_iter(
                chunk.iter().map(|row| Some(row.value.deleted)),
            )),
            Arc::new(Int64Array::from_iter_values(snapshot_kinds)),
            Arc::new(BinaryArray::from_iter(snapshot_payloads)),
            Arc::new(Int64Array::from_iter_values(metadata_kinds)),
            Arc::new(BinaryArray::from_iter(metadata_payloads)),
        ];
        if !uniform_commit_id {
            columns.push(Arc::new(BinaryArray::from_iter_values(
                chunk
                    .iter()
                    .map(|row| row.value.commit_id.as_uuid().as_bytes().as_slice()),
            )));
        }
        if !uniform_created_at {
            columns.push(Arc::new(UInt64Array::from_iter_values(
                chunk.iter().map(|row| row.value.created_at.packed()),
            )));
        }
        if !uniform_updated_at {
            columns.push(Arc::new(UInt64Array::from_iter_values(
                chunk.iter().map(|row| row.value.updated_at.packed()),
            )));
        }
        batches.push(
            RecordBatch::try_new(Arc::clone(&schema), columns)
                .map_err(|error| part_error(format!("Arrow batch construction failed: {error}")))?,
        );
    }
    Ok((
        encode_row_group_set_preserving_batches(LEAF_NAMESPACE, schema, &batches)?,
        locations,
    ))
}

fn encode_slot_ref(value: JsonSlotRef<'_>) -> (i64, Option<&[u8]>) {
    match value {
        JsonSlotRef::None => (0, None),
        JsonSlotRef::Inline(json) => (1, Some(json.as_bytes())),
        JsonSlotRef::Ref(reference) => (2, Some(reference.as_hash_bytes())),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct EncodedCurrentStateDataPart {
    encoded: EncodedRowGroupSet,
    pub(crate) digest: [u8; 32],
    pub(crate) first_key: Vec<u8>,
    pub(crate) last_key: Vec<u8>,
    pub(crate) row_count: u16,
    pub(crate) refs_digest: [u8; 32],
    pub(crate) refs_bytes: Bytes,
    refs: Vec<[u8; 32]>,
}

impl EncodedCurrentStateDataPart {
    pub(crate) fn stage(&self, writes: &mut StorageWriteSet) -> Result<ArrowStateSetId, LixError> {
        let id = stage_row_group_set(writes, &self.encoded)?;
        if id.as_bytes() != self.digest {
            return Err(part_error(
                "encoded leaf identity changed before publication",
            ));
        }
        writes.put(
            CURRENT_STATE_DATA_PART_REFS_SPACE,
            StorageKey(Bytes::copy_from_slice(&self.digest)),
            StorageValue {
                bytes: self.refs_bytes.clone(),
            },
        );
        Ok(id)
    }

    pub(crate) fn physical_bytes(&self) -> usize {
        self.encoded
            .physical_bytes()
            .saturating_add(self.refs_bytes.len())
    }
}

/// Encodes canonical leaves by selecting rows from Arrow sources. Typed
/// columns are sliced from their source arrays; only the narrow authority
/// columns are reconstructed. This is the sparse path-copy primitive and must
/// not decode snapshots into row-owned JSON.
pub(crate) fn encode_bounded_selected_current_state_data_parts(
    sources: &[LoadedRowGroupSet],
    rows: &[ArrowStateRowSelection],
) -> Result<Vec<EncodedCurrentStateDataPart>, LixError> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    validate_selected_rows(sources, rows)?;
    let mut encoded = Vec::new();
    let mut offset = 0usize;
    while offset < rows.len() {
        let mut count = (rows.len() - offset).min(CURRENT_STATE_CANONICAL_LEAF_ROWS);
        let part = loop {
            let part =
                encode_selected_current_state_data_part(sources, &rows[offset..offset + count], false)?;
            if part.physical_bytes() <= CURRENT_STATE_DATA_PART_MAX_BYTES || count == 1 {
                break part;
            }
            count = count.div_ceil(2);
        };
        encoded.push(part);
        offset += count;
    }
    Ok(encoded)
}

#[derive(Debug, Clone)]
pub(crate) struct CanonicalCurrentStateGroup {
    pub(crate) first_key: Vec<u8>,
    pub(crate) last_key: Vec<u8>,
    pub(crate) row_count: u16,
}

#[derive(Debug, Clone)]
pub(crate) struct EncodedCanonicalCurrentStateSet {
    encoded: EncodedRowGroupSet,
    pub(crate) groups: Vec<CanonicalCurrentStateGroup>,
    refs: Vec<[u8; 32]>,
}

impl EncodedCanonicalCurrentStateSet {
    pub(crate) fn stage(
        &self,
        writes: &mut StorageWriteSet,
    ) -> Result<(ArrowStateSetId, [u8; 32]), LixError> {
        let state_set_id = stage_row_group_set(writes, &self.encoded)?;
        let mut refs = self.refs.clone();
        let refs_digest = stage_current_state_ref_summary(writes, state_set_id, &mut refs)?;
        Ok((state_set_id, refs_digest))
    }
}

/// Canonicalizes a complete logical scope into one content-addressed Arrow
/// set with one batch per 512-row serving group. Lifecycle columns are made
/// explicit across every group so all batches share one physical schema.
pub(crate) fn encode_canonical_selected_current_state_set(
    sources: &[LoadedRowGroupSet],
    rows: &[ArrowStateRowSelection],
) -> Result<EncodedCanonicalCurrentStateSet, LixError> {
    if rows.is_empty() {
        return Err(part_error("canonical Arrow state set is empty"));
    }
    validate_selected_rows(sources, rows)?;
    let mut batches = Vec::with_capacity(rows.len().div_ceil(CURRENT_STATE_CANONICAL_LEAF_ROWS));
    let mut groups = Vec::with_capacity(batches.capacity());
    let mut refs = Vec::new();
    let mut schema = None;
    for chunk in rows.chunks(CURRENT_STATE_CANONICAL_LEAF_ROWS) {
        let part = encode_selected_current_state_data_part(sources, chunk, true)?;
        let projection = (0..part.encoded.manifest.fields.len()).collect::<Vec<_>>();
        let loaded = decode_encoded_row_group_set(&part.encoded, &projection)?;
        let batch = loaded
            .batches
            .into_iter()
            .next()
            .expect("one selected part encodes one batch");
        if let Some(expected) = schema.as_ref() {
            if expected != &batch.schema() {
                return Err(part_error(
                    "canonical Arrow state groups disagree on physical schema",
                ));
            }
        } else {
            schema = Some(batch.schema());
        }
        groups.push(CanonicalCurrentStateGroup {
            first_key: chunk.first().expect("non-empty chunk").encoded_key.clone(),
            last_key: chunk.last().expect("non-empty chunk").encoded_key.clone(),
            row_count: u16::try_from(chunk.len()).expect("canonical group is bounded"),
        });
        refs.extend(part.refs);
        batches.push(batch);
    }
    let schema = schema.expect("non-empty rows produce a schema");
    let encoded = encode_row_group_set_preserving_batches(LEAF_NAMESPACE, schema, &batches)?;
    Ok(EncodedCanonicalCurrentStateSet {
        encoded,
        groups,
        refs,
    })
}

fn encode_selected_current_state_data_part(
    sources: &[LoadedRowGroupSet],
    rows: &[ArrowStateRowSelection],
    force_explicit_lifecycle: bool,
) -> Result<EncodedCurrentStateDataPart, LixError> {
    let first = &rows[0].value;
    let uniform_commit_id = !force_explicit_lifecycle
        && rows
            .iter()
            .all(|row| row.value.commit_id == first.commit_id);
    let uniform_created_at = !force_explicit_lifecycle
        && rows
            .iter()
            .all(|row| row.value.created_at == first.created_at);
    let uniform_updated_at = !force_explicit_lifecycle
        && rows
            .iter()
            .all(|row| row.value.updated_at == first.updated_at);
    let lifecycle_names = ["commit_id", "created_at", "updated_at"];
    let mut payload_fields = Vec::<Field>::new();
    for source in sources {
        for field in source.manifest.schema().fields() {
            if matches!(
                field.name().as_str(),
                "physical_key"
                    | "change_id"
                    | "deleted"
                    | "snapshot_kind"
                    | "snapshot_payload"
                    | "metadata_kind"
                    | "metadata_payload"
            ) || lifecycle_names.contains(&field.name().as_str())
            {
                continue;
            }
            if let Some(existing) = payload_fields
                .iter()
                .find(|existing| existing.name() == field.name())
            {
                if existing.data_type() != field.data_type() {
                    return Err(part_error(format!(
                        "Arrow state sources disagree on the type of '{}'",
                        field.name()
                    )));
                }
            } else {
                // Sparse tombstones without a prior row require null typed
                // values, so the merged physical schema must permit them.
                payload_fields.push(field.as_ref().clone().with_nullable(true));
            }
        }
    }
    let mut fields = vec![
        Field::new("physical_key", DataType::Binary, false),
        Field::new("change_id", DataType::Binary, false),
        Field::new("deleted", DataType::Boolean, false),
        Field::new("snapshot_kind", DataType::Int64, false),
        Field::new("snapshot_payload", DataType::Binary, true),
        Field::new("metadata_kind", DataType::Int64, false),
        Field::new("metadata_payload", DataType::Binary, true),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(BinaryArray::from_iter_values(
            rows.iter().map(|row| row.encoded_key.as_slice()),
        )),
        Arc::new(BinaryArray::from_iter_values(
            rows.iter()
                .map(|row| row.value.change_id.as_uuid().as_bytes().as_slice()),
        )),
        Arc::new(BooleanArray::from_iter(
            rows.iter().map(|row| Some(row.value.deleted)),
        )),
        selected_column(sources, rows, "snapshot_kind", &DataType::Int64, false)?,
        selected_column(sources, rows, "snapshot_payload", &DataType::Binary, false)?,
        selected_column(sources, rows, "metadata_kind", &DataType::Int64, false)?,
        selected_column(sources, rows, "metadata_payload", &DataType::Binary, false)?,
    ];
    let mut schema_metadata = HashMap::from([
        (
            "lix.layout".to_owned(),
            ENTITY_ARROW_STATE_LAYOUT.to_owned(),
        ),
        ("lix.order".to_owned(), "physical_key-ascending".to_owned()),
    ]);
    for source in sources {
        for (key, value) in &source.manifest.metadata {
            if matches!(key.as_str(), "lix.layout" | "lix.order")
                || key == crate::sql2::ENTITY_COLUMNAR_COMPACT_REPLACEMENT_METADATA_KEY
                || key == crate::sql2::ENTITY_COLUMNAR_COMPACT_PATH_VALUE_METADATA_KEY
                || matches!(
                    key.as_str(),
                    ENTITY_ARROW_STATE_COMMIT_ID_METADATA
                        | ENTITY_ARROW_STATE_CREATED_AT_METADATA
                        | ENTITY_ARROW_STATE_UPDATED_AT_METADATA
                )
            {
                continue;
            }
            if schema_metadata
                .get(key)
                .is_some_and(|existing| existing != value)
            {
                return Err(part_error(format!(
                    "Arrow state sources disagree on schema metadata '{key}'"
                )));
            }
            schema_metadata.insert(key.clone(), value.clone());
        }
    }
    if uniform_commit_id {
        schema_metadata.insert(
            ENTITY_ARROW_STATE_COMMIT_ID_METADATA.to_owned(),
            first.commit_id.to_string(),
        );
    } else {
        fields.push(Field::new("commit_id", DataType::Binary, false));
        columns
            .push(Arc::new(BinaryArray::from_iter_values(rows.iter().map(
                |row| row.value.commit_id.as_uuid().as_bytes().as_slice(),
            ))));
    }
    if uniform_created_at {
        schema_metadata.insert(
            ENTITY_ARROW_STATE_CREATED_AT_METADATA.to_owned(),
            first.created_at.packed().to_string(),
        );
    } else {
        fields.push(Field::new("created_at", DataType::UInt64, false));
        columns.push(Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.value.created_at.packed()),
        )));
    }
    if uniform_updated_at {
        schema_metadata.insert(
            ENTITY_ARROW_STATE_UPDATED_AT_METADATA.to_owned(),
            first.updated_at.packed().to_string(),
        );
    } else {
        fields.push(Field::new("updated_at", DataType::UInt64, false));
        columns.push(Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.value.updated_at.packed()),
        )));
    }
    for field in payload_fields {
        columns.push(selected_column(
            sources,
            rows,
            field.name(),
            field.data_type(),
            true,
        )?);
        fields.push(field);
    }
    let schema = Arc::new(Schema::new_with_metadata(fields, schema_metadata));
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)
        .map_err(|error| part_error(format!("Arrow batch construction failed: {error}")))?;
    let encoded = encode_row_group_set_preserving_batches(LEAF_NAMESPACE, schema, &[batch])?;
    if encoded.physical_bytes() > CURRENT_STATE_DATA_PART_MAX_BYTES {
        return Err(part_error("physical Arrow leaf exceeds its bound"));
    }
    let digest = encoded.id().as_bytes();
    let mut refs = Vec::<[u8; 32]>::new();
    for row in rows.iter().filter(|row| row.retain_payload) {
        let Some((source_index, batch_index, row_index)) = row.source else {
            continue;
        };
        if compact_replacement(&sources[source_index].manifest) {
            continue;
        }
        let batch = &sources[source_index].batches[batch_index];
        for (kind_name, payload_name) in [
            ("snapshot_kind", "snapshot_payload"),
            ("metadata_kind", "metadata_payload"),
        ] {
            let kind =
                batch
                    .column(batch.schema().index_of(kind_name).map_err(|_| {
                        part_error(format!("Arrow state source omitted '{kind_name}'"))
                    })?)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| part_error(format!("{kind_name} has the wrong Arrow type")))?;
            if kind.value(row_index) != 2 {
                continue;
            }
            let payload = batch
                .column(batch.schema().index_of(payload_name).map_err(|_| {
                    part_error(format!("Arrow state source omitted '{payload_name}'"))
                })?)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| part_error(format!("{payload_name} has the wrong Arrow type")))?;
            refs.push(
                payload
                    .value(row_index)
                    .try_into()
                    .map_err(|_| part_error(format!("{payload_name} ref is not 32 bytes")))?,
            );
        }
    }
    refs.sort_unstable();
    refs.dedup();
    let refs_bytes = crate::storage_codec::encode("Arrow state leaf refs", &refs)?;
    let refs_digest = refs_digest(&refs_bytes);
    Ok(EncodedCurrentStateDataPart {
        encoded,
        digest,
        first_key: rows.first().expect("validated rows").encoded_key.clone(),
        last_key: rows.last().expect("validated rows").encoded_key.clone(),
        row_count: u16::try_from(rows.len()).expect("Arrow leaf row count is bounded"),
        refs_digest,
        refs_bytes: Bytes::from(refs_bytes),
        refs,
    })
}

fn selected_column(
    sources: &[LoadedRowGroupSet],
    rows: &[ArrowStateRowSelection],
    name: &str,
    data_type: &DataType,
    typed_payload: bool,
) -> Result<ArrayRef, LixError> {
    let mut slices = Vec::<ArrayRef>::with_capacity(rows.len());
    for row in rows {
        let retain = if typed_payload {
            row.source.is_some()
        } else {
            row.retain_payload
        };
        let compact_slice = row
            .source
            .filter(|_| retain)
            .and_then(|(source_index, batch_index, row_index)| {
                let source = sources.get(source_index)?;
                compact_replacement(&source.manifest).then(|| -> Result<ArrayRef, LixError> {
                    let batch = &source.batches[batch_index];
                    match name {
                        "snapshot_kind" => {
                            let array: ArrayRef = Arc::new(Int64Array::from_iter_values([1]));
                            Ok(array)
                        }
                        "snapshot_payload" => {
                            let JsonSlot::Inline(snapshot) = compact_snapshot(batch, row_index)?
                            else {
                                unreachable!("compact snapshots are inline")
                            };
                            let array: ArrayRef =
                                Arc::new(BinaryArray::from_iter([Some(snapshot.as_bytes())]));
                            Ok(array)
                        }
                        "metadata_kind" => {
                            let array: ArrayRef = Arc::new(Int64Array::from_iter_values([0]));
                            Ok(array)
                        }
                        "metadata_payload" => Ok(new_null_array(&DataType::Binary, 1)),
                        _ => Ok(new_null_array(data_type, 1)),
                    }
                })
            })
            .transpose()?;
        let slice = compact_slice.unwrap_or_else(|| {
            row.source
                .filter(|_| retain)
                .and_then(|(source_index, batch_index, row_index)| {
                    let batch = sources.get(source_index)?.batches.get(batch_index)?;
                    let column_index = batch.schema().index_of(name).ok()?;
                    let column = batch.column(column_index);
                    (column.data_type() == data_type).then(|| column.slice(row_index, 1))
                })
                .unwrap_or_else(|| {
                    if !typed_payload && matches!(name, "snapshot_kind" | "metadata_kind") {
                        let kind: ArrayRef = Arc::new(Int64Array::from_iter_values([0]));
                        kind
                    } else {
                        new_null_array(data_type, 1)
                    }
                })
        });
        slices.push(slice);
    }
    concat(
        &slices
            .iter()
            .map(|array| array.as_ref())
            .collect::<Vec<_>>(),
    )
    .map_err(|error| part_error(format!("cannot splice Arrow column '{name}': {error}")))
}

fn validate_selected_rows(
    sources: &[LoadedRowGroupSet],
    rows: &[ArrowStateRowSelection],
) -> Result<(), LixError> {
    if rows.iter().any(|row| row.encoded_key.is_empty())
        || rows
            .windows(2)
            .any(|pair| pair[0].encoded_key >= pair[1].encoded_key)
    {
        return Err(part_error(
            "selected Arrow state rows contain an empty or unordered identity",
        ));
    }
    for row in rows {
        if row.retain_payload != !row.value.deleted {
            return Err(part_error(
                "selected Arrow state row payload disagrees with tombstone state",
            ));
        }
        if let Some((source_index, batch_index, row_index)) = row.source {
            let Some(batch) = sources
                .get(source_index)
                .and_then(|source| source.batches.get(batch_index))
            else {
                return Err(part_error("selected Arrow state source is missing"));
            };
            if row_index >= batch.num_rows() {
                return Err(part_error("selected Arrow state row is out of bounds"));
            }
        } else if row.retain_payload {
            return Err(part_error(
                "live selected Arrow state row has no payload source",
            ));
        }
    }
    Ok(())
}

pub(crate) fn decode_current_state_data_part_refs(
    expected_digest: &[u8; 32],
    encoded: &[u8],
) -> Result<Vec<[u8; 32]>, LixError> {
    if &refs_digest(encoded) != expected_digest {
        return Err(part_error("payload-ref summary digest is invalid"));
    }
    let refs: Vec<[u8; 32]> = crate::storage_codec::decode("Arrow state leaf refs", encoded)?;
    if refs.len() > CURRENT_STATE_DATA_PART_MAX_ROWS * 2
        || refs.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(part_error("payload-ref summary is oversized or unordered"));
    }
    Ok(refs)
}

pub(crate) fn decode_current_state_data_part(
    loaded: &LoadedRowGroupSet,
    group_index: u32,
) -> Result<Vec<CurrentStateDataRow>, LixError> {
    if loaded.manifest.namespace != LEAF_NAMESPACE
        || !loaded
            .manifest
            .metadata
            .get("lix.layout")
            .is_some_and(|layout| layout == ENTITY_ARROW_STATE_LAYOUT)
    {
        return Err(part_error(
            "unsupported Arrow state leaf layout; recreate the repository",
        ));
    }
    if compact_replacement(&loaded.manifest) {
        let mut rows = Vec::with_capacity(loaded.batches.iter().map(RecordBatch::num_rows).sum());
        for batch in &loaded.batches {
            let key = binary_column(
                batch,
                batch
                    .schema()
                    .index_of("physical_key")
                    .map_err(|_| part_error("compact replacement omitted physical_key"))?,
            )?;
            for row_index in 0..batch.num_rows() {
                rows.push(CurrentStateDataRow {
                    encoded_key: key.value(row_index).to_vec(),
                    value: compact_uniform_value(&loaded.manifest, batch, group_index, row_index)?,
                    snapshot: compact_snapshot(batch, row_index)?,
                    metadata: JsonSlot::None,
                });
            }
        }
        validate_rows(&rows)?;
        return Ok(rows);
    }
    let metadata = &loaded.manifest.metadata;
    let uniform_commit_id = metadata
        .get(ENTITY_ARROW_STATE_COMMIT_ID_METADATA)
        .map(|value| {
            value
                .parse::<crate::changelog::CommitId>()
                .map_err(|error| part_error(format!("leaf commit id is invalid: {error}")))
        })
        .transpose()?;
    let timestamp = |key: &str, label: &str| -> Result<Option<_>, LixError> {
        metadata
            .get(key)
            .map(|value| {
                value
                    .parse::<u64>()
                    .map_err(|error| part_error(format!("leaf {label} is invalid: {error}")))
                    .and_then(|packed| {
                        crate::common::LixTimestamp::from_packed(packed).map_err(|error| {
                            part_error(format!("leaf {label} is invalid: {error}"))
                        })
                    })
            })
            .transpose()
    };
    let uniform_created_at = timestamp(ENTITY_ARROW_STATE_CREATED_AT_METADATA, "created_at")?;
    let uniform_updated_at = timestamp(ENTITY_ARROW_STATE_UPDATED_AT_METADATA, "updated_at")?;
    let mut rows = Vec::with_capacity(loaded.batches.iter().map(RecordBatch::num_rows).sum());
    for batch in &loaded.batches {
        let column_index = |name: &str| {
            batch
                .schema()
                .index_of(name)
                .map_err(|_| part_error(format!("Arrow state leaf omitted '{name}'")))
        };
        let key = binary_column(batch, column_index("physical_key")?)?;
        let change_id = binary_column(batch, column_index("change_id")?)?;
        let deleted = batch
            .column(column_index("deleted")?)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| part_error("deleted column has the wrong Arrow type"))?;
        let snapshot_tag = batch
            .column(column_index("snapshot_kind")?)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| part_error("snapshot kind column has the wrong Arrow type"))?;
        let snapshot_payload = binary_column(batch, column_index("snapshot_payload")?)?;
        let metadata_tag = batch
            .column(column_index("metadata_kind")?)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| part_error("metadata kind column has the wrong Arrow type"))?;
        let metadata_payload = binary_column(batch, column_index("metadata_payload")?)?;
        let commit_ids = if uniform_commit_id.is_none() {
            Some(binary_column(batch, column_index("commit_id")?)?)
        } else {
            None
        };
        let created_at = if uniform_created_at.is_none() {
            Some(
                batch
                    .column(column_index("created_at")?)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| part_error("created_at column has the wrong Arrow type"))?,
            )
        } else {
            None
        };
        let updated_at = if uniform_updated_at.is_none() {
            Some(
                batch
                    .column(column_index("updated_at")?)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| part_error("updated_at column has the wrong Arrow type"))?,
            )
        } else {
            None
        };
        for index in 0..batch.num_rows() {
            let commit_id = match uniform_commit_id {
                Some(commit_id) => commit_id,
                None => crate::changelog::CommitId::new(uuid_from_column(
                    commit_ids.expect("nonuniform commit ids have a column"),
                    index,
                    "commit_id",
                )?),
            };
            let decode_timestamp = |uniform: Option<crate::common::LixTimestamp>,
                                    column: Option<&UInt64Array>,
                                    label: &str|
             -> Result<crate::common::LixTimestamp, LixError> {
                uniform.map_or_else(
                    || {
                        crate::common::LixTimestamp::from_packed(
                            column
                                .expect("nonuniform timestamp has a column")
                                .value(index),
                        )
                        .map_err(|error| part_error(format!("{label} is invalid: {error}")))
                    },
                    Ok,
                )
            };
            rows.push(CurrentStateDataRow {
                encoded_key: key.value(index).to_vec(),
                value: TrackedStateIndexValue {
                    change_id: crate::changelog::ChangeId::new(uuid_from_column(
                        change_id,
                        index,
                        "change_id",
                    )?),
                    commit_id,
                    deleted: deleted.value(index),
                    created_at: decode_timestamp(uniform_created_at, created_at, "created_at")?,
                    updated_at: decode_timestamp(uniform_updated_at, updated_at, "updated_at")?,
                },
                snapshot: decode_slot(
                    snapshot_tag.value(index),
                    snapshot_payload,
                    index,
                    "snapshot",
                )?,
                metadata: decode_slot(
                    metadata_tag.value(index),
                    metadata_payload,
                    index,
                    "metadata",
                )?,
            });
        }
    }
    validate_rows(&rows)?;
    Ok(rows)
}

/// Reads only the narrow authority plane needed to merge ordered Arrow rows.
/// Payload and typed value columns remain in their source batches and are
/// selected by coordinate by the sparse splice.
pub(crate) fn decode_current_state_authority_rows(
    loaded: &LoadedRowGroupSet,
) -> Result<Vec<(Vec<u8>, TrackedStateIndexValue, usize, usize)>, LixError> {
    if loaded.manifest.namespace != LEAF_NAMESPACE
        || !loaded
            .manifest
            .metadata
            .get("lix.layout")
            .is_some_and(|layout| layout == ENTITY_ARROW_STATE_LAYOUT)
    {
        return Err(part_error(
            "unsupported Arrow state leaf layout; recreate the repository",
        ));
    }
    if compact_replacement(&loaded.manifest) {
        let mut rows = Vec::with_capacity(loaded.batches.iter().map(RecordBatch::num_rows).sum());
        for (batch_index, batch) in loaded.batches.iter().enumerate() {
            let key = binary_column(
                batch,
                batch
                    .schema()
                    .index_of("physical_key")
                    .map_err(|_| part_error("compact replacement omitted physical_key"))?,
            )?;
            for row_index in 0..batch.num_rows() {
                rows.push((
                    key.value(row_index).to_vec(),
                    compact_uniform_value(&loaded.manifest, batch, batch_index as u32, row_index)?,
                    batch_index,
                    row_index,
                ));
            }
        }
        return Ok(rows);
    }
    let metadata = &loaded.manifest.metadata;
    let uniform_commit_id = metadata
        .get(ENTITY_ARROW_STATE_COMMIT_ID_METADATA)
        .map(|value| {
            value
                .parse::<crate::changelog::CommitId>()
                .map_err(|error| part_error(format!("leaf commit id is invalid: {error}")))
        })
        .transpose()?;
    let timestamp = |key: &str, label: &str| -> Result<Option<_>, LixError> {
        metadata
            .get(key)
            .map(|value| {
                value
                    .parse::<u64>()
                    .map_err(|error| part_error(format!("leaf {label} is invalid: {error}")))
                    .and_then(|packed| {
                        crate::common::LixTimestamp::from_packed(packed).map_err(|error| {
                            part_error(format!("leaf {label} is invalid: {error}"))
                        })
                    })
            })
            .transpose()
    };
    let uniform_created_at = timestamp(ENTITY_ARROW_STATE_CREATED_AT_METADATA, "created_at")?;
    let uniform_updated_at = timestamp(ENTITY_ARROW_STATE_UPDATED_AT_METADATA, "updated_at")?;
    let mut rows = Vec::with_capacity(loaded.batches.iter().map(RecordBatch::num_rows).sum());
    for (batch_index, batch) in loaded.batches.iter().enumerate() {
        let column_index = |name: &str| {
            batch
                .schema()
                .index_of(name)
                .map_err(|_| part_error(format!("Arrow state leaf omitted '{name}'")))
        };
        let key = binary_column(batch, column_index("physical_key")?)?;
        let change_id = binary_column(batch, column_index("change_id")?)?;
        let deleted = batch
            .column(column_index("deleted")?)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| part_error("deleted column has the wrong Arrow type"))?;
        let commit_ids = if uniform_commit_id.is_none() {
            Some(binary_column(batch, column_index("commit_id")?)?)
        } else {
            None
        };
        let created_at = if uniform_created_at.is_none() {
            Some(
                batch
                    .column(column_index("created_at")?)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| part_error("created_at column has the wrong Arrow type"))?,
            )
        } else {
            None
        };
        let updated_at = if uniform_updated_at.is_none() {
            Some(
                batch
                    .column(column_index("updated_at")?)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| part_error("updated_at column has the wrong Arrow type"))?,
            )
        } else {
            None
        };
        for row_index in 0..batch.num_rows() {
            let commit_id = match uniform_commit_id {
                Some(commit_id) => commit_id,
                None => crate::changelog::CommitId::new(uuid_from_column(
                    commit_ids.expect("nonuniform commit ids have a column"),
                    row_index,
                    "commit_id",
                )?),
            };
            let decode_timestamp = |uniform: Option<crate::common::LixTimestamp>,
                                    column: Option<&UInt64Array>,
                                    label: &str|
             -> Result<crate::common::LixTimestamp, LixError> {
                uniform.map_or_else(
                    || {
                        crate::common::LixTimestamp::from_packed(
                            column
                                .expect("nonuniform timestamp has a column")
                                .value(row_index),
                        )
                        .map_err(|error| part_error(format!("{label} is invalid: {error}")))
                    },
                    Ok,
                )
            };
            rows.push((
                key.value(row_index).to_vec(),
                TrackedStateIndexValue {
                    change_id: crate::changelog::ChangeId::new(uuid_from_column(
                        change_id,
                        row_index,
                        "change_id",
                    )?),
                    commit_id,
                    deleted: deleted.value(row_index),
                    created_at: decode_timestamp(uniform_created_at, created_at, "created_at")?,
                    updated_at: decode_timestamp(uniform_updated_at, updated_at, "updated_at")?,
                },
                batch_index,
                row_index,
            ));
        }
    }
    if rows
        .windows(2)
        .any(|pair| pair[0].0.as_slice() >= pair[1].0.as_slice())
    {
        return Err(part_error(
            "Arrow state authority rows are not strictly ordered",
        ));
    }
    Ok(rows)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HydratedArrowStatePayload {
    pub(crate) encoded_key: Vec<u8>,
    pub(crate) value: TrackedStateIndexValue,
    pub(crate) snapshot: JsonSlot,
    pub(crate) metadata: JsonSlot,
}

/// Returns the physical identity summary without materializing any logical
/// rows. Exact readers use this to authenticate a directory descriptor before
/// hydrating only the requested ordinals.
pub(crate) fn current_state_data_group_summary(
    loaded: &LoadedRowGroupSet,
) -> Result<(usize, Vec<u8>, Vec<u8>), LixError> {
    if loaded.batches.len() != 1 {
        return Err(part_error(
            "current-state group summary requires exactly one Arrow row group",
        ));
    }
    let batch = &loaded.batches[0];
    let key = binary_column(
        batch,
        batch
            .schema()
            .index_of("physical_key")
            .map_err(|_| part_error("Arrow state leaf omitted physical_key"))?,
    )?;
    if key.is_empty() {
        return Err(part_error("Arrow state leaf group is empty"));
    }
    Ok((
        key.len(),
        key.value(0).to_vec(),
        key.value(key.len() - 1).to_vec(),
    ))
}

/// Binary-searches the physical Arrow identity column and hydrates only exact
/// matches. The input order and misses are preserved.
pub(crate) fn hydrate_current_state_data_rows_at_keys(
    loaded: &LoadedRowGroupSet,
    group_index: u32,
    encoded_keys: &[&[u8]],
) -> Result<Vec<Option<(CurrentStateDataRow, u32)>>, LixError> {
    if loaded.batches.len() != 1 {
        return Err(part_error(
            "exact current-state hydration requires exactly one Arrow row group",
        ));
    }
    let batch = &loaded.batches[0];
    let key = binary_column(
        batch,
        batch
            .schema()
            .index_of("physical_key")
            .map_err(|_| part_error("Arrow state leaf omitted physical_key"))?,
    )?;
    let mut row_indices = Vec::with_capacity(encoded_keys.len());
    for &encoded_key in encoded_keys {
        let mut low = 0usize;
        let mut high = key.len();
        while low < high {
            let middle = low + (high - low) / 2;
            if key.value(middle) < encoded_key {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        row_indices.push(
            (low < key.len() && key.value(low) == encoded_key)
                .then(|| u32::try_from(low).expect("Arrow row count fits u32")),
        );
    }
    let present_indices = row_indices.iter().flatten().copied().collect::<Vec<_>>();
    let mut hydrated =
        hydrate_current_state_payload_rows(loaded, group_index, &present_indices)?.into_iter();
    row_indices
        .into_iter()
        .map(|row_index| {
            row_index
                .map(|row_index| {
                    let row = hydrated.next().ok_or_else(|| {
                        part_error("exact current-state hydration lost a requested row")
                    })?;
                    Ok((
                        CurrentStateDataRow {
                            encoded_key: row.encoded_key,
                            value: row.value,
                            snapshot: row.snapshot,
                            metadata: row.metadata,
                        },
                        row_index,
                    ))
                })
                .transpose()
        })
        .collect::<Result<Vec<_>, _>>()
        .and_then(|rows| {
            if hydrated.next().is_some() {
                Err(part_error(
                    "exact current-state hydration returned an extra row",
                ))
            } else {
                Ok(rows)
            }
        })
}

/// Hydrates only explicitly requested history coordinates. This is a terminal
/// JSON boundary for changelog results, not an intermediate state
/// representation used to rebuild another leaf.
pub(crate) fn hydrate_current_state_payload_rows(
    loaded: &LoadedRowGroupSet,
    group_index: u32,
    row_indices: &[u32],
) -> Result<Vec<HydratedArrowStatePayload>, LixError> {
    if loaded.batches.len() != 1 {
        return Err(part_error(
            "coordinate hydration requires exactly one Arrow row group",
        ));
    }
    if compact_replacement(&loaded.manifest) {
        let batch = &loaded.batches[0];
        let key = binary_column(
            batch,
            batch
                .schema()
                .index_of("physical_key")
                .map_err(|_| part_error("compact replacement omitted physical_key"))?,
        )?;
        return row_indices
            .iter()
            .map(|&row_index| {
                let row_index = usize::try_from(row_index).expect("u32 fits usize");
                if row_index >= batch.num_rows() {
                    return Err(part_error("authored Arrow event row is outside its leaf"));
                }
                Ok(HydratedArrowStatePayload {
                    encoded_key: key.value(row_index).to_vec(),
                    value: compact_uniform_value(&loaded.manifest, batch, group_index, row_index)?,
                    snapshot: compact_snapshot(batch, row_index)?,
                    metadata: JsonSlot::None,
                })
            })
            .collect();
    }
    let batch = &loaded.batches[0];
    let column_index = |name: &str| {
        batch
            .schema()
            .index_of(name)
            .map_err(|_| part_error(format!("Arrow state leaf omitted '{name}'")))
    };
    let snapshot_kind = batch
        .column(column_index("snapshot_kind")?)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| part_error("snapshot kind column has the wrong Arrow type"))?;
    let snapshot_payload = binary_column(batch, column_index("snapshot_payload")?)?;
    let metadata_kind = batch
        .column(column_index("metadata_kind")?)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| part_error("metadata kind column has the wrong Arrow type"))?;
    let metadata_payload = binary_column(batch, column_index("metadata_payload")?)?;
    row_indices
        .iter()
        .map(|&row_index| {
            let row_index = usize::try_from(row_index).expect("u32 fits usize");
            if row_index >= batch.num_rows() {
                return Err(part_error("authored Arrow event row is outside its leaf"));
            }
            let (encoded_key, value) = decode_current_state_authority_row(
                &loaded.manifest,
                batch,
                row_index,
            )?;
            Ok(HydratedArrowStatePayload {
                encoded_key,
                value,
                snapshot: decode_slot(
                    snapshot_kind.value(row_index),
                    snapshot_payload,
                    row_index,
                    "snapshot",
                )?,
                metadata: decode_slot(
                    metadata_kind.value(row_index),
                    metadata_payload,
                    row_index,
                    "metadata",
                )?,
            })
        })
        .collect()
}

fn decode_current_state_authority_row(
    manifest: &crate::columnar_row_group::RowGroupManifest,
    batch: &RecordBatch,
    row_index: usize,
) -> Result<(Vec<u8>, TrackedStateIndexValue), LixError> {
    let column_index = |name: &str| {
        batch
            .schema()
            .index_of(name)
            .map_err(|_| part_error(format!("Arrow state leaf omitted '{name}'")))
    };
    let key = binary_column(batch, column_index("physical_key")?)?;
    let change_id = binary_column(batch, column_index("change_id")?)?;
    let deleted = batch
        .column(column_index("deleted")?)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| part_error("deleted column has the wrong Arrow type"))?;
    let commit_id = match manifest.metadata.get(ENTITY_ARROW_STATE_COMMIT_ID_METADATA) {
        Some(value) => value
            .parse::<crate::changelog::CommitId>()
            .map_err(|error| part_error(format!("leaf commit id is invalid: {error}")))?,
        None => crate::changelog::CommitId::new(uuid_from_column(
            binary_column(batch, column_index("commit_id")?)?,
            row_index,
            "commit_id",
        )?),
    };
    let timestamp = |metadata_key: &str, column_name: &str| {
        match manifest.metadata.get(metadata_key) {
            Some(value) => value
                .parse::<u64>()
                .map_err(|error| part_error(format!("leaf {column_name} is invalid: {error}")))
                .and_then(|packed| {
                    crate::common::LixTimestamp::from_packed(packed).map_err(|error| {
                        part_error(format!("leaf {column_name} is invalid: {error}"))
                    })
                }),
            None => {
                let values = batch
                    .column(column_index(column_name)?)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| part_error(format!("{column_name} has the wrong Arrow type")))?;
                crate::common::LixTimestamp::from_packed(values.value(row_index)).map_err(|error| {
                    part_error(format!("{column_name} is invalid: {error}"))
                })
            }
        }
    };
    Ok((
        key.value(row_index).to_vec(),
        TrackedStateIndexValue {
            change_id: crate::changelog::ChangeId::new(uuid_from_column(
                change_id,
                row_index,
                "change_id",
            )?),
            commit_id,
            deleted: deleted.value(row_index),
            created_at: timestamp(ENTITY_ARROW_STATE_CREATED_AT_METADATA, "created_at")?,
            updated_at: timestamp(ENTITY_ARROW_STATE_UPDATED_AT_METADATA, "updated_at")?,
        },
    ))
}

fn binary_column(batch: &RecordBatch, index: usize) -> Result<&BinaryArray, LixError> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| part_error(format!("column {index} has the wrong Arrow type")))
}

fn uuid_from_column(
    values: &BinaryArray,
    index: usize,
    label: &str,
) -> Result<uuid::Uuid, LixError> {
    let bytes: [u8; 16] = values
        .value(index)
        .try_into()
        .map_err(|_| part_error(format!("{label} is not 16 bytes")))?;
    Ok(uuid::Uuid::from_bytes(bytes))
}

fn decode_slot(
    tag: i64,
    payloads: &BinaryArray,
    index: usize,
    label: &str,
) -> Result<JsonSlot, LixError> {
    let payload = (!payloads.is_null(index)).then(|| payloads.value(index));
    match (tag, payload) {
        (0, None) => Ok(JsonSlot::None),
        (1, Some(payload)) => Ok(JsonSlot::Inline(
            std::str::from_utf8(payload)
                .map_err(|error| part_error(format!("{label} inline JSON is not UTF-8: {error}")))?
                .into(),
        )),
        (2, Some(payload)) => Ok(JsonSlot::Ref(JsonRef::from_hash_bytes(
            payload
                .try_into()
                .map_err(|_| part_error(format!("{label} ref is not 32 bytes")))?,
        ))),
        _ => Err(part_error(format!("{label} kind and payload disagree"))),
    }
}

fn validate_rows(rows: &[CurrentStateDataRow]) -> Result<(), LixError> {
    if rows.is_empty() || rows.len() > CURRENT_STATE_DATA_PART_MAX_ROWS {
        return Err(part_error(format!("row count {} is invalid", rows.len())));
    }
    for (ordinal, row) in rows.iter().enumerate() {
        if row.encoded_key.is_empty() {
            return Err(part_error(format!("row {ordinal} has an empty key")));
        }
        if row.value.deleted == !row.snapshot.is_none() {
            return Err(part_error(format!(
                "row {ordinal} tombstone={} disagrees with snapshot presence={}",
                row.value.deleted,
                !row.snapshot.is_none()
            )));
        }
    }
    if let Some(ordinal) = rows
        .windows(2)
        .position(|pair| pair[0].encoded_key >= pair[1].encoded_key)
    {
        return Err(part_error(format!(
            "rows {ordinal} and {} are not strictly ordered",
            ordinal + 1
        )));
    }
    Ok(())
}

fn refs_digest(encoded: &[u8]) -> [u8; 32] {
    *blake3::Hasher::new_derive_key(REFS_DIGEST_CONTEXT)
        .update(encoded)
        .finalize()
        .as_bytes()
}

pub(crate) fn stage_current_state_ref_summary(
    writes: &mut StorageWriteSet,
    state_set_id: ArrowStateSetId,
    refs: &mut Vec<[u8; 32]>,
) -> Result<[u8; 32], LixError> {
    refs.sort_unstable();
    refs.dedup();
    let encoded = crate::storage_codec::encode("Arrow state leaf refs", refs)?;
    let digest = refs_digest(&encoded);
    if let Some(existing) =
        writes.staged_value(CURRENT_STATE_DATA_PART_REFS_SPACE, &state_set_id.as_bytes())
    {
        if existing.as_ref() != encoded.as_slice() {
            return Err(part_error(
                "one Arrow state set has conflicting reference summaries",
            ));
        }
        return Ok(digest);
    }
    writes.put(
        CURRENT_STATE_DATA_PART_REFS_SPACE,
        StorageKey(Bytes::copy_from_slice(&state_set_id.as_bytes())),
        StorageValue {
            bytes: Bytes::from(encoded),
        },
    );
    Ok(digest)
}

fn part_error(message: impl std::fmt::Display) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("tracked_state Arrow state leaf {message}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::LixTimestamp;
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};

    fn row(index: usize) -> CurrentStateDataRow {
        CurrentStateDataRow {
            encoded_key: format!("key-{index:04}").into_bytes(),
            value: TrackedStateIndexValue {
                change_id: ChangeId::for_test_label(&format!("native-change-{index}")),
                commit_id: CommitId::for_test_label(&format!("native-commit-{index}")),
                deleted: false,
                created_at: LixTimestamp::from_unix_millis_utc_lossy(index as i64),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy(index as i64 + 1),
            },
            snapshot: JsonSlot::Inline(format!(r#"{{"index":{index}}}"#).into()),
            metadata: JsonSlot::None,
        }
    }

    #[tokio::test]
    async fn arrow_leaves_round_trip_exact_provenance_and_refs() {
        let mut rows = (0..513).map(row).collect::<Vec<_>>();
        let referenced = JsonRef::for_content(b"native referenced metadata");
        rows[0].metadata = JsonSlot::Ref(referenced);
        let scope = crate::tracked_state::types::CommitDeltaReplacementScope {
            schema_key: "native-test".to_owned(),
            file_id: None,
        };
        let inputs = rows
            .iter()
            .map(|row| ArrowStateInputRowRef {
                encoded_key: &row.encoded_key,
                value: TrackedStateIndexValueRef {
                    change_id: row.value.change_id,
                    commit_id: row.value.commit_id,
                    deleted: row.value.deleted,
                    created_at: row.value.created_at,
                    updated_at: row.value.updated_at,
                },
                snapshot: row.snapshot.as_ref_slot(),
                metadata: row.metadata.as_ref_slot(),
            })
            .collect::<Vec<_>>();
        let (encoded, _) = encode_authoritative_arrow_state_rows(&scope, &inputs)
            .expect("canonical Arrow state should encode");
        assert_eq!(
            encoded.manifest.groups.len(),
            2,
            "row bound must split the post-image"
        );
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
        let state_set_id = stage_row_group_set(&mut writes, &encoded).expect("stage Arrow state");
        let refs_digest = stage_current_state_ref_summary(
            &mut writes,
            state_set_id,
            &mut vec![*referenced.as_hash_array()],
        )
        .expect("stage payload refs");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit Arrow state");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read");
        let manifest = crate::columnar_row_group::load_row_group_manifest(&read, state_set_id)
            .await
            .expect("load manifest")
            .expect("manifest exists");
        let projection = current_state_data_projection(&manifest).expect("canonical projection");
        let mut decoded = Vec::new();
        for group_index in 0..manifest.groups.len() {
            let batch = crate::columnar_row_group::load_row_group_batch(
                &read,
                state_set_id,
                &manifest,
                group_index,
                &projection,
            )
            .await
            .expect("load Arrow group");
            let loaded = LoadedRowGroupSet {
                manifest: manifest.clone(),
                batches: vec![batch],
            };
            decoded.extend(
                decode_current_state_data_part(&loaded, group_index as u32)
                    .expect("decode Arrow group"),
            );
        }
        assert_eq!(decoded, rows);
        assert_ne!(refs_digest, [0; 32]);
    }

    #[test]
    fn arrow_leaves_retain_tombstones_and_reject_unordered_rows() {
        let mut rows = vec![row(1), row(0)];
        let scope = crate::tracked_state::types::CommitDeltaReplacementScope {
            schema_key: "native-test".to_owned(),
            file_id: None,
        };
        let encode = |rows: &[CurrentStateDataRow]| {
            let inputs = rows
                .iter()
                .map(|row| ArrowStateInputRowRef {
                    encoded_key: &row.encoded_key,
                    value: TrackedStateIndexValueRef {
                        change_id: row.value.change_id,
                        commit_id: row.value.commit_id,
                        deleted: row.value.deleted,
                        created_at: row.value.created_at,
                        updated_at: row.value.updated_at,
                    },
                    snapshot: row.snapshot.as_ref_slot(),
                    metadata: row.metadata.as_ref_slot(),
                })
                .collect::<Vec<_>>();
            encode_authoritative_arrow_state_rows(&scope, &inputs)
        };
        assert!(encode(&rows).is_err());
        rows.sort_by(|left, right| left.encoded_key.cmp(&right.encoded_key));
        rows[0].value.deleted = true;
        rows[0].snapshot = JsonSlot::None;
        assert!(encode(&rows).is_ok());
    }

    #[tokio::test]
    async fn sparse_arrow_splice_preserves_typed_columns_without_row_lowering() {
        fn source(commit_id: CommitId, keys: &[&[u8]], scores: &[i64]) -> LoadedRowGroupSet {
            let fields = vec![
                Field::new("physical_key", DataType::Binary, false),
                Field::new("change_id", DataType::Binary, false),
                Field::new("deleted", DataType::Boolean, false),
                Field::new("snapshot_kind", DataType::Int64, false),
                Field::new("snapshot_payload", DataType::Binary, true),
                Field::new("metadata_kind", DataType::Int64, false),
                Field::new("metadata_payload", DataType::Binary, true),
                Field::new("score", DataType::Int64, true),
            ];
            let metadata = HashMap::from([
                (
                    "lix.layout".to_owned(),
                    ENTITY_ARROW_STATE_LAYOUT.to_owned(),
                ),
                ("lix.order".to_owned(), "physical_key-ascending".to_owned()),
                (
                    ENTITY_ARROW_STATE_SCHEMA_KEY_METADATA.to_owned(),
                    "typed_splice".to_owned(),
                ),
                (
                    ENTITY_ARROW_STATE_COMMIT_ID_METADATA.to_owned(),
                    commit_id.to_string(),
                ),
                (
                    ENTITY_ARROW_STATE_CREATED_AT_METADATA.to_owned(),
                    "1".to_owned(),
                ),
                (
                    ENTITY_ARROW_STATE_UPDATED_AT_METADATA.to_owned(),
                    "2".to_owned(),
                ),
            ]);
            let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
            let snapshots = scores
                .iter()
                .map(|score| Some(format!(r#"{{"score":{score}}}"#).into_bytes()))
                .collect::<Vec<_>>();
            let change_ids = (0..keys.len())
                .map(|index| {
                    *ChangeId::for_test_label(&format!("typed-source-{commit_id}-{index}"))
                        .as_uuid()
                        .as_bytes()
                })
                .collect::<Vec<_>>();
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(BinaryArray::from_iter_values(keys.iter().copied())),
                    Arc::new(BinaryArray::from_iter_values(
                        change_ids.iter().map(<[u8; 16]>::as_slice),
                    )),
                    Arc::new(BooleanArray::from_iter(std::iter::repeat_n(
                        Some(false),
                        keys.len(),
                    ))),
                    Arc::new(Int64Array::from_iter_values(std::iter::repeat_n(
                        1,
                        keys.len(),
                    ))),
                    Arc::new(BinaryArray::from_iter(
                        snapshots.iter().map(|value| value.as_deref()),
                    )),
                    Arc::new(Int64Array::from_iter_values(std::iter::repeat_n(
                        0,
                        keys.len(),
                    ))),
                    Arc::new(BinaryArray::from_iter(std::iter::repeat_n(
                        None::<&[u8]>,
                        keys.len(),
                    ))),
                    Arc::new(Int64Array::from_iter_values(scores.iter().copied())),
                ],
            )
            .expect("source batch");
            let encoded = encode_row_group_set_preserving_batches(
                LEAF_NAMESPACE,
                schema,
                std::slice::from_ref(&batch),
            )
            .expect("source manifest");
            LoadedRowGroupSet {
                manifest: encoded.manifest,
                batches: vec![batch],
            }
        }

        let parent_commit = CommitId::for_test_label("typed-splice-parent");
        let update_commit = CommitId::for_test_label("typed-splice-update");
        let sources = vec![
            source(parent_commit, &[b"key-a", b"key-b"], &[1, 2]),
            source(update_commit, &[b"key-a"], &[10]),
        ];
        let created_at = LixTimestamp::from_unix_millis_utc_lossy(10);
        let updated_at = LixTimestamp::from_unix_millis_utc_lossy(20);
        let selections = vec![
            ArrowStateRowSelection {
                encoded_key: b"key-a".to_vec(),
                value: TrackedStateIndexValue {
                    change_id: ChangeId::for_test_label("typed-splice-update-a"),
                    commit_id: update_commit,
                    deleted: false,
                    created_at,
                    updated_at,
                },
                source: Some((1, 0, 0)),
                retain_payload: true,
            },
            ArrowStateRowSelection {
                encoded_key: b"key-b".to_vec(),
                value: TrackedStateIndexValue {
                    change_id: ChangeId::for_test_label("typed-splice-parent-b"),
                    commit_id: parent_commit,
                    deleted: false,
                    created_at,
                    updated_at,
                },
                source: Some((0, 0, 1)),
                retain_payload: true,
            },
        ];
        let parts = encode_bounded_selected_current_state_data_parts(&sources, &selections)
            .expect("typed sparse splice");
        assert_eq!(parts.len(), 1);
        assert!(parts[0].encoded.manifest.fields.iter().any(|field| {
            field.name == "score" && field.data_type.to_arrow() == DataType::Int64
        }));
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
        let state_set_id = parts[0].stage(&mut writes).expect("stage spliced leaf");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit spliced leaf");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read spliced leaf");
        let score_index = parts[0]
            .encoded
            .manifest
            .fields
            .iter()
            .position(|field| field.name == "score")
            .expect("typed score column");
        let manifest = crate::columnar_row_group::load_row_group_manifest(&read, state_set_id)
            .await
            .expect("load spliced leaf")
            .expect("spliced leaf exists");
        let loaded = crate::columnar_row_group::load_row_group_batch(
            &read,
            state_set_id,
            &manifest,
            0,
            &[score_index],
        )
        .await
        .expect("load projected spliced leaf");
        let scores = loaded
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("score type");
        assert_eq!((scores.value(0), scores.value(1)), (10, 2));
    }
}
