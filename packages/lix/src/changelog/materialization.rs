use std::collections::{HashMap, HashSet};

use bytes::Bytes;

use crate::LixError;
use crate::common::{ExactBatch, SharedStr};
use crate::json_store::{
    JsonLoadRequestRef, JsonReadScopeRef, JsonRef, JsonSlot, JsonStoreContext,
};
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageGetOptions, StorageProjectedValue,
};

use super::{CHANGE_SPACE, ChangeId, ChangeRecord, decode_change_record};

const CHANGE_STORAGE_KEY_BYTES: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChangeRecordProjection {
    pub(crate) snapshot_content: bool,
    pub(crate) metadata: bool,
}

impl ChangeRecordProjection {
    pub(crate) fn full() -> Self {
        Self {
            snapshot_content: true,
            metadata: true,
        }
    }

    /// Loads identity and revision columns without hydrating JSON payloads.
    pub(crate) fn identity_only() -> Self {
        Self {
            snapshot_content: false,
            metadata: false,
        }
    }

    pub(crate) fn from_columns(columns: &[String]) -> Self {
        if columns.is_empty() {
            return Self::full();
        }
        Self {
            snapshot_content: columns.iter().any(|column| column == "snapshot_content"),
            metadata: columns.iter().any(|column| column == "metadata"),
        }
    }

    pub(crate) fn requires_payload(self) -> bool {
        self.snapshot_content || self.metadata
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaterializedChangePayload {
    pub(crate) identity: Option<MaterializedChangeIdentity>,
    pub(crate) snapshot_content: Option<SharedStr>,
    pub(crate) metadata: Option<SharedStr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaterializedChangeIdentity {
    pub(crate) schema_key: String,
    pub(crate) entity_pk: crate::entity_pk::EntityPk,
    pub(crate) file_id: Option<String>,
}

/// Batched point read of change records by deduplicated change id.
pub(crate) async fn load_change_records<S>(
    store: &S,
    change_ids: impl Iterator<Item = ChangeId>,
) -> Result<HashMap<ChangeId, ChangeRecord>, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let mut unique = Vec::new();
    let mut seen = HashSet::new();
    for change_id in change_ids {
        if seen.insert(change_id) {
            unique.push(change_id);
        }
    }
    if unique.is_empty() {
        return Ok(HashMap::new());
    }
    let records = load_unique_change_records_in_order(store, &unique).await?;
    let mut by_id = HashMap::with_capacity(unique.len());
    for (change_id, record) in records {
        if let Some(record) = record {
            by_id.insert(*change_id, record);
        }
    }
    Ok(by_id)
}

/// Loads a caller-deduplicated change-id batch.
///
/// All changelog keys are fixed-width UUID bytes, so one immutable arena can
/// back every point-read key. The plan also receives the keys as already
/// unique, avoiding a second hash table, key vector, caller-order remap, and
/// materialized value vector for the common identity mapping. Decoded records
/// retain that same order so batch materialization does not need an
/// intermediate `HashMap`.
async fn load_unique_change_records_in_order<'a, S>(
    store: &S,
    unique: &'a [ChangeId],
) -> Result<ExactBatch<'a, ChangeId, ChangeRecord>, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    if unique.is_empty() {
        return ExactBatch::try_new("change materialization", unique, Vec::new());
    }
    let keys = change_storage_keys(unique)?;
    let plan = PointReadPlan::from_unique_keys(CHANGE_SPACE, keys);
    let result = plan.collect(store, StorageGetOptions::default()).await?;
    decode_change_records_in_order(unique, result.value.unique_values)
}

fn decode_change_records_in_order(
    unique: &[ChangeId],
    values: Vec<Option<StorageProjectedValue>>,
) -> Result<ExactBatch<'_, ChangeId, ChangeRecord>, LixError> {
    let records = unique
        .iter()
        .copied()
        .zip(values)
        .map(|(change_id, value)| match value {
            None => Ok(None),
            Some(StorageProjectedValue::FullValue(bytes)) => {
                decode_change_record(&bytes, change_id).map(Some)
            }
            Some(StorageProjectedValue::KeyOnly) => Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "change point read returned a key-only projection for ChangeRecord '{change_id}'"
                ),
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;
    ExactBatch::try_new("change materialization", unique, records)
}

fn change_storage_keys(
    unique: &[ChangeId],
) -> Result<Vec<crate::storage_adapter::StorageKey>, LixError> {
    let arena_len = unique
        .len()
        .checked_mul(CHANGE_STORAGE_KEY_BYTES)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "change point-read key arena size overflowed",
            )
        })?;
    let mut arena = Vec::with_capacity(arena_len);
    for change_id in unique {
        arena.extend_from_slice(change_id.as_uuid().as_bytes());
    }
    debug_assert_eq!(arena.len(), arena_len);
    let arena = Bytes::from(arena);
    Ok((0..unique.len())
        .map(|index| {
            let start = index * CHANGE_STORAGE_KEY_BYTES;
            crate::storage_adapter::StorageKey(arena.slice(start..start + CHANGE_STORAGE_KEY_BYTES))
        })
        .collect())
}

/// Hydrates records that a caller already retained from their authoritative
/// storage owner. Packed commit deltas use this path so lifecycle and HOT
/// publication never discard commit-local payloads and fall back to the
/// standalone changelog namespace.
pub(crate) async fn materialize_known_change_payloads<S>(
    store: &S,
    changes: impl Iterator<Item = ChangeRecord>,
    projection: ChangeRecordProjection,
) -> Result<HashMap<ChangeId, MaterializedChangePayload>, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    Ok(
        materialize_known_change_payloads_in_order(store, changes, projection)
            .await?
            .into_iter()
            .collect(),
    )
}

/// Hydrates retained change records without collapsing repeated change ids.
///
/// Eager plugin materialization can produce multiple semantic identities from
/// one source change. Lifecycle operations such as checkpoints must preserve
/// each identity-specific payload even though those rows share a change id.
pub(crate) async fn materialize_known_change_payloads_in_order<S>(
    store: &S,
    changes: impl Iterator<Item = ChangeRecord>,
    projection: ChangeRecordProjection,
) -> Result<Vec<(ChangeId, MaterializedChangePayload)>, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let changes = changes.collect::<Vec<_>>();
    if !projection.requires_payload() {
        return Ok(changes
            .into_iter()
            .map(|change| {
                (
                    change.change_id,
                    MaterializedChangePayload {
                        identity: None,
                        snapshot_content: None,
                        metadata: None,
                    },
                )
            })
            .collect());
    }

    let mut json_refs = Vec::new();
    let mut plans = Vec::with_capacity(changes.len());
    for mut change in changes {
        let change_id = change.change_id;
        if projection.snapshot_content {
            match (change.snapshot.is_some(), change.typed_snapshot.take()) {
                (true, Some(_)) => {
                    return Err(LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        "change member has duplicate JSON and typed snapshot authorities",
                    ));
                }
                (false, Some(snapshot)) if !snapshot.deleted => {
                    change.snapshot = JsonSlot::Inline(
                        materialize_typed_snapshot_json(&change.entity_pk, &snapshot)?
                            .into_boxed_str(),
                    );
                }
                (false, Some(_)) | (_, None) => {}
            }
        }
        plans.push((
            change_id,
            MaterializedChangeIdentity {
                schema_key: change.schema_key,
                entity_pk: change.entity_pk,
                file_id: change.file_id,
            },
            materialized_json_slot(projection.snapshot_content, change.snapshot, &mut json_refs),
            materialized_json_slot(projection.metadata, change.metadata, &mut json_refs),
        ));
    }

    let mut json_values = load_json_values(store, &json_refs).await?;
    plans
        .into_iter()
        .map(|(change_id, identity, snapshot, metadata)| {
            Ok((
                change_id,
                MaterializedChangePayload {
                    identity: Some(identity),
                    snapshot_content: materialized_json_string(
                        snapshot,
                        &json_refs,
                        &mut json_values,
                    )?,
                    metadata: materialized_json_string(metadata, &json_refs, &mut json_values)?,
                },
            ))
        })
        .collect()
}

/// Terminal current-state adapter for lifecycle callers that still consume
/// JSON objects. Durable history remains the schema-bound typed tuple; this
/// conversion is never stored and public entity history projects typed cells
/// directly.
fn materialize_typed_snapshot_json(
    entity_pk: &crate::entity_pk::EntityPk,
    snapshot: &super::TypedHistorySnapshot,
) -> Result<String, LixError> {
    let pk_values = entity_pk.as_json_array_value()?;
    let serde_json::Value::Array(pk_values) = pk_values else {
        unreachable!("EntityPk always projects as a JSON array")
    };
    if snapshot.primary_key_paths.len() != pk_values.len() {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "typed history primary-key layout disagrees with its authenticated member key",
        ));
    }
    let mut object = serde_json::Map::new();
    for (path, value) in snapshot.primary_key_paths.iter().zip(pk_values) {
        insert_typed_json_path(&mut object, path, value)?;
    }
    for field in &snapshot.fields {
        let value = match &field.value {
            None => serde_json::Value::Null,
            Some(super::TypedHistoryScalar::String(value)) => {
                serde_json::Value::String(value.clone())
            }
            Some(super::TypedHistoryScalar::Jsonb(value)) => serde_json::from_str(value)
                .map_err(|error| {
                    LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        format!("typed history jsonb cell is malformed: {error}"),
                    )
                })?,
            Some(super::TypedHistoryScalar::Int64(value)) => (*value).into(),
            Some(super::TypedHistoryScalar::Float64Bits(value)) => {
                serde_json::Number::from_f64(f64::from_bits(*value))
                    .map(serde_json::Value::Number)
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_STORAGE_ERROR,
                            "typed history float8 cell is not finite JSON",
                        )
                    })?
            }
            Some(super::TypedHistoryScalar::Boolean(value)) => (*value).into(),
            Some(super::TypedHistoryScalar::TimestampMicros(value)) => {
                let timestamp = chrono::DateTime::from_timestamp_micros(*value).ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        "typed history timestamptz cell is out of range",
                    )
                })?;
                serde_json::Value::String(timestamp.to_rfc3339_opts(
                    chrono::SecondsFormat::Micros,
                    true,
                ))
            }
        };
        if object.insert(field.name.clone(), value).is_some() {
            return Err(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "typed history payload duplicates a primary-key root",
            ));
        }
    }
    serde_json::to_string(&object).map_err(|error| {
        LixError::new(
            LixError::CODE_STORAGE_ERROR,
            format!("typed current-state terminal projection failed: {error}"),
        )
    })
}

fn insert_typed_json_path(
    object: &mut serde_json::Map<String, serde_json::Value>,
    path: &[String],
    value: serde_json::Value,
) -> Result<(), LixError> {
    let Some((first, rest)) = path.split_first() else {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "typed history primary-key path is empty",
        ));
    };
    if rest.is_empty() {
        if object.insert(first.clone(), value).is_some() {
            return Err(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "typed history primary-key paths overlap",
            ));
        }
        return Ok(());
    }
    let entry = object
        .entry(first.clone())
        .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
    let serde_json::Value::Object(child) = entry else {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "typed history primary-key paths overlap",
        ));
    };
    insert_typed_json_path(child, rest, value)
}

enum MaterializedJsonSlot {
    None,
    Inline(Box<str>),
    Loaded(usize),
}

fn materialized_json_slot(
    include: bool,
    slot: JsonSlot,
    json_refs: &mut Vec<JsonRef>,
) -> MaterializedJsonSlot {
    if !include {
        return MaterializedJsonSlot::None;
    }
    match slot {
        JsonSlot::None => MaterializedJsonSlot::None,
        JsonSlot::Inline(json) => MaterializedJsonSlot::Inline(json),
        JsonSlot::Ref(json_ref) => {
            let index = json_refs.len();
            json_refs.push(json_ref);
            MaterializedJsonSlot::Loaded(index)
        }
    }
}

async fn load_json_values<S>(
    store: &S,
    json_refs: &[JsonRef],
) -> Result<Vec<Option<Bytes>>, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    if json_refs.is_empty() {
        return Ok(Vec::new());
    }
    Ok(JsonStoreContext::new()
        .load_bytes_many(
            store,
            JsonLoadRequestRef {
                refs: json_refs,
                scope: JsonReadScopeRef::OutOfBand,
            },
        )
        .await?
        .into_values())
}

fn materialized_json_string(
    slot: MaterializedJsonSlot,
    json_refs: &[JsonRef],
    json_values: &mut [Option<Bytes>],
) -> Result<Option<SharedStr>, LixError> {
    let index = match slot {
        MaterializedJsonSlot::None => return Ok(None),
        MaterializedJsonSlot::Inline(json) => {
            return Ok(Some(SharedStr::from(json.into_string())));
        }
        MaterializedJsonSlot::Loaded(index) => index,
    };
    let json_ref = json_refs.get(index).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "change materialization lost JSON ref index",
        )
    })?;
    let bytes = json_values
        .get_mut(index)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "change materialization lost JSON value index",
            )
        })?
        .take()
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "change materialization is missing JSON payload '{}'",
                    json_ref.to_hex()
                ),
            )
        })?;
    SharedStr::from_utf8(bytes).map(Some).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("materialized ChangeRecord JSON payload is not UTF-8: {error}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::test_support::test_change_record;
    use crate::storage_adapter::{
        Memory, RequestedToUniqueRef, StorageAdapter, StorageReadOptions,
    };

    fn encoded_change(schema_key: &str) -> Bytes {
        let mut record = test_change_record();
        record.schema_key = schema_key.to_owned();
        Bytes::from(
            crate::changelog::codec::encode_change_record(&record)
                .expect("change record should encode"),
        )
    }

    #[test]
    fn change_storage_keys_share_one_contiguous_arena() {
        let change_ids = [
            ChangeId::new(uuid::Uuid::from_bytes([1; CHANGE_STORAGE_KEY_BYTES])),
            ChangeId::new(uuid::Uuid::from_bytes([2; CHANGE_STORAGE_KEY_BYTES])),
            ChangeId::new(uuid::Uuid::from_bytes([3; CHANGE_STORAGE_KEY_BYTES])),
        ];

        let keys = change_storage_keys(&change_ids).expect("change keys");
        let arena_start = keys[0].0.as_ptr() as usize;
        for (index, (change_id, key)) in change_ids.iter().zip(&keys).enumerate() {
            assert_eq!(key.0.as_ref(), change_id.as_uuid().as_bytes());
            assert_eq!(
                key.0.as_ptr() as usize,
                arena_start + index * CHANGE_STORAGE_KEY_BYTES,
                "key {index} must be a contiguous slice of the batch arena"
            );
        }

        let plan = PointReadPlan::from_unique_keys(CHANGE_SPACE, keys);
        assert_eq!(
            plan.requested_to_unique(),
            RequestedToUniqueRef::Identity {
                len: change_ids.len()
            }
        );
    }

    #[test]
    fn decoded_change_records_preserve_unique_id_order_and_missing_slots() {
        let change_ids = [
            ChangeId::for_test_label("ordered-a"),
            ChangeId::for_test_label("ordered-missing"),
            ChangeId::for_test_label("ordered-c"),
        ];
        let records = decode_change_records_in_order(
            &change_ids,
            vec![
                Some(StorageProjectedValue::FullValue(encoded_change("alpha"))),
                None,
                Some(StorageProjectedValue::FullValue(encoded_change("charlie"))),
            ],
        )
        .expect("ordered change records should decode");
        let records = records
            .into_iter()
            .map(|(_, record)| record)
            .collect::<Vec<_>>();

        assert_eq!(records.len(), change_ids.len());
        let first = records[0].as_ref().expect("first record");
        assert_eq!(first.change_id, change_ids[0]);
        assert_eq!(first.schema_key, "alpha");
        assert!(records[1].is_none());
        let third = records[2].as_ref().expect("third record");
        assert_eq!(third.change_id, change_ids[2]);
        assert_eq!(third.schema_key, "charlie");
    }

    #[test]
    fn decoded_change_records_reject_storage_cardinality_mismatch() {
        let change_ids = [
            ChangeId::for_test_label("cardinality-a"),
            ChangeId::for_test_label("cardinality-b"),
        ];
        let error = decode_change_records_in_order(&change_ids, vec![None])
            .expect_err("short storage response must fail");

        assert!(
            error
                .to_string()
                .contains("returned 1 values for 2 requested keys")
        );
    }

    #[tokio::test]
    async fn ordered_materialization_preserves_repeated_change_ids() {
        let storage = StorageAdapter::new(Memory::new());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let first = test_change_record();
        let mut second = first.clone();
        second.schema_key = "derived-row".to_owned();
        second.entity_pk = crate::entity_pk::EntityPk::single("entity-2");

        let payloads = materialize_known_change_payloads_in_order(
            &read,
            [first.clone(), second.clone()].into_iter(),
            ChangeRecordProjection::full(),
        )
        .await
        .expect("repeated source ids should materialize");

        assert_eq!(payloads.len(), 2);
        assert_eq!(payloads[0].0, first.change_id);
        assert_eq!(payloads[1].0, first.change_id);
        assert_eq!(
            payloads[0]
                .1
                .identity
                .as_ref()
                .expect("first identity")
                .schema_key,
            first.schema_key
        );
        assert_eq!(
            payloads[1]
                .1
                .identity
                .as_ref()
                .expect("second identity")
                .schema_key,
            second.schema_key
        );
    }

    #[test]
    fn materialized_json_string_consumes_owned_payload_bytes() {
        let json = Bytes::from_static(br#"{"value":1}"#);
        let json_ref = JsonRef::for_content(&json);
        let source_ptr = json.as_ptr();
        let mut json_values = vec![Some(json)];

        let materialized = materialized_json_string(
            MaterializedJsonSlot::Loaded(0),
            &[json_ref],
            &mut json_values,
        )
        .expect("json should materialize");

        let materialized = materialized.expect("materialized JSON");
        assert_eq!(materialized, r#"{"value":1}"#);
        assert_eq!(
            materialized.as_bytes().as_ptr(),
            source_ptr,
            "materialization must retain the JSON-store buffer"
        );
        assert!(json_values[0].is_none());
    }
}
