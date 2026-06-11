use std::collections::HashMap;

use crate::LixError;
use crate::changelog::{
    CHANGE_SPACE, ChangeId, ChangeRecord, CommitId, change_key, decode_change_record,
};
use crate::common::LixTimestamp;
use crate::entity_pk::EntityPk;
use crate::json_store::JsonRef;
use crate::json_store::{JsonLoadRequestRef, JsonReadScopeRef, JsonSlot, JsonStoreContext};
use crate::storage::StorageRead;
use crate::tracked_state::MaterializedTrackedStateRow;
use crate::tracked_state::types::{TrackedStateIndexValue, TrackedStateKey};

/// Materializes tracked-state index entries.
///
/// The durable tracked_state value carries only identity (change id, commit
/// id, flags, timestamps); payloads live once, in the change record the row
/// already references. Hydration is a batched changelog point read per
/// distinct change id, then json_store loads for the rare large payloads.
/// The GC contract makes this sound: a change record is only deletable when
/// no tracked-state row references its change id.
pub(crate) async fn materialize_rows_from_index_entries<S>(
    store: &S,
    entries: Vec<(TrackedStateKey, TrackedStateIndexValue)>,
    materialization: &TrackedRowMaterialization,
) -> Result<Vec<MaterializedTrackedStateRow>, LixError>
where
    S: StorageRead + Send + Sync,
{
    if !materialization.snapshot_content && !materialization.metadata {
        return Ok(entries
            .into_iter()
            .map(materialize_entry_without_json)
            .collect());
    }

    let changes = load_change_records(
        store,
        entries
            .iter()
            .filter(|(key, value)| !value.deleted && key.schema_key != COMMIT_SCHEMA_KEY)
            .map(|(_, value)| value.change_id),
    )
    .await?;

    let mut row_plans = Vec::with_capacity(entries.len());
    let mut json_refs = Vec::new();
    for (key, value) in entries {
        let (snapshot_slot, metadata_slot) = if value.deleted {
            (MaterializedJsonSlot::None, MaterializedJsonSlot::None)
        } else if key.schema_key == COMMIT_SCHEMA_KEY {
            // Commit rows have no change record; their snapshot is fully
            // derivable from the key (the entity pk is the commit id).
            (
                if materialization.snapshot_content {
                    MaterializedJsonSlot::Inline(
                        commit_row_snapshot_json(&key.entity_pk)?.into_boxed_str(),
                    )
                } else {
                    MaterializedJsonSlot::None
                },
                MaterializedJsonSlot::None,
            )
        } else {
            let change = changes.get(&value.change_id).ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "tracked-state row references change '{}' that is missing from the changelog",
                        value.change_id
                    ),
                )
            })?;
            (
                materialized_json_slot(
                    materialization.snapshot_content,
                    change.snapshot.clone(),
                    &mut json_refs,
                ),
                materialized_json_slot(
                    materialization.metadata,
                    change.metadata.clone(),
                    &mut json_refs,
                ),
            )
        };
        row_plans.push(TrackedRowMaterializationPlan {
            entity_pk: key.entity_pk,
            schema_key: key.schema_key,
            file_id: key.file_id,
            deleted: value.deleted,
            created_at: value.created_at(),
            updated_at: value.updated_at(),
            change_id: value.change_id,
            commit_id: value.commit_id,
            snapshot_slot,
            metadata_slot,
        });
    }

    let mut json_values = load_materialization_json_values(store, &json_refs).await?;
    row_plans
        .into_iter()
        .map(|plan| materialize_row_plan(plan, &json_refs, &mut json_values))
        .collect()
}

const COMMIT_SCHEMA_KEY: &str = "lix_commit";

fn commit_row_snapshot_json(entity_pk: &EntityPk) -> Result<String, LixError> {
    let Some(commit_id) = entity_pk.parts.first() else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "lix_commit row has an empty entity pk",
        ));
    };
    crate::changelog::commit_row_snapshot_json(commit_id)
}

/// Batched point read of change records by deduplicated change id.
pub(crate) async fn load_change_records<S>(
    store: &S,
    change_ids: impl Iterator<Item = ChangeId>,
) -> Result<HashMap<ChangeId, ChangeRecord>, LixError>
where
    S: StorageRead + Send + Sync,
{
    let mut unique = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for change_id in change_ids {
        if seen.insert(change_id) {
            unique.push(change_id);
        }
    }
    let keys = unique
        .iter()
        .map(|change_id| crate::storage::StorageKey(bytes::Bytes::from(change_key(*change_id))))
        .collect::<Vec<_>>();
    let result = crate::storage::PointReadPlan::new(CHANGE_SPACE, &keys)
        .materialize(store, crate::storage::StorageGetOptions::default())?;
    let mut out = HashMap::with_capacity(unique.len());
    for (change_id, value) in unique.into_iter().zip(result.value) {
        if let Some(crate::storage::StorageProjectedValue::FullValue(bytes)) = value {
            out.insert(change_id, decode_change_record(&bytes, change_id)?);
        }
    }
    Ok(out)
}

fn materialize_entry_without_json(
    (key, value): (TrackedStateKey, TrackedStateIndexValue),
) -> MaterializedTrackedStateRow {
    MaterializedTrackedStateRow {
        entity_pk: key.entity_pk,
        schema_key: key.schema_key,
        file_id: key.file_id,
        snapshot_content: None,
        metadata: None,
        deleted: value.deleted,
        created_at: value.created_at().to_string(),
        updated_at: value.updated_at().to_string(),
        change_id: value.change_id,
        commit_id: value.commit_id,
    }
}

struct TrackedRowMaterializationPlan {
    entity_pk: EntityPk,
    schema_key: String,
    file_id: Option<String>,
    deleted: bool,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    change_id: ChangeId,
    commit_id: CommitId,
    snapshot_slot: MaterializedJsonSlot,
    metadata_slot: MaterializedJsonSlot,
}

/// Where a row's JSON payload comes from at materialization time. Inline
/// payloads travel inside the tree value and need no store read.
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

async fn load_materialization_json_values<S>(
    store: &S,
    json_refs: &[JsonRef],
) -> Result<Vec<Option<Vec<u8>>>, LixError>
where
    S: StorageRead + Send + Sync,
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

fn materialize_row_plan(
    plan: TrackedRowMaterializationPlan,
    json_refs: &[JsonRef],
    json_values: &mut [Option<Vec<u8>>],
) -> Result<MaterializedTrackedStateRow, LixError> {
    Ok(MaterializedTrackedStateRow {
        entity_pk: plan.entity_pk,
        schema_key: plan.schema_key,
        file_id: plan.file_id,
        snapshot_content: materialized_json_string(plan.snapshot_slot, json_refs, json_values)?,
        metadata: materialized_json_string(plan.metadata_slot, json_refs, json_values)?,
        deleted: plan.deleted,
        created_at: plan.created_at.to_string(),
        updated_at: plan.updated_at.to_string(),
        change_id: plan.change_id,
        commit_id: plan.commit_id,
    })
}

fn materialized_json_string(
    slot: MaterializedJsonSlot,
    json_refs: &[JsonRef],
    json_values: &mut [Option<Vec<u8>>],
) -> Result<Option<String>, LixError> {
    let index = match slot {
        MaterializedJsonSlot::None => return Ok(None),
        MaterializedJsonSlot::Inline(json) => return Ok(Some(json.into_string())),
        MaterializedJsonSlot::Loaded(index) => index,
    };
    let json_ref = json_refs.get(index).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state materialization lost JSON ref index",
        )
    })?;
    // Each row plan owns its materialized JSON slots. If this path starts
    // deduplicating refs, duplicate consumers must clone intentionally.
    let bytes = json_values
        .get_mut(index)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state materialization lost JSON value index",
            )
        })?
        .take()
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state materialization missing JSON payload '{}'",
                    json_ref.to_hex()
                ),
            )
        })?;
    String::from_utf8(bytes).map(Some).map_err(|error| {
        let utf8_error = error.utf8_error();
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("tracked_state materialized JSON payload is not UTF-8: {utf8_error}"),
        )
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TrackedRowMaterialization {
    pub(crate) snapshot_content: bool,
    pub(crate) metadata: bool,
}

impl TrackedRowMaterialization {
    pub(crate) fn full() -> Self {
        Self {
            snapshot_content: true,
            metadata: true,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn materialized_json_string_consumes_owned_payload_bytes() {
        let json = br#"{"value":1}"#.to_vec();
        let json_ref = JsonRef::for_content(&json);
        let mut json_values = vec![Some(json)];

        let materialized = materialized_json_string(
            MaterializedJsonSlot::Loaded(0),
            &[json_ref],
            &mut json_values,
        )
        .expect("json should materialize");

        assert_eq!(materialized, Some(r#"{"value":1}"#.to_string()));
        assert!(json_values[0].is_none());
    }
}
