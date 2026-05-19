use crate::changelog::{
    ChangeLoadEntry, ChangeLoadRequest, ChangeProjection, ChangeVisibilityMode, ChangelogContext,
    SegmentInlinePayload,
};
use crate::entity_identity::EntityIdentity;
use crate::json_store::JsonRef;
use crate::json_store::{JsonLoadRequestRef, JsonReadScopeRef, JsonStoreContext};
use crate::storage::StorageRead;
use crate::tracked_state::types::{TrackedStateIndexValue, TrackedStateKey};
use crate::tracked_state::MaterializedTrackedStateRow;
use crate::LixError;
use std::collections::BTreeMap;

/// Materializes tracked-state index entries.
///
/// The durable tracked_state value is authoritative for scalar projection
/// fields and stores the JSON refs needed for payload projections. Snapshot and
/// metadata bytes are hydrated from grouped json_store loads only when the
/// requested projection needs them.
pub(crate) async fn materialize_rows_from_index_entries<S>(
    store: &S,
    entries: Vec<(TrackedStateKey, TrackedStateIndexValue)>,
    projection: &TrackedRowProjection,
) -> Result<Vec<MaterializedTrackedStateRow>, LixError>
where
    S: StorageRead + Send + Sync,
{
    if !projection.snapshot_content && !projection.metadata {
        return Ok(entries
            .into_iter()
            .map(materialize_entry_without_json)
            .collect());
    }

    let json_slots_per_row =
        usize::from(projection.snapshot_content) + usize::from(projection.metadata);
    let json_ref_capacity = entries.len().saturating_mul(json_slots_per_row);
    let mut row_plans = Vec::with_capacity(entries.len());
    let mut json_refs = Vec::with_capacity(json_ref_capacity);
    let mut json_ref_localities = Vec::with_capacity(json_ref_capacity);
    for (key, value) in entries {
        let row_index = row_plans.len();
        let snapshot_ref_index = projected_json_ref_index(
            projection.snapshot_content,
            value.snapshot_ref,
            row_index,
            &mut json_refs,
            &mut json_ref_localities,
        );
        let metadata_ref_index = projected_json_ref_index(
            projection.metadata,
            value.metadata_ref,
            row_index,
            &mut json_refs,
            &mut json_ref_localities,
        );
        row_plans.push(TrackedRowMaterializationPlan {
            entity_id: key.entity_id,
            schema_key: key.schema_key,
            file_id: key.file_id,
            deleted: value.deleted,
            created_at: value.created_at,
            updated_at: value.updated_at,
            change_id: value.change_locator.change_id,
            commit_id: value.change_locator.commit_id,
            snapshot_ref_index,
            metadata_ref_index,
        });
    }

    let mut json_values =
        load_projection_json_values(store, &json_refs, &json_ref_localities, &row_plans).await?;
    row_plans
        .into_iter()
        .map(|plan| materialize_row_plan(plan, &json_refs, &mut json_values))
        .collect()
}

fn materialize_entry_without_json(
    (key, value): (TrackedStateKey, TrackedStateIndexValue),
) -> MaterializedTrackedStateRow {
    MaterializedTrackedStateRow {
        entity_id: key.entity_id,
        schema_key: key.schema_key,
        file_id: key.file_id,
        snapshot_content: None,
        metadata: None,
        deleted: value.deleted,
        created_at: value.created_at,
        updated_at: value.updated_at,
        change_id: value.change_locator.change_id,
        commit_id: value.change_locator.commit_id,
    }
}

struct TrackedRowMaterializationPlan {
    entity_id: EntityIdentity,
    schema_key: String,
    file_id: Option<String>,
    deleted: bool,
    created_at: String,
    updated_at: String,
    change_id: String,
    commit_id: String,
    snapshot_ref_index: Option<usize>,
    metadata_ref_index: Option<usize>,
}

fn projected_json_ref_index(
    include: bool,
    json_ref: Option<JsonRef>,
    row_index: usize,
    json_refs: &mut Vec<JsonRef>,
    json_ref_localities: &mut Vec<JsonRefLocality>,
) -> Option<usize> {
    if !include {
        return None;
    }
    let index = json_refs.len();
    json_refs.push(json_ref?);
    json_ref_localities.push(JsonRefLocality { row_index });
    Some(index)
}

struct JsonRefLocality {
    row_index: usize,
}

async fn load_projection_json_values<S>(
    store: &S,
    json_refs: &[JsonRef],
    json_ref_localities: &[JsonRefLocality],
    row_plans: &[TrackedRowMaterializationPlan],
) -> Result<Vec<Option<Vec<u8>>>, LixError>
where
    S: StorageRead + Send + Sync,
{
    if json_refs.len() != json_ref_localities.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state materialization JSON refs and locality indexes diverged",
        ));
    }

    let mut json_values = vec![None; json_refs.len()];
    let mut change_ids = Vec::new();
    for index in 0..json_refs.len() {
        let locality = json_ref_localities.get(index).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state materialization lost JSON locality index",
            )
        })?;
        let row_plan = row_plans.get(locality.row_index).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state materialization lost JSON row locality index",
            )
        })?;
        if !change_ids.contains(&row_plan.change_id) {
            change_ids.push(row_plan.change_id.clone());
        }
    }

    let mut inline_payloads_by_change = BTreeMap::<String, Vec<SegmentInlinePayload>>::new();
    if !change_ids.is_empty() {
        let mut changelog_reader = ChangelogContext::new().reader(store);
        let changes = changelog_reader
            .load_changes(ChangeLoadRequest {
                change_ids: &change_ids,
                projection: ChangeProjection::Segment,
                visibility: ChangeVisibilityMode::RequireReachableFromVisibleCommit,
            })
            .await?;
        for (change_id, entry) in change_ids.into_iter().zip(changes.entries) {
            let Some(entry) = entry else {
                continue;
            };
            let ChangeLoadEntry::Segment(change) = entry else {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked_state materialization segment projection returned non-segment entry",
                ));
            };
            inline_payloads_by_change.insert(change_id, change.inline_payloads);
        }
    }

    let mut out_of_band_indexes = Vec::new();
    let mut out_of_band_refs = Vec::new();
    for (index, json_ref) in json_refs.iter().copied().enumerate() {
        let locality = json_ref_localities.get(index).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state materialization lost JSON locality index",
            )
        })?;
        let row_plan = row_plans.get(locality.row_index).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state materialization lost JSON row locality index",
            )
        })?;
        if let Some(bytes) = inline_payloads_by_change
            .get(row_plan.change_id.as_str())
            .and_then(|payloads| {
                payloads
                    .iter()
                    .find(|payload| payload.json_ref == json_ref)
                    .map(|payload| &payload.bytes)
            })
        {
            json_values[index] = Some(bytes.clone());
        } else {
            out_of_band_indexes.push(index);
            out_of_band_refs.push(json_ref);
        }
    }

    if !out_of_band_refs.is_empty() {
        let values = JsonStoreContext::new()
            .load_bytes_many(
                store,
                JsonLoadRequestRef {
                    refs: &out_of_band_refs,
                    scope: JsonReadScopeRef::OutOfBand,
                },
            )
            .await?
            .into_values();
        for (index, value) in out_of_band_indexes.into_iter().zip(values) {
            if value.is_some() {
                json_values[index] = value;
            }
        }
    }

    Ok(json_values)
}

fn materialize_row_plan(
    plan: TrackedRowMaterializationPlan,
    json_refs: &[JsonRef],
    json_values: &mut [Option<Vec<u8>>],
) -> Result<MaterializedTrackedStateRow, LixError> {
    Ok(MaterializedTrackedStateRow {
        entity_id: plan.entity_id,
        schema_key: plan.schema_key,
        file_id: plan.file_id,
        snapshot_content: materialized_json_string(
            plan.snapshot_ref_index,
            json_refs,
            json_values,
        )?,
        metadata: materialized_json_string(plan.metadata_ref_index, json_refs, json_values)?,
        deleted: plan.deleted,
        created_at: plan.created_at,
        updated_at: plan.updated_at,
        change_id: plan.change_id,
        commit_id: plan.commit_id,
    })
}

fn materialized_json_string(
    index: Option<usize>,
    json_refs: &[JsonRef],
    json_values: &mut [Option<Vec<u8>>],
) -> Result<Option<String>, LixError> {
    let Some(index) = index else {
        return Ok(None);
    };
    let json_ref = json_refs.get(index).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state materialization lost JSON ref index",
        )
    })?;
    // Each row plan owns its projected JSON slots. If this path starts
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
pub(crate) struct TrackedRowProjection {
    pub(crate) snapshot_content: bool,
    pub(crate) metadata: bool,
}

impl TrackedRowProjection {
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

        let materialized = materialized_json_string(Some(0), &[json_ref], &mut json_values)
            .expect("json should materialize");

        assert_eq!(materialized, Some(r#"{"value":1}"#.to_string()));
        assert!(json_values[0].is_none());
    }
}
