use crate::entity_identity::EntityIdentity;
use crate::json_store::JsonRef;
use crate::json_store::{JsonLoadRequestRef, JsonReadScopeRef, JsonStoreContext};
use crate::storage::StorageReader;
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
pub(crate) async fn materialize_index_entries<S>(
    store: &mut S,
    entries: Vec<(TrackedStateKey, TrackedStateIndexValue)>,
    projection: &TrackedMaterializationProjection,
) -> Result<Vec<MaterializedTrackedStateRow>, LixError>
where
    S: StorageReader,
{
    if !projection.snapshot_content && !projection.metadata {
        return Ok(entries
            .into_iter()
            .map(materialize_entry_without_json)
            .collect());
    }

    let mut row_plans = Vec::with_capacity(entries.len());
    let mut json_refs = Vec::new();
    let mut json_ref_localities = Vec::new();
    for (key, value) in entries {
        let row_index = row_plans.len();
        let snapshot_ref_index = projected_json_ref_index(
            projection.snapshot_content,
            value.snapshot_ref,
            row_index,
            value.change_locator.source_pack_id,
            &mut json_refs,
            &mut json_ref_localities,
        );
        let metadata_ref_index = projected_json_ref_index(
            projection.metadata,
            value.metadata_ref,
            row_index,
            value.change_locator.source_pack_id,
            &mut json_refs,
            &mut json_ref_localities,
        );
        row_plans.push(MaterializedTrackedStateRowPlan {
            entity_id: key.entity_id,
            schema_key: key.schema_key,
            file_id: key.file_id,
            deleted: value.deleted,
            created_at: value.created_at,
            updated_at: value.updated_at,
            change_id: value.change_locator.change_id,
            commit_id: value.change_locator.source_commit_id,
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
        commit_id: value.change_locator.source_commit_id,
    }
}

struct MaterializedTrackedStateRowPlan {
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
    pack_id: u32,
    json_refs: &mut Vec<JsonRef>,
    json_ref_localities: &mut Vec<JsonRefLocality>,
) -> Option<usize> {
    if !include {
        return None;
    }
    let index = json_refs.len();
    json_refs.push(json_ref?);
    json_ref_localities.push(JsonRefLocality { row_index, pack_id });
    Some(index)
}

struct JsonRefLocality {
    row_index: usize,
    pack_id: u32,
}

async fn load_projection_json_values<S>(
    store: &mut S,
    json_refs: &[JsonRef],
    json_ref_localities: &[JsonRefLocality],
    row_plans: &[MaterializedTrackedStateRowPlan],
) -> Result<Vec<Option<Vec<u8>>>, LixError>
where
    S: StorageReader,
{
    let mut json_values = vec![None; json_refs.len()];
    let mut refs_by_pack = BTreeMap::<(&str, u32), Vec<(usize, JsonRef)>>::new();
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
        refs_by_pack
            .entry((row_plan.commit_id.as_str(), locality.pack_id))
            .or_default()
            .push((index, json_ref));
    }

    let json_store = JsonStoreContext::new();
    for ((commit_id, pack_id), refs) in refs_by_pack {
        let indexes = refs.iter().map(|(index, _)| *index).collect::<Vec<_>>();
        let refs = refs
            .into_iter()
            .map(|(_, json_ref)| json_ref)
            .collect::<Vec<_>>();
        let pack_ids = [pack_id];
        let values = json_store
            .load_bytes_many(
                store,
                JsonLoadRequestRef {
                    refs: &refs,
                    scope: JsonReadScopeRef::CommitPacks {
                        commit_id: &commit_id,
                        pack_ids: &pack_ids,
                    },
                },
            )
            .await?
            .into_values();
        for (index, value) in indexes.into_iter().zip(values) {
            json_values[index] = value;
        }
    }
    Ok(json_values)
}

fn materialize_row_plan(
    plan: MaterializedTrackedStateRowPlan,
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
pub(crate) struct TrackedMaterializationProjection {
    pub(crate) snapshot_content: bool,
    pub(crate) metadata: bool,
}

impl TrackedMaterializationProjection {
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
