use crate::entity_pk::EntityPk;
use crate::json_store::JsonRef;
use crate::json_store::{JsonLoadRequestRef, JsonReadScopeRef, JsonStoreContext};
use crate::storage::StorageRead;
use crate::tracked_state::types::{TrackedStateIndexValue, TrackedStateKey};
use crate::tracked_state::MaterializedTrackedStateRow;
use crate::LixError;

/// Materializes tracked-state index entries.
///
/// The durable tracked_state value is authoritative for scalar materialization
/// fields and stores the JSON refs needed for payload hydration. Snapshot and
/// metadata bytes are hydrated from grouped json_store loads only when the
/// requested materialization needs them.
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

    let json_slots_per_row =
        usize::from(materialization.snapshot_content) + usize::from(materialization.metadata);
    let json_ref_capacity = entries.len().saturating_mul(json_slots_per_row);
    let mut row_plans = Vec::with_capacity(entries.len());
    let mut json_refs = Vec::with_capacity(json_ref_capacity);
    let mut json_ref_localities = Vec::with_capacity(json_ref_capacity);
    for (key, value) in entries {
        let snapshot_ref_index = materialized_json_ref_index(
            materialization.snapshot_content,
            value.snapshot_ref,
            &mut json_refs,
            &mut json_ref_localities,
        );
        let metadata_ref_index = materialized_json_ref_index(
            materialization.metadata,
            value.metadata_ref,
            &mut json_refs,
            &mut json_ref_localities,
        );
        row_plans.push(TrackedRowMaterializationPlan {
            entity_pk: key.entity_pk,
            schema_key: key.schema_key,
            file_id: key.file_id,
            deleted: value.deleted,
            created_at: value.created_at,
            updated_at: value.updated_at,
            change_id: value.change_id,
            commit_id: value.commit_id,
            snapshot_ref_index,
            metadata_ref_index,
        });
    }

    let mut json_values =
        load_materialization_json_values(store, &json_refs, &json_ref_localities).await?;
    row_plans
        .into_iter()
        .map(|plan| materialize_row_plan(plan, &json_refs, &mut json_values))
        .collect()
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
        created_at: value.created_at,
        updated_at: value.updated_at,
        change_id: value.change_id,
        commit_id: value.commit_id,
    }
}

struct TrackedRowMaterializationPlan {
    entity_pk: EntityPk,
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

fn materialized_json_ref_index(
    include: bool,
    json_ref: Option<JsonRef>,
    json_refs: &mut Vec<JsonRef>,
    json_ref_localities: &mut Vec<JsonRefLocality>,
) -> Option<usize> {
    if !include {
        return None;
    }
    let index = json_refs.len();
    json_refs.push(json_ref?);
    json_ref_localities.push(JsonRefLocality);
    Some(index)
}

struct JsonRefLocality;

async fn load_materialization_json_values<S>(
    store: &S,
    json_refs: &[JsonRef],
    json_ref_localities: &[JsonRefLocality],
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
    let mut out_of_band_indexes = Vec::new();
    let mut out_of_band_refs = Vec::new();
    for (index, json_ref) in json_refs.iter().copied().enumerate() {
        let _locality = json_ref_localities.get(index).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state materialization lost JSON locality index",
            )
        })?;
        out_of_band_indexes.push(index);
        out_of_band_refs.push(json_ref);
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
        entity_pk: plan.entity_pk,
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

        let materialized = materialized_json_string(Some(0), &[json_ref], &mut json_values)
            .expect("json should materialize");

        assert_eq!(materialized, Some(r#"{"value":1}"#.to_string()));
        assert!(json_values[0].is_none());
    }
}
