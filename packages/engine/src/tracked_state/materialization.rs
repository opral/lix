use crate::commit_store::CommitStoreContext;
use crate::entity_identity::EntityIdentity;
use crate::json_store::JsonRef;
use crate::json_store::{JsonLoadRequestRef, JsonReadScopeRef, JsonStoreContext};
use crate::storage::StorageReader;
use crate::tracked_state::types::{TrackedStateIndexValue, TrackedStateKey};
use crate::tracked_state::MaterializedTrackedStateRow;
use crate::LixError;
use std::collections::BTreeMap;

/// Materializes tracked-state index entries from commit_store packs.
///
/// The durable tracked_state value carries only a commit_store locator plus a
/// projection-local `updated_at` cache. Snapshot refs, metadata refs,
/// tombstone state, change id, and source commit id are hydrated at this read
/// boundary from grouped commit_store pack loads.
pub(crate) async fn materialize_index_entries<S>(
    store: &mut S,
    commit_store: &CommitStoreContext,
    entries: Vec<(TrackedStateKey, TrackedStateIndexValue)>,
    projection: &TrackedMaterializationProjection,
) -> Result<Vec<MaterializedTrackedStateRow>, LixError>
where
    S: StorageReader,
{
    let mut packs = BTreeMap::new();
    for (_, value) in &entries {
        let key = (
            value.change_locator.source_commit_id.clone(),
            value.change_locator.source_pack_id,
        );
        if packs.contains_key(&key) {
            continue;
        }
        let Some(changes) = commit_store
            .reader(&mut *store)
            .load_change_pack(&key.0, key.1)
            .await?
        else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state locator references missing change pack ({}, {})",
                    key.0, key.1
                ),
            ));
        };
        packs.insert(key, changes);
    }

    let mut row_plans = Vec::with_capacity(entries.len());
    let mut json_refs = Vec::new();
    let mut json_ref_localities = Vec::new();
    for (key, value) in entries {
        let pack_key = (
            value.change_locator.source_commit_id.clone(),
            value.change_locator.source_pack_id,
        );
        let changes = packs.get(&pack_key).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state materialization lost a loaded commit_store pack",
            )
        })?;
        let change = changes
            .get(
                usize::try_from(value.change_locator.source_ordinal).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "tracked_state locator ordinal does not fit usize",
                    )
                })?,
            )
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "tracked_state locator for '{}' points past pack ({}, {})",
                        value.change_locator.change_id,
                        value.change_locator.source_commit_id,
                        value.change_locator.source_pack_id
                    ),
                )
            })?;
        if change.id != value.change_locator.change_id {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state locator expected '{}' but found '{}'",
                    value.change_locator.change_id, change.id
                ),
            ));
        }

        let snapshot_ref_index = projected_json_ref_index(
            projection.snapshot_content,
            change.snapshot_ref,
            &value.change_locator.source_commit_id,
            value.change_locator.source_pack_id,
            &mut json_refs,
            &mut json_ref_localities,
        );
        let metadata_ref_index = projected_json_ref_index(
            projection.metadata,
            change.metadata_ref,
            &value.change_locator.source_commit_id,
            value.change_locator.source_pack_id,
            &mut json_refs,
            &mut json_ref_localities,
        );
        row_plans.push(MaterializedTrackedStateRowPlan {
            entity_id: key.entity_id,
            schema_key: key.schema_key,
            file_id: key.file_id,
            deleted: change.snapshot_ref.is_none(),
            created_at: value.created_at,
            updated_at: value.updated_at,
            change_id: change.id.clone(),
            commit_id: value.change_locator.source_commit_id,
            snapshot_ref_index,
            metadata_ref_index,
        });
    }

    let json_values = load_projection_json_values(store, &json_refs, &json_ref_localities).await?;
    row_plans
        .into_iter()
        .map(|plan| materialize_row_plan(plan, &json_refs, &json_values))
        .collect()
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
    commit_id: &str,
    pack_id: u32,
    json_refs: &mut Vec<JsonRef>,
    json_ref_localities: &mut Vec<(String, u32)>,
) -> Option<usize> {
    if !include {
        return None;
    }
    let index = json_refs.len();
    json_refs.push(json_ref?);
    json_ref_localities.push((commit_id.to_string(), pack_id));
    Some(index)
}

async fn load_projection_json_values<S>(
    store: &mut S,
    json_refs: &[JsonRef],
    json_ref_localities: &[(String, u32)],
) -> Result<Vec<Option<Vec<u8>>>, LixError>
where
    S: StorageReader,
{
    let mut json_values = vec![None; json_refs.len()];
    let mut refs_by_pack = BTreeMap::<(String, u32), Vec<(usize, JsonRef)>>::new();
    for (index, json_ref) in json_refs.iter().copied().enumerate() {
        let locality = json_ref_localities.get(index).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state materialization lost JSON locality index",
            )
        })?;
        refs_by_pack
            .entry(locality.clone())
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
    json_values: &[Option<Vec<u8>>],
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
    json_values: &[Option<Vec<u8>>],
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
    let bytes = json_values
        .get(index)
        .and_then(Option::as_deref)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state materialization missing JSON payload '{}'",
                    json_ref.to_hex()
                ),
            )
        })?;
    std::str::from_utf8(bytes)
        .map(|json| Some(json.to_string()))
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("tracked_state materialized JSON payload is not UTF-8: {error}"),
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
