use std::collections::{HashMap, HashSet};

use crate::LixError;
use crate::json_store::{
    JsonLoadRequestRef, JsonReadScopeRef, JsonRef, JsonSlot, JsonStoreContext,
};
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageGetOptions, StorageProjectedValue,
};

use super::{CHANGE_SPACE, ChangeId, ChangeRecord, change_key, decode_change_record};

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
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
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
    S: StorageAdapterRead,
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
    let keys = unique
        .iter()
        .map(|change_id| {
            crate::storage_adapter::StorageKey(bytes::Bytes::from(change_key(*change_id)))
        })
        .collect::<Vec<_>>();
    let result = PointReadPlan::new(CHANGE_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?;
    let mut out = HashMap::with_capacity(unique.len());
    for (change_id, value) in unique.into_iter().zip(result.value) {
        if let Some(StorageProjectedValue::FullValue(bytes)) = value {
            out.insert(change_id, decode_change_record(&bytes, change_id)?);
        }
    }
    Ok(out)
}

/// Hydrates the JSON slots of change-backed rows without coupling callers to
/// tracked-state tree values or sentinel commit ids.
pub(crate) async fn materialize_change_payloads<S>(
    store: &S,
    change_ids: impl Iterator<Item = ChangeId>,
    projection: ChangeRecordProjection,
    owner: &str,
) -> Result<HashMap<ChangeId, MaterializedChangePayload>, LixError>
where
    S: StorageAdapterRead,
{
    let mut unique = Vec::new();
    let mut seen = HashSet::new();
    for change_id in change_ids {
        if seen.insert(change_id) {
            unique.push(change_id);
        }
    }
    if !projection.requires_payload() {
        return Ok(unique
            .into_iter()
            .map(|change_id| {
                (
                    change_id,
                    MaterializedChangePayload {
                        identity: None,
                        snapshot_content: None,
                        metadata: None,
                    },
                )
            })
            .collect());
    }

    let mut changes = load_change_records(store, unique.iter().copied()).await?;
    let mut json_refs = Vec::new();
    let mut plans = Vec::with_capacity(unique.len());
    for change_id in unique {
        let change = changes.remove(&change_id).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "{owner} references ChangeRecord '{change_id}' that is missing from the changelog"
                ),
            )
        })?;
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
) -> Result<Vec<Option<Vec<u8>>>, LixError>
where
    S: StorageAdapterRead,
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
    String::from_utf8(bytes).map(Some).map_err(|error| {
        let utf8_error = error.utf8_error();
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("materialized ChangeRecord JSON payload is not UTF-8: {utf8_error}"),
        )
    })
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
