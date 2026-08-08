use std::collections::{HashMap, HashSet};

use bytes::Bytes;

use crate::LixError;
use crate::common::SharedStr;
use crate::json_store::{
    JsonLoadRequestRef, JsonReadScopeRef, JsonRef, JsonSlot, JsonStoreContext,
};
use crate::storage_adapter::StorageAdapterRead;

use super::{ChangeId, ChangeRecord};

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
    let records = crate::forktree::load_change_records(store, &unique).await?;
    let mut by_id = HashMap::with_capacity(unique.len());
    for (change_id, record) in unique.into_iter().zip(records) {
        if let Some(record) = record {
            by_id.insert(change_id, record);
        }
    }
    Ok(by_id)
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
    for change in changes {
        let change_id = change.change_id;
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
