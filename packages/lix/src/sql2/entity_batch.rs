//! Native entity request decoding and terminal projections.
//!
//! Entity providers consume concrete authenticated ForkTree state views.  The
//! module keeps `StateRow` and the native untracked row as the only row shapes
//! before Arrow/DataFusion takes ownership.

use bytes::Bytes;

use crate::LixError;
use crate::entity_pk::{EntityPk, EntityPkComponents};
use crate::forktree::{StateCell, StateKeyRef, decode_state_key, encode_state_key};
use crate::state::{ForkTreeStateView, StateRow, TransactionStateView, UntrackedStateRow};
use crate::storage_adapter::StorageAdapterRead;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EntityScanRequest {
    pub(crate) filter: EntityScanFilter,
    pub(crate) projection: EntityProjection,
    pub(crate) limit: Option<usize>,
}

impl Default for EntityScanRequest {
    fn default() -> Self {
        Self {
            filter: EntityScanFilter::default(),
            projection: EntityProjection::default(),
            limit: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EntityScanFilter {
    pub(crate) schema_keys: Vec<String>,
    pub(crate) branch_ids: Vec<String>,
    pub(crate) file_ids: Vec<crate::NullableKeyFilter<String>>,
    pub(crate) entity_pks: Vec<EntityPk>,
    pub(crate) untracked: Option<bool>,
    pub(crate) include_tombstones: bool,
    pub(crate) constraints: Vec<()>,
    pub(crate) rows: EntityRowSelection,
}

impl Default for EntityScanFilter {
    fn default() -> Self {
        Self {
            schema_keys: Vec::new(),
            branch_ids: Vec::new(),
            file_ids: Vec::new(),
            entity_pks: Vec::new(),
            untracked: None,
            include_tombstones: false,
            constraints: Vec::new(),
            rows: EntityRowSelection::All,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EntityRowSelection {
    All,
    None,
}

impl Default for EntityRowSelection {
    fn default() -> Self {
        Self::All
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct EntityProjection {
    pub(crate) columns: Vec<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct EntityExactBatchRequest {
    pub(crate) rows: Vec<EntityExactRowRequest>,
    pub(crate) projection: EntityProjection,
    pub(crate) untracked: Option<bool>,
    pub(crate) include_tombstones: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EntityExactRowRequest {
    pub(crate) schema_key: String,
    pub(crate) branch_id: String,
    pub(crate) entity_pk: EntityPk,
    pub(crate) file_id: Option<String>,
}

/// A slot preserves exact request order and duplicates.  Missing slots stay
/// `None`; present slots retain whether they came from tracked or untracked
/// authenticated state so projection cannot silently change ownership.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum EntityStateSlot {
    Tracked(StateRow),
    Untracked(UntrackedStateRow),
}

pub(crate) async fn scan_forktree<S>(
    view: &ForkTreeStateView<S>,
    request: &EntityScanRequest,
) -> Result<Vec<StateRow>, LixError>
where
    S: StorageAdapterRead + Clone,
{
    if matches!(request.filter.rows, EntityRowSelection::None) {
        return Ok(Vec::new());
    }
    let (lower, upper) = schema_bounds(request)?;
    if request.filter.untracked != Some(false) {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "native entity range requires the owner-scoped untracked range seam",
        ));
    }
    view.range(
        lower.as_deref(),
        upper.as_deref(),
        request.limit,
        request.filter.include_tombstones,
    )
    .await
    .map_err(|e| LixError::new(LixError::CODE_STORAGE_ERROR, e.to_string()))
}

pub(crate) async fn scan_transaction<S>(
    view: &TransactionStateView<S>,
    request: &EntityScanRequest,
) -> Result<Vec<StateRow>, LixError>
where
    S: StorageAdapterRead + Clone,
{
    if matches!(request.filter.rows, EntityRowSelection::None) {
        return Ok(Vec::new());
    }
    let (lower, upper) = schema_bounds(request)?;
    if request.filter.untracked != Some(false) {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "native transaction range requires the owner-scoped untracked range seam",
        ));
    }
    view.range(
        lower.as_deref(),
        upper.as_deref(),
        request.limit,
        request.filter.include_tombstones,
    )
    .await
    .map_err(|e| LixError::new(LixError::CODE_STORAGE_ERROR, e.to_string()))
}

pub(crate) async fn exact_forktree<S>(
    view: &ForkTreeStateView<S>,
    request: &EntityExactBatchRequest,
) -> Result<Vec<Option<EntityStateSlot>>, LixError>
where
    S: StorageAdapterRead + Clone,
{
    exact_forktree_inner(view, request).await
}

async fn exact_forktree_inner<S>(
    view: &ForkTreeStateView<S>,
    request: &EntityExactBatchRequest,
) -> Result<Vec<Option<EntityStateSlot>>, LixError>
where
    S: StorageAdapterRead + Clone,
{
    let keys = request
        .rows
        .iter()
        .map(|row| {
            encode_state_key(StateKeyRef {
                schema_key: &row.schema_key,
                file_id: row.file_id.as_deref(),
                entity_pk: &row.entity_pk,
            })
        })
        .collect::<Vec<_>>();
    let tracked = if request.untracked == Some(true) {
        vec![None; keys.len()]
    } else {
        view.points(&keys, true)
            .await
            .map_err(|e| LixError::new(LixError::CODE_STORAGE_ERROR, e.to_string()))?
    };
    let untracked = if request.untracked == Some(false) {
        vec![None; keys.len()]
    } else {
        view.untracked_points(&keys).await?
    };
    merge_exact_slots(request, keys, tracked, untracked)
}

pub(crate) async fn exact_transaction<S>(
    view: &TransactionStateView<S>,
    request: &EntityExactBatchRequest,
) -> Result<Vec<Option<EntityStateSlot>>, LixError>
where
    S: StorageAdapterRead + Clone,
{
    let keys = request
        .rows
        .iter()
        .map(|row| {
            encode_state_key(StateKeyRef {
                schema_key: &row.schema_key,
                file_id: row.file_id.as_deref(),
                entity_pk: &row.entity_pk,
            })
        })
        .collect::<Vec<_>>();
    let tracked = if request.untracked == Some(true) {
        vec![None; keys.len()]
    } else {
        view.points(&keys, true)
            .await
            .map_err(|e| LixError::new(LixError::CODE_STORAGE_ERROR, e.to_string()))?
    };
    let untracked = if request.untracked == Some(false) {
        vec![None; keys.len()]
    } else {
        view.untracked_points(&keys, request.include_tombstones)
            .await?
    };
    merge_exact_slots(request, keys, tracked, untracked)
}

fn merge_exact_slots(
    request: &EntityExactBatchRequest,
    keys: Vec<Vec<u8>>,
    tracked: Vec<Option<StateRow>>,
    untracked: Vec<Option<UntrackedStateRow>>,
) -> Result<Vec<Option<EntityStateSlot>>, LixError> {
    if tracked.len() != keys.len() || untracked.len() != keys.len() {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "native entity exact lookup returned the wrong slot count",
        ));
    }
    let mut output = Vec::with_capacity(keys.len());
    for (((requested, key), tracked), untracked) in
        request.rows.iter().zip(keys).zip(tracked).zip(untracked)
    {
        let slot = if let Some(row) = untracked {
            if row.key.schema_key != requested.schema_key
                || row.key.entity_pk != requested.entity_pk
                || row.key.file_id != requested.file_id
            {
                return Err(LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "native untracked entity row identity mismatch",
                ));
            }
            if !request.include_tombstones && row.value.cell.deleted() {
                None
            } else {
                Some(EntityStateSlot::Untracked(row))
            }
        } else if let Some(row) = tracked {
            let decoded = decode_state_key(&row.key)?;
            if decoded.schema_key != requested.schema_key
                || decoded.entity_pk != requested.entity_pk
                || decoded.file_id != requested.file_id
            {
                return Err(LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "native tracked entity row identity mismatch",
                ));
            }
            if !request.include_tombstones && row.value.cell.deleted() {
                None
            } else {
                Some(EntityStateSlot::Tracked(row))
            }
        } else {
            None
        };
        output.push(slot);
    }
    Ok(output)
}

pub(crate) fn schema_bounds(
    request: &EntityScanRequest,
) -> Result<(Option<Vec<u8>>, Option<Vec<u8>>), LixError> {
    let schema = request.filter.schema_keys.first().ok_or_else(|| {
        LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            "entity scan has no schema key",
        )
    })?;
    if request.filter.schema_keys.iter().any(|key| key != schema) {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "one native entity scan cannot mix schema keys",
        ));
    }
    if request
        .filter
        .branch_ids
        .windows(2)
        .any(|ids| ids[0] != ids[1])
    {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "one native entity scan cannot mix branch selectors",
        ));
    }
    // The native key codec exposes the schema prefix by encoding the empty
    // PK without the trailing file-id component.  This remains a bounded
    // ordered-tree range; it is never replaced by a full scan.
    if !request.filter.entity_pks.is_empty() {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "entity-PK scans use exact native points",
        ));
    }
    let empty_pk = EntityPk {
        components: EntityPkComponents::Empty,
    };
    let lower = crate::forktree::encode_state_entity_prefix(schema, &empty_pk);
    let upper = crate::forktree::exclusive_prefix_upper_bound(&lower);
    Ok((Some(lower), upper))
}

pub(crate) fn tracked_slot(row: &EntityStateSlot) -> Option<&StateRow> {
    match row {
        EntityStateSlot::Tracked(row) => Some(row),
        EntityStateSlot::Untracked(_) => None,
    }
}

pub(crate) fn slot_snapshot(row: &EntityStateSlot) -> Option<&str> {
    match row {
        EntityStateSlot::Tracked(row) => match &row.value.cell {
            StateCell::Value(value) => Some(value.as_ref()),
            StateCell::Null | StateCell::Tombstone => None,
        },
        EntityStateSlot::Untracked(row) => match &row.value.cell {
            StateCell::Value(value) => Some(value.as_ref()),
            StateCell::Null | StateCell::Tombstone => None,
        },
    }
}

pub(crate) fn row_snapshot(row: &StateRow) -> Option<&str> {
    match &row.value.cell {
        StateCell::Value(value) => Some(value.as_ref()),
        StateCell::Null | StateCell::Tombstone => None,
    }
}

pub(crate) fn project_snapshot(row: &StateRow) -> Option<Bytes> {
    row_snapshot(row).map(|value| Bytes::copy_from_slice(value.as_bytes()))
}

pub(crate) fn project_pk(row: &StateRow) -> Result<EntityPk, LixError> {
    Ok(decode_state_key(&row.key)?.entity_pk)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{LixTimestamp, SharedStr};
    use crate::forktree::{ChangeId, CommitId, StateCell, StateValue};
    use crate::state::StateRowSource;

    fn row(entity: &str, cell: StateCell) -> StateRow {
        let entity_pk = EntityPk::single(entity);
        StateRow {
            key: encode_state_key(StateKeyRef {
                schema_key: "app.message",
                file_id: None,
                entity_pk: &entity_pk,
            }),
            value: StateValue {
                change_id: ChangeId::from_bytes([1; 16]),
                commit_id: CommitId::from_bytes([2; 16]),
                created_at: LixTimestamp::from_unix_millis_utc_lossy(1),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy(2),
                cell,
                metadata: None,
                origin_key: None,
                blob_manifest_object_ids: Vec::new(),
            },
            source: StateRowSource::Branch,
        }
    }

    fn request(entities: &[&str], include_tombstones: bool) -> EntityExactBatchRequest {
        EntityExactBatchRequest {
            rows: entities
                .iter()
                .map(|entity| EntityExactRowRequest {
                    schema_key: "app.message".to_string(),
                    branch_id: "branch-a".to_string(),
                    entity_pk: EntityPk::single(*entity),
                    file_id: None,
                })
                .collect(),
            projection: EntityProjection::default(),
            untracked: Some(false),
            include_tombstones,
        }
    }

    #[test]
    fn exact_merge_preserves_order_duplicates_and_missing_slots() {
        let request = request(&["a", "a", "missing"], false);
        let rows = vec![
            Some(row("a", StateCell::Value(SharedStr::from("{}")))),
            Some(row("a", StateCell::Value(SharedStr::from("{}")))),
            None,
        ];
        let keys = request
            .rows
            .iter()
            .map(|row| {
                encode_state_key(StateKeyRef {
                    schema_key: &row.schema_key,
                    file_id: row.file_id.as_deref(),
                    entity_pk: &row.entity_pk,
                })
            })
            .collect();
        let slots = merge_exact_slots(&request, keys, rows, vec![None, None, None])
            .expect("native exact merge");
        assert_eq!(slots.len(), 3);
        assert!(slots[0].is_some());
        assert!(slots[1].is_some());
        assert!(slots[2].is_none());
    }

    #[test]
    fn exact_merge_hides_tombstone_until_requested() {
        let hidden = request(&["gone"], false);
        let keys = hidden
            .rows
            .iter()
            .map(|row| {
                encode_state_key(StateKeyRef {
                    schema_key: &row.schema_key,
                    file_id: row.file_id.as_deref(),
                    entity_pk: &row.entity_pk,
                })
            })
            .collect();
        let tombstone = Some(row("gone", StateCell::Tombstone));
        let hidden_slots = merge_exact_slots(&hidden, keys, vec![tombstone.clone()], vec![None])
            .expect("tombstone visibility");
        assert!(hidden_slots[0].is_none());

        let visible = request(&["gone"], true);
        let keys = visible
            .rows
            .iter()
            .map(|row| {
                encode_state_key(StateKeyRef {
                    schema_key: &row.schema_key,
                    file_id: row.file_id.as_deref(),
                    entity_pk: &row.entity_pk,
                })
            })
            .collect();
        let visible_slots = merge_exact_slots(&visible, keys, vec![tombstone], vec![None])
            .expect("tombstone projection");
        assert!(matches!(
            visible_slots[0],
            Some(EntityStateSlot::Tracked(_))
        ));
    }

    #[test]
    fn exact_merge_rejects_authenticated_identity_substitution() {
        let request = request(&["expected"], false);
        let keys = request
            .rows
            .iter()
            .map(|row| {
                encode_state_key(StateKeyRef {
                    schema_key: &row.schema_key,
                    file_id: row.file_id.as_deref(),
                    entity_pk: &row.entity_pk,
                })
            })
            .collect();
        let result = merge_exact_slots(
            &request,
            keys,
            vec![Some(row(
                "different",
                StateCell::Value(SharedStr::from("{}")),
            ))],
            vec![None],
        );
        assert!(result.is_err());
    }
}
