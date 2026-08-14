//! Native entity request decoding and terminal projections.
//!
//! Entity providers consume concrete authenticated ForkTree state views.  The
//! module keeps `StateRow` as the only row shape before Arrow/DataFusion takes
//! ownership.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::future::Future;

use crate::LixError;
use crate::entity_pk::{EntityPk, EntityPkComponents};
use crate::forktree::{StateCell, StateKeyRef, decode_state_key, encode_state_key};
use crate::state::{ForkTreeStateView, StateRow, TransactionStateView};
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
    /// A tracked row loaded from an explicit branch selector. The branch
    /// identity is retained for by-branch projection when one operation
    /// groups multiple branch ranges.
    TrackedAt {
        row: StateRow,
        branch_id: String,
    },
}

/// Native range scan with branch routing, tracked/untracked overlay, and
/// final visibility/limit ordering. Every branch is borrowed from the same
/// operation-owned ForkTree view; global rows are emitted once when several
/// branch selectors share them.
pub(crate) async fn scan_slots_forktree<S>(
    view: &ForkTreeStateView<S>,
    request: &EntityScanRequest,
) -> Result<Vec<EntityStateSlot>, LixError>
where
    S: StorageAdapterRead,
{
    scan_slots_by_branches(
        request,
        view.branch_id(),
        |branch_id, lower, upper| async move {
            let (source_limit, source_include_tombstones) = range_source_options(request);
            if request.filter.untracked == Some(true) {
                return Err(LixError::new(
                    LixError::CODE_UNSUPPORTED_SQL,
                    "untracked state is no longer supported",
                ));
            }
            let slots = view
                .branch_range(
                    &branch_id,
                    lower.as_deref(),
                    upper.as_deref(),
                    source_limit,
                    source_include_tombstones,
                )
                .await?
                .into_iter()
                .map(|row| EntityStateSlot::TrackedAt {
                    row,
                    branch_id: branch_id.to_string(),
                })
                .filter(|slot| request.filter.include_tombstones || !slot_is_deleted(slot))
                .collect();
            Ok(filter_slots_by_file_id(slots, &request.filter.file_ids)?
                .into_iter()
                .take(request.limit.unwrap_or(usize::MAX))
                .collect())
        },
    )
    .await
}

pub(crate) async fn scan_slots_transaction<S>(
    view: &TransactionStateView<S>,
    request: &EntityScanRequest,
) -> Result<Vec<EntityStateSlot>, LixError>
where
    S: StorageAdapterRead,
{
    scan_slots_by_branches(
        request,
        view.branch_id(),
        |branch_id, lower, upper| async move {
            let (source_limit, source_include_tombstones) = range_source_options(request);
            if request.filter.untracked == Some(true) {
                return Err(LixError::new(
                    LixError::CODE_UNSUPPORTED_SQL,
                    "untracked state is no longer supported",
                ));
            }
            let slots = view
                .branch_range(
                    &branch_id,
                    lower.as_deref(),
                    upper.as_deref(),
                    source_limit,
                    source_include_tombstones,
                )
                .await?
                .into_iter()
                .map(|row| EntityStateSlot::TrackedAt {
                    row,
                    branch_id: branch_id.to_string(),
                })
                .filter(|slot| request.filter.include_tombstones || !slot_is_deleted(slot))
                .collect();
            Ok(filter_slots_by_file_id(slots, &request.filter.file_ids)?
                .into_iter()
                .take(request.limit.unwrap_or(usize::MAX))
                .collect())
        },
    )
    .await
}

fn range_source_options(request: &EntityScanRequest) -> (Option<usize>, bool) {
    (
        request
            .filter
            .file_ids
            .is_empty()
            .then_some(request.limit)
            .flatten(),
        request.filter.include_tombstones,
    )
}

fn filter_slots_by_file_id(
    slots: Vec<EntityStateSlot>,
    file_ids: &[crate::NullableKeyFilter<String>],
) -> Result<Vec<EntityStateSlot>, LixError> {
    if file_ids.is_empty()
        || file_ids
            .iter()
            .any(|filter| matches!(filter, crate::NullableKeyFilter::Any))
    {
        return Ok(slots);
    }
    slots
        .into_iter()
        .filter_map(|slot| {
            let key = match &slot {
                EntityStateSlot::Tracked(row) | EntityStateSlot::TrackedAt { row, .. } => {
                    decode_state_key(&row.key)
                }
            };
            match key {
                Ok(key) => file_ids
                    .iter()
                    .any(|filter| match filter {
                        crate::NullableKeyFilter::Null => key.file_id.is_none(),
                        crate::NullableKeyFilter::Value(file_id) => {
                            key.file_id.as_deref() == Some(file_id.as_str())
                        }
                        crate::NullableKeyFilter::Any => true,
                    })
                    .then_some(Ok(slot)),
                Err(error) => Some(Err(error)),
            }
        })
        .collect()
}

async fn scan_slots_by_branches<F, Fut>(
    request: &EntityScanRequest,
    active_branch_id: String,
    mut scan_branch: F,
) -> Result<Vec<EntityStateSlot>, LixError>
where
    F: FnMut(String, Option<Vec<u8>>, Option<Vec<u8>>) -> Fut,
    Fut: Future<Output = Result<Vec<EntityStateSlot>, LixError>>,
{
    if request.limit == Some(0) || matches!(request.filter.rows, EntityRowSelection::None) {
        return Ok(Vec::new());
    }
    let mut bounds_request = request.clone();
    bounds_request.filter.entity_pks.clear();
    let schema_bounds = schema_bounds(&bounds_request)?;
    let mut bounds = if request.filter.entity_pks.is_empty() {
        vec![schema_bounds]
    } else {
        let schema = bounds_request
            .filter
            .schema_keys
            .first()
            .expect("schema bounds validated");
        request
            .filter
            .entity_pks
            .iter()
            .map(|entity_pk| {
                let bounds = crate::forktree::encode_state_entity_prefix_bounds(schema, entity_pk);
                (Some(bounds.lower), bounds.upper)
            })
            .collect()
    };
    bounds.sort_by(|left, right| left.0.cmp(&right.0));
    let branch_ids = if request.filter.branch_ids.is_empty() {
        vec![active_branch_id]
    } else {
        request.filter.branch_ids.clone()
    };
    let mut branch_rows = Vec::new();
    for branch_id in &branch_ids {
        let mut rows = Vec::new();
        for (lower, upper) in &bounds {
            rows.extend(scan_branch(branch_id.clone(), lower.clone(), upper.clone()).await?);
        }
        branch_rows.push(merge_range_slots(
            rows,
            Vec::new(),
            request.filter.include_tombstones,
            request.limit,
        ));
    }
    // One branch is already emitted in canonical key order by the retained
    // ForkTree range and its global/local overlay. Avoid rebuilding that same
    // order through the multi-branch heap/dedup machinery.
    if branch_rows.len() == 1 {
        return Ok(branch_rows.pop().expect("one branch result"));
    }
    let mut global_keys = std::collections::BTreeSet::new();
    let mut branch_rows = branch_rows
        .into_iter()
        .map(|rows| rows.into_iter().map(Some).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let mut cursors = BinaryHeap::new();
    for (branch_index, rows) in branch_rows.iter().enumerate() {
        if let Some(Some(slot)) = rows.first() {
            cursors.push(Reverse((
                slot_sort_key(slot),
                slot_branch_sort_key(slot),
                branch_index,
                0_usize,
            )));
        }
    }
    let mut visible = Vec::new();
    while let Some(Reverse((_key, _branch_sort, branch_index, row_index))) = cursors.pop() {
        let slot = branch_rows[branch_index][row_index]
            .take()
            .expect("k-way branch cursor row");
        if let Some(Some(next)) = branch_rows[branch_index].get(row_index + 1) {
            cursors.push(Reverse((
                slot_sort_key(next),
                slot_branch_sort_key(next),
                branch_index,
                row_index + 1,
            )));
        }
        let key = slot_sort_key(&slot);
        let is_global = matches!(&slot, EntityStateSlot::Tracked(row) if row.source == crate::state::StateRowSource::Global)
            || matches!(&slot, EntityStateSlot::TrackedAt { row, .. } if row.source == crate::state::StateRowSource::Global);
        if is_global && !global_keys.insert((slot_branch_sort_key(&slot), key)) {
            continue;
        }
        if request.filter.include_tombstones || !slot_is_deleted(&slot) {
            visible.push(slot);
            if request.limit.is_some_and(|limit| visible.len() >= limit) {
                break;
            }
        }
    }
    Ok(visible)
}

fn slot_sort_key(slot: &EntityStateSlot) -> Vec<u8> {
    match slot {
        EntityStateSlot::Tracked(row) | EntityStateSlot::TrackedAt { row, .. } => row.key.clone(),
    }
}

fn slot_branch_sort_key(slot: &EntityStateSlot) -> String {
    match slot {
        EntityStateSlot::Tracked(row) => match row.source {
            crate::state::StateRowSource::Global => crate::GLOBAL_BRANCH_ID.to_string(),
            crate::state::StateRowSource::Branch
            | crate::state::StateRowSource::StagedBranch => {
                String::new()
            }
            crate::state::StateRowSource::StagedGlobal => crate::GLOBAL_BRANCH_ID.to_string(),
        },
        EntityStateSlot::TrackedAt { branch_id, .. } => branch_id.clone(),
    }
}

fn merge_range_slots(
    tracked: Vec<EntityStateSlot>,
    untracked: Vec<EntityStateSlot>,
    include_tombstones: bool,
    limit: Option<usize>,
) -> Vec<EntityStateSlot> {
    if limit == Some(0) {
        return Vec::new();
    }
    let mut tracked = tracked
        .into_iter()
        .map(|slot| (slot_sort_key(&slot), slot))
        .peekable();
    let mut untracked = untracked
        .into_iter()
        .map(|slot| (slot_sort_key(&slot), slot))
        .peekable();
    let mut output = Vec::new();
    while tracked.peek().is_some() || untracked.peek().is_some() {
        let slot = match (tracked.peek(), untracked.peek()) {
            (Some((tracked_key, _)), Some((untracked_key, _))) if tracked_key == untracked_key => {
                tracked.next().expect("peeked tracked slot");
                untracked.next().expect("peeked untracked slot").1
            }
            (Some((tracked_key, _)), Some((untracked_key, _))) if tracked_key < untracked_key => {
                tracked.next().expect("peeked tracked slot").1
            }
            (Some(_), Some(_)) => untracked.next().expect("peeked untracked slot").1,
            (Some(_), None) => tracked.next().expect("peeked tracked slot").1,
            (None, Some(_)) => untracked.next().expect("peeked untracked slot").1,
            (None, None) => break,
        };
        if include_tombstones || !slot_is_deleted(&slot) {
            output.push(slot);
            if limit.is_some_and(|limit| output.len() >= limit) {
                break;
            }
        }
    }
    output
}

fn slot_is_deleted(slot: &EntityStateSlot) -> bool {
    match slot {
        EntityStateSlot::Tracked(row) | EntityStateSlot::TrackedAt { row, .. } => {
            row.value.cell.deleted()
        }
    }
}

pub(crate) async fn exact_forktree<S>(
    view: &ForkTreeStateView<S>,
    request: &EntityExactBatchRequest,
) -> Result<Vec<Option<EntityStateSlot>>, LixError>
where
    S: StorageAdapterRead,
{
    exact_forktree_inner(view, request).await
}

async fn exact_forktree_inner<S>(
    view: &ForkTreeStateView<S>,
    request: &EntityExactBatchRequest,
) -> Result<Vec<Option<EntityStateSlot>>, LixError>
where
    S: StorageAdapterRead,
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
    let mut tracked = vec![None; keys.len()];
    let mut groups = BTreeMap::<String, Vec<usize>>::new();
    for (index, row) in request.rows.iter().enumerate() {
        groups.entry(row.branch_id.clone()).or_default().push(index);
    }
    for (branch_id, indices) in groups {
        let branch_keys = indices
            .iter()
            .map(|index| keys[*index].clone())
            .collect::<Vec<_>>();
        if request.untracked == Some(true) {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "untracked state is no longer supported",
            ));
        }
        let branch_rows = view.branch_points(&branch_id, &branch_keys, true).await?;
        for (index, row) in indices.iter().copied().zip(branch_rows) {
            tracked[index] = row;
        }
    }
    merge_exact_slots(request, keys, tracked)
}

pub(crate) async fn exact_transaction<S>(
    view: &TransactionStateView<S>,
    request: &EntityExactBatchRequest,
) -> Result<Vec<Option<EntityStateSlot>>, LixError>
where
    S: StorageAdapterRead,
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
    let mut tracked = vec![None; keys.len()];
    let mut groups = BTreeMap::<String, Vec<usize>>::new();
    for (index, row) in request.rows.iter().enumerate() {
        groups.entry(row.branch_id.clone()).or_default().push(index);
    }
    for (branch_id, indices) in groups {
        let branch_keys = indices
            .iter()
            .map(|index| keys[*index].clone())
            .collect::<Vec<_>>();
        if request.untracked == Some(true) {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "untracked state is no longer supported",
            ));
        }
        let branch_rows = view.branch_points(&branch_id, &branch_keys, true).await?;
        for (index, row) in indices.iter().copied().zip(branch_rows) {
            tracked[index] = row;
        }
    }
    merge_exact_slots(request, keys, tracked)
}

fn merge_exact_slots(
    request: &EntityExactBatchRequest,
    keys: Vec<Vec<u8>>,
    tracked: Vec<Option<StateRow>>,
) -> Result<Vec<Option<EntityStateSlot>>, LixError> {
    if tracked.len() != keys.len() {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "native entity exact lookup returned the wrong slot count",
        ));
    }
    let mut output = Vec::with_capacity(keys.len());
    for ((requested, _key), tracked) in request.rows.iter().zip(keys).zip(tracked) {
        let slot = if let Some(row) = tracked {
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
                Some(EntityStateSlot::TrackedAt {
                    row,
                    branch_id: requested.branch_id.clone(),
                })
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

#[cfg(test)]
pub(crate) fn tracked_slot(row: &EntityStateSlot) -> Option<&StateRow> {
    match row {
        EntityStateSlot::Tracked(row) | EntityStateSlot::TrackedAt { row, .. } => Some(row),
    }
}

pub(crate) fn slot_snapshot(row: &EntityStateSlot) -> Option<&str> {
    match row {
        EntityStateSlot::Tracked(row) | EntityStateSlot::TrackedAt { row, .. } => {
            match &row.value.cell {
                StateCell::Value(value) => Some(value.as_ref()),
                StateCell::NativeRow(_) => None,
                StateCell::Null | StateCell::Tombstone => None,
            }
        }
    }
}

#[cfg(test)]
pub(crate) fn row_snapshot(row: &StateRow) -> Option<&str> {
    match &row.value.cell {
        StateCell::Value(value) => Some(value.as_ref()),
        StateCell::NativeRow(_) => None,
        StateCell::Null | StateCell::Tombstone => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::{LixTimestamp, SharedStr};
    use crate::forktree::{StateValue, encode_state_key};
    use crate::state::StateRowSource;

    fn row(file_id: Option<&str>) -> EntityStateSlot {
        let entity_pk = EntityPk::single("shared-plugin-row");
        EntityStateSlot::Tracked(StateRow {
            key: encode_state_key(StateKeyRef {
                schema_key: "plugin_row",
                file_id,
                entity_pk: &entity_pk,
            }),
            value: StateValue {
                change_id: ChangeId::for_test_label("entity-file-filter-change"),
                commit_id: CommitId::for_test_label("entity-file-filter-commit"),
                created_at: LixTimestamp::from_unix_millis_utc_lossy(1),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy(2),
                cell: StateCell::Value(SharedStr::from("{}")),
                metadata: None,
                origin_key: None,
                blob_manifest_object_ids: Vec::new(),
            },
            source: StateRowSource::Branch,
        })
    }

    #[test]
    fn file_filter_selects_one_owner_before_limit_for_shared_plugin_identity() {
        let request = EntityScanRequest {
            filter: EntityScanFilter {
                file_ids: vec![crate::NullableKeyFilter::Value("file-b".to_string())],
                ..EntityScanFilter::default()
            },
            limit: Some(1),
            ..EntityScanRequest::default()
        };
        assert_eq!(range_source_options(&request).0, None);
        let selected = filter_slots_by_file_id(
            vec![row(Some("file-a")), row(Some("file-b"))],
            &request.filter.file_ids,
        )
        .expect("authenticated file-owner filter");
        assert_eq!(selected.len(), 1);
        let key = match &selected[0] {
            EntityStateSlot::Tracked(row) | EntityStateSlot::TrackedAt { row, .. } => {
                decode_state_key(&row.key).expect("typed state key")
            }
        };
        assert_eq!(key.file_id.as_deref(), Some("file-b"));
    }

    #[test]
    fn file_filter_rejects_malformed_authenticated_key() {
        let mut malformed = row(Some("file-a"));
        match &mut malformed {
            EntityStateSlot::Tracked(row) | EntityStateSlot::TrackedAt { row, .. } => {
                row.key = vec![0xff];
            }
        }
        assert!(
            filter_slots_by_file_id(
                vec![malformed],
                &[crate::NullableKeyFilter::Value("file-a".to_string())],
            )
            .is_err()
        );
    }
}
