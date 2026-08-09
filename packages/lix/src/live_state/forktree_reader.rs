//! Current-state reader backed only by the authenticated ForkTree view.
//!
//! This adapter is deliberately small while the rest of the historical
//! reader conversion is in flight. It accepts the single-branch tracked
//! serving shape, keeps the caller's existing `StorageRead`, and refuses
//! lanes that still need a separate owner instead of falling back to a
//! deleted current-layout reader.

use std::sync::Arc;

use async_trait::async_trait;

use crate::LixError;
use crate::entity_pk::EntityPk;
use crate::forktree::{
    CanonicalBranchId, ForkTreeReadFacade, StateCell, StateKeyRef, StateSource, decode_state_key,
    encode_state_key, state_points, state_range_cursor,
};
use crate::live_state::{
    LiveStateExactBatchRequest, LiveStateRowFilter, LiveStateScanRequest,
    MaterializedLiveStateBatch, MaterializedLiveStateBatchBuilder, MaterializedLiveStateExactBatch,
    MaterializedLiveStateRow, MaterializedLiveStateRowRef,
};
use crate::storage::{ProjectedValue, ReadEntry, ScanCursor};
use crate::storage_adapter::StorageAdapterRead;

use super::derived::is_derived_schema;

pub(crate) async fn scan_view<R>(
    view: &crate::forktree::CoherentView<R>,
    request: &LiveStateScanRequest,
) -> Result<MaterializedLiveStateBatch, LixError>
where
    R: StorageAdapterRead,
{
    validate_scan_request(request)?;
    match request.filter.untracked {
        Some(true) => scan_untracked_view(view, request).await,
        Some(false) => scan_tracked_view(view, request).await,
        None => scan_combined_view(view, request).await,
    }
}

/// Runs a current-state scan through an operation-owned ForkTree facade. The
/// facade retains the caller's immutable read; this helper only authenticates
/// the requested branch view and never acquires a second storage read.
pub(crate) async fn scan_facade<R>(
    facade: &ForkTreeReadFacade<R>,
    request: &LiveStateScanRequest,
) -> Result<MaterializedLiveStateBatch, LixError>
where
    R: StorageAdapterRead,
{
    validate_scan_request(request)?;
    let branch_ids = &request.filter.branch_ids;
    if branch_ids.is_empty() {
        return Err(unsupported(
            "current ForkTree reader requires an authenticated branch scope",
        ));
    }
    if branch_ids.len() == 1 {
        let view = facade.branch(&branch_ids[0]).await?;
        return scan_view(&view, request).await;
    }

    // An explicit by-branch surface may enumerate several authenticated
    // branch views. Each view borrows the same operation-owned read; its
    // global overlay is intentionally materialized once per branch, matching
    // the public lix_file_by_branch identity contract. Apply LIMIT only after
    // the ordered branch streams have been concatenated.
    let mut rows = Vec::new();
    for branch_id in branch_ids {
        let view = facade.branch(branch_id).await?;
        let mut branch_request = request.clone();
        branch_request.filter.branch_ids = vec![branch_id.clone()];
        branch_request.limit = None;
        rows.extend(scan_view(&view, &branch_request).await?.into_rows());
    }
    if let Some(limit) = request.limit {
        rows.truncate(limit);
    }
    Ok(MaterializedLiveStateBatch::from_rows(rows))
}

async fn scan_tracked_view<R>(
    view: &crate::forktree::CoherentView<R>,
    request: &LiveStateScanRequest,
) -> Result<MaterializedLiveStateBatch, LixError>
where
    R: StorageAdapterRead,
{
    let [branch_id] = request.filter.branch_ids.as_slice() else {
        return Err(unsupported("current ForkTree reader requires one branch"));
    };
    if !request.filter.constraints.is_empty()
        || !matches!(request.filter.rows, LiveStateRowFilter::All)
    {
        return Err(unsupported(
            "current ForkTree reader does not yet own this scan lane",
        ));
    }

    let branch_id = parse_branch_id(branch_id)?;
    if view.branch_id() != branch_id {
        return Err(unsupported(
            "current ForkTree reader view does not match requested branch",
        ));
    }
    let mut cursor = state_range_cursor(&view, None, None, true)?;
    let mut output = Vec::with_capacity(request.limit.map_or(64, |limit| limit.min(64)));
    while let Some((encoded_key, value, source)) = cursor.next().await? {
        let key = decode_state_key(&encoded_key)?;
        if !request.filter.schema_keys.is_empty()
            && !request
                .filter
                .schema_keys
                .iter()
                .any(|schema| schema == &key.schema_key)
        {
            continue;
        }
        if !request.filter.entity_pks.is_empty()
            && !request
                .filter
                .entity_pks
                .iter()
                .any(|entity| entity == &key.entity_pk)
        {
            continue;
        }
        if !request
            .filter
            .file_ids
            .iter()
            .all(|filter| filter.matches(key.file_id.as_ref()))
        {
            continue;
        }
        if value.cell.deleted() && !request.filter.include_tombstones {
            continue;
        }
        let branch_owner = match source {
            StateSource::Global | StateSource::Branch => branch_id_text(branch_id),
        };
        output.push(materialize_state_value(
            value,
            source,
            key.entity_pk,
            key.schema_key,
            key.file_id,
            branch_owner,
        ));
        if request.limit.is_some_and(|limit| output.len() >= limit) {
            break;
        }
    }
    Ok(MaterializedLiveStateBatch::from_rows(output))
}

/// Resolves the complete current logical overlay while borrowing the one view
/// opened by the caller. The two physical candidate streams are transient
/// inputs to one identity-ordered result; untracked values replace tracked
/// values, including with a tombstone, before the public limit is applied.
async fn scan_combined_view<R>(
    view: &crate::forktree::CoherentView<R>,
    request: &LiveStateScanRequest,
) -> Result<MaterializedLiveStateBatch, LixError>
where
    R: StorageAdapterRead,
{
    if request.limit == Some(0) {
        return Ok(MaterializedLiveStateBatch::default());
    }
    let [branch_id] = request.filter.branch_ids.as_slice() else {
        return Err(unsupported("current ForkTree reader requires one branch"));
    };
    if !request.filter.constraints.is_empty()
        || !matches!(request.filter.rows, LiveStateRowFilter::All)
    {
        return Err(unsupported(
            "current ForkTree reader does not yet own this scan lane",
        ));
    }
    let branch_id = parse_branch_id(branch_id)?;
    if view.branch_id() != branch_id {
        return Err(unsupported(
            "current ForkTree reader view does not match requested branch",
        ));
    }
    let mut cursor = CurrentOverlayCursor::new(view).await?;
    let mut output = MaterializedLiveStateBatchBuilder::with_capacity(
        request.limit.map_or(64, |limit| limit.min(64)),
    );
    while let Some(row) = cursor.next().await? {
        let (key, materialized) = match row {
            CurrentOverlayRow::Tracked {
                encoded_key,
                value,
                source,
            } => {
                let key = decode_state_key(&encoded_key)?;
                let materialized = materialize_state_value(
                    value,
                    source,
                    key.entity_pk.clone(),
                    key.schema_key.clone(),
                    key.file_id.clone(),
                    branch_id_text(branch_id),
                );
                (key, materialized)
            }
            CurrentOverlayRow::Untracked { key, owner, value } => {
                let materialized = materialize_untracked_row(
                    value,
                    key.entity_pk.clone(),
                    key.schema_key.clone(),
                    key.file_id.clone(),
                    branch_id_text(owner),
                    owner == global_branch_id(),
                );
                (key, materialized)
            }
        };
        if !request.filter.schema_keys.is_empty()
            && !request
                .filter
                .schema_keys
                .iter()
                .any(|schema| schema == &key.schema_key)
        {
            continue;
        }
        if !request.filter.entity_pks.is_empty()
            && !request
                .filter
                .entity_pks
                .iter()
                .any(|entity| entity == &key.entity_pk)
        {
            continue;
        }
        if !request
            .filter
            .file_ids
            .iter()
            .all(|filter| filter.matches(key.file_id.as_ref()))
        {
            continue;
        }
        if materialized.deleted && !request.filter.include_tombstones {
            continue;
        }
        output.push_owned(materialized);
        if request.limit.is_some_and(|limit| output.len() >= limit) {
            break;
        }
    }
    Ok(output.finish())
}

struct UntrackedOwnerCursor<'a> {
    owner: CanonicalBranchId,
    cursor: ScanCursor<'a>,
    page: Vec<ReadEntry>,
    index: usize,
    has_more: bool,
}

impl<'a> UntrackedOwnerCursor<'a> {
    fn new(owner: CanonicalBranchId, cursor: ScanCursor<'a>) -> Self {
        Self {
            owner,
            cursor,
            page: Vec::new(),
            index: 0,
            has_more: true,
        }
    }

    async fn next(
        &mut self,
    ) -> Result<
        Option<(
            Vec<u8>,
            CanonicalBranchId,
            crate::forktree::StateKey,
            crate::forktree::UntrackedValue,
        )>,
        LixError,
    > {
        loop {
            if self.index == self.page.len() {
                if !self.has_more {
                    return Ok(None);
                }
                let chunk = self.cursor.next_page(256).await?;
                self.page = chunk.entries;
                self.index = 0;
                self.has_more = chunk.has_more;
                if self.page.is_empty() && !self.has_more {
                    return Ok(None);
                }
            }
            let entry = self.page[self.index].clone();
            self.index += 1;
            let (owner, key) = crate::forktree::decode_untracked_key(&entry.key.0)?;
            if owner != self.owner {
                return Err(LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "ForkTree untracked owner range returned a row for another owner",
                ));
            }
            let value = match entry.value {
                ProjectedValue::FullValue(bytes) => {
                    crate::forktree::decode_untracked_value(&bytes)?
                }
                ProjectedValue::KeyOnly => {
                    return Err(LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        "ForkTree untracked scan returned key-only data",
                    ));
                }
            };
            let encoded_key = encode_state_key(StateKeyRef {
                schema_key: &key.schema_key,
                file_id: key.file_id.as_deref(),
                entity_pk: &key.entity_pk,
            });
            return Ok(Some((encoded_key, owner, key, value)));
        }
    }
}

struct UntrackedOverlayCursor<'a> {
    local: UntrackedOwnerCursor<'a>,
    global: Option<UntrackedOwnerCursor<'a>>,
    local_next: Option<(
        Vec<u8>,
        CanonicalBranchId,
        crate::forktree::StateKey,
        crate::forktree::UntrackedValue,
    )>,
    global_next: Option<(
        Vec<u8>,
        CanonicalBranchId,
        crate::forktree::StateKey,
        crate::forktree::UntrackedValue,
    )>,
}

impl<'a> UntrackedOverlayCursor<'a> {
    async fn new<R>(view: &'a crate::forktree::CoherentView<R>) -> Result<Self, LixError>
    where
        R: StorageAdapterRead,
    {
        let branch = view.branch_id();
        let local =
            UntrackedOwnerCursor::new(branch, view.begin_untracked_owner_scan(branch).await?);
        let global = if branch == global_branch_id() {
            None
        } else {
            let owner = global_branch_id();
            Some(UntrackedOwnerCursor::new(
                owner,
                view.begin_untracked_owner_scan(owner).await?,
            ))
        };
        Ok(Self {
            local,
            global,
            local_next: None,
            global_next: None,
        })
    }

    async fn next(
        &mut self,
    ) -> Result<
        Option<(
            Vec<u8>,
            CanonicalBranchId,
            crate::forktree::StateKey,
            crate::forktree::UntrackedValue,
        )>,
        LixError,
    > {
        if self.local_next.is_none() {
            self.local_next = self.local.next().await?;
        }
        if let Some(global) = self.global.as_mut() {
            if self.global_next.is_none() {
                self.global_next = global.next().await?;
            }
        }
        let take_local = match (&self.local_next, &self.global_next) {
            (None, None) => return Ok(None),
            (None, Some(_)) => false,
            (Some(_), None) => true,
            (Some((local_key, ..)), Some((global_key, ..))) => local_key <= global_key,
        };
        if take_local {
            let row = self.local_next.take().expect("local row is present");
            if self
                .global_next
                .as_ref()
                .is_some_and(|(global_key, ..)| global_key == &row.0)
            {
                self.global_next = None;
            }
            Ok(Some(row))
        } else {
            Ok(self.global_next.take())
        }
    }
}

enum CurrentOverlayRow {
    Tracked {
        encoded_key: Vec<u8>,
        value: crate::forktree::StateValue,
        source: StateSource,
    },
    Untracked {
        key: crate::forktree::StateKey,
        owner: CanonicalBranchId,
        value: crate::forktree::UntrackedValue,
    },
}

struct CurrentOverlayCursor<'a, R: ?Sized> {
    tracked: crate::forktree::StateRangeCursor<'a, R>,
    untracked: UntrackedOverlayCursor<'a>,
    tracked_next: Option<(Vec<u8>, crate::forktree::StateValue, StateSource)>,
    untracked_next: Option<(
        Vec<u8>,
        CanonicalBranchId,
        crate::forktree::StateKey,
        crate::forktree::UntrackedValue,
    )>,
}

impl<'a, R> CurrentOverlayCursor<'a, R>
where
    R: StorageAdapterRead,
{
    async fn new(view: &'a crate::forktree::CoherentView<R>) -> Result<Self, LixError> {
        Ok(Self {
            tracked: state_range_cursor(view, None, None, true)?,
            untracked: UntrackedOverlayCursor::new(view).await?,
            tracked_next: None,
            untracked_next: None,
        })
    }

    async fn next(&mut self) -> Result<Option<CurrentOverlayRow>, LixError> {
        if self.tracked_next.is_none() {
            self.tracked_next = self.tracked.next().await?;
        }
        if self.untracked_next.is_none() {
            self.untracked_next = self.untracked.next().await?;
        }
        let take_untracked = match (&self.tracked_next, &self.untracked_next) {
            (None, None) => return Ok(None),
            (None, Some(_)) => true,
            (Some(_), None) => false,
            (Some((tracked_key, ..)), Some((untracked_key, ..))) => untracked_key <= tracked_key,
        };
        if take_untracked {
            let row = self
                .untracked_next
                .take()
                .expect("untracked row is present");
            if self
                .tracked_next
                .as_ref()
                .is_some_and(|(tracked_key, ..)| tracked_key == &row.0)
            {
                self.tracked_next = None;
            }
            Ok(Some(CurrentOverlayRow::Untracked {
                key: row.2,
                owner: row.1,
                value: row.3,
            }))
        } else {
            let row = self.tracked_next.take().expect("tracked row is present");
            Ok(Some(CurrentOverlayRow::Tracked {
                encoded_key: row.0,
                value: row.1,
                source: row.2,
            }))
        }
    }
}

fn merge_current_overlay(
    tracked: MaterializedLiveStateBatch,
    untracked: MaterializedLiveStateBatch,
    include_tombstones: bool,
    limit: Option<usize>,
) -> Result<MaterializedLiveStateBatch, LixError> {
    let tracked_keys = (0..tracked.len())
        .map(|index| encode_row_key_ref(tracked.row(index)))
        .collect::<Vec<_>>();
    let untracked_keys = (0..untracked.len())
        .map(|index| encode_row_key_ref(untracked.row(index)))
        .collect::<Vec<_>>();
    if tracked_keys.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(overlay_corruption("tracked"));
    }
    if untracked_keys.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(overlay_corruption("untracked"));
    }
    let mut tracked_index = 0;
    let mut untracked_index = 0;
    let mut output =
        MaterializedLiveStateBatchBuilder::with_capacity(limit.map_or(64, |limit| limit.min(64)));
    while tracked_index < tracked.len() || untracked_index < untracked.len() {
        if limit.is_some_and(|limit| output.len() >= limit) {
            break;
        }
        let take_untracked = match (
            tracked_keys.get(tracked_index),
            untracked_keys.get(untracked_index),
        ) {
            (None, Some(_)) => true,
            (Some(_), None) => false,
            (Some(tracked_key), Some(untracked_key)) => untracked_key <= tracked_key,
            (None, None) => break,
        };
        let row = if take_untracked {
            let row = untracked.row(untracked_index);
            let key = &untracked_keys[untracked_index];
            untracked_index += 1;
            if tracked_keys
                .get(tracked_index)
                .is_some_and(|tracked_key| tracked_key == key)
            {
                tracked_index += 1;
            }
            row
        } else {
            let row = tracked.row(tracked_index);
            tracked_index += 1;
            row
        };
        if row.deleted() && !include_tombstones {
            continue;
        }
        output.push_ref(row, None);
    }
    Ok(output.finish())
}

fn encode_row_key_ref(row: MaterializedLiveStateRowRef<'_>) -> Vec<u8> {
    encode_state_key(StateKeyRef {
        schema_key: row.schema_key(),
        file_id: row.file_id(),
        entity_pk: row.entity_pk(),
    })
}

/// Reads current untracked rows through the same authenticated selector view
/// as tracked rows. The raw untracked space is owned and decoded here; no
/// caller receives a space, key, or alternate serving authority.
pub(crate) async fn scan_untracked_view<R>(
    view: &crate::forktree::CoherentView<R>,
    request: &LiveStateScanRequest,
) -> Result<MaterializedLiveStateBatch, LixError>
where
    R: StorageAdapterRead,
{
    validate_scan_request(request)?;
    if !request.filter.constraints.is_empty()
        || !matches!(request.filter.rows, LiveStateRowFilter::All)
    {
        return Err(unsupported(
            "current ForkTree reader does not yet own this untracked scan lane",
        ));
    }
    let [branch_id] = request.filter.branch_ids.as_slice() else {
        return Err(unsupported(
            "current ForkTree untracked reader requires one branch",
        ));
    };
    let branch_id = parse_branch_id(branch_id)?;
    if view.branch_id() != branch_id {
        return Err(unsupported(
            "current ForkTree untracked view does not match requested branch",
        ));
    }
    let mut output = MaterializedLiveStateBatchBuilder::with_capacity(
        request.limit.map_or(64, |limit| limit.min(64)),
    );
    let mut cursor = UntrackedOverlayCursor::new(view).await?;
    while let Some((_encoded_key, owner, key, value)) = cursor.next().await? {
        if !request.filter.schema_keys.is_empty()
            && !request
                .filter
                .schema_keys
                .iter()
                .any(|schema| schema == &key.schema_key)
        {
            continue;
        }
        if !request.filter.entity_pks.is_empty()
            && !request
                .filter
                .entity_pks
                .iter()
                .any(|entity| entity == &key.entity_pk)
        {
            continue;
        }
        if !request
            .filter
            .file_ids
            .iter()
            .all(|filter| filter.matches(key.file_id.as_ref()))
        {
            continue;
        }
        if value.cell.deleted() && !request.filter.include_tombstones {
            continue;
        }
        let owner_is_global = owner.as_bytes() == global_branch_id().as_bytes();
        output.push_owned(materialize_untracked_row(
            value,
            key.entity_pk,
            key.schema_key,
            key.file_id,
            branch_id_text(owner),
            owner_is_global,
        ));
        if request.limit.is_some_and(|limit| output.len() >= limit) {
            break;
        }
    }
    Ok(output.finish())
}

/// Loads correlated current-state identities from a view borrowed from an
/// operation-owned facade. This is the exact-batch counterpart to
/// [`scan_facade`]; request slots and duplicate identities remain aligned.
pub(crate) async fn load_exact_facade<R>(
    facade: &ForkTreeReadFacade<R>,
    request: &LiveStateExactBatchRequest,
) -> Result<MaterializedLiveStateExactBatch, LixError>
where
    R: StorageAdapterRead,
{
    validate_exact_request(request)?;
    if request.rows.is_empty() {
        return Ok(MaterializedLiveStateExactBatch::default());
    }
    let view = facade.branch(&request.rows[0].branch_id).await?;
    load_exact_view(&view, request).await
}

/// The ForkTree owner itself satisfies the engine read capability. This keeps
/// transaction overlays on the caller-owned facade instead of embedding the
/// superseded legacy current-state reader as a second current-state owner.
#[async_trait]
impl<R> crate::live_state::LiveStateReader for ForkTreeReadFacade<R>
where
    R: StorageAdapterRead,
{
    async fn scan_batch(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        scan_facade(self, request).await
    }

    async fn load_exact_batch(
        &self,
        request: &LiveStateExactBatchRequest,
    ) -> Result<MaterializedLiveStateExactBatch, LixError> {
        load_exact_facade(self, request).await
    }

    async fn collection_generation(
        &self,
        branch_id: &str,
        scope: crate::collection_generation::CollectionScopeRef<'_>,
    ) -> Result<Option<crate::collection_generation::CollectionGeneration>, LixError> {
        let marker_schema = crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY;
        let rows = scan_facade(
            self,
            &LiveStateScanRequest {
                filter: crate::live_state::LiveStateFilter {
                    schema_keys: vec![marker_schema.to_owned(), scope.schema_key.to_owned()],
                    branch_ids: vec![branch_id.to_owned()],
                    untracked: Some(false),
                    include_tombstones: true,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await?;
        let expected_scope = crate::collection_generation::collection_scope_key(scope);
        let mut marker = None;
        for row in rows.iter() {
            if row.schema_key() != marker_schema
                || row.entity_pk() != &EntityPk::single(&expected_scope)
                || row.file_id().is_some()
            {
                continue;
            }
            if marker.is_some() {
                return Err(LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "collection generation has duplicate authenticated marker rows",
                ));
            }
            if row.global() || row.untracked() || row.branch_id() != branch_id {
                return Err(LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "collection generation marker has the wrong authenticated branch",
                ));
            }
            if row.deleted() || row.snapshot_content().is_none() {
                return Ok(None);
            }
            let snapshot = serde_json::from_str::<serde_json::Value>(
                row.snapshot_content()
                    .expect("checked collection generation snapshot")
                    .as_str(),
            )
            .map_err(|error| {
                LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    format!("collection generation row is malformed: {error}"),
                )
            })?;
            if snapshot
                .get("scope_key")
                .and_then(serde_json::Value::as_str)
                != Some(expected_scope.as_str())
                || snapshot
                    .get("schema_key")
                    .and_then(serde_json::Value::as_str)
                    != Some(scope.schema_key)
                || snapshot.get("file_id").and_then(serde_json::Value::as_str) != scope.file_id
            {
                return Err(LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "collection generation row identity does not match its requested scope",
                ));
            }
            let live_count = snapshot
                .get("live_count")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        "collection generation row is missing live_count",
                    )
                })?;
            let active_generation = row.commit_id().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "collection generation row is missing its authenticated commit identity",
                )
            })?;
            marker = Some((active_generation, live_count));
        }
        let Some((active_generation, live_count)) = marker else {
            return Ok(None);
        };
        let ordered_identity_digest = authenticated_ordered_generation_digest(
            &rows,
            branch_id,
            scope,
            active_generation,
            live_count,
        )?;
        Ok(Some(crate::collection_generation::CollectionGeneration {
            active_generation,
            live_count,
            ordered_identity_digest,
        }))
    }
}

fn authenticated_ordered_generation_digest(
    rows: &MaterializedLiveStateBatch,
    branch_id: &str,
    scope: crate::collection_generation::CollectionScopeRef<'_>,
    active_generation: crate::changelog::CommitId,
    live_count: u64,
) -> Result<Option<[u8; 32]>, LixError> {
    if live_count == 0 || live_count == crate::collection_generation::DEFERRED_LIVE_COUNT {
        return Ok(None);
    }
    let mut identities = Vec::new();
    for row in rows.iter() {
        if row.schema_key() != scope.schema_key || row.file_id() != scope.file_id {
            continue;
        }
        if row.global()
            || row.untracked()
            || row.branch_id() != branch_id
            || row.deleted()
            || row.snapshot_content().is_none()
            || row.commit_id() != Some(active_generation)
        {
            return Err(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "collection generation member has invalid authenticated identity",
            ));
        }
        identities.push(row.entity_pk());
    }
    if identities.len() != usize::try_from(live_count).unwrap_or(usize::MAX) {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "collection generation live_count does not match authenticated members",
        ));
    }
    crate::collection_generation::ordered_single_string_identity_digest(identities.into_iter())
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "collection generation member identity is not an ordered single string",
            )
        })
        .map(Some)
}

async fn load_exact_view<R>(
    view: &crate::forktree::CoherentView<R>,
    request: &LiveStateExactBatchRequest,
) -> Result<MaterializedLiveStateExactBatch, LixError>
where
    R: StorageAdapterRead,
{
    let mut builder = MaterializedLiveStateBatchBuilder::with_capacity(request.rows.len());
    let mut slots = Vec::with_capacity(request.rows.len());

    let requested_keys = request
        .rows
        .iter()
        .map(|requested| {
            encode_state_key(StateKeyRef {
                schema_key: &requested.schema_key,
                file_id: requested.file_id.as_deref(),
                entity_pk: &requested.entity_pk,
            })
        })
        .collect::<Vec<_>>();
    let tracked = if request.untracked == Some(true) {
        vec![None; request.rows.len()]
    } else {
        state_points(view, &requested_keys, true).await?
    };
    let untracked = if request.untracked == Some(false) {
        vec![None; request.rows.len()]
    } else {
        view.load_untracked_overlay_points(&requested_keys).await?
    };
    if tracked.len() != request.rows.len() || untracked.len() != request.rows.len() {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "ForkTree exact state lookup returned the wrong slot count",
        ));
    }
    for ((requested, tracked), untracked) in request.rows.iter().zip(tracked).zip(untracked) {
        let materialized = if let Some((owner, key, value)) = untracked {
            (!value.cell.deleted() || request.include_tombstones).then(|| {
                materialize_untracked_row(
                    value,
                    key.entity_pk,
                    key.schema_key,
                    key.file_id,
                    branch_id_text(owner),
                    owner.as_bytes() == global_branch_id().as_bytes(),
                )
            })
        } else if let Some(row) = tracked {
            (!row.value.cell.deleted() || request.include_tombstones)
                .then(|| {
                    let decoded_key = decode_state_key(&row.encoded_key)?;
                    Ok::<_, LixError>(materialize_row(
                        row,
                        decoded_key.entity_pk,
                        decoded_key.schema_key,
                        decoded_key.file_id,
                        requested.branch_id.clone(),
                    ))
                })
                .transpose()?
        } else {
            None
        };
        let Some(materialized) = materialized else {
            slots.push(None);
            continue;
        };
        builder.push_owned(materialized);
        let ordinal = u32::try_from(builder.len().saturating_sub(1)).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "exact live-state result exceeds u32 rows",
            )
        })?;
        slots.push(Some(ordinal));
    }
    MaterializedLiveStateExactBatch::new(builder.finish(), slots)
}

fn validate_scan_request(request: &LiveStateScanRequest) -> Result<(), LixError> {
    if request
        .filter
        .schema_keys
        .iter()
        .any(|schema_key| is_derived_schema(schema_key))
    {
        return Err(unsupported(
            "current ForkTree reader does not serve derived or history schemas",
        ));
    }
    Ok(())
}

fn global_branch_id() -> CanonicalBranchId {
    let uuid =
        uuid::Uuid::parse_str(crate::GLOBAL_BRANCH_ID).expect("GLOBAL_BRANCH_ID must be a UUID");
    CanonicalBranchId::from_bytes(*uuid.as_bytes())
}

fn validate_exact_request(request: &LiveStateExactBatchRequest) -> Result<(), LixError> {
    let Some(first) = request.rows.first() else {
        return Ok(());
    };
    if request
        .rows
        .iter()
        .any(|row| is_derived_schema(&row.schema_key))
    {
        return Err(unsupported(
            "current ForkTree reader does not serve derived or history schemas",
        ));
    }
    if request
        .rows
        .iter()
        .any(|row| row.branch_id != first.branch_id)
    {
        return Err(unsupported(
            "current ForkTree exact reader requires one branch per coherent view",
        ));
    }
    Ok(())
}

fn parse_branch_id(value: &str) -> Result<CanonicalBranchId, LixError> {
    let uuid = uuid::Uuid::parse_str(value).map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("branch ID must be a UUID: {error}"),
        )
    })?;
    Ok(CanonicalBranchId::from_bytes(*uuid.as_bytes()))
}

fn branch_id_text(branch_id: CanonicalBranchId) -> String {
    uuid::Uuid::from_bytes(*branch_id.as_bytes()).to_string()
}

fn materialize_state_value(
    value: crate::forktree::StateValue,
    source: StateSource,
    entity_pk: EntityPk,
    schema_key: String,
    file_id: Option<String>,
    branch_id: String,
) -> MaterializedLiveStateRow {
    let deleted = value.cell.deleted();
    let snapshot_content = match &value.cell {
        StateCell::Value(value) => Some(value.clone()),
        StateCell::Null | StateCell::Tombstone => None,
    };
    MaterializedLiveStateRow {
        entity_pk,
        schema_key,
        file_id,
        snapshot_content,
        metadata: value.metadata,
        deleted,
        created_at: value.created_at,
        updated_at: value.updated_at,
        global: matches!(source, StateSource::Global),
        change_id: Some(value.change_id),
        commit_id: Some(value.commit_id),
        untracked: false,
        branch_id: Arc::from(branch_id),
    }
}

fn materialize_row(
    row: crate::forktree::VisibleStateRow,
    entity_pk: EntityPk,
    schema_key: String,
    file_id: Option<String>,
    branch_id: String,
) -> MaterializedLiveStateRow {
    let deleted = row.value.cell.deleted();
    let snapshot_content = match &row.value.cell {
        StateCell::Value(value) => Some(value.clone()),
        StateCell::Null | StateCell::Tombstone => None,
    };
    MaterializedLiveStateRow {
        entity_pk,
        schema_key,
        file_id,
        snapshot_content,
        metadata: row.value.metadata,
        deleted,
        created_at: row.value.created_at,
        updated_at: row.value.updated_at,
        global: matches!(row.source, StateSource::Global),
        change_id: Some(row.value.change_id),
        commit_id: Some(row.value.commit_id),
        untracked: false,
        branch_id: Arc::from(branch_id),
    }
}

fn materialize_untracked_row(
    value: crate::forktree::UntrackedValue,
    entity_pk: EntityPk,
    schema_key: String,
    file_id: Option<String>,
    branch_id: String,
    global: bool,
) -> MaterializedLiveStateRow {
    let deleted = value.cell.deleted();
    let snapshot_content = match &value.cell {
        StateCell::Value(value) => Some(value.clone()),
        StateCell::Null | StateCell::Tombstone => None,
    };
    MaterializedLiveStateRow {
        entity_pk,
        schema_key,
        file_id,
        snapshot_content,
        metadata: value.metadata,
        deleted,
        created_at: value.created_at,
        updated_at: value.updated_at,
        global,
        change_id: None,
        commit_id: None,
        untracked: true,
        branch_id: Arc::from(branch_id),
    }
}

fn unsupported(message: &'static str) -> LixError {
    LixError::new(LixError::CODE_INTERNAL_ERROR, message)
}

fn overlay_corruption(stream: &'static str) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("current ForkTree overlay contains duplicate logical key in {stream} rows"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::LixTimestamp;
    use crate::entity_pk::EntityPk;
    use crate::live_state::{LiveStateExactRowRequest, MaterializedLiveStateRow};

    fn exact(schema_key: &str) -> LiveStateExactBatchRequest {
        LiveStateExactBatchRequest {
            rows: vec![LiveStateExactRowRequest {
                schema_key: schema_key.to_owned(),
                branch_id: "01920000-0000-7000-8000-0000000000a1".to_owned(),
                entity_pk: EntityPk::single("row"),
                file_id: None,
            }],
            ..Default::default()
        }
    }

    #[test]
    fn scan_rejects_derived_schema_before_view_acquisition() {
        let request = LiveStateScanRequest {
            filter: crate::live_state::LiveStateFilter {
                schema_keys: vec!["lix_commit".to_owned()],
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(validate_scan_request(&request).is_err());
    }

    #[test]
    fn exact_rejects_history_before_view_acquisition() {
        assert!(validate_exact_request(&exact("lix_commit")).is_err());
        assert!(validate_exact_request(&exact("lix_commit_edge")).is_err());
    }

    #[test]
    fn exact_rejects_cross_branch_batches_before_view_acquisition() {
        let mut request = exact("app.schema");
        request.rows.push(LiveStateExactRowRequest {
            branch_id: "01920000-0000-7000-8000-0000000000a2".to_owned(),
            ..request.rows[0].clone()
        });
        assert!(validate_exact_request(&request).is_err());
    }

    fn row(
        branch_id: &str,
        entity_pk: &str,
        value: Option<&str>,
        deleted: bool,
        global: bool,
        untracked: bool,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: EntityPk::single(entity_pk),
            schema_key: "entity".to_string(),
            file_id: None,
            snapshot_content: value.map(|value| format!(r#"{{"value":"{value}"}}"#).into()),
            metadata: None,
            deleted,
            created_at: LixTimestamp::expect_parse("created_at", "2026-01-01T00:00:00Z"),
            updated_at: LixTimestamp::expect_parse("updated_at", "2026-01-01T00:00:00Z"),
            global,
            change_id: Some(ChangeId::for_test_label("change")),
            commit_id: Some(CommitId::for_test_label("commit")),
            untracked,
            branch_id: branch_id.into(),
        }
    }

    fn generation_member(entity_pk: &str) -> MaterializedLiveStateRow {
        let mut row = row("branch", entity_pk, Some(entity_pk), false, false, false);
        row.schema_key = "entity".to_owned();
        row.commit_id = Some(CommitId::for_test_label("generation"));
        row
    }

    #[test]
    fn ordered_generation_digest_binds_members_and_order() {
        let rows = MaterializedLiveStateBatch::from_rows(vec![
            generation_member("a"),
            generation_member("b"),
        ]);
        let digest = authenticated_ordered_generation_digest(
            &rows,
            "branch",
            crate::collection_generation::CollectionScopeRef {
                schema_key: "entity",
                file_id: None,
            },
            CommitId::for_test_label("generation"),
            2,
        )
        .expect("authenticated generation digest")
        .expect("non-empty generation has digest");
        assert_eq!(
            digest,
            crate::collection_generation::ordered_single_string_identity_digest(
                rows.iter().map(|row| row.entity_pk()),
            )
            .expect("single-string identities")
        );
    }

    #[test]
    fn ordered_generation_digest_rejects_wrong_member_identity() {
        let mut substituted = generation_member("a");
        substituted.commit_id = Some(CommitId::for_test_label("other-generation"));
        let rows = MaterializedLiveStateBatch::from_rows(vec![substituted]);
        let error = authenticated_ordered_generation_digest(
            &rows,
            "branch",
            crate::collection_generation::CollectionScopeRef {
                schema_key: "entity",
                file_id: None,
            },
            CommitId::for_test_label("generation"),
            1,
        )
        .expect_err("substituted generation member must fail closed");
        assert_eq!(error.code, LixError::CODE_STORAGE_ERROR);
    }

    #[test]
    fn combined_overlay_replaces_tracked_and_preserves_order_and_limit() {
        let tracked = MaterializedLiveStateBatch::from_rows(vec![
            row("global", "a", Some("global-a"), false, true, false),
            row("branch", "b", Some("tracked-b"), false, false, false),
            row("branch", "c", Some("tracked-c"), false, false, false),
        ]);
        let untracked = MaterializedLiveStateBatch::from_rows(vec![
            row("branch", "a", Some("untracked-a"), false, false, true),
            row("branch", "b", Some("untracked-b"), false, false, true),
            row("branch", "d", Some("untracked-d"), false, false, true),
        ]);

        let rows = merge_current_overlay(tracked.clone(), untracked.clone(), false, None)
            .expect("distinct physical overlay rows should merge")
            .into_rows();
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].entity_pk, EntityPk::single("a"));
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some(r#"{"value":"untracked-a"}"#)
        );
        assert!(rows[0].untracked);
        assert_eq!(rows[1].entity_pk, EntityPk::single("b"));
        assert_eq!(
            rows[1].snapshot_content.as_deref(),
            Some(r#"{"value":"untracked-b"}"#)
        );
        assert!(rows[1].untracked);
        assert_eq!(rows[2].entity_pk, EntityPk::single("c"));
        assert_eq!(rows[3].entity_pk, EntityPk::single("d"));

        let limited = merge_current_overlay(tracked, untracked, false, Some(3))
            .expect("distinct physical overlay rows should preserve limit")
            .into_rows();
        assert_eq!(
            limited
                .iter()
                .map(|row| row.entity_pk.clone())
                .collect::<Vec<_>>(),
            vec![
                EntityPk::single("a"),
                EntityPk::single("b"),
                EntityPk::single("c"),
            ]
        );
    }

    #[test]
    fn combined_overlay_tombstone_masks_value_but_null_remains_visible() {
        let tracked = MaterializedLiveStateBatch::from_rows(vec![
            row("branch", "deleted", Some("old"), false, false, false),
            row("branch", "null", None, false, false, false),
        ]);
        let untracked = MaterializedLiveStateBatch::from_rows(vec![row(
            "branch", "deleted", None, true, false, true,
        )]);

        let visible = merge_current_overlay(tracked.clone(), untracked.clone(), false, None)
            .expect("cross-stream tombstone should mask tracked value")
            .into_rows();
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].entity_pk, EntityPk::single("null"));
        assert_eq!(visible[0].snapshot_content, None);
        assert!(!visible[0].deleted);

        let with_tombstone = merge_current_overlay(tracked, untracked, true, None)
            .expect("cross-stream tombstone should remain visible when requested")
            .into_rows();
        assert_eq!(with_tombstone.len(), 2);
        assert!(
            with_tombstone
                .iter()
                .any(|row| row.entity_pk == EntityPk::single("deleted") && row.deleted)
        );
    }

    #[test]
    fn combined_overlay_rejects_duplicate_tracked_logical_keys() {
        let duplicate = row("branch", "duplicate", Some("value"), false, false, false);
        let error = merge_current_overlay(
            MaterializedLiveStateBatch::from_rows(vec![duplicate.clone(), duplicate]),
            MaterializedLiveStateBatch::default(),
            false,
            None,
        )
        .expect_err("duplicate tracked rows must fail closed");
        assert_eq!(error.code, LixError::CODE_INTERNAL_ERROR);
        assert!(error.message.contains("tracked"));
    }

    #[test]
    fn combined_overlay_rejects_duplicate_untracked_logical_keys() {
        let duplicate = row("branch", "duplicate", Some("value"), false, false, true);
        let error = merge_current_overlay(
            MaterializedLiveStateBatch::default(),
            MaterializedLiveStateBatch::from_rows(vec![duplicate.clone(), duplicate]),
            false,
            None,
        )
        .expect_err("duplicate untracked rows must fail closed");
        assert_eq!(error.code, LixError::CODE_INTERNAL_ERROR);
        assert!(error.message.contains("untracked"));
    }
}
