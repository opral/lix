//! Current-state reader backed only by the authenticated ForkTree view.
//!
//! This adapter is deliberately small while the rest of the historical
//! reader conversion is in flight. It accepts the single-branch tracked
//! serving shape, keeps the caller's existing `StorageRead`, and refuses
//! lanes that still need a separate owner instead of falling back to a
//! deleted current-layout reader.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;

use crate::LixError;
use crate::entity_pk::EntityPk;
use crate::forktree::{
    CanonicalBranchId, ForkTreeReadFacade, StateCell, StateKeyRef, StateSource, decode_state_key,
    encode_state_key, state_point, state_range,
};
use crate::live_state::{
    LiveStateExactBatchRequest, LiveStateExactRowRequest, LiveStateRowFilter, LiveStateScanRequest,
    MaterializedLiveStateBatch, MaterializedLiveStateBatchBuilder, MaterializedLiveStateExactBatch,
    MaterializedLiveStateRow,
};
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
    let concrete_branch_ids = branch_ids
        .iter()
        .filter(|branch_id| branch_id.as_str() != crate::GLOBAL_BRANCH_ID)
        .collect::<Vec<_>>();
    if concrete_branch_ids.is_empty() {
        if facade.bound_branch_id().is_some() {
            return Err(unsupported(
                "branch-bound ForkTree reader cannot serve a global-only request",
            ));
        }
        let view = facade.branch(crate::GLOBAL_BRANCH_ID).await?;
        return scan_view(&view, request).await;
    }
    if let Some(bound_branch_id) = facade.bound_branch_id() {
        if concrete_branch_ids
            .iter()
            .any(|branch_id| branch_id.as_str() != bound_branch_id)
        {
            return Err(unsupported(
                "branch-bound ForkTree reader cannot serve multiple branch identities",
            ));
        }
        let view = facade.branch(bound_branch_id).await?;
        let mut branch_request = request.clone();
        branch_request.filter.branch_ids = vec![bound_branch_id.to_owned()];
        return scan_view(&view, &branch_request).await;
    }
    if concrete_branch_ids.len() == 1 {
        let view = facade.branch(concrete_branch_ids[0]).await?;
        let mut branch_request = request.clone();
        branch_request.filter.branch_ids = vec![concrete_branch_ids[0].to_string()];
        return scan_view(&view, &branch_request).await;
    }

    // An explicit by-branch surface may enumerate several authenticated
    // branch views. Each view borrows the same operation-owned read; its
    // global overlay is intentionally materialized once per branch, matching
    // the public lix_file_by_branch identity contract. Apply LIMIT only after
    // the ordered branch streams have been concatenated.
    let mut rows = Vec::new();
    for branch_id in concrete_branch_ids {
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
    let rows = state_range(&view, None, None, None, true).await?;
    let mut output = Vec::with_capacity(rows.len());
    for row in rows {
        let key = decode_state_key(&row.encoded_key)?;
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
        if row.value.cell.deleted() && !request.filter.include_tombstones {
            continue;
        }
        let branch_owner = match row.source {
            StateSource::Global | StateSource::Branch => branch_id_text(branch_id),
        };
        output.push(materialize_row(
            row,
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
    let mut candidate_request = request.clone();
    candidate_request.filter.include_tombstones = true;
    candidate_request.limit = None;
    let tracked = scan_tracked_view(view, &candidate_request).await?;
    let untracked = scan_untracked_view(view, &candidate_request).await?;
    merge_current_overlay(
        tracked,
        untracked,
        request.filter.include_tombstones,
        request.limit,
    )
}

fn merge_current_overlay(
    tracked: MaterializedLiveStateBatch,
    untracked: MaterializedLiveStateBatch,
    include_tombstones: bool,
    limit: Option<usize>,
) -> Result<MaterializedLiveStateBatch, LixError> {
    let mut by_key = BTreeMap::new();
    let mut tracked_keys = BTreeSet::new();
    for row in tracked.into_rows() {
        let key = encode_row_key(&row);
        if !tracked_keys.insert(key.clone()) {
            return Err(overlay_corruption("tracked"));
        }
        by_key.insert(key, row);
    }
    let mut untracked_keys = BTreeSet::new();
    for row in untracked.into_rows() {
        let key = encode_row_key(&row);
        if !untracked_keys.insert(key.clone()) {
            return Err(overlay_corruption("untracked"));
        }
        by_key.insert(key, row);
    }

    let mut output = Vec::with_capacity(limit.unwrap_or(by_key.len()).min(by_key.len()));
    for row in by_key.into_values() {
        if row.deleted && !include_tombstones {
            continue;
        }
        output.push(row);
        if limit.is_some_and(|limit| output.len() >= limit) {
            break;
        }
    }
    Ok(MaterializedLiveStateBatch::from_rows(output))
}

fn encode_row_key(row: &MaterializedLiveStateRow) -> Vec<u8> {
    encode_state_key(StateKeyRef {
        schema_key: &row.schema_key,
        file_id: row.file_id.as_deref(),
        entity_pk: &row.entity_pk,
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
    let owner_rows = merge_untracked_overlay_rows(view, view.scan_untracked_overlay_rows().await?)?;
    let mut rows = Vec::new();
    for (_encoded_key, (owner, key, value)) in owner_rows {
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
        rows.push(materialize_untracked_row(
            value,
            key.entity_pk,
            key.schema_key,
            key.file_id,
            branch_id_text(owner),
            owner_is_global,
        ));
        if request.limit.is_some_and(|limit| rows.len() >= limit) {
            break;
        }
    }
    Ok(MaterializedLiveStateBatch::from_rows(rows))
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
    if let Some(bound_branch_id) = facade.bound_branch_id()
        && request
            .rows
            .iter()
            .any(|row| row.branch_id != bound_branch_id)
    {
        return Err(unsupported(
            "branch-bound ForkTree exact reader cannot serve multiple branch identities",
        ));
    }
    let mut grouped = BTreeMap::<String, Vec<(usize, LiveStateExactRowRequest)>>::new();
    for (index, row) in request.rows.iter().cloned().enumerate() {
        grouped
            .entry(row.branch_id.clone())
            .or_default()
            .push((index, row));
    }
    if grouped.len() == 1 {
        let rows = grouped
            .into_values()
            .next()
            .expect("one exact branch group");
        let branch_id = rows[0].1.branch_id.clone();
        let view = facade.branch(&branch_id).await?;
        let mut branch_request = request.clone();
        branch_request.rows = rows.into_iter().map(|(_, row)| row).collect();
        return load_exact_view(&view, &branch_request).await;
    }

    let mut output = vec![None; request.rows.len()];
    for rows in grouped.into_values() {
        let branch_id = rows[0].1.branch_id.clone();
        let view = facade.branch(&branch_id).await?;
        let mut branch_request = request.clone();
        branch_request.rows = rows.iter().map(|(_, row)| row.clone()).collect();
        let branch_output = load_exact_view(&view, &branch_request).await?;
        for ((original_index, _), row) in rows.into_iter().zip(branch_output.into_rows()) {
            output[original_index] = row;
        }
    }
    let mut builder = MaterializedLiveStateBatchBuilder::with_capacity(output.len());
    let mut slots = Vec::with_capacity(output.len());
    for row in output {
        slots.push(row.map(|row| {
            let ordinal = u32::try_from(builder.len())
                .expect("exact live-state row count exceeds u32 ordinals");
            builder.push_owned(row);
            ordinal
        }));
    }
    MaterializedLiveStateExactBatch::new(builder.finish(), slots)
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

    if request.untracked == Some(true) {
        let untracked_rows =
            merge_untracked_overlay_rows(&view, view.scan_untracked_overlay_rows().await?)?;
        for requested in &request.rows {
            let key = encode_state_key(StateKeyRef {
                schema_key: &requested.schema_key,
                file_id: requested.file_id.as_deref(),
                entity_pk: &requested.entity_pk,
            });
            let Some((_owner, decoded_key, value)) = untracked_rows.get(&key) else {
                slots.push(None);
                continue;
            };
            let ordinal = u32::try_from(builder.len()).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "exact live-state result exceeds u32 rows",
                )
            })?;
            builder.push_owned(materialize_untracked_row(
                value.clone(),
                decoded_key.entity_pk.clone(),
                decoded_key.schema_key.clone(),
                decoded_key.file_id.clone(),
                branch_id_text(*_owner),
                _owner.as_bytes() == global_branch_id().as_bytes(),
            ));
            slots.push(Some(ordinal));
        }
        return MaterializedLiveStateExactBatch::new(builder.finish(), slots);
    }

    for requested in &request.rows {
        let key = encode_state_key(StateKeyRef {
            schema_key: &requested.schema_key,
            file_id: requested.file_id.as_deref(),
            entity_pk: &requested.entity_pk,
        });
        let Some(row) = state_point(&view, &key, request.include_tombstones).await? else {
            slots.push(None);
            continue;
        };
        let decoded_key = decode_state_key(&row.encoded_key)?;
        builder.push_owned(materialize_row(
            row,
            decoded_key.entity_pk,
            decoded_key.schema_key,
            decoded_key.file_id,
            requested.branch_id.clone(),
        ));
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

fn merge_untracked_overlay_rows(
    view: &crate::forktree::CoherentView<impl StorageAdapterRead>,
    rows: Vec<(
        CanonicalBranchId,
        crate::forktree::StateKey,
        crate::forktree::UntrackedValue,
    )>,
) -> Result<
    BTreeMap<
        Vec<u8>,
        (
            CanonicalBranchId,
            crate::forktree::StateKey,
            crate::forktree::UntrackedValue,
        ),
    >,
    LixError,
> {
    let branch_id = view.branch_id();
    let global_id = global_branch_id();
    let mut selected = BTreeMap::new();
    for (owner, key, value) in rows {
        let encoded_key = encode_state_key(StateKeyRef {
            schema_key: &key.schema_key,
            file_id: key.file_id.as_deref(),
            entity_pk: &key.entity_pk,
        });
        if let Some((existing_owner, _, _)) = selected.get(&encoded_key) {
            if *existing_owner == owner {
                return Err(overlay_corruption("untracked"));
            }
            if owner != branch_id {
                debug_assert_eq!(owner, global_id);
                continue;
            }
        }
        selected.insert(encoded_key, (owner, key, value));
    }
    Ok(selected)
}

fn validate_exact_request(request: &LiveStateExactBatchRequest) -> Result<(), LixError> {
    if request.rows.is_empty() {
        return Ok(());
    }
    if request
        .rows
        .iter()
        .any(|row| is_derived_schema(&row.schema_key))
    {
        return Err(unsupported(
            "current ForkTree reader does not serve derived or history schemas",
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
            row("branch", "null", None, false, false, false),
            row("branch", "deleted", Some("old"), false, false, false),
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
