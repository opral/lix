#![allow(clippy::borrow_deref_ref, clippy::clone_on_copy)]

use crate::GLOBAL_BRANCH_ID;
use crate::LixError;
use crate::NullableKeyFilter;
use crate::branch::{BRANCH_REF_SCHEMA_KEY, BranchHeadControl, BranchHeadControlContext};
use crate::commit_graph::CommitGraphContext;
use crate::entity_pk::EntityPk;
use crate::filesystem::{
    FilesystemPathIndex, FilesystemPathIndexCache, FilesystemPathIndexReader,
    FilesystemPathIndexRequest, build_path_index, load_path_index_revision,
};
use crate::live_state::index::{
    LiveStateIndexContext, LiveStateIndexFilter, LiveStateIndexScanRequest,
    load_untracked_schema_presence_marker,
};
use crate::live_state::tracked_head::TrackedHeadContext;
use crate::live_state::{
    LiveStateExactBatchRequest, LiveStateReader, LiveStateRowIdentity, LiveStateRowRequest,
    LiveStateScanRequest, MaterializedLiveStateRow, VisibilityBranchScope, VisibilityRequest,
    expanded_branch_ids, resolve_visible_rows,
};
use crate::storage_adapter::StorageAdapterRead;
use crate::tracked_state::{
    MaterializedTrackedStateRow, TrackedStateContext, TrackedStateFilter, TrackedStateReadColumns,
    TrackedStateScanRequest,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt, stream};

const BRANCH_READ_CONCURRENCY: usize = 8;
type BranchHeads = std::collections::BTreeMap<String, BranchHeadControl>;

const COMMIT_SCHEMA_KEY: &str = "lix_commit";
const COMMIT_EDGE_SCHEMA_KEY: &str = "lix_commit_edge";

/// Serving facade for visible live-state reads.
///
/// Tracked rows are resolved from the branch head's durable head projection;
/// sparse immutable roots and changelog replay are historical fallbacks.
/// Untracked and engine-owned current rows are resolved through a flat mutable
/// identity-to-change index, then both sources are combined for serving.
pub(crate) struct LiveStateContext {
    tracked_state: TrackedStateContext,
    tracked_head: TrackedHeadContext,
    live_index: LiveStateIndexContext,
    commit_graph: CommitGraphContext,
    filesystem_path_index_cache: std::sync::Arc<FilesystemPathIndexCache>,
}

impl LiveStateContext {
    pub(crate) fn new(
        tracked_state: TrackedStateContext,
        live_index: LiveStateIndexContext,
        commit_graph: CommitGraphContext,
    ) -> Self {
        Self {
            tracked_state,
            tracked_head: TrackedHeadContext::new(),
            live_index,
            commit_graph,
            filesystem_path_index_cache: std::sync::Arc::new(FilesystemPathIndexCache::default()),
        }
    }

    /// Creates a visible live-state reader over a caller-provided KV store.
    pub(crate) fn reader<S>(&self, store: S) -> LiveStateStoreReader<S>
    where
        S: StorageAdapterRead,
    {
        LiveStateStoreReader {
            store,
            tracked_state: self.tracked_state.clone(),
            tracked_head: self.tracked_head,
            live_index: self.live_index.clone(),
            commit_graph: self.commit_graph.clone(),
            filesystem_path_index_cache: std::sync::Arc::clone(&self.filesystem_path_index_cache),
        }
    }

    pub(crate) fn index(&self) -> &LiveStateIndexContext {
        &self.live_index
    }

    pub(crate) fn advance_filesystem_path_indexes(
        &self,
        previous_revision: Option<&[u8]>,
        next_revision: Option<&[u8]>,
        rows: &[MaterializedLiveStateRow],
    ) {
        self.filesystem_path_index_cache
            .advance_committed(previous_revision, next_revision, rows);
    }
}

/// Visible live-state reader backed by a caller-provided KV store.
pub(crate) struct LiveStateStoreReader<S> {
    store: S,
    tracked_state: TrackedStateContext,
    tracked_head: TrackedHeadContext,
    live_index: LiveStateIndexContext,
    commit_graph: CommitGraphContext,
    filesystem_path_index_cache: std::sync::Arc<FilesystemPathIndexCache>,
}

impl<S> LiveStateStoreReader<S>
where
    S: StorageAdapterRead,
{
    /// Returns raw tracked snapshot bytes for one broad SQL entity scan.
    ///
    /// `None` means the normal materialized visibility path remains
    /// authoritative: untracked rows, a global overlay, multiple result
    /// branch scopes, commit-derived state, or an unavailable tracked-head
    /// projection all deliberately fall back rather than approximating
    /// visibility.
    pub(crate) async fn scan_direct_entity_snapshots(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Option<Vec<Option<Bytes>>>, LixError> {
        // This capability relies on v10's complete untracked-schema marker.
        // Keep the proof at its storage-facing boundary so a future caller
        // cannot accidentally treat a v9 marker absence as tracked-only.
        if crate::init::repository_protocol_status(&self.store).await?
            != crate::init::RepositoryProtocolStatus::Current
        {
            return Ok(None);
        }
        if request.filter.untracked == Some(true)
            || request_may_include_commit_derived(request)
            || request
                .filter
                .schema_keys
                .iter()
                .any(|schema_key| schema_key == BRANCH_REF_SCHEMA_KEY)
        {
            return Ok(None);
        }
        let [schema_key] = request.filter.schema_keys.as_slice() else {
            return Ok(None);
        };
        let scope = scan_scope(&self.store, &self.live_index, request, true).await?;
        let [requested_branch_id] = scope.projection_branch_ids.as_slice() else {
            return Ok(None);
        };
        // A normal SQL entity query transparently includes an untracked row
        // when one exists. In a v10 repository the monotonic marker proves
        // that every relevant branch/schema is tracked-only; otherwise the
        // established merged visibility path remains authoritative. An
        // explicit tracked-only request does not need this proof.
        if request.filter.untracked.is_none() {
            for branch_id in &scope.storage_branch_ids {
                if load_untracked_schema_presence_marker(&self.store, branch_id, schema_key)
                    .await?
                    .is_some()
                {
                    return Ok(None);
                }
            }
        }
        if scope
            .storage_branch_ids
            .iter()
            .any(|branch_id| branch_id != requested_branch_id && branch_id != GLOBAL_BRANCH_ID)
        {
            return Ok(None);
        }
        let Some(requested_control) = scope.branch_heads.get(requested_branch_id).copied() else {
            return Ok(None);
        };
        let tracked_head = self.tracked_head.reader(&self.store);
        if requested_branch_id != GLOBAL_BRANCH_ID
            && let Some(global_control) = scope.branch_heads.get(GLOBAL_BRANCH_ID).copied()
        {
            let Some(global_has_rows) = tracked_head
                .has_schema_rows_if_control_current(GLOBAL_BRANCH_ID, global_control, schema_key)
                .await?
            else {
                return Ok(None);
            };
            if global_has_rows {
                return Ok(None);
            }
        }
        tracked_head
            .scan_entity_snapshots_if_control_current(
                requested_branch_id,
                requested_control,
                schema_key,
                request.limit,
            )
            .await
    }

    pub(crate) async fn scan_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        let store = &self.store;
        let reads_tracked =
            request.filter.untracked != Some(true) && !is_commit_derived_only_request(request);
        let scope = scan_scope(store, &self.live_index, request, reads_tracked).await?;
        let commit_derived_rows = if request.filter.untracked != Some(true) {
            scan_commit_derived_rows(store, &self.commit_graph, request, &scope).await?
        } else {
            Vec::new()
        };
        let mut tracked_branch_rows =
            if request.filter.untracked != Some(true) && !is_commit_derived_only_request(request) {
                self.scan_tracked_branch_rows(request, &scope).await?
            } else {
                Vec::new()
            };
        let mut untracked_rows = if request.filter.untracked != Some(false)
            && !is_commit_derived_only_request(request)
        {
            let branch_rows = stream::iter(scope.storage_branch_ids.clone())
                .map(|branch_id| async move {
                    let rows: Vec<_> = self
                        .live_index
                        .reader(store)
                        .scan_rows(&index_scan_request_from_live(request, &branch_id))
                        .await?
                        .into_iter()
                        .filter(|row| row.schema_key != BRANCH_REF_SCHEMA_KEY)
                        .map(MaterializedLiveStateRow::from)
                        .collect();
                    Ok::<_, LixError>(rows)
                })
                .buffered(BRANCH_READ_CONCURRENCY)
                .try_collect::<Vec<_>>()
                .await?;
            branch_rows.into_iter().flatten().collect()
        } else {
            Vec::new()
        };
        if request.filter.untracked != Some(false) {
            untracked_rows.extend(scan_direct_branch_ref_rows(store, request, &scope).await?);
        }
        if commit_derived_rows.is_empty()
            && untracked_rows.is_empty()
            && let Some(index) =
                ordered_unique_branch_row_index(&tracked_branch_rows, &scope.projection_branch_ids)
        {
            return Ok(finalize_ordered_unique_rows(
                std::mem::take(&mut tracked_branch_rows[index].rows),
                request.filter.include_tombstones,
                request.limit,
            ));
        }
        let mut rows = commit_derived_rows;
        rows.extend(
            tracked_branch_rows
                .into_iter()
                .flat_map(|branch_rows| branch_rows.rows),
        );
        rows.extend(untracked_rows);
        Ok(resolve_visible_rows(
            rows,
            Vec::new(),
            &VisibilityRequest {
                branch_scope: VisibilityBranchScope::BranchIds {
                    branch_ids: scope.projection_branch_ids.clone(),
                },
                include_tombstones: request.filter.include_tombstones,
                limit: request.limit,
            },
        ))
    }

    pub(crate) async fn load_row(
        &self,
        request: &LiveStateRowRequest,
    ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
        let rows = self
            .scan_rows(&LiveStateScanRequest {
                filter: crate::live_state::LiveStateFilter {
                    schema_keys: vec![request.schema_key.clone()],
                    entity_pks: vec![request.entity_pk.clone()],
                    branch_ids: vec![request.branch_id.clone()],
                    file_ids: vec![request.file_id.clone()],
                    include_tombstones: false,
                    ..Default::default()
                },
                limit: Some(1),
                ..Default::default()
            })
            .await?;
        Ok(rows.into_iter().next())
    }

    /// Loads exact visible identities without lowering correlated row keys to
    /// independent scan dimensions.
    pub(crate) async fn load_exact_rows(
        &self,
        request: &LiveStateExactBatchRequest,
    ) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError> {
        if request.rows.is_empty() {
            return Ok(Vec::new());
        }
        // Commit-derived rows are synthesized rather than stored under the
        // requested identity. Preserve their exact scan semantics without
        // widening the optimized durable-state batch.
        if request.rows.iter().any(|row| {
            matches!(
                row.schema_key.as_str(),
                COMMIT_SCHEMA_KEY | COMMIT_EDGE_SCHEMA_KEY | BRANCH_REF_SCHEMA_KEY
            )
        }) {
            let mut rows = Vec::with_capacity(request.rows.len());
            for row in &request.rows {
                rows.push(
                    self.scan_rows(&request.row_scan_request(row))
                        .await?
                        .into_iter()
                        .next(),
                );
            }
            return Ok(rows);
        }

        let branch_ids = request
            .rows
            .iter()
            .map(|row| row.branch_id.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let scope_request = LiveStateScanRequest {
            filter: crate::live_state::LiveStateFilter {
                branch_ids,
                untracked: request.untracked,
                ..Default::default()
            },
            ..Default::default()
        };
        let reads_tracked = request.untracked != Some(true);
        let scope =
            scan_scope(&self.store, &self.live_index, &scope_request, reads_tracked).await?;
        let visible_branch_ids = scope
            .projection_branch_ids
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();

        let mut storage_identities = std::collections::BTreeSet::new();
        for row in &request.rows {
            if !visible_branch_ids.contains(&row.branch_id) {
                continue;
            }
            storage_identities.insert(LiveStateRowIdentity {
                branch_id: row.branch_id.clone(),
                schema_key: row.schema_key.clone(),
                entity_pk: row.entity_pk.clone(),
                file_id: row.file_id.clone(),
            });
            if row.branch_id != GLOBAL_BRANCH_ID {
                storage_identities.insert(LiveStateRowIdentity {
                    branch_id: GLOBAL_BRANCH_ID.to_string(),
                    schema_key: row.schema_key.clone(),
                    entity_pk: row.entity_pk.clone(),
                    file_id: row.file_id.clone(),
                });
            }
        }
        let storage_identities = storage_identities.into_iter().collect::<Vec<_>>();
        let mut candidates =
            std::collections::BTreeMap::<LiveStateRowIdentity, MaterializedLiveStateRow>::new();

        if request.untracked != Some(true) {
            let mut identities_by_branch = std::collections::BTreeMap::<String, Vec<_>>::new();
            for identity in &storage_identities {
                if scope.branch_heads.contains_key(&identity.branch_id) {
                    identities_by_branch
                        .entry(identity.branch_id.clone())
                        .or_default()
                        .push(identity.clone());
                }
            }
            let projection =
                crate::changelog::ChangeRecordProjection::from_columns(&request.projection.columns);
            let tracked_batches = stream::iter(identities_by_branch)
                .map(|(branch_id, identities)| {
                    let control = scope.branch_heads[&branch_id];
                    let commit_id = control.head_commit_id.to_string();
                    let projection = projection.clone();
                    async move {
                        let keys = identities
                            .iter()
                            .map(|identity| crate::tracked_state::TrackedStateKey {
                                schema_key: identity.schema_key.clone(),
                                entity_pk: identity.entity_pk.clone(),
                                file_id: identity.file_id.clone(),
                            })
                            .collect::<Vec<_>>();
                        let source = tracked_source_from_branch_id(&branch_id);
                        let rows = if let Some(rows) = self
                            .tracked_head
                            .reader(&self.store)
                            .load_projected_live_rows_if_control_current(
                                &branch_id,
                                control,
                                &keys,
                                &projection,
                            )
                            .await?
                        {
                            rows
                        } else {
                            self.tracked_state
                                .reader(&self.store)
                                .load_projected_rows_at_commit(&commit_id, &keys, &projection)
                                .await?
                                .into_iter()
                                .map(|row| {
                                    row.map(|row| project_tracked_row(row, &branch_id, source))
                                })
                                .collect()
                        };
                        Ok::<_, LixError>((branch_id, identities, rows))
                    }
                })
                .buffered(BRANCH_READ_CONCURRENCY)
                .try_collect::<Vec<_>>()
                .await?;
            for (_branch_id, identities, rows) in tracked_batches {
                for (identity, row) in identities.into_iter().zip(rows) {
                    if let Some(row) = row {
                        candidates.insert(identity, row);
                    }
                }
            }
        }

        if request.untracked != Some(false) {
            let index_requests = storage_identities
                .iter()
                .map(|identity| crate::live_state::LiveStateIndexRowRequest {
                    branch_id: identity.branch_id.clone(),
                    schema_key: identity.schema_key.clone(),
                    entity_pk: identity.entity_pk.clone(),
                    file_id: identity.file_id.clone(),
                })
                .collect::<Vec<_>>();
            let rows = self
                .live_index
                .reader(&self.store)
                .load_rows(&index_requests, &request.projection.columns)
                .await?;
            for (identity, row) in storage_identities.iter().cloned().zip(rows) {
                if let Some(row) = row {
                    // Mutable flat state is canonical over the tracked head for
                    // the same storage identity.
                    candidates.insert(identity, MaterializedLiveStateRow::from(row));
                }
            }
        }

        Ok(request
            .rows
            .iter()
            .map(|requested| {
                if !visible_branch_ids.contains(&requested.branch_id) {
                    return None;
                }
                let branch_identity = LiveStateRowIdentity {
                    branch_id: requested.branch_id.clone(),
                    schema_key: requested.schema_key.clone(),
                    entity_pk: requested.entity_pk.clone(),
                    file_id: requested.file_id.clone(),
                };
                let global_identity = LiveStateRowIdentity {
                    branch_id: GLOBAL_BRANCH_ID.to_string(),
                    schema_key: requested.schema_key.clone(),
                    entity_pk: requested.entity_pk.clone(),
                    file_id: requested.file_id.clone(),
                };
                let mut row = candidates
                    .get(&branch_identity)
                    .or_else(|| candidates.get(&global_identity))?
                    .clone();
                if row.branch_id.as_ref() == GLOBAL_BRANCH_ID
                    && requested.branch_id != GLOBAL_BRANCH_ID
                {
                    row.branch_id = requested.branch_id.clone().into();
                    row.global = true;
                }
                if row.deleted && !request.include_tombstones {
                    None
                } else {
                    Some(row)
                }
            })
            .collect())
    }

    pub(crate) async fn scan_tracked_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        let store = &self.store;
        let reads_tracked = !is_commit_derived_only_request(request);
        let scope = scan_scope(store, &self.live_index, request, reads_tracked).await?;
        let commit_derived_rows =
            scan_commit_derived_rows(store, &self.commit_graph, request, &scope).await?;
        let mut tracked_branch_rows = if !is_commit_derived_only_request(request) {
            self.scan_tracked_branch_rows(request, &scope).await?
        } else {
            Vec::new()
        };
        if commit_derived_rows.is_empty()
            && let Some(index) =
                ordered_unique_branch_row_index(&tracked_branch_rows, &scope.projection_branch_ids)
        {
            return Ok(finalize_ordered_unique_rows(
                std::mem::take(&mut tracked_branch_rows[index].rows),
                request.filter.include_tombstones,
                request.limit,
            ));
        }
        let mut rows = commit_derived_rows;
        rows.extend(
            tracked_branch_rows
                .into_iter()
                .flat_map(|branch_rows| branch_rows.rows),
        );
        Ok(resolve_visible_rows(
            rows,
            Vec::new(),
            &VisibilityRequest {
                branch_scope: VisibilityBranchScope::BranchIds {
                    branch_ids: scope.projection_branch_ids,
                },
                include_tombstones: request.filter.include_tombstones,
                limit: request.limit,
            },
        ))
    }

    async fn scan_tracked_branch_rows(
        &self,
        request: &LiveStateScanRequest,
        scope: &LiveStateScanScope,
    ) -> Result<Vec<TrackedBranchRows>, LixError> {
        let store = &self.store;
        let tracked_request = tracked_scan_request_from_live(request);
        let branches = scope
            .storage_branch_ids
            .iter()
            .filter_map(|branch_id| {
                scope
                    .branch_heads
                    .get(branch_id)
                    .map(|control| (branch_id.clone(), *control))
            })
            .collect::<Vec<_>>();
        let branch_rows = stream::iter(branches)
            .map(|(branch_id, control)| {
                let tracked_request = tracked_request.clone();
                async move {
                    let (rows, ordered_unique) = if let Some(rows) = self
                        .tracked_head
                        .reader(store)
                        .scan_live_rows_if_control_current(&branch_id, control, &tracked_request)
                        .await?
                    {
                        (rows, true)
                    } else {
                        (
                            self.tracked_state
                                .reader(store)
                                .scan_rows_at_commit(
                                    &control.head_commit_id.to_string(),
                                    &tracked_request,
                                )
                                .await?
                                .into_iter()
                                .map(|row| {
                                    project_tracked_row(
                                        row,
                                        &branch_id,
                                        tracked_source_from_branch_id(&branch_id),
                                    )
                                })
                                .collect(),
                            false,
                        )
                    };
                    Ok::<_, LixError>(TrackedBranchRows {
                        branch_id: branch_id.clone(),
                        rows,
                        ordered_unique,
                    })
                }
            })
            .buffered(BRANCH_READ_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(branch_rows)
    }
}

#[async_trait]
impl<S> LiveStateReader for LiveStateStoreReader<S>
where
    S: StorageAdapterRead,
{
    async fn scan_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        Self::scan_rows(self, request).await
    }

    async fn load_row(
        &self,
        request: &LiveStateRowRequest,
    ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
        Self::load_row(self, request).await
    }

    async fn load_exact_rows(
        &self,
        request: &LiveStateExactBatchRequest,
    ) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError> {
        Self::load_exact_rows(self, request).await
    }

    async fn scan_tracked_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        Self::scan_tracked_rows(self, request).await
    }
}

#[async_trait]
impl<S> FilesystemPathIndexReader for LiveStateStoreReader<S>
where
    S: StorageAdapterRead + Send + Sync,
{
    async fn path_index(
        &self,
        request: &FilesystemPathIndexRequest,
    ) -> Result<std::sync::Arc<FilesystemPathIndex>, LixError> {
        let revision = load_path_index_revision(&self.store).await?;
        if let Some(index) = self
            .filesystem_path_index_cache
            .get(request, revision.as_deref())
        {
            return Ok(index);
        }
        let index = build_path_index(self, request).await?;
        Ok(self
            .filesystem_path_index_cache
            .insert(request, revision.as_deref(), index))
    }
}

async fn scan_commit_derived_rows(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_graph: &CommitGraphContext,
    request: &LiveStateScanRequest,
    scope: &LiveStateScanScope,
) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
    if request.filter.untracked == Some(true) || !request_may_include_commit_derived(request) {
        return Ok(Vec::new());
    }
    if !file_filter_allows_null(&request.filter.file_ids) {
        return Ok(Vec::new());
    }

    let branch_ids = if scope.projection_branch_ids.is_empty() {
        vec![GLOBAL_BRANCH_ID.to_string()]
    } else {
        scope.projection_branch_ids.clone()
    };
    let mut graph = commit_graph.reader(store);
    let commits = graph.all_commits().await?;
    let include_commit = schema_filter_allows(&request.filter.schema_keys, COMMIT_SCHEMA_KEY);
    let include_commit_edge =
        schema_filter_allows(&request.filter.schema_keys, COMMIT_EDGE_SCHEMA_KEY);

    let mut rows = Vec::new();
    for branch_id in &branch_ids {
        if include_commit {
            for commit in &commits {
                rows.push(commit_row(commit, branch_id)?);
            }
        }
        if include_commit_edge {
            for edge in graph.commit_edges(&commits) {
                rows.push(commit_edge_row(&edge, branch_id)?);
            }
        }
    }

    rows.retain(|row| {
        (request.filter.entity_pks.is_empty() || request.filter.entity_pks.contains(&row.entity_pk))
            && (request.filter.branch_ids.is_empty()
                || request
                    .filter
                    .branch_ids
                    .iter()
                    .any(|branch_id| branch_id == row.branch_id.as_ref()))
    });
    Ok(rows)
}

/// Synthesizes the public `lix_branch_ref` metadata entity from authoritative
/// v6 controls. The old physical flat row is intentionally filtered out at
/// the caller so SQL/entity consumers keep seeing exactly one current ref per
/// branch even while a rare lifecycle write retains its legacy projection for
/// validation and changelog compatibility.
async fn scan_direct_branch_ref_rows(
    store: &(impl StorageAdapterRead + ?Sized),
    request: &LiveStateScanRequest,
    scope: &LiveStateScanScope,
) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
    if !schema_filter_allows(&request.filter.schema_keys, BRANCH_REF_SCHEMA_KEY)
        || !file_filter_allows_null(&request.filter.file_ids)
        || !scope
            .storage_branch_ids
            .iter()
            .any(|branch_id| branch_id == GLOBAL_BRANCH_ID)
    {
        return Ok(Vec::new());
    }

    let requested_branch_ids = if request.filter.entity_pks.is_empty() {
        Vec::new()
    } else {
        request
            .filter
            .entity_pks
            .iter()
            .filter_map(|entity_pk| entity_pk.as_single_string_owned().ok())
            .collect::<Vec<_>>()
    };
    if !request.filter.entity_pks.is_empty()
        && requested_branch_ids.len() != request.filter.entity_pks.len()
    {
        return Ok(Vec::new());
    }
    let controls = BranchHeadControlContext::new().reader(store);
    let entries = if requested_branch_ids.is_empty() {
        controls.scan().await?
    } else {
        controls
            .load_many(&requested_branch_ids)
            .await?
            .into_iter()
            .zip(requested_branch_ids)
            .filter_map(|(control, branch_id)| control.map(|control| (branch_id, control)))
            .collect()
    };
    entries
        .into_iter()
        .map(|(branch_id, control)| direct_branch_ref_row(&branch_id, control))
        .collect()
}

fn direct_branch_ref_row(
    branch_id: &str,
    control: BranchHeadControl,
) -> Result<MaterializedLiveStateRow, LixError> {
    let snapshot_content = serde_json::to_string(&serde_json::json!({
        "id": branch_id,
        "commit_id": control.head_commit_id.to_string(),
    }))
    .map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to encode v6 direct branch-ref snapshot: {error}"),
        )
    })?;
    Ok(MaterializedLiveStateRow {
        entity_pk: EntityPk::single(branch_id),
        schema_key: BRANCH_REF_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot_content: Some(snapshot_content),
        metadata: None,
        deleted: false,
        // These read-only columns are part of every public entity surface.
        // Preserve the same replacement semantics the old flat current row
        // had: creation time is stable, while each ref publication gets an
        // updated timestamp and distinct change id.
        created_at: control.created_at,
        updated_at: control.updated_at,
        global: true,
        change_id: Some(control.ref_change_id),
        commit_id: None,
        untracked: true,
        branch_id: GLOBAL_BRANCH_ID.into(),
    })
}

fn request_may_include_commit_derived(request: &LiveStateScanRequest) -> bool {
    request.filter.schema_keys.is_empty()
        || request
            .filter
            .schema_keys
            .iter()
            .any(|schema_key| is_commit_derived_schema(schema_key))
}

fn is_commit_derived_only_request(request: &LiveStateScanRequest) -> bool {
    !request.filter.schema_keys.is_empty()
        && request
            .filter
            .schema_keys
            .iter()
            .all(|schema_key| is_commit_derived_schema(schema_key))
}

fn is_commit_derived_schema(schema_key: &str) -> bool {
    matches!(schema_key, COMMIT_SCHEMA_KEY | COMMIT_EDGE_SCHEMA_KEY)
}

fn schema_filter_allows(schema_keys: &[String], schema_key: &str) -> bool {
    schema_keys.is_empty() || schema_keys.iter().any(|candidate| candidate == schema_key)
}

fn file_filter_allows_null(file_ids: &[NullableKeyFilter<String>]) -> bool {
    file_ids.is_empty()
        || file_ids
            .iter()
            .any(|file_id| matches!(file_id, NullableKeyFilter::Any | NullableKeyFilter::Null))
}

fn commit_row(
    commit: &crate::commit_graph::CommitGraphCommit,
    branch_id: &str,
) -> Result<MaterializedLiveStateRow, LixError> {
    let snapshot_content = serde_json::to_string(&serde_json::json!({
        "id": commit.commit_id,
    }))
    .map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to encode derived lix_commit snapshot: {error}"),
        )
    })?;
    Ok(MaterializedLiveStateRow {
        entity_pk: EntityPk::single(commit.commit_id),
        schema_key: COMMIT_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot_content: Some(snapshot_content),
        metadata: None,
        deleted: false,
        created_at: commit.change.created_at,
        updated_at: commit.change.created_at,
        global: true,
        change_id: Some(commit.change.id),
        commit_id: Some(commit.commit_id),
        untracked: false,
        branch_id: branch_id.into(),
    })
}

fn commit_edge_row(
    edge: &crate::commit_graph::CommitGraphEdge,
    branch_id: &str,
) -> Result<MaterializedLiveStateRow, LixError> {
    let snapshot_content = serde_json::to_string(&serde_json::json!({
        "parent_id": edge.parent_commit_id,
        "child_id": edge.child_commit_id,
        "parent_order": edge.parent_order,
    }))
    .map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to encode derived lix_commit_edge snapshot: {error}"),
        )
    })?;
    Ok(MaterializedLiveStateRow {
        entity_pk: EntityPk {
            parts: vec![
                edge.parent_commit_id.to_string(),
                edge.child_commit_id.to_string(),
            ],
        },
        schema_key: COMMIT_EDGE_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot_content: Some(snapshot_content),
        metadata: None,
        deleted: false,
        created_at: crate::common::LixTimestamp::from_unix_millis_utc_lossy(0),
        updated_at: crate::common::LixTimestamp::from_unix_millis_utc_lossy(0),
        global: true,
        change_id: None,
        commit_id: Some(edge.child_commit_id),
        untracked: false,
        branch_id: branch_id.into(),
    })
}

fn tracked_scan_request_from_live(request: &LiveStateScanRequest) -> TrackedStateScanRequest {
    TrackedStateScanRequest {
        filter: TrackedStateFilter {
            schema_keys: request.filter.schema_keys.clone(),
            entity_pks: request.filter.entity_pks.clone(),
            file_ids: request.filter.file_ids.clone(),
            // Scan tombstones internally so branch-local tombstones can hide
            // global fallback rows before the serving facade filters them.
            include_tombstones: true,
        },
        read_columns: TrackedStateReadColumns {
            columns: request.projection.columns.clone(),
        },
        limit: None,
    }
}

fn index_scan_request_from_live(
    request: &LiveStateScanRequest,
    branch_id: &str,
) -> LiveStateIndexScanRequest {
    LiveStateIndexScanRequest {
        branch_id: branch_id.to_string(),
        filter: LiveStateIndexFilter {
            schema_keys: request.filter.schema_keys.clone(),
            entity_pks: request.filter.entity_pks.clone(),
            file_ids: request.filter.file_ids.clone(),
        },
        projection: request.projection.columns.clone(),
        limit: None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LiveStateScanScope {
    storage_branch_ids: Vec<String>,
    projection_branch_ids: Vec<String>,
    branch_heads: BranchHeads,
}

/// Rows read from one durable tracked branch source.
///
/// A matching tracked-head projection is storage-key ordered by visible
/// identity and has one row per identity. Immutable-root fallback rows retain
/// their existing, more general materialization behavior and therefore never
/// claim this fast-path contract.
struct TrackedBranchRows {
    branch_id: String,
    rows: Vec<MaterializedLiveStateRow>,
    ordered_unique: bool,
}

/// Returns the only nonempty branch candidate when it can be served without
/// overlay arbitration. Global rows, multiple requested branches, an
/// immutable-root fallback, and staged/untracked candidates all stay on the
/// general visibility path.
fn ordered_unique_branch_row_index(
    branch_rows: &[TrackedBranchRows],
    projection_branch_ids: &[String],
) -> Option<usize> {
    let [requested_branch_id] = projection_branch_ids else {
        return None;
    };
    let mut candidates = branch_rows
        .iter()
        .enumerate()
        .filter(|(_, branch_rows)| !branch_rows.rows.is_empty());
    let (index, candidate) = candidates.next()?;
    if candidates.next().is_some()
        || !candidate.ordered_unique
        || candidate.branch_id != *requested_branch_id
    {
        return None;
    }
    Some(index)
}

/// Finalizes a table scan whose rows are already ordered and unique for the
/// sole requested branch. This intentionally does no identity sort or
/// deduplication; the tracked-head key codec proves both properties.
fn finalize_ordered_unique_rows(
    mut rows: Vec<MaterializedLiveStateRow>,
    include_tombstones: bool,
    limit: Option<usize>,
) -> Vec<MaterializedLiveStateRow> {
    if !include_tombstones {
        rows.retain(|row| !row.deleted);
    }
    if let Some(limit) = limit {
        rows.truncate(limit);
    }
    rows
}

async fn scan_scope(
    store: &(impl StorageAdapterRead + ?Sized),
    _live_index: &LiveStateIndexContext,
    request: &LiveStateScanRequest,
    resolve_branch_heads: bool,
) -> Result<LiveStateScanScope, LixError> {
    if request.filter.branch_ids.is_empty() {
        if resolve_branch_heads {
            let branch_heads = load_branch_head_controls(store, &[]).await?;
            return Ok(LiveStateScanScope {
                storage_branch_ids: branch_heads.keys().cloned().collect(),
                projection_branch_ids: Vec::new(),
                branch_heads,
            });
        }
        return Ok(LiveStateScanScope {
            storage_branch_ids: all_branch_head_control_ids(store).await?,
            projection_branch_ids: Vec::new(),
            branch_heads: BranchHeads::new(),
        });
    }

    if resolve_branch_heads {
        let candidate_branch_ids = expanded_branch_ids(&request.filter.branch_ids);
        let branch_heads = load_branch_head_controls(store, &candidate_branch_ids).await?;
        let projection_branch_ids = request
            .filter
            .branch_ids
            .iter()
            .filter(|branch_id| branch_heads.contains_key(*branch_id))
            .cloned()
            .collect::<Vec<_>>();
        let storage_branch_ids = expanded_branch_ids(&projection_branch_ids);
        return Ok(LiveStateScanScope {
            storage_branch_ids,
            projection_branch_ids,
            branch_heads,
        });
    }

    let existing_branch_ids = load_branch_head_control_ids(store, &request.filter.branch_ids)
        .await?
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    let projection_branch_ids = request
        .filter
        .branch_ids
        .iter()
        .filter(|branch_id| existing_branch_ids.contains(*branch_id))
        .cloned()
        .collect::<Vec<_>>();

    let storage_branch_ids = expanded_branch_ids(&projection_branch_ids);
    Ok(LiveStateScanScope {
        storage_branch_ids,
        projection_branch_ids,
        branch_heads: BranchHeads::new(),
    })
}

/// Loads v6 controls without touching the mutable live-state index. A
/// nonempty request is point-read; the empty request is the explicit
/// all-branches scan used only by broad scans.
async fn load_branch_head_controls(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_ids: &[String],
) -> Result<BranchHeads, LixError> {
    let reader = BranchHeadControlContext::new().reader(store);
    if branch_ids.is_empty() {
        return Ok(reader.scan().await?.into_iter().collect());
    }
    let controls = reader.load_many(branch_ids).await?;
    Ok(branch_ids
        .iter()
        .cloned()
        .zip(controls)
        .filter_map(|(branch_id, control)| control.map(|control| (branch_id, control)))
        .collect())
}

async fn all_branch_head_control_ids(
    store: &(impl StorageAdapterRead + ?Sized),
) -> Result<Vec<String>, LixError> {
    Ok(BranchHeadControlContext::new()
        .reader(store)
        .scan()
        .await?
        .into_iter()
        .map(|(branch_id, _)| branch_id)
        .collect())
}

async fn load_branch_head_control_ids(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_ids: &[String],
) -> Result<Vec<String>, LixError> {
    if branch_ids.is_empty() {
        return all_branch_head_control_ids(store).await;
    }
    let controls = BranchHeadControlContext::new()
        .reader(store)
        .load_many(branch_ids)
        .await?;
    Ok(branch_ids
        .iter()
        .cloned()
        .zip(controls)
        .filter_map(|(branch_id, control)| control.map(|_| branch_id))
        .collect())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TrackedRowSource {
    Global,
    Branch,
}

fn tracked_source_from_branch_id(branch_id: &str) -> TrackedRowSource {
    if branch_id == GLOBAL_BRANCH_ID {
        TrackedRowSource::Global
    } else {
        TrackedRowSource::Branch
    }
}

fn project_tracked_row(
    row: MaterializedTrackedStateRow,
    view_branch_id: &str,
    source: TrackedRowSource,
) -> MaterializedLiveStateRow {
    MaterializedLiveStateRow {
        entity_pk: row.entity_pk,
        schema_key: row.schema_key,
        file_id: row.file_id,
        snapshot_content: row.snapshot_content,
        metadata: row.metadata,
        deleted: row.deleted,
        created_at: crate::common::LixTimestamp::expect_parse(
            "tracked-state row created_at",
            &row.created_at,
        ),
        updated_at: crate::common::LixTimestamp::expect_parse(
            "tracked-state row updated_at",
            &row.updated_at,
        ),
        global: source == TrackedRowSource::Global,
        change_id: Some(row.change_id),
        commit_id: Some(row.commit_id),
        untracked: false,
        branch_id: view_branch_id.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NullableKeyFilter;
    use crate::changelog::{ChangeId, ChangeRecord, ChangelogAppend, CommitId};
    use crate::entity_pk::EntityPk;
    use crate::json_store::{JsonRef, JsonStoreContext, JsonWritePlacementRef, NormalizedJsonRef};
    use crate::live_state::index::{LiveStateIndexContext, LiveStateIndexDeltaRef};
    use crate::live_state::{
        LiveStateExactBatchRequest, LiveStateExactRowRequest, LiveStateFilter, LiveStateProjection,
        TrackedHeadDeltaRef,
    };
    use crate::storage_adapter::{Memory, StorageReadOptions, StorageWriteOptions};
    use crate::storage_adapter::{StorageAdapter, StorageWriteSet};
    use crate::tracked_state::{TrackedStateDeltaRef, TrackedStateScanRequest};
    use serde_json::json;

    const COMMIT_SCHEMA_KEY: &str = "lix_commit";

    #[derive(Clone)]
    struct MaterializedUntrackedStateRow {
        entity_pk: EntityPk,
        schema_key: String,
        file_id: Option<String>,
        snapshot_content: Option<String>,
        metadata: Option<String>,
        deleted: bool,
        created_at: String,
        updated_at: String,
        branch_id: String,
    }

    fn ts(value: &str) -> crate::common::LixTimestamp {
        crate::common::LixTimestamp::expect_parse("timestamp", value)
    }

    fn change_id(label: &str) -> ChangeId {
        ChangeId::for_test_label(label)
    }

    fn live_state_context() -> LiveStateContext {
        LiveStateContext::new(
            TrackedStateContext::new(),
            LiveStateIndexContext::new(),
            CommitGraphContext::new(),
        )
    }

    async fn stage_direct_entity_head(
        storage: &StorageAdapter,
        branch_id: &str,
        head: CommitId,
        schema_key: &str,
        entity_pk: &EntityPk,
        snapshot: &str,
    ) {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open direct-head write read");
        let mut writes = StorageWriteSet::new();
        crate::init::stage_repository_protocol(&mut writes);
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit(
                branch_id,
                None,
                head,
                &[TrackedHeadDeltaRef {
                    schema_key,
                    file_id: None,
                    entity_pk,
                    change_id: ChangeId::for_test_label(&format!("{branch_id}-change")),
                    commit_id: head,
                    deleted: false,
                    created_at: ts("2026-01-01T00:00:00Z"),
                    updated_at: ts("2026-01-01T00:00:00Z"),
                    snapshot: crate::json_store::JsonSlotRef::Inline(snapshot),
                    metadata: crate::json_store::JsonSlotRef::None,
                }],
                &std::collections::BTreeSet::new(),
                None,
            )
            .await
            .expect("stage direct entity head");
        crate::branch::stage_branch_head_control(
            &mut writes,
            branch_id,
            BranchHeadControl {
                head_commit_id: head,
                generation: head,
                created_at: ts("2026-01-01T00:00:00Z"),
                updated_at: ts("2026-01-01T00:00:00Z"),
                ref_change_id: ChangeId::for_test_label(&format!("{branch_id}-branch-ref")),
            },
        )
        .expect("stage direct entity branch control");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit direct entity head");
    }

    async fn scan_direct_entity_snapshots_for_test(
        live_state: &LiveStateContext,
        storage: &StorageAdapter,
        branch_id: &str,
        schema_key: &str,
    ) -> Result<Option<Vec<Option<Bytes>>>, LixError> {
        live_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("open direct entity read"),
            )
            .scan_direct_entity_snapshots(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec![schema_key.to_string()],
                    branch_ids: vec![branch_id.to_string()],
                    ..LiveStateFilter::default()
                },
                ..LiveStateScanRequest::default()
            })
            .await
    }

    #[test]
    fn ordered_head_fast_path_requires_one_matching_branch_candidate() {
        let requested_branch_ids = vec!["branch".to_string()];
        let branch = TrackedBranchRows {
            branch_id: "branch".to_string(),
            rows: vec![tracked_row_at_with_commit(
                "branch", "branch", None, "branch",
            )],
            ordered_unique: true,
        };
        assert_eq!(
            ordered_unique_branch_row_index(&[branch], &requested_branch_ids),
            Some(0)
        );

        let branch = TrackedBranchRows {
            branch_id: "branch".to_string(),
            rows: vec![tracked_row_at_with_commit(
                "branch", "branch", None, "branch",
            )],
            ordered_unique: true,
        };
        let global = TrackedBranchRows {
            branch_id: GLOBAL_BRANCH_ID.to_string(),
            rows: vec![tracked_row_at_with_commit(
                GLOBAL_BRANCH_ID,
                "global",
                None,
                "global",
            )],
            ordered_unique: true,
        };
        assert_eq!(
            ordered_unique_branch_row_index(&[branch, global], &requested_branch_ids),
            None,
            "a global candidate needs normal overlay arbitration"
        );

        let immutable_fallback = TrackedBranchRows {
            branch_id: "branch".to_string(),
            rows: vec![tracked_row_at_with_commit(
                "branch", "branch", None, "branch",
            )],
            ordered_unique: false,
        };
        assert_eq!(
            ordered_unique_branch_row_index(&[immutable_fallback], &requested_branch_ids),
            None,
            "the immutable-root fallback does not make the table ordering promise"
        );
    }

    #[tokio::test]
    async fn direct_entity_snapshots_fall_back_when_the_untracked_lane_is_present() {
        let storage = StorageAdapter::new(Memory::new());
        let live_state = live_state_context();
        let branch_id = "branch";
        let schema_key = "schema";
        stage_direct_entity_head(
            &storage,
            branch_id,
            CommitId::for_test_label("branch-head"),
            schema_key,
            &EntityPk::single("row"),
            r#"{"value":"tracked"}"#,
        )
        .await;

        let snapshots =
            scan_direct_entity_snapshots_for_test(&live_state, &storage, branch_id, schema_key)
                .await
                .expect("tracked-only entity scan should execute")
                .expect("tracked-only entity scan should use direct snapshots");
        assert_eq!(snapshots.len(), 1);
        assert_eq!(
            snapshots[0]
                .as_deref()
                .and_then(|snapshot| std::str::from_utf8(snapshot).ok()),
            Some(r#"{"value":"tracked"}"#)
        );

        let mut writes = StorageWriteSet::new();
        crate::live_state::stage_untracked_schema_presence_marker(
            &mut writes,
            branch_id,
            schema_key,
        )
        .expect("stage untracked schema marker");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit untracked schema marker");
        assert!(
            scan_direct_entity_snapshots_for_test(&live_state, &storage, branch_id, schema_key)
                .await
                .expect("mixed entity scan should execute")
                .is_none(),
            "normal SQL must retain the merged tracked/untracked visibility path"
        );
    }

    #[tokio::test]
    async fn direct_entity_snapshots_fall_back_when_global_tracks_the_schema() {
        let storage = StorageAdapter::new(Memory::new());
        let live_state = live_state_context();
        let branch_id = "branch";
        let schema_key = "schema";
        stage_direct_entity_head(
            &storage,
            branch_id,
            CommitId::for_test_label("branch-head"),
            schema_key,
            &EntityPk::single("local-row"),
            r#"{"value":"local"}"#,
        )
        .await;
        stage_direct_entity_head(
            &storage,
            GLOBAL_BRANCH_ID,
            CommitId::for_test_label("global-head"),
            schema_key,
            &EntityPk::single("global-row"),
            r#"{"value":"global"}"#,
        )
        .await;

        assert!(
            scan_direct_entity_snapshots_for_test(&live_state, &storage, branch_id, schema_key)
                .await
                .expect("global-overlaid entity scan should execute")
                .is_none(),
            "a global tracked row requires the established overlay resolver"
        );
    }

    async fn write_untracked_rows_to_store(
        storage: &StorageAdapter,
        _read: &(impl StorageAdapterRead + ?Sized),
        rows: &[MaterializedUntrackedStateRow],
    ) {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("current index read should open");
        let mut writes = storage.new_write_set();
        let mut json_writer = JsonStoreContext::new().writer();
        let changes = rows
            .iter()
            .enumerate()
            .map(|(index, row)| {
                if let Some(snapshot) = row.snapshot_content.as_deref() {
                    json_writer.stage_batch(
                        &mut writes,
                        JsonWritePlacementRef::OutOfBand,
                        [NormalizedJsonRef::trusted_prehashed(
                            snapshot,
                            JsonRef::for_content(snapshot.as_bytes()),
                        )],
                    )?;
                }
                let change_id = ChangeId::for_test_label(&format!(
                    "current:{}:{}:{index}",
                    row.branch_id, row.schema_key
                ));
                Ok::<_, LixError>((
                    row,
                    ChangeRecord {
                        format_version: 1,
                        change_id,
                        schema_key: row.schema_key.clone(),
                        entity_pk: row.entity_pk.clone(),
                        file_id: row.file_id.clone(),
                        snapshot: row
                            .snapshot_content
                            .as_deref()
                            .map_or(crate::json_store::JsonSlot::None, |snapshot| {
                                crate::json_store::JsonSlot::from_json(snapshot)
                            }),
                        metadata: row
                            .metadata
                            .as_deref()
                            .map_or(crate::json_store::JsonSlot::None, |metadata| {
                                crate::json_store::JsonSlot::from_json(metadata)
                            }),
                        created_at: ts(&row.updated_at),
                        origin_key: None,
                    },
                ))
            })
            .collect::<Result<Vec<_>, _>>()
            .expect("untracked changes should canonicalize");
        let mut changelog_read = &read;
        let mut changelog_writer =
            crate::changelog::ChangelogContext::new().writer(&mut changelog_read, &mut writes);
        crate::changelog::ChangelogWriter::stage_append(
            &mut changelog_writer,
            ChangelogAppend {
                changes: changes.iter().map(|(_, change)| change.clone()).collect(),
                ..Default::default()
            },
        )
        .await
        .expect("untracked changes should write");
        drop(changelog_writer);

        // Unit fixtures historically modeled branch heads as flat
        // `lix_branch_ref` rows. V6 deliberately rejects that physical
        // authority, so keep the fixture's public row while seeding the
        // matching direct control used by every production reader.
        for (row, change) in &changes {
            if row.schema_key != BRANCH_REF_SCHEMA_KEY || row.deleted {
                continue;
            }
            let branch_id = row
                .entity_pk
                .as_single_string_owned()
                .expect("test branch ref must have one branch id key");
            let snapshot = row
                .snapshot_content
                .as_deref()
                .expect("test branch ref must have a snapshot");
            let commit_id = serde_json::from_str::<serde_json::Value>(snapshot)
                .expect("test branch-ref snapshot should be JSON")
                .get("commit_id")
                .and_then(serde_json::Value::as_str)
                .map(|value| CommitId::parse_lix(value, "test branch-ref commit"))
                .transpose()
                .expect("test branch-ref commit should parse")
                .expect("test branch-ref snapshot should name a commit");
            crate::branch::stage_branch_head_control(
                &mut writes,
                &branch_id,
                BranchHeadControl {
                    head_commit_id: commit_id,
                    generation: commit_id,
                    created_at: ts(&row.created_at),
                    updated_at: ts(&row.updated_at),
                    ref_change_id: change.change_id,
                },
            )
            .expect("test branch-head control should stage");
        }

        let mut rows_by_branch = std::collections::BTreeMap::<&str, Vec<_>>::new();
        for (row, change) in &changes {
            rows_by_branch
                .entry(&row.branch_id)
                .or_default()
                .push(LiveStateIndexDeltaRef {
                    schema_key: &row.schema_key,
                    file_id: row.file_id.as_deref(),
                    entity_pk: &row.entity_pk,
                    change_id: change.change_id,
                    commit_id: None,
                    deleted: row.deleted,
                    created_at: ts(&row.created_at),
                    updated_at: ts(&row.updated_at),
                });
        }
        let live_index = LiveStateIndexContext::new();
        let mut index_writer = live_index.writer(&read, &mut writes);
        for (branch_id, deltas) in rows_by_branch {
            index_writer
                .stage_branch_rows(branch_id, deltas)
                .await
                .expect("current rows should write");
        }
        drop(index_writer);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("current rows should commit");
    }

    async fn write_empty_commits_to_store(
        storage: &StorageAdapter,
        read: &impl StorageAdapterRead,
        commit_ids: &[&str],
    ) {
        let mut writes = storage.new_write_set();
        let mut append = ChangelogAppend::default();
        for commit_id in commit_ids {
            let commit_id_text = CommitId::for_test_label(commit_id).to_string();
            let commit_change_id = format!("{commit_id_text}:commit");
            append.commits.push(crate::changelog::CommitRecord {
                format_version: 1,
                commit_id: CommitId::for_test_label(&commit_id_text),
                parent_commit_ids: Vec::new(),
                tracked_state_rootless: false,
                change_id: ChangeId::for_test_label(&commit_change_id),
                author_account_ids: Vec::new(),
                created_at: ts("1970-01-01T00:00:00.000Z"),
            });
            append
                .commit_change_refs
                .push(crate::changelog::CommitChangeRefSet {
                    commit_id: CommitId::for_test_label(&commit_id_text),
                    entries: Vec::new(),
                });
        }
        let mut changelog_read = read;
        let mut writer =
            crate::changelog::ChangelogContext::new().writer(&mut changelog_read, &mut writes);
        crate::changelog::ChangelogWriter::stage_append(&mut writer, append)
            .await
            .expect("empty changelog commits should stage");
        drop(writer);
        for commit_id in commit_ids {
            let commit_id_text = CommitId::for_test_label(commit_id).to_string();
            TrackedStateContext::new()
                .writer(read, &mut writes)
                .stage_commit_root(&commit_id_text, None, [])
                .await
                .expect("empty tracked roots should stage");
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("empty commits should commit");
    }

    async fn stage_materialized_live_rows(
        store: &impl StorageAdapterRead,
        writes: &mut StorageWriteSet,
        json_writer: &mut crate::json_store::JsonStoreWriter,
        rows: &[MaterializedLiveStateRow],
    ) -> Result<(), LixError> {
        let mut current_rows = Vec::<(String, MaterializedTrackedStateRow)>::new();
        let mut tracked_rows_by_commit = std::collections::BTreeMap::<
            String,
            Vec<(
                ChangeRecord,
                crate::common::LixTimestamp,
                crate::common::LixTimestamp,
            )>,
        >::new();
        let mut parent_by_commit = std::collections::BTreeMap::<String, Option<String>>::new();

        for row in rows {
            if row.untracked {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "test tracked-row helper does not accept untracked rows",
                ));
            }
            let materialized = MaterializedTrackedStateRow::try_from(row)?;
            let commit_id = row.commit_id.clone().ok_or_else(|| {
                LixError::new("LIX_ERROR_UNKNOWN", "test tracked row missing commit_id")
            })?;
            let commit_id_text = commit_id.to_string();
            if row.schema_key == COMMIT_SCHEMA_KEY {
                parent_by_commit.insert(
                    commit_id_text.clone(),
                    parent_commit_id_from_test_commit_row(row)?,
                );
            }
            if row.schema_key != COMMIT_SCHEMA_KEY {
                let change = crate::test_support::tracked_change_from_materialized(&materialized)?;
                stage_json_payloads_from_materialized(writes, json_writer, &materialized)?;
                current_rows.push((row.branch_id.to_string(), materialized.clone()));
                tracked_rows_by_commit
                    .entry(commit_id_text)
                    .or_default()
                    .push((
                        change,
                        ts(&materialized.created_at),
                        ts(&materialized.updated_at),
                    ));
            }
        }

        for (commit_id, rows) in tracked_rows_by_commit {
            let parent_commit_id = parent_by_commit.remove(&commit_id).flatten();
            let parent_ids = parent_commit_id
                .as_ref()
                .map(|parent| vec![parent.clone()])
                .unwrap_or_default();
            let commit_created_at = rows
                .first()
                .map(|(change, _, _)| change.created_at)
                .unwrap_or_else(|| ts("1970-01-01T00:00:00.000Z"));
            let change_refs = rows
                .iter()
                .map(|(change, _, _)| change.change_id)
                .collect::<Vec<_>>();
            let commit_change_id = format!("{commit_id}:commit");
            let mut append = ChangelogAppend::default();
            append
                .changes
                .extend(rows.iter().map(|(change, _, _)| change.clone()));
            append.commits.push(crate::changelog::CommitRecord {
                format_version: 1,
                commit_id: CommitId::for_test_label(&commit_id),
                parent_commit_ids: parent_ids
                    .iter()
                    .map(|id| CommitId::for_test_label(id))
                    .collect(),
                tracked_state_rootless: false,
                change_id: ChangeId::for_test_label(&commit_change_id),
                author_account_ids: Vec::new(),
                created_at: commit_created_at,
            });
            append
                .commit_change_refs
                .push(crate::changelog::CommitChangeRefSet {
                    commit_id: CommitId::for_test_label(&commit_id),
                    entries: change_refs,
                });
            let mut changelog_read = store;
            let mut writer =
                crate::changelog::ChangelogContext::new().writer(&mut changelog_read, writes);
            crate::changelog::ChangelogWriter::stage_append(&mut writer, append).await?;
            drop(writer);
            let typed_commit_id = CommitId::for_test_label(&commit_id);
            let deltas = rows
                .iter()
                .map(|(change, created_at, updated_at)| TrackedStateDeltaRef {
                    schema_key: &change.schema_key,
                    file_id: change.file_id.as_deref(),
                    entity_pk: &change.entity_pk,
                    change_id: change.change_id,
                    commit_id: typed_commit_id,
                    deleted: change.snapshot.is_none(),
                    created_at: *created_at,
                    updated_at: *updated_at,
                })
                .collect::<Vec<_>>();
            TrackedStateContext::new()
                .writer(&*store, writes)
                .stage_commit_root(&commit_id, parent_commit_id.as_deref(), deltas)
                .await?;
        }

        let mut current_rows_by_branch =
            std::collections::BTreeMap::<&str, Vec<LiveStateIndexDeltaRef<'_>>>::new();
        for (branch_id, row) in &current_rows {
            current_rows_by_branch
                .entry(branch_id)
                .or_default()
                .push(LiveStateIndexDeltaRef {
                    schema_key: &row.schema_key,
                    file_id: row.file_id.as_deref(),
                    entity_pk: &row.entity_pk,
                    change_id: row.change_id,
                    commit_id: Some(row.commit_id),
                    deleted: row.deleted,
                    created_at: ts(&row.created_at),
                    updated_at: ts(&row.updated_at),
                });
        }
        let live_index = LiveStateIndexContext::new();
        let mut index_writer = live_index.writer(store, writes);
        for (branch_id, deltas) in current_rows_by_branch {
            index_writer.stage_branch_rows(branch_id, deltas).await?;
        }
        Ok(())
    }

    fn stage_json_payloads_from_materialized(
        writes: &mut StorageWriteSet,
        json_writer: &mut crate::json_store::JsonStoreWriter,
        row: &MaterializedTrackedStateRow,
    ) -> Result<(), LixError> {
        if let Some(snapshot) = row.snapshot_content.as_deref() {
            json_writer.stage_batch(
                writes,
                JsonWritePlacementRef::OutOfBand,
                [NormalizedJsonRef::trusted_prehashed(
                    snapshot,
                    JsonRef::for_content(snapshot.as_bytes()),
                )],
            )?;
        }
        if let Some(metadata) = row.metadata.as_ref() {
            let serialized = crate::serialize_row_metadata(metadata);
            json_writer.stage_batch(
                writes,
                JsonWritePlacementRef::OutOfBand,
                [NormalizedJsonRef::trusted_prehashed(
                    &serialized,
                    JsonRef::for_content(serialized.as_bytes()),
                )],
            )?;
        }
        Ok(())
    }

    fn parent_commit_id_from_test_commit_row(
        row: &MaterializedLiveStateRow,
    ) -> Result<Option<String>, LixError> {
        let Some(metadata) = row.metadata.as_deref() else {
            return Ok(None);
        };
        let metadata = serde_json::from_str::<serde_json::Value>(metadata).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("test commit row has invalid metadata: {error}"),
            )
        })?;
        Ok(metadata
            .get("test_parents")
            .and_then(serde_json::Value::as_array)
            .and_then(|parents| parents.first())
            .and_then(serde_json::Value::as_str)
            .map(str::to_string))
    }

    #[tokio::test]
    async fn live_state_serves_untracked_change_from_flat_index() {
        let storage = StorageAdapter::new(Memory::new());
        let live_state = live_state_context();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = StorageWriteSet::new();
        let mut json_writer = JsonStoreContext::new().writer();
        stage_materialized_live_rows(
            &read,
            &mut writes,
            &mut json_writer,
            &[tracked_row_with_commit(
                "tracked-value",
                Some("change-tracked"),
                "commit-tracked",
            )],
        )
        .await
        .expect("tracked row should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("tracked row should commit");
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[
                branch_ref_row("global", "commit-tracked"),
                untracked_row("untracked-value"),
            ],
        )
        .await;

        let rows = scan_selected_tab_at(&live_state, &storage, "global", false)
            .await
            .expect("scan should succeed");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"untracked-value\"}")
        );
        assert!(rows[0].untracked);
        assert!(rows[0].change_id.is_some());

        let loaded = live_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_row(&LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                branch_id: "global".to_string(),
                entity_pk: crate::entity_pk::EntityPk::single("selected-tab"),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("load should succeed")
            .expect("current row should be visible");
        assert!(loaded.untracked);
        assert!(loaded.change_id.is_some());
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"untracked-value\"}")
        );
    }

    #[tokio::test]
    async fn exact_batch_preserves_duplicate_and_missing_slots_for_flat_rows() {
        let storage = StorageAdapter::new(Memory::new());
        let live_state = live_state_context();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = StorageWriteSet::new();
        let mut json_writer = JsonStoreContext::new().writer();
        stage_materialized_live_rows(
            &read,
            &mut writes,
            &mut json_writer,
            &[tracked_row_with_commit(
                "tracked-value",
                Some("change-tracked"),
                "commit-tracked",
            )],
        )
        .await
        .expect("tracked row should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("tracked row should commit");
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[
                branch_ref_row("global", "commit-tracked"),
                untracked_row("untracked-value"),
            ],
        )
        .await;

        let selected = LiveStateExactRowRequest {
            schema_key: "lix_key_value".to_string(),
            branch_id: "global".to_string(),
            entity_pk: identity("selected-tab"),
            file_id: None,
        };
        let rows = live_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should reopen"),
            )
            .load_exact_rows(&LiveStateExactBatchRequest {
                rows: vec![
                    selected.clone(),
                    selected,
                    LiveStateExactRowRequest {
                        schema_key: "lix_key_value".to_string(),
                        branch_id: "global".to_string(),
                        entity_pk: identity("missing"),
                        file_id: None,
                    },
                ],
                projection: LiveStateProjection {
                    columns: vec!["snapshot_content".to_string()],
                },
                ..Default::default()
            })
            .await
            .expect("exact batch should load");

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], rows[1]);
        assert_eq!(
            rows[0]
                .as_ref()
                .and_then(|row| row.snapshot_content.as_deref()),
            Some("{\"value\":\"untracked-value\"}")
        );
        assert!(rows[0].as_ref().is_some_and(|row| row.untracked));
        assert_eq!(rows[2], None);
    }

    #[tokio::test]
    async fn tracked_row_is_visible_from_commit_root() {
        let storage = StorageAdapter::new(Memory::new());
        let live_state = live_state_context();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        {
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(
                    &read,
                    &mut writes,
                    &mut json_writer,
                    &[tracked_row_with_commit(
                        "tracked-value",
                        Some("change-tracked"),
                        "commit-tracked",
                    )],
                )
                .await
                .expect("tracked row should stage");
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("writes should commit");
        }
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[branch_ref_row("global", "commit-tracked")],
        )
        .await;

        let loaded = load_selected_tab(&live_state, &storage)
            .await
            .expect("load should succeed")
            .expect("tracked row should be visible");
        assert!(!loaded.untracked);
        assert_eq!(loaded.change_id, Some(change_id("change-tracked")));
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"tracked-value\"}")
        );
    }

    #[tokio::test]
    async fn load_row_falls_back_to_global_tracked_row_for_requested_branch() {
        let storage = StorageAdapter::new(Memory::new());
        let live_state = live_state_context();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        {
            let rows = [tracked_row_with_commit(
                "global-tracked",
                Some("change-global"),
                "commit-global",
            )];
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &rows)
                    .await
                    .expect("tracked row should stage");
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("writes should commit");
        }
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[
                branch_ref_row("global", "commit-global"),
                branch_ref_row("branch-a", "commit-branch-a"),
            ],
        )
        .await;
        write_empty_commits_to_store(&storage, &read, &["commit-branch-a"]).await;

        let loaded = load_selected_tab_at(&live_state, &storage, "branch-a")
            .await
            .expect("load should succeed")
            .expect("global row should be visible for requested branch");

        assert_eq!(loaded.branch_id.as_ref(), "branch-a");
        assert!(loaded.global);
        assert!(!loaded.untracked);
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"global-tracked\"}")
        );
    }

    #[tokio::test]
    async fn main_sees_global_row_by_reading_global_root_separately() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let live_state = LiveStateContext::new(
            tracked_state.clone(),
            crate::live_state::index::LiveStateIndexContext::new(),
            CommitGraphContext::new(),
        );

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        {
            let rows = [tracked_row_with_commit(
                "global-tracked",
                Some("change-global"),
                "commit-global",
            )];
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &rows)
                    .await
                    .expect("global tracked row should stage");
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("writes should commit");
        }
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[
                branch_ref_row("global", "commit-global"),
                branch_ref_row("main", "commit-main"),
            ],
        )
        .await;
        write_empty_commits_to_store(&storage, &read, &["commit-main"]).await;

        let loaded = load_selected_tab_at(&live_state, &storage, "main")
            .await
            .expect("load should succeed")
            .expect("global row should be projected into main");
        assert_eq!(loaded.branch_id.as_ref(), "main");
        assert!(loaded.global);
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"global-tracked\"}")
        );

        let main_root_rows = scan_tracked_root(&tracked_state, &storage, "commit-main").await;
        assert!(
            main_root_rows.is_empty(),
            "derived commit rows must not be stored in tracked roots"
        );
    }

    #[tokio::test]
    async fn load_row_prefers_requested_branch_over_global() {
        let storage = StorageAdapter::new(Memory::new());
        let live_state = live_state_context();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        {
            let rows = [
                tracked_row_with_commit("global-tracked", Some("change-global"), "commit-global"),
                tracked_row_at_with_commit(
                    "branch-a",
                    "branch-tracked",
                    Some("change-branch"),
                    "commit-branch",
                ),
            ];
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &rows)
                    .await
                    .expect("tracked rows should stage");
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("writes should commit");
        }
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[
                branch_ref_row("global", "commit-global"),
                branch_ref_row("branch-a", "commit-branch"),
            ],
        )
        .await;

        let loaded = load_selected_tab_at(&live_state, &storage, "branch-a")
            .await
            .expect("load should succeed")
            .expect("branch row should be visible");

        assert_eq!(loaded.branch_id.as_ref(), "branch-a");
        assert!(!loaded.untracked);
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"branch-tracked\"}")
        );
    }

    #[tokio::test]
    async fn main_override_hides_global_row() {
        let storage = StorageAdapter::new(Memory::new());
        let live_state = live_state_context();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        {
            let rows = [
                tracked_row_with_commit("global-tracked", Some("change-global"), "commit-global"),
                tracked_row_at_with_commit(
                    "main",
                    "main-tracked",
                    Some("change-main"),
                    "commit-main",
                ),
            ];
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &rows)
                    .await
                    .expect("tracked rows should stage");
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("writes should commit");
        }
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[
                branch_ref_row("global", "commit-global"),
                branch_ref_row("main", "commit-main"),
            ],
        )
        .await;

        let loaded = load_selected_tab_at(&live_state, &storage, "main")
            .await
            .expect("load should succeed")
            .expect("main row should be visible");

        assert_eq!(loaded.branch_id.as_ref(), "main");
        assert!(!loaded.global);
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"main-tracked\"}")
        );
    }

    #[tokio::test]
    async fn scan_rows_overlays_requested_branch_over_global() {
        let storage = StorageAdapter::new(Memory::new());
        let live_state = live_state_context();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        {
            let rows = [
                tracked_row_with_commit("global-tracked", Some("change-global"), "commit-global"),
                tracked_row_at_with_commit(
                    "branch-a",
                    "branch-tracked",
                    Some("change-branch"),
                    "commit-branch",
                ),
            ];
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &rows)
                    .await
                    .expect("rows should stage");
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("writes should commit");
        }
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[
                branch_ref_row("global", "commit-global"),
                branch_ref_row("branch-a", "commit-branch"),
            ],
        )
        .await;

        let rows = scan_selected_tab_at(&live_state, &storage, "branch-a", false)
            .await
            .expect("scan should succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].branch_id.as_ref(), "branch-a");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"branch-tracked\"}")
        );
    }

    #[tokio::test]
    async fn scan_rows_projects_global_row_into_requested_branch() {
        let storage = StorageAdapter::new(Memory::new());
        let live_state = live_state_context();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        {
            let rows = [tracked_row_with_commit(
                "global-tracked",
                Some("change-global"),
                "commit-global",
            )];
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &rows)
                    .await
                    .expect("rows should stage");
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("writes should commit");
        }
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[
                branch_ref_row("global", "commit-global"),
                branch_ref_row("branch-a", "commit-branch-a"),
            ],
        )
        .await;
        write_empty_commits_to_store(&storage, &read, &["commit-branch-a"]).await;

        let rows = scan_selected_tab_at(&live_state, &storage, "branch-a", false)
            .await
            .expect("scan should succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].branch_id.as_ref(), "branch-a");
        assert!(rows[0].global);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"global-tracked\"}")
        );
    }

    #[tokio::test]
    async fn scan_rows_does_not_project_global_rows_into_missing_branch() {
        let storage = StorageAdapter::new(Memory::new());
        let live_state = live_state_context();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        {
            let rows = [tracked_row_with_commit(
                "global-tracked",
                Some("change-global"),
                "commit-global",
            )];
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &rows)
                    .await
                    .expect("tracked row should stage");
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("writes should commit");
        }
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[branch_ref_row("global", "commit-global")],
        )
        .await;

        let rows = scan_selected_tab_at(&live_state, &storage, "missing-branch", false)
            .await
            .expect("scan should succeed");

        assert_eq!(
            rows.len(),
            0,
            "global rows must not be projected into a missing branch scope"
        );
    }

    #[tokio::test]
    async fn winning_tombstone_hides_row_unless_tombstones_are_included() {
        let storage = StorageAdapter::new(Memory::new());
        let live_state = live_state_context();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        {
            let rows = [
                tracked_row_with_commit("global-tracked", Some("change-global"), "commit-global"),
                tombstone_tracked_row_at_with_commit(
                    "branch-a",
                    Some("change-tombstone"),
                    "commit-branch",
                ),
            ];
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &rows)
                    .await
                    .expect("rows should stage");
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("writes should commit");
        }
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[
                branch_ref_row("global", "commit-global"),
                branch_ref_row("branch-a", "commit-branch"),
            ],
        )
        .await;

        let hidden = scan_selected_tab_at(&live_state, &storage, "branch-a", false)
            .await
            .expect("scan should succeed");
        assert_eq!(hidden.len(), 0);

        let with_tombstone = scan_selected_tab_at(&live_state, &storage, "branch-a", true)
            .await
            .expect("scan should succeed");
        assert_eq!(with_tombstone.len(), 1);
        assert_eq!(with_tombstone[0].branch_id.as_ref(), "branch-a");
        assert_eq!(with_tombstone[0].snapshot_content, None);
    }

    #[tokio::test]
    async fn main_tombstone_hides_global_row() {
        let storage = StorageAdapter::new(Memory::new());
        let live_state = live_state_context();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        {
            let rows = [
                tracked_row_with_commit("global-tracked", Some("change-global"), "commit-global"),
                tombstone_tracked_row_at_with_commit(
                    "main",
                    Some("change-main-tombstone"),
                    "commit-main",
                ),
            ];
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &rows)
                    .await
                    .expect("tracked rows should stage");
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("writes should commit");
        }
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[
                branch_ref_row("global", "commit-global"),
                branch_ref_row("main", "commit-main"),
            ],
        )
        .await;

        let hidden = scan_selected_tab_at(&live_state, &storage, "main", false)
            .await
            .expect("scan should succeed");
        assert_eq!(hidden.len(), 0);

        let tombstones = scan_selected_tab_at(&live_state, &storage, "main", true)
            .await
            .expect("scan should succeed");
        assert_eq!(tombstones.len(), 1);
        assert_eq!(tombstones[0].branch_id.as_ref(), "main");
        assert!(!tombstones[0].global);
        assert_eq!(tombstones[0].snapshot_content, None);
    }

    #[tokio::test]
    async fn exact_batch_resolves_branch_global_tombstone_projection_and_correlated_keys() {
        let storage = StorageAdapter::new(Memory::new());
        let live_state = live_state_context();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");

        let mut global_fallback =
            tracked_row_with_commit("global-fallback", Some("change-fallback"), "commit-global");
        global_fallback.entity_pk = identity("fallback");
        global_fallback.file_id = Some("fallback".to_string());
        global_fallback.metadata = Some("{\"source\":\"global\"}".to_string());
        let mut global_overridden =
            tracked_row_with_commit("global-old", Some("change-global-old"), "commit-global");
        global_overridden.entity_pk = identity("overridden");
        global_overridden.file_id = Some("overridden".to_string());
        let mut branch_override = tracked_row_at_with_commit(
            "branch-a",
            "branch-new",
            Some("change-branch-new"),
            "commit-branch",
        );
        branch_override.entity_pk = identity("overridden");
        branch_override.file_id = Some("overridden".to_string());
        let mut global_hidden =
            tracked_row_with_commit("global-hidden", Some("change-hidden"), "commit-global");
        global_hidden.entity_pk = identity("hidden");
        global_hidden.file_id = Some("hidden".to_string());
        let mut branch_tombstone = tombstone_tracked_row_at_with_commit(
            "branch-a",
            Some("change-tombstone"),
            "commit-branch",
        );
        branch_tombstone.entity_pk = identity("hidden");
        branch_tombstone.file_id = Some("hidden".to_string());
        let mut malformed_cross_pair =
            tracked_row_with_commit("cross-pair", Some("change-cross"), "commit-global");
        malformed_cross_pair.entity_pk = identity("entity-a");
        malformed_cross_pair.file_id = Some("file-b".to_string());

        let rows = [
            global_fallback,
            global_overridden,
            global_hidden,
            malformed_cross_pair,
            branch_override,
            branch_tombstone,
        ];
        let mut writes = StorageWriteSet::new();
        let mut json_writer = JsonStoreContext::new().writer();
        stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &rows)
            .await
            .expect("tracked rows should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("tracked rows should commit");
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[
                branch_ref_row("global", "commit-global"),
                branch_ref_row("branch-a", "commit-branch"),
            ],
        )
        .await;

        let exact = |entity: &str, file_id: &str| LiveStateExactRowRequest {
            schema_key: "lix_key_value".to_string(),
            branch_id: "branch-a".to_string(),
            entity_pk: identity(entity),
            file_id: Some(file_id.to_string()),
        };
        let reader = live_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should reopen"),
        );
        let loaded = reader
            .load_exact_rows(&LiveStateExactBatchRequest {
                rows: vec![
                    exact("fallback", "fallback"),
                    exact("overridden", "overridden"),
                    exact("hidden", "hidden"),
                    exact("entity-a", "file-a"),
                    exact("entity-b", "file-b"),
                    exact("entity-a", "file-b"),
                    exact("missing", "missing"),
                ],
                projection: LiveStateProjection {
                    columns: vec!["snapshot_content".to_string()],
                },
                ..Default::default()
            })
            .await
            .expect("exact tracked batch should load");

        let fallback = loaded[0].as_ref().expect("global fallback should load");
        assert!(fallback.global);
        assert_eq!(fallback.branch_id.as_ref(), "branch-a");
        assert_eq!(
            fallback.snapshot_content.as_deref(),
            Some("{\"value\":\"global-fallback\"}")
        );
        assert_eq!(fallback.metadata, None, "projection should omit metadata");
        let overridden = loaded[1].as_ref().expect("branch override should load");
        assert!(!overridden.global);
        assert_eq!(
            overridden.snapshot_content.as_deref(),
            Some("{\"value\":\"branch-new\"}")
        );
        assert_eq!(loaded[2], None, "branch tombstone must hide global row");
        assert_eq!(loaded[3], None, "entity A/file A must not cross-match");
        assert_eq!(loaded[4], None, "entity B/file B must not cross-match");
        assert_eq!(
            loaded[5]
                .as_ref()
                .and_then(|row| row.snapshot_content.as_deref()),
            Some("{\"value\":\"cross-pair\"}")
        );
        assert_eq!(loaded[6], None);

        let tombstone = reader
            .load_exact_rows(&LiveStateExactBatchRequest {
                rows: vec![exact("hidden", "hidden")],
                include_tombstones: true,
                ..Default::default()
            })
            .await
            .expect("exact tombstone read should load")
            .pop()
            .flatten()
            .expect("tombstone should be returned when requested");
        assert!(tombstone.deleted);
        assert!(!tombstone.global);
    }

    #[tokio::test]
    async fn writer_allows_commit_fact_to_share_the_touched_branch_commit_id() {
        let storage = StorageAdapter::new(Memory::new());
        let live_state = live_state_context();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");

        {
            let rows = [
                tracked_row_at_with_commit(
                    "branch-a",
                    "branch-row",
                    Some("change-branch"),
                    "commit-branch",
                ),
                commit_live_state_row("commit-branch"),
            ];
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &rows)
                    .await
                    .expect("commit facts are changelog projections, not root-local rows");
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("writes should commit");
        }
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[branch_ref_row("branch-a", "commit-branch")],
        )
        .await;

        let loaded = load_selected_tab_at(&live_state, &storage, "branch-a")
            .await
            .expect("load should succeed")
            .expect("branch row should be visible");
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"branch-row\"}")
        );
    }

    #[tokio::test]
    async fn writer_uses_first_parent_as_merge_root_base() {
        let storage = StorageAdapter::new(Memory::new());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        write_empty_commits_to_store(&storage, &read, &["parent-left"]).await;
        let mut writes = StorageWriteSet::new();
        TrackedStateContext::new()
            .writer(&read, &mut writes)
            .stage_commit_root("parent-left", None, [])
            .await
            .expect("first parent tracked root should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("first parent tracked root should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");

        {
            let rows = [
                tracked_row_at_with_commit(
                    "branch-a",
                    "branch-row",
                    Some("change-branch"),
                    "commit-merge",
                ),
                commit_live_state_row_with_parents(
                    "commit-merge",
                    &["parent-left", "parent-right"],
                ),
            ];
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &rows)
                    .await
                    .expect("merge commit should use first parent as tracked-root base");
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("writes should commit");
        }
    }

    #[tokio::test]
    async fn non_global_root_does_not_store_global_rows() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");

        {
            let rows = [
                tracked_row_with_commit("global-tracked", Some("change-global"), "commit-global"),
                tracked_row_at_with_commit(
                    "main",
                    "main-tracked",
                    Some("change-main"),
                    "commit-main",
                ),
            ];
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &rows)
                    .await
                    .expect("tracked rows should stage");
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("writes should commit");
        }

        let global_root_rows = scan_tracked_root(&tracked_state, &storage, "commit-global").await;
        assert_eq!(global_root_rows.len(), 1);
        let Some(global_row) = global_root_rows
            .iter()
            .find(|row| row.schema_key == "lix_key_value")
        else {
            panic!("global root should contain the explicit global tracked row");
        };
        assert_eq!(
            global_row.snapshot_content.as_deref(),
            Some("{\"value\":\"global-tracked\"}")
        );

        let main_root_rows = scan_tracked_root(&tracked_state, &storage, "commit-main").await;
        assert_eq!(main_root_rows.len(), 1);
        let Some(main_row) = main_root_rows
            .iter()
            .find(|row| row.schema_key == "lix_key_value")
        else {
            panic!("main root should contain the explicit main tracked row");
        };
        assert_eq!(
            main_row.snapshot_content.as_deref(),
            Some("{\"value\":\"main-tracked\"}")
        );
    }

    async fn load_selected_tab(
        live_state: &LiveStateContext,
        storage: &StorageAdapter,
    ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
        live_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_row(&LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                branch_id: "global".to_string(),
                entity_pk: crate::entity_pk::EntityPk::single("selected-tab"),
                file_id: NullableKeyFilter::Null,
            })
            .await
    }

    async fn load_selected_tab_at(
        live_state: &LiveStateContext,
        storage: &StorageAdapter,
        branch_id: &str,
    ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
        live_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_row(&LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                branch_id: branch_id.to_string(),
                entity_pk: crate::entity_pk::EntityPk::single("selected-tab"),
                file_id: NullableKeyFilter::Null,
            })
            .await
    }

    async fn scan_selected_tab_at(
        live_state: &LiveStateContext,
        storage: &StorageAdapter,
        branch_id: &str,
        include_tombstones: bool,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        live_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .scan_rows(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec!["lix_key_value".to_string()],
                    entity_pks: vec![crate::entity_pk::EntityPk::single("selected-tab")],
                    branch_ids: vec![branch_id.to_string()],
                    file_ids: vec![NullableKeyFilter::Null],
                    include_tombstones,
                    ..LiveStateFilter::default()
                },
                ..LiveStateScanRequest::default()
            })
            .await
    }

    async fn scan_tracked_root(
        tracked_state: &TrackedStateContext,
        storage: &StorageAdapter,
        commit_id: &str,
    ) -> Vec<MaterializedTrackedStateRow> {
        tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .scan_rows_at_commit(
                commit_id,
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        include_tombstones: true,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("tracked root should scan")
    }

    fn tracked_row_with_commit(
        value: &str,
        change_id: Option<&str>,
        commit_id: &str,
    ) -> MaterializedLiveStateRow {
        tracked_row_at_with_commit("global", value, change_id, commit_id)
    }

    fn tracked_row_at_with_commit(
        branch_id: &str,
        value: &str,
        change_id: Option<&str>,
        commit_id: &str,
    ) -> MaterializedLiveStateRow {
        let commit_id = CommitId::for_test_label(commit_id);
        MaterializedLiveStateRow {
            entity_pk: identity("selected-tab"),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}")),
            metadata: None,
            deleted: false,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:00Z"),
            global: branch_id == "global",
            change_id: change_id.map(ChangeId::for_test_label),
            commit_id: Some(commit_id),
            untracked: false,
            branch_id: branch_id.into(),
        }
    }

    fn tombstone_tracked_row_at_with_commit(
        branch_id: &str,
        change_id: Option<&str>,
        commit_id: &str,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            snapshot_content: None,
            deleted: true,
            ..tracked_row_at_with_commit(branch_id, "ignored", change_id, commit_id)
        }
    }

    fn untracked_row(value: &str) -> MaterializedUntrackedStateRow {
        untracked_row_at("global", value)
    }

    fn untracked_row_at(branch_id: &str, value: &str) -> MaterializedUntrackedStateRow {
        MaterializedUntrackedStateRow {
            entity_pk: identity("selected-tab"),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}")),
            metadata: None,
            deleted: false,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            branch_id: branch_id.to_string(),
        }
    }

    fn branch_ref_row(branch_id: &str, commit_id: &str) -> MaterializedUntrackedStateRow {
        let commit_id = CommitId::for_test_label(commit_id).to_string();
        MaterializedUntrackedStateRow {
            entity_pk: identity(branch_id),
            schema_key: "lix_branch_ref".to_string(),
            file_id: None,
            snapshot_content: Some(
                serde_json::to_string(&json!({
                    "id": branch_id,
                    "commit_id": commit_id,
                }))
                .expect("branch ref should serialize"),
            ),
            metadata: None,
            deleted: false,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            branch_id: "global".to_string(),
        }
    }

    fn commit_live_state_row(commit_id: &str) -> MaterializedLiveStateRow {
        commit_live_state_row_with_parents(commit_id, &[])
    }

    fn commit_live_state_row_with_parents(
        commit_id: &str,
        parent_ids: &[&str],
    ) -> MaterializedLiveStateRow {
        let commit_id_text = CommitId::for_test_label(commit_id).to_string();
        let parent_id_texts = parent_ids
            .iter()
            .map(|parent| CommitId::for_test_label(parent).to_string())
            .collect::<Vec<_>>();
        let mut row = commit_live_state_row_with_snapshot(
            &commit_id_text,
            json!({
                "id": commit_id_text,
            }),
        );
        row.metadata = Some(
            serde_json::to_string(&json!({ "test_parents": parent_id_texts }))
                .expect("test metadata should serialize"),
        );
        row
    }

    fn commit_live_state_row_with_snapshot(
        commit_id: &str,
        snapshot: serde_json::Value,
    ) -> MaterializedLiveStateRow {
        let commit_id = CommitId::for_test_label(commit_id);
        let commit_id_text = commit_id.to_string();
        MaterializedLiveStateRow {
            entity_pk: identity(&commit_id_text),
            schema_key: COMMIT_SCHEMA_KEY.to_string(),
            file_id: None,
            snapshot_content: Some(
                serde_json::to_string(&snapshot).expect("commit snapshot should serialize"),
            ),
            metadata: None,
            deleted: false,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:00Z"),
            global: true,
            change_id: Some(ChangeId::for_test_label(&format!("change-{commit_id}"))),
            commit_id: Some(commit_id),
            untracked: false,
            branch_id: "global".into(),
        }
    }

    fn identity(entity_pk: &str) -> EntityPk {
        EntityPk::single(entity_pk)
    }
}
