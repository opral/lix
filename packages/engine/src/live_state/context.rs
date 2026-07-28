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
use crate::live_state::tracked_head::TrackedHeadContext;
use crate::live_state::{
    LiveStateExactBatchRequest, LiveStateReader, LiveStateRowFilter, LiveStateRowIdentity,
    LiveStateRowRequest, LiveStateScanRequest, MaterializedLiveStateRow, VisibilityBranchScope,
    VisibilityRequest, expanded_branch_ids, resolve_visible_rows,
};
use crate::storage_adapter::StorageAdapterRead;
use crate::tracked_state::{
    TrackedStateContext, TrackedStateFilter, TrackedStateReadColumns, TrackedStateScanRequest,
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
/// Normal rows are resolved from one durable hot-state projection. Each row
/// carries its own tracked|untracked retention, so readers do not route
/// through a separate retention index or merge retention candidates.
pub(crate) struct LiveStateContext {
    tracked_head: TrackedHeadContext,
    commit_graph: CommitGraphContext,
    filesystem_path_index_cache: std::sync::Arc<FilesystemPathIndexCache>,
}

impl LiveStateContext {
    pub(crate) fn new(
        _tracked_state: TrackedStateContext,
        commit_graph: CommitGraphContext,
    ) -> Self {
        Self {
            tracked_head: TrackedHeadContext::new(),
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
            tracked_head: self.tracked_head,
            commit_graph: self.commit_graph.clone(),
            filesystem_path_index_cache: std::sync::Arc::clone(&self.filesystem_path_index_cache),
        }
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
    tracked_head: TrackedHeadContext,
    commit_graph: CommitGraphContext,
    filesystem_path_index_cache: std::sync::Arc<FilesystemPathIndexCache>,
}

impl<S> LiveStateStoreReader<S>
where
    S: StorageAdapterRead,
{
    /// Returns raw current-state snapshot bytes for one current SQL entity scan.
    ///
    /// `None` means the normal materialized visibility path remains
    /// authoritative: a global branch projection, multiple result branch
    /// scopes, or commit-derived state all require its full visibility
    /// semantics.
    pub(crate) async fn scan_direct_entity_snapshots(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Option<Vec<Option<Bytes>>>, LixError> {
        // The hot index carries tracked and untracked rows in one serving
        // plane, so this route never probes a separate retention index.
        if request.filter.untracked.is_some()
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
        let scope = scan_scope(&self.store, request, true).await?;
        let [requested_branch_id] = scope.projection_branch_ids.as_slice() else {
            return Ok(None);
        };
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
            let global_has_rows = tracked_head
                .has_schema_rows(GLOBAL_BRANCH_ID, global_control, schema_key)
                .await?;
            if global_has_rows {
                return Ok(None);
            }
        }
        Ok(Some(
            tracked_head
                .scan_entity_snapshots(
                    requested_branch_id,
                    requested_control,
                    schema_key,
                    &request.filter.entity_pks,
                    request.limit,
                )
                .await?,
        ))
    }

    pub(crate) async fn scan_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        self.scan_rows_with_schema_presence(request, false).await
    }

    async fn scan_rows_with_schema_presence(
        &self,
        request: &LiveStateScanRequest,
        skip_proven_empty_schema: bool,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        let store = &self.store;
        let reads_tracked = !is_commit_derived_only_request(request);
        let scope = scan_scope(store, request, reads_tracked).await?;
        if skip_proven_empty_schema && !scope_may_have_schema_rows(request, &scope) {
            return Ok(Vec::new());
        }
        if let Some(rows) = self.scan_direct_entity_pk_rows(request, &scope).await? {
            return Ok(rows);
        }
        let commit_derived_rows = if request.filter.untracked != Some(true) {
            scan_commit_derived_rows(store, &self.commit_graph, request, &scope).await?
        } else {
            Vec::new()
        };
        let mut hot_branch_rows = if !is_commit_derived_only_request(request) {
            self.scan_hot_branch_rows(request, &scope).await?
        } else {
            Vec::new()
        };
        if request.filter.untracked != Some(false) {
            let branch_ref_rows = scan_direct_branch_ref_rows(store, request, &scope).await?;
            if !branch_ref_rows.is_empty() {
                hot_branch_rows.push(HotBranchRows {
                    branch_id: GLOBAL_BRANCH_ID.to_string(),
                    rows: branch_ref_rows,
                    ordered_unique: false,
                });
            }
        }
        // The ordered single-branch route bypasses the generic visibility
        // resolver, so apply the retention predicate before taking that fast
        // path. Otherwise `untracked = Some(..)` accidentally returned both
        // member kinds from an already-unified group.
        for branch_rows in &mut hot_branch_rows {
            branch_rows
                .rows
                .retain(|row| current_row_matches_retention(row, request.filter.untracked));
        }
        if commit_derived_rows.is_empty()
            && let Some(index) =
                ordered_unique_branch_row_index(&hot_branch_rows, &scope.projection_branch_ids)
        {
            return Ok(finalize_ordered_unique_rows(
                std::mem::take(&mut hot_branch_rows[index].rows),
                request.filter.include_tombstones,
                request.limit,
            ));
        }
        let mut rows = commit_derived_rows;
        rows.extend(
            hot_branch_rows
                .into_iter()
                .flat_map(|branch_rows| branch_rows.rows),
        );
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

    /// Serves finite entity-PK scans from the hot current-state index. Every
    /// row already has its retention tag, so an unrelated untracked row
    /// cannot route selected tracked identities through a separate scan.
    async fn scan_direct_entity_pk_rows(
        &self,
        request: &LiveStateScanRequest,
        scope: &LiveStateScanScope,
    ) -> Result<Option<Vec<MaterializedLiveStateRow>>, LixError> {
        if !matches!(request.filter.rows, LiveStateRowFilter::All)
            || request.filter.branch_ids.is_empty()
            || request.filter.schema_keys.is_empty()
            || request.filter.entity_pks.is_empty()
            || !request.filter.file_ids.is_empty()
            || !request.filter.constraints.is_empty()
            || request_may_include_commit_derived(request)
            || request
                .filter
                .schema_keys
                .iter()
                .any(|schema_key| schema_key == BRANCH_REF_SCHEMA_KEY)
        {
            return Ok(None);
        }
        let controls = scope
            .storage_branch_ids
            .iter()
            .map(|branch_id| {
                scope
                    .branch_heads
                    .get(branch_id)
                    .copied()
                    .map(|control| (branch_id.clone(), control))
            })
            .collect::<Option<Vec<_>>>();
        let Some(controls) = controls else {
            return Ok(None);
        };
        let tracked_request = tracked_scan_request_from_live(request);
        let rows_by_branch = self
            .tracked_head
            .reader(&self.store)
            .scan_live_rows_for_controls(&controls, &tracked_request)
            .await?;
        let rows = rows_by_branch
            .into_iter()
            .flat_map(|(_, rows)| rows)
            .filter(|row| current_row_matches_retention(row, request.filter.untracked))
            .collect();
        Ok(Some(resolve_visible_rows(
            rows,
            Vec::new(),
            &VisibilityRequest {
                branch_scope: VisibilityBranchScope::BranchIds {
                    branch_ids: scope.projection_branch_ids.clone(),
                },
                include_tombstones: request.filter.include_tombstones,
                limit: request.limit,
            },
        )))
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
        // The hot current-state rows are authoritative for both retention modes.
        // Even an untracked-only exact batch therefore needs the branch
        // controls that select the active generation; treating that request
        // as "not tracked" used to skip the controls entirely and made the
        // workspace selector (an untracked row) invisible after hot-index init.
        let scope = scan_scope(&self.store, &scope_request, true).await?;
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
        let current_batches = stream::iter(identities_by_branch)
            .map(|(branch_id, identities)| {
                let control = scope.branch_heads[&branch_id];
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
                    let rows = self
                        .tracked_head
                        .reader(&self.store)
                        .load_projected_live_rows(&branch_id, control, &keys, &projection)
                        .await?;
                    Ok::<_, LixError>((identities, rows))
                }
            })
            .buffered(BRANCH_READ_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        for (identities, rows) in current_batches {
            for (identity, row) in identities.into_iter().zip(rows) {
                if let Some(row) = row {
                    candidates.insert(identity, row);
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
                // Filter each source before branch/global precedence. A local
                // row of the other retention must not mask a matching global
                // row from an explicit retention-scoped internal read.
                let mut row = candidates
                    .get(&branch_identity)
                    .filter(|row| current_row_matches_retention(row, request.untracked))
                    .or_else(|| {
                        candidates
                            .get(&global_identity)
                            .filter(|row| current_row_matches_retention(row, request.untracked))
                    })?
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
        self.scan_tracked_rows_with_schema_presence(request, false)
            .await
    }

    async fn scan_tracked_rows_with_schema_presence(
        &self,
        request: &LiveStateScanRequest,
        skip_proven_empty_schema: bool,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        let store = &self.store;
        let reads_tracked = !is_commit_derived_only_request(request);
        let scope = scan_scope(store, request, reads_tracked).await?;
        if skip_proven_empty_schema && !scope_may_have_schema_rows(request, &scope) {
            return Ok(Vec::new());
        }
        let commit_derived_rows =
            scan_commit_derived_rows(store, &self.commit_graph, request, &scope).await?;
        let mut hot_branch_rows = if !is_commit_derived_only_request(request) {
            self.scan_hot_branch_rows(request, &scope).await?
        } else {
            Vec::new()
        };
        for branch_rows in &mut hot_branch_rows {
            branch_rows.rows.retain(|row| !row.untracked);
        }
        if commit_derived_rows.is_empty()
            && let Some(index) =
                ordered_unique_branch_row_index(&hot_branch_rows, &scope.projection_branch_ids)
        {
            return Ok(finalize_ordered_unique_rows(
                std::mem::take(&mut hot_branch_rows[index].rows),
                request.filter.include_tombstones,
                request.limit,
            ));
        }
        let mut rows = commit_derived_rows;
        rows.extend(
            hot_branch_rows
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

    async fn scan_hot_branch_rows(
        &self,
        request: &LiveStateScanRequest,
        scope: &LiveStateScanScope,
    ) -> Result<Vec<HotBranchRows>, LixError> {
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
                    let rows = self
                        .tracked_head
                        .reader(store)
                        .scan_live_rows(&branch_id, control, &tracked_request)
                        .await?;
                    Ok::<_, LixError>(HotBranchRows {
                        branch_id: branch_id.clone(),
                        rows,
                        ordered_unique: true,
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
    async fn scan_constraint_rows(
        &self,
        request: &LiveStateScanRequest,
        tracked_only: bool,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        if tracked_only {
            self.scan_tracked_rows_with_schema_presence(request, true)
                .await
        } else {
            self.scan_rows_with_schema_presence(request, true).await
        }
    }

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
        let mut index = build_path_index(self, request).await?;
        if request.cache_small_blob_data {
            index = std::sync::Arc::new(
                (*index)
                    .clone()
                    .hydrate_small_blob_data(&self.store)
                    .await?,
            );
        }
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
/// branch-head controls. The mutable live-state projection is intentionally
/// filtered out at the caller so SQL/entity consumers keep seeing exactly one
/// current ref per branch while lifecycle operations retain their validated
/// changelog fact.
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
            format!("failed to encode direct branch-ref snapshot: {error}"),
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct LiveStateScanScope {
    storage_branch_ids: Vec<String>,
    projection_branch_ids: Vec<String>,
    branch_heads: BranchHeads,
}

/// Rows read from one durable hot-state branch source.
///
/// A matching hot-state projection is storage-key ordered by visible identity
/// and has one row per identity.
struct HotBranchRows {
    branch_id: String,
    rows: Vec<MaterializedLiveStateRow>,
    ordered_unique: bool,
}

/// Returns the only nonempty branch candidate when it can be served without
/// branch/global visibility resolution. Global rows, multiple requested
/// branches, and synthesized branch-ref candidates all stay on the general
/// visibility path.
fn ordered_unique_branch_row_index(
    branch_rows: &[HotBranchRows],
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

fn current_row_matches_retention(
    row: &MaterializedLiveStateRow,
    requested_untracked: Option<bool>,
) -> bool {
    requested_untracked.is_none_or(|untracked| row.untracked == untracked)
}

/// Proves a single-schema hot-state scan empty from the atomic branch
/// publication metadata. Missing controls and Bloom false positives fall back
/// to the storage scan; only a negative result from every selected generation
/// can skip it.
fn scope_may_have_schema_rows(request: &LiveStateScanRequest, scope: &LiveStateScanScope) -> bool {
    let [schema_key] = request.filter.schema_keys.as_slice() else {
        return true;
    };
    if schema_key == BRANCH_REF_SCHEMA_KEY || request_may_include_commit_derived(request) {
        return true;
    }
    scope.storage_branch_ids.iter().any(|branch_id| {
        scope
            .branch_heads
            .get(branch_id)
            .is_none_or(|control| control.may_have_schema(schema_key))
    })
}

async fn scan_scope(
    store: &(impl StorageAdapterRead + ?Sized),
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

/// Loads branch-head controls without touching the mutable live-state index. A
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NullableKeyFilter;
    use crate::changelog::{ChangeId, ChangeRecord, ChangelogAppend, CommitId};
    use crate::entity_pk::EntityPk;
    use crate::json_store::{JsonRef, JsonStoreContext, JsonWritePlacementRef, NormalizedJsonRef};
    use crate::live_state::{
        CurrentStateDeltaRef, LiveStateExactBatchRequest, LiveStateExactRowRequest,
        LiveStateFilter, LiveStateProjection, TrackedHeadDeltaRef, WorkingDiffIndexCoverage,
    };
    use crate::storage_adapter::{Memory, StorageReadOptions, StorageWriteOptions};
    use crate::storage_adapter::{StorageAdapter, StorageWriteSet};
    use crate::tracked_state::{
        MaterializedTrackedStateRow, TrackedStateDeltaRef, TrackedStateScanRequest,
    };
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
        LiveStateContext::new(TrackedStateContext::new(), CommitGraphContext::new())
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
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit direct entity head");
    }

    #[derive(Clone, Copy)]
    struct DirectTrackedHeadRow<'a> {
        schema_key: &'a str,
        entity_pk: &'a EntityPk,
        file_id: Option<&'a str>,
        snapshot: Option<&'a str>,
        deleted: bool,
    }

    async fn stage_direct_tracked_head_rows(
        storage: &StorageAdapter,
        branch_id: &str,
        head: CommitId,
        rows: &[DirectTrackedHeadRow<'_>],
    ) {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open hot-state write read");
        let mut writes = StorageWriteSet::new();
        crate::init::stage_repository_protocol(&mut writes);
        let deltas = rows
            .iter()
            .enumerate()
            .map(|(index, row)| TrackedHeadDeltaRef {
                schema_key: row.schema_key,
                file_id: row.file_id,
                entity_pk: row.entity_pk,
                change_id: ChangeId::for_test_label(&format!("{branch_id}-change-{index}")),
                commit_id: head,
                deleted: row.deleted,
                created_at: ts("2026-01-01T00:00:00Z"),
                updated_at: ts("2026-01-01T00:00:00Z"),
                snapshot: row.snapshot.map_or(
                    crate::json_store::JsonSlotRef::None,
                    crate::json_store::JsonSlotRef::Inline,
                ),
                metadata: crate::json_store::JsonSlotRef::None,
            })
            .collect::<Vec<_>>();
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit(
                branch_id,
                None,
                head,
                &deltas,
                &std::collections::BTreeSet::new(),
                None,
            )
            .await
            .expect("stage direct hot state");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit direct hot state");
    }

    fn finite_pk_scan_request(
        branch_id: &str,
        schema_key: &str,
        entity_pks: Vec<EntityPk>,
    ) -> LiveStateScanRequest {
        LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec![schema_key.to_string()],
                entity_pks,
                branch_ids: vec![branch_id.to_string()],
                ..LiveStateFilter::default()
            },
            ..LiveStateScanRequest::default()
        }
    }

    async fn scan_direct_entity_pk_rows_for_test(
        live_state: &LiveStateContext,
        storage: &StorageAdapter,
        request: &LiveStateScanRequest,
    ) -> Result<Option<Vec<MaterializedLiveStateRow>>, LixError> {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open direct hot-state scan read");
        let scope = scan_scope(&read, request, true).await?;
        live_state
            .reader(read)
            .scan_direct_entity_pk_rows(request, &scope)
            .await
    }

    async fn scan_rows_for_test(
        live_state: &LiveStateContext,
        storage: &StorageAdapter,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        live_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("open normal scan read"),
            )
            .scan_rows(request)
            .await
    }

    async fn scan_direct_entity_snapshots_for_test(
        live_state: &LiveStateContext,
        storage: &StorageAdapter,
        branch_id: &str,
        schema_key: &str,
        entity_pks: &[EntityPk],
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
                    entity_pks: entity_pks.to_vec(),
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
        let branch = HotBranchRows {
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

        let branch = HotBranchRows {
            branch_id: "branch".to_string(),
            rows: vec![tracked_row_at_with_commit(
                "branch", "branch", None, "branch",
            )],
            ordered_unique: true,
        };
        let global = HotBranchRows {
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
            "a global candidate needs normal branch/global resolution"
        );

        let unordered_candidate = HotBranchRows {
            branch_id: "branch".to_string(),
            rows: vec![tracked_row_at_with_commit(
                "branch", "branch", None, "branch",
            )],
            ordered_unique: false,
        };
        assert_eq!(
            ordered_unique_branch_row_index(&[unordered_candidate], &requested_branch_ids),
            None,
            "an unordered candidate does not make the table ordering promise"
        );
    }

    #[tokio::test]
    async fn direct_entity_snapshots_fall_back_when_global_tracks_the_schema() {
        let storage = StorageAdapter::new(Memory::new());
        let live_state = live_state_context();
        let branch_id = "branch";
        let schema_key = "schema";
        let local_pk = EntityPk::single("local-row");
        stage_direct_entity_head(
            &storage,
            branch_id,
            CommitId::for_test_label("branch-head"),
            schema_key,
            &local_pk,
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
            scan_direct_entity_snapshots_for_test(
                &live_state,
                &storage,
                branch_id,
                schema_key,
                std::slice::from_ref(&local_pk),
            )
            .await
            .expect("global entity scan should execute")
            .is_none(),
            "a global tracked row requires the established branch/global resolver"
        );
    }

    #[tokio::test]
    async fn direct_entity_snapshots_fall_back_for_retention_scoped_reads() {
        let storage = StorageAdapter::new(Memory::new());
        let live_state = live_state_context();
        for untracked in [false, true] {
            let snapshots = live_state
                .reader(
                    storage
                        .begin_read(StorageReadOptions::default())
                        .await
                        .expect("open retention-scoped entity read"),
                )
                .scan_direct_entity_snapshots(&LiveStateScanRequest {
                    filter: LiveStateFilter {
                        schema_keys: vec!["schema".to_string()],
                        entity_pks: vec![EntityPk::single("row")],
                        branch_ids: vec!["branch".to_string()],
                        untracked: Some(untracked),
                        ..LiveStateFilter::default()
                    },
                    ..LiveStateScanRequest::default()
                })
                .await
                .expect("retention-scoped entity read should execute");
            assert!(
                snapshots.is_none(),
                "raw snapshot serving must not bypass retention filtering"
            );
        }
    }

    #[tokio::test]
    async fn direct_entity_snapshots_read_sorted_exact_primary_keys() {
        let storage = StorageAdapter::new(Memory::new());
        let live_state = live_state_context();
        let branch_id = "branch";
        let schema_key = "schema";
        let first = EntityPk::single("first");
        let second = EntityPk::single("second");
        let rows = [
            DirectTrackedHeadRow {
                schema_key,
                entity_pk: &second,
                file_id: None,
                snapshot: Some(r#"{"id":"second","value":"two"}"#),
                deleted: false,
            },
            DirectTrackedHeadRow {
                schema_key,
                entity_pk: &first,
                file_id: None,
                snapshot: Some(r#"{"id":"first","value":"one"}"#),
                deleted: false,
            },
        ];
        stage_direct_tracked_head_rows(
            &storage,
            branch_id,
            CommitId::for_test_label("exact-primary-keys"),
            &rows,
        )
        .await;

        let snapshots = scan_direct_entity_snapshots_for_test(
            &live_state,
            &storage,
            branch_id,
            schema_key,
            &[
                second.clone(),
                EntityPk::single("missing"),
                first.clone(),
                second,
            ],
        )
        .await
        .expect("exact tracked entity scan should execute")
        .expect("tracked-only exact entity scan should use direct snapshots");
        assert_eq!(
            snapshots
                .iter()
                .map(|snapshot| snapshot
                    .as_deref()
                    .and_then(|bytes| std::str::from_utf8(bytes).ok()))
                .collect::<Vec<_>>(),
            vec![
                Some(r#"{"id":"first","value":"one"}"#),
                Some(r#"{"id":"second","value":"two"}"#),
            ]
        );
    }

    #[tokio::test]
    async fn finite_pk_hot_scan_returns_all_file_id_siblings() {
        let storage = StorageAdapter::new(Memory::new());
        let live_state = live_state_context();
        let entity_pk = EntityPk::single("shared-entity");
        let rows = [
            DirectTrackedHeadRow {
                schema_key: "schema",
                entity_pk: &entity_pk,
                file_id: Some("file-a"),
                snapshot: Some(r#"{"value":"a"}"#),
                deleted: false,
            },
            DirectTrackedHeadRow {
                schema_key: "schema",
                entity_pk: &entity_pk,
                file_id: Some("file-b"),
                snapshot: Some(r#"{"value":"b"}"#),
                deleted: false,
            },
        ];
        stage_direct_tracked_head_rows(
            &storage,
            GLOBAL_BRANCH_ID,
            CommitId::for_test_label("hot-row-siblings"),
            &rows,
        )
        .await;

        let request = finite_pk_scan_request(GLOBAL_BRANCH_ID, "schema", vec![entity_pk]);
        let direct = scan_direct_entity_pk_rows_for_test(&live_state, &storage, &request)
            .await
            .expect("direct hot-state scan should execute")
            .expect("finite tracked primary-key scan should use the hot route");
        let file_values = direct
            .iter()
            .map(|row| (row.file_id.as_deref(), row.snapshot_content.as_deref()))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(
            file_values,
            std::collections::BTreeMap::from([
                (Some("file-a"), Some(r#"{"value":"a"}"#)),
                (Some("file-b"), Some(r#"{"value":"b"}"#)),
            ])
        );

        let normal = scan_rows_for_test(&live_state, &storage, &request)
            .await
            .expect("normal finite primary-key scan should execute");
        assert_eq!(normal, direct);
    }

    #[tokio::test]
    async fn finite_pk_hot_scan_resolves_branch_override_and_tombstone_against_global() {
        let storage = StorageAdapter::new(Memory::new());
        let live_state = live_state_context();
        let entity_pk = EntityPk::single("shared-entity");
        stage_direct_tracked_head_rows(
            &storage,
            GLOBAL_BRANCH_ID,
            CommitId::for_test_label("global-head"),
            &[DirectTrackedHeadRow {
                schema_key: "schema",
                entity_pk: &entity_pk,
                file_id: None,
                snapshot: Some(r#"{"value":"global"}"#),
                deleted: false,
            }],
        )
        .await;
        stage_direct_tracked_head_rows(
            &storage,
            "branch-a",
            CommitId::for_test_label("branch-head"),
            &[DirectTrackedHeadRow {
                schema_key: "schema",
                entity_pk: &entity_pk,
                file_id: None,
                snapshot: Some(r#"{"value":"branch"}"#),
                deleted: false,
            }],
        )
        .await;

        let request = finite_pk_scan_request("branch-a", "schema", vec![entity_pk.clone()]);
        let direct = scan_direct_entity_pk_rows_for_test(&live_state, &storage, &request)
            .await
            .expect("direct hot-state scan should execute")
            .expect("current branch and global controls should use the hot route");
        assert_eq!(direct.len(), 1);
        assert_eq!(direct[0].branch_id.as_ref(), "branch-a");
        assert!(!direct[0].global);
        assert_eq!(
            direct[0].snapshot_content.as_deref(),
            Some(r#"{"value":"branch"}"#)
        );
        assert_eq!(
            scan_rows_for_test(&live_state, &storage, &request)
                .await
                .expect("normal branch override scan should execute"),
            direct
        );

        stage_direct_tracked_head_rows(
            &storage,
            "branch-a",
            CommitId::for_test_label("branch-tombstone"),
            &[DirectTrackedHeadRow {
                schema_key: "schema",
                entity_pk: &entity_pk,
                file_id: None,
                snapshot: None,
                deleted: true,
            }],
        )
        .await;

        let hidden = scan_direct_entity_pk_rows_for_test(&live_state, &storage, &request)
            .await
            .expect("direct hot-state tombstone scan should execute")
            .expect("current controls should retain the hot route");
        assert!(hidden.is_empty(), "local tombstone must hide global row");
        assert!(
            scan_rows_for_test(&live_state, &storage, &request)
                .await
                .expect("normal branch tombstone scan should execute")
                .is_empty()
        );

        let mut including_tombstones = request.clone();
        including_tombstones.filter.include_tombstones = true;
        let tombstones =
            scan_direct_entity_pk_rows_for_test(&live_state, &storage, &including_tombstones)
                .await
                .expect("direct hot-state tombstone scan should execute")
                .expect("current controls should retain the hot route");
        assert_eq!(tombstones.len(), 1);
        assert!(tombstones[0].deleted);
        assert!(!tombstones[0].global);
        assert_eq!(tombstones[0].branch_id.as_ref(), "branch-a");
    }

    #[tokio::test]
    async fn finite_pk_hot_scan_serves_mixed_current_state() {
        let storage = StorageAdapter::new(Memory::new());
        let live_state = live_state_context();
        let tracked_pk = EntityPk::single("tracked-entity");
        let untracked_pk = EntityPk::single("untracked-entity");
        stage_direct_tracked_head_rows(
            &storage,
            GLOBAL_BRANCH_ID,
            CommitId::for_test_label("tracked-head"),
            &[DirectTrackedHeadRow {
                schema_key: "schema",
                entity_pk: &tracked_pk,
                file_id: None,
                snapshot: Some(r#"{"value":"tracked"}"#),
                deleted: false,
            }],
        )
        .await;

        let request = finite_pk_scan_request(
            GLOBAL_BRANCH_ID,
            "schema",
            vec![tracked_pk.clone(), untracked_pk.clone()],
        );
        assert!(
            scan_direct_entity_pk_rows_for_test(&live_state, &storage, &request)
                .await
                .expect("initial direct hot-state scan should execute")
                .is_some(),
            "the hot current state serves tracked-only rows directly"
        );

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open untracked write read");
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[MaterializedUntrackedStateRow {
                entity_pk: untracked_pk.clone(),
                schema_key: "schema".to_string(),
                file_id: None,
                snapshot_content: Some(r#"{"value":"untracked"}"#.to_string()),
                metadata: None,
                deleted: false,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                updated_at: "2026-01-01T00:00:00Z".to_string(),
                branch_id: GLOBAL_BRANCH_ID.to_string(),
            }],
        )
        .await;

        let direct = scan_direct_entity_pk_rows_for_test(&live_state, &storage, &request)
            .await
            .expect("mixed hot-state scan should execute")
            .expect("one hot index serves both retention modes");
        assert_eq!(direct.len(), 2);
        let rows = scan_rows_for_test(&live_state, &storage, &request)
            .await
            .expect("mixed normal scan should execute");
        assert_eq!(rows, direct);
        assert_eq!(rows.len(), 2);
        let tracked = rows
            .iter()
            .find(|row| row.entity_pk == tracked_pk)
            .expect("tracked row should remain visible");
        assert!(!tracked.untracked);
        assert_eq!(
            tracked.snapshot_content.as_deref(),
            Some(r#"{"value":"tracked"}"#)
        );
        let untracked = rows
            .iter()
            .find(|row| row.entity_pk == untracked_pk)
            .expect("untracked row should be merged into normal query results");
        assert!(untracked.untracked);
        assert_eq!(
            untracked.snapshot_content.as_deref(),
            Some(r#"{"value":"untracked"}"#)
        );
    }

    #[tokio::test]
    async fn explicit_file_id_predicate_retains_member_read_path() {
        let storage = StorageAdapter::new(Memory::new());
        let live_state = live_state_context();
        let entity_pk = EntityPk::single("shared-entity");
        stage_direct_tracked_head_rows(
            &storage,
            GLOBAL_BRANCH_ID,
            CommitId::for_test_label("member-path"),
            &[
                DirectTrackedHeadRow {
                    schema_key: "schema",
                    entity_pk: &entity_pk,
                    file_id: Some("file-a"),
                    snapshot: Some(r#"{"value":"a"}"#),
                    deleted: false,
                },
                DirectTrackedHeadRow {
                    schema_key: "schema",
                    entity_pk: &entity_pk,
                    file_id: Some("file-b"),
                    snapshot: Some(r#"{"value":"b"}"#),
                    deleted: false,
                },
            ],
        )
        .await;

        let mut request = finite_pk_scan_request(GLOBAL_BRANCH_ID, "schema", vec![entity_pk]);
        request.filter.file_ids = vec![NullableKeyFilter::Value("file-a".to_string())];
        assert!(
            scan_direct_entity_pk_rows_for_test(&live_state, &storage, &request)
                .await
                .expect("file-filtered direct hot-state scan should execute")
                .is_none(),
            "an explicit file-id predicate must retain the member projection route"
        );
        let rows = scan_rows_for_test(&live_state, &storage, &request)
            .await
            .expect("file-filtered normal scan should execute");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].file_id.as_deref(), Some("file-a"));
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some(r#"{"value":"a"}"#)
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
            .expect("current-state read should open");
        let mut writes = storage.new_write_set();
        let mut json_writer = JsonStoreContext::new().writer();
        let mut branch_refs = std::collections::BTreeMap::new();
        let mut rows_by_branch =
            std::collections::BTreeMap::<String, Vec<&MaterializedUntrackedStateRow>>::new();
        for row in rows {
            if row.schema_key == BRANCH_REF_SCHEMA_KEY {
                if row.deleted {
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
                assert!(
                    branch_refs
                        .insert(
                            branch_id,
                            (commit_id, ts(&row.created_at), ts(&row.updated_at))
                        )
                        .is_none(),
                    "test fixture contains duplicate branch refs"
                );
                continue;
            }
            if let Some(snapshot) = row.snapshot_content.as_deref() {
                json_writer
                    .stage_batch(
                        &mut writes,
                        JsonWritePlacementRef::OutOfBand,
                        [NormalizedJsonRef::trusted_prehashed(
                            snapshot,
                            JsonRef::for_content(snapshot.as_bytes()),
                        )],
                    )
                    .expect("untracked snapshot should stage");
            }
            if let Some(metadata) = row.metadata.as_deref() {
                json_writer
                    .stage_batch(
                        &mut writes,
                        JsonWritePlacementRef::OutOfBand,
                        [NormalizedJsonRef::trusted_prehashed(
                            metadata,
                            JsonRef::for_content(metadata.as_bytes()),
                        )],
                    )
                    .expect("untracked metadata should stage");
            }
            rows_by_branch
                .entry(row.branch_id.clone())
                .or_default()
                .push(row);
        }

        // A branch ref selects a fresh hot-state generation. Its tracked
        // portion is reconstructed from the immutable root and its untracked
        // portion is materialized into the same snapshot; untracked rows have
        // no changelog record.
        for (branch_id, (head_commit_id, created_at, updated_at)) in branch_refs {
            let branch_rows = rows_by_branch.remove(&branch_id).unwrap_or_default();
            let head_commit_id_text = head_commit_id.to_string();
            let mut tracked_reader = TrackedStateContext::new().reader(&read);
            let parent_rows = if tracked_reader
                .has_durable_commit_root(&head_commit_id_text)
                .await
                .expect("test branch root should inspect")
            {
                tracked_reader
                    .scan_rows_at_commit(
                        &head_commit_id_text,
                        &TrackedStateScanRequest {
                            filter: TrackedStateFilter {
                                include_tombstones: true,
                                ..Default::default()
                            },
                            read_columns: TrackedStateReadColumns::default(),
                            limit: None,
                        },
                    )
                    .await
                    .expect("test branch root should load")
            } else {
                Vec::new()
            };
            let snapshots = branch_rows
                .iter()
                .map(|row| {
                    row.snapshot_content.as_deref().map_or(
                        crate::json_store::JsonSlot::None,
                        crate::json_store::JsonSlot::from_json,
                    )
                })
                .collect::<Vec<_>>();
            let metadata = branch_rows
                .iter()
                .map(|row| {
                    row.metadata.as_deref().map_or(
                        crate::json_store::JsonSlot::None,
                        crate::json_store::JsonSlot::from_json,
                    )
                })
                .collect::<Vec<_>>();
            let deltas = branch_rows
                .iter()
                .zip(snapshots.iter())
                .zip(metadata.iter())
                .map(|((row, snapshot), metadata)| CurrentStateDeltaRef {
                    schema_key: &row.schema_key,
                    file_id: row.file_id.as_deref(),
                    entity_pk: &row.entity_pk,
                    change_id: None,
                    commit_id: None,
                    untracked: true,
                    deleted: row.deleted,
                    created_at: ts(&row.created_at),
                    updated_at: ts(&row.updated_at),
                    snapshot: snapshot.as_ref_slot(),
                    metadata: metadata.as_ref_slot(),
                })
                .collect::<Vec<_>>();
            let schema_keys = parent_rows
                .iter()
                .map(|row| row.schema_key.clone())
                .chain(branch_rows.iter().map(|row| row.schema_key.clone()))
                .collect::<Vec<_>>();
            let mut working_diff_coverage = WorkingDiffIndexCoverage::default();
            let generation = TrackedHeadContext::new()
                .writer(&read, &mut writes)
                .stage_current_state_with_working_diff(
                    &branch_id,
                    None,
                    head_commit_id,
                    &deltas,
                    &std::collections::BTreeSet::new(),
                    Some(parent_rows),
                    None,
                    None,
                    &mut working_diff_coverage,
                )
                .await
                .expect("test current-state generation should stage");
            let mut control = BranchHeadControl {
                head_commit_id,
                generation,
                current_state_revision: 0,
                schema_presence_bloom: [0; 4],
                working_diff_checkpoint_commit_id: None,
                created_at,
                updated_at,
                ref_change_id: ChangeId::for_test_label(&format!("test-branch-ref-{branch_id}")),
            };
            control.note_schemas(schema_keys.iter().map(String::as_str));
            crate::branch::stage_branch_head_control(&mut writes, &branch_id, control)
                .expect("test branch-head control should stage");
        }

        // A pure untracked write mutates the active generation in place and
        // publishes a distinct control revision so concurrent writers cannot
        // satisfy the same stale control precondition.
        for (branch_id, branch_rows) in rows_by_branch {
            let control = BranchHeadControlContext::new()
                .reader(&read)
                .load(&branch_id)
                .await
                .expect("test branch control should load")
                .expect("untracked fixture needs an existing branch control");
            let snapshots = branch_rows
                .iter()
                .map(|row| {
                    row.snapshot_content.as_deref().map_or(
                        crate::json_store::JsonSlot::None,
                        crate::json_store::JsonSlot::from_json,
                    )
                })
                .collect::<Vec<_>>();
            let metadata = branch_rows
                .iter()
                .map(|row| {
                    row.metadata.as_deref().map_or(
                        crate::json_store::JsonSlot::None,
                        crate::json_store::JsonSlot::from_json,
                    )
                })
                .collect::<Vec<_>>();
            let deltas = branch_rows
                .iter()
                .zip(snapshots.iter())
                .zip(metadata.iter())
                .map(|((row, snapshot), metadata)| CurrentStateDeltaRef {
                    schema_key: &row.schema_key,
                    file_id: row.file_id.as_deref(),
                    entity_pk: &row.entity_pk,
                    change_id: None,
                    commit_id: None,
                    untracked: true,
                    deleted: row.deleted,
                    created_at: ts(&row.created_at),
                    updated_at: ts(&row.updated_at),
                    snapshot: snapshot.as_ref_slot(),
                    metadata: metadata.as_ref_slot(),
                })
                .collect::<Vec<_>>();
            let mut working_diff_coverage = WorkingDiffIndexCoverage::default();
            TrackedHeadContext::new()
                .writer(&read, &mut writes)
                .stage_current_state_with_working_diff(
                    &branch_id,
                    Some(control.generation),
                    control.head_commit_id,
                    &deltas,
                    &std::collections::BTreeSet::new(),
                    None,
                    None,
                    None,
                    &mut working_diff_coverage,
                )
                .await
                .expect("test untracked current state should stage");
            crate::branch::stage_branch_head_control(
                &mut writes,
                &branch_id,
                control
                    .next_current_state_revision()
                    .expect("test branch control revision should advance"),
            )
            .expect("test untracked control should stage");
        }
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
    async fn live_state_serves_untracked_member_from_current_state() {
        let storage = StorageAdapter::new(Memory::new());
        let live_state = live_state_context();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = StorageWriteSet::new();
        let mut json_writer = JsonStoreContext::new().writer();
        // Keep the tracked commit fixture separate from the untracked member
        // under test: a single identity cannot change retention class.
        let mut tracked_row =
            tracked_row_with_commit("tracked-value", Some("change-tracked"), "commit-tracked");
        tracked_row.entity_pk = identity("tracked-tab");
        stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &[tracked_row])
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
        assert_eq!(rows[0].change_id, None);

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
        assert_eq!(loaded.change_id, None);
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"untracked-value\"}")
        );
    }

    #[tokio::test]
    async fn exact_batch_preserves_duplicate_and_missing_slots_for_current_rows() {
        let storage = StorageAdapter::new(Memory::new());
        let live_state = live_state_context();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = StorageWriteSet::new();
        let mut json_writer = JsonStoreContext::new().writer();
        // The tracked fixture establishes the branch head; use a distinct
        // identity so the selected untracked row is not a retention conflict.
        let mut tracked_row =
            tracked_row_with_commit("tracked-value", Some("change-tracked"), "commit-tracked");
        tracked_row.entity_pk = identity("tracked-tab");
        stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &[tracked_row])
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
        let live_state = LiveStateContext::new(tracked_state.clone(), CommitGraphContext::new());

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
    async fn scan_rows_resolves_requested_branch_over_global() {
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
