#![allow(clippy::borrow_deref_ref, clippy::clone_on_copy)]

use crate::GLOBAL_BRANCH_ID;
use crate::LixError;
#[cfg(test)]
use crate::branch::BRANCH_REF_SCHEMA_KEY;
use crate::commit_graph::CommitGraphContext;
use crate::filesystem::{
    FilesystemPathIndex, FilesystemPathIndexCache, FilesystemPathIndexReader,
    FilesystemPathIndexRequest, build_path_index, load_path_index_revision,
};
use crate::live_state::{
    LiveStateExactBatchRequest, LiveStateReader, LiveStateRowRequest, LiveStateScanRequest,
    MaterializedLiveStateBatch, MaterializedLiveStateExactBatch, MaterializedLiveStateRow,
    MaterializedLiveStateRowRef,
};
use crate::storage_adapter::StorageAdapterRead;
use crate::tracked_state::TrackedStateContext;
#[cfg(test)]
use crate::tracked_state::{TrackedStateFilter, TrackedStateReadColumns};
use async_trait::async_trait;
#[cfg(test)]
use std::sync::Mutex as StdMutex;

/// Serving facade for visible live-state reads.
///
/// Normal rows are resolved from one durable hot-state projection. Each row
/// carries its own tracked|untracked retention, so readers do not route
/// through a separate retention index or merge retention candidates.
pub(crate) struct LiveStateContext {
    commit_graph: CommitGraphContext,
    filesystem_path_index_cache: std::sync::Arc<FilesystemPathIndexCache>,
}

impl LiveStateContext {
    pub(crate) fn new(
        _tracked_state: TrackedStateContext,
        commit_graph: CommitGraphContext,
    ) -> Self {
        Self {
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
            commit_graph: self.commit_graph.clone(),
            filesystem_path_index_cache: std::sync::Arc::clone(&self.filesystem_path_index_cache),
        }
    }

    /// Creates a reader whose derived indexes are private to one retained
    /// storage snapshot. Process-wide caches intentionally advance with live
    /// commits and therefore cannot serve an older explicit transaction.
    pub(crate) fn snapshot_reader<S>(&self, store: S) -> LiveStateStoreReader<S>
    where
        S: StorageAdapterRead,
    {
        LiveStateStoreReader {
            store,
            commit_graph: self.commit_graph.clone(),
            filesystem_path_index_cache: std::sync::Arc::new(FilesystemPathIndexCache::default()),
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

    pub(crate) fn clear_filesystem_path_indexes(&self) {
        self.filesystem_path_index_cache.clear();
    }
}

/// Visible live-state reader backed by a caller-provided KV store.
pub(crate) struct LiveStateStoreReader<S> {
    store: S,
    commit_graph: CommitGraphContext,
    filesystem_path_index_cache: std::sync::Arc<FilesystemPathIndexCache>,
}

impl<S> LiveStateStoreReader<S>
where
    S: StorageAdapterRead,
{
    pub(crate) async fn scan_batch(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        self.scan_forktree_operation(request).await
    }

    pub(crate) async fn load_row(
        &self,
        request: &LiveStateRowRequest,
    ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
        let rows = self
            .scan_batch(&LiveStateScanRequest {
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
        Ok(rows.get(0).map(MaterializedLiveStateRowRef::to_owned))
    }

    pub(crate) async fn load_exact_batch(
        &self,
        request: &LiveStateExactBatchRequest,
    ) -> Result<MaterializedLiveStateExactBatch, LixError> {
        crate::live_state::load_forktree_exact_batch(&self.store, request).await
    }

    pub(crate) async fn scan_tracked_batch(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        self.scan_forktree_operation(request).await
    }

    async fn scan_forktree_operation(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        let branch_ids = &request.filter.branch_ids;
        let non_global_branch_ids = branch_ids
            .iter()
            .filter(|branch_id| branch_id.as_str() != GLOBAL_BRANCH_ID)
            .collect::<Vec<_>>();
        let has_global = branch_ids
            .iter()
            .any(|branch_id| branch_id.as_str() == GLOBAL_BRANCH_ID);
        let valid_branch_scope = match non_global_branch_ids.as_slice() {
            [] => branch_ids.len() == 1 && has_global,
            [branch_id] => {
                branch_ids.len() == 1 + usize::from(has_global)
                    && branch_ids.iter().all(|candidate| {
                        candidate.as_str() == GLOBAL_BRANCH_ID || candidate == *branch_id
                    })
            }
            _ => false,
        };
        if !valid_branch_scope {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "ForkTree live-state operation requires exactly one branch",
            ));
        }
        let branch_id = non_global_branch_ids
            .first()
            .map_or_else(|| GLOBAL_BRANCH_ID, |branch_id| branch_id.as_str());
        let mut fork_tree_request = request.clone();
        fork_tree_request.filter.branch_ids = vec![branch_id.to_owned()];
        let facade = crate::forktree::ForkTreeReadFacade::new(&self.store);
        let view = facade.branch(branch_id).await?;
        crate::live_state::scan_forktree_view(&view, &fork_tree_request).await
    }
}

#[async_trait]
impl<S> LiveStateReader for LiveStateStoreReader<S>
where
    S: StorageAdapterRead,
{
    async fn scan_constraint_batch(
        &self,
        request: &LiveStateScanRequest,
        tracked_only: bool,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        let _ = tracked_only;
        self.scan_forktree_operation(request).await
    }

    async fn scan_batch(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        Self::scan_batch(self, request).await
    }

    async fn load_exact_batch(
        &self,
        request: &LiveStateExactBatchRequest,
    ) -> Result<MaterializedLiveStateExactBatch, LixError> {
        Self::load_exact_batch(self, request).await
    }

    async fn collection_generation(
        &self,
        branch_id: &str,
        scope: crate::collection_generation::CollectionScopeRef<'_>,
    ) -> Result<Option<crate::collection_generation::CollectionGeneration>, LixError> {
        let rows = self
            .scan_forktree_operation(&LiveStateScanRequest {
                filter: crate::live_state::LiveStateFilter {
                    schema_keys: vec![
                        crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY.to_owned(),
                    ],
                    branch_ids: vec![branch_id.to_owned()],
                    include_tombstones: true,
                    ..Default::default()
                },
                ..Default::default()
            })
            .await?;
        let expected_scope = crate::collection_generation::collection_scope_key(scope);
        for row in rows.iter() {
            if row.entity_pk() != &crate::entity_pk::EntityPk::single(&expected_scope)
                || row.file_id().is_some()
            {
                continue;
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
            return Ok(Some(crate::collection_generation::CollectionGeneration {
                active_generation,
                live_count,
                ordered_identity_digest: None,
            }));
        }
        Ok(None)
    }

    async fn scan_tracked_batch(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        Self::scan_tracked_batch(self, request).await
    }
}

#[async_trait]
impl<S> FilesystemPathIndexReader for LiveStateStoreReader<S>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
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
            let store = self.store.clone();
            index = std::sync::Arc::new((*index).clone().hydrate_small_blob_data(&store).await?);
        }
        Ok(self
            .filesystem_path_index_cache
            .insert(request, revision.as_deref(), index))
    }
}
