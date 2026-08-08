#![allow(clippy::borrow_deref_ref, clippy::clone_on_copy)]

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

/// Transaction-local branch publication controls.
///
/// A transaction is fenced by the tracked mutation revision observed when it
/// opens, so repeatedly loading the same immutable generation selector only
/// adds storage round trips. Missing controls are cached as well: branch
/// creation rotates that revision and therefore conflicts with the pinned
/// transaction before commit.
#[derive(Default)]
pub(crate) struct BranchHeadControlCache;

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

    /// Creates a reader whose branch generation selectors are pinned to one
    /// transaction-local cache.
    pub(crate) fn transaction_reader<S>(
        &self,
        store: S,
        _branch_head_control_cache: std::sync::Arc<BranchHeadControlCache>,
    ) -> LiveStateStoreReader<S>
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
        let [branch_id] = request.filter.branch_ids.as_slice() else {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "ForkTree live-state operation requires exactly one branch",
            ));
        };
        let facade = crate::forktree::ForkTreeReadFacade::new(&self.store);
        let view = facade.branch(branch_id).await?;
        crate::live_state::scan_forktree_view(&view, request).await
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
        _branch_id: &str,
        _scope: crate::collection_generation::CollectionScopeRef<'_>,
    ) -> Result<Option<crate::collection_generation::CollectionGeneration>, LixError> {
        Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "collection generation is deferred until its ForkTree publication owner is lowered",
        ))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GLOBAL_BRANCH_ID;
    use crate::NullableKeyFilter;
    use crate::changelog::{
        ChangeId, ChangeRecord, ChangelogAppend, ChangelogContext, ChangelogReader, CommitId,
        CommitLoadRequest,
    };
    use crate::entity_pk::EntityPk;
    use crate::json_store::{JsonRef, JsonStoreContext, JsonWritePlacementRef, NormalizedJsonRef};
    use crate::live_state::{
        CurrentStateDeltaRef, LiveStateExactBatchRequest, LiveStateExactRowRequest,
        LiveStateFilter, LiveStateProjection, TrackedHeadDeltaRef, WorkingDiffIndexCoverage,
    };
    use crate::storage_adapter::{Memory, StorageReadOptions, StorageWriteOptions};
    use crate::storage_adapter::{StorageAdapter, StorageWriteSet};
    use crate::tracked_state::{
        CommitStateManifest, CommitStateReplayDebt, MaterializedTrackedStateRow,
        TrackedStateCommitDeltaRef, TrackedStateDeltaRef, TrackedStateScanRequest,
        stage_commit_deltas_for_commit_state, stage_commit_state_manifest,
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

    #[tokio::test]
    async fn transaction_branch_head_control_cache_pins_loaded_generation() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "ffffffff-ffff-7fff-bfff-ffffffffffff";
        let entity_pk = EntityPk::single("cached-control-row");
        stage_direct_entity_head(
            &storage,
            branch_id,
            CommitId::for_test_label("cached-control-head"),
            "schema",
            &entity_pk,
            r#"{"value":"one"}"#,
        )
        .await;

        let cache = BranchHeadControlCache::default();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open first branch-control read");
        let first = load_branch_head_controls(&read, &[branch_id.to_string()], Some(&cache))
            .await
            .expect("first branch control should load")[branch_id];
        drop(read);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open branch-control update read");
        let mut current = BranchHeadControlContext::new()
            .reader(&read)
            .load(branch_id)
            .await
            .expect("branch control should load")
            .expect("branch control should exist");
        current.current_state_revision += 1;
        let mut writes = StorageWriteSet::new();
        crate::branch::stage_branch_head_control(&mut writes, branch_id, current)
            .expect("updated branch control should stage");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("updated branch control should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open repeated branch-control read");
        let pinned = load_branch_head_controls(&read, &[branch_id.to_string()], Some(&cache))
            .await
            .expect("cached branch control should load")[branch_id];
        let uncached = load_branch_head_controls(&read, &[branch_id.to_string()], None)
            .await
            .expect("uncached branch control should load")[branch_id];
        assert_eq!(pinned, first);
        assert_eq!(uncached, current);
        assert_ne!(
            pinned.current_state_revision,
            uncached.current_state_revision
        );

        let full_cache = BranchHeadControlCache::default();
        {
            let mut controls = full_cache
                .controls
                .lock()
                .expect("branch-control cache lock should not be poisoned");
            for index in 0..TRANSACTION_BRANCH_HEAD_CONTROL_CACHE_MAX_ENTRIES {
                controls.insert(format!("uncached-branch-{index}"), None);
            }
        }
        let overflow =
            load_branch_head_controls(&read, &[branch_id.to_string()], Some(&full_cache))
                .await
                .expect("control beyond the cache capacity should still load")[branch_id];
        assert_eq!(overflow, current);
        assert!(
            !full_cache
                .controls
                .lock()
                .expect("branch-control cache lock should not be poisoned")
                .contains_key(branch_id)
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
                    .scan_batch_at_commit(
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
                    .into_rows()
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
                tracked_generation: generation,
                untracked_generation: generation,
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
                    Some(control.tracked_generation),
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
        let mut records = std::collections::BTreeMap::new();
        for commit_id in commit_ids {
            let commit_id_text = CommitId::for_test_label(commit_id).to_string();
            let commit_change_id = format!("{commit_id_text}:commit");
            let record = crate::changelog::CommitRecord {
                format_version: 2,
                commit_id: CommitId::for_test_label(&commit_id_text),
                generation: 0,
                parent_commit_ids: Vec::new(),
                change_id: ChangeId::for_test_label(&commit_change_id),
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: ts("1970-01-01T00:00:00.000Z"),
            };
            records.insert(record.commit_id, record.clone());
            append.commits.push(record);
        }
        let mut changelog_read = read;
        let mut writer = ChangelogContext::new().writer(&mut changelog_read, &mut writes);
        crate::changelog::ChangelogWriter::stage_append(&mut writer, append)
            .await
            .expect("empty changelog commits should stage");
        drop(writer);
        for commit_id in commit_ids {
            let commit_id_text = CommitId::for_test_label(commit_id).to_string();
            let typed_commit_id = CommitId::for_test_label(commit_id);
            let tracked_state = TrackedStateContext::new();
            let mut root_writer = tracked_state.writer(read, &mut writes);
            root_writer
                .stage_commit_root(&commit_id_text, None, [])
                .await
                .expect("empty tracked roots should stage");
            let snapshot_root = root_writer
                .staged_commit_roots()
                .find(|root| root.commit_id == typed_commit_id)
                .cloned()
                .expect("empty tracked snapshot should stage");
            drop(root_writer);
            let record = records
                .get(&typed_commit_id)
                .expect("empty commit record should exist");
            stage_commit_state_manifest(
                &mut writes,
                &CommitStateManifest {
                    commit_id: record.commit_id,
                    change_account_id: record.account_id.clone(),
                    replay_debt: CommitStateReplayDebt::default(),
                    mutations: Default::default(),
                    touched_scope_filter: Default::default(),
                    current_state_scoped_ranges: None,
                    snapshot_root: Some(Box::new(snapshot_root)),
                },
            )
            .expect("empty commit-state authority should stage");
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

        let mut generations = std::collections::BTreeMap::<String, u64>::new();
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
            let commit_change_id = format!("{commit_id}:commit");
            let generation = if let Some(parent) = parent_ids.first() {
                let parent_generation = if let Some(generation) = generations.get(parent) {
                    *generation
                } else {
                    let typed_parent = CommitId::for_test_label(parent);
                    let mut changelog_read = store;
                    ChangelogContext::new()
                        .reader(&mut changelog_read)
                        .load_commits(CommitLoadRequest {
                            commit_ids: &[typed_parent],
                        })
                        .await?
                        .into_iter()
                        .next()
                        .and_then(|(_, value)| value)
                        .ok_or_else(|| {
                            LixError::unknown("test changelog parent commit is missing")
                        })?
                        .generation
                };
                parent_generation
                    .checked_add(1)
                    .ok_or_else(|| LixError::unknown("test commit generation exceeds u64"))?
            } else {
                0
            };
            let typed_parent_ids = parent_ids
                .iter()
                .map(|id| CommitId::for_test_label(id))
                .collect::<Vec<_>>();
            let record = crate::changelog::CommitRecord {
                format_version: 2,
                commit_id: CommitId::for_test_label(&commit_id),
                generation,
                parent_commit_ids: typed_parent_ids,
                change_id: ChangeId::for_test_label(&commit_change_id),
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: commit_created_at,
            };
            let mut append = ChangelogAppend::default();
            append.commits.push(record.clone());
            let mut changelog_read = store;
            let mut writer = ChangelogContext::new().writer(&mut changelog_read, writes);
            crate::changelog::ChangelogWriter::stage_append(&mut writer, append).await?;
            drop(writer);
            generations.insert(commit_id.clone(), generation);
            let typed_commit_id = CommitId::for_test_label(&commit_id);
            let root_deltas = rows
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
            let commit_deltas = rows
                .iter()
                .zip(&root_deltas)
                .map(|((change, _, _), delta)| TrackedStateCommitDeltaRef {
                    delta: *delta,
                    snapshot: change.snapshot.as_ref_slot(),
                    metadata: change.metadata.as_ref_slot(),
                    origin_key: change.origin_key.as_deref(),
                    base_coordinate: None,
                    authored: true,
                })
                .collect::<Vec<_>>();
            let staged_delta = stage_commit_deltas_for_commit_state(writes, &commit_deltas)?;
            let mutation_inventory = staged_delta.mutation_inventory().clone();
            let tracked_state = TrackedStateContext::new();
            let mut root_writer = tracked_state.writer(&*store, writes);
            root_writer
                .stage_commit_root(&commit_id, parent_commit_id.as_deref(), root_deltas)
                .await?;
            let snapshot_root = root_writer
                .staged_commit_roots()
                .find(|root| root.commit_id == typed_commit_id)
                .cloned()
                .ok_or_else(|| LixError::unknown("test materialization did not stage a root"))?;
            drop(root_writer);
            stage_commit_state_manifest(
                writes,
                &CommitStateManifest {
                    commit_id: record.commit_id,
                    change_account_id: record.account_id.clone(),
                    replay_debt: CommitStateReplayDebt::default(),
                    mutations: mutation_inventory,
                    touched_scope_filter: Default::default(),
                    current_state_scoped_ranges: None,
                    snapshot_root: Some(Box::new(snapshot_root)),
                },
            )?;
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
                branch_ref_row("ffffffff-ffff-7fff-bfff-ffffffffffff", "commit-tracked"),
                untracked_row("untracked-value"),
            ],
        )
        .await;

        let rows = scan_selected_tab_at(
            &live_state,
            &storage,
            "ffffffff-ffff-7fff-bfff-ffffffffffff",
            false,
        )
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
                branch_id: "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string(),
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
                branch_ref_row("ffffffff-ffff-7fff-bfff-ffffffffffff", "commit-tracked"),
                untracked_row("untracked-value"),
            ],
        )
        .await;

        let selected = LiveStateExactRowRequest {
            schema_key: "lix_key_value".to_string(),
            branch_id: "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string(),
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
            .load_exact_batch(&LiveStateExactBatchRequest {
                rows: vec![
                    selected.clone(),
                    selected,
                    LiveStateExactRowRequest {
                        schema_key: "lix_key_value".to_string(),
                        branch_id: "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string(),
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
            .expect("exact batch should load")
            .into_rows();

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
            &[branch_ref_row(
                "ffffffff-ffff-7fff-bfff-ffffffffffff",
                "commit-tracked",
            )],
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
                branch_ref_row("ffffffff-ffff-7fff-bfff-ffffffffffff", "commit-global"),
                branch_ref_row(
                    "01920000-0000-7000-8000-0000000000a1",
                    "commit-01920000-0000-7000-8000-0000000000a1",
                ),
            ],
        )
        .await;
        write_empty_commits_to_store(
            &storage,
            &read,
            &["commit-01920000-0000-7000-8000-0000000000a1"],
        )
        .await;

        let loaded = load_selected_tab_at(
            &live_state,
            &storage,
            "01920000-0000-7000-8000-0000000000a1",
        )
        .await
        .expect("load should succeed")
        .expect("global row should be visible for requested branch");

        assert_eq!(
            loaded.branch_id.as_ref(),
            "01920000-0000-7000-8000-0000000000a1"
        );
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
                branch_ref_row("ffffffff-ffff-7fff-bfff-ffffffffffff", "commit-global"),
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
                    "01920000-0000-7000-8000-0000000000a1",
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
                branch_ref_row("ffffffff-ffff-7fff-bfff-ffffffffffff", "commit-global"),
                branch_ref_row("01920000-0000-7000-8000-0000000000a1", "commit-branch"),
            ],
        )
        .await;

        let loaded = load_selected_tab_at(
            &live_state,
            &storage,
            "01920000-0000-7000-8000-0000000000a1",
        )
        .await
        .expect("load should succeed")
        .expect("branch row should be visible");

        assert_eq!(
            loaded.branch_id.as_ref(),
            "01920000-0000-7000-8000-0000000000a1"
        );
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
                branch_ref_row("ffffffff-ffff-7fff-bfff-ffffffffffff", "commit-global"),
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
                    "01920000-0000-7000-8000-0000000000a1",
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
                branch_ref_row("ffffffff-ffff-7fff-bfff-ffffffffffff", "commit-global"),
                branch_ref_row("01920000-0000-7000-8000-0000000000a1", "commit-branch"),
            ],
        )
        .await;

        let rows = scan_selected_tab_at(
            &live_state,
            &storage,
            "01920000-0000-7000-8000-0000000000a1",
            false,
        )
        .await
        .expect("scan should succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].branch_id.as_ref(),
            "01920000-0000-7000-8000-0000000000a1"
        );
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
                branch_ref_row("ffffffff-ffff-7fff-bfff-ffffffffffff", "commit-global"),
                branch_ref_row(
                    "01920000-0000-7000-8000-0000000000a1",
                    "commit-01920000-0000-7000-8000-0000000000a1",
                ),
            ],
        )
        .await;
        write_empty_commits_to_store(
            &storage,
            &read,
            &["commit-01920000-0000-7000-8000-0000000000a1"],
        )
        .await;

        let rows = scan_selected_tab_at(
            &live_state,
            &storage,
            "01920000-0000-7000-8000-0000000000a1",
            false,
        )
        .await
        .expect("scan should succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].branch_id.as_ref(),
            "01920000-0000-7000-8000-0000000000a1"
        );
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
            &[branch_ref_row(
                "ffffffff-ffff-7fff-bfff-ffffffffffff",
                "commit-global",
            )],
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
                    "01920000-0000-7000-8000-0000000000a1",
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
                branch_ref_row("ffffffff-ffff-7fff-bfff-ffffffffffff", "commit-global"),
                branch_ref_row("01920000-0000-7000-8000-0000000000a1", "commit-branch"),
            ],
        )
        .await;

        let hidden = scan_selected_tab_at(
            &live_state,
            &storage,
            "01920000-0000-7000-8000-0000000000a1",
            false,
        )
        .await
        .expect("scan should succeed");
        assert_eq!(hidden.len(), 0);

        let with_tombstone = scan_selected_tab_at(
            &live_state,
            &storage,
            "01920000-0000-7000-8000-0000000000a1",
            true,
        )
        .await
        .expect("scan should succeed");
        assert_eq!(with_tombstone.len(), 1);
        assert_eq!(
            with_tombstone[0].branch_id.as_ref(),
            "01920000-0000-7000-8000-0000000000a1"
        );
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
                branch_ref_row("ffffffff-ffff-7fff-bfff-ffffffffffff", "commit-global"),
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
        global_fallback.metadata = Some("{\"source\":\"global\"}".into());
        let mut global_overridden =
            tracked_row_with_commit("global-old", Some("change-global-old"), "commit-global");
        global_overridden.entity_pk = identity("overridden");
        global_overridden.file_id = Some("overridden".to_string());
        let mut branch_override = tracked_row_at_with_commit(
            "01920000-0000-7000-8000-0000000000a1",
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
            "01920000-0000-7000-8000-0000000000a1",
            Some("change-tombstone"),
            "commit-branch",
        );
        branch_tombstone.entity_pk = identity("hidden");
        branch_tombstone.file_id = Some("hidden".to_string());
        let mut malformed_cross_pair =
            tracked_row_with_commit("cross-pair", Some("change-cross"), "commit-global");
        malformed_cross_pair.entity_pk = identity("entity-a");
        malformed_cross_pair.file_id = Some("01920000-0000-7000-8000-0000000000b2".to_string());

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
                branch_ref_row("ffffffff-ffff-7fff-bfff-ffffffffffff", "commit-global"),
                branch_ref_row("01920000-0000-7000-8000-0000000000a1", "commit-branch"),
            ],
        )
        .await;

        let exact = |entity: &str, file_id: &str| LiveStateExactRowRequest {
            schema_key: "lix_key_value".to_string(),
            branch_id: "01920000-0000-7000-8000-0000000000a1".to_string(),
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
            .load_exact_batch(&LiveStateExactBatchRequest {
                rows: vec![
                    exact("fallback", "fallback"),
                    exact("overridden", "overridden"),
                    exact("hidden", "hidden"),
                    exact("entity-a", "01920000-0000-7000-8000-0000000000a2"),
                    exact("entity-b", "01920000-0000-7000-8000-0000000000b2"),
                    exact("entity-a", "01920000-0000-7000-8000-0000000000b2"),
                    exact("missing", "missing"),
                ],
                projection: LiveStateProjection {
                    columns: vec!["snapshot_content".to_string()],
                },
                ..Default::default()
            })
            .await
            .expect("exact tracked batch should load")
            .into_rows();

        let fallback = loaded[0].as_ref().expect("global fallback should load");
        assert!(fallback.global);
        assert_eq!(
            fallback.branch_id.as_ref(),
            "01920000-0000-7000-8000-0000000000a1"
        );
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
            .load_exact_batch(&LiveStateExactBatchRequest {
                rows: vec![exact("hidden", "hidden")],
                include_tombstones: true,
                ..Default::default()
            })
            .await
            .expect("exact tombstone read should load")
            .into_rows()
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
                    "01920000-0000-7000-8000-0000000000a1",
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
            &[branch_ref_row(
                "01920000-0000-7000-8000-0000000000a1",
                "commit-branch",
            )],
        )
        .await;

        let loaded = load_selected_tab_at(
            &live_state,
            &storage,
            "01920000-0000-7000-8000-0000000000a1",
        )
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
                    "01920000-0000-7000-8000-0000000000a1",
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
                branch_id: "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string(),
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
            .scan_batch(&LiveStateScanRequest {
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
            .map(MaterializedLiveStateBatch::into_rows)
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
            .scan_batch_at_commit(
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
            .into_rows()
    }

    fn tracked_row_with_commit(
        value: &str,
        change_id: Option<&str>,
        commit_id: &str,
    ) -> MaterializedLiveStateRow {
        tracked_row_at_with_commit(
            "ffffffff-ffff-7fff-bfff-ffffffffffff",
            value,
            change_id,
            commit_id,
        )
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
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}").into()),
            metadata: None,
            deleted: false,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:00Z"),
            global: branch_id == "ffffffff-ffff-7fff-bfff-ffffffffffff",
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
        untracked_row_at("ffffffff-ffff-7fff-bfff-ffffffffffff", value)
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
            branch_id: "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string(),
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
                .expect("test metadata should serialize")
                .into(),
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
                serde_json::to_string(&snapshot)
                    .expect("commit snapshot should serialize")
                    .into(),
            ),
            metadata: None,
            deleted: false,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:00Z"),
            global: true,
            change_id: Some(ChangeId::for_test_label(&format!("change-{commit_id}"))),
            commit_id: Some(commit_id),
            untracked: false,
            branch_id: "ffffffff-ffff-7fff-bfff-ffffffffffff".into(),
        }
    }

    fn identity(entity_pk: &str) -> EntityPk {
        EntityPk::single(entity_pk)
    }
}
