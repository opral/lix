#![allow(clippy::borrow_deref_ref, clippy::clone_on_copy)]

use async_trait::async_trait;
use futures_util::{StreamExt, TryStreamExt, stream};

use crate::GLOBAL_BRANCH_ID;
use crate::LixError;
use crate::NullableKeyFilter;
use crate::branch::BRANCH_REF_SCHEMA_KEY;
use crate::commit_graph::CommitGraphContext;
use crate::current_state::{
    CurrentStateContext, CurrentStateFilter, CurrentStateRowRequest, CurrentStateScanRequest,
};
use crate::entity_pk::EntityPk;
use crate::filesystem::{
    FilesystemPathIndex, FilesystemPathIndexCache, FilesystemPathIndexReader,
    FilesystemPathIndexRequest, build_path_index, load_path_index_revision,
};
use crate::live_state::{
    LiveStateReader, LiveStateRowRequest, LiveStateScanRequest, MaterializedLiveStateRow,
    VisibilityBranchScope, VisibilityRequest, expanded_branch_ids, resolve_visible_rows,
};
use crate::storage::StorageRead;
use crate::tracked_state::{
    MaterializedTrackedStateRow, TrackedStateContext, TrackedStateFilter, TrackedStateReadColumns,
    TrackedStateScanRequest,
};

const BRANCH_READ_CONCURRENCY: usize = 8;

const COMMIT_SCHEMA_KEY: &str = "lix_commit";
const COMMIT_EDGE_SCHEMA_KEY: &str = "lix_commit_edge";

/// Serving facade for visible live-state reads.
///
/// Live state serves one canonical current-state root per branch. Immutable
/// tracked roots remain a separate history and validation concern.
pub(crate) struct LiveStateContext {
    tracked_state: TrackedStateContext,
    current_state: CurrentStateContext,
    commit_graph: CommitGraphContext,
    filesystem_path_index_cache: std::sync::Arc<FilesystemPathIndexCache>,
}

impl LiveStateContext {
    pub(crate) fn new(
        tracked_state: TrackedStateContext,
        current_state: CurrentStateContext,
        commit_graph: CommitGraphContext,
    ) -> Self {
        Self {
            tracked_state,
            current_state,
            commit_graph,
            filesystem_path_index_cache: std::sync::Arc::new(FilesystemPathIndexCache::default()),
        }
    }

    /// Creates a visible live-state reader over a caller-provided KV store.
    pub(crate) fn reader<S>(&self, store: S) -> LiveStateStoreReader<S>
    where
        S: StorageRead,
    {
        LiveStateStoreReader {
            store,
            tracked_state: self.tracked_state.clone(),
            current_state: self.current_state.clone(),
            commit_graph: self.commit_graph.clone(),
            filesystem_path_index_cache: std::sync::Arc::clone(&self.filesystem_path_index_cache),
        }
    }
}

/// Visible live-state reader backed by a caller-provided KV store.
pub(crate) struct LiveStateStoreReader<S> {
    store: S,
    tracked_state: TrackedStateContext,
    current_state: CurrentStateContext,
    commit_graph: CommitGraphContext,
    filesystem_path_index_cache: std::sync::Arc<FilesystemPathIndexCache>,
}

impl<S> LiveStateStoreReader<S>
where
    S: StorageRead,
{
    pub(crate) async fn scan_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        let store = &self.store;
        let scope = scan_scope(store, &self.current_state, request).await?;
        let derived_rows =
            scan_commit_derived_rows(store, &self.commit_graph, request, &scope).await?;
        let mut rows = Vec::new();
        if !is_commit_derived_only_request(request) {
            for branch_id in &scope.storage_branch_ids {
                rows.extend(
                    self.current_state
                        .reader(store)
                        .scan_rows(&current_scan_request_from_live(request, branch_id))
                        .await?
                        .into_iter()
                        .map(MaterializedLiveStateRow::from)
                        .filter(|row| {
                            request
                                .filter
                                .untracked
                                .is_none_or(|untracked| row.untracked == untracked)
                        }),
                );
            }
        }
        rows.extend(derived_rows);
        rows = resolve_visible_rows(
            rows,
            Vec::new(),
            &VisibilityRequest {
                branch_scope: VisibilityBranchScope::BranchIds {
                    branch_ids: scope.projection_branch_ids.clone(),
                },
                include_tombstones: request.filter.include_tombstones,
                limit: request.limit,
            },
        );
        Ok(rows)
    }

    pub(crate) async fn load_row(
        &self,
        request: &LiveStateRowRequest,
    ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
        {
            if !branch_ref_exists(&self.store, &self.current_state, &request.branch_id).await? {
                return Ok(None);
            }
        }
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

    pub(crate) async fn scan_tracked_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        let store = &self.store;
        let scope = scan_scope(store, &self.current_state, request).await?;
        let mut rows = scan_commit_derived_rows(store, &self.commit_graph, request, &scope).await?;
        if !is_commit_derived_only_request(request) {
            for branch_id in &scope.storage_branch_ids {
                let Some(commit_id) =
                    load_branch_ref_commit_id(store, &self.current_state, branch_id).await?
                else {
                    continue;
                };
                let source = tracked_source_from_branch_id(branch_id);
                rows.extend(
                    self.tracked_state
                        .reader(store)
                        .scan_rows_at_commit(&commit_id, &tracked_scan_request_from_live(request))
                        .await?
                        .into_iter()
                        .map(|row| project_tracked_row(row, branch_id, source)),
                );
            }
        }
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
}

#[async_trait]
impl<S> LiveStateReader for LiveStateStoreReader<S>
where
    S: StorageRead,
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
    S: StorageRead + Send + Sync,
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
    store: &(impl StorageRead + ?Sized),
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
                || request.filter.branch_ids.contains(&row.branch_id))
    });
    Ok(rows)
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
        created_at: commit.change.created_at.to_string(),
        updated_at: commit.change.created_at.to_string(),
        global: true,
        change_id: Some(commit.change.id),
        commit_id: Some(commit.commit_id),
        untracked: false,
        branch_id: branch_id.to_string(),
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
        created_at: "1970-01-01T00:00:00.000Z".to_string(),
        updated_at: "1970-01-01T00:00:00.000Z".to_string(),
        global: true,
        change_id: None,
        commit_id: Some(edge.child_commit_id),
        untracked: false,
        branch_id: branch_id.to_string(),
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

fn current_scan_request_from_live(
    request: &LiveStateScanRequest,
    branch_id: &str,
) -> CurrentStateScanRequest {
    CurrentStateScanRequest {
        branch_id: branch_id.to_string(),
        filter: CurrentStateFilter {
            schema_keys: request.filter.schema_keys.clone(),
            entity_pks: request.filter.entity_pks.clone(),
            file_ids: request.filter.file_ids.clone(),
            // Tombstones must win global/branch resolution before the caller's
            // requested visibility is applied.
            include_tombstones: true,
        },
        projection: request.projection.columns.clone(),
        limit: None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LiveStateScanScope {
    storage_branch_ids: Vec<String>,
    projection_branch_ids: Vec<String>,
}

async fn scan_scope(
    store: &(impl StorageRead + Send + Sync + ?Sized),
    current_state: &CurrentStateContext,
    request: &LiveStateScanRequest,
) -> Result<LiveStateScanScope, LixError> {
    if request.filter.branch_ids.is_empty() {
        return Ok(LiveStateScanScope {
            storage_branch_ids: all_branch_ref_ids(store, current_state).await?,
            projection_branch_ids: Vec::new(),
        });
    }

    let mut projection_branch_ids = Vec::new();
    for branch_id in &request.filter.branch_ids {
        if branch_ref_exists(store, current_state, branch_id).await? {
            projection_branch_ids.push(branch_id.clone());
        }
    }

    let storage_branch_ids = expanded_branch_ids(&projection_branch_ids);
    Ok(LiveStateScanScope {
        storage_branch_ids,
        projection_branch_ids,
    })
}

async fn all_branch_ref_ids(
    store: &(impl StorageRead + Send + Sync + ?Sized),
    current_state: &CurrentStateContext,
) -> Result<Vec<String>, LixError> {
    let rows = current_state
        .reader(store)
        .scan_rows(&CurrentStateScanRequest {
            branch_id: GLOBAL_BRANCH_ID.to_string(),
            filter: CurrentStateFilter {
                schema_keys: vec![BRANCH_REF_SCHEMA_KEY.to_string()],
                ..Default::default()
            },
            projection: Vec::new(),
            limit: None,
        })
        .await?;
    rows.into_iter()
        .map(|row| row.entity_pk.as_single_string_owned())
        .collect()
}

async fn load_branch_ref_commit_id(
    store: &(impl StorageRead + Send + Sync + ?Sized),
    current_state: &CurrentStateContext,
    branch_id: &str,
) -> Result<Option<String>, LixError> {
    let Some(row) = current_state
        .reader(store)
        .load_row(&CurrentStateRowRequest {
            schema_key: BRANCH_REF_SCHEMA_KEY.to_string(),
            branch_id: GLOBAL_BRANCH_ID.to_string(),
            entity_pk: EntityPk::single(branch_id),
            file_id: None,
        })
        .await?
    else {
        return Ok(None);
    };
    let Some(snapshot_content) = row.snapshot_content.as_deref() else {
        return Ok(None);
    };
    let snapshot =
        serde_json::from_str::<serde_json::Value>(snapshot_content).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("live_state branch-ref snapshot parse failed: {error}"),
            )
        })?;
    Ok(snapshot
        .get("commit_id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string))
}

async fn branch_ref_exists(
    store: &(impl StorageRead + Send + Sync + ?Sized),
    current_state: &CurrentStateContext,
    branch_id: &str,
) -> Result<bool, LixError> {
    Ok(load_branch_ref_commit_id(store, current_state, branch_id)
        .await?
        .is_some())
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
        created_at: row.created_at,
        updated_at: row.updated_at,
        global: source == TrackedRowSource::Global,
        change_id: Some(row.change_id),
        commit_id: Some(row.commit_id),
        untracked: false,
        branch_id: view_branch_id.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NullableKeyFilter;
    use crate::changelog::{ChangeId, ChangeRecord, ChangelogAppend, CommitId};
    use crate::current_state::{CurrentStateContext, CurrentStateDeltaRef};
    use crate::entity_pk::EntityPk;
    use crate::json_store::{JsonRef, JsonStoreContext, JsonWritePlacementRef, NormalizedJsonRef};
    use crate::live_state::LiveStateFilter;
    use crate::storage::{InMemoryStorageBackend, StorageReadOptions, StorageWriteOptions};
    use crate::storage::{StorageContext, StorageWriteSet};
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
            CurrentStateContext::new(),
            CommitGraphContext::new(),
        )
    }

    async fn write_untracked_rows_to_store(
        storage: &StorageContext,
        _read: &(impl StorageRead + ?Sized),
        rows: &[MaterializedUntrackedStateRow],
    ) {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("current-state read should open");
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

        let mut rows_by_branch = std::collections::BTreeMap::<&str, Vec<_>>::new();
        for (row, change) in &changes {
            rows_by_branch
                .entry(&row.branch_id)
                .or_default()
                .push(CurrentStateDeltaRef {
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
        let current_state = CurrentStateContext::new();
        let mut current_writer = current_state.writer(&read, &mut writes);
        for (branch_id, deltas) in rows_by_branch {
            current_writer
                .stage_branch_rows(branch_id, deltas)
                .await
                .expect("current rows should write");
        }
        drop(current_writer);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("current rows should commit");
    }

    async fn write_empty_commits_to_store(
        storage: &StorageContext,
        read: &impl StorageRead,
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

            let change_id = format!("{commit_id_text}:commit");
            let entity_pk = EntityPk::single(&commit_id_text);
            let deltas = [TrackedStateDeltaRef {
                schema_key: COMMIT_SCHEMA_KEY,
                file_id: None,
                entity_pk: &entity_pk,
                change_id: ChangeId::for_test_label(&change_id),
                commit_id: CommitId::for_test_label(&commit_id_text),
                deleted: false,
                created_at: ts("1970-01-01T00:00:00.000Z"),
                updated_at: ts("1970-01-01T00:00:00.000Z"),
            }];
            TrackedStateContext::new()
                .writer(read, &mut writes)
                .stage_commit_root(&commit_id_text, None, deltas)
                .await
                .expect("empty tracked roots should stage");
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("empty commits should commit");
    }

    async fn stage_materialized_live_rows(
        store: &impl StorageRead,
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
                current_rows.push((row.branch_id.clone(), materialized.clone()));
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
            let commit_entity_pk = EntityPk::single(&commit_id);
            let typed_commit_id = CommitId::for_test_label(&commit_id);
            let mut deltas = rows
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
            deltas.push(TrackedStateDeltaRef {
                schema_key: COMMIT_SCHEMA_KEY,
                file_id: None,
                entity_pk: &commit_entity_pk,
                change_id: ChangeId::for_test_label(&commit_change_id),
                commit_id: typed_commit_id,
                deleted: false,
                created_at: commit_created_at,
                updated_at: commit_created_at,
            });
            TrackedStateContext::new()
                .writer(&*store, writes)
                .stage_commit_root(&commit_id, parent_commit_id.as_deref(), deltas)
                .await?;
        }

        let mut current_rows_by_branch =
            std::collections::BTreeMap::<&str, Vec<CurrentStateDeltaRef<'_>>>::new();
        for (branch_id, row) in &current_rows {
            current_rows_by_branch
                .entry(branch_id)
                .or_default()
                .push(CurrentStateDeltaRef {
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
        let current_state = CurrentStateContext::new();
        let mut current_writer = current_state.writer(store, writes);
        for (branch_id, deltas) in current_rows_by_branch {
            current_writer.stage_branch_rows(branch_id, deltas).await?;
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
    async fn live_state_serves_untracked_change_from_current_root() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
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
    async fn tracked_row_is_visible_from_current_root() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
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
    async fn deleting_untracked_row_persists_tombstone_without_revealing_tracked_row() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
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
            &[
                branch_ref_row("global", "commit-tracked"),
                untracked_row("untracked-value"),
            ],
        )
        .await;
        write_untracked_rows_to_store(&storage, &read, &[untracked_tombstone_at("global")]).await;

        let loaded = load_selected_tab(&live_state, &storage)
            .await
            .expect("load should succeed");
        assert_eq!(loaded, None, "the tracked predecessor must stay hidden");

        let rows = scan_selected_tab_at(&live_state, &storage, "global", true)
            .await
            .expect("tombstone scan should succeed");
        assert_eq!(rows.len(), 1);
        assert!(rows[0].deleted);
        assert!(rows[0].untracked);
        assert!(rows[0].change_id.is_some());
    }

    #[tokio::test]
    async fn load_row_falls_back_to_global_tracked_row_for_requested_branch() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
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

        assert_eq!(loaded.branch_id, "branch-a");
        assert!(loaded.global);
        assert!(!loaded.untracked);
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"global-tracked\"}")
        );
    }

    #[tokio::test]
    async fn main_sees_global_row_by_reading_global_root_separately() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        let live_state = LiveStateContext::new(
            tracked_state.clone(),
            crate::current_state::CurrentStateContext::new(),
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
        assert_eq!(loaded.branch_id, "main");
        assert!(loaded.global);
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"global-tracked\"}")
        );

        let main_root_rows = scan_tracked_root(&tracked_state, &storage, "commit-main").await;
        assert_eq!(
            main_root_rows.len(),
            1,
            "empty commit root should contain only its derived lix_commit row"
        );
        assert_eq!(main_root_rows[0].schema_key, "lix_commit");
    }

    #[tokio::test]
    async fn load_row_prefers_requested_branch_over_global() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
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

        assert_eq!(loaded.branch_id, "branch-a");
        assert!(!loaded.untracked);
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"branch-tracked\"}")
        );
    }

    #[tokio::test]
    async fn main_override_hides_global_row() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
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

        assert_eq!(loaded.branch_id, "main");
        assert!(!loaded.global);
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"main-tracked\"}")
        );
    }

    #[tokio::test]
    async fn load_row_prefers_requested_untracked_over_requested_tracked_and_global_rows() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
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
                untracked_row_at("global", "global-untracked"),
                untracked_row_at("branch-a", "branch-untracked"),
            ],
        )
        .await;

        let loaded = load_selected_tab_at(&live_state, &storage, "branch-a")
            .await
            .expect("load should succeed")
            .expect("branch untracked row should be visible");

        assert_eq!(loaded.branch_id, "branch-a");
        assert!(loaded.untracked);
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"branch-untracked\"}")
        );
    }

    #[tokio::test]
    async fn scan_rows_overlays_requested_branch_over_global() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
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
        assert_eq!(rows[0].branch_id, "branch-a");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"branch-tracked\"}")
        );
    }

    #[tokio::test]
    async fn scan_rows_projects_global_row_into_requested_branch() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
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
        assert_eq!(rows[0].branch_id, "branch-a");
        assert!(rows[0].global);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"global-tracked\"}")
        );
    }

    #[tokio::test]
    async fn scan_rows_does_not_project_global_rows_into_missing_branch() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
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
        let storage = StorageContext::new(InMemoryStorageBackend::new());
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
        assert_eq!(with_tombstone[0].branch_id, "branch-a");
        assert_eq!(with_tombstone[0].snapshot_content, None);
    }

    #[tokio::test]
    async fn main_tombstone_hides_global_row() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
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
        assert_eq!(tombstones[0].branch_id, "main");
        assert!(!tombstones[0].global);
        assert_eq!(tombstones[0].snapshot_content, None);
    }

    #[tokio::test]
    async fn writer_allows_commit_fact_to_share_the_touched_branch_commit_id() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
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
        let storage = StorageContext::new(InMemoryStorageBackend::new());
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
        let storage = StorageContext::new(InMemoryStorageBackend::new());
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
        assert_eq!(global_root_rows.len(), 2);
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
        assert_eq!(main_root_rows.len(), 2);
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
        storage: &StorageContext,
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
        storage: &StorageContext,
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
        storage: &StorageContext,
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
        storage: &StorageContext,
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
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global: branch_id == "global",
            change_id: change_id.map(ChangeId::for_test_label),
            commit_id: Some(commit_id),
            untracked: false,
            branch_id: branch_id.to_string(),
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

    fn untracked_tombstone_at(branch_id: &str) -> MaterializedUntrackedStateRow {
        MaterializedUntrackedStateRow {
            snapshot_content: None,
            deleted: true,
            updated_at: "2026-01-02T00:00:00Z".to_string(),
            ..untracked_row_at(branch_id, "ignored")
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
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global: true,
            change_id: Some(ChangeId::for_test_label(&format!("change-{commit_id}"))),
            commit_id: Some(commit_id),
            untracked: false,
            branch_id: "global".to_string(),
        }
    }

    fn identity(entity_pk: &str) -> EntityPk {
        EntityPk::single(entity_pk)
    }
}
