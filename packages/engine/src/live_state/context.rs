use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::branch::BRANCH_REF_SCHEMA_KEY;
use crate::commit_graph::CommitGraphContext;
use crate::entity_pk::EntityPk;
use crate::live_state::{
    expanded_branch_ids, resolve_visible_rows, LiveStateReader, LiveStateRowRequest,
    LiveStateScanRequest, MaterializedLiveStateRow, VisibilityBranchScope, VisibilityRequest,
};
use crate::storage::StorageRead;
use crate::tracked_state::{
    MaterializedTrackedStateRow, TrackedStateContext, TrackedStateFilter, TrackedStateReadColumns,
    TrackedStateScanRequest,
};
use crate::untracked_state::{
    UntrackedStateContext, UntrackedStateRowRequest, UntrackedStateScanRequest,
};
use crate::LixError;
use crate::NullableKeyFilter;
use crate::GLOBAL_BRANCH_ID;

const COMMIT_SCHEMA_KEY: &str = "lix_commit";
const COMMIT_EDGE_SCHEMA_KEY: &str = "lix_commit_edge";

/// Serving facade for visible live-state reads.
///
/// Live state composes the rebuildable tracked projection with the durable
/// untracked local overlay. Lower stores own persistence; this facade owns the
/// visibility rule.
pub(crate) struct LiveStateContext {
    tracked_state: TrackedStateContext,
    untracked_state: UntrackedStateContext,
    commit_graph: CommitGraphContext,
}

impl LiveStateContext {
    pub(crate) fn new(
        tracked_state: TrackedStateContext,
        untracked_state: UntrackedStateContext,
        commit_graph: CommitGraphContext,
    ) -> Self {
        Self {
            tracked_state,
            untracked_state,
            commit_graph,
        }
    }

    /// Creates a visible live-state reader over a caller-provided KV store.
    pub(crate) fn reader<S>(&self, store: S) -> LiveStateStoreReader<S>
    where
        S: StorageRead + Send + Sync,
    {
        LiveStateStoreReader {
            store: Mutex::new(store),
            tracked_state: self.tracked_state.clone(),
            untracked_state: self.untracked_state,
            commit_graph: self.commit_graph.clone(),
        }
    }
}

/// Visible live-state reader backed by a caller-provided KV store.
pub(crate) struct LiveStateStoreReader<S> {
    store: Mutex<S>,
    tracked_state: TrackedStateContext,
    untracked_state: UntrackedStateContext,
    commit_graph: CommitGraphContext,
}

impl<S> LiveStateStoreReader<S>
where
    S: StorageRead + Send + Sync,
{
    pub(crate) async fn scan_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        let store = self.store.lock().await;
        let scope = scan_scope(&*store, &self.untracked_state, request).await?;
        let derived_rows =
            scan_commit_derived_rows(&*store, &self.commit_graph, request, &scope).await?;
        let mut tracked_rows = Vec::new();
        if request.filter.untracked != Some(true) && !is_commit_derived_only_request(request) {
            for branch_id in &scope.storage_branch_ids {
                let Some(commit_id) =
                    load_branch_ref_commit_id(&*store, &self.untracked_state, branch_id).await?
                else {
                    continue;
                };
                let tracked_request = tracked_scan_request_from_live(request);
                let source = tracked_source_from_branch_id(branch_id);
                let store = &*store;
                tracked_rows.extend(
                    self.tracked_state
                        .reader(store)
                        .scan_rows_at_commit(&commit_id, &tracked_request)
                        .await?
                        .into_iter()
                        .map(|row| project_tracked_row(row, branch_id, source)),
                );
            }
        }

        let untracked_rows = if request.filter.untracked != Some(false) {
            let store = &*store;
            self.untracked_state
                .reader(store)
                .scan_rows(&untracked_scan_request_from_live(
                    request,
                    &scope.storage_branch_ids,
                ))
                .await?
                .into_iter()
                .map(MaterializedLiveStateRow::from)
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        let mut rows = if request.filter.untracked.is_some() {
            tracked_rows
                .into_iter()
                .chain(untracked_rows)
                .chain(derived_rows)
                .collect()
        } else {
            crate::live_state::overlay::overlay_untracked_rows(tracked_rows, untracked_rows)
                .into_iter()
                .chain(derived_rows)
                .collect()
        };
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
            let store = self.store.lock().await;
            if !branch_ref_exists(&*store, &self.untracked_state, &request.branch_id).await? {
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
}

#[async_trait]
impl<S> LiveStateReader for LiveStateStoreReader<S>
where
    S: StorageRead + Send + Sync,
{
    async fn scan_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        LiveStateStoreReader::scan_rows(self, request).await
    }

    async fn load_row(
        &self,
        request: &LiveStateRowRequest,
    ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
        LiveStateStoreReader::load_row(self, request).await
    }
}

async fn scan_commit_derived_rows(
    store: &(impl StorageRead + Send + Sync + ?Sized),
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
        entity_pk: EntityPk::single(commit.commit_id.clone()),
        schema_key: COMMIT_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot_content: Some(snapshot_content),
        metadata: None,
        deleted: false,
        created_at: commit.change.created_at.clone(),
        updated_at: commit.change.created_at.clone(),
        global: true,
        change_id: Some(commit.change.id.clone()),
        commit_id: Some(commit.commit_id.clone()),
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
            parts: vec![edge.parent_commit_id.clone(), edge.child_commit_id.clone()],
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
        commit_id: Some(edge.child_commit_id.clone()),
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

fn untracked_scan_request_from_live(
    request: &LiveStateScanRequest,
    branch_ids: &[String],
) -> UntrackedStateScanRequest {
    let mut filter: crate::untracked_state::UntrackedStateFilter = request.filter.clone().into();
    filter.branch_ids = branch_ids.to_vec();
    UntrackedStateScanRequest {
        filter,
        projection: crate::untracked_state::UntrackedStateProjection {
            columns: request.projection.columns.clone(),
        },
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
    untracked_state: &UntrackedStateContext,
    request: &LiveStateScanRequest,
) -> Result<LiveStateScanScope, LixError> {
    if request.filter.branch_ids.is_empty() {
        return Ok(LiveStateScanScope {
            storage_branch_ids: all_branch_ref_ids(store, untracked_state).await?,
            projection_branch_ids: Vec::new(),
        });
    }

    let mut projection_branch_ids = Vec::new();
    for branch_id in &request.filter.branch_ids {
        if branch_ref_exists(store, untracked_state, branch_id).await? {
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
    untracked_state: &UntrackedStateContext,
) -> Result<Vec<String>, LixError> {
    let rows = untracked_state
        .reader(store)
        .scan_rows(&UntrackedStateScanRequest {
            filter: crate::untracked_state::UntrackedStateFilter {
                schema_keys: vec![BRANCH_REF_SCHEMA_KEY.to_string()],
                branch_ids: vec![GLOBAL_BRANCH_ID.to_string()],
                ..Default::default()
            },
            ..Default::default()
        })
        .await?;
    rows.into_iter()
        .map(|row| row.entity_pk.as_single_string_owned())
        .collect()
}

async fn load_branch_ref_commit_id(
    store: &(impl StorageRead + Send + Sync + ?Sized),
    untracked_state: &UntrackedStateContext,
    branch_id: &str,
) -> Result<Option<String>, LixError> {
    let Some(row) = untracked_state
        .reader(store)
        .load_row(&UntrackedStateRowRequest {
            schema_key: BRANCH_REF_SCHEMA_KEY.to_string(),
            branch_id: GLOBAL_BRANCH_ID.to_string(),
            entity_pk: crate::entity_pk::EntityPk::single(branch_id),
            file_id: crate::NullableKeyFilter::Null,
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
    untracked_state: &UntrackedStateContext,
    branch_id: &str,
) -> Result<bool, LixError> {
    Ok(load_branch_ref_commit_id(store, untracked_state, branch_id)
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
    use crate::entity_pk::EntityPk;
    use crate::json_store::{JsonRef, JsonStoreContext, JsonWritePlacementRef, NormalizedJsonRef};
    use crate::live_state::LiveStateFilter;
    use crate::storage::{InMemoryStorageBackend, StorageReadOptions, StorageWriteOptions};
    use crate::storage::{StorageContext, StorageWriteSet};
    use crate::tracked_state::{TrackedStateDeltaRef, TrackedStateScanRequest};
    use crate::untracked_state::{MaterializedUntrackedStateRow, UntrackedStateContext};
    use crate::NullableKeyFilter;
    use serde_json::json;

    const COMMIT_SCHEMA_KEY: &str = "lix_commit";

    fn live_state_context() -> LiveStateContext {
        LiveStateContext::new(
            crate::tracked_state::TrackedStateContext::new(),
            crate::untracked_state::UntrackedStateContext::new(),
            crate::commit_graph::CommitGraphContext::new(),
        )
    }

    async fn write_untracked_rows_to_store(
        storage: &StorageContext,
        _read: &(impl crate::storage::StorageRead + Send + Sync + ?Sized),
        rows: &[MaterializedUntrackedStateRow],
    ) {
        let mut writes = storage.new_write_set();
        let canonical_rows = rows
            .iter()
            .map(|row| crate::test_support::untracked_state_row_from_materialized(&mut writes, row))
            .collect::<Result<Vec<_>, _>>()
            .expect("untracked rows should canonicalize");
        UntrackedStateContext::new()
            .writer(&mut writes)
            .stage_rows(canonical_rows.iter().map(|row| row.as_ref()))
            .expect("untracked rows should write");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("untracked rows should commit");
    }

    async fn write_empty_commits_to_store(
        storage: &StorageContext,
        read: &(impl crate::storage::StorageRead + Send + Sync),
        commit_ids: &[&str],
    ) {
        let mut writes = storage.new_write_set();
        let mut json_writer = JsonStoreContext::new().writer();
        let mut append = crate::changelog::ChangelogAppend::default();
        for commit_id in commit_ids {
            let commit_change_id = format!("{commit_id}:commit");
            append.commits.push(crate::changelog::CommitRecord {
                format_version: 1,
                commit_id: (*commit_id).to_string(),
                parent_commit_ids: Vec::new(),
                change_id: commit_change_id.clone(),
                author_account_ids: Vec::new(),
                created_at: "1970-01-01T00:00:00.000Z".to_string(),
            });
            append
                .commit_change_refs
                .push(crate::changelog::CommitChangeRefSet {
                    commit_id: (*commit_id).to_string(),
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
            let snapshot_content =
                commit_row_snapshot_content(commit_id).expect("commit snapshot should encode");
            let snapshot_ref = JsonRef::for_content(snapshot_content.as_bytes());
            json_writer
                .stage_batch(
                    &mut writes,
                    JsonWritePlacementRef::OutOfBand,
                    [NormalizedJsonRef::trusted_prehashed(
                        &snapshot_content,
                        snapshot_ref.clone(),
                    )],
                )
                .expect("commit snapshot should stage");
            let change_id = format!("{commit_id}:commit");
            let entity_pk = EntityPk::single(*commit_id);
            let deltas = [TrackedStateDeltaRef {
                schema_key: COMMIT_SCHEMA_KEY,
                file_id: None,
                entity_pk: &entity_pk,
                change_id: &change_id,
                commit_id,
                snapshot_ref: Some(&snapshot_ref),
                metadata_ref: None,
                deleted: false,
                created_at: "1970-01-01T00:00:00.000Z",
                updated_at: "1970-01-01T00:00:00.000Z",
            }];
            TrackedStateContext::new()
                .writer(read, &mut writes)
                .stage_commit_root(commit_id, None, deltas)
                .await
                .expect("empty tracked roots should stage");
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("empty commits should commit");
    }

    async fn stage_materialized_live_rows(
        store: &(impl StorageRead + Send + Sync),
        writes: &mut StorageWriteSet,
        json_writer: &mut crate::json_store::JsonStoreWriter,
        rows: &[MaterializedLiveStateRow],
    ) -> Result<(), LixError> {
        let mut untracked_rows = Vec::new();
        let mut tracked_rows_by_commit = std::collections::BTreeMap::<
            String,
            Vec<(crate::changelog::ChangeRecord, String, String)>,
        >::new();
        let mut parent_by_commit = std::collections::BTreeMap::<String, Option<String>>::new();

        for row in rows {
            if row.untracked {
                let materialized = crate::untracked_state::MaterializedUntrackedStateRow::from(row);
                let canonical = crate::test_support::untracked_state_row_from_materialized(
                    writes,
                    &materialized,
                )?;
                untracked_rows.push(canonical);
                continue;
            }
            let materialized = MaterializedTrackedStateRow::try_from(row)?;
            let commit_id = row.commit_id.clone().ok_or_else(|| {
                LixError::new("LIX_ERROR_UNKNOWN", "test tracked row missing commit_id")
            })?;
            if row.schema_key == COMMIT_SCHEMA_KEY {
                parent_by_commit.insert(
                    commit_id.clone(),
                    parent_commit_id_from_test_commit_row(row)?,
                );
            }
            if row.schema_key != COMMIT_SCHEMA_KEY {
                let change = crate::test_support::tracked_change_from_materialized(&materialized)?;
                stage_json_payloads_from_materialized(writes, json_writer, &materialized)?;
                tracked_rows_by_commit.entry(commit_id).or_default().push((
                    change,
                    materialized.created_at,
                    materialized.updated_at,
                ));
            }
        }

        UntrackedStateContext::new()
            .writer(writes)
            .stage_rows(untracked_rows.iter().map(|row| row.as_ref()))?;
        for (commit_id, rows) in tracked_rows_by_commit {
            let parent_commit_id = parent_by_commit.remove(&commit_id).flatten();
            let parent_ids = parent_commit_id
                .as_ref()
                .map(|parent| vec![parent.clone()])
                .unwrap_or_default();
            let commit_created_at = rows
                .first()
                .map(|(change, _, _)| change.created_at.as_str())
                .unwrap_or("1970-01-01T00:00:00.000Z")
                .to_string();
            let change_refs = rows
                .iter()
                .map(|(change, _, _)| crate::changelog::CommitChangeRef {
                    schema_key: change.schema_key.clone(),
                    file_id: change.file_id.clone(),
                    entity_pk: change.entity_pk.clone(),
                    change_id: change.change_id.clone(),
                })
                .collect::<Vec<_>>();
            let commit_change_id = format!("{commit_id}:commit");
            let mut append = crate::changelog::ChangelogAppend::default();
            append
                .changes
                .extend(rows.iter().map(|(change, _, _)| change.clone()));
            append.commits.push(crate::changelog::CommitRecord {
                format_version: 1,
                commit_id: commit_id.clone(),
                parent_commit_ids: parent_ids,
                change_id: commit_change_id.clone(),
                author_account_ids: Vec::new(),
                created_at: commit_created_at.clone(),
            });
            append
                .commit_change_refs
                .push(crate::changelog::CommitChangeRefSet {
                    commit_id: commit_id.clone(),
                    entries: change_refs,
                });
            let mut changelog_read = store;
            let mut writer =
                crate::changelog::ChangelogContext::new().writer(&mut changelog_read, writes);
            crate::changelog::ChangelogWriter::stage_append(&mut writer, append).await?;
            drop(writer);
            let snapshot_content = commit_row_snapshot_content(&commit_id)?;
            let snapshot_ref = JsonRef::for_content(snapshot_content.as_bytes());
            json_writer.stage_batch(
                writes,
                JsonWritePlacementRef::OutOfBand,
                [NormalizedJsonRef::trusted_prehashed(
                    &snapshot_content,
                    snapshot_ref.clone(),
                )],
            )?;
            let commit_entity_pk = EntityPk::single(&commit_id);
            let mut deltas = rows
                .iter()
                .map(|(change, created_at, updated_at)| TrackedStateDeltaRef {
                    schema_key: &change.schema_key,
                    file_id: change.file_id.as_deref(),
                    entity_pk: &change.entity_pk,
                    change_id: &change.change_id,
                    commit_id: &commit_id,
                    snapshot_ref: change.snapshot_ref.as_ref(),
                    metadata_ref: change.metadata_ref.as_ref(),
                    deleted: change.snapshot_ref.is_none(),
                    created_at,
                    updated_at,
                })
                .collect::<Vec<_>>();
            deltas.push(TrackedStateDeltaRef {
                schema_key: COMMIT_SCHEMA_KEY,
                file_id: None,
                entity_pk: &commit_entity_pk,
                change_id: &commit_change_id,
                commit_id: &commit_id,
                snapshot_ref: Some(&snapshot_ref),
                metadata_ref: None,
                deleted: false,
                created_at: &commit_created_at,
                updated_at: &commit_created_at,
            });
            TrackedStateContext::new()
                .writer(&*store, writes)
                .stage_commit_root(&commit_id, parent_commit_id.as_deref(), deltas)
                .await?;
        }
        Ok(())
    }

    fn commit_row_snapshot_content(commit_id: &str) -> Result<String, LixError> {
        serde_json::to_string(&json!({ "id": commit_id })).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("failed to encode test commit snapshot: {error}"),
            )
        })
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
    async fn live_state_overlays_untracked_rows() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let live_state = live_state_context();

        let read = storage
            .begin_read(StorageReadOptions::default())
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
        assert_eq!(rows[0].change_id, None);

        let loaded = live_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
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
            .expect("overlay row should be visible");
        assert!(loaded.untracked);
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"untracked-value\"}")
        );
    }

    #[tokio::test]
    async fn tracked_row_is_visible_without_untracked_overlay() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let live_state = live_state_context();

        let read = storage
            .begin_read(StorageReadOptions::default())
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
        assert_eq!(loaded.change_id.as_deref(), Some("change-tracked"));
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"tracked-value\"}")
        );
    }

    #[tokio::test]
    async fn deleting_untracked_row_reveals_tracked_row() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let live_state = live_state_context();

        let read = storage
            .begin_read(StorageReadOptions::default())
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
        {
            let mut writes = StorageWriteSet::new();
            let identity = crate::untracked_state::UntrackedStateIdentity {
                branch_id: "global".to_string(),
                schema_key: "lix_key_value".to_string(),
                entity_pk: EntityPk::single("selected-tab"),
                file_id: None,
            };
            UntrackedStateContext::new()
                .writer(&mut writes)
                .stage_delete_rows(std::iter::once(identity.as_ref()))
                .expect("delete identity should stage");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .expect("writes should commit");
        }

        let loaded = load_selected_tab(&live_state, &storage)
            .await
            .expect("load should succeed")
            .expect("tracked row should be visible again");
        assert!(!loaded.untracked);
        assert_eq!(loaded.change_id.as_deref(), Some("change-tracked"));
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"tracked-value\"}")
        );
    }

    #[tokio::test]
    async fn load_row_falls_back_to_global_tracked_row_for_requested_branch() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let live_state = live_state_context();

        let read = storage
            .begin_read(StorageReadOptions::default())
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
            UntrackedStateContext::new(),
            crate::commit_graph::CommitGraphContext::new(),
        );

        let read = storage
            .begin_read(StorageReadOptions::default())
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
            .expect("first parent tracked root should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
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
                .expect("writes should commit");
        }
    }

    #[tokio::test]
    async fn non_global_root_does_not_store_global_rows() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_state = TrackedStateContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
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
            change_id: change_id.map(str::to_string),
            commit_id: Some(commit_id.to_string()),
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
            global: branch_id == "global",
            branch_id: branch_id.to_string(),
        }
    }

    fn branch_ref_row(branch_id: &str, commit_id: &str) -> MaterializedUntrackedStateRow {
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
            global: true,
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
        let mut row = commit_live_state_row_with_snapshot(
            commit_id,
            json!({
                "id": commit_id,
            }),
        );
        row.metadata = Some(
            serde_json::to_string(&json!({ "test_parents": parent_ids }))
                .expect("test metadata should serialize"),
        );
        row
    }

    fn commit_live_state_row_with_snapshot(
        commit_id: &str,
        snapshot: serde_json::Value,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: identity(commit_id),
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
            change_id: Some(format!("change-{commit_id}")),
            commit_id: Some(commit_id.to_string()),
            untracked: false,
            branch_id: "global".to_string(),
        }
    }

    fn identity(entity_pk: &str) -> EntityPk {
        EntityPk::single(entity_pk)
    }
}
