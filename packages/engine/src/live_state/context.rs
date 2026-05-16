use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::commit_graph::CommitGraphContext;
use crate::entity_identity::EntityIdentity;
use crate::live_state::{
    expanded_version_ids, resolve_visible_rows, LiveStateReader, LiveStateRowFilter,
    LiveStateRowRequest, LiveStateScanRequest, MaterializedLiveStateRow, VisibilityRequest,
    VisibilityVersionScope,
};
use crate::storage::StorageReader;
use crate::tracked_state::{
    MaterializedTrackedStateRow, TrackedStateContext, TrackedStateFilter, TrackedStateProjection,
    TrackedStateScanRequest,
};
use crate::untracked_state::{
    UntrackedStateContext, UntrackedStateRowRequest, UntrackedStateScanRequest,
};
use crate::version::VERSION_REF_SCHEMA_KEY;
use crate::LixError;
use crate::NullableKeyFilter;
use crate::GLOBAL_VERSION_ID;

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
        S: StorageReader,
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
    S: StorageReader,
{
    pub(crate) async fn scan_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        if matches!(request.filter.rows, LiveStateRowFilter::None) {
            return Ok(Vec::new());
        }
        let mut store = self.store.lock().await;
        let scope = scan_scope(&mut *store, &self.untracked_state, request).await?;
        let derived_rows =
            scan_commit_derived_rows(&mut *store, &self.commit_graph, request, &scope).await?;
        let mut tracked_rows = Vec::new();
        if request.filter.untracked != Some(true) && !is_commit_derived_only_request(request) {
            for version_id in &scope.storage_version_ids {
                let Some(commit_id) =
                    load_version_ref_commit_id(&mut *store, &self.untracked_state, version_id)
                        .await?
                else {
                    continue;
                };
                let tracked_request = tracked_scan_request_from_live(request);
                let source = tracked_source_from_version_id(version_id);
                let store: &mut dyn StorageReader = &mut *store;
                tracked_rows.extend(
                    self.tracked_state
                        .reader(store)
                        .scan_rows_at_commit(&commit_id, &tracked_request)
                        .await?
                        .into_iter()
                        .map(|row| project_tracked_row(row, version_id, source)),
                );
            }
        }

        let untracked_rows = if request.filter.untracked != Some(false) {
            let store: &mut dyn StorageReader = &mut *store;
            self.untracked_state
                .reader(store)
                .scan_rows(&untracked_scan_request_from_live(
                    request,
                    &scope.storage_version_ids,
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
                version_scope: VisibilityVersionScope::VersionIds {
                    version_ids: scope.projection_version_ids.clone(),
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
        let rows = self
            .scan_rows(&LiveStateScanRequest {
                filter: crate::live_state::LiveStateFilter {
                    schema_keys: vec![request.schema_key.clone()],
                    entity_ids: vec![request.entity_id.clone()],
                    version_ids: vec![request.version_id.clone()],
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
    S: StorageReader + Sync,
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
    store: &mut dyn StorageReader,
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

    let version_ids = if scope.projection_version_ids.is_empty() {
        vec![GLOBAL_VERSION_ID.to_string()]
    } else {
        scope.projection_version_ids.clone()
    };
    let mut graph = commit_graph.reader(store);
    let commits = graph.all_commits().await?;
    let include_commit = schema_filter_allows(&request.filter.schema_keys, COMMIT_SCHEMA_KEY);
    let include_commit_edge =
        schema_filter_allows(&request.filter.schema_keys, COMMIT_EDGE_SCHEMA_KEY);

    let mut rows = Vec::new();
    for version_id in &version_ids {
        if include_commit {
            for commit in &commits {
                rows.push(commit_row(commit, version_id)?);
            }
        }
        if include_commit_edge {
            for edge in graph.commit_edges(&commits) {
                rows.push(commit_edge_row(&edge, version_id)?);
            }
        }
    }

    rows.retain(|row| {
        (request.filter.entity_ids.is_empty() || request.filter.entity_ids.contains(&row.entity_id))
            && (request.filter.version_ids.is_empty()
                || request.filter.version_ids.contains(&row.version_id))
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
    version_id: &str,
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
        entity_id: EntityIdentity::single(commit.commit_id.clone()),
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
        version_id: version_id.to_string(),
    })
}

fn commit_edge_row(
    edge: &crate::commit_graph::CommitGraphEdge,
    version_id: &str,
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
        entity_id: EntityIdentity {
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
        version_id: version_id.to_string(),
    })
}

fn tracked_scan_request_from_live(request: &LiveStateScanRequest) -> TrackedStateScanRequest {
    TrackedStateScanRequest {
        filter: TrackedStateFilter {
            schema_keys: request.filter.schema_keys.clone(),
            entity_ids: request.filter.entity_ids.clone(),
            file_ids: request.filter.file_ids.clone(),
            // Scan tombstones internally so version-local tombstones can hide
            // global fallback rows before the serving facade filters them.
            include_tombstones: true,
        },
        projection: TrackedStateProjection {
            columns: request.projection.columns.clone(),
        },
        limit: None,
    }
}

fn untracked_scan_request_from_live(
    request: &LiveStateScanRequest,
    version_ids: &[String],
) -> UntrackedStateScanRequest {
    let mut filter: crate::untracked_state::UntrackedStateFilter = request.filter.clone().into();
    filter.version_ids = version_ids.to_vec();
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
    storage_version_ids: Vec<String>,
    projection_version_ids: Vec<String>,
}

async fn scan_scope(
    store: &mut dyn StorageReader,
    untracked_state: &UntrackedStateContext,
    request: &LiveStateScanRequest,
) -> Result<LiveStateScanScope, LixError> {
    if request.filter.version_ids.is_empty() {
        return Ok(LiveStateScanScope {
            storage_version_ids: all_version_ref_ids(store, untracked_state).await?,
            projection_version_ids: Vec::new(),
        });
    }

    let mut projection_version_ids = Vec::new();
    for version_id in &request.filter.version_ids {
        if version_ref_exists(store, untracked_state, version_id).await? {
            projection_version_ids.push(version_id.clone());
        }
    }

    let storage_version_ids = expanded_version_ids(&projection_version_ids);
    Ok(LiveStateScanScope {
        storage_version_ids,
        projection_version_ids,
    })
}

async fn all_version_ref_ids(
    store: &mut dyn StorageReader,
    untracked_state: &UntrackedStateContext,
) -> Result<Vec<String>, LixError> {
    let rows = untracked_state
        .reader(store)
        .scan_rows(&UntrackedStateScanRequest {
            filter: crate::untracked_state::UntrackedStateFilter {
                schema_keys: vec![VERSION_REF_SCHEMA_KEY.to_string()],
                version_ids: vec![GLOBAL_VERSION_ID.to_string()],
                ..Default::default()
            },
            ..Default::default()
        })
        .await?;
    rows.into_iter()
        .map(|row| row.entity_id.as_single_string_owned())
        .collect()
}

async fn load_version_ref_commit_id(
    store: &mut dyn StorageReader,
    untracked_state: &UntrackedStateContext,
    version_id: &str,
) -> Result<Option<String>, LixError> {
    let Some(row) = untracked_state
        .reader(store)
        .load_row(&UntrackedStateRowRequest {
            schema_key: VERSION_REF_SCHEMA_KEY.to_string(),
            version_id: GLOBAL_VERSION_ID.to_string(),
            entity_id: crate::entity_identity::EntityIdentity::single(version_id),
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
                format!("live_state version-ref snapshot parse failed: {error}"),
            )
        })?;
    Ok(snapshot
        .get("commit_id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string))
}

async fn version_ref_exists(
    store: &mut dyn StorageReader,
    untracked_state: &UntrackedStateContext,
    version_id: &str,
) -> Result<bool, LixError> {
    Ok(
        load_version_ref_commit_id(store, untracked_state, version_id)
            .await?
            .is_some(),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TrackedRowSource {
    Global,
    Version,
}

fn tracked_source_from_version_id(version_id: &str) -> TrackedRowSource {
    if version_id == GLOBAL_VERSION_ID {
        TrackedRowSource::Global
    } else {
        TrackedRowSource::Version
    }
}

fn project_tracked_row(
    row: MaterializedTrackedStateRow,
    view_version_id: &str,
    source: TrackedRowSource,
) -> MaterializedLiveStateRow {
    MaterializedLiveStateRow {
        entity_id: row.entity_id,
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
        version_id: view_version_id.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::backend::{testing::UnitTestBackend, Backend};
    use crate::commit_store::{CommitDraftRef, CommitStoreContext};
    use crate::entity_identity::EntityIdentity;
    use crate::json_store::{
        JsonStoreContext, JsonWritePlacementRef, NormalizedJson, NormalizedJsonRef,
    };
    use crate::live_state::LiveStateFilter;
    use crate::storage::{StorageContext, StorageWriteSet, StorageWriteTransaction};
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
        store: &mut (impl StorageWriteTransaction + ?Sized),
        rows: &[MaterializedUntrackedStateRow],
    ) {
        let mut writes = StorageWriteSet::new();
        let canonical_rows = rows
            .iter()
            .map(|row| crate::test_support::untracked_state_row_from_materialized(&mut writes, row))
            .collect::<Result<Vec<_>, _>>()
            .expect("untracked rows should canonicalize");
        UntrackedStateContext::new()
            .writer(&mut writes)
            .stage_rows(canonical_rows.iter().map(|row| row.as_ref()))
            .expect("untracked rows should write");
        writes
            .apply(store)
            .await
            .expect("untracked rows should apply");
    }

    async fn write_empty_commits_to_store(
        store: &mut (impl StorageWriteTransaction + ?Sized),
        commit_ids: &[&str],
    ) {
        let mut writes = StorageWriteSet::new();
        for commit_id in commit_ids {
            let commit_change_id = format!("{commit_id}:commit");
            CommitStoreContext::new()
                .writer(&mut *store, &mut writes)
                .stage_commit_draft(
                    CommitDraftRef {
                        id: commit_id,
                        change_id: &commit_change_id,
                        parent_ids: &[],
                        author_account_ids: &[],
                        created_at: "1970-01-01T00:00:00.000Z",
                    },
                    Vec::new(),
                    Vec::new(),
                )
                .await
                .expect("empty commit should stage");
        }
        writes
            .apply(store)
            .await
            .expect("empty commits should apply");
    }

    async fn stage_materialized_live_rows(
        store: &mut (impl StorageReader + ?Sized),
        writes: &mut StorageWriteSet,
        _json_writer: &mut crate::json_store::JsonStoreWriter,
        rows: &[MaterializedLiveStateRow],
    ) -> Result<(), LixError> {
        let mut untracked_rows = Vec::new();
        let mut tracked_rows_by_commit = std::collections::BTreeMap::<
            String,
            Vec<(crate::commit_store::Change, String, String)>,
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
                stage_tracked_materialized_json(writes, &commit_id, &materialized)?;
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
            let commit_change_id = format!("{commit_id}:commit");
            let commit = CommitDraftRef {
                id: &commit_id,
                change_id: &commit_change_id,
                parent_ids: &parent_ids,
                author_account_ids: &[],
                created_at: rows
                    .first()
                    .map(|(change, _, _)| change.created_at.as_str())
                    .unwrap_or("1970-01-01T00:00:00.000Z"),
            };
            let staged = CommitStoreContext::new()
                .writer(&mut *store, writes)
                .stage_tracked_commit_draft(
                    commit,
                    rows.iter().map(|(change, _, _)| change.as_ref()).collect(),
                    Vec::new(),
                )
                .await?;
            let deltas = rows
                .iter()
                .zip(&staged.authored_locators)
                .map(
                    |((change, created_at, updated_at), locator)| TrackedStateDeltaRef {
                        change: change.as_ref(),
                        locator: locator.as_ref(),
                        created_at,
                        updated_at,
                    },
                )
                .collect::<Vec<_>>();
            TrackedStateContext::new()
                .writer(&mut *store, writes)
                .stage_delta(&commit_id, parent_commit_id.as_deref(), &deltas)
                .await?;
        }
        Ok(())
    }

    fn stage_tracked_materialized_json(
        writes: &mut StorageWriteSet,
        commit_id: &str,
        row: &MaterializedTrackedStateRow,
    ) -> Result<(), LixError> {
        let mut payloads = Vec::new();
        if let Some(snapshot) = row.snapshot_content.as_deref() {
            payloads.push(NormalizedJson::from_arc_unchecked(Arc::from(snapshot)));
        }
        if let Some(metadata) = row.metadata.as_ref() {
            payloads.push(NormalizedJson::from_arc_unchecked(Arc::from(
                crate::serialize_row_metadata(metadata),
            )));
        }
        JsonStoreContext::new().writer().stage_batch(
            writes,
            JsonWritePlacementRef::CommitPack {
                commit_id,
                pack_id: 0,
            },
            payloads
                .iter()
                .map(|payload| NormalizedJsonRef::from(payload)),
        )?;
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
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let live_state = live_state_context();

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        {
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(
                    transaction.as_mut(),
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
            writes
                .apply(&mut transaction.as_mut())
                .await
                .expect("tracked row should apply");
        }
        write_untracked_rows_to_store(
            transaction.as_mut(),
            &[
                version_ref_row("global", "commit-tracked"),
                untracked_row("untracked-value"),
            ],
        )
        .await;
        transaction.commit().await.expect("commit should persist");

        let rows = scan_selected_tab_at(&live_state, storage.clone(), "global", false)
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
            .reader(storage.clone())
            .load_row(&LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                version_id: "global".to_string(),
                entity_id: crate::entity_identity::EntityIdentity::single("selected-tab"),
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
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let live_state = live_state_context();

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        {
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(
                    transaction.as_mut(),
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
            writes
                .apply(&mut transaction.as_mut())
                .await
                .expect("tracked row should apply");
        }
        write_untracked_rows_to_store(
            transaction.as_mut(),
            &[version_ref_row("global", "commit-tracked")],
        )
        .await;
        transaction.commit().await.expect("commit should persist");

        let loaded = load_selected_tab(&live_state, storage.clone())
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
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let live_state = live_state_context();

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        {
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(
                    transaction.as_mut(),
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
            writes
                .apply(&mut transaction.as_mut())
                .await
                .expect("tracked row should apply");
        }
        write_untracked_rows_to_store(
            transaction.as_mut(),
            &[
                version_ref_row("global", "commit-tracked"),
                untracked_row("untracked-value"),
            ],
        )
        .await;
        {
            let mut writes = StorageWriteSet::new();
            let identity = crate::untracked_state::UntrackedStateIdentity {
                version_id: "global".to_string(),
                schema_key: "lix_key_value".to_string(),
                entity_id: EntityIdentity::single("selected-tab"),
                file_id: None,
            };
            UntrackedStateContext::new()
                .writer(&mut writes)
                .stage_delete_rows(std::iter::once(identity.as_ref()));
            writes
                .apply(&mut transaction.as_mut())
                .await
                .expect("untracked row should delete");
        }
        transaction.commit().await.expect("commit should persist");

        let loaded = load_selected_tab(&live_state, storage.clone())
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
    async fn load_row_falls_back_to_global_tracked_row_for_requested_version() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let live_state = live_state_context();

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        {
            let rows = [tracked_row_with_commit(
                "global-tracked",
                Some("change-global"),
                "commit-global",
            )];
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(
                    transaction.as_mut(),
                    &mut writes,
                    &mut json_writer,
                    &rows,
                )
                .await
                .expect("tracked row should stage");
            }
            writes
                .apply(&mut transaction.as_mut())
                .await
                .expect("tracked row should apply");
        }
        write_untracked_rows_to_store(
            transaction.as_mut(),
            &[
                version_ref_row("global", "commit-global"),
                version_ref_row("version-a", "commit-version-a"),
            ],
        )
        .await;
        write_empty_commits_to_store(transaction.as_mut(), &["commit-version-a"]).await;
        transaction.commit().await.expect("commit should persist");

        let loaded = load_selected_tab_at(&live_state, storage.clone(), "version-a")
            .await
            .expect("load should succeed")
            .expect("global row should be visible for requested version");

        assert_eq!(loaded.version_id, "version-a");
        assert!(loaded.global);
        assert!(!loaded.untracked);
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"global-tracked\"}")
        );
    }

    #[tokio::test]
    async fn main_sees_global_row_by_reading_global_root_separately() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let tracked_state = TrackedStateContext::new();
        let live_state = LiveStateContext::new(
            tracked_state.clone(),
            UntrackedStateContext::new(),
            crate::commit_graph::CommitGraphContext::new(),
        );

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        {
            let rows = [tracked_row_with_commit(
                "global-tracked",
                Some("change-global"),
                "commit-global",
            )];
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(
                    transaction.as_mut(),
                    &mut writes,
                    &mut json_writer,
                    &rows,
                )
                .await
                .expect("global tracked row should stage");
            }
            writes
                .apply(&mut transaction.as_mut())
                .await
                .expect("global tracked row should apply");
        }
        write_untracked_rows_to_store(
            transaction.as_mut(),
            &[
                version_ref_row("global", "commit-global"),
                version_ref_row("main", "commit-main"),
            ],
        )
        .await;
        write_empty_commits_to_store(transaction.as_mut(), &["commit-main"]).await;
        transaction.commit().await.expect("commit should persist");

        let loaded = load_selected_tab_at(&live_state, storage.clone(), "main")
            .await
            .expect("load should succeed")
            .expect("global row should be projected into main");
        assert_eq!(loaded.version_id, "main");
        assert!(loaded.global);
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"global-tracked\"}")
        );

        let main_root_rows =
            scan_tracked_root(&tracked_state, storage.clone(), "commit-main").await;
        assert_eq!(
            main_root_rows.len(),
            0,
            "global fallback must come from the global root, not a copied main root row"
        );
    }

    #[tokio::test]
    async fn load_row_prefers_requested_version_over_global() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let live_state = live_state_context();

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        {
            let rows = [
                tracked_row_with_commit("global-tracked", Some("change-global"), "commit-global"),
                tracked_row_at_with_commit(
                    "version-a",
                    "version-tracked",
                    Some("change-version"),
                    "commit-version",
                ),
            ];
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(
                    transaction.as_mut(),
                    &mut writes,
                    &mut json_writer,
                    &rows,
                )
                .await
                .expect("tracked rows should stage");
            }
            writes
                .apply(&mut transaction.as_mut())
                .await
                .expect("tracked rows should apply");
        }
        write_untracked_rows_to_store(
            transaction.as_mut(),
            &[
                version_ref_row("global", "commit-global"),
                version_ref_row("version-a", "commit-version"),
            ],
        )
        .await;
        transaction.commit().await.expect("commit should persist");

        let loaded = load_selected_tab_at(&live_state, storage.clone(), "version-a")
            .await
            .expect("load should succeed")
            .expect("version row should be visible");

        assert_eq!(loaded.version_id, "version-a");
        assert!(!loaded.untracked);
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"version-tracked\"}")
        );
    }

    #[tokio::test]
    async fn main_override_hides_global_row() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let live_state = live_state_context();

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
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
                stage_materialized_live_rows(
                    transaction.as_mut(),
                    &mut writes,
                    &mut json_writer,
                    &rows,
                )
                .await
                .expect("tracked rows should stage");
            }
            writes
                .apply(&mut transaction.as_mut())
                .await
                .expect("tracked rows should apply");
        }
        write_untracked_rows_to_store(
            transaction.as_mut(),
            &[
                version_ref_row("global", "commit-global"),
                version_ref_row("main", "commit-main"),
            ],
        )
        .await;
        transaction.commit().await.expect("commit should persist");

        let loaded = load_selected_tab_at(&live_state, storage.clone(), "main")
            .await
            .expect("load should succeed")
            .expect("main row should be visible");

        assert_eq!(loaded.version_id, "main");
        assert!(!loaded.global);
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"main-tracked\"}")
        );
    }

    #[tokio::test]
    async fn load_row_prefers_requested_untracked_over_requested_tracked_and_global_rows() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let live_state = live_state_context();

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        {
            let rows = [
                tracked_row_with_commit("global-tracked", Some("change-global"), "commit-global"),
                tracked_row_at_with_commit(
                    "version-a",
                    "version-tracked",
                    Some("change-version"),
                    "commit-version",
                ),
            ];
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(
                    transaction.as_mut(),
                    &mut writes,
                    &mut json_writer,
                    &rows,
                )
                .await
                .expect("tracked rows should stage");
            }
            writes
                .apply(&mut transaction.as_mut())
                .await
                .expect("tracked rows should apply");
        }
        write_untracked_rows_to_store(
            transaction.as_mut(),
            &[
                version_ref_row("global", "commit-global"),
                version_ref_row("version-a", "commit-version"),
                untracked_row_at("global", "global-untracked"),
                untracked_row_at("version-a", "version-untracked"),
            ],
        )
        .await;
        transaction.commit().await.expect("commit should persist");

        let loaded = load_selected_tab_at(&live_state, storage.clone(), "version-a")
            .await
            .expect("load should succeed")
            .expect("version untracked row should be visible");

        assert_eq!(loaded.version_id, "version-a");
        assert!(loaded.untracked);
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"version-untracked\"}")
        );
    }

    #[tokio::test]
    async fn scan_rows_overlays_requested_version_over_global() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let live_state = live_state_context();

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        {
            let rows = [
                tracked_row_with_commit("global-tracked", Some("change-global"), "commit-global"),
                tracked_row_at_with_commit(
                    "version-a",
                    "version-tracked",
                    Some("change-version"),
                    "commit-version",
                ),
            ];
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(
                    transaction.as_mut(),
                    &mut writes,
                    &mut json_writer,
                    &rows,
                )
                .await
                .expect("rows should stage");
            }
            writes
                .apply(&mut transaction.as_mut())
                .await
                .expect("rows should apply");
        }
        write_untracked_rows_to_store(
            transaction.as_mut(),
            &[
                version_ref_row("global", "commit-global"),
                version_ref_row("version-a", "commit-version"),
            ],
        )
        .await;
        transaction.commit().await.expect("commit should persist");

        let rows = scan_selected_tab_at(&live_state, storage.clone(), "version-a", false)
            .await
            .expect("scan should succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].version_id, "version-a");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"version-tracked\"}")
        );
    }

    #[tokio::test]
    async fn scan_rows_projects_global_row_into_requested_version() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let live_state = live_state_context();

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        {
            let rows = [tracked_row_with_commit(
                "global-tracked",
                Some("change-global"),
                "commit-global",
            )];
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(
                    transaction.as_mut(),
                    &mut writes,
                    &mut json_writer,
                    &rows,
                )
                .await
                .expect("rows should stage");
            }
            writes
                .apply(&mut transaction.as_mut())
                .await
                .expect("rows should apply");
        }
        write_untracked_rows_to_store(
            transaction.as_mut(),
            &[
                version_ref_row("global", "commit-global"),
                version_ref_row("version-a", "commit-version-a"),
            ],
        )
        .await;
        write_empty_commits_to_store(transaction.as_mut(), &["commit-version-a"]).await;
        transaction.commit().await.expect("commit should persist");

        let rows = scan_selected_tab_at(&live_state, storage.clone(), "version-a", false)
            .await
            .expect("scan should succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].version_id, "version-a");
        assert!(rows[0].global);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"global-tracked\"}")
        );
    }

    #[tokio::test]
    async fn scan_rows_does_not_project_global_rows_into_missing_version() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let live_state = live_state_context();

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        {
            let rows = [tracked_row_with_commit(
                "global-tracked",
                Some("change-global"),
                "commit-global",
            )];
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(
                    transaction.as_mut(),
                    &mut writes,
                    &mut json_writer,
                    &rows,
                )
                .await
                .expect("tracked row should stage");
            }
            writes
                .apply(&mut transaction.as_mut())
                .await
                .expect("tracked row should apply");
        }
        write_untracked_rows_to_store(
            transaction.as_mut(),
            &[version_ref_row("global", "commit-global")],
        )
        .await;
        transaction.commit().await.expect("commit should persist");

        let rows = scan_selected_tab_at(&live_state, storage.clone(), "missing-version", false)
            .await
            .expect("scan should succeed");

        assert_eq!(
            rows.len(),
            0,
            "global rows must not be projected into a missing version scope"
        );
    }

    #[tokio::test]
    async fn winning_tombstone_hides_row_unless_tombstones_are_included() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let live_state = live_state_context();

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        {
            let rows = [
                tracked_row_with_commit("global-tracked", Some("change-global"), "commit-global"),
                tombstone_tracked_row_at_with_commit(
                    "version-a",
                    Some("change-tombstone"),
                    "commit-version",
                ),
            ];
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(
                    transaction.as_mut(),
                    &mut writes,
                    &mut json_writer,
                    &rows,
                )
                .await
                .expect("rows should stage");
            }
            writes
                .apply(&mut transaction.as_mut())
                .await
                .expect("rows should apply");
        }
        write_untracked_rows_to_store(
            transaction.as_mut(),
            &[
                version_ref_row("global", "commit-global"),
                version_ref_row("version-a", "commit-version"),
            ],
        )
        .await;
        transaction.commit().await.expect("commit should persist");

        let hidden = scan_selected_tab_at(&live_state, storage.clone(), "version-a", false)
            .await
            .expect("scan should succeed");
        assert_eq!(hidden.len(), 0);

        let with_tombstone = scan_selected_tab_at(&live_state, storage.clone(), "version-a", true)
            .await
            .expect("scan should succeed");
        assert_eq!(with_tombstone.len(), 1);
        assert_eq!(with_tombstone[0].version_id, "version-a");
        assert_eq!(with_tombstone[0].snapshot_content, None);
    }

    #[tokio::test]
    async fn main_tombstone_hides_global_row() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let live_state = live_state_context();

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
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
                stage_materialized_live_rows(
                    transaction.as_mut(),
                    &mut writes,
                    &mut json_writer,
                    &rows,
                )
                .await
                .expect("tracked rows should stage");
            }
            writes
                .apply(&mut transaction.as_mut())
                .await
                .expect("tracked rows should apply");
        }
        write_untracked_rows_to_store(
            transaction.as_mut(),
            &[
                version_ref_row("global", "commit-global"),
                version_ref_row("main", "commit-main"),
            ],
        )
        .await;
        transaction.commit().await.expect("commit should persist");

        let hidden = scan_selected_tab_at(&live_state, storage.clone(), "main", false)
            .await
            .expect("scan should succeed");
        assert_eq!(hidden.len(), 0);

        let tombstones = scan_selected_tab_at(&live_state, storage.clone(), "main", true)
            .await
            .expect("scan should succeed");
        assert_eq!(tombstones.len(), 1);
        assert_eq!(tombstones[0].version_id, "main");
        assert!(!tombstones[0].global);
        assert_eq!(tombstones[0].snapshot_content, None);
    }

    #[tokio::test]
    async fn writer_allows_commit_fact_to_share_the_touched_version_commit_id() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let live_state = live_state_context();
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");

        {
            let rows = [
                tracked_row_at_with_commit(
                    "version-a",
                    "version-row",
                    Some("change-version"),
                    "commit-version",
                ),
                commit_live_state_row("commit-version"),
            ];
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(
                    transaction.as_mut(),
                    &mut writes,
                    &mut json_writer,
                    &rows,
                )
                .await
                .expect("commit facts are changelog projections, not root-local rows");
            }
            writes
                .apply(&mut transaction.as_mut())
                .await
                .expect("commit fact rows should apply");
        }
        write_untracked_rows_to_store(
            transaction.as_mut(),
            &[version_ref_row("version-a", "commit-version")],
        )
        .await;
        transaction.commit().await.expect("commit should persist");

        let loaded = load_selected_tab_at(&live_state, storage.clone(), "version-a")
            .await
            .expect("load should succeed")
            .expect("version row should be visible");
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"version-row\"}")
        );
    }

    #[tokio::test]
    async fn writer_uses_first_parent_as_merge_root_base() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let mut seed_transaction = storage
            .begin_write_transaction()
            .await
            .expect("seed transaction should open");
        let mut writes = StorageWriteSet::new();
        {
            CommitStoreContext::new()
                .writer(&mut seed_transaction.as_mut(), &mut writes)
                .stage_commit_draft(
                    CommitDraftRef {
                        id: "parent-left",
                        change_id: "parent-left:commit",
                        parent_ids: &[],
                        author_account_ids: &[],
                        created_at: "1970-01-01T00:00:00.000Z",
                    },
                    Vec::new(),
                    Vec::new(),
                )
                .await
                .expect("first parent commit should stage");
            TrackedStateContext::new()
                .writer(&mut seed_transaction.as_mut(), &mut writes)
                .stage_delta("parent-left", None, &[])
                .await
                .expect("first parent root should exist");
        }
        writes
            .apply(&mut seed_transaction.as_mut())
            .await
            .expect("first parent root should apply");
        seed_transaction
            .commit()
            .await
            .expect("seed transaction should commit");

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");

        {
            let rows = [
                tracked_row_at_with_commit(
                    "version-a",
                    "version-row",
                    Some("change-version"),
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
                stage_materialized_live_rows(
                    transaction.as_mut(),
                    &mut writes,
                    &mut json_writer,
                    &rows,
                )
                .await
                .expect("merge commit should use first parent as tracked-root base");
            }
            writes
                .apply(&mut transaction.as_mut())
                .await
                .expect("merge commit rows should apply");
        }
    }

    #[tokio::test]
    async fn non_global_root_does_not_store_global_rows() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let tracked_state = TrackedStateContext::new();
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");

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
                stage_materialized_live_rows(
                    transaction.as_mut(),
                    &mut writes,
                    &mut json_writer,
                    &rows,
                )
                .await
                .expect("tracked rows should stage");
            }
            writes
                .apply(&mut transaction.as_mut())
                .await
                .expect("tracked rows should apply");
        }
        transaction.commit().await.expect("commit should persist");

        let global_root_rows =
            scan_tracked_root(&tracked_state, storage.clone(), "commit-global").await;
        assert_eq!(global_root_rows.len(), 1);
        assert_eq!(
            global_root_rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"global-tracked\"}")
        );

        let main_root_rows =
            scan_tracked_root(&tracked_state, storage.clone(), "commit-main").await;
        assert_eq!(main_root_rows.len(), 1);
        assert_eq!(
            main_root_rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"main-tracked\"}")
        );
    }

    async fn load_selected_tab(
        live_state: &LiveStateContext,
        storage: StorageContext,
    ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
        live_state
            .reader(storage)
            .load_row(&LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                version_id: "global".to_string(),
                entity_id: crate::entity_identity::EntityIdentity::single("selected-tab"),
                file_id: NullableKeyFilter::Null,
            })
            .await
    }

    async fn load_selected_tab_at(
        live_state: &LiveStateContext,
        storage: StorageContext,
        version_id: &str,
    ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
        live_state
            .reader(storage)
            .load_row(&LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                version_id: version_id.to_string(),
                entity_id: crate::entity_identity::EntityIdentity::single("selected-tab"),
                file_id: NullableKeyFilter::Null,
            })
            .await
    }

    async fn scan_selected_tab_at(
        live_state: &LiveStateContext,
        storage: StorageContext,
        version_id: &str,
        include_tombstones: bool,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        live_state
            .reader(storage)
            .scan_rows(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec!["lix_key_value".to_string()],
                    entity_ids: vec![crate::entity_identity::EntityIdentity::single(
                        "selected-tab",
                    )],
                    version_ids: vec![version_id.to_string()],
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
        storage: StorageContext,
        commit_id: &str,
    ) -> Vec<MaterializedTrackedStateRow> {
        tracked_state
            .reader(storage)
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
        version_id: &str,
        value: &str,
        change_id: Option<&str>,
        commit_id: &str,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_id: identity("selected-tab"),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}")),
            metadata: None,
            deleted: false,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global: version_id == "global",
            change_id: change_id.map(str::to_string),
            commit_id: Some(commit_id.to_string()),
            untracked: false,
            version_id: version_id.to_string(),
        }
    }

    fn tombstone_tracked_row_at_with_commit(
        version_id: &str,
        change_id: Option<&str>,
        commit_id: &str,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            snapshot_content: None,
            deleted: true,
            ..tracked_row_at_with_commit(version_id, "ignored", change_id, commit_id)
        }
    }

    fn untracked_row(value: &str) -> MaterializedUntrackedStateRow {
        untracked_row_at("global", value)
    }

    fn untracked_row_at(version_id: &str, value: &str) -> MaterializedUntrackedStateRow {
        MaterializedUntrackedStateRow {
            entity_id: identity("selected-tab"),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}")),
            metadata: None,
            deleted: false,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global: version_id == "global",
            version_id: version_id.to_string(),
        }
    }

    fn version_ref_row(version_id: &str, commit_id: &str) -> MaterializedUntrackedStateRow {
        MaterializedUntrackedStateRow {
            entity_id: identity(version_id),
            schema_key: "lix_version_ref".to_string(),
            file_id: None,
            snapshot_content: Some(
                serde_json::to_string(&json!({
                    "id": version_id,
                    "commit_id": commit_id,
                }))
                .expect("version ref should serialize"),
            ),
            metadata: None,
            deleted: false,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global: true,
            version_id: "global".to_string(),
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
            entity_id: identity(commit_id),
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
            version_id: "global".to_string(),
        }
    }

    fn identity(entity_id: &str) -> EntityIdentity {
        EntityIdentity::single(entity_id)
    }
}
