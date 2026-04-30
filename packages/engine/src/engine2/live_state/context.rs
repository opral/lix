use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::backend::{KvStore, KvWriter};
use crate::engine2::commit_graph::{CommitGraphCommit, CommitGraphContext};
use crate::engine2::live_state::visibility;
use crate::engine2::live_state::{
    LiveStateFilter, LiveStateReader, LiveStateRow, LiveStateRowRequest, LiveStateScanRequest,
};
use crate::engine2::tracked_state::{
    TrackedStateContext, TrackedStateFilter, TrackedStateProjection, TrackedStateRow,
    TrackedStateRowRequest, TrackedStateScanRequest,
};
use crate::engine2::untracked_state::{
    UntrackedStateContext, UntrackedStateIdentity, UntrackedStateRow, UntrackedStateRowRequest,
    UntrackedStateScanRequest,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::LixError;

/// Serving facade for visible live-state readers and writers.
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
        S: KvStore,
    {
        LiveStateStoreReader {
            store: Mutex::new(store),
            tracked_state: self.tracked_state.clone(),
            untracked_state: self.untracked_state,
            commit_graph: self.commit_graph.clone(),
        }
    }

    /// Creates a visible live-state writer over a caller-provided KV writer.
    ///
    /// The writer owns the tracked/untracked routing rule: tracked rows update
    /// the tracked projection and clear matching untracked overlay rows, while
    /// untracked rows update only the local untracked overlay.
    pub(crate) fn writer<S>(&self, store: S) -> LiveStateWriter<S>
    where
        S: KvWriter,
    {
        LiveStateWriter {
            store,
            tracked_state: self.tracked_state.clone(),
            untracked_state: self.untracked_state,
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
    S: KvStore,
{
    pub(crate) async fn scan_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<LiveStateRow>, LixError> {
        let mut store = self.store.lock().await;
        let scope = scan_scope(&mut *store, &self.untracked_state, request).await?;
        let mut tracked_rows = Vec::new();
        for version_id in &scope.storage_version_ids {
            let Some(commit_id) =
                load_version_ref_commit_id(&mut *store, &self.untracked_state, version_id).await?
            else {
                continue;
            };
            let tracked_request = tracked_scan_request_from_live(request);
            let source = tracked_source_from_version_id(version_id);
            let store: &mut dyn KvStore = &mut *store;
            tracked_rows.extend(
                self.tracked_state
                    .reader(store)
                    .scan_rows_at_commit(&commit_id, &tracked_request)
                    .await?
                    .into_iter()
                    .map(|row| project_tracked_row(row, version_id, source)),
            );
        }

        let untracked_rows = {
            let store: &mut dyn KvStore = &mut *store;
            self.untracked_state
                .reader(store)
                .scan_rows(&untracked_scan_request_from_live(
                    request,
                    &scope.storage_version_ids,
                ))
                .await?
        }
        .into_iter()
        .map(LiveStateRow::from)
        .collect::<Vec<_>>();

        let mut commit_rows = if scope.includes_commit_graph_projection {
            let store: &mut dyn KvStore = &mut *store;
            self.commit_graph
                .reader(store)
                .all_commits()
                .await?
                .into_iter()
                .map(live_state_row_from_commit)
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        commit_rows.retain(|row| live_state_row_matches_filter(row, &request.filter));

        let mut rows = crate::engine2::live_state::overlay::overlay_untracked_rows(
            tracked_rows,
            untracked_rows,
        );
        rows.extend(commit_rows);
        rows = visibility::resolve_scan_rows(
            rows,
            &scope.projection_version_ids,
            request.filter.include_tombstones,
        );
        if let Some(limit) = request.limit {
            rows.truncate(limit);
        }
        Ok(rows)
    }

    pub(crate) async fn load_row(
        &self,
        request: &LiveStateRowRequest,
    ) -> Result<Option<LiveStateRow>, LixError> {
        let mut store = self.store.lock().await;
        if !version_ref_exists(&mut *store, &self.untracked_state, &request.version_id).await? {
            return Ok(None);
        }
        if request.schema_key == COMMIT_SCHEMA_KEY {
            let store: &mut dyn KvStore = &mut *store;
            return self.load_commit_row(store, request).await;
        }
        for candidate in load_row_candidates(request) {
            match candidate.source {
                LiveStateLookupSource::Untracked => {
                    let store: &mut dyn KvStore = &mut *store;
                    if let Some(row) = self
                        .untracked_state
                        .reader(store)
                        .load_row(&untracked_row_request_from_live(
                            request,
                            &candidate.version_id,
                        ))
                        .await?
                    {
                        return Ok(Some(visibility::project_loaded_row(
                            LiveStateRow::from(row),
                            &request.version_id,
                            &candidate.version_id,
                        )));
                    }
                }
                LiveStateLookupSource::Tracked => {
                    let Some(commit_id) = load_version_ref_commit_id(
                        &mut *store,
                        &self.untracked_state,
                        &candidate.version_id,
                    )
                    .await?
                    else {
                        continue;
                    };
                    let store: &mut dyn KvStore = &mut *store;
                    if let Some(row) = self
                        .tracked_state
                        .reader(store)
                        .load_row_at_commit(&commit_id, &tracked_row_request_from_live(request))
                        .await?
                    {
                        return Ok(Some(project_tracked_row(
                            row,
                            &request.version_id,
                            tracked_source_from_version_id(&candidate.version_id),
                        )));
                    }
                }
            }
        }
        Ok(None)
    }

    async fn load_commit_row(
        &self,
        store: &mut dyn KvStore,
        request: &LiveStateRowRequest,
    ) -> Result<Option<LiveStateRow>, LixError> {
        if !nullable_filter_matches(&request.file_id, &None) {
            return Ok(None);
        }
        let Some(commit) = self
            .commit_graph
            .reader(store)
            .load_commit(&request.entity_id.as_string()?)
            .await?
        else {
            return Ok(None);
        };
        let row = live_state_row_from_commit(commit);
        Ok(Some(visibility::project_loaded_row(
            row,
            &request.version_id,
            GLOBAL_VERSION_ID,
        )))
    }
}

#[async_trait]
impl<S> LiveStateReader for LiveStateStoreReader<S>
where
    S: KvStore + Sync,
{
    async fn scan_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<LiveStateRow>, LixError> {
        LiveStateStoreReader::scan_rows(self, request).await
    }

    async fn load_row(
        &self,
        request: &LiveStateRowRequest,
    ) -> Result<Option<LiveStateRow>, LixError> {
        LiveStateStoreReader::load_row(self, request).await
    }
}

/// Writer for visible live-state rows over a caller-provided KV writer.
pub(crate) struct LiveStateWriter<S> {
    store: S,
    tracked_state: TrackedStateContext,
    untracked_state: UntrackedStateContext,
}

impl<S> LiveStateWriter<S>
where
    S: KvWriter,
{
    pub(crate) async fn write_rows(&mut self, rows: &[LiveStateRow]) -> Result<(), LixError> {
        let (tracked_rows, untracked_rows): (Vec<_>, Vec<_>) =
            rows.iter().partition(|row| !row.untracked);

        if !untracked_rows.is_empty() {
            let untracked_rows = untracked_rows
                .into_iter()
                .map(UntrackedStateRow::from)
                .collect::<Vec<_>>();
            let store: &mut dyn KvWriter = &mut self.store;
            self.untracked_state
                .writer(store)
                .write_rows(&untracked_rows)
                .await?;
        }

        if tracked_rows.is_empty() {
            return Ok(());
        }

        let identities = tracked_rows
            .iter()
            .map(|row| {
                Ok(UntrackedStateIdentity {
                    version_id: row.version_id.clone(),
                    schema_key: row.schema_key.clone(),
                    entity_id: row.entity_id.clone(),
                    file_id: row.file_id.clone(),
                })
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        {
            let store: &mut dyn KvWriter = &mut self.store;
            self.untracked_state
                .writer(store)
                .delete_rows(&identities)
                .await?;
        }

        for (commit_id, rows) in grouped_live_rows_by_commit(&tracked_rows)? {
            let parent_commit_id = parent_commit_id_for_commit_rows(commit_id, &rows)?;
            validate_root_local_write_batch(commit_id, &rows)?;
            // Commit graph facts live in the changelog/commit_graph projection.
            // They are present in the write batch so the tracked root can inherit
            // parent metadata, but they are not stored as version entities.
            let root_rows = rows
                .iter()
                .filter(|row| row.schema_key != COMMIT_SCHEMA_KEY)
                .map(|row| TrackedStateRow::try_from(*row))
                .collect::<Result<Vec<_>, _>>()?;
            if root_rows.is_empty() {
                continue;
            }
            let store: &mut dyn KvWriter = &mut self.store;
            self.tracked_state
                .writer(store)
                .write_root(commit_id, parent_commit_id.as_deref(), &root_rows)
                .await?;
        }

        Ok(())
    }
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
    let mut filter: crate::engine2::untracked_state::UntrackedStateFilter =
        request.filter.clone().into();
    filter.version_ids = version_ids.to_vec();
    UntrackedStateScanRequest {
        filter,
        projection: Default::default(),
        limit: None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LiveStateScanScope {
    storage_version_ids: Vec<String>,
    projection_version_ids: Vec<String>,
    includes_commit_graph_projection: bool,
}

async fn scan_scope(
    store: &mut dyn KvStore,
    untracked_state: &UntrackedStateContext,
    request: &LiveStateScanRequest,
) -> Result<LiveStateScanScope, LixError> {
    if request.filter.version_ids.is_empty() {
        return Ok(LiveStateScanScope {
            storage_version_ids: all_version_ref_ids(store, untracked_state).await?,
            projection_version_ids: Vec::new(),
            includes_commit_graph_projection: true,
        });
    }

    let mut projection_version_ids = Vec::new();
    for version_id in &request.filter.version_ids {
        if version_ref_exists(store, untracked_state, version_id).await? {
            projection_version_ids.push(version_id.clone());
        }
    }

    let storage_version_ids = visibility::expanded_version_ids(&projection_version_ids);
    Ok(LiveStateScanScope {
        storage_version_ids,
        includes_commit_graph_projection: !projection_version_ids.is_empty(),
        projection_version_ids,
    })
}

async fn all_version_ref_ids(
    store: &mut dyn KvStore,
    untracked_state: &UntrackedStateContext,
) -> Result<Vec<String>, LixError> {
    let rows = untracked_state
        .reader(store)
        .scan_rows(&UntrackedStateScanRequest {
            filter: crate::engine2::untracked_state::UntrackedStateFilter {
                schema_keys: vec![VERSION_REF_SCHEMA_KEY.to_string()],
                version_ids: vec![GLOBAL_VERSION_ID.to_string()],
                ..Default::default()
            },
            ..Default::default()
        })
        .await?;
    rows.into_iter()
        .map(|row| row.entity_id.as_string())
        .collect()
}

async fn load_version_ref_commit_id(
    store: &mut dyn KvStore,
    untracked_state: &UntrackedStateContext,
    version_id: &str,
) -> Result<Option<String>, LixError> {
    let Some(row) = untracked_state
        .reader(store)
        .load_row(&UntrackedStateRowRequest {
            schema_key: VERSION_REF_SCHEMA_KEY.to_string(),
            version_id: GLOBAL_VERSION_ID.to_string(),
            entity_id: crate::engine2::entity_identity::EntityIdentity::single(version_id),
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
    store: &mut dyn KvStore,
    untracked_state: &UntrackedStateContext,
    version_id: &str,
) -> Result<bool, LixError> {
    Ok(
        load_version_ref_commit_id(store, untracked_state, version_id)
            .await?
            .is_some(),
    )
}

const VERSION_REF_SCHEMA_KEY: &str = "lix_version_ref";
const COMMIT_SCHEMA_KEY: &str = "lix_commit";

fn live_state_row_from_commit(commit: CommitGraphCommit) -> LiveStateRow {
    let change = commit.change;
    LiveStateRow {
        entity_id: change.entity_id,
        schema_key: change.schema_key,
        file_id: change.file_id,
        snapshot_content: change.snapshot_content,
        metadata: change.metadata,
        schema_version: change.schema_version,
        created_at: change.created_at.clone(),
        updated_at: change.created_at,
        global: true,
        change_id: Some(change.id),
        commit_id: Some(commit.commit_id),
        untracked: false,
        version_id: GLOBAL_VERSION_ID.to_string(),
    }
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
    row: TrackedStateRow,
    view_version_id: &str,
    source: TrackedRowSource,
) -> LiveStateRow {
    LiveStateRow {
        entity_id: row.entity_id,
        schema_key: row.schema_key,
        file_id: row.file_id,
        snapshot_content: row.snapshot_content,
        metadata: row.metadata,
        schema_version: row.schema_version,
        created_at: row.created_at,
        updated_at: row.updated_at,
        global: source == TrackedRowSource::Global,
        change_id: Some(row.change_id),
        commit_id: Some(row.commit_id),
        untracked: false,
        version_id: view_version_id.to_string(),
    }
}

fn live_state_row_matches_filter(row: &LiveStateRow, filter: &LiveStateFilter) -> bool {
    if !filter.schema_keys.is_empty() && !filter.schema_keys.contains(&row.schema_key) {
        return false;
    }
    if !filter.entity_ids.is_empty() && !filter.entity_ids.contains(&row.entity_id) {
        return false;
    }
    if !filter.file_ids.is_empty()
        && !filter
            .file_ids
            .iter()
            .any(|filter| nullable_filter_matches(filter, &row.file_id))
    {
        return false;
    }
    true
}

fn nullable_filter_matches(
    filter: &crate::NullableKeyFilter<String>,
    value: &Option<String>,
) -> bool {
    match filter {
        crate::NullableKeyFilter::Any => true,
        crate::NullableKeyFilter::Null => value.is_none(),
        crate::NullableKeyFilter::Value(expected) => value.as_ref() == Some(expected),
    }
}

fn grouped_live_rows_by_commit<'a>(
    rows: &[&'a LiveStateRow],
) -> Result<Vec<(&'a str, Vec<&'a LiveStateRow>)>, LixError> {
    let mut grouped = Vec::<(&str, Vec<&LiveStateRow>)>::new();
    for row in rows {
        let commit_id = row.commit_id.as_deref().ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked live-state row is missing commit_id before tracked root write",
            )
        })?;
        if let Some((_, bucket)) = grouped
            .iter_mut()
            .find(|(existing_commit_id, _)| *existing_commit_id == commit_id)
        {
            bucket.push(*row);
        } else {
            grouped.push((commit_id, vec![*row]));
        }
    }
    Ok(grouped)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RootWriteScope {
    version_id: String,
    global: bool,
}

fn validate_root_local_write_batch(
    commit_id: &str,
    rows: &[&LiveStateRow],
) -> Result<(), LixError> {
    let mut root_scope = None::<RootWriteScope>;
    for row in rows
        .iter()
        .copied()
        .filter(|row| row.schema_key != COMMIT_SCHEMA_KEY)
    {
        let scope = RootWriteScope {
            version_id: row.version_id.clone(),
            global: row.global,
        };
        if row.global != (row.version_id == GLOBAL_VERSION_ID) {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "tracked root write for commit '{commit_id}' has invalid storage scope: version_id='{}', global={}",
                    row.version_id, row.global
                ),
            ));
        }
        if let Some(existing) = &root_scope {
            if existing != &scope {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "tracked root write for commit '{commit_id}' mixes multiple storage scopes"
                    ),
                ));
            }
        } else {
            root_scope = Some(scope);
        }
    }
    Ok(())
}

fn parent_commit_id_for_commit_rows(
    commit_id: &str,
    rows: &[&LiveStateRow],
) -> Result<Option<String>, LixError> {
    let Some(row) = rows.iter().find(|row| {
        row.schema_key == COMMIT_SCHEMA_KEY
            && row
                .entity_id
                .as_string()
                .is_ok_and(|entity_id| entity_id == commit_id)
    }) else {
        return Ok(None);
    };
    parent_commit_id_from_commit_row(row)
}

fn parent_commit_id_from_commit_row(row: &&LiveStateRow) -> Result<Option<String>, LixError> {
    let snapshot = serde_json::from_str::<serde_json::Value>(
        row.snapshot_content.as_deref().ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked root commit row is missing snapshot_content",
            )
        })?,
    )
    .map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("tracked root commit snapshot parse failed: {error}"),
        )
    })?;
    let Some(parent_commit_ids_value) = snapshot.get("parent_commit_ids") else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked root commit row is missing parent_commit_ids",
        ));
    };
    let Some(parent_commit_ids_array) = parent_commit_ids_value.as_array() else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked root commit parent_commit_ids must be an array",
        ));
    };
    let parent_commit_ids = parent_commit_ids_array
        .iter()
        .map(|value| {
            value.as_str().map(str::to_string).ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "tracked root commit parent_commit_ids must contain strings",
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Tracked roots inherit from the first parent. Merge commits record
    // additional parents for graph ancestry, but the merge operation has
    // already materialized the source-side rows as target-version writes.
    Ok(parent_commit_ids.into_iter().next())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LiveStateLookupSource {
    Untracked,
    Tracked,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LiveStateLookupCandidate {
    source: LiveStateLookupSource,
    version_id: String,
}

fn load_row_candidates(request: &LiveStateRowRequest) -> Vec<LiveStateLookupCandidate> {
    let mut candidates = vec![
        LiveStateLookupCandidate {
            source: LiveStateLookupSource::Untracked,
            version_id: request.version_id.clone(),
        },
        LiveStateLookupCandidate {
            source: LiveStateLookupSource::Tracked,
            version_id: request.version_id.clone(),
        },
    ];

    if request.version_id != GLOBAL_VERSION_ID {
        candidates.extend([
            LiveStateLookupCandidate {
                source: LiveStateLookupSource::Untracked,
                version_id: GLOBAL_VERSION_ID.to_string(),
            },
            LiveStateLookupCandidate {
                source: LiveStateLookupSource::Tracked,
                version_id: GLOBAL_VERSION_ID.to_string(),
            },
        ]);
    }

    candidates
}

fn untracked_row_request_from_live(
    request: &LiveStateRowRequest,
    version_id: &str,
) -> crate::engine2::untracked_state::UntrackedStateRowRequest {
    crate::engine2::untracked_state::UntrackedStateRowRequest {
        schema_key: request.schema_key.clone(),
        version_id: version_id.to_string(),
        entity_id: request.entity_id.clone(),
        file_id: request.file_id.clone(),
    }
}

fn tracked_row_request_from_live(request: &LiveStateRowRequest) -> TrackedStateRowRequest {
    TrackedStateRowRequest {
        schema_key: request.schema_key.clone(),
        entity_id: request.entity_id.clone(),
        file_id: request.file_id.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::backend::{testing::UnitTestBackend, LixBackend, TransactionBeginMode};
    use crate::engine2::entity_identity::EntityIdentity;
    use crate::engine2::live_state::LiveStateFilter;
    use crate::engine2::tracked_state::TrackedStateScanRequest;
    use crate::engine2::untracked_state::{UntrackedStateContext, UntrackedStateRow};
    use crate::NullableKeyFilter;
    use serde_json::json;

    fn live_state_context() -> LiveStateContext {
        LiveStateContext::new(
            crate::engine2::tracked_state::TrackedStateContext::new(),
            crate::engine2::untracked_state::UntrackedStateContext::new(),
            crate::engine2::commit_graph::CommitGraphContext::new(
                crate::engine2::changelog::ChangelogContext::new(),
            ),
        )
    }

    #[tokio::test]
    async fn live_state_overlays_untracked_rows() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = live_state_context();
        let untracked_state = UntrackedStateContext::new();

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        live_state
            .writer(transaction.as_mut())
            .write_rows(&[tracked_row_with_commit(
                "tracked-value",
                Some("change-tracked"),
                "commit-tracked",
            )])
            .await
            .expect("tracked row should write");
        untracked_state
            .writer(transaction.as_mut())
            .write_rows(&[
                version_ref_row("global", "commit-tracked"),
                untracked_row("untracked-value"),
            ])
            .await
            .expect("untracked row should write");
        transaction.commit().await.expect("commit should persist");

        let rows = scan_selected_tab_at(&live_state, Arc::clone(&backend), "global", false)
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
            .reader(Arc::clone(&backend))
            .load_row(&LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                version_id: "global".to_string(),
                entity_id: crate::engine2::entity_identity::EntityIdentity::single("selected-tab"),
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
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = live_state_context();

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        live_state
            .writer(transaction.as_mut())
            .write_rows(&[tracked_row_with_commit(
                "tracked-value",
                Some("change-tracked"),
                "commit-tracked",
            )])
            .await
            .expect("tracked row should write");
        UntrackedStateContext::new()
            .writer(transaction.as_mut())
            .write_rows(&[version_ref_row("global", "commit-tracked")])
            .await
            .expect("version ref should write");
        transaction.commit().await.expect("commit should persist");

        let loaded = load_selected_tab(&live_state, Arc::clone(&backend))
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
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = live_state_context();
        let untracked_state = UntrackedStateContext::new();

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        live_state
            .writer(transaction.as_mut())
            .write_rows(&[tracked_row_with_commit(
                "tracked-value",
                Some("change-tracked"),
                "commit-tracked",
            )])
            .await
            .expect("tracked row should write");
        let mut untracked_writer = untracked_state.writer(transaction.as_mut());
        untracked_writer
            .write_rows(&[
                version_ref_row("global", "commit-tracked"),
                untracked_row("untracked-value"),
            ])
            .await
            .expect("untracked row should write");
        untracked_writer
            .delete_rows(&[crate::engine2::untracked_state::UntrackedStateIdentity {
                version_id: "global".to_string(),
                schema_key: "lix_key_value".to_string(),
                entity_id: EntityIdentity::single("selected-tab"),
                file_id: None,
            }])
            .await
            .expect("untracked row should delete");
        transaction.commit().await.expect("commit should persist");

        let loaded = load_selected_tab(&live_state, Arc::clone(&backend))
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
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = live_state_context();

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        live_state
            .writer(transaction.as_mut())
            .write_rows(&[tracked_row_with_commit(
                "global-tracked",
                Some("change-global"),
                "commit-global",
            )])
            .await
            .expect("tracked row should write");
        UntrackedStateContext::new()
            .writer(transaction.as_mut())
            .write_rows(&[
                version_ref_row("global", "commit-global"),
                version_ref_row("version-a", "commit-version-a"),
            ])
            .await
            .expect("version refs should write");
        transaction.commit().await.expect("commit should persist");

        let loaded = load_selected_tab_at(&live_state, Arc::clone(&backend), "version-a")
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
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let tracked_state = TrackedStateContext::new();
        let live_state = LiveStateContext::new(
            tracked_state.clone(),
            UntrackedStateContext::new(),
            crate::engine2::commit_graph::CommitGraphContext::new(
                crate::engine2::changelog::ChangelogContext::new(),
            ),
        );

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        live_state
            .writer(transaction.as_mut())
            .write_rows(&[tracked_row_with_commit(
                "global-tracked",
                Some("change-global"),
                "commit-global",
            )])
            .await
            .expect("global tracked row should write");
        UntrackedStateContext::new()
            .writer(transaction.as_mut())
            .write_rows(&[
                version_ref_row("global", "commit-global"),
                version_ref_row("main", "commit-main"),
            ])
            .await
            .expect("global version ref should write");
        transaction.commit().await.expect("commit should persist");

        let loaded = load_selected_tab_at(&live_state, Arc::clone(&backend), "main")
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
            scan_tracked_root(&tracked_state, Arc::clone(&backend), "commit-main").await;
        assert_eq!(
            main_root_rows.len(),
            0,
            "global fallback must come from the global root, not a copied main root row"
        );
    }

    #[tokio::test]
    async fn load_row_prefers_requested_version_over_global() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = live_state_context();

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        live_state
            .writer(transaction.as_mut())
            .write_rows(&[
                tracked_row_with_commit("global-tracked", Some("change-global"), "commit-global"),
                tracked_row_at_with_commit(
                    "version-a",
                    "version-tracked",
                    Some("change-version"),
                    "commit-version",
                ),
            ])
            .await
            .expect("tracked rows should write");
        UntrackedStateContext::new()
            .writer(transaction.as_mut())
            .write_rows(&[
                version_ref_row("global", "commit-global"),
                version_ref_row("version-a", "commit-version"),
            ])
            .await
            .expect("version refs should write");
        transaction.commit().await.expect("commit should persist");

        let loaded = load_selected_tab_at(&live_state, Arc::clone(&backend), "version-a")
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
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = live_state_context();

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        live_state
            .writer(transaction.as_mut())
            .write_rows(&[
                tracked_row_with_commit("global-tracked", Some("change-global"), "commit-global"),
                tracked_row_at_with_commit(
                    "main",
                    "main-tracked",
                    Some("change-main"),
                    "commit-main",
                ),
            ])
            .await
            .expect("tracked rows should write");
        UntrackedStateContext::new()
            .writer(transaction.as_mut())
            .write_rows(&[
                version_ref_row("global", "commit-global"),
                version_ref_row("main", "commit-main"),
            ])
            .await
            .expect("version refs should write");
        transaction.commit().await.expect("commit should persist");

        let loaded = load_selected_tab_at(&live_state, Arc::clone(&backend), "main")
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
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = live_state_context();
        let untracked_state = UntrackedStateContext::new();

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        live_state
            .writer(transaction.as_mut())
            .write_rows(&[
                tracked_row_with_commit("global-tracked", Some("change-global"), "commit-global"),
                tracked_row_at_with_commit(
                    "version-a",
                    "version-tracked",
                    Some("change-version"),
                    "commit-version",
                ),
            ])
            .await
            .expect("tracked rows should write");
        untracked_state
            .writer(transaction.as_mut())
            .write_rows(&[
                version_ref_row("global", "commit-global"),
                version_ref_row("version-a", "commit-version"),
                untracked_row_at("global", "global-untracked"),
                untracked_row_at("version-a", "version-untracked"),
            ])
            .await
            .expect("untracked rows should write");
        transaction.commit().await.expect("commit should persist");

        let loaded = load_selected_tab_at(&live_state, Arc::clone(&backend), "version-a")
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
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = live_state_context();

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        live_state
            .writer(transaction.as_mut())
            .write_rows(&[
                tracked_row_with_commit("global-tracked", Some("change-global"), "commit-global"),
                tracked_row_at_with_commit(
                    "version-a",
                    "version-tracked",
                    Some("change-version"),
                    "commit-version",
                ),
            ])
            .await
            .expect("rows should write");
        UntrackedStateContext::new()
            .writer(transaction.as_mut())
            .write_rows(&[
                version_ref_row("global", "commit-global"),
                version_ref_row("version-a", "commit-version"),
            ])
            .await
            .expect("version refs should write");
        transaction.commit().await.expect("commit should persist");

        let rows = scan_selected_tab_at(&live_state, Arc::clone(&backend), "version-a", false)
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
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = live_state_context();

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        live_state
            .writer(transaction.as_mut())
            .write_rows(&[tracked_row_with_commit(
                "global-tracked",
                Some("change-global"),
                "commit-global",
            )])
            .await
            .expect("rows should write");
        UntrackedStateContext::new()
            .writer(transaction.as_mut())
            .write_rows(&[
                version_ref_row("global", "commit-global"),
                version_ref_row("version-a", "commit-version-a"),
            ])
            .await
            .expect("version refs should write");
        transaction.commit().await.expect("commit should persist");

        let rows = scan_selected_tab_at(&live_state, Arc::clone(&backend), "version-a", false)
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
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = live_state_context();

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        live_state
            .writer(transaction.as_mut())
            .write_rows(&[tracked_row_with_commit(
                "global-tracked",
                Some("change-global"),
                "commit-global",
            )])
            .await
            .expect("tracked row should write");
        UntrackedStateContext::new()
            .writer(transaction.as_mut())
            .write_rows(&[version_ref_row("global", "commit-global")])
            .await
            .expect("global version ref should write");
        transaction.commit().await.expect("commit should persist");

        let rows =
            scan_selected_tab_at(&live_state, Arc::clone(&backend), "missing-version", false)
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
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = live_state_context();

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        live_state
            .writer(transaction.as_mut())
            .write_rows(&[
                tracked_row_with_commit("global-tracked", Some("change-global"), "commit-global"),
                tombstone_tracked_row_at_with_commit(
                    "version-a",
                    Some("change-tombstone"),
                    "commit-version",
                ),
            ])
            .await
            .expect("rows should write");
        UntrackedStateContext::new()
            .writer(transaction.as_mut())
            .write_rows(&[
                version_ref_row("global", "commit-global"),
                version_ref_row("version-a", "commit-version"),
            ])
            .await
            .expect("version refs should write");
        transaction.commit().await.expect("commit should persist");

        let hidden = scan_selected_tab_at(&live_state, Arc::clone(&backend), "version-a", false)
            .await
            .expect("scan should succeed");
        assert_eq!(hidden.len(), 0);

        let with_tombstone =
            scan_selected_tab_at(&live_state, Arc::clone(&backend), "version-a", true)
                .await
                .expect("scan should succeed");
        assert_eq!(with_tombstone.len(), 1);
        assert_eq!(with_tombstone[0].version_id, "version-a");
        assert_eq!(with_tombstone[0].snapshot_content, None);
    }

    #[tokio::test]
    async fn main_tombstone_hides_global_row() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = live_state_context();

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        live_state
            .writer(transaction.as_mut())
            .write_rows(&[
                tracked_row_with_commit("global-tracked", Some("change-global"), "commit-global"),
                tombstone_tracked_row_at_with_commit(
                    "main",
                    Some("change-main-tombstone"),
                    "commit-main",
                ),
            ])
            .await
            .expect("tracked rows should write");
        UntrackedStateContext::new()
            .writer(transaction.as_mut())
            .write_rows(&[
                version_ref_row("global", "commit-global"),
                version_ref_row("main", "commit-main"),
            ])
            .await
            .expect("version refs should write");
        transaction.commit().await.expect("commit should persist");

        let hidden = scan_selected_tab_at(&live_state, Arc::clone(&backend), "main", false)
            .await
            .expect("scan should succeed");
        assert_eq!(hidden.len(), 0);

        let tombstones = scan_selected_tab_at(&live_state, Arc::clone(&backend), "main", true)
            .await
            .expect("scan should succeed");
        assert_eq!(tombstones.len(), 1);
        assert_eq!(tombstones[0].version_id, "main");
        assert!(!tombstones[0].global);
        assert_eq!(tombstones[0].snapshot_content, None);
    }

    #[tokio::test]
    async fn scan_rows_projects_commit_graph_facts_as_global_rows() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = live_state_context();
        append_commit_change(Arc::clone(&backend), "commit-a").await;
        write_version_refs(
            Arc::clone(&backend),
            &[version_ref_row("version-a", "commit-a")],
        )
        .await;

        let rows = live_state
            .reader(Arc::clone(&backend))
            .scan_rows(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec![COMMIT_SCHEMA_KEY.to_string()],
                    version_ids: vec!["version-a".to_string()],
                    ..LiveStateFilter::default()
                },
                ..LiveStateScanRequest::default()
            })
            .await
            .expect("commit rows should scan");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id.as_string().as_deref(), Ok("commit-a"));
        assert_eq!(rows[0].schema_key, COMMIT_SCHEMA_KEY);
        assert_eq!(rows[0].version_id, "version-a");
        assert!(rows[0].global);
        assert!(!rows[0].untracked);
        assert_eq!(rows[0].change_id.as_deref(), Some("change-commit-a"));
        assert_eq!(rows[0].commit_id.as_deref(), Some("commit-a"));
    }

    #[tokio::test]
    async fn load_row_reads_commit_graph_fact() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = live_state_context();
        append_commit_change(Arc::clone(&backend), "commit-a").await;
        write_version_refs(
            Arc::clone(&backend),
            &[version_ref_row("version-a", "commit-a")],
        )
        .await;

        let row = live_state
            .reader(Arc::clone(&backend))
            .load_row(&LiveStateRowRequest {
                schema_key: COMMIT_SCHEMA_KEY.to_string(),
                version_id: "version-a".to_string(),
                entity_id: crate::engine2::entity_identity::EntityIdentity::single("commit-a"),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("commit row should load")
            .expect("commit row should exist");

        assert_eq!(row.entity_id.as_string().as_deref(), Ok("commit-a"));
        assert_eq!(row.version_id, "version-a");
        assert!(row.global);
        assert_eq!(row.change_id.as_deref(), Some("change-commit-a"));
        assert_eq!(row.commit_id.as_deref(), Some("commit-a"));
    }

    #[tokio::test]
    async fn load_commit_row_does_not_project_into_missing_version() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = live_state_context();
        append_commit_change(Arc::clone(&backend), "commit-a").await;

        let row = live_state
            .reader(Arc::clone(&backend))
            .load_row(&LiveStateRowRequest {
                schema_key: COMMIT_SCHEMA_KEY.to_string(),
                version_id: "missing-version".to_string(),
                entity_id: crate::engine2::entity_identity::EntityIdentity::single("commit-a"),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("commit row load should succeed");

        assert_eq!(
            row, None,
            "commit rows must not be projected into a missing version scope"
        );
    }

    #[tokio::test]
    async fn writer_rejects_tracked_root_batches_that_mix_global_and_version_rows() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = live_state_context();
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");

        let error = live_state
            .writer(transaction.as_mut())
            .write_rows(&[
                tracked_row_at_with_commit(
                    "global",
                    "global-row",
                    Some("change-global"),
                    "commit-shared",
                ),
                tracked_row_at_with_commit(
                    "version-a",
                    "version-row",
                    Some("change-version"),
                    "commit-shared",
                ),
            ])
            .await
            .expect_err("one tracked root must not mix global and version rows");

        assert!(
            error.description.contains("mixes multiple storage scopes"),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn writer_rejects_tracked_rows_with_invalid_storage_scope() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = live_state_context();
        let mut invalid_row =
            tracked_row_at_with_commit("version-a", "bad-row", Some("change-bad"), "commit-bad");
        invalid_row.global = true;
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");

        let error = live_state
            .writer(transaction.as_mut())
            .write_rows(&[invalid_row])
            .await
            .expect_err("global rows must be stored in the global root only");

        assert!(
            error.description.contains("invalid storage scope"),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn writer_allows_commit_fact_to_share_the_touched_version_commit_id() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = live_state_context();
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");

        live_state
            .writer(transaction.as_mut())
            .write_rows(&[
                tracked_row_at_with_commit(
                    "version-a",
                    "version-row",
                    Some("change-version"),
                    "commit-version",
                ),
                commit_live_state_row("commit-version"),
            ])
            .await
            .expect("commit facts are changelog projections, not root-local rows");
        UntrackedStateContext::new()
            .writer(transaction.as_mut())
            .write_rows(&[version_ref_row("version-a", "commit-version")])
            .await
            .expect("version ref should write");
        transaction.commit().await.expect("commit should persist");

        let loaded = load_selected_tab_at(&live_state, Arc::clone(&backend), "version-a")
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
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = live_state_context();
        let mut seed_transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("seed transaction should open");
        TrackedStateContext::new()
            .writer(seed_transaction.as_mut())
            .write_root("parent-left", None, &[])
            .await
            .expect("first parent root should exist");
        seed_transaction
            .commit()
            .await
            .expect("seed transaction should commit");

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");

        live_state
            .writer(transaction.as_mut())
            .write_rows(&[
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
            ])
            .await
            .expect("merge commit should use first parent as tracked-root base");
    }

    #[tokio::test]
    async fn writer_rejects_commit_root_with_missing_parent_commit_ids() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = live_state_context();
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");

        let error = live_state
            .writer(transaction.as_mut())
            .write_rows(&[
                tracked_row_at_with_commit(
                    "version-a",
                    "version-row",
                    Some("change-version"),
                    "commit-malformed",
                ),
                commit_live_state_row_with_snapshot(
                    "commit-malformed",
                    json!({
                        "id": "commit-malformed",
                        "change_set_id": "change-set-commit-malformed",
                        "change_ids": ["change-version"],
                    }),
                ),
            ])
            .await
            .expect_err("commit roots must declare parent_commit_ids");

        assert!(
            error.description.contains("missing parent_commit_ids"),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn writer_rejects_commit_root_with_non_array_parent_commit_ids() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = live_state_context();
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");

        let error = live_state
            .writer(transaction.as_mut())
            .write_rows(&[
                tracked_row_at_with_commit(
                    "version-a",
                    "version-row",
                    Some("change-version"),
                    "commit-malformed",
                ),
                commit_live_state_row_with_snapshot(
                    "commit-malformed",
                    json!({
                        "id": "commit-malformed",
                        "change_set_id": "change-set-commit-malformed",
                        "change_ids": ["change-version"],
                        "parent_commit_ids": "parent-1",
                    }),
                ),
            ])
            .await
            .expect_err("commit root parent_commit_ids must be an array");

        assert!(
            error
                .description
                .contains("parent_commit_ids must be an array"),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn non_global_root_does_not_store_global_rows() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let tracked_state = TrackedStateContext::new();
        let live_state = LiveStateContext::new(
            tracked_state.clone(),
            UntrackedStateContext::new(),
            crate::engine2::commit_graph::CommitGraphContext::new(
                crate::engine2::changelog::ChangelogContext::new(),
            ),
        );
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");

        live_state
            .writer(transaction.as_mut())
            .write_rows(&[
                tracked_row_with_commit("global-tracked", Some("change-global"), "commit-global"),
                tracked_row_at_with_commit(
                    "main",
                    "main-tracked",
                    Some("change-main"),
                    "commit-main",
                ),
            ])
            .await
            .expect("tracked rows should write");
        transaction.commit().await.expect("commit should persist");

        let global_root_rows =
            scan_tracked_root(&tracked_state, Arc::clone(&backend), "commit-global").await;
        assert_eq!(global_root_rows.len(), 1);
        assert_eq!(
            global_root_rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"global-tracked\"}")
        );

        let main_root_rows =
            scan_tracked_root(&tracked_state, Arc::clone(&backend), "commit-main").await;
        assert_eq!(main_root_rows.len(), 1);
        assert_eq!(
            main_root_rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"main-tracked\"}")
        );
    }

    async fn load_selected_tab(
        live_state: &LiveStateContext,
        backend: Arc<dyn LixBackend + Send + Sync>,
    ) -> Result<Option<LiveStateRow>, LixError> {
        live_state
            .reader(backend)
            .load_row(&LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                version_id: "global".to_string(),
                entity_id: crate::engine2::entity_identity::EntityIdentity::single("selected-tab"),
                file_id: NullableKeyFilter::Null,
            })
            .await
    }

    async fn load_selected_tab_at(
        live_state: &LiveStateContext,
        backend: Arc<dyn LixBackend + Send + Sync>,
        version_id: &str,
    ) -> Result<Option<LiveStateRow>, LixError> {
        live_state
            .reader(backend)
            .load_row(&LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                version_id: version_id.to_string(),
                entity_id: crate::engine2::entity_identity::EntityIdentity::single("selected-tab"),
                file_id: NullableKeyFilter::Null,
            })
            .await
    }

    async fn scan_selected_tab_at(
        live_state: &LiveStateContext,
        backend: Arc<dyn LixBackend + Send + Sync>,
        version_id: &str,
        include_tombstones: bool,
    ) -> Result<Vec<LiveStateRow>, LixError> {
        live_state
            .reader(backend)
            .scan_rows(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec!["lix_key_value".to_string()],
                    entity_ids: vec![crate::engine2::entity_identity::EntityIdentity::single(
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
        backend: Arc<dyn LixBackend + Send + Sync>,
        commit_id: &str,
    ) -> Vec<TrackedStateRow> {
        tracked_state
            .reader(backend)
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

    fn tracked_row(value: &str, change_id: Option<&str>) -> LiveStateRow {
        tracked_row_with_commit(value, change_id, "commit-tracked")
    }

    fn tracked_row_with_commit(
        value: &str,
        change_id: Option<&str>,
        commit_id: &str,
    ) -> LiveStateRow {
        tracked_row_at_with_commit("global", value, change_id, commit_id)
    }

    fn tracked_row_at(version_id: &str, value: &str, change_id: Option<&str>) -> LiveStateRow {
        tracked_row_at_with_commit(version_id, value, change_id, "commit-tracked")
    }

    fn tracked_row_at_with_commit(
        version_id: &str,
        value: &str,
        change_id: Option<&str>,
        commit_id: &str,
    ) -> LiveStateRow {
        LiveStateRow {
            entity_id: identity("selected-tab"),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}")),
            metadata: None,
            schema_version: "1".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global: version_id == "global",
            change_id: change_id.map(str::to_string),
            commit_id: Some(commit_id.to_string()),
            untracked: false,
            version_id: version_id.to_string(),
        }
    }

    fn tombstone_tracked_row_at(version_id: &str, change_id: Option<&str>) -> LiveStateRow {
        tombstone_tracked_row_at_with_commit(version_id, change_id, "commit-tracked")
    }

    fn tombstone_tracked_row_at_with_commit(
        version_id: &str,
        change_id: Option<&str>,
        commit_id: &str,
    ) -> LiveStateRow {
        LiveStateRow {
            snapshot_content: None,
            ..tracked_row_at_with_commit(version_id, "ignored", change_id, commit_id)
        }
    }

    fn untracked_row(value: &str) -> UntrackedStateRow {
        untracked_row_at("global", value)
    }

    fn untracked_row_at(version_id: &str, value: &str) -> UntrackedStateRow {
        UntrackedStateRow {
            entity_id: identity("selected-tab"),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}")),
            metadata: None,
            schema_version: "1".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global: version_id == "global",
            version_id: version_id.to_string(),
        }
    }

    fn version_ref_row(version_id: &str, commit_id: &str) -> UntrackedStateRow {
        UntrackedStateRow {
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
            schema_version: "1".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global: true,
            version_id: "global".to_string(),
        }
    }

    async fn write_version_refs(
        backend: Arc<dyn LixBackend + Send + Sync>,
        refs: &[UntrackedStateRow],
    ) {
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("version-ref transaction should open");
        UntrackedStateContext::new()
            .writer(transaction.as_mut())
            .write_rows(refs)
            .await
            .expect("version refs should write");
        transaction
            .commit()
            .await
            .expect("version-ref transaction should commit");
    }

    fn commit_live_state_row(commit_id: &str) -> LiveStateRow {
        commit_live_state_row_with_parents(commit_id, &[])
    }

    fn commit_live_state_row_with_parents(
        commit_id: &str,
        parent_commit_ids: &[&str],
    ) -> LiveStateRow {
        commit_live_state_row_with_snapshot(
            commit_id,
            json!({
                "id": commit_id,
                "change_set_id": format!("change-set-{commit_id}"),
                "change_ids": ["change-version"],
                "parent_commit_ids": parent_commit_ids,
            }),
        )
    }

    fn commit_live_state_row_with_snapshot(
        commit_id: &str,
        snapshot: serde_json::Value,
    ) -> LiveStateRow {
        LiveStateRow {
            entity_id: identity(commit_id),
            schema_key: COMMIT_SCHEMA_KEY.to_string(),
            file_id: None,
            snapshot_content: Some(
                serde_json::to_string(&snapshot).expect("commit snapshot should serialize"),
            ),
            metadata: None,
            schema_version: "1".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global: true,
            change_id: Some(format!("change-{commit_id}")),
            commit_id: Some(commit_id.to_string()),
            untracked: false,
            version_id: "global".to_string(),
        }
    }

    async fn append_commit_change(backend: Arc<dyn LixBackend + Send + Sync>, commit_id: &str) {
        let changelog = crate::engine2::changelog::ChangelogContext::new();
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        changelog
            .writer(transaction.as_mut())
            .append_changes(&[crate::engine2::changelog::CanonicalChange {
                id: format!("change-{commit_id}"),
                entity_id: crate::engine2::entity_identity::EntityIdentity::single(commit_id),
                schema_key: COMMIT_SCHEMA_KEY.to_string(),
                schema_version: "1".to_string(),
                file_id: None,
                snapshot_content: Some(
                    serde_json::to_string(&json!({
                        "id": commit_id,
                        "change_set_id": format!("change-set-{commit_id}"),
                        "change_ids": [],
                        "parent_commit_ids": [],
                    }))
                    .expect("commit snapshot should serialize"),
                ),
                metadata: None,
                created_at: "2026-01-01T00:00:00Z".to_string(),
            }])
            .await
            .expect("commit change should append");
        transaction
            .commit()
            .await
            .expect("transaction should commit");
    }

    fn identity(entity_id: &str) -> EntityIdentity {
        EntityIdentity::single(entity_id)
    }
}
