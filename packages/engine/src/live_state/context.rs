use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::commit_graph::{CommitGraphCommit, CommitGraphContext};
use crate::live_state::visibility;
use crate::live_state::{
    LiveStateFilter, LiveStateReader, LiveStateRowRequest, LiveStateScanRequest,
    LiveStateTrackedRowRef, MaterializedLiveStateRow,
};
use crate::storage::{StorageReader, StorageWriteSet};
use crate::tracked_state::{
    MaterializedTrackedStateRow, TrackedStateContext, TrackedStateFilter, TrackedStateProjection,
    TrackedStateRowRequest, TrackedStateScanRequest,
};
use crate::untracked_state::{
    UntrackedStateContext, UntrackedStateIdentityRef, UntrackedStateRowRef,
    UntrackedStateRowRequest, UntrackedStateScanRequest,
};
use crate::version::VERSION_REF_SCHEMA_KEY;
use crate::LixError;
use crate::GLOBAL_VERSION_ID;

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
        S: StorageReader,
    {
        LiveStateStoreReader {
            store: Mutex::new(store),
            tracked_state: self.tracked_state.clone(),
            untracked_state: self.untracked_state,
            commit_graph: self.commit_graph.clone(),
        }
    }

    /// Creates a visible live-state writer over a caller-provided KV reader.
    ///
    /// The writer owns the tracked/untracked routing rule: tracked rows update
    /// the tracked projection and clear matching untracked overlay rows, while
    /// untracked rows update only the local untracked overlay.
    pub(crate) fn writer<S>(&self, store: S) -> LiveStateWriter<S>
    where
        S: StorageReader,
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
    S: StorageReader,
{
    pub(crate) async fn scan_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        let mut store = self.store.lock().await;
        let scope = scan_scope(&mut *store, &self.untracked_state, request).await?;
        let mut tracked_rows = Vec::new();
        if request.filter.untracked != Some(true) {
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

        let mut commit_rows = if scope.includes_commit_graph_projection {
            let store: &mut dyn StorageReader = &mut *store;
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

        let mut rows = if request.filter.untracked.is_some() {
            tracked_rows.into_iter().chain(untracked_rows).collect()
        } else {
            crate::live_state::overlay::overlay_untracked_rows(tracked_rows, untracked_rows)
        };
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
    ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
        let mut store = self.store.lock().await;
        if !version_ref_exists(&mut *store, &self.untracked_state, &request.version_id).await? {
            return Ok(None);
        }
        if request.schema_key == COMMIT_SCHEMA_KEY {
            let store: &mut dyn StorageReader = &mut *store;
            return self.load_commit_row(store, request).await;
        }
        for candidate in load_row_candidates(request) {
            match candidate.source {
                LiveStateLookupSource::Untracked => {
                    let store: &mut dyn StorageReader = &mut *store;
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
                            MaterializedLiveStateRow::from(row),
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
                    let store: &mut dyn StorageReader = &mut *store;
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
        store: &mut dyn StorageReader,
        request: &LiveStateRowRequest,
    ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
        if !nullable_filter_matches(&request.file_id, &None) {
            return Ok(None);
        }
        let Some(commit) = self
            .commit_graph
            .reader(store)
            .load_commit(&request.entity_id.as_single_string_owned()?)
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

/// Writer for visible live-state rows over a caller-provided KV reader.
pub(crate) struct LiveStateWriter<S> {
    store: S,
    tracked_state: TrackedStateContext,
    untracked_state: UntrackedStateContext,
}

impl<S> LiveStateWriter<S>
where
    S: StorageReader,
{
    pub(crate) fn stage_untracked_rows<'a>(
        &mut self,
        writes: &mut StorageWriteSet,
        rows: impl IntoIterator<Item = UntrackedStateRowRef<'a>>,
    ) -> Result<(), LixError> {
        self.untracked_state.writer(writes).stage_rows(rows)
    }

    pub(crate) fn stage_delete_untracked_rows<'a>(
        &mut self,
        writes: &mut StorageWriteSet,
        identities: impl IntoIterator<Item = UntrackedStateIdentityRef<'a>>,
    ) {
        self.untracked_state
            .writer(writes)
            .stage_delete_rows(identities);
    }

    pub(crate) async fn stage_tracked_root<'a>(
        &mut self,
        writes: &mut StorageWriteSet,
        storage_version_id: &str,
        commit_id: &str,
        parent_commit_id: Option<&str>,
        rows: impl IntoIterator<Item = LiveStateTrackedRowRef<'a>>,
    ) -> Result<(), LixError> {
        let rows = rows.into_iter().collect::<Vec<_>>();
        validate_root_ref_write_batch(storage_version_id, commit_id, &rows)?;
        self.tracked_state
            .writer()
            .stage_root(
                &mut self.store,
                writes,
                commit_id,
                parent_commit_id,
                rows.iter().map(|row| row.row),
            )
            .await?;
        Ok(())
    }
}

fn validate_root_ref_write_batch(
    storage_version_id: &str,
    commit_id: &str,
    rows: &[LiveStateTrackedRowRef<'_>],
) -> Result<(), LixError> {
    for row in rows {
        require_valid_storage_scope(row.version_id, row.global)?;
        let row_storage_version_id = row.storage_version_id();
        if row_storage_version_id != storage_version_id {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "tracked live-state root '{}' mixes multiple storage scopes: root storage scope is '{}' but row schema '{}' belongs to '{}'",
                    commit_id, storage_version_id, row.row.key.schema_key, row_storage_version_id
                ),
            ));
        }
        if row.row.value.commit_id != commit_id {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "tracked live-state row for schema '{}' has commit_id '{}' but root commit_id is '{}'",
                    row.row.key.schema_key, row.row.value.commit_id, commit_id
                ),
            ));
        }
        if row.row.key.schema_key == COMMIT_SCHEMA_KEY {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked_state roots must not store lix_commit rows",
            ));
        }
    }
    Ok(())
}

fn require_valid_storage_scope(version_id: &str, global: bool) -> Result<(), LixError> {
    if global != (version_id == GLOBAL_VERSION_ID) {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("invalid storage scope: version_id='{version_id}', global={global}"),
        ));
    }
    Ok(())
}

fn tracked_scan_request_from_live(request: &LiveStateScanRequest) -> TrackedStateScanRequest {
    let mut columns = request.projection.columns.clone();
    if !columns.is_empty() && !columns.iter().any(|column| column == "snapshot_content") {
        columns.push("snapshot_content".to_string());
    }

    TrackedStateScanRequest {
        filter: TrackedStateFilter {
            schema_keys: request.filter.schema_keys.clone(),
            entity_ids: request.filter.entity_ids.clone(),
            file_ids: request.filter.file_ids.clone(),
            // Scan tombstones internally so version-local tombstones can hide
            // global fallback rows before the serving facade filters them.
            include_tombstones: true,
        },
        projection: TrackedStateProjection { columns },
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
    store: &mut dyn StorageReader,
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

const COMMIT_SCHEMA_KEY: &str = "lix_commit";

fn live_state_row_from_commit(commit: CommitGraphCommit) -> MaterializedLiveStateRow {
    let change = commit.change;
    MaterializedLiveStateRow {
        entity_id: change.entity_id,
        schema_key: change.schema_key,
        file_id: change.file_id,
        snapshot_content: change.snapshot_content,
        metadata: change.metadata,
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
        created_at: row.created_at,
        updated_at: row.updated_at,
        global: source == TrackedRowSource::Global,
        change_id: Some(row.change_id),
        commit_id: Some(row.commit_id),
        untracked: false,
        version_id: view_version_id.to_string(),
    }
}

fn live_state_row_matches_filter(row: &MaterializedLiveStateRow, filter: &LiveStateFilter) -> bool {
    if filter
        .untracked
        .is_some_and(|untracked| row.untracked != untracked)
    {
        return false;
    }
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
) -> crate::untracked_state::UntrackedStateRowRequest {
    crate::untracked_state::UntrackedStateRowRequest {
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
    use crate::backend::{testing::UnitTestBackend, Backend};
    use crate::changelog::MaterializedCanonicalChange;
    use crate::entity_identity::EntityIdentity;
    use crate::json_store::JsonStoreContext;
    use crate::live_state::LiveStateFilter;
    use crate::storage::{StorageContext, StorageWriteTransaction};
    use crate::tracked_state::TrackedStateScanRequest;
    use crate::untracked_state::{MaterializedUntrackedStateRow, UntrackedStateContext};
    use crate::NullableKeyFilter;
    use serde_json::json;

    fn live_state_context() -> LiveStateContext {
        LiveStateContext::new(
            crate::tracked_state::TrackedStateContext::new(),
            crate::untracked_state::UntrackedStateContext::new(),
            crate::commit_graph::CommitGraphContext::new(crate::changelog::ChangelogContext::new()),
        )
    }

    async fn write_untracked_rows_to_store(
        store: &mut (impl StorageWriteTransaction + ?Sized),
        rows: &[MaterializedUntrackedStateRow],
    ) {
        let mut writes = StorageWriteSet::new();
        let canonical_rows = {
            let mut json_writer = JsonStoreContext::new().writer();
            rows.iter()
                .map(|row| {
                    crate::test_support::untracked_state_row_from_materialized(
                        &mut writes,
                        &mut json_writer,
                        row,
                    )
                })
                .collect::<Result<Vec<_>, _>>()
                .expect("untracked rows should canonicalize")
        };
        UntrackedStateContext::new()
            .writer(&mut writes)
            .stage_rows(canonical_rows.iter().map(|row| row.as_ref()))
            .expect("untracked rows should write");
        writes
            .apply(store)
            .await
            .expect("untracked rows should apply");
    }

    async fn stage_materialized_live_rows<S>(
        writer: &mut LiveStateWriter<S>,
        writes: &mut StorageWriteSet,
        json_writer: &mut crate::json_store::JsonStoreWriter,
        rows: &[MaterializedLiveStateRow],
    ) -> Result<(), LixError>
    where
        S: StorageReader,
    {
        let mut untracked_rows = Vec::new();
        let mut tracked_rows_by_commit =
            std::collections::BTreeMap::<String, Vec<TestTrackedLiveRow>>::new();
        let mut parent_by_commit = std::collections::BTreeMap::<String, Option<String>>::new();

        for row in rows {
            if row.untracked {
                let materialized = crate::untracked_state::MaterializedUntrackedStateRow::from(row);
                let canonical = crate::test_support::untracked_state_row_from_materialized(
                    writes,
                    json_writer,
                    &materialized,
                )?;
                untracked_rows.push(canonical);
                continue;
            }
            let materialized = MaterializedTrackedStateRow::try_from(row)?;
            let canonical = crate::test_support::tracked_state_row_from_materialized(
                writes,
                json_writer,
                &materialized,
            )?;
            let commit_id = row.commit_id.clone().ok_or_else(|| {
                LixError::new("LIX_ERROR_UNKNOWN", "test tracked row missing commit_id")
            })?;
            if row.schema_key == COMMIT_SCHEMA_KEY {
                parent_by_commit.insert(
                    commit_id.clone(),
                    parent_commit_id_from_test_commit_row(row)?,
                );
            }
            tracked_rows_by_commit
                .entry(commit_id)
                .or_default()
                .push(TestTrackedLiveRow {
                    row: canonical,
                    global: row.global,
                    version_id: row.version_id.clone(),
                });
        }

        writer.stage_untracked_rows(writes, untracked_rows.iter().map(|row| row.as_ref()))?;
        for (commit_id, rows) in tracked_rows_by_commit {
            let parent_commit_id = parent_by_commit.remove(&commit_id).flatten();
            let storage_version_id = rows
                .first()
                .map(TestTrackedLiveRow::storage_version_id)
                .unwrap_or(GLOBAL_VERSION_ID);
            writer
                .stage_tracked_root(
                    writes,
                    storage_version_id,
                    &commit_id,
                    parent_commit_id.as_deref(),
                    rows.iter()
                        .filter(|row| row.row.schema_key != COMMIT_SCHEMA_KEY)
                        .map(TestTrackedLiveRow::as_ref),
                )
                .await?;
        }
        Ok(())
    }

    struct TestTrackedLiveRow {
        row: crate::tracked_state::TrackedStateRow,
        global: bool,
        version_id: String,
    }

    impl TestTrackedLiveRow {
        fn as_ref(&self) -> LiveStateTrackedRowRef<'_> {
            LiveStateTrackedRowRef {
                row: self.row.as_ref(),
                global: self.global,
                version_id: &self.version_id,
            }
        }

        fn storage_version_id(&self) -> &str {
            if self.global {
                GLOBAL_VERSION_ID
            } else {
                &self.version_id
            }
        }
    }

    fn parent_commit_id_from_test_commit_row(
        row: &MaterializedLiveStateRow,
    ) -> Result<Option<String>, LixError> {
        let Some(snapshot_content) = row.snapshot_content.as_deref() else {
            return Ok(None);
        };
        let snapshot =
            serde_json::from_str::<serde_json::Value>(snapshot_content).map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("test commit row has invalid snapshot: {error}"),
                )
            })?;
        Ok(snapshot
            .get("parent_commit_ids")
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
                let mut writer = live_state.writer(transaction.as_mut());
                stage_materialized_live_rows(
                    &mut writer,
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
                let mut writer = live_state.writer(transaction.as_mut());
                stage_materialized_live_rows(
                    &mut writer,
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
                let mut writer = live_state.writer(transaction.as_mut());
                stage_materialized_live_rows(
                    &mut writer,
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
                let mut writer = live_state.writer(transaction.as_mut());
                stage_materialized_live_rows(&mut writer, &mut writes, &mut json_writer, &rows)
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
            crate::commit_graph::CommitGraphContext::new(crate::changelog::ChangelogContext::new()),
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
                let mut writer = live_state.writer(transaction.as_mut());
                stage_materialized_live_rows(&mut writer, &mut writes, &mut json_writer, &rows)
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
                let mut writer = live_state.writer(transaction.as_mut());
                stage_materialized_live_rows(&mut writer, &mut writes, &mut json_writer, &rows)
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
                let mut writer = live_state.writer(transaction.as_mut());
                stage_materialized_live_rows(&mut writer, &mut writes, &mut json_writer, &rows)
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
                let mut writer = live_state.writer(transaction.as_mut());
                stage_materialized_live_rows(&mut writer, &mut writes, &mut json_writer, &rows)
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
                let mut writer = live_state.writer(transaction.as_mut());
                stage_materialized_live_rows(&mut writer, &mut writes, &mut json_writer, &rows)
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
                let mut writer = live_state.writer(transaction.as_mut());
                stage_materialized_live_rows(&mut writer, &mut writes, &mut json_writer, &rows)
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
                let mut writer = live_state.writer(transaction.as_mut());
                stage_materialized_live_rows(&mut writer, &mut writes, &mut json_writer, &rows)
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
                let mut writer = live_state.writer(transaction.as_mut());
                stage_materialized_live_rows(&mut writer, &mut writes, &mut json_writer, &rows)
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
                let mut writer = live_state.writer(transaction.as_mut());
                stage_materialized_live_rows(&mut writer, &mut writes, &mut json_writer, &rows)
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
    async fn scan_rows_projects_commit_graph_facts_as_global_rows() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let live_state = live_state_context();
        append_commit_change(storage.clone(), "commit-a").await;
        write_version_refs(storage.clone(), &[version_ref_row("version-a", "commit-a")]).await;

        let rows = live_state
            .reader(storage.clone())
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
        assert_eq!(
            rows[0].entity_id.as_single_string_owned().as_deref(),
            Ok("commit-a")
        );
        assert_eq!(rows[0].schema_key, COMMIT_SCHEMA_KEY);
        assert_eq!(rows[0].version_id, "version-a");
        assert!(rows[0].global);
        assert!(!rows[0].untracked);
        assert_eq!(rows[0].change_id.as_deref(), Some("change-commit-a"));
        assert_eq!(rows[0].commit_id.as_deref(), Some("commit-a"));
    }

    #[tokio::test]
    async fn load_row_reads_commit_graph_fact() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let live_state = live_state_context();
        append_commit_change(storage.clone(), "commit-a").await;
        write_version_refs(storage.clone(), &[version_ref_row("version-a", "commit-a")]).await;

        let row = live_state
            .reader(storage.clone())
            .load_row(&LiveStateRowRequest {
                schema_key: COMMIT_SCHEMA_KEY.to_string(),
                version_id: "version-a".to_string(),
                entity_id: crate::entity_identity::EntityIdentity::single("commit-a"),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("commit row should load")
            .expect("commit row should exist");

        assert_eq!(
            row.entity_id.as_single_string_owned().as_deref(),
            Ok("commit-a")
        );
        assert_eq!(row.version_id, "version-a");
        assert!(row.global);
        assert_eq!(row.change_id.as_deref(), Some("change-commit-a"));
        assert_eq!(row.commit_id.as_deref(), Some("commit-a"));
    }

    #[tokio::test]
    async fn load_commit_row_does_not_project_into_missing_version() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let live_state = live_state_context();
        append_commit_change(storage.clone(), "commit-a").await;

        let row = live_state
            .reader(storage.clone())
            .load_row(&LiveStateRowRequest {
                schema_key: COMMIT_SCHEMA_KEY.to_string(),
                version_id: "missing-version".to_string(),
                entity_id: crate::entity_identity::EntityIdentity::single("commit-a"),
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
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let live_state = live_state_context();
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");

        let error = {
            let rows = [
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
            ];
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            let mut writer = live_state.writer(transaction.as_mut());
            stage_materialized_live_rows(&mut writer, &mut writes, &mut json_writer, &rows).await
        }
        .expect_err("one tracked root must not mix global and version rows");

        assert!(
            error.message.contains("mixes multiple storage scopes"),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn writer_rejects_tracked_rows_with_invalid_storage_scope() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let live_state = live_state_context();
        let mut invalid_row =
            tracked_row_at_with_commit("version-a", "bad-row", Some("change-bad"), "commit-bad");
        invalid_row.global = true;
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");

        let error = {
            let rows = [invalid_row];
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            let mut writer = live_state.writer(transaction.as_mut());
            stage_materialized_live_rows(&mut writer, &mut writes, &mut json_writer, &rows).await
        }
        .expect_err("global rows must be stored in the global root only");

        assert!(
            error.message.contains("invalid storage scope"),
            "unexpected error: {error:?}"
        );
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
                let mut writer = live_state.writer(transaction.as_mut());
                stage_materialized_live_rows(&mut writer, &mut writes, &mut json_writer, &rows)
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
        let live_state = live_state_context();
        let mut seed_transaction = storage
            .begin_write_transaction()
            .await
            .expect("seed transaction should open");
        let mut writes = StorageWriteSet::new();
        {
            TrackedStateContext::new()
                .writer()
                .stage_root(
                    &mut seed_transaction.as_mut(),
                    &mut writes,
                    "parent-left",
                    None,
                    std::iter::empty(),
                )
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
                let mut writer = live_state.writer(transaction.as_mut());
                stage_materialized_live_rows(&mut writer, &mut writes, &mut json_writer, &rows)
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
        let live_state = LiveStateContext::new(
            tracked_state.clone(),
            UntrackedStateContext::new(),
            crate::commit_graph::CommitGraphContext::new(crate::changelog::ChangelogContext::new()),
        );
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
                let mut writer = live_state.writer(transaction.as_mut());
                stage_materialized_live_rows(&mut writer, &mut writes, &mut json_writer, &rows)
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
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global: true,
            version_id: "global".to_string(),
        }
    }

    async fn write_version_refs(storage: StorageContext, refs: &[MaterializedUntrackedStateRow]) {
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("version-ref transaction should open");
        let mut writes = StorageWriteSet::new();
        let canonical_refs = {
            let mut json_writer = JsonStoreContext::new().writer();
            refs.iter()
                .map(|row| {
                    crate::test_support::untracked_state_row_from_materialized(
                        &mut writes,
                        &mut json_writer,
                        row,
                    )
                })
                .collect::<Result<Vec<_>, _>>()
                .expect("version refs should canonicalize")
        };
        UntrackedStateContext::new()
            .writer(&mut writes)
            .stage_rows(canonical_refs.iter().map(|row| row.as_ref()))
            .expect("version refs should write");
        writes
            .apply(&mut transaction.as_mut())
            .await
            .expect("version refs should apply");
        transaction
            .commit()
            .await
            .expect("version-ref transaction should commit");
    }

    fn commit_live_state_row(commit_id: &str) -> MaterializedLiveStateRow {
        commit_live_state_row_with_parents(commit_id, &[])
    }

    fn commit_live_state_row_with_parents(
        commit_id: &str,
        parent_commit_ids: &[&str],
    ) -> MaterializedLiveStateRow {
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
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_id: identity(commit_id),
            schema_key: COMMIT_SCHEMA_KEY.to_string(),
            file_id: None,
            snapshot_content: Some(
                serde_json::to_string(&snapshot).expect("commit snapshot should serialize"),
            ),
            metadata: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global: true,
            change_id: Some(format!("change-{commit_id}")),
            commit_id: Some(commit_id.to_string()),
            untracked: false,
            version_id: "global".to_string(),
        }
    }

    async fn append_commit_change(storage: StorageContext, commit_id: &str) {
        let changelog = crate::changelog::ChangelogContext::new();
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let change = MaterializedCanonicalChange {
            id: format!("change-{commit_id}"),
            entity_id: crate::entity_identity::EntityIdentity::single(commit_id),
            schema_key: COMMIT_SCHEMA_KEY.to_string(),
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
        };
        let mut writes = StorageWriteSet::new();
        let canonical_change = {
            let mut json_writer = JsonStoreContext::new().writer();
            crate::test_support::canonical_change_from_materialized(
                &mut writes,
                &mut json_writer,
                &change,
            )
            .expect("commit change should canonicalize")
        };
        changelog
            .writer(&mut writes)
            .stage_changes(std::iter::once(canonical_change.as_ref()))
            .expect("commit change should append");
        writes
            .apply(&mut transaction.as_mut())
            .await
            .expect("commit change should apply");
        transaction
            .commit()
            .await
            .expect("transaction should commit");
    }

    fn identity(entity_id: &str) -> EntityIdentity {
        EntityIdentity::single(entity_id)
    }
}
