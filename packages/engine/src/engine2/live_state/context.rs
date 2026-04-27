use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::backend::{KvStore, KvWriter};
use crate::engine2::live_state::LiveStateRow;
use crate::engine2::live_state::{LiveStateReader, LiveStateRowRequest, LiveStateScanRequest};
use crate::engine2::tracked_state::{
    TrackedStateContext, TrackedStateFilter, TrackedStateProjection, TrackedStateRow,
    TrackedStateRowRequest, TrackedStateScanRequest,
};
use crate::engine2::untracked_state::{
    UntrackedStateContext, UntrackedStateIdentity, UntrackedStateRow, UntrackedStateScanRequest,
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
}

impl LiveStateContext {
    pub(crate) fn new() -> Self {
        Self {
            tracked_state: TrackedStateContext::new(),
            untracked_state: UntrackedStateContext::new(),
        }
    }

    /// Creates a visible live-state reader over a caller-provided KV store.
    pub(crate) fn reader<S>(&self, store: S) -> LiveStateContextReader<S>
    where
        S: KvStore,
    {
        LiveStateContextReader {
            store: Mutex::new(store),
            tracked_state: self.tracked_state,
            untracked_state: self.untracked_state,
        }
    }

    /// Creates a visible live-state writer over a caller-provided KV writer.
    ///
    /// The writer owns the tracked/untracked routing rule: tracked rows update
    /// the tracked projection and clear matching untracked overlay rows, while
    /// untracked rows update only the local untracked overlay.
    pub(crate) fn writer<S>(&self, store: S) -> LiveStateContextWriter<S>
    where
        S: KvWriter,
    {
        LiveStateContextWriter {
            store,
            tracked_state: self.tracked_state,
            untracked_state: self.untracked_state,
        }
    }
}

/// Visible live-state reader backed by a caller-provided KV store.
pub(crate) struct LiveStateContextReader<S> {
    store: Mutex<S>,
    tracked_state: TrackedStateContext,
    untracked_state: UntrackedStateContext,
}

impl<S> LiveStateContextReader<S>
where
    S: KvStore,
{
    pub(crate) async fn scan_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<LiveStateRow>, LixError> {
        let mut store = self.store.lock().await;
        let tracked_request = tracked_scan_request_from_live(request);
        let tracked_rows = {
            let store: &mut dyn KvStore = &mut *store;
            self.tracked_state
                .reader(store)
                .scan_rows(&tracked_request)
                .await?
                .into_iter()
                .map(LiveStateRow::from)
                .collect::<Vec<_>>()
        };

        let untracked_rows = {
            let store: &mut dyn KvStore = &mut *store;
            self.untracked_state
                .reader(store)
                .scan_rows(&UntrackedStateScanRequest {
                    filter: request.filter.clone().into(),
                    projection: Default::default(),
                    limit: None,
                })
                .await?
        }
        .into_iter()
        .map(LiveStateRow::from)
        .collect::<Vec<_>>();

        let mut rows = crate::engine2::live_state::overlay::overlay_untracked_rows(
            tracked_rows,
            untracked_rows,
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
                        return Ok(Some(LiveStateRow::from(row)));
                    }
                }
                LiveStateLookupSource::Tracked => {
                    let store: &mut dyn KvStore = &mut *store;
                    if let Some(row) = self
                        .tracked_state
                        .reader(store)
                        .load_row(&tracked_row_request_from_live(
                            request,
                            &candidate.version_id,
                        ))
                        .await?
                    {
                        return Ok(Some(LiveStateRow::from(row)));
                    }
                }
            }
        }
        Ok(None)
    }
}

#[async_trait]
impl<S> LiveStateReader for LiveStateContextReader<S>
where
    S: KvStore + Sync,
{
    async fn scan_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<LiveStateRow>, LixError> {
        LiveStateContextReader::scan_rows(self, request).await
    }

    async fn load_row(
        &self,
        request: &LiveStateRowRequest,
    ) -> Result<Option<LiveStateRow>, LixError> {
        LiveStateContextReader::load_row(self, request).await
    }
}

/// Writer for visible live-state rows over a caller-provided KV writer.
pub(crate) struct LiveStateContextWriter<S> {
    store: S,
    tracked_state: TrackedStateContext,
    untracked_state: UntrackedStateContext,
}

impl<S> LiveStateContextWriter<S>
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
            .map(|row| UntrackedStateIdentity {
                version_id: row.version_id.clone(),
                schema_key: row.schema_key.clone(),
                entity_id: row.entity_id.clone(),
                file_id: row.file_id.clone(),
            })
            .collect::<Vec<_>>();
        {
            let store: &mut dyn KvWriter = &mut self.store;
            self.untracked_state
                .writer(store)
                .delete_rows(&identities)
                .await?;
        }

        let tracked_rows = tracked_rows
            .into_iter()
            .map(TrackedStateRow::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let store: &mut dyn KvWriter = &mut self.store;
        self.tracked_state
            .writer(store)
            .write_rows(&tracked_rows)
            .await?;

        Ok(())
    }
}

fn tracked_scan_request_from_live(request: &LiveStateScanRequest) -> TrackedStateScanRequest {
    TrackedStateScanRequest {
        filter: TrackedStateFilter {
            schema_keys: request.filter.schema_keys.clone(),
            entity_ids: request.filter.entity_ids.clone(),
            version_ids: request.filter.version_ids.clone(),
            file_ids: request.filter.file_ids.clone(),
            plugin_keys: request.filter.plugin_keys.clone(),
            include_tombstones: request.filter.include_tombstones,
        },
        projection: TrackedStateProjection {
            columns: request.projection.columns.clone(),
        },
        limit: request.limit,
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
) -> crate::engine2::untracked_state::UntrackedStateRowRequest {
    crate::engine2::untracked_state::UntrackedStateRowRequest {
        schema_key: request.schema_key.clone(),
        version_id: version_id.to_string(),
        entity_id: request.entity_id.clone(),
        file_id: request.file_id.clone(),
    }
}

fn tracked_row_request_from_live(
    request: &LiveStateRowRequest,
    version_id: &str,
) -> TrackedStateRowRequest {
    TrackedStateRowRequest {
        schema_key: request.schema_key.clone(),
        version_id: version_id.to_string(),
        entity_id: request.entity_id.clone(),
        file_id: request.file_id.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::backend::{testing::UnitTestBackend, LixBackend, TransactionBeginMode};
    use crate::engine2::live_state::LiveStateFilter;
    use crate::engine2::untracked_state::{UntrackedStateContext, UntrackedStateRow};
    use crate::NullableKeyFilter;

    #[tokio::test]
    async fn live_state_overlays_untracked_rows() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = LiveStateContext::new();
        let untracked_state = UntrackedStateContext::new();

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        live_state
            .writer(transaction.as_mut())
            .write_rows(&[tracked_row("tracked-value", Some("change-tracked"))])
            .await
            .expect("tracked row should write");
        untracked_state
            .writer(transaction.as_mut())
            .write_rows(&[untracked_row("untracked-value")])
            .await
            .expect("untracked row should write");
        transaction.commit().await.expect("commit should persist");

        let rows = live_state
            .reader(Arc::clone(&backend))
            .scan_rows(&LiveStateScanRequest::default())
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
                entity_id: "selected-tab".to_string(),
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
        let live_state = LiveStateContext::new();

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        live_state
            .writer(transaction.as_mut())
            .write_rows(&[tracked_row("tracked-value", Some("change-tracked"))])
            .await
            .expect("tracked row should write");
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
        let live_state = LiveStateContext::new();
        let untracked_state = UntrackedStateContext::new();

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        live_state
            .writer(transaction.as_mut())
            .write_rows(&[tracked_row("tracked-value", Some("change-tracked"))])
            .await
            .expect("tracked row should write");
        let mut untracked_writer = untracked_state.writer(transaction.as_mut());
        untracked_writer
            .write_rows(&[untracked_row("untracked-value")])
            .await
            .expect("untracked row should write");
        untracked_writer
            .delete_rows(&[crate::engine2::untracked_state::UntrackedStateIdentity {
                version_id: "global".to_string(),
                schema_key: "lix_key_value".to_string(),
                entity_id: "selected-tab".to_string(),
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
        let live_state = LiveStateContext::new();

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        live_state
            .writer(transaction.as_mut())
            .write_rows(&[tracked_row("global-tracked", Some("change-global"))])
            .await
            .expect("tracked row should write");
        transaction.commit().await.expect("commit should persist");

        let loaded = load_selected_tab_at(&live_state, Arc::clone(&backend), "version-a")
            .await
            .expect("load should succeed")
            .expect("global row should be visible for requested version");

        assert_eq!(loaded.version_id, "global");
        assert!(!loaded.untracked);
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"global-tracked\"}")
        );
    }

    #[tokio::test]
    async fn load_row_prefers_requested_version_over_global() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = LiveStateContext::new();

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        live_state
            .writer(transaction.as_mut())
            .write_rows(&[
                tracked_row("global-tracked", Some("change-global")),
                tracked_row_at("version-a", "version-tracked", Some("change-version")),
            ])
            .await
            .expect("tracked rows should write");
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
    async fn load_row_prefers_requested_untracked_over_requested_tracked_and_global_rows() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = LiveStateContext::new();
        let untracked_state = UntrackedStateContext::new();

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        live_state
            .writer(transaction.as_mut())
            .write_rows(&[
                tracked_row("global-tracked", Some("change-global")),
                tracked_row_at("version-a", "version-tracked", Some("change-version")),
            ])
            .await
            .expect("tracked rows should write");
        untracked_state
            .writer(transaction.as_mut())
            .write_rows(&[
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
        let live_state = LiveStateContext::new();

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        live_state
            .writer(transaction.as_mut())
            .write_rows(&[
                tracked_row("global-tracked", Some("change-global")),
                tracked_row_at("version-a", "version-tracked", Some("change-version")),
            ])
            .await
            .expect("rows should write");
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
    async fn winning_tombstone_hides_row_unless_tombstones_are_included() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = LiveStateContext::new();

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        live_state
            .writer(transaction.as_mut())
            .write_rows(&[
                tracked_row("global-tracked", Some("change-global")),
                tombstone_tracked_row_at("version-a", Some("change-tombstone")),
            ])
            .await
            .expect("rows should write");
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

    async fn load_selected_tab(
        live_state: &LiveStateContext,
        backend: Arc<dyn LixBackend + Send + Sync>,
    ) -> Result<Option<LiveStateRow>, LixError> {
        live_state
            .reader(backend)
            .load_row(&LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                version_id: "global".to_string(),
                entity_id: "selected-tab".to_string(),
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
                entity_id: "selected-tab".to_string(),
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
                    entity_ids: vec!["selected-tab".to_string()],
                    version_ids: vec![version_id.to_string()],
                    file_ids: vec![NullableKeyFilter::Null],
                    include_tombstones,
                    ..LiveStateFilter::default()
                },
                ..LiveStateScanRequest::default()
            })
            .await
    }

    fn tracked_row(value: &str, change_id: Option<&str>) -> LiveStateRow {
        tracked_row_at("global", value, change_id)
    }

    fn tracked_row_at(version_id: &str, value: &str, change_id: Option<&str>) -> LiveStateRow {
        LiveStateRow {
            entity_id: "selected-tab".to_string(),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}")),
            metadata: None,
            schema_version: "1".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global: version_id == "global",
            change_id: change_id.map(str::to_string),
            commit_id: Some("commit-tracked".to_string()),
            untracked: false,
            version_id: version_id.to_string(),
        }
    }

    fn tombstone_tracked_row_at(version_id: &str, change_id: Option<&str>) -> LiveStateRow {
        LiveStateRow {
            snapshot_content: None,
            ..tracked_row_at(version_id, "ignored", change_id)
        }
    }

    fn untracked_row(value: &str) -> UntrackedStateRow {
        untracked_row_at("global", value)
    }

    fn untracked_row_at(version_id: &str, value: &str) -> UntrackedStateRow {
        UntrackedStateRow {
            entity_id: "selected-tab".to_string(),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}")),
            metadata: None,
            schema_version: "1".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global: version_id == "global",
            version_id: version_id.to_string(),
        }
    }
}
