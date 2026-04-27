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
        {
            let store: &mut dyn KvStore = &mut *store;
            if let Some(row) = self
                .untracked_state
                .reader(store)
                .load_row(&request.into())
                .await?
                .map(LiveStateRow::from)
            {
                return Ok(Some(row));
            }
        }

        let tracked_request = tracked_row_request_from_live(request);
        let store: &mut dyn KvStore = &mut *store;
        self.tracked_state
            .reader(store)
            .load_row(&tracked_request)
            .await
            .map(|row| row.map(LiveStateRow::from))
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

fn tracked_row_request_from_live(request: &LiveStateRowRequest) -> TrackedStateRowRequest {
    TrackedStateRowRequest {
        schema_key: request.schema_key.clone(),
        version_id: request.version_id.clone(),
        entity_id: request.entity_id.clone(),
        file_id: request.file_id.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::backend::{testing::UnitTestBackend, LixBackend, TransactionBeginMode};
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

    fn tracked_row(value: &str, change_id: Option<&str>) -> LiveStateRow {
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
            global: true,
            change_id: change_id.map(str::to_string),
            commit_id: Some("commit-tracked".to_string()),
            untracked: false,
            version_id: "global".to_string(),
        }
    }

    fn untracked_row(value: &str) -> UntrackedStateRow {
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
            global: true,
            version_id: "global".to_string(),
        }
    }
}
