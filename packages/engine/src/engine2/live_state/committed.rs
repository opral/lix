use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::backend::{KvScanRange, KvStore, KvWriter};
use crate::engine2::live_state::LiveStateRow;
use crate::engine2::live_state::{
    LiveStateContext as EngineLiveStateContext, LiveStateRowRequest, LiveStateScanRequest,
};
use crate::engine2::untracked_state::{
    storage as untracked_storage, UntrackedStateIdentity, UntrackedStateRow,
    UntrackedStateScanRequest,
};
use crate::{LixError, NullableKeyFilter};

const LIVE_STATE_ROW_NAMESPACE: &str = "live_state.row";

/// Factory for committed live-state readers and writers.
///
/// The context does not perform reads itself. Callers choose the backing KV
/// store explicitly with `reader(store)`, so all reads go through a
/// caller-provided KV store.
pub(crate) struct CommittedLiveStateContext;

impl CommittedLiveStateContext {
    pub(crate) fn new() -> Self {
        Self
    }

    /// Creates a visible live-state reader over a caller-provided KV store.
    pub(crate) fn reader<S>(&self, store: S) -> CommittedLiveStateReader<S>
    where
        S: KvStore,
    {
        CommittedLiveStateReader {
            store: Mutex::new(store),
        }
    }

    /// Creates a visible live-state writer over a caller-provided KV writer.
    ///
    /// The writer owns the tracked/untracked routing rule: tracked rows update
    /// the tracked projection and clear matching untracked overlay rows, while
    /// untracked rows update only the local untracked overlay.
    pub(crate) fn writer<S>(&self, store: S) -> CommittedLiveStateWriter<S>
    where
        S: KvWriter,
    {
        CommittedLiveStateWriter { store }
    }
}

/// Visible live-state reader backed by a caller-provided KV store.
pub(crate) struct CommittedLiveStateReader<S> {
    store: Mutex<S>,
}

impl<S> CommittedLiveStateReader<S>
where
    S: KvStore,
{
    pub(crate) async fn scan_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<LiveStateRow>, LixError> {
        let mut store = self.store.lock().await;
        let mut tracked_rows = scan_all_state_rows(&mut *store).await?;
        tracked_rows.retain(|row| state_row_matches_engine_scan(row, request));
        if !request.filter.include_tombstones {
            tracked_rows.retain(|row| row.snapshot_content.is_some());
        }

        let untracked_rows = {
            untracked_storage::scan_rows(
                &mut *store,
                &UntrackedStateScanRequest {
                    filter: request.filter.clone().into(),
                    projection: Default::default(),
                    limit: None,
                },
            )
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
            if let Some(row) = untracked_storage::load_row(&mut *store, &request.into())
                .await?
                .map(LiveStateRow::from)
            {
                return Ok(Some(row));
            }
        }

        let Some(identity) = StateRowIdentity::from_exact_parts(
            false,
            request.version_id.clone(),
            request.schema_key.clone(),
            request.entity_id.clone(),
            request.file_id.clone(),
        ) else {
            return Ok(None);
        };
        let Some(bytes) = store
            .kv_get(LIVE_STATE_ROW_NAMESPACE, &encode_state_row_key(&identity))
            .await?
        else {
            return Ok(None);
        };
        let row = decode_state_row(&bytes)?;
        Ok(row.snapshot_content.is_some().then_some(row))
    }
}

#[async_trait]
impl<S> EngineLiveStateContext for CommittedLiveStateReader<S>
where
    S: KvStore + Sync,
{
    async fn scan_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<LiveStateRow>, LixError> {
        CommittedLiveStateReader::scan_rows(self, request).await
    }

    async fn load_row(
        &self,
        request: &LiveStateRowRequest,
    ) -> Result<Option<LiveStateRow>, LixError> {
        CommittedLiveStateReader::load_row(self, request).await
    }
}

/// Writer for committed live-state visibility over a caller-provided KV writer.
pub(crate) struct CommittedLiveStateWriter<S> {
    store: S,
}

impl<S> CommittedLiveStateWriter<S>
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
            untracked_storage::write_rows(&mut self.store, &untracked_rows).await?;
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
            untracked_storage::delete_rows(&mut self.store, &identities).await?;
        }

        for row in tracked_rows {
            put_state_row(&mut self.store, row).await?;
        }

        Ok(())
    }
}

async fn scan_all_state_rows(store: &mut impl KvStore) -> Result<Vec<LiveStateRow>, LixError> {
    store
        .kv_scan(
            LIVE_STATE_ROW_NAMESPACE,
            KvScanRange::prefix(Vec::new()),
            None,
        )
        .await?
        .into_iter()
        .map(|pair| decode_state_row(&pair.value))
        .collect()
}

fn state_row_matches_engine_scan(row: &LiveStateRow, request: &LiveStateScanRequest) -> bool {
    (request.filter.schema_keys.is_empty() || request.filter.schema_keys.contains(&row.schema_key))
        && (request.filter.entity_ids.is_empty()
            || request.filter.entity_ids.contains(&row.entity_id))
        && (request.filter.version_ids.is_empty()
            || request.filter.version_ids.contains(&row.version_id))
        && nullable_matches_filters(&row.file_id, &request.filter.file_ids)
        && nullable_matches_filters(&row.plugin_key, &request.filter.plugin_keys)
}

fn nullable_matches_filters(value: &Option<String>, filters: &[NullableKeyFilter<String>]) -> bool {
    filters.is_empty()
        || filters.iter().any(|filter| match filter {
            NullableKeyFilter::Any => true,
            NullableKeyFilter::Null => value.is_none(),
            NullableKeyFilter::Value(expected) => value.as_ref() == Some(expected),
        })
}

/// Stable row identity for the simple key/value live-state projection.
///
/// This is intentionally the same identity used by transaction staging: one
/// visible row per version/schema/entity/file/untracked tuple.
#[derive(Debug, Clone, PartialEq, Eq)]
struct StateRowIdentity {
    untracked: bool,
    version_id: String,
    schema_key: String,
    entity_id: String,
    file_id: Option<String>,
}

impl StateRowIdentity {
    fn from_row(row: &LiveStateRow) -> Self {
        Self {
            untracked: row.untracked,
            version_id: row.version_id.clone(),
            schema_key: row.schema_key.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        }
    }

    fn from_exact_parts(
        untracked: bool,
        version_id: String,
        schema_key: String,
        entity_id: String,
        file_id: NullableKeyFilter<String>,
    ) -> Option<Self> {
        let file_id = match file_id {
            NullableKeyFilter::Null => None,
            NullableKeyFilter::Value(value) => Some(value),
            NullableKeyFilter::Any => return None,
        };
        Some(Self {
            untracked,
            version_id,
            schema_key,
            entity_id,
            file_id,
        })
    }
}

fn encode_state_row(row: &LiveStateRow) -> Result<Vec<u8>, LixError> {
    serde_json::to_vec(row).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to encode live-state row: {error}"),
        )
    })
}

fn decode_state_row(bytes: &[u8]) -> Result<LiveStateRow, LixError> {
    serde_json::from_slice(bytes).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to decode live-state row: {error}"),
        )
    })
}

fn encode_state_row_key(identity: &StateRowIdentity) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(if identity.untracked { 1 } else { 0 });
    push_component(&mut out, &identity.version_id);
    push_component(&mut out, &identity.schema_key);
    push_component(&mut out, &identity.entity_id);
    match &identity.file_id {
        Some(file_id) => {
            out.push(1);
            push_component(&mut out, file_id);
        }
        None => out.push(0),
    }
    out
}

async fn put_state_row(writer: &mut impl KvWriter, row: &LiveStateRow) -> Result<(), LixError> {
    let identity = StateRowIdentity::from_row(row);
    writer
        .kv_put(
            LIVE_STATE_ROW_NAMESPACE,
            &encode_state_row_key(&identity),
            &encode_state_row(row)?,
        )
        .await
}

fn push_component(out: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::backend::{testing::UnitTestBackend, LixBackend, TransactionBeginMode};
    use crate::engine2::untracked_state::{UntrackedStateContext, UntrackedStateRow};

    #[test]
    fn row_key_distinguishes_null_and_value_file_id() {
        let null_key = encode_state_row_key(&StateRowIdentity {
            untracked: true,
            version_id: "global".to_string(),
            schema_key: "lix_key_value".to_string(),
            entity_id: "key".to_string(),
            file_id: None,
        });
        let value_key = encode_state_row_key(&StateRowIdentity {
            untracked: true,
            version_id: "global".to_string(),
            schema_key: "lix_key_value".to_string(),
            entity_id: "key".to_string(),
            file_id: Some("file".to_string()),
        });

        assert_ne!(null_key, value_key);
    }

    #[tokio::test]
    async fn committed_live_state_overlays_untracked_rows() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = CommittedLiveStateContext::new();
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
        let live_state = CommittedLiveStateContext::new();

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
        let live_state = CommittedLiveStateContext::new();
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
        live_state: &CommittedLiveStateContext,
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
