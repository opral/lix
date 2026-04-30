use std::sync::Arc;

use serde_json::json;
use tokio::sync::Mutex;

use crate::backend::{KvStore, KvWriter};
use crate::engine2::entity_identity::EntityIdentity;
use crate::engine2::untracked_state::{
    UntrackedStateContext, UntrackedStateFilter, UntrackedStateRow, UntrackedStateRowRequest,
    UntrackedStateScanRequest, UntrackedStateWriter,
};
use crate::engine2::version_ref::{VersionHead, VersionRefReader};
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixError, NullableKeyFilter};

const VERSION_REF_SCHEMA_KEY: &str = "lix_version_ref";
const VERSION_REF_SCHEMA_VERSION: &str = "1";

/// Typed access to moving version heads stored in untracked state.
///
/// Version refs are one of the inputs used by live_state visibility, so this
/// context deliberately bypasses live_state and reads the underlying untracked
/// rows directly. That keeps the dependency acyclic:
/// untracked_state -> version_ref -> live_state.
pub(crate) struct VersionRefContext {
    untracked_state: Arc<UntrackedStateContext>,
}

impl VersionRefContext {
    pub(crate) fn new(untracked_state: Arc<UntrackedStateContext>) -> Self {
        Self { untracked_state }
    }

    /// Creates a version-ref reader over a caller-provided KV store.
    pub(crate) fn reader<S>(&self, store: S) -> VersionRefStoreReader<S>
    where
        S: KvStore,
    {
        VersionRefStoreReader {
            store: Mutex::new(store),
        }
    }

    /// Creates a version-ref writer over a caller-provided KV writer.
    pub(crate) fn writer<S>(&self, store: S) -> VersionRefWriter<S>
    where
        S: KvWriter,
    {
        VersionRefWriter {
            untracked_state_writer: self.untracked_state.writer(store),
        }
    }
}

/// Read side for version heads.
pub(crate) struct VersionRefStoreReader<S>
where
    S: KvStore,
{
    store: Mutex<S>,
}

impl<S> VersionRefStoreReader<S>
where
    S: KvStore,
{
    pub(crate) async fn load_head(
        &self,
        version_id: &str,
    ) -> Result<Option<VersionHead>, LixError> {
        let mut store = self.store.lock().await;
        let Some(row) = crate::engine2::untracked_state::storage::load_row(
            &mut *store,
            &UntrackedStateRowRequest {
                schema_key: VERSION_REF_SCHEMA_KEY.to_string(),
                version_id: GLOBAL_VERSION_ID.to_string(),
                entity_id: EntityIdentity::single(version_id),
                file_id: NullableKeyFilter::Null,
            },
        )
        .await?
        else {
            return Ok(None);
        };

        decode_version_head(version_id, &row)
    }

    pub(crate) async fn load_head_commit_id(
        &self,
        version_id: &str,
    ) -> Result<Option<String>, LixError> {
        Ok(self.load_head(version_id).await?.map(|head| head.commit_id))
    }

    pub(crate) async fn scan_heads(&self) -> Result<Vec<VersionHead>, LixError> {
        let mut store = self.store.lock().await;
        let rows = crate::engine2::untracked_state::storage::scan_rows(
            &mut *store,
            &UntrackedStateScanRequest {
                filter: UntrackedStateFilter {
                    schema_keys: vec![VERSION_REF_SCHEMA_KEY.to_string()],
                    version_ids: vec![GLOBAL_VERSION_ID.to_string()],
                    ..UntrackedStateFilter::default()
                },
                ..UntrackedStateScanRequest::default()
            },
        )
        .await?;
        let mut heads = rows
            .iter()
            .map(|row| {
                let version_id = row.entity_id.as_string()?;
                decode_version_head(&version_id, row)
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        heads.sort_by(|left, right| left.version_id.cmp(&right.version_id));
        Ok(heads)
    }
}

#[async_trait::async_trait]
impl<S> VersionRefReader for VersionRefStoreReader<S>
where
    S: KvStore + Send,
{
    async fn load_head(&self, version_id: &str) -> Result<Option<VersionHead>, LixError> {
        VersionRefStoreReader::load_head(self, version_id).await
    }

    async fn load_head_commit_id(&self, version_id: &str) -> Result<Option<String>, LixError> {
        VersionRefStoreReader::load_head_commit_id(self, version_id).await
    }

    async fn scan_heads(&self) -> Result<Vec<VersionHead>, LixError> {
        VersionRefStoreReader::scan_heads(self).await
    }
}

/// Write side for moving version heads.
pub(crate) struct VersionRefWriter<S>
where
    S: KvWriter,
{
    untracked_state_writer: UntrackedStateWriter<S>,
}

impl<S> VersionRefWriter<S>
where
    S: KvWriter,
{
    /// Advances a version ref to `commit_id`.
    ///
    /// The row is untracked by design: refs are mutable local pointers over the
    /// changelog, not changelog facts themselves.
    pub(crate) async fn advance_head(
        &mut self,
        version_id: &str,
        commit_id: &str,
        timestamp: &str,
    ) -> Result<(), LixError> {
        let row = version_ref_row(version_id, commit_id, timestamp)?;
        self.untracked_state_writer.write_rows(&[row]).await
    }
}

fn decode_version_head(
    requested_version_id: &str,
    row: &UntrackedStateRow,
) -> Result<Option<VersionHead>, LixError> {
    let Some(snapshot_content) = row.snapshot_content.as_deref() else {
        return Ok(None);
    };
    let snapshot =
        serde_json::from_str::<serde_json::Value>(snapshot_content).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("engine2 version-ref snapshot parse failed: {error}"),
            )
        })?;
    let commit_id = snapshot
        .get("commit_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("version ref for version '{requested_version_id}' is missing commit_id"),
            )
        })?;
    Ok(Some(VersionHead {
        version_id: requested_version_id.to_string(),
        commit_id: commit_id.to_string(),
    }))
}

fn version_ref_row(
    version_id: &str,
    commit_id: &str,
    timestamp: &str,
) -> Result<UntrackedStateRow, LixError> {
    let snapshot_content = serde_json::to_string(&json!({
        "id": version_id,
        "commit_id": commit_id,
    }))
    .map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("engine2 version-ref snapshot serialization failed: {error}"),
        )
    })?;

    Ok(UntrackedStateRow {
        entity_id: crate::engine2::entity_identity::EntityIdentity::single(version_id),
        schema_key: VERSION_REF_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot_content: Some(snapshot_content),
        metadata: None,
        schema_version: VERSION_REF_SCHEMA_VERSION.to_string(),
        created_at: timestamp.to_string(),
        updated_at: timestamp.to_string(),
        global: true,
        version_id: GLOBAL_VERSION_ID.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::backend::{testing::UnitTestBackend, LixBackend, TransactionBeginMode};
    use crate::engine2::untracked_state::{UntrackedStateContext, UntrackedStateRowRequest};

    use super::*;

    #[tokio::test]
    async fn load_head_returns_none_when_missing() {
        let backend = UnitTestBackend::new();
        let version_ref = test_version_ref();

        let head = version_ref
            .reader(&backend)
            .load_head("missing-version")
            .await
            .expect("missing version ref should load cleanly");

        assert_eq!(head, None);
    }

    #[tokio::test]
    async fn advance_head_writes_untracked_global_ref() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let version_ref = VersionRefContext::new(Arc::new(UntrackedStateContext::new()));
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");

        version_ref
            .writer(transaction.as_mut())
            .advance_head("version-a", "commit-a", "2026-01-01T00:00:00Z")
            .await
            .expect("version head should advance");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let head = version_ref
            .reader(Arc::clone(&backend))
            .load_head("version-a")
            .await
            .expect("version head should load")
            .expect("version head should exist");
        assert_eq!(head.version_id, "version-a");
        assert_eq!(head.commit_id, "commit-a");

        let mut reader = UntrackedStateContext::new().reader(backend);
        let row = reader
            .load_row(&UntrackedStateRowRequest {
                schema_key: VERSION_REF_SCHEMA_KEY.to_string(),
                version_id: GLOBAL_VERSION_ID.to_string(),
                entity_id: crate::engine2::entity_identity::EntityIdentity::single("version-a"),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("version-ref row should load")
            .expect("version-ref row should exist");
        assert!(row.global);
        assert_eq!(row.created_at, "2026-01-01T00:00:00Z");
        assert_eq!(row.updated_at, "2026-01-01T00:00:00Z");
    }

    #[tokio::test]
    async fn scan_heads_returns_sorted_version_heads() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let version_ref = test_version_ref();
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");

        version_ref
            .writer(transaction.as_mut())
            .advance_head("version-b", "commit-b", "2026-01-01T00:00:00Z")
            .await
            .expect("version-b should advance");
        version_ref
            .writer(transaction.as_mut())
            .advance_head("version-a", "commit-a", "2026-01-01T00:00:00Z")
            .await
            .expect("version-a should advance");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let heads = version_ref
            .reader(backend)
            .scan_heads()
            .await
            .expect("heads should scan");

        assert_eq!(
            heads,
            vec![
                VersionHead {
                    version_id: "version-a".to_string(),
                    commit_id: "commit-a".to_string(),
                },
                VersionHead {
                    version_id: "version-b".to_string(),
                    commit_id: "commit-b".to_string(),
                },
            ]
        );
    }

    #[tokio::test]
    async fn malformed_snapshot_errors_clearly() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let untracked_state = UntrackedStateContext::new();
        let version_ref = VersionRefContext::new(Arc::new(UntrackedStateContext::new()));
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        let mut row = version_ref_row("version-b", "commit-b", "2026-01-01T00:00:00Z")
            .expect("version-ref row should plan");
        row.snapshot_content = Some("{not-json".to_string());
        untracked_state
            .writer(transaction.as_mut())
            .write_rows(&[row])
            .await
            .expect("malformed row should write for test setup");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let error = version_ref
            .reader(backend)
            .load_head("version-b")
            .await
            .expect_err("malformed snapshot should error");

        assert!(
            error
                .description
                .contains("engine2 version-ref snapshot parse failed"),
            "unexpected error: {error:?}"
        );
    }

    fn test_version_ref() -> VersionRefContext {
        VersionRefContext::new(Arc::new(UntrackedStateContext::new()))
    }
}
