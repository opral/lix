use std::sync::Arc;

use tokio::sync::Mutex;

use crate::entity_pk::EntityPk;
use crate::storage::{StorageRead, StorageWriteSet};
use crate::untracked_state::{
    MaterializedUntrackedStateRow, UntrackedStateContext, UntrackedStateFilter, UntrackedStateRow,
    UntrackedStateRowRequest, UntrackedStateScanRequest,
};
use crate::version::VERSION_REF_SCHEMA_KEY;
use crate::version::{VersionHead, VersionRefReader};
use crate::GLOBAL_VERSION_ID;
use crate::{LixError, NullableKeyFilter};

/// Typed access to moving version heads stored in untracked state.
///
/// Version refs are one of the inputs used by live_state visibility, so this
/// context deliberately bypasses live_state and reads the underlying untracked
/// rows directly. That keeps the dependency acyclic:
/// untracked_state -> version_ref -> live_state.
pub(super) struct VersionRefContext {
    untracked_state: Arc<UntrackedStateContext>,
}

impl VersionRefContext {
    pub(super) fn new(untracked_state: Arc<UntrackedStateContext>) -> Self {
        Self { untracked_state }
    }

    /// Creates a version-ref reader over a caller-provided KV store.
    pub(super) fn reader<S>(&self, store: S) -> VersionRefStoreReader<S>
    where
        S: StorageRead + Send + Sync,
    {
        VersionRefStoreReader {
            untracked_state: Arc::clone(&self.untracked_state),
            store: Mutex::new(store),
        }
    }

    /// Creates a version-ref writer over a transaction-local storage write set.
    pub(super) fn writer<'a>(&self, writes: &'a mut StorageWriteSet) -> VersionRefWriter<'a> {
        VersionRefWriter {
            untracked_state: Arc::clone(&self.untracked_state),
            writes,
        }
    }
}

/// Read side for version heads.
pub(super) struct VersionRefStoreReader<S>
where
    S: StorageRead + Send + Sync,
{
    untracked_state: Arc<UntrackedStateContext>,
    store: Mutex<S>,
}

impl<S> VersionRefStoreReader<S>
where
    S: StorageRead + Send + Sync,
{
    pub(crate) async fn load_head(
        &self,
        version_id: &str,
    ) -> Result<Option<VersionHead>, LixError> {
        let store = self.store.lock().await;
        let Some(row) = self
            .untracked_state
            .reader(&*store)
            .load_row(&UntrackedStateRowRequest {
                schema_key: VERSION_REF_SCHEMA_KEY.to_string(),
                version_id: GLOBAL_VERSION_ID.to_string(),
                entity_pk: EntityPk::single(version_id),
                file_id: NullableKeyFilter::Null,
            })
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
        let store = self.store.lock().await;
        let rows = self
            .untracked_state
            .reader(&*store)
            .scan_rows(&UntrackedStateScanRequest {
                filter: UntrackedStateFilter {
                    schema_keys: vec![VERSION_REF_SCHEMA_KEY.to_string()],
                    version_ids: vec![GLOBAL_VERSION_ID.to_string()],
                    ..UntrackedStateFilter::default()
                },
                ..UntrackedStateScanRequest::default()
            })
            .await?;
        let mut heads = rows
            .iter()
            .map(|row| {
                let version_id = row.entity_pk.as_single_string_owned()?;
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
    S: StorageRead + Send + Sync,
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
pub(super) struct VersionRefWriter<'a> {
    untracked_state: Arc<UntrackedStateContext>,
    writes: &'a mut StorageWriteSet,
}

impl VersionRefWriter<'_> {
    pub(crate) fn stage_rows(&mut self, rows: &[UntrackedStateRow]) -> Result<(), LixError> {
        self.untracked_state
            .writer(self.writes)
            .stage_rows(rows.iter().map(|row| row.as_ref()))
    }
}

fn decode_version_head(
    requested_version_id: &str,
    row: &MaterializedUntrackedStateRow,
) -> Result<Option<VersionHead>, LixError> {
    let Some(snapshot_content) = row.snapshot_content.as_deref() else {
        return Ok(None);
    };
    let snapshot =
        serde_json::from_str::<serde_json::Value>(snapshot_content).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("engine version-ref snapshot parse failed: {error}"),
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::storage::{InMemoryStorageBackend, StorageReadOptions, StorageWriteOptions};
    use crate::storage::{StorageContext, StorageWriteSet};
    use crate::transaction::prepare_version_ref_row;
    use crate::untracked_state::{UntrackedStateContext, UntrackedStateRowRequest};

    use super::*;

    #[tokio::test]
    async fn load_head_returns_none_when_missing() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let version_ref = test_version_ref();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");

        let head = version_ref
            .reader(read)
            .load_head("missing-version")
            .await
            .expect("missing version ref should load cleanly");

        assert_eq!(head, None);
    }

    #[tokio::test]
    async fn advance_head_writes_untracked_global_ref() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let version_ref = VersionRefContext::new(Arc::new(UntrackedStateContext::new()));

        let mut writes = storage.new_write_set();
        stage_version_head(
            &version_ref,
            &mut writes,
            "version-a",
            "commit-a",
            "2026-01-01T00:00:00Z",
        )
        .expect("version head should advance");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("version head should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let head = version_ref
            .reader(read)
            .load_head("version-a")
            .await
            .expect("version head should load")
            .expect("version head should exist");
        assert_eq!(head.version_id, "version-a");
        assert_eq!(head.commit_id, "commit-a");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut reader = UntrackedStateContext::new().reader(read);
        let row = reader
            .load_row(&UntrackedStateRowRequest {
                schema_key: VERSION_REF_SCHEMA_KEY.to_string(),
                version_id: GLOBAL_VERSION_ID.to_string(),
                entity_pk: crate::entity_pk::EntityPk::single("version-a"),
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
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let version_ref = test_version_ref();

        let mut writes = storage.new_write_set();
        stage_version_head(
            &version_ref,
            &mut writes,
            "version-b",
            "commit-b",
            "2026-01-01T00:00:00Z",
        )
        .expect("version-b should advance");
        stage_version_head(
            &version_ref,
            &mut writes,
            "version-a",
            "commit-a",
            "2026-01-01T00:00:00Z",
        )
        .expect("version-a should advance");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("version heads should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let heads = version_ref
            .reader(read)
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

    fn test_version_ref() -> VersionRefContext {
        VersionRefContext::new(Arc::new(UntrackedStateContext::new()))
    }

    fn stage_version_head(
        version_ref: &VersionRefContext,
        writes: &mut StorageWriteSet,
        version_id: &str,
        commit_id: &str,
        timestamp: &str,
    ) -> Result<(), LixError> {
        let canonical_row = prepare_version_ref_row(version_id, commit_id, timestamp)?;
        version_ref.writer(writes).stage_rows(&[canonical_row.row])
    }
}
