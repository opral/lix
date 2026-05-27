use std::sync::Arc;

use tokio::sync::Mutex;

use crate::branch::BRANCH_REF_SCHEMA_KEY;
use crate::branch::{BranchHead, BranchRefReader};
use crate::entity_pk::EntityPk;
use crate::storage::{StorageRead, StorageWriteSet};
use crate::untracked_state::{
    MaterializedUntrackedStateRow, UntrackedStateContext, UntrackedStateFilter, UntrackedStateRow,
    UntrackedStateRowRequest, UntrackedStateScanRequest,
};
use crate::GLOBAL_BRANCH_ID;
use crate::{LixError, NullableKeyFilter};

/// Typed access to moving branch heads stored in untracked state.
///
/// Branch refs are one of the inputs used by live_state visibility, so this
/// context deliberately bypasses live_state and reads the underlying untracked
/// rows directly. That keeps the dependency acyclic:
/// untracked_state -> branch_ref -> live_state.
pub(super) struct BranchRefContext {
    untracked_state: Arc<UntrackedStateContext>,
}

impl BranchRefContext {
    pub(super) fn new(untracked_state: Arc<UntrackedStateContext>) -> Self {
        Self { untracked_state }
    }

    /// Creates a branch-ref reader over a caller-provided KV store.
    pub(super) fn reader<S>(&self, store: S) -> BranchRefStoreReader<S>
    where
        S: StorageRead + Send + Sync,
    {
        BranchRefStoreReader {
            untracked_state: Arc::clone(&self.untracked_state),
            store: Mutex::new(store),
        }
    }

    /// Creates a branch-ref writer over a transaction-local storage write set.
    pub(super) fn writer<'a>(&self, writes: &'a mut StorageWriteSet) -> BranchRefWriter<'a> {
        BranchRefWriter {
            untracked_state: Arc::clone(&self.untracked_state),
            writes,
        }
    }
}

/// Read side for branch heads.
pub(super) struct BranchRefStoreReader<S>
where
    S: StorageRead + Send + Sync,
{
    untracked_state: Arc<UntrackedStateContext>,
    store: Mutex<S>,
}

impl<S> BranchRefStoreReader<S>
where
    S: StorageRead + Send + Sync,
{
    pub(crate) async fn load_head(&self, branch_id: &str) -> Result<Option<BranchHead>, LixError> {
        let store = self.store.lock().await;
        let Some(row) = self
            .untracked_state
            .reader(&*store)
            .load_row(&UntrackedStateRowRequest {
                schema_key: BRANCH_REF_SCHEMA_KEY.to_string(),
                branch_id: GLOBAL_BRANCH_ID.to_string(),
                entity_pk: EntityPk::single(branch_id),
                file_id: NullableKeyFilter::Null,
            })
            .await?
        else {
            return Ok(None);
        };

        decode_branch_head(branch_id, &row)
    }

    pub(crate) async fn load_head_commit_id(
        &self,
        branch_id: &str,
    ) -> Result<Option<String>, LixError> {
        Ok(self.load_head(branch_id).await?.map(|head| head.commit_id))
    }

    pub(crate) async fn scan_heads(&self) -> Result<Vec<BranchHead>, LixError> {
        let store = self.store.lock().await;
        let rows = self
            .untracked_state
            .reader(&*store)
            .scan_rows(&UntrackedStateScanRequest {
                filter: UntrackedStateFilter {
                    schema_keys: vec![BRANCH_REF_SCHEMA_KEY.to_string()],
                    branch_ids: vec![GLOBAL_BRANCH_ID.to_string()],
                    ..UntrackedStateFilter::default()
                },
                ..UntrackedStateScanRequest::default()
            })
            .await?;
        let mut heads = rows
            .iter()
            .map(|row| {
                let branch_id = row.entity_pk.as_single_string_owned()?;
                decode_branch_head(&branch_id, row)
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        heads.sort_by(|left, right| left.branch_id.cmp(&right.branch_id));
        Ok(heads)
    }
}

#[async_trait::async_trait]
impl<S> BranchRefReader for BranchRefStoreReader<S>
where
    S: StorageRead + Send + Sync,
{
    async fn load_head(&self, branch_id: &str) -> Result<Option<BranchHead>, LixError> {
        BranchRefStoreReader::load_head(self, branch_id).await
    }

    async fn load_head_commit_id(&self, branch_id: &str) -> Result<Option<String>, LixError> {
        BranchRefStoreReader::load_head_commit_id(self, branch_id).await
    }

    async fn scan_heads(&self) -> Result<Vec<BranchHead>, LixError> {
        BranchRefStoreReader::scan_heads(self).await
    }
}

/// Write side for moving branch heads.
pub(super) struct BranchRefWriter<'a> {
    untracked_state: Arc<UntrackedStateContext>,
    writes: &'a mut StorageWriteSet,
}

impl BranchRefWriter<'_> {
    pub(crate) fn stage_rows(&mut self, rows: &[UntrackedStateRow]) -> Result<(), LixError> {
        self.untracked_state
            .writer(self.writes)
            .stage_rows(rows.iter().map(|row| row.as_ref()))
    }
}

fn decode_branch_head(
    requested_branch_id: &str,
    row: &MaterializedUntrackedStateRow,
) -> Result<Option<BranchHead>, LixError> {
    let Some(snapshot_content) = row.snapshot_content.as_deref() else {
        return Ok(None);
    };
    let snapshot =
        serde_json::from_str::<serde_json::Value>(snapshot_content).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("engine branch-ref snapshot parse failed: {error}"),
            )
        })?;
    let commit_id = snapshot
        .get("commit_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("branch ref for branch '{requested_branch_id}' is missing commit_id"),
            )
        })?;
    Ok(Some(BranchHead {
        branch_id: requested_branch_id.to_string(),
        commit_id: commit_id.to_string(),
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::storage::{InMemoryStorageBackend, StorageReadOptions, StorageWriteOptions};
    use crate::storage::{StorageContext, StorageWriteSet};
    use crate::transaction::prepare_branch_ref_row;
    use crate::untracked_state::{UntrackedStateContext, UntrackedStateRowRequest};

    use super::*;

    #[tokio::test]
    async fn load_head_returns_none_when_missing() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let branch_ref = test_branch_ref();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");

        let head = branch_ref
            .reader(read)
            .load_head("missing-branch")
            .await
            .expect("missing branch ref should load cleanly");

        assert_eq!(head, None);
    }

    #[tokio::test]
    async fn advance_head_writes_untracked_global_ref() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let branch_ref = BranchRefContext::new(Arc::new(UntrackedStateContext::new()));

        let mut writes = storage.new_write_set();
        stage_branch_head(
            &branch_ref,
            &mut writes,
            "branch-a",
            "commit-a",
            "2026-01-01T00:00:00Z",
        )
        .expect("branch head should advance");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("branch head should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let head = branch_ref
            .reader(read)
            .load_head("branch-a")
            .await
            .expect("branch head should load")
            .expect("branch head should exist");
        assert_eq!(head.branch_id, "branch-a");
        assert_eq!(head.commit_id, "commit-a");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut reader = UntrackedStateContext::new().reader(read);
        let row = reader
            .load_row(&UntrackedStateRowRequest {
                schema_key: BRANCH_REF_SCHEMA_KEY.to_string(),
                branch_id: GLOBAL_BRANCH_ID.to_string(),
                entity_pk: crate::entity_pk::EntityPk::single("branch-a"),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("branch-ref row should load")
            .expect("branch-ref row should exist");
        assert!(row.global);
        assert_eq!(row.created_at, "2026-01-01T00:00:00Z");
        assert_eq!(row.updated_at, "2026-01-01T00:00:00Z");
    }

    #[tokio::test]
    async fn scan_heads_returns_sorted_branch_heads() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let branch_ref = test_branch_ref();

        let mut writes = storage.new_write_set();
        stage_branch_head(
            &branch_ref,
            &mut writes,
            "branch-b",
            "commit-b",
            "2026-01-01T00:00:00Z",
        )
        .expect("branch-b should advance");
        stage_branch_head(
            &branch_ref,
            &mut writes,
            "branch-a",
            "commit-a",
            "2026-01-01T00:00:00Z",
        )
        .expect("branch-a should advance");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("branch heads should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let heads = branch_ref
            .reader(read)
            .scan_heads()
            .await
            .expect("heads should scan");

        assert_eq!(
            heads,
            vec![
                BranchHead {
                    branch_id: "branch-a".to_string(),
                    commit_id: "commit-a".to_string(),
                },
                BranchHead {
                    branch_id: "branch-b".to_string(),
                    commit_id: "commit-b".to_string(),
                },
            ]
        );
    }

    fn test_branch_ref() -> BranchRefContext {
        BranchRefContext::new(Arc::new(UntrackedStateContext::new()))
    }

    fn stage_branch_head(
        branch_ref: &BranchRefContext,
        writes: &mut StorageWriteSet,
        branch_id: &str,
        commit_id: &str,
        timestamp: &str,
    ) -> Result<(), LixError> {
        let canonical_row = prepare_branch_ref_row(branch_id, commit_id, timestamp)?;
        branch_ref.writer(writes).stage_rows(&[canonical_row.row])
    }
}
