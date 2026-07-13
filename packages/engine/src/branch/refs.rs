use crate::GLOBAL_BRANCH_ID;
use crate::LixError;
use crate::branch::BRANCH_REF_SCHEMA_KEY;
use crate::branch::{BranchHead, BranchRefReader};
use crate::changelog::CommitId;
use crate::entity_pk::EntityPk;
use crate::live_state::index::{
    LiveStateIndexContext, LiveStateIndexFilter, LiveStateIndexRowRequest,
    LiveStateIndexScanRequest, MaterializedLiveStateIndexRow,
};
use crate::storage::StorageRead;

/// Typed access to moving branch heads stored in live state.
///
/// Branch refs are one of the inputs used by live_state visibility, so this
/// context deliberately bypasses live_state and reads the canonical current
/// rows directly. That keeps the dependency acyclic:
/// live_index -> branch_ref -> live_state.
pub(super) struct BranchRefContext {}

impl BranchRefContext {
    pub(super) fn new() -> Self {
        Self {}
    }

    /// Creates a branch-ref reader over a caller-provided KV store.
    #[expect(clippy::unused_self)]
    pub(super) fn reader<S>(&self, store: S) -> BranchRefStoreReader<S>
    where
        S: StorageRead,
    {
        BranchRefStoreReader { store }
    }
}

/// Read side for branch heads.
pub(super) struct BranchRefStoreReader<S>
where
    S: StorageRead,
{
    store: S,
}

impl<S> BranchRefStoreReader<S>
where
    S: StorageRead,
{
    pub(crate) async fn load_head(&self, branch_id: &str) -> Result<Option<BranchHead>, LixError> {
        let Some(row) = LiveStateIndexContext::new()
            .reader(&self.store)
            .load_row(&LiveStateIndexRowRequest {
                schema_key: BRANCH_REF_SCHEMA_KEY.to_string(),
                branch_id: GLOBAL_BRANCH_ID.to_string(),
                entity_pk: EntityPk::single(branch_id),
                file_id: None,
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
    ) -> Result<Option<CommitId>, LixError> {
        Ok(self.load_head(branch_id).await?.map(|head| head.commit_id))
    }

    pub(crate) async fn scan_heads(&self) -> Result<Vec<BranchHead>, LixError> {
        let rows = LiveStateIndexContext::new()
            .reader(&self.store)
            .scan_rows(&LiveStateIndexScanRequest {
                branch_id: GLOBAL_BRANCH_ID.to_string(),
                filter: LiveStateIndexFilter {
                    schema_keys: vec![BRANCH_REF_SCHEMA_KEY.to_string()],
                    ..LiveStateIndexFilter::default()
                },
                projection: Vec::new(),
                limit: None,
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
    S: StorageRead,
{
    async fn load_head(&self, branch_id: &str) -> Result<Option<BranchHead>, LixError> {
        Self::load_head(self, branch_id).await
    }

    async fn load_head_commit_id(&self, branch_id: &str) -> Result<Option<CommitId>, LixError> {
        Self::load_head_commit_id(self, branch_id).await
    }

    async fn scan_heads(&self) -> Result<Vec<BranchHead>, LixError> {
        Self::scan_heads(self).await
    }
}

fn decode_branch_head(
    requested_branch_id: &str,
    row: &MaterializedLiveStateIndexRow,
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
        commit_id: CommitId::parse_lix(commit_id, "branch ref commit_id")?,
    }))
}

#[cfg(test)]
mod tests {
    use crate::changelog::{
        ChangeId, ChangeRecord, ChangelogAppend, ChangelogContext, ChangelogWriter,
    };
    use crate::live_state::index::{LiveStateIndexDeltaRef, LiveStateIndexRowRequest};
    use crate::storage::StorageContext;
    use crate::storage::{InMemoryStorageBackend, StorageReadOptions, StorageWriteOptions};

    use super::*;

    #[tokio::test]
    async fn load_head_returns_none_when_missing() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let branch_ref = test_branch_ref();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");

        let head = branch_ref
            .reader(read)
            .load_head("missing-branch")
            .await
            .expect("missing branch ref should load cleanly");

        assert_eq!(head, None);
    }

    #[tokio::test]
    async fn advance_head_writes_global_current_ref() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let branch_ref = BranchRefContext::new();

        stage_branch_head(&storage, "branch-a", "commit-a", "2026-01-01T00:00:00Z")
            .await
            .expect("branch head should advance");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
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
            .await
            .expect("read should open");
        let row = LiveStateIndexContext::new()
            .reader(read)
            .load_row(&LiveStateIndexRowRequest {
                schema_key: BRANCH_REF_SCHEMA_KEY.to_string(),
                branch_id: GLOBAL_BRANCH_ID.to_string(),
                entity_pk: EntityPk::single("branch-a"),
                file_id: None,
            })
            .await
            .expect("branch-ref row should load")
            .expect("branch-ref row should exist");
        assert_eq!(row.created_at, "2026-01-01T00:00:00.000Z");
        assert_eq!(row.updated_at, "2026-01-01T00:00:00.000Z");
    }

    #[tokio::test]
    async fn scan_heads_returns_sorted_branch_heads() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let branch_ref = test_branch_ref();

        stage_branch_head(&storage, "branch-b", "commit-b", "2026-01-01T00:00:00Z")
            .await
            .expect("branch-b should advance");
        stage_branch_head(&storage, "branch-a", "commit-a", "2026-01-01T00:00:00Z")
            .await
            .expect("branch-a should advance");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
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
                    commit_id: CommitId::for_test_label("commit-a"),
                },
                BranchHead {
                    branch_id: "branch-b".to_string(),
                    commit_id: CommitId::for_test_label("commit-b"),
                },
            ]
        );
    }

    fn test_branch_ref() -> BranchRefContext {
        BranchRefContext::new()
    }

    async fn stage_branch_head(
        storage: &StorageContext,
        branch_id: &str,
        commit_id: &str,
        timestamp: &str,
    ) -> Result<(), LixError> {
        let commit_id = CommitId::parse_lix(commit_id, "test branch head commit_id")?;
        let timestamp = crate::common::LixTimestamp::expect_parse("timestamp", timestamp);
        let entity_pk = EntityPk::single(branch_id);
        let snapshot = serde_json::json!({
            "id": branch_id,
            "commit_id": commit_id,
        })
        .to_string();
        let change_id = ChangeId::for_test_label(&format!("branch-ref-{branch_id}"));
        let read = storage.begin_read(StorageReadOptions::default()).await?;
        let mut writes = storage.new_write_set();
        {
            let mut changelog_read = &read;
            ChangelogContext::new()
                .writer(&mut changelog_read, &mut writes)
                .stage_append(ChangelogAppend {
                    changes: vec![ChangeRecord {
                        format_version: 2,
                        change_id,
                        schema_key: BRANCH_REF_SCHEMA_KEY.to_string(),
                        entity_pk: entity_pk.clone(),
                        file_id: None,
                        snapshot: crate::json_store::JsonSlot::from_json(&snapshot),
                        metadata: crate::json_store::JsonSlot::None,
                        created_at: timestamp,
                        origin_key: None,
                    }],
                    ..ChangelogAppend::default()
                })
                .await?;
        }
        LiveStateIndexContext::new()
            .writer(&read, &mut writes)
            .stage_branch_rows(
                GLOBAL_BRANCH_ID,
                [LiveStateIndexDeltaRef {
                    schema_key: BRANCH_REF_SCHEMA_KEY,
                    file_id: None,
                    entity_pk: &entity_pk,
                    change_id,
                    commit_id: None,
                    deleted: false,
                    created_at: timestamp,
                    updated_at: timestamp,
                }],
            )
            .await?;
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await?;
        Ok(())
    }
}
