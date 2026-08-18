use crate::LixError;
use crate::branch::{BranchHead, BranchHeadControlContext, BranchRefReader};
use crate::storage_adapter::StorageAdapterRead;

/// Typed access to moving branch heads stored in the direct control plane.
///
/// The control record is deliberately below live-state visibility, keeping
/// the dependency acyclic: `branch-control -> tracked-head -> live-state`.
pub(super) struct BranchRefContext {}

impl BranchRefContext {
    pub(super) fn new() -> Self {
        Self {}
    }

    /// Creates a branch-ref reader over a caller-provided KV store.
    #[expect(clippy::unused_self)]
    pub(super) fn reader<S>(&self, store: S) -> BranchRefStoreReader<S>
    where
        S: StorageAdapterRead,
    {
        BranchRefStoreReader {
            controls: BranchHeadControlContext::new().reader(store),
        }
    }
}

/// Read side for branch heads.
pub(super) struct BranchRefStoreReader<S>
where
    S: StorageAdapterRead,
{
    controls: crate::branch::BranchHeadControlReader<S>,
}

impl<S> BranchRefStoreReader<S>
where
    S: StorageAdapterRead,
{
    pub(crate) async fn load_head(&self, branch_id: &str) -> Result<Option<BranchHead>, LixError> {
        Ok(self
            .controls
            .load(branch_id)
            .await?
            .map(|control| BranchHead {
                branch_id: branch_id.to_string(),
                commit_id: control.head_commit_id,
            }))
    }

    pub(crate) async fn scan_heads(&self) -> Result<Vec<BranchHead>, LixError> {
        Ok(self
            .controls
            .scan()
            .await?
            .into_iter()
            .map(|(branch_id, control)| BranchHead {
                branch_id,
                commit_id: control.head_commit_id,
            })
            .collect())
    }
}

#[async_trait::async_trait]
impl<S> BranchRefReader for BranchRefStoreReader<S>
where
    S: StorageAdapterRead,
{
    async fn load_head(&self, branch_id: &str) -> Result<Option<BranchHead>, LixError> {
        Self::load_head(self, branch_id).await
    }

    async fn scan_heads(&self) -> Result<Vec<BranchHead>, LixError> {
        Self::scan_heads(self).await
    }
}

#[cfg(test)]
mod tests {
    use crate::branch::{BranchHeadControl, stage_branch_head_control};
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::LixTimestamp;
    use crate::storage_adapter::StorageAdapter;
    use crate::storage_adapter::{Memory, StorageReadOptions, StorageWriteOptions};

    use super::*;

    #[tokio::test]
    async fn load_head_returns_none_when_missing() {
        let storage = StorageAdapter::new(Memory::new());
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
    async fn advance_head_writes_direct_control() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_ref = BranchRefContext::new();

        stage_branch_head(&storage, "01920000-0000-7000-8000-0000000000a1", "commit-a")
            .await
            .expect("branch head should advance");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let head = branch_ref
            .reader(read)
            .load_head("01920000-0000-7000-8000-0000000000a1")
            .await
            .expect("branch head should load")
            .expect("branch head should exist");
        assert_eq!(head.branch_id, "01920000-0000-7000-8000-0000000000a1");
        assert_eq!(head.commit_id, "commit-a");
    }

    #[tokio::test]
    async fn scan_heads_returns_sorted_branch_heads() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_ref = test_branch_ref();

        stage_branch_head(&storage, "01920000-0000-7000-8000-0000000000b1", "commit-b")
            .await
            .expect("01920000-0000-7000-8000-0000000000b1 should advance");
        stage_branch_head(&storage, "01920000-0000-7000-8000-0000000000a1", "commit-a")
            .await
            .expect("01920000-0000-7000-8000-0000000000a1 should advance");

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
                    branch_id: "01920000-0000-7000-8000-0000000000a1".to_string(),
                    commit_id: CommitId::for_test_label("commit-a"),
                },
                BranchHead {
                    branch_id: "01920000-0000-7000-8000-0000000000b1".to_string(),
                    commit_id: CommitId::for_test_label("commit-b"),
                },
            ]
        );
    }

    fn test_branch_ref() -> BranchRefContext {
        BranchRefContext::new()
    }

    async fn stage_branch_head(
        storage: &StorageAdapter,
        branch_id: &str,
        commit_id: &str,
    ) -> Result<(), LixError> {
        let commit_id = CommitId::parse_lix(commit_id, "test branch head commit_id")?;
        let mut writes = storage.new_write_set();
        stage_branch_head_control(
            &mut writes,
            branch_id,
            BranchHeadControl {
                head_commit_id: commit_id,
                tracked_generation: commit_id,
                current_state_revision: 0,
                schema_presence_bloom: [u64::MAX; 4],
                working_diff_checkpoint_commit_id: None,
                created_at: LixTimestamp::expect_parse(
                    "test branch ref created_at",
                    "2026-01-01T00:00:00Z",
                ),
                updated_at: LixTimestamp::expect_parse(
                    "test branch ref updated_at",
                    "2026-01-01T00:00:00Z",
                ),
                ref_change_id: ChangeId::for_test_label("test-branch-ref-change"),
            },
        )?;
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await?;
        Ok(())
    }
}
