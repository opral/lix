use crate::LixError;
use crate::gc::{
    RepositoryGcPlan, load_checkpoint_gc_state, stage_checkpoint_gc_state, stage_repository_gc,
};
use crate::storage_adapter::{
    SharedStorageAdapterRead, Storage, StorageReadOptions, StorageWriteOptions,
};
use crate::transaction::{begin_commit_boundary, commit_at_boundary};

use super::SessionContext;
use super::checkpoint::checkpoint_gc_due;

impl<StorageImpl> SessionContext<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Runs one repository-wide sweep after a checkpoint has committed.
    ///
    /// The checkpoint transaction has already atomically published both the
    /// new branch head and its rotated recovery root. This follow-up pass takes
    /// the same repository write gate as ordinary implicit writes, plans from
    /// one pinned read, and commits the entire sweep as one write set.
    async fn collect_checkpoint_garbage(&self) -> Result<Option<RepositoryGcPlan>, LixError> {
        let write_access = self.begin_session_write_access().await?;
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let mut gc_state = load_checkpoint_gc_state(&read).await?;
        if !checkpoint_gc_due(gc_state)? {
            return Ok(None);
        }
        let mut writes = self.storage.new_write_set();
        let plan = stage_repository_gc(read, &mut writes).await?;
        gc_state.mark_collected();
        stage_checkpoint_gc_state(&mut writes, &gc_state)?;
        let commit_boundary = self.transaction_commit_boundary();
        let _commit_guard = begin_commit_boundary(Some(&commit_boundary));
        let prepared_commit = self
            .storage
            .prepare_write_set(writes, StorageWriteOptions::default())
            .await?;
        let stats = commit_at_boundary(Some(&commit_boundary), || async move {
            let (_, stats) = prepared_commit.commit().await?;
            Ok(stats)
        })
        .await?;
        drop(write_access);
        self.observe_invalidation.bump_if_storage_changed(&stats);
        Ok(Some(plan))
    }

    /// Checkpoint creation must not fail merely because opportunistic cleanup
    /// could not complete. Repository-global debt is cleared only in the same
    /// atomic write as a successful sweep, so every later checkpoint retries
    /// while collection remains due.
    pub(super) async fn collect_checkpoint_garbage_best_effort(&self) {
        match self.collect_checkpoint_garbage().await {
            Ok(Some(plan)) => {
                tracing::debug!(
                    swept_commits = plan.changelog.sweep.commits.len(),
                    swept_changes = plan.changelog.sweep.changes.len(),
                    swept_tracked_roots = plan.sweep.tracked_commit_roots.len(),
                    root_discovery_us = plan.profile.root_discovery_us,
                    changelog_us = plan.profile.changelog_us,
                    tracked_root_stage_us = plan.profile.tracked_root_stage_us,
                    gc_total_us = plan.profile.total_us,
                    "completed post-checkpoint garbage collection"
                );
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "post-checkpoint garbage collection failed; checkpoint remains committed"
                );
            }
        }
    }
}
