use crate::LixError;
use crate::forktree::{GcBudget, GcStepStatus};
use crate::storage_adapter::Storage;

use super::SessionContext;

impl<StorageImpl> SessionContext<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Runs one repository-wide sweep after a checkpoint has committed.
    ///
    /// The checkpoint transaction has already atomically published both the
    /// new branch head and its rotated recovery root. This follow-up pass takes
    /// the same repository write gate as ordinary implicit writes, plans from
    /// one pinned read, and commits at most one bounded epoch-fenced page.
    async fn collect_checkpoint_garbage(&self) -> Result<GcStepStatus, LixError> {
        let write_access = self.begin_session_write_access().await?;
        let status = self
            .storage
            .advance_forktree_gc(GcBudget::default())
            .await?;
        drop(write_access);
        Ok(status)
    }

    /// Checkpoint creation must not fail merely because opportunistic cleanup
    /// could not complete. Authenticated GC progress remains durable only when
    /// its bounded sweep commits, so every later checkpoint can resume safely.
    pub(super) async fn collect_checkpoint_garbage_best_effort(&self) {
        match self.collect_checkpoint_garbage().await {
            Ok(status) => {
                tracing::debug!(
                    ?status,
                    "completed bounded ForkTree garbage-collection step"
                );
            }
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "post-checkpoint garbage collection failed; checkpoint remains committed"
                );
            }
        }
    }
}
