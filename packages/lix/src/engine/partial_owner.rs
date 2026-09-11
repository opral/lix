// Engine/runtime-shared lifecycle: closed Engine allocation need not die
// before another engine can open. Storage token lives only in holder/task clones.
#[derive(Clone, Debug, Default)]
pub(crate) struct PartialOwnerLifetime {
    configured: bool,
    inner: std::sync::Arc<std::sync::Mutex<Option<crate::storage_adapter::StorageOwnerLease>>>,
}
impl PartialOwnerLifetime {
    pub(crate) fn install(lease: crate::storage_adapter::StorageOwnerLease) -> Self {
        Self {
            configured: true,
            inner: std::sync::Arc::new(std::sync::Mutex::new(Some(lease))),
        }
    }
    pub(crate) fn retain_for_owned_work(
        &self,
    ) -> Result<crate::storage_adapter::StorageOwnerLease, crate::LixError> {
        self.inner
            .lock()
            .map_err(|_| {
                crate::LixError::new("LIX_STORAGE_IN_USE", "partial owner lifecycle poisoned")
            })?
            .clone()
            .ok_or_else(|| {
                crate::LixError::new("LIX_STORAGE_CLOSED", "partial owner lifetime ended")
            })
    }
    pub(crate) fn is_installed(&self) -> bool {
        self.configured
    }
    pub(crate) fn close(&self) {
        // Remove permission to start new work while outstanding task clones preserve
        // actual storage exclusion. Drop invokes synchronous provider release signal.
        if let Ok(mut owner) = self.inner.lock() {
            owner.take();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_adapter::{Memory, Storage, StorageSession};
    #[tokio::test]
    async fn closed_holder_releases_without_dropping_engine_handle() {
        let storage = StorageSession::acquire(Memory::new()).await.unwrap();
        let owner = storage
            .acquire_partial_replica_owner(storage.token())
            .await
            .unwrap();
        let lifetime = PartialOwnerLifetime::install(owner);
        let retained_engine = lifetime.clone();
        lifetime.close();
        assert!(retained_engine.retain_for_owned_work().is_err());
        assert!(
            storage
                .acquire_partial_replica_owner(storage.token())
                .await
                .is_ok()
        );
    }
    #[tokio::test]
    async fn detached_commit_holds_owner_after_close_and_caller_cancellation() {
        let storage = StorageSession::acquire(Memory::new()).await.unwrap();
        let owner = storage
            .acquire_partial_replica_owner(storage.token())
            .await
            .unwrap();
        let lifetime = PartialOwnerLifetime::install(owner);
        let guard = lifetime.retain_for_owned_work().unwrap();
        let (finish_tx, finish_rx) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let _guard = guard;
            let _ = finish_rx.await;
        });
        lifetime.close();
        assert!(
            storage
                .acquire_partial_replica_owner(storage.token())
                .await
                .is_err()
        );
        finish_tx.send(()).unwrap();
        task.await.unwrap();
        assert!(
            storage
                .acquire_partial_replica_owner(storage.token())
                .await
                .is_ok()
        );
    }
}
