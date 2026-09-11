//! Storage-owned lifetime exclusion, not a durable format or epoch token.
use crate::storage::StorageError;
use std::sync::{Arc, Mutex, Weak};

/// Adapter-owned guard whose Drop releases the physical-store exclusion.
/// A browser implementation retains a JS Web Lock release handle. A native
/// implementation retains a physical DB owner guard. Never serialize this.
pub trait StorageOwnerGuard: Send + Sync + 'static {}

#[derive(Clone)]
pub struct StorageOwnerLease(Arc<dyn StorageOwnerGuard>);
impl std::fmt::Debug for StorageOwnerLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageOwnerLease").finish_non_exhaustive()
    }
}
impl StorageOwnerLease {
    pub fn from_guard(guard: impl StorageOwnerGuard) -> Self {
        Self(Arc::new(guard))
    }
}

/// In-process implementation only for storage whose physical identity already
/// has exactly one shared owner object (Memory or one exclusively opened DB).
#[derive(Default, Debug)]
pub struct StorageOwnerGate(Mutex<Weak<GateGuard>>);
#[derive(Debug)]
struct GateGuard;
impl StorageOwnerGuard for GateGuard {}
impl StorageOwnerGate {
    /// Caller must validate the captured storage session before invoking this.
    pub fn try_acquire(&self) -> Result<StorageOwnerLease, StorageError> {
        let mut current = self
            .0
            .lock()
            .map_err(|_| StorageError::Corruption("storage owner gate poisoned".into()))?;
        if current.upgrade().is_some() {
            // Map to a dedicated StorageError::InUse, requiring stable bridge mapping.
            return Err(StorageError::InUse);
        }
        let guard = Arc::new(GateGuard);
        *current = Arc::downgrade(&guard);
        Ok(StorageOwnerLease(guard))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{Memory, Storage, StorageSession};
    #[tokio::test]
    async fn cloned_memory_refuses_second_owner_and_last_guard_releases() {
        let memory = Memory::new();
        let session = StorageSession::acquire(memory.clone()).await.unwrap();
        let owner = session
            .acquire_partial_replica_owner(session.token())
            .await
            .unwrap();
        let child = owner.clone();
        assert!(
            memory
                .acquire_partial_replica_owner(session.token())
                .await
                .is_err()
        );
        drop(owner);
        assert!(
            memory
                .acquire_partial_replica_owner(session.token())
                .await
                .is_err()
        );
        drop(child);
        let _reopened = memory
            .acquire_partial_replica_owner(session.token())
            .await
            .unwrap();
        let unrelated = StorageSession::acquire(Memory::new()).await.unwrap();
        assert!(
            unrelated
                .acquire_partial_replica_owner(unrelated.token())
                .await
                .is_ok()
        );
    }
    #[tokio::test]
    async fn session_wrapper_rejects_foreign_token_without_displacing_owner() {
        let session = StorageSession::acquire(Memory::new()).await.unwrap();
        let other = StorageSession::acquire(Memory::new()).await.unwrap();
        assert!(matches!(
            session.acquire_partial_replica_owner(other.token()).await,
            Err(StorageError::Fenced)
        ));
        assert!(
            session
                .acquire_partial_replica_owner(session.token())
                .await
                .is_ok()
        );
    }
}
