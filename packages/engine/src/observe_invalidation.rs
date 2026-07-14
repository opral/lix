#[cfg(not(target_family = "wasm"))]
use std::sync::Arc;
#[cfg(not(target_family = "wasm"))]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(not(target_family = "wasm"))]
use std::time::Duration;

use tokio::sync::watch;

#[cfg(not(target_family = "wasm"))]
use crate::LixError;
#[cfg(not(target_family = "wasm"))]
use crate::storage_adapter::Storage;
#[cfg(not(target_family = "wasm"))]
use crate::storage_adapter::StorageAdapter;
use crate::storage_adapter::StorageWriteSetStats;

#[cfg(not(target_family = "wasm"))]
const EXTERNAL_MUTATION_REVISION_POLL_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug)]
pub(crate) struct ObserveInvalidation {
    generation: AtomicU64,
    sender: watch::Sender<u64>,
    #[cfg(not(target_family = "wasm"))]
    external_watcher_started: AtomicBool,
}

impl ObserveInvalidation {
    pub(crate) fn new() -> Self {
        let (sender, _) = watch::channel(0);
        Self {
            generation: AtomicU64::new(0),
            sender,
            #[cfg(not(target_family = "wasm"))]
            external_watcher_started: AtomicBool::new(false),
        }
    }

    pub(crate) fn bump(&self) -> u64 {
        let next = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
        self.sender.send_replace(next);
        next
    }

    pub(crate) fn bump_if_storage_changed(&self, stats: &StorageWriteSetStats) {
        if stats.staged_puts > 0 || stats.staged_deletes > 0 {
            self.bump();
        }
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<u64> {
        self.sender.subscribe()
    }

    #[cfg(not(target_family = "wasm"))]
    pub(crate) async fn ensure_external_watcher<StorageImpl>(
        self: &Arc<Self>,
        storage: StorageAdapter<StorageImpl>,
    ) -> Result<(), LixError>
    where
        StorageImpl: Storage + Clone + Send + Sync + 'static,
    {
        if self
            .external_watcher_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok(());
        }
        let mut last_seen_revision = match storage.load_mutation_revision().await {
            Ok(revision) => revision,
            Err(error) => {
                self.external_watcher_started.store(false, Ordering::SeqCst);
                return Err(error.into());
            }
        };
        let invalidation = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(EXTERNAL_MUTATION_REVISION_POLL_INTERVAL).await;
                let Some(invalidation) = invalidation.upgrade() else {
                    break;
                };
                let Ok(current_revision) = storage.load_mutation_revision().await else {
                    continue;
                };
                if current_revision != last_seen_revision {
                    last_seen_revision = current_revision;
                    invalidation.bump();
                }
            }
        });
        Ok(())
    }
}
