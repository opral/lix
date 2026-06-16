use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use tokio::sync::watch;

use crate::LixError;
use crate::storage::{StorageBackend, StorageContext, StorageWriteSetStats};

const EXTERNAL_MUTATION_REVISION_POLL_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug)]
pub(crate) struct ObserveInvalidation {
    generation: AtomicU64,
    sender: watch::Sender<u64>,
    external_watcher_started: AtomicBool,
}

impl ObserveInvalidation {
    pub(crate) fn new() -> Self {
        let (sender, _) = watch::channel(0);
        Self {
            generation: AtomicU64::new(0),
            sender,
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

    pub(crate) fn ensure_external_watcher<B>(
        self: &Arc<Self>,
        storage: StorageContext<B>,
    ) -> Result<(), LixError>
    where
        B: StorageBackend + Clone + Send + Sync + 'static,
        for<'backend> B::Read<'backend>: Send,
        for<'backend> B::Write<'backend>: Send,
    {
        let mut last_seen_revision = storage.load_mutation_revision()?;
        let mut last_seen_generation = self.generation.load(Ordering::SeqCst);
        if self
            .external_watcher_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok(());
        }
        let invalidation = Arc::downgrade(self);
        let spawn_result = thread::Builder::new()
            .name("lix-observe-invalidation".to_string())
            .spawn(move || {
                loop {
                    thread::sleep(EXTERNAL_MUTATION_REVISION_POLL_INTERVAL);
                    let Some(invalidation) = invalidation.upgrade() else {
                        break;
                    };
                    let Ok(current_revision) = storage.load_mutation_revision() else {
                        continue;
                    };
                    if current_revision != last_seen_revision {
                        last_seen_revision = current_revision;
                        let current_generation = invalidation.generation.load(Ordering::SeqCst);
                        if current_generation == last_seen_generation {
                            last_seen_generation = invalidation.bump();
                        } else {
                            last_seen_generation = current_generation;
                        }
                    }
                }
            });
        if let Err(error) = spawn_result {
            self.external_watcher_started.store(false, Ordering::SeqCst);
            return Err(LixError::unknown(format!(
                "failed to spawn observe invalidation watcher: {error}"
            )));
        }
        Ok(())
    }
}
