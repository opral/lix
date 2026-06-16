use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::watch;

use crate::storage::StorageWriteSetStats;

#[derive(Debug)]
pub(crate) struct ObserveInvalidation {
    generation: AtomicU64,
    sender: watch::Sender<u64>,
}

impl ObserveInvalidation {
    pub(crate) fn new() -> Self {
        let (sender, _) = watch::channel(0);
        Self {
            generation: AtomicU64::new(0),
            sender,
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
}
