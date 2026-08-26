use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(not(target_family = "wasm"))]
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::sync::watch;

use crate::LixError;
use crate::storage_adapter::Storage;
use crate::storage_adapter::StorageAdapter;
use crate::storage_adapter::StorageCapability;
use crate::storage_adapter::StorageError;
use crate::storage_adapter::StorageWriteSetStats;

#[cfg(not(target_family = "wasm"))]
const EXTERNAL_MUTATION_REVISION_POLL_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Clone, Debug)]
pub(crate) enum ObserveInvalidationEvent {
    Generation(u64),
    TerminalError(LixError),
}

#[derive(Debug)]
pub(crate) struct ObserveInvalidation {
    generation: AtomicU64,
    sender: watch::Sender<ObserveInvalidationEvent>,
    external_watcher_started: Mutex<bool>,
}

impl ObserveInvalidation {
    pub(crate) fn new() -> Self {
        let (sender, _) = watch::channel(ObserveInvalidationEvent::Generation(0));
        Self {
            generation: AtomicU64::new(0),
            sender,
            external_watcher_started: Mutex::new(false),
        }
    }

    pub(crate) fn bump(&self) -> u64 {
        let next = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
        self.sender.send_modify(|event| {
            if matches!(event, ObserveInvalidationEvent::TerminalError(_)) {
                return;
            }
            *event = ObserveInvalidationEvent::Generation(next);
        });
        next
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    pub(crate) fn fail_terminal(&self, error: LixError) {
        self.sender.send_modify(|event| {
            if !matches!(event, ObserveInvalidationEvent::TerminalError(_)) {
                *event = ObserveInvalidationEvent::TerminalError(error);
            }
        });
    }

    pub(crate) fn bump_if_storage_changed(&self, stats: &StorageWriteSetStats) {
        if stats.staged_puts > 0 || stats.staged_deletes > 0 {
            self.bump();
        }
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<ObserveInvalidationEvent> {
        self.sender.subscribe()
    }

    pub(crate) async fn ensure_external_watcher<StorageImpl>(
        self: &Arc<Self>,
        storage: StorageAdapter<StorageImpl>,
    ) -> Result<(), LixError>
    where
        StorageImpl: Storage + Clone + Send + Sync + 'static,
    {
        // Keep contenders behind the startup gate until the watcher has read
        // its baseline. Otherwise they can evaluate an older snapshot that
        // the watcher treats as already seen; cancellation also releases this
        // gate so a contender can retry startup.
        let mut watcher_started = self.external_watcher_started.lock().await;
        let event = self.sender.borrow().clone();
        if let ObserveInvalidationEvent::TerminalError(error) = event {
            return Err(error);
        }
        if *watcher_started {
            return Ok(());
        }

        match storage.watch_for_changes().await {
            Ok(mut changes) => {
                let invalidation = Arc::downgrade(self);
                crate::background_task::spawn("lix-observe-change-watch", move || async move {
                    loop {
                        let Some(invalidation) = invalidation.upgrade() else {
                            break;
                        };
                        match changes.changed().await {
                            Ok(()) => {
                                invalidation.bump();
                            }
                            Err(error) => {
                                if matches!(error, StorageError::Fenced | StorageError::Closed(_)) {
                                    invalidation.fail_terminal(error.into());
                                } else {
                                    // Wake observers so the stable-read loop can retry and
                                    // reopen the adapter watch after a transient failure.
                                    invalidation.bump();
                                }
                                *invalidation.external_watcher_started.lock().await = false;
                                break;
                            }
                        }
                    }
                })?;
                *watcher_started = true;
                return Ok(());
            }
            Err(StorageError::Unsupported(StorageCapability::ChangeWatch)) => {}
            Err(error) => {
                let error: LixError = error.into();
                if matches!(
                    error.code.as_str(),
                    LixError::CODE_STORAGE_FENCED | LixError::CODE_STORAGE_CLOSED
                ) {
                    self.fail_terminal(error.clone());
                }
                return Err(error);
            }
        }

        #[cfg(target_family = "wasm")]
        {
            // Browser memory storage cannot be modified outside its engine.
            // Shared JS providers implement the change-watch capability.
            *watcher_started = true;
            return Ok(());
        }

        #[cfg(not(target_family = "wasm"))]
        {
            let mut last_seen_revision = match storage.load_mutation_revision().await {
                Ok(revision) => revision,
                Err(error) => {
                    let error: LixError = error.into();
                    if matches!(
                        error.code.as_str(),
                        LixError::CODE_STORAGE_FENCED | LixError::CODE_STORAGE_CLOSED
                    ) {
                        self.fail_terminal(error.clone());
                    }
                    return Err(error);
                }
            };
            let invalidation = Arc::downgrade(self);
            crate::background_task::spawn("lix-observe-invalidation", move || async move {
                loop {
                    // This is a dedicated Lix-owned worker. Sleeping the worker
                    // thread avoids requiring a Tokio timer driver from the host.
                    std::thread::sleep(EXTERNAL_MUTATION_REVISION_POLL_INTERVAL);
                    let Some(invalidation) = invalidation.upgrade() else {
                        break;
                    };
                    if invalidation.sender.receiver_count() == 0 {
                        // Synchronize shutdown with startup. A new subscriber can race this
                        // check; rechecking under the startup gate either keeps this watcher
                        // alive or lets the contender start its replacement.
                        let mut watcher_started =
                            invalidation.external_watcher_started.lock().await;
                        if invalidation.sender.receiver_count() == 0 {
                            *watcher_started = false;
                            break;
                        }
                        drop(watcher_started);
                    }
                    let current_revision = match storage.load_mutation_revision().await {
                        Ok(revision) => revision,
                        Err(error) => {
                            let error: LixError = error.into();
                            if matches!(
                                error.code.as_str(),
                                LixError::CODE_STORAGE_FENCED | LixError::CODE_STORAGE_CLOSED
                            ) {
                                invalidation.fail_terminal(error);
                                break;
                            }
                            continue;
                        }
                    };
                    if current_revision != last_seen_revision {
                        last_seen_revision = current_revision;
                        invalidation.bump();
                    }
                }
            })?;
            *watcher_started = true;
            Ok(())
        }
    }
}

#[cfg(all(test, not(target_family = "wasm")))]
mod tests {
    use super::*;
    use crate::storage::{
        Memory, MemoryRead, MemoryWrite, ReadOptions, StorageChangeSource, StorageChangeWatch,
        StorageError, WriteOptions,
    };
    use crate::storage_adapter::StorageAdapter;

    #[tokio::test]
    async fn terminal_runtime_error_is_sticky_for_observers() {
        let invalidation = ObserveInvalidation::new();
        let mut observer = invalidation.subscribe();
        invalidation.fail_terminal(LixError::new(
            "LIX_ERROR_SYNC_ITEM_TOO_LARGE",
            "sync cannot make progress",
        ));

        observer
            .changed()
            .await
            .expect("terminal runtime error should notify observers");
        invalidation.bump();
        assert!(matches!(
            observer.borrow_and_update().clone(),
            ObserveInvalidationEvent::TerminalError(error)
                if error.code == "LIX_ERROR_SYNC_ITEM_TOO_LARGE"
        ));
    }
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::AtomicBool;
    use tokio::sync::Notify;

    #[derive(Clone)]
    struct BlockingFirstReadStorage {
        inner: Memory,
        first_read: Arc<AtomicBool>,
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl BlockingFirstReadStorage {
        fn new() -> Self {
            Self {
                inner: Memory::new(),
                first_read: Arc::new(AtomicBool::new(true)),
                entered: Arc::new(Notify::new()),
                release: Arc::new(Notify::new()),
            }
        }

        async fn wait_for_initial_read(&self) {
            loop {
                let notified = self.entered.notified();
                if !self.first_read.load(Ordering::Acquire) {
                    return;
                }
                notified.await;
            }
        }
    }

    impl Storage for BlockingFirstReadStorage {
        type Read<'a>
            = MemoryRead
        where
            Self: 'a;
        type Write<'a>
            = MemoryWrite
        where
            Self: 'a;

        async fn acquire_session(
            &self,
        ) -> Result<crate::storage::StorageSessionToken, StorageError> {
            self.inner.acquire_session().await
        }

        async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
            if self.first_read.swap(false, Ordering::AcqRel) {
                self.entered.notify_waiters();
                self.release.notified().await;
            }
            self.inner.begin_read(options).await
        }

        async fn begin_write(
            &self,
            options: WriteOptions,
        ) -> Result<Self::Write<'_>, StorageError> {
            self.inner.begin_write(options).await
        }
    }

    #[derive(Clone)]
    struct FencedInitialReadStorage {
        inner: Memory,
    }

    #[derive(Clone)]
    struct FencedChangeWatchStorage {
        inner: Memory,
    }

    struct FencedChangeSource;

    impl Storage for FencedInitialReadStorage {
        type Read<'a>
            = MemoryRead
        where
            Self: 'a;
        type Write<'a>
            = MemoryWrite
        where
            Self: 'a;

        async fn acquire_session(
            &self,
        ) -> Result<crate::storage::StorageSessionToken, StorageError> {
            self.inner.acquire_session().await
        }

        async fn begin_read(&self, _options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
            Err(StorageError::Fenced)
        }

        async fn begin_write(
            &self,
            options: WriteOptions,
        ) -> Result<Self::Write<'_>, StorageError> {
            self.inner.begin_write(options).await
        }
    }

    impl Storage for FencedChangeWatchStorage {
        type Read<'a>
            = MemoryRead
        where
            Self: 'a;
        type Write<'a>
            = MemoryWrite
        where
            Self: 'a;

        async fn acquire_session(
            &self,
        ) -> Result<crate::storage::StorageSessionToken, StorageError> {
            self.inner.acquire_session().await
        }

        async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
            self.inner.begin_read(options).await
        }

        async fn begin_write(
            &self,
            options: WriteOptions,
        ) -> Result<Self::Write<'_>, StorageError> {
            self.inner.begin_write(options).await
        }

        async fn watch_for_changes(&self) -> Result<StorageChangeWatch, StorageError> {
            Ok(StorageChangeWatch::from_source(FencedChangeSource))
        }
    }

    impl StorageChangeSource for FencedChangeSource {
        fn changed(
            &mut self,
        ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + '_>> {
            Box::pin(async { Err(StorageError::Fenced) })
        }
    }

    #[tokio::test]
    async fn initial_terminal_watcher_error_is_sticky() {
        let invalidation = Arc::new(ObserveInvalidation::new());
        let mut observer = invalidation.subscribe();
        let storage = FencedInitialReadStorage {
            inner: Memory::new(),
        };

        let error = invalidation
            .ensure_external_watcher(StorageAdapter::new(storage.clone()))
            .await
            .expect_err("initial fenced watcher read should fail");
        assert_eq!(error.code, LixError::CODE_STORAGE_FENCED);
        observer
            .changed()
            .await
            .expect("initial terminal error should notify observers");
        assert!(matches!(
            observer.borrow_and_update().clone(),
            ObserveInvalidationEvent::TerminalError(error)
                if error.code == LixError::CODE_STORAGE_FENCED
        ));

        let retry_error = invalidation
            .ensure_external_watcher(StorageAdapter::new(storage))
            .await
            .expect_err("terminal watcher failure should remain sticky");
        assert_eq!(retry_error.code, LixError::CODE_STORAGE_FENCED);
    }

    #[tokio::test]
    async fn fenced_change_watch_is_terminal_for_observers() {
        let invalidation = Arc::new(ObserveInvalidation::new());
        let mut observer = invalidation.subscribe();
        invalidation
            .ensure_external_watcher(StorageAdapter::new(FencedChangeWatchStorage {
                inner: Memory::new(),
            }))
            .await
            .expect("watch should establish before reporting fencing");

        tokio::time::timeout(Duration::from_secs(1), observer.changed())
            .await
            .expect("fenced watch should wake observers")
            .expect("watch channel should remain open");
        assert!(matches!(
            observer.borrow_and_update().clone(),
            ObserveInvalidationEvent::TerminalError(error)
                if error.code == LixError::CODE_STORAGE_FENCED
        ));
    }

    #[tokio::test]
    async fn contending_observer_waits_for_cancelled_watcher_start_and_retries() {
        let invalidation = Arc::new(ObserveInvalidation::new());
        let storage = BlockingFirstReadStorage::new();
        let cancelled_start = {
            let invalidation = Arc::clone(&invalidation);
            let storage = StorageAdapter::new(storage.clone());
            tokio::spawn(async move { invalidation.ensure_external_watcher(storage).await })
        };
        tokio::time::timeout(Duration::from_secs(1), storage.wait_for_initial_read())
            .await
            .expect("watcher startup should begin its initial read");
        let (contender_entered_tx, contender_entered_rx) = tokio::sync::oneshot::channel();
        let mut contending_start = {
            let invalidation = Arc::clone(&invalidation);
            let storage = StorageAdapter::new(storage.clone());
            tokio::spawn(async move {
                let _ = contender_entered_tx.send(());
                invalidation.ensure_external_watcher(storage).await
            })
        };
        contender_entered_rx
            .await
            .expect("contending observer task should start");
        tokio::task::yield_now().await;
        assert!(
            !contending_start.is_finished(),
            "contending observer must wait until the initial watcher startup completes"
        );
        cancelled_start.abort();
        assert!(
            cancelled_start
                .await
                .expect_err("cancelled watcher startup task")
                .is_cancelled(),
            "initial watcher startup should be cancelled"
        );
        tokio::time::timeout(Duration::from_secs(1), &mut contending_start)
            .await
            .expect("contending observer should retry after cancelled startup")
            .expect("contending observer task should not panic")
            .expect("contending observer should establish the watcher");
        assert!(
            *invalidation.external_watcher_started.lock().await,
            "contending retry should mark the watcher as started"
        );
    }

    #[tokio::test]
    async fn external_watcher_stops_without_subscribers_and_restarts_on_demand() {
        let invalidation = Arc::new(ObserveInvalidation::new());
        let storage = StorageAdapter::new(Memory::new());
        let observer = invalidation.subscribe();
        invalidation
            .ensure_external_watcher(storage.clone())
            .await
            .expect("watcher should start");
        drop(observer);

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if !*invalidation.external_watcher_started.lock().await {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("watcher should stop after its last subscriber is dropped");

        let _replacement = invalidation.subscribe();
        invalidation
            .ensure_external_watcher(storage)
            .await
            .expect("watcher should restart");
        assert!(*invalidation.external_watcher_started.lock().await);
    }
}
