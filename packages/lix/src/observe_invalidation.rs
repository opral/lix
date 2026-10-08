use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
#[cfg(not(target_family = "wasm"))]
use std::time::Duration;

use bytes::Bytes;
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
    signals: Arc<ObserveSignals>,
    watcher: Arc<ObserveWatcherLifecycle>,
}

#[derive(Debug)]
struct ObserveSignals {
    generation: AtomicU64,
    sender: watch::Sender<ObserveInvalidationEvent>,
    observable_revision: StdMutex<ObservableRevisionState>,
}

#[derive(Debug, Default)]
struct ObservableRevisionState {
    initialized: bool,
    revision: Option<Bytes>,
}

#[derive(Debug, Default)]
struct ObserveWatcherLifecycle {
    started: Mutex<bool>,
    task: StdMutex<Option<crate::background_task::OwnedBackgroundTask>>,
}

impl ObserveInvalidation {
    pub(crate) fn new() -> Self {
        let (sender, _) = watch::channel(ObserveInvalidationEvent::Generation(0));
        Self {
            signals: Arc::new(ObserveSignals {
                generation: AtomicU64::new(0),
                sender,
                observable_revision: StdMutex::default(),
            }),
            watcher: Arc::default(),
        }
    }

    pub(crate) fn bump(&self) -> u64 {
        bump_signals(&self.signals)
    }

    pub(crate) fn generation(&self) -> u64 {
        self.signals.generation.load(Ordering::SeqCst)
    }

    pub(crate) fn fail_terminal(&self, error: LixError) {
        self.signals.sender.send_modify(|event| {
            if !matches!(event, ObserveInvalidationEvent::TerminalError(_)) {
                *event = ObserveInvalidationEvent::TerminalError(error);
            }
        });
    }

    pub(crate) fn bump_if_storage_changed(&self, stats: &StorageWriteSetStats) {
        if let Some(revision) = stats.observable_revision {
            observe_storage_revision(&self.signals, Some(Bytes::copy_from_slice(&revision)));
        }
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<ObserveInvalidationEvent> {
        self.signals.sender.subscribe()
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
        let mut watcher_started = self.watcher.started.lock().await;
        let event = self.signals.sender.borrow().clone();
        if let ObserveInvalidationEvent::TerminalError(error) = event {
            return Err(error);
        }
        if *watcher_started {
            return Ok(());
        }

        // A previous watcher can have marked itself stopped immediately
        // before returning. Reap it while holding the startup gate so a new
        // watcher cannot race its final storage access.
        if let Some(previous) = self
            .watcher
            .task
            .lock()
            .expect("observer watcher task lock should not poison")
            .take()
        {
            previous.cancel_and_join()?;
        }

        match storage.watch_for_changes().await {
            Ok(mut changes) => {
                // The backend watch is physical and may wake on private work.
                // Subscribe first, then establish the observable-token
                // baseline while holding the startup gate. An event racing
                // the baseline is either reflected in the first observer read
                // or remains queued for the task below to compare.
                let initial_revision = match storage.load_observable_revision().await {
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
                seed_observable_revision(&self.signals, initial_revision);
                let watched_storage = storage.clone();
                let weak_signals = Arc::downgrade(&self.signals);
                let lifecycle = Arc::clone(&self.watcher);
                let no_receivers = self.signals.sender.clone();
                let task = crate::background_task::spawn_owned(
                    "lix-observe-change-watch",
                    move || async move {
                        loop {
                            let Some(live_signals) = weak_signals.upgrade() else {
                                break;
                            };
                            let has_receivers = live_signals.sender.receiver_count() > 0;
                            drop(live_signals);
                            if !has_receivers {
                                let mut started = lifecycle.started.lock().await;
                                let still_has_receivers = weak_signals
                                    .upgrade()
                                    .is_some_and(|signals| signals.sender.receiver_count() > 0);
                                if !still_has_receivers {
                                    *started = false;
                                    break;
                                }
                                continue;
                            }
                            let changed = futures_lite::future::race(
                                async { Some(changes.changed().await) },
                                async {
                                    no_receivers.closed().await;
                                    None
                                },
                            )
                            .await;
                            match changed {
                                Some(Ok(())) => {
                                    // Only a canonical engine commit that
                                    // advanced `o` changes an observer result.
                                    // Keep the backend's physical watch contract
                                    // untouched and filter at this consumer.
                                    match watched_storage.load_observable_revision().await {
                                        Ok(current_revision) => {
                                            if let Some(signals) = weak_signals.upgrade() {
                                                observe_storage_revision(&signals, current_revision);
                                            } else {
                                                break;
                                            }
                                        }
                                        Err(error) => {
                                            if let Some(signals) = weak_signals.upgrade() {
                                                if matches!(
                                                    error,
                                                    StorageError::Fenced | StorageError::Closed(_)
                                                ) {
                                                    fail_terminal_signals(&signals, error.into());
                                                } else {
                                                    // Let the stable-read loop retry and reopen
                                                    // the watcher after a transient token read.
                                                    bump_signals(&signals);
                                                }
                                            }
                                            *lifecycle.started.lock().await = false;
                                            break;
                                        }
                                    }
                                    // Storage adapters are permitted to report
                                    // an already-ready invalidation repeatedly.
                                    // Yield so cancellation remains pollable even
                                    // when the source never becomes pending.
                                    futures_lite::future::yield_now().await;
                                }
                                None => {
                                    let mut started = lifecycle.started.lock().await;
                                    let still_has_receivers = weak_signals
                                        .upgrade()
                                        .is_some_and(|signals| signals.sender.receiver_count() > 0);
                                    if !still_has_receivers {
                                        *started = false;
                                        break;
                                    }
                                }
                                Some(Err(error)) => {
                                    if let Some(signals) = weak_signals.upgrade() {
                                        if matches!(
                                            error,
                                            StorageError::Fenced | StorageError::Closed(_)
                                        ) {
                                            fail_terminal_signals(&signals, error.into());
                                        } else {
                                            // Wake observers so the stable-read loop can retry and
                                            // reopen the adapter watch after a transient failure.
                                            bump_signals(&signals);
                                        }
                                    }
                                    *lifecycle.started.lock().await = false;
                                    break;
                                }
                            }
                        }
                    },
                )?;
                *watcher_started = true;
                *self
                    .watcher
                    .task
                    .lock()
                    .expect("observer watcher task lock should not poison") = Some(task);
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
            let initial_revision = match storage.load_observable_revision().await {
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
            seed_observable_revision(&self.signals, initial_revision);
            let weak_signals = Arc::downgrade(&self.signals);
            let lifecycle = Arc::clone(&self.watcher);
            let no_receivers = self.signals.sender.clone();
            let task = crate::background_task::spawn_owned(
                "lix-observe-invalidation",
                move || async move {
                    loop {
                        let Some(live_signals) = weak_signals.upgrade() else {
                            break;
                        };
                        let has_receivers = live_signals.sender.receiver_count() > 0;
                        drop(live_signals);
                        if !has_receivers {
                            let mut started = lifecycle.started.lock().await;
                            let still_has_receivers = weak_signals
                                .upgrade()
                                .is_some_and(|signals| signals.sender.receiver_count() > 0);
                            if !still_has_receivers {
                                *started = false;
                                break;
                            }
                            continue;
                        }
                        let elapsed = tokio::time::sleep(EXTERNAL_MUTATION_REVISION_POLL_INTERVAL);
                        tokio::pin!(elapsed);
                        let no_receivers = no_receivers.closed();
                        tokio::pin!(no_receivers);
                        tokio::select! {
                            _ = &mut elapsed => {}
                            _ = &mut no_receivers => {
                                let mut started = lifecycle.started.lock().await;
                                let still_has_receivers = weak_signals
                                    .upgrade()
                                    .is_some_and(|signals| signals.sender.receiver_count() > 0);
                                if !still_has_receivers {
                                    *started = false;
                                    break;
                                }
                            }
                        }
                        let current_revision = match storage.load_observable_revision().await {
                            Ok(revision) => revision,
                            Err(error) => {
                                let error: LixError = error.into();
                                if matches!(
                                    error.code.as_str(),
                                    LixError::CODE_STORAGE_FENCED | LixError::CODE_STORAGE_CLOSED
                                ) {
                                    if let Some(signals) = weak_signals.upgrade() {
                                        fail_terminal_signals(&signals, error);
                                    }
                                    *lifecycle.started.lock().await = false;
                                    break;
                                }
                                continue;
                            }
                        };
                        if let Some(signals) = weak_signals.upgrade() {
                            observe_storage_revision(&signals, current_revision);
                        } else {
                            break;
                        }
                    }
                },
            )?;
            *watcher_started = true;
            *self
                .watcher
                .task
                .lock()
                .expect("observer watcher task lock should not poison") = Some(task);
            Ok(())
        }
    }
}

impl Drop for ObserveInvalidation {
    fn drop(&mut self) {
        let task = self
            .watcher
            .task
            .lock()
            .expect("observer watcher task lock should not poison")
            .take();
        if let Some(task) = task {
            let _ = task.cancel_and_join();
        }
    }
}

fn bump_signals(signals: &ObserveSignals) -> u64 {
    let mut next = 0;
    signals.sender.send_modify(|event| {
        next = signals.generation.fetch_add(1, Ordering::SeqCst) + 1;
        if matches!(event, ObserveInvalidationEvent::TerminalError(_)) {
            return;
        }
        *event = ObserveInvalidationEvent::Generation(next);
    });
    next
}

fn seed_observable_revision(signals: &ObserveSignals, revision: Option<Bytes>) {
    let mut state = signals
        .observable_revision
        .lock()
        .expect("observer revision lock should not poison");
    if !state.initialized {
        state.revision = revision;
        state.initialized = true;
    }
}

fn observe_storage_revision(signals: &ObserveSignals, revision: Option<Bytes>) {
    let mut state = signals
        .observable_revision
        .lock()
        .expect("observer revision lock should not poison");
    let changed = !state.initialized || state.revision != revision;
    state.revision = revision;
    state.initialized = true;
    if changed {
        bump_signals(signals);
    }
}

fn fail_terminal_signals(signals: &ObserveSignals, error: LixError) {
    signals.sender.send_modify(|event| {
        if !matches!(event, ObserveInvalidationEvent::TerminalError(_)) {
            *event = ObserveInvalidationEvent::TerminalError(error);
        }
    });
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_bumps_publish_the_final_monotonic_generation() {
        const BUMPERS: usize = 16;
        let invalidation = Arc::new(ObserveInvalidation::new());
        let mut observer = invalidation.subscribe();
        let barrier = Arc::new(std::sync::Barrier::new(BUMPERS + 1));
        let mut tasks = Vec::with_capacity(BUMPERS);
        for _ in 0..BUMPERS {
            let invalidation = Arc::clone(&invalidation);
            let barrier = Arc::clone(&barrier);
            tasks.push(tokio::task::spawn_blocking(move || {
                barrier.wait();
                invalidation.bump();
            }));
        }
        barrier.wait();
        for task in tasks {
            task.await.expect("bump worker should finish");
        }

        let expected_generation =
            u64::try_from(BUMPERS).expect("test bump count fits in a generation");
        assert_eq!(invalidation.generation(), expected_generation);
        assert!(matches!(
            observer.borrow_and_update().clone(),
            ObserveInvalidationEvent::Generation(generation)
                if generation == expected_generation
        ));
    }

    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
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
            // Take the read snapshot before blocking so callers can exercise
            // a commit racing a stale watcher baseline.
            let read = self.inner.begin_read(options).await?;
            if self.first_read.swap(false, Ordering::AcqRel) {
                self.entered.notify_waiters();
                self.release.notified().await;
            }
            Ok(read)
        }

        async fn begin_write(
            &self,
            options: WriteOptions,
        ) -> Result<Self::Write<'_>, StorageError> {
            self.inner.begin_write(options).await
        }
    }

    #[derive(Clone)]
    struct ProbeStorage {
        inner: Memory,
        _lifetime: Arc<()>,
        reads: Arc<AtomicUsize>,
        poll_started: Arc<Notify>,
    }

    impl Storage for ProbeStorage {
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
            if self.reads.fetch_add(1, Ordering::AcqRel) > 0 {
                self.poll_started.notify_one();
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

    struct PendingChangeSource {
        entered: Arc<Notify>,
        dropped: Arc<AtomicUsize>,
        _storage_lifetime: Arc<()>,
    }

    impl Drop for PendingChangeSource {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::AcqRel);
        }
    }

    impl StorageChangeSource for PendingChangeSource {
        fn changed(
            &mut self,
        ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + '_>> {
            let entered = Arc::clone(&self.entered);
            Box::pin(async move {
                entered.notify_one();
                std::future::pending().await
            })
        }
    }

    #[derive(Clone)]
    struct PendingChangeWatchStorage {
        inner: Memory,
        entered: Arc<Notify>,
        dropped: Arc<AtomicUsize>,
        lifetime: Arc<()>,
    }

    impl Storage for PendingChangeWatchStorage {
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
            Ok(StorageChangeWatch::from_source(PendingChangeSource {
                entered: Arc::clone(&self.entered),
                dropped: Arc::clone(&self.dropped),
                _storage_lifetime: Arc::clone(&self.lifetime),
            }))
        }
    }

    struct AlwaysReadyChangeSource {
        dropped: Arc<AtomicUsize>,
        _storage_lifetime: Arc<()>,
    }

    impl Drop for AlwaysReadyChangeSource {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::AcqRel);
        }
    }

    impl StorageChangeSource for AlwaysReadyChangeSource {
        fn changed(
            &mut self,
        ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[derive(Clone)]
    struct AlwaysReadyChangeWatchStorage {
        inner: Memory,
        dropped: Arc<AtomicUsize>,
        lifetime: Arc<()>,
    }

    impl Storage for AlwaysReadyChangeWatchStorage {
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
            Ok(StorageChangeWatch::from_source(AlwaysReadyChangeSource {
                dropped: Arc::clone(&self.dropped),
                _storage_lifetime: Arc::clone(&self.lifetime),
            }))
        }
    }

    #[derive(Clone)]
    struct SignalChangeWatchStorage {
        inner: Memory,
        changes: watch::Sender<u64>,
    }

    impl SignalChangeWatchStorage {
        fn signal_physical_change(&self) {
            self.changes.send_modify(|generation| *generation += 1);
        }
    }

    struct SignalChangeSource {
        changes: watch::Receiver<u64>,
    }

    impl StorageChangeSource for SignalChangeSource {
        fn changed(
            &mut self,
        ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + '_>> {
            Box::pin(async move {
                self.changes
                    .changed()
                    .await
                    .map_err(|_| StorageError::Closed("change signal closed".into()))?;
                Ok(())
            })
        }
    }

    impl Storage for SignalChangeWatchStorage {
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
            Ok(StorageChangeWatch::from_source(SignalChangeSource {
                changes: self.changes.subscribe(),
            }))
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
        let _observer = invalidation.subscribe();
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
            *invalidation.watcher.started.lock().await,
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
                if !*invalidation.watcher.started.lock().await {
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
        assert!(*invalidation.watcher.started.lock().await);
    }

    #[tokio::test]
    async fn pending_change_watch_stops_without_receivers_and_restarts() {
        let invalidation = Arc::new(ObserveInvalidation::new());
        let entered = Arc::new(Notify::new());
        let dropped = Arc::new(AtomicUsize::new(0));
        let storage = PendingChangeWatchStorage {
            inner: Memory::new(),
            entered: Arc::clone(&entered),
            dropped: Arc::clone(&dropped),
            lifetime: Arc::new(()),
        };
        let adapter = StorageAdapter::new(storage);
        let observer = invalidation.subscribe();
        invalidation
            .ensure_external_watcher(adapter.clone())
            .await
            .expect("change watcher should start");
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("change watcher should enter its pending wait");

        drop(observer);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if !*invalidation.watcher.started.lock().await
                    && dropped.load(Ordering::Acquire) == 1
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("change watcher should stop after its last observer closes");
        assert_eq!(dropped.load(Ordering::Acquire), 1);

        let replacement = invalidation.subscribe();
        invalidation
            .ensure_external_watcher(adapter)
            .await
            .expect("change watcher should restart for a new observer");
        assert!(*invalidation.watcher.started.lock().await);
        drop(replacement);
    }

    #[tokio::test]
    async fn dropping_owner_joins_pending_change_watcher_and_releases_storage() {
        let invalidation = Arc::new(ObserveInvalidation::new());
        let owner = Arc::downgrade(&invalidation);
        let entered = Arc::new(Notify::new());
        let dropped = Arc::new(AtomicUsize::new(0));
        let storage_lifetime = Arc::new(());
        let storage_weak = Arc::downgrade(&storage_lifetime);
        let storage = PendingChangeWatchStorage {
            inner: Memory::new(),
            entered: Arc::clone(&entered),
            dropped: Arc::clone(&dropped),
            lifetime: Arc::clone(&storage_lifetime),
        };
        drop(storage_lifetime);
        let adapter = StorageAdapter::new(storage);
        let observer = invalidation.subscribe();
        invalidation
            .ensure_external_watcher(adapter)
            .await
            .expect("change watcher should start");
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("change watcher should enter its pending wait");

        drop(invalidation);
        assert!(owner.upgrade().is_none(), "owner should be fully dropped");
        assert_eq!(
            dropped.load(Ordering::Acquire),
            1,
            "joined watcher should release its pending change source"
        );
        assert!(
            storage_weak.upgrade().is_none(),
            "joined watcher should release its storage-owned change watch"
        );
        drop(observer);
    }

    #[tokio::test]
    async fn dropping_owner_joins_always_ready_change_watcher() {
        let invalidation = Arc::new(ObserveInvalidation::new());
        let owner = Arc::downgrade(&invalidation);
        let dropped = Arc::new(AtomicUsize::new(0));
        let lifetime = Arc::new(());
        let storage_lifetime = Arc::downgrade(&lifetime);
        let storage = AlwaysReadyChangeWatchStorage {
            inner: Memory::new(),
            dropped: Arc::clone(&dropped),
            lifetime: Arc::clone(&lifetime),
        };
        drop(lifetime);
        let observer = invalidation.subscribe();
        invalidation
            .ensure_external_watcher(StorageAdapter::new(storage))
            .await
            .expect("always-ready change watcher should start");
        tokio::task::yield_now().await;
        assert_eq!(
            invalidation.generation(),
            0,
            "spurious physical notifications with an unchanged observable token are ignored"
        );

        drop(invalidation);
        assert!(owner.upgrade().is_none(), "owner should be fully dropped");
        assert_eq!(
            dropped.load(Ordering::Acquire),
            1,
            "joined watcher should release its always-ready source"
        );
        assert!(
            storage_lifetime.upgrade().is_none(),
            "joined watcher should release storage retained by its source"
        );
        drop(observer);
    }

    #[tokio::test]
    async fn physical_private_change_is_ignored_but_external_visible_change_wakes_observer() {
        let inner = Memory::new();
        let (changes, _) = watch::channel(0_u64);
        let storage = SignalChangeWatchStorage { inner, changes };
        let observer_storage = StorageAdapter::new(storage.clone());
        let writer_storage = StorageAdapter::new(storage.clone());
        let invalidation = Arc::new(ObserveInvalidation::new());
        let mut observer = invalidation.subscribe();
        invalidation
            .ensure_external_watcher(observer_storage)
            .await
            .expect("observer should establish a physical watch and o baseline");

        let mut private = writer_storage.new_write_set();
        private.put(
            crate::sync::PARTIAL_READ_INTEREST_SPACE,
            crate::storage::Key(Bytes::from_static(b"recipe")),
            b"private query recipe".as_slice(),
        );
        let (_, private_stats) = writer_storage
            .commit_write_set(private, WriteOptions::default())
            .await
            .expect("external private journal write");
        assert_eq!(private_stats.observable_revision, None);
        invalidation.bump_if_storage_changed(&private_stats);
        storage.signal_physical_change();
        assert!(
            tokio::time::timeout(Duration::from_millis(40), observer.changed())
                .await
                .is_err(),
            "private physical changes must not invalidate observers"
        );
        assert_eq!(invalidation.generation(), 0);

        let mut visible = writer_storage.new_write_set();
        visible.put(
            crate::hot_state::ROW_SPACE,
            crate::storage::Key(Bytes::from_static(b"visible")),
            b"visible result".as_slice(),
        );
        let (_, visible_stats) = writer_storage
            .commit_write_set(visible, WriteOptions::default())
            .await
            .expect("external visible row write");
        storage.signal_physical_change();
        tokio::time::timeout(Duration::from_secs(1), observer.changed())
            .await
            .expect("visible revision change should wake observer")
            .expect("observer channel should remain open");
        assert_eq!(invalidation.generation(), 1);
        invalidation.bump_if_storage_changed(&visible_stats);
        assert_eq!(
            invalidation.generation(),
            1,
            "local completion after the watcher must deduplicate the accepted token"
        );
    }

    #[tokio::test]
    async fn local_and_watcher_notifications_deduplicate_the_committed_revision_in_both_orders() {
        for local_first in [true, false] {
            let storage = StorageAdapter::new(Memory::new());
            let mut visible = storage.new_write_set();
            visible.put(
                crate::hot_state::ROW_SPACE,
                crate::storage::Key(Bytes::from_static(b"visible")),
                b"committed value".as_slice(),
            );
            let (_, stats) = storage
                .commit_write_set(visible, WriteOptions::default())
                .await
                .expect("visible commit should return its accepted token");
            let token = stats
                .observable_revision
                .expect("visible commit should return the exact generated token");
            assert_eq!(
                storage.load_observable_revision().await.unwrap(),
                Some(Bytes::copy_from_slice(&token)),
                "returned token must match the committed observer revision"
            );

            let invalidation = ObserveInvalidation::new();
            seed_observable_revision(&invalidation.signals, None);
            let watcher_observation = Some(Bytes::copy_from_slice(&token));
            if local_first {
                invalidation.bump_if_storage_changed(&stats);
                assert_eq!(invalidation.generation(), 1);
                observe_storage_revision(&invalidation.signals, watcher_observation);
            } else {
                observe_storage_revision(&invalidation.signals, watcher_observation);
                assert_eq!(invalidation.generation(), 1);
                invalidation.bump_if_storage_changed(&stats);
            }
            assert_eq!(
                invalidation.generation(),
                1,
                "local completion and watcher must share one revision observation"
            );

            let mut private = storage.new_write_set();
            private.put(
                crate::sync::PARTIAL_READ_INTEREST_SPACE,
                crate::storage::Key(Bytes::from_static(b"private")),
                b"private journal entry".as_slice(),
            );
            let (_, private_stats) = storage
                .commit_write_set(private, WriteOptions::default())
                .await
                .expect("private commit should succeed");
            assert_eq!(private_stats.observable_revision, None);
            invalidation.bump_if_storage_changed(&private_stats);
            observe_storage_revision(&invalidation.signals, Some(Bytes::copy_from_slice(&token)));
            assert_eq!(
                invalidation.generation(),
                1,
                "a private completion must neither bump nor reset the last visible token"
            );
        }
    }

    #[tokio::test]
    async fn local_revision_during_stale_watcher_baseline_is_not_overwritten_and_polling_still_wakes() {
        let invalidation = Arc::new(ObserveInvalidation::new());
        let mut observer = invalidation.subscribe();
        let storage = BlockingFirstReadStorage::new();
        let adapter = StorageAdapter::new(storage.clone());
        let startup = {
            let invalidation = Arc::clone(&invalidation);
            let adapter = adapter.clone();
            tokio::spawn(async move { invalidation.ensure_external_watcher(adapter).await })
        };
        tokio::time::timeout(Duration::from_secs(1), storage.wait_for_initial_read())
            .await
            .expect("watcher should block after capturing its stale initial snapshot");

        let mut visible = adapter.new_write_set();
        visible.put(
            crate::hot_state::ROW_SPACE,
            crate::storage::Key(Bytes::from_static(b"visible")),
            b"first value".as_slice(),
        );
        let (_, stats) = adapter
            .commit_write_set(visible, WriteOptions::default())
            .await
            .expect("local visible commit should succeed during baseline read");
        invalidation.bump_if_storage_changed(&stats);
        assert_eq!(invalidation.generation(), 1);
        observer
            .changed()
            .await
            .expect("local commit should notify the observer");
        observer.borrow_and_update();

        storage.release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), startup)
            .await
            .expect("watcher startup should finish after baseline release")
            .expect("watcher startup task should not panic")
            .expect("watcher should start its native revision poller");
        assert!(
            tokio::time::timeout(Duration::from_millis(350), observer.changed())
                .await
                .is_err(),
            "stale baseline must not overwrite the local token and cause a duplicate poll wake"
        );
        assert_eq!(invalidation.generation(), 1);

        let mut external = adapter.new_write_set();
        external.put(
            crate::hot_state::ROW_SPACE,
            crate::storage::Key(Bytes::from_static(b"external")),
            b"second value".as_slice(),
        );
        adapter
            .commit_write_set(external, WriteOptions::default())
            .await
            .expect("external visible commit should succeed");
        tokio::time::timeout(Duration::from_secs(1), observer.changed())
            .await
            .expect("native polling must observe a distinct external revision")
            .expect("observer channel should remain open");
        assert_eq!(invalidation.generation(), 2);
    }

    #[tokio::test]
    async fn dropping_owner_joins_poll_watcher_and_releases_storage() {
        let invalidation = Arc::new(ObserveInvalidation::new());
        let owner = Arc::downgrade(&invalidation);
        let lifetime = Arc::new(());
        let storage_lifetime = Arc::downgrade(&lifetime);
        let reads = Arc::new(AtomicUsize::new(0));
        let poll_started = Arc::new(Notify::new());
        let storage = ProbeStorage {
            inner: Memory::new(),
            _lifetime: Arc::clone(&lifetime),
            reads,
            poll_started: Arc::clone(&poll_started),
        };
        let observer = invalidation.subscribe();
        invalidation
            .ensure_external_watcher(StorageAdapter::new(storage.clone()))
            .await
            .expect("unsupported change watch should start revision polling");
        tokio::time::timeout(Duration::from_secs(2), poll_started.notified())
            .await
            .expect("revision poller should perform a poll after its baseline read");
        drop(storage);
        drop(lifetime);

        let shutdown_started = tokio::time::Instant::now();
        drop(invalidation);
        let shutdown_elapsed = shutdown_started.elapsed();
        assert!(owner.upgrade().is_none(), "owner should be fully dropped");
        assert!(
            storage_lifetime.upgrade().is_none(),
            "joined poller should release its storage clone"
        );
        println!(
            "OBSERVER_WATCHER_SHUTDOWN_PROFILE_JSON={}",
            serde_json::json!({
                "watcher": "revision_poll",
                "shutdown_us": shutdown_elapsed.as_micros(),
                "poll_interval_us": EXTERNAL_MUTATION_REVISION_POLL_INTERVAL.as_micros(),
            })
        );
        drop(observer);
    }
}
