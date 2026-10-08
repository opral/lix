use std::future::Future;
use std::sync::Mutex;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use std::sync::{Arc, OnceLock};

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use futures_util::FutureExt;

use crate::LixError;

/// Runs engine-owned background work without borrowing the embedding
/// application's async runtime.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) fn spawn<Factory, F>(name: &str, factory: Factory) -> Result<(), LixError>
where
    Factory: FnOnce() -> F + Send + 'static,
    F: Future<Output = ()> + 'static,
{
    std::thread::Builder::new()
        .name(name.to_owned())
        .spawn(move || futures_lite::future::block_on(factory()))
        .map(|_| ())
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("start {name}: {error}"),
            )
        })
}

/// Runs owned async work on one persistent engine runtime, independently of the
/// embedding application's runtime. The executor is started lazily once per
/// process, and task inputs are owned.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) fn spawn_runtime_compatible<Factory, F>(
    name: &str,
    factory: Factory,
) -> Result<(), LixError>
where
    Factory: FnOnce() -> F + Send + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    let executor = shared_executor()?;
    let task_name = name.to_owned();
    executor.handle.spawn(async move {
        if std::panic::AssertUnwindSafe(factory())
            .catch_unwind()
            .await
            .is_err()
        {
            tracing::error!(task = %task_name, "engine-owned background task panicked");
        }
    });
    Ok(())
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
struct SharedExecutor {
    handle: tokio::runtime::Handle,
    state: Arc<std::sync::atomic::AtomicU8>,
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
static SHARED_EXECUTOR: OnceLock<Result<Arc<SharedExecutor>, String>> = OnceLock::new();

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
fn shared_executor() -> Result<&'static Arc<SharedExecutor>, LixError> {
    let executor = SHARED_EXECUTOR.get_or_init(|| {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .thread_name("lix-transfer-worker")
            .enable_all()
            .build()
            .map_err(|error| format!("build shared engine background runtime: {error}"))?;
        let handle = runtime.handle().clone();
        let state = Arc::new(std::sync::atomic::AtomicU8::new(0));
        let worker_state = Arc::clone(&state);
        std::thread::Builder::new()
            .name("lix-engine-background-runtime".to_owned())
            .spawn(move || {
                struct RuntimeStateGuard(Arc<std::sync::atomic::AtomicU8>);
                impl Drop for RuntimeStateGuard {
                    fn drop(&mut self) {
                        self.0.store(2, std::sync::atomic::Ordering::Release);
                    }
                }
                worker_state.store(1, std::sync::atomic::Ordering::Release);
                let _state_guard = RuntimeStateGuard(worker_state);
                runtime.block_on(std::future::pending::<()>());
            })
            .map_err(|error| format!("start shared engine background runtime: {error}"))?;
        Ok(Arc::new(SharedExecutor { handle, state }))
    });
    let executor = executor.as_ref().map_err(|message| {
        LixError::new(LixError::CODE_INTERNAL_ERROR, message.clone())
    })?;
    if executor
        .state
        .load(std::sync::atomic::Ordering::Acquire)
        == 2
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "shared engine background runtime has stopped",
        ));
    }
    Ok(executor)
}

/// A background task whose owner can synchronously cancel it and, on native
/// targets, wait for its worker thread to finish.
#[derive(Debug)]
pub(crate) struct OwnedBackgroundTask {
    cancel_sender: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl OwnedBackgroundTask {
    pub(crate) fn cancel_and_join(&self) -> Result<(), LixError> {
        self.cancel();
        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        {
            let mut worker = self.worker.lock().map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "owned background task join lock was poisoned",
                )
            })?;
            if let Some(worker) = worker.take() {
                worker.join().map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "owned background task panicked",
                    )
                })?;
            }
        }
        Ok(())
    }

    fn cancel(&self) {
        // Sending more than once is harmless; take the sender so cancellation
        // and Drop can share this path.
        if let Ok(mut cancel) = self.cancel_sender.lock()
            && let Some(cancel) = cancel.take()
        {
            let _ = cancel.send(());
        }
    }
}

impl Drop for OwnedBackgroundTask {
    fn drop(&mut self) {
        let _ = self.cancel_and_join();
    }
}

/// Runs owned work independently of the embedding runtime while retaining a
/// cancellation and join handle with the owner.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) fn spawn_owned<Factory, F>(
    name: &str,
    factory: Factory,
) -> Result<OwnedBackgroundTask, LixError>
where
    Factory: FnOnce() -> F + Send + 'static,
    F: Future<Output = ()> + 'static,
{
    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
    let (startup_tx, startup_rx) = std::sync::mpsc::sync_channel(1);
    let worker = std::thread::Builder::new()
        .name(name.to_owned())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => {
                    let _ = startup_tx.send(Ok(()));
                    runtime
                }
                Err(error) => {
                    let _ = startup_tx.send(Err(error.to_string()));
                    return;
                }
            };
            runtime.block_on(async move {
                let work = factory();
                let cancelled = async move {
                    let _ = cancel_rx.await;
                };
                futures_lite::future::race(work, cancelled).await;
            });
        })
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("start {name}: {error}"),
            )
        })?;
    match startup_rx.recv() {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            let _ = worker.join();
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("build {name} runtime: {error}"),
            ));
        }
        Err(error) => {
            let _ = worker.join();
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("start {name} runtime: {error}"),
            ));
        }
    }
    Ok(OwnedBackgroundTask {
        cancel_sender: Mutex::new(Some(cancel_tx)),
        worker: Mutex::new(Some(worker)),
    })
}

/// Browser Wasm has no threads; dropping the owner cancels its local future.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub(crate) fn spawn_owned<Factory, F>(
    _name: &str,
    factory: Factory,
) -> Result<OwnedBackgroundTask, LixError>
where
    Factory: FnOnce() -> F + Send + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
    wasm_bindgen_futures::spawn_local(async move {
        let work = factory();
        let cancelled = async move {
            let _ = cancel_rx.await;
        };
        futures_lite::future::race(work, cancelled).await;
    });
    Ok(OwnedBackgroundTask {
        cancel_sender: Mutex::new(Some(cancel_tx)),
    })
}

/// Browser Wasm has no threads; its embedding worker owns a local task queue.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub(crate) fn spawn<Factory, F>(_name: &str, factory: Factory) -> Result<(), LixError>
where
    Factory: FnOnce() -> F + 'static,
    F: Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(factory());
    Ok(())
}

/// Browser Wasm has no threads or Tokio runtime handle; its embedding worker
/// owns the local task queue for engine cleanup.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub(crate) fn spawn_runtime_compatible<Factory, F>(
    _name: &str,
    factory: Factory,
) -> Result<(), LixError>
where
    Factory: FnOnce() -> F + 'static,
    F: Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(factory());
    Ok(())
}
