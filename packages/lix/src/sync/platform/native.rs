//! Native mechanics for the shared synchronization state machine.

use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::LixError;

#[derive(Debug)]
pub(in crate::sync) struct SyncTask {
    finished: Arc<AtomicBool>,
    finished_notify: Arc<tokio::sync::Notify>,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
}

pub(in crate::sync) fn spawn_sync_task<Worker>(worker: Worker) -> Result<SyncTask, LixError>
where
    Worker: Future<Output = ()> + Send + 'static,
{
    let finished = Arc::new(AtomicBool::new(false));
    let worker_finished = Arc::clone(&finished);
    let finished_notify = Arc::new(tokio::sync::Notify::new());
    let worker_finished_notify = Arc::clone(&finished_notify);
    // Plugin reconciliation and the nested single-thread Tokio runtime use
    // more than Rust's 2 MiB default stack, and full-history hydration can
    // cross 4 MiB in debug builds. The reservation is demand-paged.
    let worker = std::thread::Builder::new()
        .name("lix-sync".to_owned())
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            let _done = WorkerDone {
                finished: worker_finished,
                notify: worker_finished_notify,
            };
            let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            runtime.block_on(worker);
        })
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("start sync worker: {error}"),
            )
        })?;
    Ok(SyncTask {
        finished,
        finished_notify,
        worker: Mutex::new(Some(worker)),
    })
}

impl SyncTask {
    pub(in crate::sync) async fn join(&self) -> Result<(), LixError> {
        loop {
            let notified = self.finished_notify.notified();
            if self.finished.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
        let worker = self
            .worker
            .lock()
            .map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "sync worker lock is poisoned",
                )
            })?
            .take();
        if let Some(worker) = worker {
            worker.join().map_err(|_| {
                LixError::new(LixError::CODE_INTERNAL_ERROR, "sync worker panicked")
            })?;
        }
        Ok(())
    }
}

pub(crate) async fn sleep(duration: Duration) {
    tokio::time::sleep(duration).await;
}

struct WorkerDone {
    finished: Arc<AtomicBool>,
    notify: Arc<tokio::sync::Notify>,
}

impl Drop for WorkerDone {
    fn drop(&mut self) {
        self.finished.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }
}
